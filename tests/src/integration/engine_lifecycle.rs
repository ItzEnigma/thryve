// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Full engine lifecycle integration tests.
//!
//! Unlike the unit tests under `tests_run` (which test `summon_requirements`/
//! `create_service` in isolation without ever running the engine loop), these
//! spin up the real `Engine` on a background thread via `run_interactive`/
//! `create_service` and drive it to completion through real channels - the
//! only way to prove the full task/notification pipeline actually works.

mod tests_run_interactive {
    use std::sync::Arc;

    use thryve_run::run::run_interactive::{run_interactive, summon_requirements};
    use thryve_utils::bases::observer::Observer;
    use thryve_utils::bases::plugin::Plugin;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::extras::jobs::{ObsJob, TaskJob};
    use thryve_utils::packets::task::Task;
    use thryve_utils::res::TaskResult;
    use thryve_utils::traits::observer_logic::ObsLogic;
    use thryve_utils::traits::plugin_logic::PlgLogic;

    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        thryve_utils::impl_as_any!(TestObsLogic);
        fn clean_up(&self, _observer: Arc<Observer>) {}
    }

    struct TestPlgLogic;
    impl PlgLogic for TestPlgLogic {
        thryve_utils::impl_as_any!(TestPlgLogic);
        fn clean_up(&self) {}
    }

    #[test]
    fn test_summon_requirements_creates_ctx() {
        // Arrange
        let config = Config::default();

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert!(ctx.config.service.can_stop);
        assert!(ctx.config.service.can_pause);
    }

    #[test]
    fn test_summon_requirements_with_custom_config() {
        // Arrange
        let mut config = Config::default();
        config.pipes.task_pipe_capacity = 100;
        config.pipes.notif_pipe_capacity = 200;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 100);
        assert_eq!(ctx.config.pipes.notif_pipe_capacity, 200);
    }

    #[test]
    fn test_summon_requirements_with_disabled_service_features() {
        // Arrange
        let mut config = Config::default();
        config.service.can_stop = false;
        config.service.can_pause = false;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert!(!ctx.config.service.can_stop);
        assert!(!ctx.config.service.can_pause);
    }

    #[test]
    fn test_summon_requirements_with_zero_pipe_capacity() {
        // Arrange
        let mut config = Config::default();
        config.pipes.task_pipe_capacity = 0;
        config.pipes.notif_pipe_capacity = 0;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 0);
        assert_eq!(ctx.config.pipes.notif_pipe_capacity, 0);
    }

    #[test]
    fn test_summon_requirements_preserves_all_config_values() {
        // Arrange
        let mut config = Config::default();
        config.engine.sleep_tick_rate = 150;
        config.service.can_stop = false;
        config.pipes.task_pipe_capacity = 50;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert_eq!(ctx.config.engine.sleep_tick_rate, 150);
        assert!(!ctx.config.service.can_stop);
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 50);
    }

    #[test]
    fn test_summon_requirements_with_debug_enabled() {
        // Arrange
        let mut config = Config::default();
        config.debugging.is_debugging = true;
        config.debugging.visual_graph = true;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert!(ctx.config.debugging.is_debugging);
        assert!(ctx.config.debugging.visual_graph);
    }

    #[test]
    fn test_summon_requirements_with_large_pipe_capacity() {
        // Arrange
        let mut config = Config::default();
        config.pipes.task_pipe_capacity = 10000;
        config.pipes.notif_pipe_capacity = 10000;

        // Act
        let ctx = summon_requirements(config);

        // Assert
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 10000);
        assert_eq!(ctx.config.pipes.notif_pipe_capacity, 10000);
    }

    #[test]
    fn test_run_interactive_processes_task_and_exits_on_fin() {
        // Arrange
        let config = Config::default();
        let ctx = summon_requirements(config);
        let pkt = ctx.packet_handle();

        // run_interactive's spawned thread calls init_panic_hook(), which
        // installs a process-wide panic hook that calls std::process::exit(1)
        // on ANY panic. That hook has no test-scoping -- left in place, it
        // silently breaks every #[should_panic] test that runs afterward in
        // this same process. Save and restore the hook around this test to
        // keep that side effect contained.
        let prev_hook = std::panic::take_hook();

        // Act - starts the engine's run loop on a background thread.
        let handle = run_interactive(ctx).expect("run_interactive should always return a handle");

        // The engine's run loop only ever exits once it processes a task
        // result with result_type == "Fin" (engine.rs handle_result) -- this
        // is the only way to make the spawned thread actually terminate.
        let shutdown_task = Task::new("Shutdown", 0, TaskJob::new(TaskResult::fin));
        pkt.send_task(shutdown_task);

        // Assert - if the loop never exits, this blocks forever instead of
        // returning, so a successful join is itself the proof it works.
        handle
            .join()
            .expect("engine thread should exit cleanly after the Fin task");

        std::panic::set_hook(prev_hook);
    }

    #[test]
    fn test_run_interactive_loads_observers_and_plugins_before_looping() {
        // Arrange
        let mut config = Config::default();
        config.debugging.is_debugging = true;
        config.debugging.wait_before_execution = 0;
        let mut ctx = summon_requirements(config);
        let pkt = ctx.packet_handle();

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        ctx.observers.push(Observer::new(
            "extra_observer",
            "ExtraObserver",
            obs_core,
            obs_job,
            ctx.packet_handle(),
        ));

        let plg_core = Arc::new(TestPlgLogic);
        let plg_job = TaskJob::new(|| TaskResult::new("Ok", "plugin ran".to_string(), "Main"));
        ctx.plugins.push(Plugin::new(
            "extra_plugin",
            "ExtraPlugin",
            plg_core,
            ctx.packet_handle(),
            plg_job,
        ));

        // See test_run_interactive_processes_task_and_exits_on_fin -- same
        // process-wide panic hook containment is needed here.
        let prev_hook = std::panic::take_hook();

        // Act - this exercises the first-iteration setup branches that load
        // any observers/plugins registered on the context, plus the
        // debug-mode wait-before-execution branch (set to 0 to stay fast).
        let handle = run_interactive(ctx).expect("run_interactive should always return a handle");

        let shutdown_task = Task::new("Shutdown", 0, TaskJob::new(TaskResult::fin));
        pkt.send_task(shutdown_task);

        // Assert
        handle
            .join()
            .expect("engine thread should exit cleanly after the Fin task");

        std::panic::set_hook(prev_hook);
    }
}

mod tests_run_service {
    use thryve_run::run::run_service::create_service;
    use thryve_utils::cfg::config::Config;

    #[test]
    fn test_create_service_with_default_config() {
        // Arrange
        let config = Config::default();

        // Act
        let (service, _) = create_service(config);

        // Assert - service and ctx should be created successfully
        // The service struct has Arc<AtomicBool> fields which can't be directly compared
        assert!(!service.is_running.load(std::sync::atomic::Ordering::SeqCst));
        assert!(!service.is_paused.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_create_service_with_custom_pipe_capacity() {
        // Arrange
        let mut config = Config::default();
        config.pipes.task_pipe_capacity = 100;
        config.pipes.notif_pipe_capacity = 200;

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 100);
        assert_eq!(ctx.config.pipes.notif_pipe_capacity, 200);
    }

    #[test]
    fn test_create_service_with_disabled_service_features() {
        // Arrange
        let mut config = Config::default();
        config.service.can_stop = false;
        config.service.can_pause = false;

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert!(!ctx.config.service.can_stop);
        assert!(!ctx.config.service.can_pause);
    }

    #[test]
    fn test_create_service_with_debug_enabled() {
        // Arrange
        let mut config = Config::default();
        config.debugging.is_debugging = true;
        config.debugging.visual_graph = true;

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert!(ctx.config.debugging.is_debugging);
        assert!(ctx.config.debugging.visual_graph);
    }

    #[test]
    fn test_create_service_preserves_all_config_values() {
        // Arrange
        let mut config = Config::default();
        config.engine.sleep_tick_rate = 150;
        config.service.can_stop = true;
        config.service.can_pause = true;
        config.pipes.task_pipe_capacity = 50;

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert_eq!(ctx.config.engine.sleep_tick_rate, 150);
        assert!(ctx.config.service.can_stop);
        assert!(ctx.config.service.can_pause);
        assert_eq!(ctx.config.pipes.task_pipe_capacity, 50);
    }

    #[test]
    fn test_create_service_returns_valid_context() {
        // Arrange
        let config = Config::default();

        // Act
        let (_, ctx) = create_service(config);

        // Assert
        assert!(ctx.config.service.can_stop); // can_stop should be true
        assert!(ctx.config.service.can_pause); // can_pause should be true
    }
}
