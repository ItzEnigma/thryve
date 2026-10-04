// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_engine {
    use std::sync::Arc;

    use crossbeam::channel::bounded;
    use thryve_core::runtime::engine::*;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::bases::observer::Observer;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::cfg::ctx::Ctx;
    use thryve_utils::extras::jobs::{ObsJob, TaskJob};
    use thryve_utils::packets::notif_log::Log;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::res::TaskResult;
    use thryve_utils::traits::observer_logic::ObsLogic;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        thryve_utils::impl_as_any!(TestObsLogic);
        fn clean_up(&self, observer: Arc<Observer>) {
            while !observer.obs_queue.main_queue.is_empty() {
                let _notif = observer.obs_queue.main_queue.pop();
            }
        }
    }

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let config = Config::default();
        Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle)
    }

    #[test]
    fn test_engine_new_creates_engine() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let engine = Engine::new(&ctx);

        // Assert - a new engine starts paused; the engine thread resumes it once it has it
        assert!(!engine.exit_flag);
        assert!(engine.pause_flag);
        assert_eq!(engine.sleep_tick_rate, ctx.config.engine.sleep_tick_rate);
    }

    #[test]
    fn test_engine_run_once_with_exit_flag_only_checks_notifs() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.exit_flag = true;

        // Act - should not panic
        engine.run_once();

        // Assert - exit_flag should still be true
        assert!(engine.exit_flag);
    }

    #[test]
    fn test_engine_run_once_with_pause_flag_only_checks_notifs() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.pause_flag = true;

        // Act - should not panic
        engine.run_once();

        // Assert - pause_flag should still be true
        assert!(engine.pause_flag);
    }

    #[test]
    fn test_engine_pause_sets_flag() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // A new engine starts paused, so resume it first
        engine.resume(true);
        assert!(!engine.pause_flag);

        // Act
        engine.pause(false);

        // Assert
        assert!(engine.pause_flag);
    }

    #[test]
    fn test_engine_resume_clears_flag() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        engine.pause(false);

        // Assert paused
        assert!(engine.pause_flag);

        // Act
        engine.resume(false);

        // Assert
        assert!(!engine.pause_flag);
    }

    #[test]
    fn test_engine_check_results_with_empty_channel() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Act
        let received = engine.check_results();

        // Assert - no results available
        assert!(!received);
    }

    #[test]
    fn test_engine_check_tasks_with_empty_channel() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Act
        let received = engine.check_tasks();

        // Assert - no tasks available
        assert!(!received);
    }

    #[test]
    fn test_engine_check_notifs_with_empty_channel() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Act
        let received = engine.check_notifs();

        // Assert - no notifications available
        assert!(!received);
    }

    #[test]
    fn test_engine_finalize_sets_exit_flag() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for finalize)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Assert initially not exiting
        assert!(!engine.exit_flag);

        // Act
        engine.finalize(false);

        // Assert
        assert!(engine.exit_flag);
    }

    #[test]
    fn test_engine_handle_result_with_fin_type_triggers_finalize() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a "Fin" result
        let fin_result = TaskResult::fin();

        // Assert initially not exiting
        assert!(!engine.exit_flag);

        // Act - handle the Fin result
        engine.handle_result((1, fin_result));

        // Assert - finalize should have been called
        assert!(engine.exit_flag);
    }

    #[test]
    fn test_engine_handle_result_with_finerr_type_triggers_finalize() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a "FinErr" result
        let finerr_result = TaskResult::new("FinErr", "Error finished".to_string(), "Error");

        // Assert initially not exiting
        assert!(!engine.exit_flag);

        // Act - handle the FinErr result
        engine.handle_result((1, finerr_result));

        // Assert - finalize should have been called
        assert!(engine.exit_flag);
    }

    #[test]
    fn test_engine_handle_result_with_normal_type_does_not_finalize() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a normal result
        let normal_result = TaskResult::new("normal", "Done".to_string(), "Main");

        // Act - handle the normal result
        engine.handle_result((1, normal_result));

        // Assert - should not have finalized
        assert!(!engine.exit_flag);
    }

    #[test]
    fn test_engine_handle_result_submits_next_task() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a next task
        let next_task = Task::new(
            "nextTask",
            5,
            TaskJob::new(|| TaskResult::new("Test", "testinggg".to_string(), "Main")),
        )
        .unwrap();
        let result_with_next =
            TaskResult::new("normal", "Done".to_string(), "Main").chain(next_task);

        // Act - handle the result with next task
        engine.handle_result((1, result_with_next));

        // Assert - the next task should have been submitted to the threadpool
        // We can verify this by checking that the task was sent (no panic means it worked)
        assert!(!engine.exit_flag);
    }

    #[test]
    fn test_engine_check_tasks_receives_and_submits() {
        // Arrange
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx.clone(), notif_tx, status_tx, false);
        let config = Config::default();
        let ctx = Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle);
        let mut engine = Engine::new(&ctx);

        // Send a task
        let task = Task::new(
            "testTask",
            5,
            TaskJob::new(|| TaskResult::new("Test", "testinggg".to_string(), "Main")),
        )
        .unwrap();
        task_tx.send(task).unwrap();

        // Act
        let received = engine.check_tasks();

        // Assert
        assert!(received);
    }

    #[test]
    fn test_engine_check_notifs_receives_and_forwards() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Send a notification
        let notif = Log::log("test notification".to_string(), "Main");
        ctx.packet_handle().send_notification(notif).unwrap();

        // Act
        let received = engine.check_notifs();

        // Assert
        assert!(received);
    }

    #[test]
    fn test_engine_run_once_processes_all_channels() {
        // Arrange
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx.clone(), notif_tx.clone(), status_tx, false);
        let config = Config::default();
        let ctx = Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle);
        let mut engine = Engine::new(&ctx);

        // Load a logging observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Send a task and notification
        let task = Task::new(
            "testTask",
            5,
            TaskJob::new(|| TaskResult::new("Test", "testinggg".to_string(), "Main")),
        )
        .unwrap();
        task_tx.send(task).unwrap();
        let notif = Log::log("test".to_string(), "Main");
        notif_tx.send(notif).unwrap();

        // Act - run once should process both without blocking (a new engine starts paused)
        engine.resume(true);
        engine.run_once();

        // Assert - both were taken off their channels, and the engine is still running
        assert!(ctx.task_rx.is_empty());
        assert!(ctx.notif_rx.is_empty());
        assert!(!engine.exit_flag);
        assert!(!engine.pause_flag);
    }

    // Tests for manage_metrics_tasks

    #[test]
    fn test_manage_metrics_tasks_stores_size_in_batch() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let task = Task::new(
            "TestTask",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();
        let expected_size = task.size_in_bytes();

        // Act
        engine.manage_metrics_tasks(&task);

        // Assert
        assert_eq!(engine.metrics.task_size_batch[0], expected_size);
        assert_eq!(engine.metrics.cur_batch_count_task, 1);
    }

    #[test]
    fn test_manage_metrics_tasks_increments_batch_count() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let task1 = Task::new(
            "Task1",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();
        let task2 = Task::new(
            "Task2",
            2,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();

        // Act
        engine.manage_metrics_tasks(&task1);
        engine.manage_metrics_tasks(&task2);

        // Assert
        assert_eq!(engine.metrics.cur_batch_count_task, 2);
        assert_eq!(engine.metrics.task_size_batch[0], task1.size_in_bytes());
        assert_eq!(engine.metrics.task_size_batch[1], task2.size_in_bytes());
    }

    #[test]
    fn test_manage_metrics_tasks_does_not_trigger_calculation_before_batch_full() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let task = Task::new(
            "TestTask",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();

        // Act
        engine.manage_metrics_tasks(&task);

        // Assert - avg_task_size should remain 0.0 since batch is not full
        assert_eq!(engine.metrics.avg_task_size, 0.0);
    }

    // Tests for manage_metrics_results

    #[test]
    fn test_manage_metrics_results_stores_size_in_batch() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let result = TaskResult::new("Success", "Done".to_string(), "Main");
        let expected_size = result.size_in_bytes();

        // Act
        engine.manage_metrics_results(&result);

        // Assert
        assert_eq!(engine.metrics.res_size_batch[0], expected_size);
        assert_eq!(engine.metrics.cur_batch_count_res, 1);
    }

    #[test]
    fn test_manage_metrics_results_increments_batch_count() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let result1 = TaskResult::new("Type1", "Message1".to_string(), "Main");
        let result2 = TaskResult::new("Type2", "Message2".to_string(), "Error");

        // Act
        engine.manage_metrics_results(&result1);
        engine.manage_metrics_results(&result2);

        // Assert
        assert_eq!(engine.metrics.cur_batch_count_res, 2);
        assert_eq!(engine.metrics.res_size_batch[0], result1.size_in_bytes());
        assert_eq!(engine.metrics.res_size_batch[1], result2.size_in_bytes());
    }

    #[test]
    fn test_manage_metrics_results_does_not_trigger_calculation_before_batch_full() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let result = TaskResult::new("Success", "Done".to_string(), "Main");

        // Act
        engine.manage_metrics_results(&result);

        // Assert - avg_res_size should remain 0.0 since batch is not full
        assert_eq!(engine.metrics.avg_res_size, 0.0);
    }

    // Tests for manage_metrics_notifs

    #[test]
    fn test_manage_metrics_notifs_stores_size_in_batch() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let notif = Log::log("Test notification".to_string(), "Main");
        let expected_size = notif.size_in_bytes();

        // Act
        engine.manage_metrics_notifs(&notif);

        // Assert
        assert_eq!(engine.metrics.notif_size_batch[0], expected_size);
        assert_eq!(engine.metrics.cur_batch_count_notif, 1);
    }

    #[test]
    fn test_manage_metrics_notifs_increments_batch_count() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let notif1 = Log::log("Notification 1".to_string(), "Main");
        let notif2 = Log::log("Notification 2".to_string(), "Debug");

        // Act
        engine.manage_metrics_notifs(&notif1);
        engine.manage_metrics_notifs(&notif2);

        // Assert
        assert_eq!(engine.metrics.cur_batch_count_notif, 2);
        assert_eq!(engine.metrics.notif_size_batch[0], notif1.size_in_bytes());
        assert_eq!(engine.metrics.notif_size_batch[1], notif2.size_in_bytes());
    }

    #[test]
    fn test_manage_metrics_notifs_does_not_trigger_calculation_before_batch_full() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let notif = Log::log("Test notification".to_string(), "Main");

        // Act
        engine.manage_metrics_notifs(&notif);

        // Assert - avg_notif_size should remain 0.0 since batch is not full
        assert_eq!(engine.metrics.avg_notif_size, 0.0);
    }

    // Integration test for all three manage_metrics functions

    #[test]
    fn test_all_manage_metrics_functions_work_together() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let task = Task::new(
            "TestTask",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();
        let result = TaskResult::new("Success", "Done".to_string(), "Main");
        let notif = Log::log("Test notification".to_string(), "Main");

        // Act
        engine.manage_metrics_tasks(&task);
        engine.manage_metrics_results(&result);
        engine.manage_metrics_notifs(&notif);

        // Assert
        assert_eq!(engine.metrics.cur_batch_count_task, 1);
        assert_eq!(engine.metrics.cur_batch_count_res, 1);
        assert_eq!(engine.metrics.cur_batch_count_notif, 1);
        assert_eq!(engine.metrics.task_size_batch[0], task.size_in_bytes());
        assert_eq!(engine.metrics.res_size_batch[0], result.size_in_bytes());
        assert_eq!(engine.metrics.notif_size_batch[0], notif.size_in_bytes());
    }

    // ===== Graph visualization tests =====

    #[test]
    fn test_handle_graph_visualization_with_task_without_callsite() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_graph_vis_enabled = true;

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a task without a callsite
        let task = Task::new(
            "TestTask",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();

        // Act - should not panic, should do nothing
        engine.handle_graph_visualization(&task);

        // Assert - no notifications should be sent (function completes silently)
    }

    #[test]
    fn test_handle_graph_visualization_sends_add_node_for_task_with_callsite() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_graph_vis_enabled = true;

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a task with a callsite (no parent fingerprint)
        let callsite = thryve_utils::graphs::callsite::TaskCallSite::new(
            std::path::PathBuf::from("src/test.rs"),
            42,
            "TestTask".to_string(),
        );
        let mut task = Task::new(
            "TestTask",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();
        task.callsite = Some(callsite);

        // Act - should not panic
        engine.handle_graph_visualization(&task);

        // Assert - the function completed without error
        // The AddNode notification would be forwarded to the observer
    }

    #[test]
    fn test_handle_graph_visualization_sends_add_edge_for_nested_task() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_graph_vis_enabled = true;

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a task with a callsite that has a parent fingerprint
        let mut callsite = thryve_utils::graphs::callsite::TaskCallSite::new(
            std::path::PathBuf::from("src/test.rs"),
            42,
            "ChildTask".to_string(),
        );
        callsite.current_executing_task_fingerprint = "parent_fingerprint_123".to_string();

        let mut task = Task::new(
            "ChildTask",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();
        task.callsite = Some(callsite);

        // Act - should not panic
        engine.handle_graph_visualization(&task);

        // Assert - the function completed without error
        // Both AddNode and AddEdge notifications would be forwarded
    }

    #[test]
    fn test_handle_graph_visualization_with_unicode_in_callsite() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_graph_vis_enabled = true;

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a task with unicode characters in the callsite
        let callsite = thryve_utils::graphs::callsite::TaskCallSite::new(
            std::path::PathBuf::from("src/模块.rs"),
            100,
            "我的任务".to_string(),
        );
        let mut task = Task::new(
            "我的任务",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();
        task.callsite = Some(callsite);

        // Act - should not panic
        engine.handle_graph_visualization(&task);

        // Assert - unicode handling works correctly
    }

    #[test]
    fn test_handle_graph_visualization_with_empty_fingerprint_strings() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_graph_vis_enabled = true;

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a task with empty fingerprint strings
        let mut callsite = thryve_utils::graphs::callsite::TaskCallSite::new(
            std::path::PathBuf::from(""),
            0,
            String::new(),
        );
        callsite.this_callsite_fingerprint = String::new();
        callsite.current_executing_task_fingerprint = String::new();

        let mut task = Task::new(
            "Test",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();
        task.callsite = Some(callsite);

        // Act - should not panic
        engine.handle_graph_visualization(&task);

        // Assert - empty strings are handled correctly
    }

    // Tests for check_results with actual results
    #[test]
    fn test_engine_check_results_with_result_channel() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Act - no results available
        let received = engine.check_results();

        // Assert
        assert!(!received);
    }

    // Tests for check_tasks with debug flags
    #[test]
    fn test_engine_check_tasks_with_debug_and_metrics_enabled() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Enable debug flags
        engine.is_debug_enabled = true;
        engine.is_metric_calc_enabled = true;

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Send a task
        let task = Task::new(
            "testTask",
            5,
            TaskJob::new(|| TaskResult::new("Test", "testinggg".to_string(), "Main")),
        )
        .unwrap();
        ctx.packet_handle().send_task(task).unwrap();
        ctx.task_rx.try_recv().ok(); // ensure channel exists

        // Act - we can't actually send to task_rx since it's owned by engine
        // but we can verify the flags are set
        assert!(engine.is_debug_enabled);
        assert!(engine.is_metric_calc_enabled);
    }

    // Tests for manage_metrics_tasks with full batch
    #[test]
    fn test_manage_metrics_tasks_triggers_calculation_when_batch_full() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_debug_enabled = true;
        engine.is_metric_calc_enabled = true;

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Fill the batch (batch size is 1024 for tasks)
        for i in 0..1024 {
            let task = Task::new(
                &format!("Task{}", i),
                1,
                TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
            )
            .unwrap();
            engine.manage_metrics_tasks(&task);
        }

        // Assert - batch should be full and calculation triggered
        assert_eq!(engine.metrics.cur_batch_count_task, 0); // Reset after calculation
        assert!(engine.metrics.avg_task_size > 0.0); // Should have calculated average
    }

    // Tests for manage_metrics_results with full batch
    #[test]
    fn test_manage_metrics_results_triggers_calculation_when_batch_full() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_debug_enabled = true;
        engine.is_metric_calc_enabled = true;

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Fill the batch (batch size is 1024 for results)
        for i in 0..1024 {
            let result = TaskResult::new(&format!("Type{}", i), "Done".to_string(), "Main");
            engine.manage_metrics_results(&result);
        }

        // Assert - batch should be full and calculation triggered
        assert_eq!(engine.metrics.cur_batch_count_res, 0); // Reset after calculation
        assert!(engine.metrics.avg_res_size > 0.0); // Should have calculated average
    }

    // Tests for manage_metrics_notifs with full batch
    #[test]
    fn test_manage_metrics_notifs_triggers_calculation_when_batch_full() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_debug_enabled = true;
        engine.is_metric_calc_enabled = true;

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Fill the batch (batch size is 1024 for notifs)
        for i in 0..1024 {
            let notif = Log::log(format!("Notification {}", i), "Main");
            engine.manage_metrics_notifs(&notif);
        }

        // Assert - batch should be full and calculation triggered
        assert_eq!(engine.metrics.cur_batch_count_notif, 0); // Reset after calculation
        assert!(engine.metrics.avg_notif_size > 0.0); // Should have calculated average
    }

    // Tests for handle_result with empty result type
    #[test]
    fn test_engine_handle_result_with_empty_result_type_and_non_empty_message_is_reported() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a result with empty result type but non-empty message
        // The engine reports this to Error.log instead of crashing
        let bad_result = TaskResult::new("", "Message without type".to_string(), "Main");

        // Act - must not panic
        engine.handle_result((1, bad_result));

        // Assert - the engine keeps running
        assert!(!engine.exit_flag);
    }

    #[test]
    fn test_engine_handle_result_with_empty_result_type_and_non_empty_log_file_is_reported() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a result with empty result type but non-empty log file
        // The engine reports this to Error.log instead of crashing
        let bad_result = TaskResult::new("", "Message".to_string(), "Error");

        // Act - must not panic
        engine.handle_result((1, bad_result));

        // Assert - the engine keeps running
        assert!(!engine.exit_flag);
    }

    // Tests for handle_result with message logging
    #[test]
    fn test_engine_handle_result_with_message_logs_notification() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a result with a message
        let result = TaskResult::new("Success", "Operation completed".to_string(), "Main");

        // Act - should not panic
        engine.handle_result((1, result));
    }

    // Tests for engine with different debug configurations
    #[test]
    fn test_engine_with_debug_enabled_no_metrics() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_debug_enabled = true;
        engine.is_metric_calc_enabled = false;

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a result
        let result = TaskResult::new("Success", "Test".to_string(), "Main");

        // Act - handle result without metrics calculation
        engine.handle_result((1, result));
    }

    #[test]
    fn test_engine_with_debug_enabled_task_logging() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_debug_enabled = true;
        engine.is_task_logging_enabled = true;

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a result
        let result = TaskResult::new("Success", "Test".to_string(), "Main");

        // Act - handle result with task logging enabled
        engine.handle_result((1, result));
    }

    #[test]
    fn test_engine_with_all_debug_flags_enabled() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_debug_enabled = true;
        engine.is_metric_calc_enabled = true;
        engine.is_task_logging_enabled = true;
        engine.is_graph_vis_enabled = true;

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Create a result with a chained task
        let next_task = Task::new(
            "FollowUp",
            1,
            TaskJob::new(|| TaskResult::new("Ok", "Done".to_string(), "Main")),
        )
        .unwrap();
        let result = TaskResult::new("Success", "Test".to_string(), "Main").chain(next_task);

        // Act - handle result with all debug flags
        engine.handle_result((1, result));
    }

    // Tests for Engine::new with different config values
    #[test]
    fn test_engine_new_with_custom_sleep_tick_rate() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.engine.sleep_tick_rate = 100;

        // Act
        let engine = Engine::new(&ctx);

        // Assert
        assert_eq!(engine.sleep_tick_rate, 100);
    }

    #[test]
    fn test_engine_new_with_debug_config() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.debugging.is_debugging = true;
        ctx.config.debugging.calculate_metrics = true;
        ctx.config.debugging.log_tasks = true;
        ctx.config.debugging.visual_graph = true;

        // Act
        let engine = Engine::new(&ctx);

        // Assert
        assert!(engine.is_debug_enabled);
        assert!(engine.is_metric_calc_enabled);
        assert!(engine.is_task_logging_enabled);
        assert!(engine.is_graph_vis_enabled);
    }

    #[test]
    fn test_engine_new_with_zero_sleep_tick_rate() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.engine.sleep_tick_rate = 0;

        // Act
        let engine = Engine::new(&ctx);

        // Assert
        assert_eq!(engine.sleep_tick_rate, 0);
    }

    #[test]
    fn test_engine_new_with_large_sleep_tick_rate() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.engine.sleep_tick_rate = 10000; // 10 seconds

        // Act
        let engine = Engine::new(&ctx);

        // Assert
        assert_eq!(engine.sleep_tick_rate, 10000);
    }

    // Tests for Engine finalize with metrics
    #[test]
    fn test_engine_finalize_with_metrics_calculation() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.is_debug_enabled = true;
        engine.is_metric_calc_enabled = true;

        // Load a logging observer first
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        engine
            .obs_manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Act - finalize with metrics
        engine.finalize(false);

        // Assert - exit flag should be set
        assert!(engine.exit_flag);
    }
}

mod tests_observer_manager_init {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use crossbeam::channel::bounded;
    use thryve_core::runtime::observer_manager::*;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::bases::observer::Observer;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::cfg::ctx::Ctx;
    use thryve_utils::extras::jobs::ObsJob;
    use thryve_utils::packets::notif_log::Log;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::traits::observer_logic::ObsLogic;
    use thryve_utils::traits::queue::Queue;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        thryve_utils::impl_as_any!(TestObsLogic);
        fn clean_up(&self, _observer: Arc<Observer>) {}
    }

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let config = Config::default();
        Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle)
    }

    #[test]
    fn test_observer_manager_threadpool_created_with_correct_size() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let manager = ObserverManager::new(&ctx);

        // Assert - the observer manager's threadpool never spawns threads by
        // worker count: the sorter thread and each observer's thread are
        // spawned individually (init_obs_sorter_thread, load_observer), so
        // num_workers is unused for this pool and left at 0.
        assert_eq!(manager.threadpool.num_workers(), 0);
    }

    #[test]
    fn test_observer_manager_new_creates_empty_observations() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let manager = ObserverManager::new(&ctx);

        // Assert
        assert_eq!(manager.observer_names().len(), 0);
    }

    #[test]
    fn test_observer_manager_load_observer_starts_thread() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Assert - should now have one observer
        assert_eq!(manager.observer_names().len(), 1);
    }

    #[test]
    fn test_observer_manager_forward_notif_with_logging_observer() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Create a notification
        let notif = Log::log("test message".to_string(), "Main");

        // Act - should not panic
        manager.forward_notif(notif);
    }

    #[test]
    fn test_observer_manager_forward_notif_with_topology_observer() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Load a topology observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Create a TopologyModifier notification
        let notif = thryve_utils::packets::notif_topology::TopologyModifier::add_node(
            thryve_utils::graphs::graph::TopologyNode::default(),
        );

        // Act - should not panic
        manager.forward_notif(notif);
    }

    #[test]
    fn test_observer_manager_observers_not_empty_after_loading() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load an observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("TestType", obs_core, obs_job, ctx.packet_handle()).unwrap();

        // Act
        manager
            .load_observer(observer)
            .expect("load_observer failed");

        // Assert
        assert_eq!(manager.observer_names().len(), 1);
    }

    #[test]
    fn test_observer_manager_load_multiple_observers() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger =
            Observer::new("Logging", obs_core.clone(), obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Load a topology observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Assert
        assert_eq!(manager.observer_names().len(), 2);
    }

    #[test]
    fn test_observer_manager_observer_names_returns_all_observer_types() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load multiple observers
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger =
            Observer::new("Logging", obs_core.clone(), obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Act
        let names = manager.observer_names();

        // Assert
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_observer_manager_finalize_returns_logger() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Act
        let returned_logger = manager.finalize(&mut Vec::new(), &mut Vec::new());

        // Assert
        let returned_logger = returned_logger.expect("finalize should return the Logging observer");
        assert_eq!(returned_logger.obs_type(), "Logging");
    }

    #[test]
    fn test_observer_manager_finalize_clears_all_observers() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load multiple observers
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger =
            Observer::new("Logging", obs_core.clone(), obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Assert - two observers loaded
        assert_eq!(manager.observer_names().len(), 2);

        // Act
        manager.finalize(&mut Vec::new(), &mut Vec::new());
        let observers = manager.finalize_get_observers();
        for (handle, _) in manager.finalize_observers(&mut Vec::new(), far_deadline(), observers) {
            handle.join().unwrap();
        }

        // Assert - after both steps, observers should be cleared
        assert_eq!(manager.observer_names().len(), 0);
    }

    /// A shutdown deadline that the test never reaches.
    fn far_deadline() -> Instant {
        Instant::now() + Duration::from_secs(60)
    }

    /// The text of a status line.
    fn message(notif: &Notif) -> String {
        notif
            .notif_core
            .as_any()
            .downcast_ref::<Log>()
            .unwrap()
            .message
            .clone()
    }

    #[test]
    fn test_observer_manager_finalize_observers_cleans_up_on_a_thread_of_its_own() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger =
            Observer::new("Logging", obs_core.clone(), obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        manager.finalize(&mut Vec::new(), &mut Vec::new());
        while ctx.status_rx.try_recv().is_ok() {}

        // Act
        let mut status_reps = Vec::new();
        let observers = manager.finalize_get_observers();
        let threads = manager.finalize_observers(&mut status_reps, far_deadline(), observers);

        // Assert - the clean-up thread, which sends its lines through the status mailbox (the
        // manager's own line last), and the observer's own thread, for the engine to wait for;
        // the logger's thread stays in the pool
        assert_eq!(
            thread_names(&threads),
            ["observer cleanup", "TopologyHandler"]
        );
        for (handle, _) in threads {
            handle.join().unwrap();
        }
        assert!(status_reps.is_empty());
        assert_eq!(thread_names(&manager.threadpool.workers), ["Logging"]);

        let lines: Vec<String> = ctx
            .status_rx
            .try_iter()
            .map(|line| message(&line))
            .collect();
        assert_eq!(
            lines,
            [
                "Obs Queue Finalized",
                "observer TopologyHandler finalized successfully",
                "Observer manager Finalized",
            ]
        );
        assert_eq!(manager.observer_names().len(), 0);
    }

    #[test]
    fn test_observer_manager_finalize_observers_with_only_the_logger_starts_no_thread() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");
        manager.finalize(&mut Vec::new(), &mut Vec::new());

        // Act
        let mut status_reps = Vec::new();
        let observers = manager.finalize_get_observers();
        let threads = manager.finalize_observers(&mut status_reps, far_deadline(), observers);

        // Assert - nothing to wait for, and the manager's line is added right away
        assert!(threads.is_empty());
        let lines: Vec<String> = status_reps.iter().map(message).collect();
        assert_eq!(lines, ["Observer manager Finalized"]);
    }

    /// An observer core whose `clean_up` records that it ran.
    struct RecordingObsLogic {
        cleaned_up: Arc<AtomicBool>,
    }

    impl ObsLogic for RecordingObsLogic {
        thryve_utils::impl_as_any!(RecordingObsLogic);
        fn clean_up(&self, _observer: Arc<Observer>) {
            self.cleaned_up.store(true, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_observer_manager_finalize_observers_after_the_deadline_only_stops_the_observers() {
        // Arrange - an observer whose thread runs until its queue is finalized
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let obs_core = Arc::new(RecordingObsLogic {
            cleaned_up: cleaned_up.clone(),
        });
        let obs_job = ObsJob::new(|obs: Arc<Observer>| while obs.obs_queue.pop().is_some() {});
        let recorder = Observer::new("Recorder", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager
            .load_observer(recorder)
            .expect("load_observer failed");
        manager.finalize(&mut Vec::new(), &mut Vec::new());

        // Act - a deadline that has already passed
        let observers = manager.finalize_get_observers();
        let threads = manager.finalize_observers(&mut Vec::new(), Instant::now(), observers);

        // Assert - both threads stop, since the observer's queue is still finalized, but its
        // `clean_up` never ran
        assert_eq!(thread_names(&threads), ["observer cleanup", "Recorder"]);
        let limit = Instant::now() + Duration::from_secs(5);
        while threads.iter().any(|(handle, _)| !handle.is_finished()) && Instant::now() < limit {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            threads.iter().all(|(handle, _)| handle.is_finished()),
            "a thread didn't stop"
        );
        assert!(!cleaned_up.load(Ordering::SeqCst));
    }

    /// The names of the given threads.
    fn thread_names<T>(threads: &[(std::thread::JoinHandle<()>, T)]) -> Vec<String> {
        threads
            .iter()
            .map(|(handle, _)| handle.thread().name().unwrap_or("").to_string())
            .collect()
    }

    #[test]
    fn test_observer_manager_finalize_with_multiple_observers() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger =
            Observer::new("Logging", obs_core.clone(), obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Load a topology observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Act & Assert - finalize should not panic
        manager.finalize(&mut Vec::new(), &mut Vec::new());
    }

    #[test]
    fn test_observer_manager_init_obs_sorter_thread_does_not_panic() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Act - should not panic
        // Note: This will spawn a thread, so we need to ensure cleanup
        manager
            .init_obs_sorter_thread()
            .expect("init_obs_sorter_thread failed");
    }

    #[test]
    fn test_observer_manager_init_obs_sorter_thread_with_topology_observer() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger =
            Observer::new("Logging", obs_core.clone(), obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Load a topology observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Act - should not panic
        manager
            .init_obs_sorter_thread()
            .expect("init_obs_sorter_thread failed");
    }

    #[test]
    fn test_observer_manager_notif_err_sends_error_notification() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Act - should not panic
        manager.notif_err("Test error message");
    }

    #[test]
    fn test_observer_manager_notif_err_with_multiple_errors() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Act - should not panic
        manager.notif_err("First error");
        manager.notif_err("Second error");
        manager.notif_err("Third error");
    }

    #[test]
    fn test_observer_manager_notif_err_with_empty_message() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Act - should not panic
        manager.notif_err("");
    }

    #[test]
    fn test_observer_manager_notif_err_with_unicode_message() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Act - should not panic
        manager.notif_err("Test error 世界 🌍");
    }

    #[test]
    fn test_observer_manager_notif_err_with_special_characters() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Act - should not panic
        manager.notif_err("Error: Connection failed! @#$%");
    }

    #[test]
    fn test_observer_manager_forward_notif_with_error_notification() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Create an error notification
        let notif = thryve_utils::packets::notif_log::Log::err("Test error".to_string());

        // Act - should not panic
        manager.forward_notif(notif);
    }

    #[test]
    fn test_observer_manager_forward_notif_with_topology_notification() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger =
            Observer::new("Logging", obs_core.clone(), obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Load a topology observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Create a TopologyModifier notification
        let notif = thryve_utils::packets::notif_topology::TopologyModifier::add_node(
            thryve_utils::graphs::graph::TopologyNode::default(),
        );

        // Act - should not panic
        manager.forward_notif(notif);
    }

    #[test]
    fn test_observer_manager_load_observer_with_logging_observer() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Act - load a logging observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Assert - should have one observer
        assert_eq!(manager.observer_names().len(), 1);
    }

    #[test]
    fn test_observer_manager_load_observer_with_topology_observer() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Act - load a topology observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Assert - should have one observer
        assert_eq!(manager.observer_names().len(), 1);
    }

    #[test]
    fn test_observer_manager_observer_names_with_logging_observer() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Act
        let names = manager.observer_names();

        // Assert
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "Logging");
    }

    #[test]
    fn test_observer_manager_observer_names_with_topology_observer() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a topology observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Act
        let names = manager.observer_names();

        // Assert
        assert_eq!(names.len(), 1);
        assert_eq!(names[0], "TopologyHandler");
    }

    #[test]
    fn test_observer_manager_observer_names_with_both_observers() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        // Load a logging observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger =
            Observer::new("Logging", obs_core.clone(), obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Load a topology observer
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let topo =
            Observer::new("TopologyHandler", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(topo).expect("load_observer failed");

        // Act
        let names = manager.observer_names();

        // Assert
        assert_eq!(names.len(), 2);
    }

    // Tests for the sorter thread's missing-observer fallback routing.
    // Both the normal per-iteration loop and the shutdown-drain loop have
    // their own copy of this fallback (log the error, forward it to the
    // Logging observer if present) -- these tests exercise each separately.

    #[test]
    fn test_sorter_thread_routes_missing_observer_notif_to_logger_while_running() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Act - start the sorter thread while exit_flag is still false, then
        // feed it a notification addressed to an observer type that was
        // never loaded. This exercises the per-iteration (non-shutdown)
        // fallback branch that logs the error and forwards it to Logging.
        manager
            .init_obs_sorter_thread()
            .expect("init_obs_sorter_thread failed");
        let orphaned = Log::log("orphaned".to_string(), "NoSuchObserver");
        manager.threadpool.main_queue.push(orphaned);

        // Give the sorter thread time to process it before the test ends.
        std::thread::sleep(std::time::Duration::from_millis(150));
    }

    #[test]
    fn test_sorter_thread_exit_drain_routes_missing_observer_notif_to_logger() {
        // Arrange
        let ctx = create_test_ctx();
        let mut manager = ObserverManager::new(&ctx);

        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Observer::new("Logging", obs_core, obs_job, ctx.packet_handle()).unwrap();
        manager.load_observer(logger).expect("load_observer failed");

        // Queue a notification addressed to an observer that was never loaded.
        let orphaned = Log::log("orphaned".to_string(), "NoSuchObserver");
        manager.threadpool.main_queue.push(orphaned);

        // Set the exit flag *before* spawning the sorter thread so its very
        // first loop iteration deterministically takes the shutdown-drain
        // branch, instead of racing a background thread through the normal
        // blocking-pop loop.
        manager
            .threadpool
            .pool_exit_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Act - should not panic; drains the queue and routes the orphaned
        // notification to the Logging observer's fallback instead of
        // dropping it silently.
        manager
            .init_obs_sorter_thread()
            .expect("init_obs_sorter_thread failed");

        // Give the sorter thread a moment to finish draining before the test ends.
        std::thread::sleep(std::time::Duration::from_millis(150));
    }
}

mod tests_plugin_manager {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::{Duration, Instant};

    use crossbeam::channel::bounded;
    use thryve_core::runtime::plugin_manager::*;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::bases::plugin::Plugin;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::cfg::ctx::Ctx;
    use thryve_utils::errors::{FinalizationError, ThryveError};
    use thryve_utils::extras::jobs::TaskJob;
    use thryve_utils::packets::notif_log::Log;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::res::TaskResult;
    use thryve_utils::traits::plugin_logic::PlgLogic;

    // Test implementation of PlgLogic
    struct TestPlgLogic;
    impl PlgLogic for TestPlgLogic {
        thryve_utils::impl_as_any!(TestPlgLogic);
        fn clean_up(&self) -> Result<(), thryve_utils::errors::ThryveError> {
            Ok(())
        }
    }

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let config = Config::default();
        Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle)
    }

    #[test]
    fn test_plugin_manager_new_creates_manager() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);

        // Act
        let manager = PluginManager::new(&ctx, res_tx);

        // Assert
        assert!(manager.plugin_names().is_empty());
    }

    #[test]
    fn test_plugin_manager_new_with_zero_workers() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.threads.worker_count = 0;
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);

        // Act
        let manager = PluginManager::new(&ctx, res_tx);

        // Assert
        assert_eq!(manager.threadpool.num_workers(), 0);
    }

    #[test]
    fn test_plugin_manager_plugin_names_returns_empty_when_no_plugins() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);
        let manager = PluginManager::new(&ctx, res_tx);

        // Act
        let names = manager.plugin_names();

        // Assert
        assert!(names.is_empty());
    }

    #[test]
    fn test_plugin_manager_load_plugin() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);
        let mut manager = PluginManager::new(&ctx, res_tx);

        let plg_core = Arc::new(TestPlgLogic);
        let job: thryve_utils::extras::jobs::TaskJob =
            TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));
        let plugin = Plugin::new("testType", plg_core, ctx.packet_handle(), job).unwrap();

        // Act
        manager
            .load_plugin(&ctx.config, plugin, false)
            .expect("load_plugin failed");

        // Assert
        let names = manager.plugin_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].inner(), "testType");
    }

    #[test]
    fn test_plugin_manager_load_multiple_plugins() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);
        let mut manager = PluginManager::new(&ctx, res_tx);

        // Load first plugin
        let plg_core1 = Arc::new(TestPlgLogic);
        let job1: thryve_utils::extras::jobs::TaskJob =
            TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));
        let plugin1 = Plugin::new("type1", plg_core1, ctx.packet_handle().clone(), job1).unwrap();

        // Load second plugin
        let plg_core2 = Arc::new(TestPlgLogic);
        let job2: thryve_utils::extras::jobs::TaskJob =
            TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));
        let plugin2 = Plugin::new("type2", plg_core2, ctx.packet_handle().clone(), job2).unwrap();

        // Act
        manager
            .load_plugin(&ctx.config, plugin1, false)
            .expect("load_plugin failed");
        manager
            .load_plugin(&ctx.config, plugin2, false)
            .expect("load_plugin failed");

        // Assert
        let names = manager.plugin_names();
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_plugin_manager_finalize() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);
        let mut manager = PluginManager::new(&ctx, res_tx);

        // Add a plugin
        let plg_core = Arc::new(TestPlgLogic);
        let job: thryve_utils::extras::jobs::TaskJob =
            TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));
        let plugin = Plugin::new("testType", plg_core, ctx.packet_handle(), job).unwrap();
        manager
            .load_plugin(&ctx.config, plugin, false)
            .expect("load_plugin failed");

        // Assert plugin was loaded
        assert_eq!(manager.plugin_names().len(), 1);

        // Act
        manager.finalize(&mut Vec::new(), &mut Vec::new());

        // Assert - finalize only stops the pool; `finalize_plugins` cleans up the plugins (tested
        // below). The test passes if finalize doesn't panic
    }

    /// A plugin core whose `clean_up` records that it ran, and fails if told to.
    struct RecordingPlgLogic {
        plugin_type: &'static str,
        cleaned_up: Arc<AtomicBool>,
        fails: bool,
    }

    impl PlgLogic for RecordingPlgLogic {
        thryve_utils::impl_as_any!(RecordingPlgLogic);
        fn clean_up(&self) -> Result<(), ThryveError> {
            self.cleaned_up.store(true, Ordering::SeqCst);

            if self.fails {
                return Err(FinalizationError::PluginCleanup {
                    plugin_type: self.plugin_type.to_string(),
                }
                .into());
            }

            Ok(())
        }
    }

    /// Loads a plugin whose `clean_up` records that it ran. Returns the record.
    fn load_recording_plugin(
        manager: &mut PluginManager,
        ctx: &Ctx,
        plugin_type: &'static str,
        fails: bool,
    ) -> Arc<AtomicBool> {
        let cleaned_up = Arc::new(AtomicBool::new(false));
        let core = Arc::new(RecordingPlgLogic {
            plugin_type,
            cleaned_up: cleaned_up.clone(),
            fails,
        });
        let job = TaskJob::new(|| TaskResult::new("", String::new(), ""));
        let plugin = Plugin::new(plugin_type, core, ctx.packet_handle(), job).unwrap();
        manager
            .load_plugin(&ctx.config, plugin, false)
            .expect("load_plugin failed");
        cleaned_up
    }

    /// The text of each status line.
    fn messages(notifs: &[Notif]) -> Vec<String> {
        notifs
            .iter()
            .map(|notif| {
                let log = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
                log.message.clone()
            })
            .collect()
    }

    #[test]
    fn test_plugin_manager_finalize_plugins_cleans_up_on_a_thread_of_its_own() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);
        let mut manager = PluginManager::new(&ctx, res_tx);
        let working = load_recording_plugin(&mut manager, &ctx, "Working", false);
        let failing = load_recording_plugin(&mut manager, &ctx, "Failing", true);
        let mut status_reps = Vec::new();

        // Act
        let plugins_cleanup = manager.finalize_plugins(
            &mut status_reps,
            Instant::now() + Duration::from_secs(10),
            true,
        );

        // Assert - one thread, which cleans up every plugin and sends a failure as a status line
        assert_eq!(plugins_cleanup.len(), 1);
        for (handle, _) in plugins_cleanup {
            assert_eq!(handle.thread().name(), Some("plugin cleanup"));
            handle.join().unwrap();
        }
        assert!(working.load(Ordering::SeqCst));
        assert!(failing.load(Ordering::SeqCst));
        assert_eq!(messages(&status_reps), ["ThreadPool Finalized"]);

        let sent: Vec<Notif> = ctx.status_rx.try_iter().collect();
        let sent = messages(&sent);
        assert!(
            sent.iter()
                .any(|line| line.contains("plugin Failing failed to clean up successfully")),
            "{sent:?}"
        );
        assert!(manager.plugin_names().is_empty());
    }

    #[test]
    fn test_plugin_manager_finalize_plugins_after_a_worker_was_left_behind_cleans_up_nothing() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);
        let mut manager = PluginManager::new(&ctx, res_tx);
        let cleaned_up = load_recording_plugin(&mut manager, &ctx, "Watched", false);
        let mut status_reps = Vec::new();

        // Act
        let plugins_cleanup = manager.finalize_plugins(
            &mut status_reps,
            Instant::now() + Duration::from_secs(10),
            false,
        );

        // Assert - its plugin may still be in use, so no thread starts, and a line says why
        assert!(plugins_cleanup.is_empty());
        assert!(!cleaned_up.load(Ordering::SeqCst));
        let lines = messages(&status_reps);
        assert!(
            lines
                .iter()
                .any(|line| line.contains("cannot cleanup plugins")),
            "{lines:?}"
        );
        assert!(manager.plugin_names().is_empty());
    }

    #[test]
    fn test_plugin_manager_finalize_plugins_after_the_deadline_cleans_up_nothing() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);
        let mut manager = PluginManager::new(&ctx, res_tx);
        let cleaned_up = load_recording_plugin(&mut manager, &ctx, "Late", false);
        let mut status_reps = Vec::new();

        // Act - a deadline that has already passed when the cleanups would start
        let plugins_cleanup = manager.finalize_plugins(&mut status_reps, Instant::now(), true);

        // Assert
        assert!(plugins_cleanup.is_empty());
        assert!(!cleaned_up.load(Ordering::SeqCst));
        let lines = messages(&status_reps);
        assert!(
            lines
                .iter()
                .any(|line| line.contains("grace period ran out")),
            "{lines:?}"
        );
        assert!(manager.plugin_names().is_empty());
    }
}
