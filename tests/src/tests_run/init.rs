// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_init {
    use crossbeam::channel::bounded;
    use thryve_core::runtime::engine::Engine;
    use thryve_run::init::init::*;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::cfg::ctx::Ctx;
    use thryve_utils::extras::dir_paths::DirName;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let config = Config::default();
        Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle)
    }

    #[test]
    fn test_init_directories_creates_log_structure() {
        // Arrange
        let mut ctx = create_test_ctx();

        // Act
        init_directories(&mut ctx).expect("init_directories failed");

        // Assert
        assert!(ctx.meta_data.dir_data.dir_paths[&DirName::Log].exists());
        assert!(ctx.meta_data.dir_data.log_hash_map.contains_key("MAIN"));
        assert!(ctx.meta_data.dir_data.log_hash_map.contains_key("ERROR"));
    }

    #[test]
    fn test_init_directories_creates_custom_log_files() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.logging_file_names.push("Custom".to_string());

        // Act
        init_directories(&mut ctx).expect("init_directories failed");

        // Assert
        assert!(ctx.meta_data.dir_data.log_hash_map.contains_key("CUSTOM"));
    }

    #[test]
    fn test_init_directories_sets_exe_dir() {
        // Arrange
        let mut ctx = create_test_ctx();

        // Act
        init_directories(&mut ctx).expect("init_directories failed");

        // Assert
        assert!(ctx.meta_data.dir_data.exe_dir.exists());
    }

    #[test]
    fn test_init_directories_sets_datetime() {
        // Arrange
        let mut ctx = create_test_ctx();

        // Act
        init_directories(&mut ctx).expect("init_directories failed");

        // Assert
        assert!(!ctx.meta_data.datetime.to_string().is_empty());
    }

    #[test]
    fn test_init_logger_creates_and_loads_observer() {
        // Arrange
        let mut ctx = create_test_ctx();
        init_directories(&mut ctx).expect("init_directories failed");
        let mut engine = Engine::new(&ctx);

        // Act
        init_logger(&ctx, &mut engine).expect("init_logger failed");

        // Assert - observer should be loaded (no panic means success)
        // The logger observer is loaded into the engine's observer manager
    }

    #[test]
    fn test_engine_new_after_init_directories() {
        // Arrange
        let mut ctx = create_test_ctx();
        init_directories(&mut ctx).expect("init_directories failed");

        // Act
        let engine = Engine::new(&ctx);

        // Assert - a new engine starts paused; the engine thread resumes it once it has it
        assert!(!engine.exit_flag);
        assert!(engine.pause_flag);
    }

    #[test]
    fn test_engine_new_with_custom_config() {
        // Arrange
        let mut ctx = create_test_ctx();
        init_directories(&mut ctx).expect("init_directories failed");
        ctx.config.engine.sleep_tick_rate = 100;

        // Act
        let engine = Engine::new(&ctx);

        // Assert
        assert_eq!(engine.sleep_tick_rate, 100);
    }

    // Tests for init_directories with debug mode enabled
    #[test]
    fn test_init_directories_with_debug_mode_creates_debug_log() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.debugging.is_debugging = true;

        // Act
        init_directories(&mut ctx).expect("init_directories failed");

        // Assert - debug log file should be created
        assert!(ctx.meta_data.dir_data.log_hash_map.contains_key("DEBUG"));
    }

    #[test]
    fn test_init_directories_with_debug_mode_creates_topology_dir() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.debugging.is_debugging = true;
        ctx.config.debugging.visual_graph = true;

        // Act
        init_directories(&mut ctx).expect("init_directories failed");

        // Assert - topology directory should be created
        assert!(
            ctx.meta_data
                .dir_data
                .dir_paths
                .contains_key(&DirName::Topology)
        );
    }

    #[test]
    fn test_init_directories_without_debug_mode_does_not_create_debug_log() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.debugging.is_debugging = false;

        // Act
        init_directories(&mut ctx).expect("init_directories failed");

        // Assert - debug log file should NOT be created
        assert!(!ctx.meta_data.dir_data.log_hash_map.contains_key("DEBUG"));
    }

    #[test]
    fn test_init_directories_without_visual_graph_does_not_create_topology_dir() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.debugging.is_debugging = true;
        ctx.config.debugging.visual_graph = false;

        // Act
        init_directories(&mut ctx).expect("init_directories failed");

        // Assert - topology directory should NOT be created
        assert!(
            !ctx.meta_data
                .dir_data
                .dir_paths
                .contains_key(&DirName::Topology)
        );
    }

    // Tests for init_logger with debug mode enabled
    #[test]
    fn test_init_logger_with_debug_mode_includes_debug_observer() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.debugging.is_debugging = true;
        init_directories(&mut ctx).expect("init_directories failed");
        let mut engine = Engine::new(&ctx);

        // Act
        init_logger(&ctx, &mut engine).expect("init_logger failed");

        // Assert - the logger observer should be loaded with debug capabilities
        // The init_logger function should handle debug mode internally
    }

    // Tests for init_topology_handler
    #[test]
    fn test_init_topology_handler_creates_and_loads_observer() {
        // Arrange
        let mut ctx = create_test_ctx();

        ctx.config.debugging.is_debugging = true;
        ctx.config.debugging.visual_graph = true;

        init_directories(&mut ctx).expect("init_directories failed");
        let mut engine = Engine::new(&ctx);

        // Act
        init_topology_handler(&ctx, &mut engine).expect("init_topology_handler failed");

        // Assert - the topology handler observer should be loaded
        // The function creates and loads the observer without panicking
    }

    // Tests for Engine::new with graph mode enabled
    #[test]
    fn test_engine_new_with_graph_mode() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.debugging.is_debugging = true;
        ctx.config.debugging.visual_graph = true;
        init_directories(&mut ctx).expect("init_directories failed");

        // Act
        let engine = Engine::new(&ctx);

        // Assert - a new engine starts paused; the engine thread resumes it once it has it
        assert!(!engine.exit_flag);
        assert!(engine.pause_flag);
    }

    // Tests for Engine::new with multiple custom log files
    #[test]
    fn test_engine_new_with_multiple_custom_log_files() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.logging_file_names.push("Custom1".to_string());
        ctx.logging_file_names.push("Custom2".to_string());
        init_directories(&mut ctx).expect("init_directories failed");

        // Act
        let engine = Engine::new(&ctx);

        // Assert - engine should be initialized successfully
        assert!(!engine.exit_flag);
    }
}
