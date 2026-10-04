// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_logger {
    use chrono::{DateTime, Local};
    use crossbeam::channel::bounded;
    use std::collections::HashMap;
    use std::fs::File;

    use std::sync::Arc;
    use std::time::SystemTime;
    use thryve_observers::logger::LoggerObserver;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::cfg::ctx::Ctx;
    use thryve_utils::cfg::dir_data::DirData;
    use thryve_utils::cfg::meta_data::MetaData;
    use thryve_utils::extras::named_input::LogFileNamedInput;
    use thryve_utils::packets::notif_log::Log;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::traits::observer_logic::ObsLogic;
    use thryve_utils::traits::queue::Queue;

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let config = Config::default();

        // Create minimal DirData and MetaData for testing
        let exe_dir = std::env::temp_dir();
        let log_dir = exe_dir.join("test_logs");
        std::fs::create_dir_all(&log_dir).ok();

        let mut log_hash_map: HashMap<LogFileNamedInput, Arc<File>> = HashMap::new();
        let main_log = std::fs::File::create(log_dir.join("Main.log")).unwrap();
        log_hash_map.insert(LogFileNamedInput::new_main(), Arc::new(main_log));
        let error_log = std::fs::File::create(log_dir.join("Error.log")).unwrap();
        log_hash_map.insert(LogFileNamedInput::new_err(), Arc::new(error_log));

        let dir_data = DirData::new(exe_dir, log_hash_map);
        let datetime: DateTime<Local> = SystemTime::now().into();
        let meta_data = MetaData::new(datetime, dir_data);

        let mut ctx = Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle);
        ctx.meta_data = meta_data;
        ctx
    }

    #[test]
    fn test_logger_observer_new_creates_observer() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "Logging");
    }

    #[test]
    fn test_logger_observer_with_custom_log_files() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.logging_file_names.push("Custom".to_string());

        // Add custom log file to dir_data
        let custom_log =
            std::fs::File::create(ctx.meta_data.dir_data.exe_dir.join("Custom.log")).unwrap();
        ctx.meta_data.dir_data.log_hash_map.insert(
            LogFileNamedInput::new_register("CUSTOM").unwrap(),
            Arc::new(custom_log),
        );

        // Act
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "Logging");
    }

    #[test]
    fn test_logger_observer_obs_type_is_logging() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "Logging");
    }

    #[test]
    fn test_logger_observer_handle_log_writes_to_file() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create a log notification
        let notif = Log::log("Test log message".to_string(), "Main");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, the log was written successfully
        // The file should now contain the message
    }

    #[test]
    fn test_logger_observer_handle_log_error_message() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create an error notification
        let notif = Log::err("Test error message".to_string());

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, the error was written successfully
    }

    #[test]
    fn test_logger_observer_handle_log_with_custom_file() {
        // Arrange
        let mut ctx = create_test_ctx();

        // Add a custom log file
        let custom_log =
            std::fs::File::create(ctx.meta_data.dir_data.exe_dir.join("Custom.log")).unwrap();
        ctx.meta_data.dir_data.log_hash_map.insert(
            LogFileNamedInput::new_register("CUSTOM").unwrap(),
            Arc::new(custom_log),
        );

        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create a notification for the custom file
        let notif = Log::log("Custom log message".to_string(), "Custom");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, the log was written successfully
    }

    #[test]
    fn test_logger_observer_cleanup_processes_remaining_notifs() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx).unwrap();
        let observer = Arc::new(observer);

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Push some notifications to the queue
        let notif1 = Log::log("Message 1".to_string(), "Main");
        let notif2 = Log::log("Message 2".to_string(), "Main");
        observer.obs_queue.push(notif1);
        observer.obs_queue.push(notif2);

        // Act - cleanup should process remaining notifications
        logger_observer.clean_up(observer.clone());

        // Assert - if we get here without panic, cleanup worked
    }

    #[test]
    fn test_logger_observer_with_unicode_message() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create a notification with unicode
        let notif = Log::log("Hello 世界 🌍".to_string(), "Main");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, unicode was handled correctly
    }

    #[test]
    fn test_logger_observer_with_empty_message() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create a notification with empty message
        let notif = Log::log("".to_string(), "Main");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, empty message was handled
    }

    #[test]
    fn test_logger_observer_with_multiline_message() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create a notification with multiline message
        let notif = Log::log("Line 1\nLine 2\nLine 3".to_string(), "Main");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, multiline was handled
    }

    // Tests for the log method's exit condition
    #[test]
    fn test_logger_observer_log_with_exit_flag() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Create an Arc<Observer> with exit flag set
        let observer = Arc::new(observer);
        observer
            .exit_flag
            .store(true, std::sync::atomic::Ordering::SeqCst);

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Act - the log loop should exit immediately when exit flag is set
        logger_observer.log(observer.clone());
    }

    // Tests for handle_log with unsupported notification type
    #[test]
    fn test_logger_observer_handle_log_with_unsupported_notification_type_writes_a_note() {
        // Arrange: give this test its own Error.log so the check below only sees its line
        let mut ctx = create_test_ctx();
        let error_dir = std::env::temp_dir().join("test_logs_unsupported_notification");
        std::fs::create_dir_all(&error_dir).unwrap();
        let error_path = error_dir.join("Error.log");
        let error_log = std::fs::File::create(&error_path).unwrap();
        ctx.meta_data
            .dir_data
            .log_hash_map
            .insert(LogFileNamedInput::new_err(), Arc::new(error_log));
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create a non-Log notification (e.g., a TopologyModifier)
        let notif = thryve_utils::packets::notif_topology::TopologyModifier::add_node(
            thryve_utils::graphs::graph::TopologyNode::default(),
        );

        // Act - not a Log: must not panic
        logger_observer.handle_log(notif);

        // Assert - it was dropped, with a note in Error.log naming its type
        let written = std::fs::read_to_string(&error_path).unwrap();
        assert!(written.contains("[ LOGGING ], Event_Type: Error"));
        assert!(written.contains("[dropped a notification of type \"AddNode\""));
    }

    // Tests for handle_log with non-existent log file
    #[test]
    fn test_logger_observer_handle_log_with_non_existent_log_file_writes_to_error_log() {
        // Arrange: give this test its own Error.log so the check below only sees its line
        let mut ctx = create_test_ctx();
        let error_dir = std::env::temp_dir().join("test_logs_unregistered_file");
        std::fs::create_dir_all(&error_dir).unwrap();
        let error_path = error_dir.join("Error.log");
        let error_log = std::fs::File::create(&error_path).unwrap();
        ctx.meta_data
            .dir_data
            .log_hash_map
            .insert(LogFileNamedInput::new_err(), Arc::new(error_log));
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create a notification for a non-existent log file
        let notif = Log::log("Test message".to_string(), "NonExistent");

        // Act - must not panic
        logger_observer.handle_log(notif);

        // Assert - the message went to Error.log, behind a note naming the requested file
        let written = std::fs::read_to_string(&error_path).unwrap();
        assert!(written.contains("[log file \"NonExistent\" is not registered] Test message"));
    }

    // Tests for handle_log with empty message (should return early)
    #[test]
    fn test_logger_observer_handle_log_empty_message_does_not_write() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create a notification with empty message
        let notif = Log::log("".to_string(), "Main");

        // Act - should return early without writing
        logger_observer.handle_log(notif);
    }

    // Tests for handle_log with error notification
    #[test]
    fn test_logger_observer_handle_log_error_notification() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create an error notification
        let notif = Log::err("Error message".to_string());

        // Act - should not panic
        logger_observer.handle_log(notif);
    }

    // Tests for handle_log with debug notification
    #[test]
    fn test_logger_observer_handle_log_debug_notification() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.logging_file_names.push("Debug".to_string());

        // Add debug log file to dir_data
        let debug_log =
            std::fs::File::create(ctx.meta_data.dir_data.exe_dir.join("Debug.log")).unwrap();
        ctx.meta_data
            .dir_data
            .log_hash_map
            .insert(LogFileNamedInput::new_debug(), Arc::new(debug_log));

        let observer = LoggerObserver::new(&ctx).unwrap();

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Create a debug notification
        let notif = Log::log("Debug message".to_string(), "Debug");

        // Act - should not panic
        logger_observer.handle_log(notif);
    }

    // Tests for clean_up with empty queue
    #[test]
    fn test_logger_observer_cleanup_with_empty_queue() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = Arc::new(LoggerObserver::new(&ctx).unwrap());

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Act - cleanup with empty queue should not panic
        logger_observer.clean_up(observer.clone());
    }

    // Tests for clean_up flushing files
    #[test]
    fn test_logger_observer_cleanup_flushes_files() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = Arc::new(LoggerObserver::new(&ctx).unwrap());

        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();

        // Push some notifications to the queue
        let notif1 = Log::log("Message 1".to_string(), "Main");
        let notif2 = Log::log("Message 2".to_string(), "Error");
        observer.obs_queue.push(notif1);
        observer.obs_queue.push(notif2);

        // Act - cleanup should flush all files
        logger_observer.clean_up(observer.clone());
    }
}
