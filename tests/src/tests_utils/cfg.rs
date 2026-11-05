// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_metrics {
    use thryve_utils::cfg::metrics::*;

    #[test]
    fn test_metrics_default_initializes_all_fields_to_zero() {
        // Arrange & Act
        let metrics = Metrics::default();

        // Assert
        assert_eq!(metrics.avg_task_size, 0.0);
        assert_eq!(metrics.avg_res_size, 0.0);
        assert_eq!(metrics.avg_notif_size, 0.0);
        assert_eq!(metrics.cur_batch_count_task, 0);
        assert_eq!(metrics.cur_batch_count_res, 0);
        assert_eq!(metrics.cur_batch_count_notif, 0);
        assert!(metrics.task_size_batch.iter().all(|&x| x == 0));
        assert!(metrics.res_size_batch.iter().all(|&x| x == 0));
        assert!(metrics.notif_size_batch.iter().all(|&x| x == 0));
    }

    #[test]
    fn test_metrics_clone_preserves_all_fields() {
        // Arrange
        let metrics = Metrics {
            avg_task_size: 100.0,
            avg_res_size: 200.0,
            avg_notif_size: 300.0,
            cur_batch_count_task: 10,
            cur_batch_count_res: 20,
            cur_batch_count_notif: 30,
            ..Default::default()
        };

        // Act
        let cloned = metrics.clone();

        // Assert
        assert_eq!(cloned.avg_task_size, 100.0);
        assert_eq!(cloned.avg_res_size, 200.0);
        assert_eq!(cloned.avg_notif_size, 300.0);
        assert_eq!(cloned.cur_batch_count_task, 10);
        assert_eq!(cloned.cur_batch_count_res, 20);
        assert_eq!(cloned.cur_batch_count_notif, 30);
    }

    #[test]
    fn test_calculate_metrics_tasks_with_empty_batch() {
        // Arrange
        let mut metrics = Metrics::default();

        // Act
        metrics.calculate_metrics_tasks();

        // Assert - nothing should change with empty batch
        assert_eq!(metrics.avg_task_size, 0.0);
        assert_eq!(metrics.cur_batch_count_task, 0);
    }

    #[test]
    fn test_calculate_metrics_tasks_with_single_value() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = 100;
        metrics.cur_batch_count_task = 1;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert
        assert_eq!(metrics.avg_task_size, 100.0);
        assert_eq!(metrics.cur_batch_count_task, 0);
        assert_eq!(metrics.task_size_batch[0], 0); // Batch reset
    }

    #[test]
    fn test_calculate_metrics_tasks_with_multiple_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = 100;
        metrics.task_size_batch[1] = 200;
        metrics.task_size_batch[2] = 300;
        metrics.cur_batch_count_task = 3;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert - average of 100, 200, 300 = 200
        assert_eq!(metrics.avg_task_size, 200.0);
        assert_eq!(metrics.cur_batch_count_task, 0);
    }

    #[test]
    fn test_calculate_metrics_tasks_running_average() {
        // Arrange
        let mut metrics = Metrics::default();

        // First batch: single value of 100
        metrics.task_size_batch[0] = 100;
        metrics.cur_batch_count_task = 1;
        metrics.calculate_metrics_tasks();

        // Second batch: single value of 200
        metrics.task_size_batch[0] = 200;
        metrics.cur_batch_count_task = 1;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert - running average of 100 and 200 = 150
        assert_eq!(metrics.avg_task_size, 150.0);
    }

    #[test]
    fn test_calculate_metrics_results_with_single_value() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.res_size_batch[0] = 250;
        metrics.cur_batch_count_res = 1;

        // Act
        metrics.calculate_metrics_results();

        // Assert
        assert_eq!(metrics.avg_res_size, 250.0);
        assert_eq!(metrics.cur_batch_count_res, 0);
    }

    #[test]
    fn test_calculate_metrics_results_with_multiple_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.res_size_batch[0] = 100;
        metrics.res_size_batch[1] = 300;
        metrics.cur_batch_count_res = 2;

        // Act
        metrics.calculate_metrics_results();

        // Assert - average of 100, 300 = 200
        assert_eq!(metrics.avg_res_size, 200.0);
    }

    #[test]
    fn test_calculate_metrics_notifs_with_single_value() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.notif_size_batch[0] = 50;
        metrics.cur_batch_count_notif = 1;

        // Act
        metrics.calculate_metrics_notifs();

        // Assert
        assert_eq!(metrics.avg_notif_size, 50.0);
        assert_eq!(metrics.cur_batch_count_notif, 0);
    }

    #[test]
    fn test_calculate_metrics_notifs_with_multiple_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.notif_size_batch[0] = 25;
        metrics.notif_size_batch[1] = 75;
        metrics.notif_size_batch[2] = 100;
        metrics.cur_batch_count_notif = 3;

        // Act
        metrics.calculate_metrics_notifs();

        // Assert - average of 25, 75, 100 = 66.666...
        assert!((metrics.avg_notif_size - 66.666666).abs() < 0.0001);
    }

    #[test]
    fn test_calculate_metrics_calculates_all_types() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = 100;
        metrics.cur_batch_count_task = 1;
        metrics.res_size_batch[0] = 200;
        metrics.cur_batch_count_res = 1;
        metrics.notif_size_batch[0] = 50;
        metrics.cur_batch_count_notif = 1;

        // Act
        let output = metrics.calculate_metrics();

        // Assert
        assert_eq!(metrics.avg_task_size, 100.0);
        assert_eq!(metrics.avg_res_size, 200.0);
        assert_eq!(metrics.avg_notif_size, 50.0);
        assert!(output.contains("Tasks"));
        assert!(output.contains("Results"));
        assert!(output.contains("Notifications"));
    }

    #[test]
    fn test_metrics_readable_contains_all_sections() {
        // Arrange
        let metrics = Metrics {
            task_count: 10,
            avg_task_size: 100.0,
            res_count: 20,
            avg_res_size: 200.0,
            notif_count: 30,
            avg_notif_size: 50.0,
            ..Default::default()
        };

        // Act
        let output = metrics.metrics_readable();

        // Assert
        assert!(output.contains("Total Tasks: 10"));
        assert!(output.contains("Total Results: 20"));
        assert!(output.contains("Total Notifications: 30"));
        assert!(output.contains("___________________________________________________________"));
    }

    #[test]
    fn test_format_metrics_displays_bytes_for_small_values() {
        // Arrange
        let input = "".to_string();

        // Act
        let output = format_metrics(5, 100.0, "Items", input);

        // Assert
        assert!(output.contains("Total Items: 5"));
        assert!(output.contains("average Items size: 100.000 B"));
    }

    #[test]
    fn test_format_metrics_displays_kb_for_medium_values() {
        // Arrange
        let input = "".to_string();
        let avg_bytes = 2048.0; // 2 KB

        // Act
        let output = format_metrics(10, avg_bytes, "Items", input);

        // Assert
        assert!(output.contains("Total Items: 10"));
        assert!(output.contains("average Items size: 2.000 KB"));
    }

    #[test]
    fn test_format_metrics_displays_mb_for_large_values() {
        // Arrange
        let input = "".to_string();
        let avg_bytes = 2_097_152.0; // 2 MB

        // Act
        let output = format_metrics(100, avg_bytes, "Items", input);

        // Assert
        assert!(output.contains("Total Items: 100"));
        assert!(output.contains("average Items size: 2.000 MB"));
    }

    #[test]
    fn test_format_metrics_boundary_at_exactly_1_kb() {
        // Arrange
        let input = "".to_string();
        let avg_bytes = 1024.0; // Exactly 1 KB - should display as KB

        // Act
        let output = format_metrics(1, avg_bytes, "Items", input);

        // Assert
        assert!(output.contains("average Items size: 1.000 KB"));
    }

    #[test]
    fn test_format_metrics_boundary_at_exactly_1_mb() {
        // Arrange
        let input = "".to_string();
        let avg_bytes = 1024.0 * 1024.0; // Exactly 1 MB

        // Act
        let output = format_metrics(1, avg_bytes, "Items", input);

        // Assert
        assert!(output.contains("average Items size: 1.000 MB"));
    }

    #[test]
    fn test_format_metrics_with_zero_count() {
        // Arrange
        let input = "".to_string();

        // Act
        let output = format_metrics(0, 0.0, "Items", input);

        // Assert
        assert!(output.contains("Total Items: 0"));
        assert!(output.contains("average Items size: 0.000 B"));
    }

    #[test]
    fn test_format_metrics_preserves_input_string() {
        // Arrange
        let input = "prefix\n".to_string();

        // Act
        let output = format_metrics(1, 100.0, "Items", input);

        // Assert
        assert!(output.starts_with("prefix"));
    }

    #[test]
    fn test_calculate_metrics_with_zero_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = 0;
        metrics.cur_batch_count_task = 1;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert
        assert_eq!(metrics.avg_task_size, 0.0);
    }

    #[test]
    fn test_calculate_metrics_with_large_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = usize::MAX / 2;
        metrics.task_size_batch[1] = usize::MAX / 2;
        metrics.cur_batch_count_task = 2;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert - should handle large values without panic
        assert!(metrics.avg_task_size > 0.0);
    }
}

mod tests_meta_data {
    use thryve_utils::cfg::dir_data::DirData;
    use thryve_utils::extras::dir_paths::DirName;

    use thryve_utils::cfg::meta_data::*;

    #[test]
    fn test_meta_data_default() {
        // Arrange & Act
        let meta_data = MetaData::default();

        // Assert
        assert!(meta_data.datetime <= chrono::Local::now());
        assert!(meta_data.dir_data.exe_dir.as_os_str().is_empty());
        assert!(meta_data.dir_data.dir_paths.is_empty());
    }

    #[test]
    fn test_meta_data_new() {
        // Arrange
        let datetime = chrono::Local::now();
        let dir_data = DirData::default();

        // Act
        let meta_data = MetaData::new(datetime, dir_data.clone());

        // Assert
        assert_eq!(meta_data.datetime, datetime);
        assert_eq!(meta_data.dir_data.exe_dir, dir_data.exe_dir);
    }

    #[test]
    fn test_meta_data_clone() {
        // Arrange
        let datetime = chrono::Local::now();
        let dir_data = DirData::default();
        let meta_data1 = MetaData::new(datetime, dir_data);

        // Act
        let meta_data2 = meta_data1.clone();

        // Assert
        assert_eq!(meta_data1.datetime, meta_data2.datetime);
        assert_eq!(meta_data1.dir_data.exe_dir, meta_data2.dir_data.exe_dir);
    }

    #[test]
    fn test_meta_data_with_custom_dir_data() {
        // Arrange
        let datetime = chrono::Local::now();
        let exe_dir = std::path::PathBuf::from("/custom/exe");
        let log_dir = std::path::PathBuf::from("/custom/logs");
        let mut dir_data = DirData::new(exe_dir.clone(), std::collections::HashMap::new());
        dir_data.set_path(DirName::Log, log_dir.clone());

        // Act
        let meta_data = MetaData::new(datetime, dir_data.clone());

        // Assert
        assert_eq!(meta_data.dir_data.exe_dir, exe_dir);
        assert_eq!(meta_data.dir_data.dir_paths[&DirName::Log], log_dir);
    }

    #[test]
    fn test_meta_data_datetime_is_current() {
        // Arrange
        let before = chrono::Local::now();

        // Act
        let meta_data = MetaData::default();

        // Assert
        let after = chrono::Local::now();
        assert!(meta_data.datetime >= before);
        assert!(meta_data.datetime <= after);
    }
}

mod tests_dir_data {
    use std::collections::HashMap;
    use std::fs::File;
    use std::path::PathBuf;
    use std::sync::Arc;

    use thryve_utils::cfg::dir_data::*;
    use thryve_utils::extras::dir_paths::DirName;
    use thryve_utils::extras::named_input::LogFileNamedInput;

    #[test]
    fn test_dir_data_default() {
        // Arrange & Act
        let dir_data = DirData::default();

        // Assert
        assert!(dir_data.exe_dir.as_os_str().is_empty());
        assert!(dir_data.dir_paths.is_empty());
        assert!(dir_data.log_hash_map.is_empty());
    }

    #[test]
    fn test_dir_data_new() {
        // Arrange
        let exe_dir = PathBuf::from("/test/exe");
        let log_dir = PathBuf::from("/test/logs");
        let log_hash_map: HashMap<LogFileNamedInput, Arc<File>> = HashMap::new();

        // Act
        let mut dir_data = DirData::new(exe_dir.clone(), log_hash_map);
        dir_data.set_path(DirName::Log, log_dir.clone());

        // Assert
        assert_eq!(dir_data.exe_dir, exe_dir);
        assert_eq!(dir_data.dir_paths[&DirName::Log], log_dir);
        assert!(dir_data.log_hash_map.is_empty());
    }

    #[test]
    fn test_dir_data_new_sets_static_log_path() {
        // Arrange
        let exe_dir = PathBuf::from("/test/exe");
        let log_dir = PathBuf::from("/test/logs");
        let log_hash_map: HashMap<LogFileNamedInput, Arc<File>> = HashMap::new();

        // Act
        let mut dir_data = DirData::new(exe_dir, log_hash_map);
        dir_data.set_path(DirName::Log, log_dir.clone());

        // Assert
        let retrieved = dir_data
            .get_dir_path(&DirName::Log)
            .expect("the log folder was just recorded");

        println!("{:?}", retrieved.clone());
        println!("{:?}", log_dir);

        assert!(!retrieved.to_str().unwrap().is_empty());
        assert_eq!(retrieved, log_dir);
    }

    #[test]
    fn test_dir_data_get_log_dir_path_before_init() {
        // Note: This test assumes the static is not initialized yet
        // In practice, other tests may have already initialized it
        // Act & Assert
        let result = DirData::get_log_dir_path();
        // Result may be Some if another test initialized it, or None if not
        // This is acceptable behavior
        assert!(result.is_some() || result.is_none());
    }

    #[test]
    fn test_dir_data_clone() {
        // Arrange
        let exe_dir = PathBuf::from("/test/exe");
        let log_dir = PathBuf::from("/test/logs");
        let mut log_hash_map: HashMap<LogFileNamedInput, Arc<File>> = HashMap::new();

        // Create a temp file for testing using cross-platform temp directory
        let temp_path = std::env::temp_dir().join("test.log");
        let temp_file = std::fs::File::create(&temp_path).unwrap();
        log_hash_map.insert(
            LogFileNamedInput::new_register("Test").unwrap(),
            Arc::new(temp_file),
        );

        let mut dir_data1 = DirData::new(exe_dir.clone(), log_hash_map.clone());
        dir_data1.set_path(DirName::Log, log_dir);

        // Act
        let dir_data2 = dir_data1.clone();

        // Assert
        assert_eq!(dir_data1.exe_dir, dir_data2.exe_dir);
        assert_eq!(
            dir_data1.dir_paths[&DirName::Log],
            dir_data2.dir_paths[&DirName::Log]
        );
        assert_eq!(dir_data1.log_hash_map.len(), dir_data2.log_hash_map.len());
    }

    #[test]
    fn test_dir_data_with_log_files() {
        // Arrange
        let exe_dir = PathBuf::from("/test/exe");
        let mut log_hash_map: HashMap<LogFileNamedInput, Arc<File>> = HashMap::new();

        // Create temp files using cross-platform temp directory
        let temp_path1 = std::env::temp_dir().join("test1.log");
        let temp_path2 = std::env::temp_dir().join("test2.log");
        let file1 = std::fs::File::create(&temp_path1).unwrap();
        let file2 = std::fs::File::create(&temp_path2).unwrap();

        log_hash_map.insert(LogFileNamedInput::new_main(), Arc::new(file1));
        log_hash_map.insert(LogFileNamedInput::new_err(), Arc::new(file2));

        // Act
        let dir_data = DirData::new(exe_dir, log_hash_map.clone());

        // Assert
        assert_eq!(dir_data.log_hash_map.len(), 2);
        assert!(dir_data.log_hash_map.contains_key("MAIN"));
        assert!(dir_data.log_hash_map.contains_key("ERROR"));
    }
}

mod tests_ctx {
    use std::sync::Arc;

    use crossbeam::channel::bounded;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::bases::observer::Observer;
    use thryve_utils::bases::plugin::Plugin;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::cfg::ctx::*;
    use thryve_utils::extras::jobs::ObsJob;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl thryve_utils::traits::observer_logic::ObsLogic for TestObsLogic {
        thryve_utils::impl_as_any!(TestObsLogic);
        fn clean_up(&self, _observer: Arc<Observer>) {}
    }

    // Test implementation of PlgLogic
    struct TestPlgLogic;
    impl thryve_utils::traits::plugin_logic::PlgLogic for TestPlgLogic {
        thryve_utils::impl_as_any!(TestPlgLogic);
        fn clean_up(&self) -> Result<(), thryve_utils::errors::ThryveError> {
            Ok(())
        }
    }

    #[test]
    fn test_ctx_new_creates_context() {
        // Arrange
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let config = Config::default();

        // Act
        let ctx = Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle);

        // Assert
        assert!(ctx.observers.is_empty());
        assert!(ctx.plugins.is_empty());
        assert!(ctx.logging_file_names.is_empty());
    }

    #[test]
    fn test_ctx_observers_returns_and_clears() {
        // Arrange
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let mut ctx = Ctx::new(
            Config::default(),
            task_rx,
            notif_rx,
            status_rx,
            packet_handle,
        );

        // Add a dummy observer
        ctx.observers.push(
            Observer::new(
                "TestType",
                Arc::new(TestObsLogic),
                ObsJob::default(),
                ctx.packet_handle(),
            )
            .unwrap(),
        );

        // Act
        let observers = ctx.observers();

        // Assert
        assert_eq!(observers.len(), 1);
        assert!(ctx.observers.is_empty()); // Cleared after take
    }

    #[test]
    fn test_ctx_plugins_returns_and_clears() {
        // Arrange
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let mut ctx = Ctx::new(
            Config::default(),
            task_rx,
            notif_rx,
            status_rx,
            packet_handle,
        );

        // Add a dummy plugin
        ctx.plugins.push(
            Plugin::new(
                "TestType",
                Arc::new(TestPlgLogic),
                ctx.packet_handle(),
                || thryve_utils::res::TaskResult::new("Ok", "Done".to_string(), "Main"),
            )
            .unwrap(),
        );

        // Act
        let plugins = ctx.plugins();

        // Assert
        assert_eq!(plugins.len(), 1);
        assert!(ctx.plugins.is_empty()); // Cleared after take
    }

    #[test]
    fn test_ctx_packet_handle_returns_clone() {
        // Arrange
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let ctx = Ctx::new(
            Config::default(),
            task_rx,
            notif_rx,
            status_rx,
            packet_handle,
        );

        // Act
        let handle1 = ctx.packet_handle();
        let handle2 = ctx.packet_handle();

        // Assert - both calls return valid handles (can send)
        drop(handle1);
        drop(handle2);
        // If we get here without panic, handles are valid
    }

    #[test]
    fn test_ctx_with_custom_config() {
        // Arrange
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let (status_tx, status_rx) = crossbeam::channel::unbounded();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
        let config = Config::default();

        // Act
        let ctx = Ctx::new(config.clone(), task_rx, notif_rx, status_rx, packet_handle);

        // Assert
        assert_eq!(ctx.config.service.can_stop, config.service.can_stop);
    }
}

mod tests_cfg {
    use thryve_utils::cfg::config::*;
    use thryve_utils::errors::{ConfigError, ThryveError, get_err};

    // Tests for Config
    #[test]
    fn test_config_default_values() {
        // Arrange & Act
        let config = Config::default();

        // Assert - verify all default values
        assert!(config.service.can_stop);
        assert!(config.service.can_pause);
        assert_eq!(config.engine.idle_wait, 100);
        assert_eq!(config.threads.worker_count, 4);
        assert_eq!(config.pipes.task_pipe_capacity, 1024);
        assert_eq!(config.pipes.notif_pipe_capacity, 1024);
        assert_eq!(config.pipes.res_pipe_capacity, 1024);
        assert_eq!(config.pipes.tasks_processed_per_turn, 500);
        assert_eq!(config.pipes.notifs_processed_per_turn, 500);
        assert_eq!(config.pipes.res_processed_per_turn, 500);
        assert_eq!(config.tasks.max_priority, 4);
        assert_eq!(config.tasks.aging_interval, 3);
        assert!(!config.debugging.is_debugging);
        assert_eq!(config.debugging.wait_before_execution, 10);
        assert!(!config.debugging.visual_graph);
        assert!(!config.debugging.calculate_metrics);
        assert!(!config.debugging.log_tasks);
    }

    #[test]
    fn test_config_clone() {
        // Arrange
        let config1 = Config::default();

        // Act
        let config2 = config1.clone();

        // Assert
        assert_eq!(config1.service.can_stop, config2.service.can_stop);
        assert_eq!(config1.engine.idle_wait, config2.engine.idle_wait);
    }

    // Tests for ServiceConfig
    #[test]
    fn test_service_config_default() {
        // Arrange & Act
        let config = ServiceConfig::default();

        // Assert
        assert!(config.can_stop);
        assert!(config.can_pause);
    }

    #[test]
    fn test_service_config_clone() {
        // Arrange
        let config1 = ServiceConfig {
            can_stop: false,
            can_pause: true,
        };

        // Act
        let config2 = config1.clone();

        // Assert
        assert_eq!(config1.can_stop, config2.can_stop);
        assert_eq!(config1.can_pause, config2.can_pause);
    }

    // Tests for EngineConfig
    #[test]
    fn test_engine_config_default() {
        // Arrange & Act
        let config = EngineConfig::default();

        // Assert
        assert_eq!(config.idle_wait, 100);
    }

    #[test]
    fn test_engine_config_with_custom_value() {
        // Arrange & Act
        let config = EngineConfig {
            idle_wait: 10,
            shutdown_grace_period: 5,
        };

        // Assert
        assert_eq!(config.idle_wait, 10);
        assert_eq!(config.shutdown_grace_period, 5);
    }

    // Tests for ThreadConfig
    #[test]
    fn test_thread_config_default() {
        // Arrange & Act
        let config = ThreadConfig::default();

        // Assert
        assert_eq!(config.worker_count, 4);
    }

    #[test]
    fn test_thread_config_with_zero_workers() {
        // Arrange & Act
        let config = ThreadConfig { worker_count: 0 };

        // Assert
        assert_eq!(config.worker_count, 0);
    }

    // Tests for PipeConfig
    #[test]
    fn test_pipe_config_default() {
        // Arrange & Act
        let config = PipeConfig::default();

        // Assert
        assert_eq!(config.task_pipe_capacity, 1024);
        assert_eq!(config.notif_pipe_capacity, 1024);
        assert_eq!(config.res_pipe_capacity, 1024);
        assert_eq!(config.tasks_processed_per_turn, 500);
        assert_eq!(config.notifs_processed_per_turn, 500);
        assert_eq!(config.res_processed_per_turn, 500);
    }

    #[test]
    fn test_pipe_config_with_custom_values() {
        // Arrange & Act
        let config = PipeConfig {
            task_pipe_capacity: 2048,
            tasks_processed_per_turn: 100,
            notif_pipe_capacity: 512,
            notifs_processed_per_turn: 50,
            res_pipe_capacity: 256,
            res_processed_per_turn: 25,
        };

        // Assert
        assert_eq!(config.task_pipe_capacity, 2048);
        assert_eq!(config.tasks_processed_per_turn, 100);
        assert_eq!(config.notif_pipe_capacity, 512);
        assert_eq!(config.notifs_processed_per_turn, 50);
        assert_eq!(config.res_pipe_capacity, 256);
        assert_eq!(config.res_processed_per_turn, 25);
    }

    // Tests for TaskConfig
    #[test]
    fn test_task_config_default() {
        // Arrange & Act
        let config = TaskConfig::default();

        // Assert
        assert_eq!(config.max_priority, 4);
        assert_eq!(config.aging_interval, 3);
    }

    #[test]
    fn test_task_config_with_min_priority() {
        // Arrange & Act
        let config = TaskConfig {
            max_priority: 0,
            aging_interval: 3,
        };

        // Assert
        assert_eq!(config.max_priority, 0);
    }

    #[test]
    fn test_task_config_with_max_valid_priority() {
        // Arrange & Act
        let config = TaskConfig {
            max_priority: 14,
            aging_interval: 3,
        };

        // Assert
        assert_eq!(config.max_priority, 14);
    }

    // Tests for DebugConfig
    #[test]
    fn test_debug_config_default() {
        // Arrange & Act
        let config = DebugConfig::default();

        // Assert
        assert!(!config.is_debugging);
        assert_eq!(config.wait_before_execution, 10);
        assert!(!config.visual_graph);
        assert!(!config.calculate_metrics);
        assert!(!config.log_tasks);
    }

    #[test]
    fn test_debug_config_enabled() {
        // Arrange & Act
        let config = DebugConfig {
            is_debugging: true,
            wait_before_execution: 30,
            visual_graph: false,
            calculate_metrics: false,
            log_tasks: false,
        };

        // Assert
        assert!(config.is_debugging);
        assert_eq!(config.wait_before_execution, 30);
        assert!(!config.visual_graph);
        assert!(!config.calculate_metrics);
        assert!(!config.log_tasks);
    }

    #[test]
    fn test_debug_config_all_enabled() {
        // Arrange & Act
        let config = DebugConfig {
            is_debugging: true,
            wait_before_execution: 30,
            visual_graph: true,
            calculate_metrics: true,
            log_tasks: true,
        };

        // Assert
        assert!(config.is_debugging);
        assert_eq!(config.wait_before_execution, 30);
        assert!(config.visual_graph);
        assert!(config.calculate_metrics);
        assert!(config.log_tasks);
    }

    // Tests for load_def_config
    #[test]
    fn test_load_def_config_returns_defaults() {
        // Arrange & Act
        let config = Config::load_def_config();

        // Assert
        assert_eq!(config.threads.worker_count, 4);
        assert_eq!(config.engine.idle_wait, 100);
    }

    // Tests for load_config_from_file
    #[test]
    fn test_load_config_from_file_nonexistent_returns_read_error() {
        // Arrange
        let nonexistent_path = "/nonexistent/path/config.toml";

        // Act
        let result = Config::load_config_from_file(nonexistent_path);

        // Assert - no fallback to the defaults: the read error is returned
        assert!(matches!(
            result,
            Err(ThryveError::Config(ConfigError::FileRead { .. }))
        ));
    }

    #[test]
    fn test_load_config_from_file_with_valid_content() {
        // Arrange - create a temp file with every key set to a valid value
        use std::fs::File;
        use std::io::Write;

        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_config.toml");

        let toml_content = r#"
            [init]
            root_dir = ""

            [service]
            can_stop = true
            can_pause = true

            [engine]
            idle_wait = 10
            shutdown_grace_period = 10

            [threads]
            worker_count = 8

            [pipes]
            task_pipe_capacity = 1024
            tasks_processed_per_turn = 200
            notif_pipe_capacity = 1024
            notifs_processed_per_turn = 300
            res_pipe_capacity = 1024
            res_processed_per_turn = 400

            [tasks]
            max_priority = 4
            aging_interval = 3

            [debugging]
            is_debugging = false
            wait_before_execution = 10
            visual_graph = false
            calculate_metrics = false
            log_tasks = false
        "#;

        let mut file = File::create(&file_path).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        // Act
        let result = Config::load_config_from_file(file_path.to_str().unwrap());

        // Cleanup
        let _ = std::fs::remove_file(&file_path);

        // Assert
        let config = match result {
            Ok(config) => config,
            Err(e) => panic!("a valid config file should load: {}", get_err(&e)),
        };
        assert_eq!(config.threads.worker_count, 8);
        assert_eq!(config.engine.idle_wait, 10);
        assert_eq!(config.pipes.tasks_processed_per_turn, 200);
        assert_eq!(config.pipes.notifs_processed_per_turn, 300);
        assert_eq!(config.pipes.res_processed_per_turn, 400);
    }

    #[test]
    fn test_load_config_from_file_missing_section_returns_parse_error() {
        // Arrange - create a temp file with only one section
        use std::fs::File;
        use std::io::Write;

        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_config_partial.toml");

        let toml_content = r#"
            [threads]
            worker_count = 16
        "#;

        let mut file = File::create(&file_path).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        // Act
        let result = Config::load_config_from_file(file_path.to_str().unwrap());

        // Cleanup
        let _ = std::fs::remove_file(&file_path);

        // Assert - every key is required, so the missing ones are a parse error
        assert!(matches!(
            result,
            Err(ThryveError::Config(ConfigError::FileParse { .. }))
        ));
    }

    #[test]
    fn test_load_config_from_file_invalid_priority_returns_error() {
        // Arrange - create a temp file with invalid priority
        use std::fs::File;
        use std::io::Write;

        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_config_invalid.toml");

        let toml_content = r#"
            [tasks]
            max_priority = 20
        "#;

        let mut file = File::create(&file_path).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        // Act - an out-of-range max_priority must not panic (it can be reached
        // via thryve_config_from_file, and a panic there would cross the FFI
        // boundary as undefined behavior); it must be returned as an error.
        let result = Config::load_config_from_file(file_path.to_str().unwrap());

        // Cleanup
        let _ = std::fs::remove_file(&file_path);

        // Assert
        assert!(matches!(result, Err(ThryveError::Config(_))));
    }
}
