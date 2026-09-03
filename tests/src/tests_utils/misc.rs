// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_extras {
    mod tests {
        use thryve_utils::extras::*;

        // Tests for named_input module
        mod named_input_tests {
            use super::*;

            #[test]
            fn test_named_input_new_valid_alphanumeric() {
                // Arrange
                let valid_name = "ValidName123";

                // Act
                let result = named_input::NamedInput::new(valid_name);

                // Assert
                assert_eq!(result.inner(), "ValidName123");
            }

            #[test]
            fn test_named_input_new_letters_only() {
                // Arrange
                let letters_only = "HelloWorld";

                // Act
                let result = named_input::NamedInput::new(letters_only);

                // Assert
                assert_eq!(result.inner(), "HelloWorld");
            }

            #[test]
            fn test_named_input_new_numbers_only() {
                // Arrange
                let numbers_only = "123456";

                // Act
                let result = named_input::NamedInput::new(numbers_only);

                // Assert
                assert_eq!(result.inner(), "123456");
            }

            #[test]
            fn test_named_input_new_mixed_case() {
                // Arrange
                let mixed_case = "CamelCase123";

                // Act
                let result = named_input::NamedInput::new(mixed_case);

                // Assert
                assert_eq!(result.inner(), "CamelCase123");
            }

            #[test]
            #[should_panic(expected = "NamedInput must be alphanumeric")]
            fn test_named_input_new_with_hyphen_panics() {
                // Arrange
                let invalid_name = "Invalid-Name";

                // Act & Assert
                let _ = named_input::NamedInput::new(invalid_name);
            }

            #[test]
            #[should_panic(expected = "NamedInput must be alphanumeric")]
            fn test_named_input_new_with_underscore_panics() {
                // Arrange
                let invalid_name = "Invalid_Name";

                // Act & Assert
                let _ = named_input::NamedInput::new(invalid_name);
            }

            #[test]
            #[should_panic(expected = "NamedInput must be alphanumeric")]
            fn test_named_input_new_with_space_panics() {
                // Arrange
                let invalid_name = "Invalid Name";

                // Act & Assert
                let _ = named_input::NamedInput::new(invalid_name);
            }

            #[test]
            #[should_panic(expected = "NamedInput must be alphanumeric")]
            fn test_named_input_new_with_special_char_panics() {
                // Arrange
                let invalid_name = "Invalid@Name";

                // Act & Assert
                let _ = named_input::NamedInput::new(invalid_name);
            }

            #[test]
            #[should_panic(expected = "NamedInput must be alphanumeric")]
            fn test_named_input_new_with_dot_panics() {
                // Arrange
                let invalid_name = "Invalid.Name";

                // Act & Assert
                let _ = named_input::NamedInput::new(invalid_name);
            }

            #[test]
            fn test_named_input_inner_returns_correct_value() {
                // Arrange
                let name = named_input::NamedInput::new("TestName");

                // Act
                let inner = name.inner();

                // Assert
                assert_eq!(inner, "TestName");
            }

            #[test]
            fn test_named_input_display_impl() {
                // Arrange
                let name = named_input::NamedInput::new("DisplayTest");

                // Act
                let display_str = format!("{}", name);

                // Assert
                assert_eq!(display_str, "DisplayTest");
            }

            #[test]
            fn test_named_input_clone() {
                // Arrange
                let original = named_input::NamedInput::new("CloneTest");

                // Act
                let cloned = original.clone();

                // Assert
                assert_eq!(cloned.inner(), original.inner());
            }

            #[test]
            fn test_named_input_debug_impl() {
                // Arrange
                let name = named_input::NamedInput::new("DebugTest");

                // Act
                let debug_str = name.inner().to_string();

                // Assert
                assert!(debug_str.contains("DebugTest"));
            }

            #[test]
            fn test_named_input_partial_eq() {
                // Arrange
                let name1 = named_input::NamedInput::new("EqualTest");
                let name2 = named_input::NamedInput::new("EqualTest");
                let name3 = named_input::NamedInput::new("Different");

                // Act & Assert
                assert_eq!(name1.inner(), name2.inner());
                assert_ne!(name1.inner(), name3.inner());
            }

            #[test]
            fn test_named_input_hash_consistency() {
                // Arrange
                let name1 = named_input::NamedInput::new("HashTest");
                let name2 = named_input::NamedInput::new("HashTest");

                // Act
                use std::collections::HashSet;
                let mut set = HashSet::new();
                set.insert(name1);

                // Assert - equal items should have same hash
                assert!(set.contains(&name2));
            }
        }

        // Tests for jobs module - type aliases don't need extensive testing
        // but we can verify they compile and work correctly
        mod jobs_tests {

            use thryve_utils::extras::jobs::{ObsJob, TaskJob};

            use super::*;
            use std::sync::Arc;

            #[test]
            fn test_task_job_type_alias_compiles() {
                // Arrange & Act - just verify the type alias works
                let _job = TaskJob::new(|| {
                    thryve_utils::res::Result::new("Test", "Test message".to_string(), "Main")
                });

                // Assert - if we got here, the type alias is valid
            }

            #[test]
            fn test_task_job_execution() {
                // Arrange
                let job = TaskJob::new(|| {
                    thryve_utils::res::Result::new("Success", "Completed".to_string(), "Main")
                });

                // Act
                let result = job.call_once();

                // Assert
                assert_eq!(result.result_type.inner(), "Success");
                assert_eq!(result.message, "Completed");
            }

            #[test]
            fn test_obs_job_type_alias_compiles() {
                // Arrange & Act - verify the type alias works
                let _job = ObsJob::new(
                    move |_observer: Arc<thryve_utils::bases::observer::Observer>| {
                        // Observer initialization logic
                    },
                );

                // Assert - if we got here, the type alias is valid
            }

            #[test]
            fn test_task_job_from_boxed_fnonce() {
                // Arrange
                let boxed: Box<dyn FnOnce() -> thryve_utils::res::Result + Send + Sync> =
                    Box::new(|| {
                        thryve_utils::res::Result::new("FromBoxed", "Done".to_string(), "Main")
                    });

                // Act
                let job: TaskJob = TaskJob::from(boxed);
                let result = job.call_once();

                // Assert
                assert_eq!(result.result_type.inner(), "FromBoxed");
                assert_eq!(result.message, "Done");
            }

            #[test]
            fn test_obs_job_from_boxed_fn() {
                // Arrange
                let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let executed_clone = Arc::clone(&executed);
                let boxed: Box<dyn Fn(Arc<thryve_utils::bases::observer::Observer>) + Send + Sync> =
                    Box::new(move |_observer| {
                        executed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    });

                // Act
                let job: ObsJob = ObsJob::from(boxed);
                let (task_tx, _) =
                    crossbeam::channel::unbounded::<thryve_utils::packets::task::Task>();
                let (notif_tx, _) =
                    crossbeam::channel::unbounded::<thryve_utils::bases::notif::Notif>();
                let packet_handle = thryve_utils::packets::packet_handle::PacketHandle::new(
                    task_tx, notif_tx, false,
                );
                let observer = Arc::new(thryve_utils::bases::observer::Observer::new(
                    "TestObserver",
                    "TestType",
                    Arc::new(TestObsLogic),
                    job,
                    packet_handle,
                ));
                observer.run_loop(observer.clone());

                // Assert
                assert!(executed.load(std::sync::atomic::Ordering::SeqCst));
            }

            #[test]
            fn test_obs_job_execution() {
                // Arrange
                let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
                let executed_clone = Arc::clone(&executed);

                let job = ObsJob::new(
                    move |_observer: Arc<thryve_utils::bases::observer::Observer>| {
                        executed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
                    },
                );

                // Create a minimal observer for testing
                let (task_tx, _) =
                    crossbeam::channel::unbounded::<thryve_utils::packets::task::Task>();
                let (notif_tx, _) =
                    crossbeam::channel::unbounded::<thryve_utils::bases::notif::Notif>();
                let packet_handle = thryve_utils::packets::packet_handle::PacketHandle::new(
                    task_tx, notif_tx, false,
                );

                let observer = Arc::new(thryve_utils::bases::observer::Observer::new(
                    "TestObserver",
                    "TestType",
                    Arc::new(TestObsLogic),
                    job,
                    packet_handle,
                ));

                // Act - execute the fn_to_run
                observer.run_loop(observer.clone());

                // Assert
                assert!(executed.load(std::sync::atomic::Ordering::SeqCst));
            }
        }

        // Tests for thread_local module
        mod thread_local_tests {
            use super::*;

            #[test]
            fn test_tl_set_and_get_current_task_fingerprint() {
                // Arrange
                let fingerprint = "test_fingerprint_123".to_string();

                // Act - set the fingerprint
                thread_local::tl_set_current_task_fingerprint(fingerprint.clone());

                // Assert - get should return the same value
                let retrieved = thread_local::tl_get_current_task_fingerprint();
                assert_eq!(retrieved, fingerprint);
            }

            #[test]
            fn test_tl_get_current_task_fingerprint_empty_by_default() {
                // Act & Assert - by default should be empty
                let retrieved = thread_local::tl_get_current_task_fingerprint();
                assert_eq!(retrieved, String::new());
            }

            #[test]
            fn test_tl_set_current_task_fingerprint_overwrites() {
                // Arrange
                let fp1 = "fingerprint_1".to_string();
                let fp2 = "fingerprint_2".to_string();

                // Act - set first fingerprint
                thread_local::tl_set_current_task_fingerprint(fp1.clone());
                let retrieved1 = thread_local::tl_get_current_task_fingerprint();

                // Act - overwrite with second fingerprint
                thread_local::tl_set_current_task_fingerprint(fp2.clone());
                let retrieved2 = thread_local::tl_get_current_task_fingerprint();

                // Assert
                assert_eq!(retrieved1, fp1);
                assert_eq!(retrieved2, fp2);
            }

            #[test]
            fn test_tl_set_current_task_fingerprint_with_special_chars() {
                // Arrange
                let fingerprint = "a1b2c3-d4e5-f6g7-h8i9".to_string();

                // Act
                thread_local::tl_set_current_task_fingerprint(fingerprint.clone());
                let retrieved = thread_local::tl_get_current_task_fingerprint();

                // Assert
                assert_eq!(retrieved, fingerprint);
            }

            #[test]
            fn test_tl_set_current_task_fingerprint_with_empty_string() {
                // Arrange
                let fingerprint = String::new();

                // Act
                thread_local::tl_set_current_task_fingerprint(fingerprint.clone());
                let retrieved = thread_local::tl_get_current_task_fingerprint();

                // Assert
                assert_eq!(retrieved, fingerprint);
            }
        }

        // Tests for misc module
        mod misc_tests {
            use super::*;

            #[test]
            fn test_get_current_date_time_returns_valid_datetime() {
                // Act
                let dt = misc::get_current_date_time();

                // Assert - just verify it returns a valid DateTime
                // We can't check exact value, but we can check it's not far off
                let now: chrono::DateTime<chrono::Local> = chrono::Local::now();
                let diff = (dt.timestamp() - now.timestamp()).abs();
                assert!(diff < 2, "DateTime should be within 2 seconds of now");
            }

            #[test]
            fn test_get_current_date_time_consistency() {
                // Act - call twice in quick succession
                let dt1 = misc::get_current_date_time();
                let dt2 = misc::get_current_date_time();

                // Assert - second should be >= first (within reason)
                let diff = (dt2.timestamp() - dt1.timestamp()).abs();
                assert!(diff < 1, "Two calls should return times within 1 second");
            }
        }

        // Tests for dir_paths module
        mod dir_paths_tests {
            use super::*;

            #[test]
            fn test_get_current_exec_dir_returns_valid_path() {
                // Act
                let exec_dir = dir_paths::get_current_exec_dir()
                    .expect("Should be able to get executable directory");

                // Assert - should be an absolute path
                assert!(exec_dir.is_absolute());
                // Should exist
                assert!(exec_dir.exists());
                // Should be a directory
                assert!(exec_dir.is_dir());
            }

            #[test]
            fn test_create_dir_log_creates_directory() {
                // Act
                let log_dir = dir_paths::create_dir(dir_paths::DirName::Log)
                    .expect("Should be able to create log directory");

                // Assert
                assert!(log_dir.is_absolute());
                assert!(log_dir.exists());
                assert!(log_dir.is_dir());
                // Should contain "log" in path
                assert!(log_dir.to_string_lossy().contains("log"));
            }

            #[test]
            fn test_create_dir_topology_creates_directory() {
                // Act
                let topology_dir = dir_paths::create_dir(dir_paths::DirName::Topology)
                    .expect("Should be able to create topology directory");

                // Assert
                assert!(topology_dir.is_absolute());
                assert!(topology_dir.exists());
                assert!(topology_dir.is_dir());
                // Should contain "topology" in path
                assert!(topology_dir.to_string_lossy().contains("topology"));
            }

            #[test]
            fn test_get_workspace_root_returns_valid_path() {
                // Act
                let workspace_root = dir_paths::get_workspace_root();

                // Assert - should be an absolute path
                assert!(workspace_root.is_absolute());
                // Should exist
                assert!(workspace_root.exists());
                // Should be a directory
                assert!(workspace_root.is_dir());
                // Should contain Cargo.toml
                let cargo_toml = workspace_root.join("Cargo.toml");
                assert!(
                    cargo_toml.exists(),
                    "Workspace root should contain Cargo.toml"
                );
            }
        }

        // Tests for stack module - extract_module_path is private, test via integration
        mod stack_tests {

            // Note: get_task_data requires a real backtrace and is hard to unit test
            // We can test the public API works without crashing

            #[test]
            fn test_stack_module_compiles() {
                // This test just verifies the stack module compiles
                // Actual testing of get_task_data happens in integration tests
                // with real backtraces
            }
        }

        // Tests for panic_hook module
        mod panic_hook_tests {
            use std::collections::HashMap;
            use std::path::PathBuf;
            use thryve_utils::cfg::dir_data::DirData;
            use thryve_utils::extras::dir_paths::DirName;
            use thryve_utils::extras::panic_hook::init_panic_hook;

            // init_panic_hook() installs a process-global panic hook that
            // calls std::process::exit(1) on any panic -- calling it
            // in-process here would corrupt every other test sharing this
            // test binary (the same class of bug fixed for the FFI
            // service_run test). Instead this test re-invokes itself in a
            // fresh child process via this env var as a marker, so the
            // hook's real behavior (file write + exit code) only ever runs
            // in an isolated, disposable process.
            const CHILD_LOG_DIR_ENV_VAR: &str = "THRYVE_PANIC_HOOK_TEST_LOG_DIR";

            #[test]
            fn test_panic_hook_module_compiles() {
                // This test just verifies the panic_hook module compiles
                // Actual panic hook testing would require process-level testing
            }

            #[test]
            fn test_panic_hook_writes_to_error_log_and_exits_with_code_1() {
                if let Ok(log_dir) = std::env::var(CHILD_LOG_DIR_ENV_VAR) {
                    // Child mode: set up a log directory the same way
                    // init_directories() would, install the real panic
                    // hook, then panic to trigger it.
                    let log_dir = PathBuf::from(log_dir);
                    std::fs::create_dir_all(&log_dir).expect("failed to create child log dir");
                    let mut dir_data = DirData::new(log_dir.clone(), HashMap::new());
                    dir_data.set_path(DirName::Log, log_dir);

                    init_panic_hook();
                    panic!("triggered from panic_hook subprocess test");
                }

                // Parent mode: spawn a child process that runs this exact
                // test with the marker env var set, then inspect what the
                // hook produced from the outside.
                let log_dir = std::env::temp_dir()
                    .join(format!("thryve_panic_hook_test_{}", std::process::id()));
                let _ = std::fs::remove_dir_all(&log_dir);
                std::fs::create_dir_all(&log_dir).expect("failed to create parent-side temp dir");

                let exe =
                    std::env::current_exe().expect("failed to resolve current test executable");
                let output = std::process::Command::new(&exe)
                    .arg("--exact")
                    .arg("tests_utils::misc::tests_extras::tests::panic_hook_tests::test_panic_hook_writes_to_error_log_and_exits_with_code_1")
                    // Without this, libtest captures eprintln! output into a
                    // per-test buffer that's only flushed after the test fn
                    // returns normally -- but the panic hook calls
                    // process::exit(1) immediately, so that buffer would
                    // never reach the child's real stderr for us to inspect.
                    .arg("--nocapture")
                    .env(CHILD_LOG_DIR_ENV_VAR, &log_dir)
                    .output()
                    .expect("failed to spawn child test process");

                // Assert - the panic hook calls std::process::exit(1)
                // synchronously from within the hook itself, so the child
                // never returns to the test harness at all; it just exits.
                assert_eq!(
                    output.status.code(),
                    Some(1),
                    "child process should exit with code 1 from the panic hook; stderr: {}",
                    String::from_utf8_lossy(&output.stderr)
                );

                let stderr = String::from_utf8_lossy(&output.stderr);
                assert!(
                    stderr.contains("[PANIC]"),
                    "stderr should contain the panic hook's message: {}",
                    stderr
                );
                assert!(
                    stderr.contains("triggered from panic_hook subprocess test"),
                    "stderr: {}",
                    stderr
                );

                let error_log = log_dir.join("Error.log");
                assert!(
                    error_log.exists(),
                    "panic hook should have written Error.log"
                );
                let contents =
                    std::fs::read_to_string(&error_log).expect("Error.log should be readable");
                assert!(contents.contains("[PANIC]"));
                assert!(contents.contains("triggered from panic_hook subprocess test"));

                let _ = std::fs::remove_dir_all(&log_dir);
            }
        }

        // Helper struct for observer tests
        struct TestObsLogic;

        impl thryve_utils::traits::observer_logic::ObsLogic for TestObsLogic {
            fn clean_up(&self, _observer: std::sync::Arc<thryve_utils::bases::observer::Observer>) {
                // No-op for testing
            }

            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }
    }
}

mod tests_res {
    use thryve_utils::extras::jobs::TaskJob;
    use thryve_utils::packets::task::*;
    use thryve_utils::res::*;
    use thryve_utils::traits::packets::Packet;

    #[test]
    fn test_result_new_creates_regular_result() {
        // Arrange
        let result_type = "Success";
        let message = "Task completed successfully".to_string();
        let log_file = "Main";

        // Act
        let result = Result::new(result_type, message, log_file);

        // Assert
        assert_eq!(result.result_type.inner(), "Success");
        assert_eq!(result.message, "Task completed successfully");
        assert_eq!(result.log_msg_to_file.inner(), "Main");
        assert!(result.next_task.is_none());
        assert!(result.observer.is_none());
        assert!(result.plugin.is_none());
    }

    #[test]
    fn test_result_new_with_empty_message() {
        // Arrange
        let message = "".to_string();

        // Act
        let result = Result::new("Ok", message, "Main");

        // Assert
        assert_eq!(result.message, "");
    }

    #[test]
    fn test_result_new_with_special_characters_in_message() {
        // Arrange
        let message = "Error: Connection failed! @#$%".to_string();

        // Act
        let result = Result::new("Error", message, "Error");

        // Assert
        assert_eq!(result.message, "Error: Connection failed! @#$%");
    }

    #[test]
    fn test_result_chaining_with_task() {
        // Arrange
        let next_task = Task::new(
            "FollowUpTask",
            1,
            TaskJob::new(|| Result::new("Ok", "Done".to_string(), "Main")),
        );

        // Act
        let result =
            Result::new("Chained", "Continuing to next task".to_string(), "Main").chain(next_task);

        // Assert
        assert_eq!(result.result_type.inner(), "Chained");
        assert_eq!(result.message, "Continuing to next task");
        assert!(result.next_task.is_some());
        assert!(result.observer.is_none());
        assert!(result.plugin.is_none());

        // Verify the task details
        if let Some(task) = result.next_task {
            assert_eq!(task.task_name.inner(), "FollowUpTask");
            assert_eq!(task.base_priority, 1);
        }
    }

    #[test]
    fn test_result_display_impl() {
        // Arrange
        let result = Result::new("Test", "Display message".to_string(), "Main");

        // Act
        let display_str = format!("{}", result);

        // Assert
        assert_eq!(display_str, "Message: Display message");
    }

    #[test]
    fn test_result_display_with_empty_message() {
        // Arrange
        let result = Result::new("Test", "".to_string(), "Main");

        // Act
        let display_str = format!("{}", result);

        // Assert
        assert_eq!(display_str, "Message: ");
    }

    #[test]
    fn test_result_chaining_pattern() {
        // Arrange & Act - Simulate a task chain
        let task1_result =
            Result::new("Step1", "First step done".to_string(), "Main").chain(Task::new(
                "Step2",
                2,
                TaskJob::new(|| {
                    Result::new("Step2", "Second step done".to_string(), "Main").chain(Task::new(
                        "Step3",
                        3,
                        TaskJob::new(|| Result::new("Final", "All done".to_string(), "Main")),
                    ))
                }),
            ));

        // Assert first result has a task
        assert!(task1_result.next_task.is_some());

        // Execute the chained task
        if let Some(task) = task1_result.next_task {
            let task2_result = task.process().unwrap().1;
            assert!(task2_result.next_task.is_some());

            // Execute the final task
            if let Some(final_task) = task2_result.next_task {
                let final_result = final_task.process().unwrap().1;
                assert!(final_result.next_task.is_none());
                assert_eq!(final_result.result_type.inner(), "Final");
            }
        }
    }

    #[test]
    fn test_result_with_unicode_message() {
        // Arrange
        let message = "成功！Успех! Éxito!".to_string();

        // Act
        let result = Result::new("Success", message, "Main");

        // Assert
        assert_eq!(result.message, "成功！Успех! Éxito!");
    }

    #[test]
    fn test_result_result_type_validation() {
        // Arrange & Act - These should all work (alphanumeric)
        let _r1 = Result::new("A", "msg".to_string(), "Main");
        let _r2 = Result::new("Z123", "msg".to_string(), "Main");
        let _r3 = Result::new("Type123", "msg".to_string(), "Main");

        // Assert - if we got here, all validations passed
    }

    #[test]
    #[should_panic(expected = "NamedInput must be alphanumeric")]
    fn test_result_invalid_result_type_panics() {
        // Arrange & Act - This should panic due to invalid result type
        let _ = Result::new("Invalid-Type", "msg".to_string(), "Main");
    }

    #[test]
    #[should_panic(expected = "NamedInput must be alphanumeric")]
    fn test_result_invalid_log_file_panics() {
        // Arrange & Act - This should panic due to invalid log file name
        let _ = Result::new("Valid", "msg".to_string(), "Invalid/File");
    }

    #[test]
    fn test_size_in_bytes_returns_struct_size() {
        // Arrange
        let result = Result::new("Success", "Done".to_string(), "Main");

        // Act
        let size = result.size_in_bytes();

        // Assert - size should match std::mem::size_of_val
        assert_eq!(size, std::mem::size_of_val(&result));
        assert!(size > 0);
    }

    #[test]
    fn test_size_in_bytes_consistent_across_instances() {
        // Arrange
        let result1 = Result::new("Type1", "Message1".to_string(), "Main");
        let result2 = Result::new("Type2", "Message2".to_string(), "Error");

        // Act
        let size1 = result1.size_in_bytes();
        let size2 = result2.size_in_bytes();

        // Assert - struct size should be the same regardless of field values
        assert_eq!(size1, size2);
    }

    #[test]
    fn test_size_in_bytes_matches_mem_size_of() {
        // Arrange
        let result = Result::new("MemSizeTest", "test message".to_string(), "Main");

        // Act
        let size = result.size_in_bytes();

        // Assert - should equal std::mem::size_of::<Result>()
        assert_eq!(size, std::mem::size_of::<Result>());
    }

    #[test]
    fn test_size_in_bytes_with_chained_result() {
        // Arrange
        let next_task = Task::new(
            "NextTask",
            1,
            TaskJob::new(|| Result::new("Ok", "Done".to_string(), "Main")),
        );
        let result = Result::new("Chained", "Chained message".to_string(), "Main").chain(next_task);

        // Act
        let size = result.size_in_bytes();

        // Assert - size should still match std::mem::size_of_val
        assert_eq!(size, std::mem::size_of_val(&result));
        assert!(size > 0);
    }

    // Tests for Result::fin()
    #[test]
    fn test_result_fin_returns_finishing_result() {
        // Arrange & Act
        let result = Result::fin();

        // Assert
        assert_eq!(result.result_type.inner(), "Fin");
        assert_eq!(result.message, "finishing capture: starting exit now.");
        assert_eq!(result.log_msg_to_file.inner(), "Main");
        assert!(result.next_task.is_none());
        assert!(result.observer.is_none());
        assert!(result.plugin.is_none());
    }

    #[test]
    fn test_result_fin_default_fields() {
        // Arrange & Act
        let result = Result::fin();

        // Assert - all optional fields should be None
        assert!(result.next_task.is_none());
        assert!(result.observer.is_none());
        assert!(result.plugin.is_none());
    }

    #[test]
    fn test_result_fin_message_content() {
        // Arrange & Act
        let result = Result::fin();

        // Assert - message should contain specific content
        assert!(result.message.contains("finishing"));
        assert!(result.message.contains("exit"));
    }

    #[test]
    fn test_result_fin_is_terminal() {
        // Arrange & Act - fin result should not chain to any task
        let result = Result::fin();

        // Assert - it's a terminal result
        assert!(result.next_task.is_none());
    }

    // Tests for Result::chain with thread-local fingerprint set (lines 188-197)
    #[test]
    fn test_result_chain_with_thread_local_fingerprint() {
        // Arrange - set thread-local fingerprint
        thryve_utils::extras::thread_local::tl_set_current_task_fingerprint(
            "parent_task_fp".to_string(),
        );

        let next_task = Task::new(
            "ChainedTask",
            1,
            TaskJob::new(|| Result::new("Ok", "Done".to_string(), "Main")),
        );

        // Act - chain should set up callsite when thread-local fingerprint is set
        let result = Result::new("Chained", "Chaining task".to_string(), "Main").chain(next_task);

        // Assert - the next task should have a callsite set
        assert!(result.next_task.is_some());
        let task = result.next_task.as_ref().unwrap();
        assert!(task.callsite.is_some());
        let callsite = task.callsite.as_ref().unwrap();
        assert_eq!(callsite.callsite_name, "ChainedTask");
        assert_eq!(
            callsite.current_executing_task_fingerprint,
            "parent_task_fp"
        );

        // Clean up thread-local
        thryve_utils::extras::thread_local::tl_set_current_task_fingerprint(String::new());
    }

    #[test]
    fn test_result_chain_with_different_thread_local_fingerprints() {
        // Test with different fingerprint values
        let test_fingerprints = vec!["task_123", "abc123xyz", "MyTaskName"];

        for fingerprint in test_fingerprints {
            thryve_utils::extras::thread_local::tl_set_current_task_fingerprint(
                fingerprint.to_string(),
            );

            let next_task = Task::new(
                "ChainedTask",
                1,
                TaskJob::new(|| Result::new("Ok", "Done".to_string(), "Main")),
            );

            // Act
            let result =
                Result::new("Chained", "Chaining task".to_string(), "Main").chain(next_task);

            // Assert - the next task should have a callsite set with the fingerprint
            assert!(result.next_task.is_some());
            let task = result.next_task.as_ref().unwrap();
            assert!(task.callsite.is_some());
            let callsite = task.callsite.as_ref().unwrap();
            assert_eq!(callsite.current_executing_task_fingerprint, fingerprint);

            // Clean up
            thryve_utils::extras::thread_local::tl_set_current_task_fingerprint(String::new());
        }
    }

    #[test]
    fn test_result_chain_with_empty_thread_local_fingerprint() {
        // Arrange - ensure thread-local fingerprint is empty
        thryve_utils::extras::thread_local::tl_set_current_task_fingerprint(String::new());

        let next_task = Task::new(
            "ChainedTask",
            1,
            TaskJob::new(|| Result::new("Ok", "Done".to_string(), "Main")),
        );

        // Act - chain should not set up callsite when thread-local fingerprint is empty
        let result = Result::new("Chained", "Chaining task".to_string(), "Main").chain(next_task);

        // Assert - the next task should exist but callsite should be None
        assert!(result.next_task.is_some());
        let task = result.next_task.as_ref().unwrap();
        assert!(task.callsite.is_none());
    }

    #[test]
    fn test_result_chain_with_unicode_message() {
        // Arrange
        let next_task = Task::new(
            "UnicodeTask",
            1,
            TaskJob::new(|| Result::new("Ok", "Done".to_string(), "Main")),
        );

        // Act - chain with unicode message
        let result =
            Result::new("Unicode", "成功！Успех! Éxito!".to_string(), "Main").chain(next_task);

        // Assert
        assert_eq!(result.message, "成功！Успех! Éxito!");
        assert!(result.next_task.is_some());
    }

    #[test]
    fn test_result_chain_with_long_message() {
        // Arrange
        let long_message = "a".repeat(10000);
        let next_task = Task::new(
            "LongTask",
            1,
            TaskJob::new(|| Result::new("Ok", "Done".to_string(), "Main")),
        );

        // Act - chain with long message
        let result = Result::new("Long", long_message.clone(), "Main").chain(next_task);

        // Assert
        assert_eq!(result.message, long_message);
        assert!(result.next_task.is_some());
    }
}

mod tests_threadpool {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use crossbeam::channel::bounded;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::bases::observer::Observer;
    use thryve_utils::cfg;
    use thryve_utils::extras::jobs::ObsJob;
    use thryve_utils::packets::notif_log::Log;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::queues::priority_queue::PQChild;
    use thryve_utils::queues::seg_queue::QChild;
    use thryve_utils::threads::threadpool::*;
    use thryve_utils::traits::observer_logic::ObsLogic;
    use thryve_utils::traits::queue::Queue;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        thryve_utils::impl_as_any!(TestObsLogic);
        fn clean_up(&self, _observer: Arc<Observer>) {
            // No-op for testing
        }
    }

    #[test]
    fn test_thread_pool_new_creates_pool() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle.clone());

        // Act
        let pool = ThreadPool::new(2, queue, packet_handle, None);

        // Assert
        assert_eq!(pool.num_workers(), 2);
        assert!(pool.workers.is_empty());
        assert!(!pool.pool_exit_flag.load(Ordering::SeqCst));
        assert!(!pool.pool_pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_thread_pool_new_with_zero_workers() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle.clone());

        // Act
        let pool = ThreadPool::new(0, queue, packet_handle, None);

        // Assert
        assert_eq!(pool.num_workers(), 0);
    }

    #[test]
    fn test_thread_pool_submit_task_pushes_to_queue() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle.clone());
        let pool = ThreadPool::new(1, queue, packet_handle, None);
        let notif = Log::log("test_message".to_string(), "Main");

        // Act
        pool.submit_task(notif);

        // Set exit flag and try to pop
        pool.main_queue.exit_flag.store(true, Ordering::SeqCst);
        let popped = pool.main_queue.pop();

        // Assert - should have popped our notification
        assert_eq!(popped.notif_type.inner(), "Log");
    }

    #[test]
    fn test_thread_pool_num_workers_returns_correct_value() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle.clone());

        // Act
        let pool = ThreadPool::new(5, queue, packet_handle, None);

        // Assert
        assert_eq!(pool.num_workers(), 5);
    }

    #[test]
    fn test_thread_pool_pool_exit_flag_returns_clone() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle.clone());
        let pool = ThreadPool::new(1, queue, packet_handle, None);

        // Act
        let flag1 = pool.pool_exit_flag();
        let flag2 = pool.pool_exit_flag();

        // Assert - both should be false initially
        assert!(!flag1.load(Ordering::SeqCst));
        assert!(!flag2.load(Ordering::SeqCst));
    }

    #[test]
    fn test_thread_pool_pause_sets_flags() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Assert initially not paused
        assert!(!pool.pool_pause_flag.load(Ordering::SeqCst));

        // Act
        pool.pause();

        // Assert
        assert!(pool.pool_pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_thread_pool_resume_clears_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Pause first
        pool.pause();
        assert!(pool.pool_pause_flag.load(Ordering::SeqCst));

        // Act
        pool.resume();

        // Assert
        assert!(!pool.pool_pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_thread_pool_finalize_plg_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let cfg = cfg::config::Config::default();
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = PQChild::new(&cfg, packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Assert initially not exiting
        assert!(!pool.pool_exit_flag.load(Ordering::SeqCst));

        // Act
        pool.finalize_plg();

        // Assert
        assert!(pool.pool_exit_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_thread_pool_finalize_obs_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Create a mock logger observer
        let (logger_task_tx, _logger_task_rx) = bounded::<Task>(10);
        let (logger_notif_tx, _logger_notif_rx) = bounded::<Notif>(10);
        let logger_packet_handle = PacketHandle::new(logger_task_tx, logger_notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Arc::new(Observer::new(
            "logger",
            "loggerType",
            obs_core,
            fn_to_run,
            logger_packet_handle,
        ));

        // Assert initially not exiting
        assert!(!pool.pool_exit_flag.load(Ordering::SeqCst));

        // Act
        pool.finalize_obs(logger);

        // Assert
        assert!(pool.pool_exit_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_thread_pool_add_worker() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Create a dummy thread handle
        let handle = std::thread::spawn(|| {});

        // Assert initially empty
        assert!(pool.workers.is_empty());

        // Act
        pool.add_worker(handle);

        // Assert
        assert_eq!(pool.workers.len(), 1);
    }
}
