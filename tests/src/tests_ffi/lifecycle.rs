// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! FFI lifecycle tests — full create/use/free sequences.
//!
//! These tests verify that the FFI layer can be used in a complete lifecycle
//! without crashes: config → context → plugins → observers → run → free.

mod tests_config_lifecycle {
    use thryve_ffi::{thryve_config_default, thryve_config_free};

    #[test]
    fn test_config_create_free_cycle() {
        // Arrange & Act — create and immediately free
        let config = thryve_config_default();
        assert!(!config.is_null(), "config should not be null");
        thryve_config_free(config);
    }

    #[test]
    fn test_config_create_ctx_free_cycle() {
        // Arrange & Act — create config, ctx, then free both
        let config = thryve_config_default();
        let ctx = thryve_ffi::thryve_ctx_create(config);
        assert!(!ctx.is_null(), "ctx should not be null");
        thryve_ffi::thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_config_create_ctx_packet_handle_free_cycle() {
        // Arrange & Act — full setup: config → ctx → packet handle (from ctx)
        let config = thryve_config_default();
        let ctx = thryve_ffi::thryve_ctx_create(config);
        let pkt = thryve_ffi::thryve_ctx_get_packet_handle(ctx);

        assert!(!ctx.is_null(), "ctx should not be null");
        assert!(!pkt.is_null(), "packet handle should not be null");

        // Cleanup in reverse order
        thryve_ffi::thryve_packet_handle_free(pkt);
        thryve_ffi::thryve_ctx_free(ctx);
        thryve_config_free(config);
    }
}

mod tests_ctx_lifecycle {
    use thryve_ffi::{
        thryve_config_default, thryve_config_free, thryve_ctx_add_observer, thryve_ctx_add_plugin,
        thryve_ctx_create, thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_observer_create,
        thryve_packet_handle_free, thryve_plugin_create,
    };

    #[test]
    fn test_ctx_create_with_packet_handle() {
        // Arrange
        let config = thryve_config_default();

        // Act
        let ctx = thryve_ctx_create(config);
        assert!(!ctx.is_null(), "ctx should not be null");
        let pkt = thryve_ctx_get_packet_handle(ctx);
        assert!(!pkt.is_null(), "packet handle should not be null");

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_ctx_get_packet_handle_lifecycle() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);

        // Act — get engine packet handle
        let engine_pkt = thryve_ctx_get_packet_handle(ctx);
        assert!(
            !engine_pkt.is_null(),
            "engine packet handle should not be null"
        );

        // Cleanup
        thryve_packet_handle_free(engine_pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_ctx_add_plugin_lifecycle() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Create plugin
        extern "C" fn test_entry() -> thryve_ffi::ThryveTaskResult {
            thryve_ffi::thryve_task_result_new(
                std::ffi::CString::new("Success").unwrap().as_ptr(),
                std::ffi::CString::new("Done").unwrap().as_ptr(),
                std::ffi::CString::new("Main").unwrap().as_ptr(),
            )
        }

        extern "C" fn test_cleanup(_core_ptr: *const u8) -> i32 {
            thryve_ffi::ThryveStatus::Ok as i32
        }

        let plugin = unsafe {
            thryve_plugin_create(
                std::ffi::CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                test_cleanup,
                test_entry,
            )
        };

        assert!(!plugin.is_null(), "plugin should not be null");

        // Act — add plugin to context
        let status = thryve_ctx_add_plugin(ctx, plugin);
        assert_eq!(status, thryve_ffi::ThryveStatus::Ok);

        // Plugin is now owned by ctx, don't free it
        thryve_ctx_free(ctx);
        thryve_packet_handle_free(pkt);
        thryve_config_free(config);
    }

    #[test]
    fn test_ctx_add_observer_lifecycle() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        extern "C" fn test_obs_cleanup(_core_ptr: *const u8) {}
        extern "C" fn test_obs_job(_observer: thryve_ffi::ThryveObserver) {}

        // Create observer
        let observer = unsafe {
            thryve_observer_create(
                std::ffi::CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                test_obs_cleanup,
                test_obs_job,
            )
        };

        assert!(!observer.is_null(), "observer should not be null");

        // Act — add observer to context
        let status = thryve_ctx_add_observer(ctx, observer);
        assert_eq!(status, thryve_ffi::ThryveStatus::Ok);

        // Observer is now owned by ctx, don't free it
        thryve_ctx_free(ctx);
        thryve_packet_handle_free(pkt);
        thryve_config_free(config);
    }
}

mod tests_plugin_lifecycle {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveTaskResult, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_packet_handle_free,
        thryve_plugin_create, thryve_plugin_free, thryve_task_result_new,
    };

    extern "C" fn test_plugin_entry() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    extern "C" fn test_plugin_cleanup(_core_ptr: *const u8) -> i32 {
        thryve_ffi::ThryveStatus::Ok as i32
    }

    #[test]
    fn test_plugin_create_free_lifecycle() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act — create plugin
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                test_plugin_cleanup,
                test_plugin_entry,
            )
        };

        assert!(!plugin.is_null(), "plugin should not be null");

        // Act — free plugin
        thryve_plugin_free(plugin);

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_plugin_create_free_without_pkt() {
        // Arrange
        let config = thryve_config_default();

        // Act — try to create plugin without packet handle (should fail)
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(), // NULL packet handle
                test_plugin_cleanup,
                test_plugin_entry,
            )
        };

        // Assert — plugin creation should fail with NULL packet handle
        assert!(
            plugin.is_null(),
            "plugin should be null when packet handle is null"
        );

        // Cleanup
        thryve_config_free(config);
    }
}

mod tests_observer_lifecycle {
    use std::ffi::CString;
    use thryve_ffi::{
        thryve_config_default, thryve_config_free, thryve_ctx_create, thryve_ctx_free,
        thryve_ctx_get_packet_handle, thryve_observer_create, thryve_observer_free,
        thryve_packet_handle_free,
    };

    extern "C" fn test_obs_cleanup(_core_ptr: *const u8) {}
    extern "C" fn test_obs_job(_observer: thryve_ffi::ThryveObserver) {}

    #[test]
    fn test_observer_create_free_lifecycle() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act — create observer
        let observer = unsafe {
            thryve_observer_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                test_obs_cleanup,
                test_obs_job,
            )
        };

        assert!(!observer.is_null(), "observer should not be null");

        // Act — free observer
        thryve_observer_free(observer);

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_observer_create_null_packet_handle_fails() {
        // Arrange
        let config = thryve_config_default();

        // Act — try to create observer with null packet handle
        let observer = unsafe {
            thryve_observer_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(), // NULL packet handle
                test_obs_cleanup,
                test_obs_job,
            )
        };

        // Assert — observer creation should fail
        assert!(
            observer.is_null(),
            "observer should be null when packet handle is null"
        );

        // Cleanup
        thryve_config_free(config);
    }

    #[test]
    fn test_observer_free_null() {
        // Act — free null observer (should not panic)
        thryve_observer_free(std::ptr::null_mut());
    }
}

mod tests_task_lifecycle {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveTaskResult, thryve_task_create, thryve_task_free, thryve_task_result_new,
    };

    extern "C" fn test_task_job() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    #[test]
    fn test_task_create_free_lifecycle() {
        // Act — create task
        let task = unsafe {
            thryve_task_create(
                CString::new("TestTask").unwrap().as_ptr(),
                5, // priority
                test_task_job,
            )
        };

        assert!(!task.is_null(), "task should not be null");

        // Act — free task
        thryve_task_free(task);
    }

    #[test]
    fn test_task_create_null_name_fails() {
        // Act — try to create task with null name
        let task = unsafe {
            thryve_task_create(
                std::ptr::null(), // NULL name
                5,                // priority
                test_task_job,
            )
        };

        // Assert — task creation should fail
        assert!(task.is_null(), "task should be null when name is null");
    }

    #[test]
    fn test_task_free_null() {
        // Act — free null task (should not panic)
        thryve_task_free(std::ptr::null_mut());
    }
}

mod tests_full_interactive_lifecycle {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveTaskResult, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_observer_create,
        thryve_packet_handle_free, thryve_plugin_create, thryve_task_result_new,
    };

    extern "C" fn test_entry() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    extern "C" fn test_cleanup(_core_ptr: *const u8) -> i32 {
        thryve_ffi::ThryveStatus::Ok as i32
    }

    #[test]
    fn test_full_interactive_setup() {
        // This test verifies the full interactive setup flow without actually
        // running the engine (which would block).

        // 1. Create config
        let config = thryve_config_default();
        assert!(!config.is_null(), "config should not be null");

        // 2. Create context
        let ctx = thryve_ctx_create(config);
        assert!(!ctx.is_null(), "ctx should not be null");

        // 3. Get the engine's connected packet handle
        let pkt = thryve_ctx_get_packet_handle(ctx);
        assert!(!pkt.is_null(), "packet handle should not be null");

        // 4. Create plugin
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                test_cleanup,
                test_entry,
            )
        };

        assert!(!plugin.is_null(), "plugin should not be null");

        // 5. Add plugin to context
        let status = thryve_ffi::thryve_ctx_add_plugin(ctx, plugin);
        assert_eq!(status, thryve_ffi::ThryveStatus::Ok);

        // Plugin is now owned by ctx, don't free it

        // 6. Cleanup — free only what we didn't transfer
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_full_interactive_with_observer() {
        extern "C" fn test_obs_cleanup(_core_ptr: *const u8) {}
        extern "C" fn test_obs_job(_observer: thryve_ffi::ThryveObserver) {}

        // 1. Create config
        let config = thryve_config_default();

        // 2. Create context
        let ctx = thryve_ctx_create(config);

        // 3. Get the engine's connected packet handle
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // 4. Create plugin
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                test_cleanup,
                test_entry,
            )
        };

        // 5. Add plugin to context
        thryve_ffi::thryve_ctx_add_plugin(ctx, plugin);

        // 6. Create observer
        let observer = unsafe {
            thryve_observer_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                test_obs_cleanup,
                test_obs_job,
            )
        };

        // 7. Add observer to context
        let status = thryve_ffi::thryve_ctx_add_observer(ctx, observer);
        assert_eq!(status, thryve_ffi::ThryveStatus::Ok);

        // Both plugin and observer are now owned by ctx

        // 8. Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    mod tests_service_lifecycle {
        use thryve_ffi::{
            thryve_config_default, thryve_config_free, thryve_ctx_free, thryve_service_create,
            thryve_service_free,
        };

        #[test]
        fn test_service_create_free_lifecycle() {
            // Arrange
            let config = thryve_config_default();

            // Act — create service (bundled with its matching ctx)
            let created = thryve_service_create(config);

            // Note: service/ctx may be NULL if the service subsystem is not available
            // (e.g., not on Windows)
            if !created.service.is_null() {
                // Act — free service
                thryve_service_free(created.service);
            }
            if !created.ctx.is_null() {
                thryve_ctx_free(created.ctx);
            }

            // Cleanup
            thryve_config_free(config);
        }

        #[test]
        fn test_service_create_with_valid_config() {
            // Arrange
            let config = thryve_config_default();

            // Act — create service (bundled with its matching ctx)
            let created = thryve_service_create(config);

            // Assert — service/ctx are created (may be NULL on non-Windows)
            // The important thing is that it doesn't crash

            // Cleanup
            if !created.service.is_null() {
                thryve_service_free(created.service);
            }
            if !created.ctx.is_null() {
                thryve_ctx_free(created.ctx);
            }
            thryve_config_free(config);
        }
    }

    mod tests_ctx_get_packet_handle_lifecycle {
        use thryve_ffi::{
            thryve_config_default, thryve_config_free, thryve_ctx_create, thryve_ctx_free,
            thryve_ctx_get_packet_handle, thryve_packet_handle_free,
        };

        #[test]
        fn test_ctx_get_packet_handle_returns_valid_handle() {
            // Arrange
            let config = thryve_config_default();
            let ctx = thryve_ctx_create(config);

            // Act — get packet handle from ctx
            let pkt = thryve_ctx_get_packet_handle(ctx);

            // Assert — packet handle should be valid
            assert!(
                !pkt.is_null(),
                "thryve_ctx_get_packet_handle should return a valid handle"
            );

            // Cleanup
            thryve_packet_handle_free(pkt);
            thryve_ctx_free(ctx);
            thryve_config_free(config);
        }

        #[test]
        fn test_ctx_get_packet_handle_multiple_times() {
            // Arrange
            let config = thryve_config_default();
            let ctx = thryve_ctx_create(config);

            // Act — get packet handle multiple times
            let pkt1 = thryve_ctx_get_packet_handle(ctx);
            let pkt2 = thryve_ctx_get_packet_handle(ctx);

            // Assert — both should be valid
            assert!(!pkt1.is_null(), "first handle should not be null");
            assert!(!pkt2.is_null(), "second handle should not be null");

            // Cleanup
            thryve_packet_handle_free(pkt1);
            thryve_packet_handle_free(pkt2);
            thryve_ctx_free(ctx);
            thryve_config_free(config);
        }
    }

    mod tests_notification_lifecycle {
        use std::ffi::CString;
        use thryve_ffi::{thryve_notification_create, thryve_notification_free};

        #[test]
        fn test_notification_create_free_lifecycle() {
            // Act — create notification
            let notif = unsafe {
                thryve_notification_create(
                    CString::new("TestNotif").unwrap().as_ptr(),
                    CString::new("TestReceiver").unwrap().as_ptr(),
                    std::ptr::null_mut(),
                )
            };

            assert!(!notif.is_null(), "notification should not be null");

            // Act — free notification
            thryve_notification_free(notif);
        }

        #[test]
        fn test_notification_create_with_core_ptr() {
            // Arrange — create a valid core pointer
            let core_data = Box::into_raw(Box::new(42u32)) as *mut u8;

            // Act — create notification with core pointer
            let notif = unsafe {
                thryve_notification_create(
                    CString::new("TestNotif").unwrap().as_ptr(),
                    CString::new("TestReceiver").unwrap().as_ptr(),
                    core_data,
                )
            };

            assert!(!notif.is_null(), "notification should not be null");

            // Cleanup
            thryve_notification_free(notif);
            // Note: core_data is owned by the notification, don't free it separately
        }
    }

    mod tests_task_result_lifecycle {
        use std::ffi::CString;
        use thryve_ffi::{
            ThryveTaskResult, ffi_result_to_result, thryve_task_create, thryve_task_result_new,
            thryve_task_result_new_with_chain,
        };

        extern "C" fn dummy_task_job() -> ThryveTaskResult {
            thryve_task_result_new(
                CString::new("Success").unwrap().as_ptr(),
                CString::new("Done").unwrap().as_ptr(),
                CString::new("Main").unwrap().as_ptr(),
            )
        }

        #[test]
        fn test_task_result_conversion() {
            // Arrange
            let result_type = CString::new("Success").unwrap();
            let message = CString::new("Task completed").unwrap();
            let log_file = CString::new("Main").unwrap();

            // Act — create FFI result and convert to internal result
            let ffi_result =
                thryve_task_result_new(result_type.as_ptr(), message.as_ptr(), log_file.as_ptr());

            let internal_result = ffi_result_to_result(ffi_result);

            // Assert
            assert_eq!(internal_result.result_type(), "Success");
            assert_eq!(internal_result.message, "Task completed");
            assert_eq!(internal_result.log_msg_to_file(), "Main");
        }

        #[test]
        fn test_task_result_with_chain_conversion() {
            // Arrange
            let result_type = CString::new("Chained").unwrap();
            let message = CString::new("Chaining").unwrap();
            let log_file = CString::new("Main").unwrap();

            // Create a task to chain
            let task_name = CString::new("ChainedTask").unwrap();
            let task = unsafe { thryve_task_create(task_name.as_ptr(), 5, dummy_task_job) };

            // Act — create FFI result with chain and convert
            let ffi_result = thryve_task_result_new_with_chain(
                result_type.as_ptr(),
                message.as_ptr(),
                log_file.as_ptr(),
                task,
            );

            let internal_result = ffi_result_to_result(ffi_result);

            // Assert
            assert_eq!(internal_result.result_type(), "Chained");
            assert_eq!(internal_result.message, "Chaining");
            assert_eq!(internal_result.log_msg_to_file(), "Main");
            assert!(internal_result.next_task.is_some());

            // NOTE: Do NOT free task here — ffi_result_to_result takes ownership
            // of the chained task via Box::from_raw, so the task is already
            // owned by the internal_result. Freeing it again would cause a
            // double-free (heap corruption).
        }

        #[test]
        fn test_task_result_null_chain_conversion() {
            // Arrange
            let result_type = CString::new("Chained").unwrap();
            let message = CString::new("Chaining").unwrap();
            let log_file = CString::new("Main").unwrap();

            // Act — create FFI result with null chain and convert
            let ffi_result = thryve_task_result_new_with_chain(
                result_type.as_ptr(),
                message.as_ptr(),
                log_file.as_ptr(),
                std::ptr::null_mut(),
            );

            let internal_result = ffi_result_to_result(ffi_result);

            // Assert — should not panic, next_task should be None
            assert_eq!(internal_result.result_type(), "Chained");
            assert!(internal_result.next_task.is_none());
        }
    }
}
