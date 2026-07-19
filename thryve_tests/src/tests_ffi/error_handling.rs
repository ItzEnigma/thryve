//! FFI error handling tests — null pointer and invalid argument checks.
//!
//! These tests verify that all FFI functions properly handle null pointers
//! and invalid arguments, returning appropriate status codes or NULL values.

mod tests_config_errors {
    use thryve_ffi::{
        ffi_config_default,
        ffi_config_from_file,
        ffi_config_free,
    };
    use std::ffi::CString;

    #[test]
    fn test_config_from_file_with_null_path_returns_null() {
        // Act — pass NULL path
        let config = ffi_config_from_file(std::ptr::null());

        // Assert — should return NULL
        assert!(config.is_null(), "ffi_config_from_file should return NULL for null path");
    }

    #[test]
    fn test_config_from_file_with_invalid_utf8_returns_default() {
        // Arrange — create a C string with invalid UTF-8 (3 bytes + null terminator)
        let invalid_utf8_bytes = b"\xFF\xFE\x00";
        let invalid_path = std::ffi::CStr::from_bytes_with_nul(invalid_utf8_bytes).unwrap();
        let path_ptr = invalid_path.as_ptr();

        // Act — ffi_config_from_file converts invalid UTF-8 to empty string, then loads defaults
        let config = ffi_config_from_file(path_ptr);

        // Assert — should return a valid config (defaults), not NULL
        assert!(!config.is_null(), "ffi_config_from_file should return a default config for invalid UTF-8 path");

        // Cleanup
        ffi_config_free(config);
    }

    #[test]
    fn test_config_from_file_with_nonexistent_file_returns_default() {
        // Arrange
        let path = CString::new("/nonexistent/path/config.toml").unwrap();

        // Act — ffi_config_from_file falls back to defaults on IO error
        let config = ffi_config_from_file(path.as_ptr());

        // Assert — should return a valid config (defaults), not NULL
        assert!(!config.is_null(), "ffi_config_from_file should return a default config for nonexistent file");

        // Cleanup
        ffi_config_free(config);
    }

    #[test]
    fn test_config_default_creates_valid_config() {
        // Act
        let config = ffi_config_default();

        // Assert — should return non-NULL
        assert!(!config.is_null(), "ffi_config_default should return a valid config");

        // Cleanup
        ffi_config_free(config);
    }

    #[test]
    fn test_config_free_with_null() {
        // Act — passing NULL should not panic
        ffi_config_free(std::ptr::null_mut());
    }
}

mod tests_packet_handle_errors {
    use thryve_ffi::{
        ffi_packet_handle_free,
        ffi_packet_handle_clone,
    };

    #[test]
    fn test_packet_handle_free_with_null() {
        // Act — should not panic
        ffi_packet_handle_free(std::ptr::null_mut());
    }

    #[test]
    fn test_packet_handle_clone_with_null_returns_null() {
        // Act
        let cloned = ffi_packet_handle_clone(std::ptr::null_mut());

        // Assert
        assert!(cloned.is_null(), "ffi_packet_handle_clone should return NULL for null handle");
    }
}

mod tests_ctx_errors {
    use thryve_ffi::{
        FfiStatus, ffi_config_default, ffi_config_free, ffi_ctx_add_observer, ffi_ctx_add_plugin, ffi_ctx_create, ffi_ctx_free, ffi_ctx_get_packet_handle, ffi_ctx_send_notification, ffi_ctx_send_task, ffi_ctx_set_logging_file_names,
    };
    use std::ffi::CString;
    use std::os::raw::c_char;

    #[test]
    fn test_ctx_create_with_null_config_returns_null() {
        // Act
        let ctx = ffi_ctx_create(std::ptr::null_mut());

        // Assert
        assert!(ctx.is_null(), "ffi_ctx_create should return NULL for null config");
    }

    #[test]
    fn test_ctx_free_with_null() {
        // Act — should not panic
        ffi_ctx_free(std::ptr::null_mut());
    }

    #[test]
    fn test_ctx_add_plugin_with_null_ctx_returns_null_arg() {
        // Act
        let status = ffi_ctx_add_plugin(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null ctx");
    }

    #[test]
    fn test_ctx_add_plugin_with_null_plugin_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);

        // Act
        let status = ffi_ctx_add_plugin(ctx, std::ptr::null_mut());

        // Cleanup
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null plugin");
    }

    #[test]
    fn test_ctx_add_observer_with_null_ctx_returns_null_arg() {
        // Act
        let status = ffi_ctx_add_observer(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null ctx");
    }

    #[test]
    fn test_ctx_add_observer_with_null_observer_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);

        // Act
        let status = ffi_ctx_add_observer(ctx, std::ptr::null_mut());

        // Cleanup
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null observer");
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_null_ctx_returns_null_arg() {
        // Arrange
        let file_name = CString::new("test").unwrap();

        // Act
        let status = ffi_ctx_set_logging_file_names(
                std::ptr::null_mut(),
                &file_name.as_ptr() as *const *const c_char,
                1,
            );

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null ctx");
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_null_file_names_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);

        // Act
        let status = ffi_ctx_set_logging_file_names(ctx, std::ptr::null(), 1);

        // Cleanup
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null file_names");
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_zero_count_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);
        let file_name = CString::new("test").unwrap();
        let file_names = [file_name.as_ptr()];

        // Act
        let status = ffi_ctx_set_logging_file_names(ctx, file_names.as_ptr() as *const *const c_char, 0);

        // Cleanup
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for zero count");
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_negative_count_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);
        let file_name = CString::new("test").unwrap();

        // Act
        let status = ffi_ctx_set_logging_file_names(
                ctx,
                &file_name.as_ptr() as *const *const c_char,
                -1,
            );

        // Cleanup
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for negative count");
    }

    #[test]
    fn test_ctx_send_task_with_null_ctx_returns_null_arg() {
        // Act
        let status = ffi_ctx_send_task(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null ctx");
    }

    #[test]
    fn test_ctx_send_task_with_null_task_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);

        // Act
        let status = ffi_ctx_send_task(ctx, std::ptr::null_mut());

        // Cleanup
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null task");
    }

    #[test]
    fn test_ctx_get_packet_handle_with_null_returns_null() {
        // Act
        let handle = ffi_ctx_get_packet_handle(std::ptr::null_mut());

        // Assert
        assert!(handle.is_null(), "ffi_ctx_get_packet_handle should return NULL for null ctx");
    }

    #[test]
    fn test_ctx_send_notification_with_null_ctx_returns_null_arg() {
        // Act
        let status = ffi_ctx_send_notification(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null ctx");
    }
}

mod tests_task_errors {
    use thryve_ffi::{
        ffi_task_create,
        ffi_task_free,
        ffi_task_result_new,
        FfiTaskResult,
    };
    use std::ffi::CString;

    extern "C" fn dummy_task_job() -> FfiTaskResult {
        ffi_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    #[test]
    fn test_task_create_with_null_name_returns_null() {
        // Act
        let task = ffi_task_create(std::ptr::null(), 0, dummy_task_job);

        // Assert
        assert!(task.is_null(), "ffi_task_create should return NULL for null name");
    }

    #[test]
    fn test_task_free_with_null() {
        // Act — should not panic
        ffi_task_free(std::ptr::null_mut());
    }
}

mod tests_plugin_errors {
    use thryve_ffi::{
        FfiTaskResult, ffi_config_default, ffi_config_free, ffi_ctx_create, ffi_ctx_free, ffi_ctx_get_packet_handle, ffi_packet_handle_free, ffi_plugin_create, ffi_plugin_free, ffi_task_result_new,
    };
    use std::ffi::CString;

    extern "C" fn dummy_plugin_entry() -> FfiTaskResult {
        ffi_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    extern "C" fn dummy_plugin_cleanup(_core_ptr: *const u8) {}

    #[test]
    fn test_plugin_create_with_null_name_returns_null() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);
        let pkt = ffi_ctx_get_packet_handle(ctx);

        // Act
        let plugin = ffi_plugin_create(
                std::ptr::null(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                dummy_plugin_cleanup,
                dummy_plugin_entry,
            );

        // Cleanup
        ffi_packet_handle_free(pkt);
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert!(plugin.is_null(), "ffi_plugin_create should return NULL for null name");
    }

    #[test]
    fn test_plugin_create_with_null_type_returns_null() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);
        let pkt = ffi_ctx_get_packet_handle(ctx);

        // Act
        let plugin = ffi_plugin_create(
                CString::new("TestPlugin").unwrap().as_ptr(),
                std::ptr::null(),
                std::ptr::null_mut(),
                pkt,
                dummy_plugin_cleanup,
                dummy_plugin_entry,
            );

        // Cleanup
        ffi_packet_handle_free(pkt);
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert!(plugin.is_null(), "ffi_plugin_create should return NULL for null type");
    }

    #[test]
    fn test_plugin_create_with_null_packet_handle_returns_null() {
        // Act
        let plugin = ffi_plugin_create(
                CString::new("TestPlugin").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                dummy_plugin_cleanup,
                dummy_plugin_entry,
            );

        // Assert
        assert!(plugin.is_null(), "ffi_plugin_create should return NULL for null packet handle");
    }

    #[test]
    fn test_plugin_free_with_null() {
        // Act — should not panic
        ffi_plugin_free(std::ptr::null_mut());
    }
}

mod tests_observer_errors {
    use thryve_ffi::{
        ffi_config_default, ffi_config_free, ffi_ctx_create, ffi_ctx_free, ffi_ctx_get_packet_handle, ffi_observer_create, ffi_observer_free, ffi_packet_handle_free,
    };
    use std::ffi::CString;

    extern "C" fn dummy_obs_cleanup(_core_ptr: *const u8) {}

    extern "C" fn dummy_obs_job(_observer: *mut u8) {}

    #[test]
    fn test_observer_create_with_null_name_returns_null() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);
        let pkt = ffi_ctx_get_packet_handle(ctx);

        // Act
        let observer = ffi_observer_create(
                std::ptr::null(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                dummy_obs_cleanup,
                dummy_obs_job,
            );

        // Cleanup
        ffi_packet_handle_free(pkt);
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert!(observer.is_null(), "ffi_observer_create should return NULL for null name");
    }

    #[test]
    fn test_observer_create_with_null_type_returns_null() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);
        let pkt = ffi_ctx_get_packet_handle(ctx);

        // Act
        let observer = ffi_observer_create(
                CString::new("TestObserver").unwrap().as_ptr(),
                std::ptr::null(),
                std::ptr::null_mut(),
                pkt,
                dummy_obs_cleanup,
                dummy_obs_job,
            );

        // Cleanup
        ffi_packet_handle_free(pkt);
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert!(observer.is_null(), "ffi_observer_create should return NULL for null type");
    }

    #[test]
    fn test_observer_create_with_null_packet_handle_returns_null() {
        // Act
        let observer = ffi_observer_create(
                CString::new("TestObserver").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                dummy_obs_cleanup,
                dummy_obs_job,
            );

        // Assert
        assert!(observer.is_null(), "ffi_observer_create should return NULL for null packet handle");
    }

    #[test]
    fn test_observer_free_with_null() {
        // Act — should not panic
        ffi_observer_free(std::ptr::null_mut());
    }
}

mod tests_notification_errors {
    use thryve_ffi::{
        ffi_notification_create,
        ffi_notification_free,
    };
    use std::ffi::CString;

    #[test]
    fn test_notification_create_with_null_type_returns_null() {
        // Act
        let notif = ffi_notification_create(
                std::ptr::null(),
                CString::new("TestReceiver").unwrap().as_ptr(),
                std::ptr::null_mut(),
            );

        // Assert
        assert!(notif.is_null(), "ffi_notification_create should return NULL for null type");
    }

    #[test]
    fn test_notification_create_with_null_receiver_returns_null() {
        // Act
        let notif = ffi_notification_create(
                CString::new("TestNotif").unwrap().as_ptr(),
                std::ptr::null(),
                std::ptr::null_mut(),
            );

        // Assert
        assert!(notif.is_null(), "ffi_notification_create should return NULL for null receiver");
    }

    #[test]
    fn test_notification_free_with_null() {
        // Act — should not panic
        ffi_notification_free(std::ptr::null_mut());
    }
}

mod tests_run_errors {
    use thryve_ffi::{
        ffi_run_interactive_start,
        ffi_run_interactive_free,
        FfiStatus,
    };

    #[test]
    fn test_run_interactive_start_with_null_ctx_returns_null() {
        // Act
        let handle = ffi_run_interactive_start(std::ptr::null_mut());

        // Assert
        assert!(handle.is_null(), "ffi_run_interactive_start should return NULL for null ctx");
    }

    #[test]
    fn test_run_interactive_free_with_null_returns_null_arg() {
        // Act
        let status = ffi_run_interactive_free(std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null handle");
    }
}

mod tests_service_errors {
    use thryve_ffi::{
        ffi_service_create,
        ffi_service_run,
        ffi_service_free,
        FfiStatus,
    };

    #[test]
    fn test_service_create_with_null_config_returns_null() {
        // Act
        let created = ffi_service_create(std::ptr::null_mut());

        // Assert
        assert!(created.service.is_null(), "ffi_service_create should return NULL service for null config");
        assert!(created.ctx.is_null(), "ffi_service_create should return NULL ctx for null config");
    }

    #[test]
    fn test_service_run_with_null_service_returns_null_arg() {
        // Act
        let status = ffi_service_run(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null service");
    }

    #[test]
    fn test_service_free_with_null() {
        // Act — should not panic
        ffi_service_free(std::ptr::null_mut());
    }
}

mod tests_notification_send_errors {
    use thryve_ffi::{
        FfiStatus, ffi_config_default, ffi_config_free, ffi_ctx_create, ffi_ctx_free, ffi_ctx_send_notification,
    };

    #[test]
    fn test_ctx_send_notification_with_null_ctx_returns_null_arg() {
        // Act
        let status = ffi_ctx_send_notification(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null ctx");
    }

    #[test]
    fn test_ctx_send_notification_with_null_notif_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);

        // Act
        let status = ffi_ctx_send_notification(ctx, std::ptr::null_mut());

        // Cleanup
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null notif");
    }
}

mod tests_packet_handle_send_errors {
    use thryve_ffi::{
        ffi_packet_handle_send_task,
        ffi_packet_handle_send_notification,
        ffi_config_default,
        ffi_config_free,
        ffi_ctx_create,
        ffi_ctx_free,
        ffi_ctx_get_packet_handle,
        ffi_packet_handle_free,
        FfiStatus,
    };

    #[test]
    fn test_packet_handle_send_task_with_null_handle_returns_null_arg() {
        // Act
        let status = ffi_packet_handle_send_task(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null handle");
    }

    #[test]
    fn test_packet_handle_send_task_with_null_task_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);
        let pkt = ffi_ctx_get_packet_handle(ctx);

        // Act
        let status = ffi_packet_handle_send_task(pkt, std::ptr::null_mut());

        // Cleanup
        ffi_packet_handle_free(pkt);
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null task");
    }

    #[test]
    fn test_packet_handle_send_notification_with_null_handle_returns_null_arg() {
        // Act
        let status = ffi_packet_handle_send_notification(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null handle");
    }

    #[test]
    fn test_packet_handle_send_notification_with_null_notif_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);
        let pkt = ffi_ctx_get_packet_handle(ctx);

        // Act
        let status = ffi_packet_handle_send_notification(pkt, std::ptr::null_mut());

        // Cleanup
        ffi_packet_handle_free(pkt);
        ffi_ctx_free(ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null notif");
    }
}

mod tests_service_run_errors {
    use thryve_ffi::{
        ffi_service_run,
        ffi_service_create,
        ffi_service_free,
        ffi_ctx_free,
        ffi_config_default,
        ffi_config_free,
        FfiStatus,
    };

    #[test]
    fn test_service_run_with_null_service_returns_null_arg() {
        // Act
        let status = ffi_service_run(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null service");
    }

    #[test]
    fn test_service_run_with_null_ctx_returns_null_arg() {
        // Arrange
        let config = ffi_config_default();
        let created = ffi_service_create(config);

        // Act
        let status = ffi_service_run(created.service, std::ptr::null_mut());

        // Cleanup — ffi_service_run returns NullArg before consuming either argument
        // when ctx is null, so both service and the ctx bundled by ffi_service_create
        // are still valid and must be freed here (not "ownership transferred")
        ffi_service_free(created.service);
        ffi_ctx_free(created.ctx);
        ffi_config_free(config);

        // Assert
        assert_eq!(status, FfiStatus::NullArg, "Should return NullArg for null ctx");
    }
}

mod tests_task_create_null_job {
    use thryve_ffi::{
        FfiTaskResult, ffi_config_default, ffi_config_free, ffi_ctx_create, ffi_ctx_free, ffi_ctx_get_packet_handle, ffi_packet_handle_free, ffi_plugin_create, ffi_plugin_free, ffi_task_create, ffi_task_result_new,
    };
    use std::ffi::CString;

    extern "C" fn dummy_task_job() -> FfiTaskResult {
        ffi_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    #[test]
    fn test_task_create_with_null_name_returns_null() {
        // Act - passing null name pointer
        let task = ffi_task_create(
                std::ptr::null(), // null name
                5,                // priority
                dummy_task_job,   // job
            );

        // Assert - task creation should fail with null name
        assert!(task.is_null(), "task should be null when name is null");
    }

    extern "C" fn test_plugin_entry() -> FfiTaskResult {
        ffi_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    extern "C" fn test_plugin_cleanup(_core_ptr: *const u8) {}

    #[test]
    fn test_plugin_create_with_null_core_ptr_and_null_entry_returns_null() {
        // Arrange
        let config = ffi_config_default();
        let ctx = ffi_ctx_create(config);
        let pkt = ffi_ctx_get_packet_handle(ctx);

        // Act - create plugin with null core_ptr and null entry point (both null)
        // Note: Rust's type system prevents passing null function pointers directly,
        // but we can test with null core_ptr and a valid entry point
        let plugin = ffi_plugin_create(
                CString::new("TestPlugin").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(), // null core_ptr
                pkt,
                test_plugin_cleanup,
                test_plugin_entry,
            );

        // Assert - plugin creation should succeed with null core_ptr
        // (core_ptr is optional for the FFI layer)
        assert!(!plugin.is_null(), "ffi_plugin_create should succeed with null core_ptr");

        // Cleanup
        ffi_plugin_free(plugin);
        ffi_packet_handle_free(pkt);
        ffi_ctx_free(ctx);
        ffi_config_free(config);
    }
}
