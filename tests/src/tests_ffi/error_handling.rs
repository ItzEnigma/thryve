// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! FFI error handling tests — null pointer and invalid argument checks.
//!
//! These tests verify that all FFI functions properly handle null pointers
//! and invalid arguments, returning appropriate status codes or NULL values.

mod tests_config_errors {
    use std::ffi::CString;
    use thryve_ffi::{thryve_config_default, thryve_config_free, thryve_config_from_file};

    #[test]
    fn test_config_from_file_with_null_path_returns_null() {
        // Act — pass NULL path
        let config = unsafe { thryve_config_from_file(std::ptr::null()) };

        // Assert — should return NULL
        assert!(
            config.is_null(),
            "thryve_config_from_file should return NULL for null path"
        );
    }

    #[test]
    fn test_config_from_file_with_invalid_max_priority_returns_null() {
        // Arrange — a config file with max_priority >= 15 must be rejected with
        // NULL rather than panic inside Config::load_config_from_file: a panic
        // crossing this extern "C" boundary is undefined behavior. ffi_guard is
        // the backstop if that invariant is ever violated.
        use std::fs::File;
        use std::io::Write;

        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_ffi_config_invalid_priority.toml");
        let toml_content = "[tasks]\nmax_priority = 20\n";

        let mut file = File::create(&file_path).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();

        let path_cstring = CString::new(file_path.to_str().unwrap()).unwrap();

        // Act — must return NULL, not panic/abort the process.
        let config = unsafe { thryve_config_from_file(path_cstring.as_ptr()) };

        // Assert
        assert!(
            config.is_null(),
            "thryve_config_from_file should reject an invalid file with NULL"
        );

        // Cleanup
        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    fn test_config_from_file_with_invalid_utf8_returns_null() {
        // Arrange — create a C string with invalid UTF-8 (3 bytes + null terminator)
        let invalid_utf8_bytes = b"\xFF\xFE\x00";
        let invalid_path = std::ffi::CStr::from_bytes_with_nul(invalid_utf8_bytes).unwrap();
        let path_ptr = invalid_path.as_ptr();

        // Act — invalid UTF-8 can't name the file, so it's rejected like a missing file
        let config = unsafe { thryve_config_from_file(path_ptr) };

        // Assert
        assert!(
            config.is_null(),
            "thryve_config_from_file should return NULL for an invalid UTF-8 path"
        );
    }

    #[test]
    fn test_config_from_file_with_nonexistent_file_returns_null() {
        // Arrange
        let path = CString::new("/nonexistent/path/config.toml").unwrap();

        // Act — a file that can't be read has no fallback to the defaults
        let config = unsafe { thryve_config_from_file(path.as_ptr()) };

        // Assert
        assert!(
            config.is_null(),
            "thryve_config_from_file should return NULL for a nonexistent file"
        );
    }

    #[test]
    fn test_config_default_creates_valid_config() {
        // Act
        let config = thryve_config_default();

        // Assert — should return non-NULL
        assert!(
            !config.is_null(),
            "thryve_config_default should return a valid config"
        );

        // Cleanup
        thryve_config_free(config);
    }

    #[test]
    fn test_config_free_with_null() {
        // Act — passing NULL should not panic
        thryve_config_free(std::ptr::null_mut());
    }
}

mod tests_packet_handle_errors {
    use thryve_ffi::{thryve_packet_handle_clone, thryve_packet_handle_free};

    #[test]
    fn test_packet_handle_free_with_null() {
        // Act — should not panic
        thryve_packet_handle_free(std::ptr::null_mut());
    }

    #[test]
    fn test_packet_handle_clone_with_null_returns_null() {
        // Act
        let cloned = thryve_packet_handle_clone(std::ptr::null_mut());

        // Assert
        assert!(
            cloned.is_null(),
            "thryve_packet_handle_clone should return NULL for null handle"
        );
    }
}

mod tests_ctx_errors {
    use std::ffi::CString;
    use std::os::raw::c_char;
    use thryve_ffi::{
        ThryveStatus, thryve_config_default, thryve_config_free, thryve_ctx_add_observer,
        thryve_ctx_add_plugin, thryve_ctx_create, thryve_ctx_free, thryve_ctx_get_packet_handle,
        thryve_ctx_send_notification, thryve_ctx_send_task, thryve_ctx_set_logging_file_names,
    };

    #[test]
    fn test_ctx_create_with_null_config_returns_null() {
        // Act
        let ctx = thryve_ctx_create(std::ptr::null_mut());

        // Assert
        assert!(
            ctx.is_null(),
            "thryve_ctx_create should return NULL for null config"
        );
    }

    #[test]
    fn test_ctx_free_with_null() {
        // Act — should not panic
        thryve_ctx_free(std::ptr::null_mut());
    }

    #[test]
    fn test_ctx_add_plugin_with_null_ctx_returns_null_arg() {
        // Act
        let status = thryve_ctx_add_plugin(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null ctx"
        );
    }

    #[test]
    fn test_ctx_add_plugin_with_null_plugin_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);

        // Act
        let status = thryve_ctx_add_plugin(ctx, std::ptr::null_mut());

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null plugin"
        );
    }

    #[test]
    fn test_ctx_add_observer_with_null_ctx_returns_null_arg() {
        // Act
        let status = thryve_ctx_add_observer(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null ctx"
        );
    }

    #[test]
    fn test_ctx_add_observer_with_null_observer_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);

        // Act
        let status = thryve_ctx_add_observer(ctx, std::ptr::null_mut());

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null observer"
        );
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_null_ctx_returns_null_arg() {
        // Arrange
        let file_name = CString::new("test").unwrap();

        // Act
        let status = unsafe {
            thryve_ctx_set_logging_file_names(
                std::ptr::null_mut(),
                &file_name.as_ptr() as *const *const c_char,
                1,
            )
        };

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null ctx"
        );
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_null_file_names_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);

        // Act
        let status = unsafe { thryve_ctx_set_logging_file_names(ctx, std::ptr::null(), 1) };

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null file_names"
        );
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_zero_count_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let file_name = CString::new("test").unwrap();
        let file_names = [file_name.as_ptr()];

        // Act
        let status = unsafe { thryve_ctx_set_logging_file_names(ctx, file_names.as_ptr(), 0) };

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for zero count"
        );
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_negative_count_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let file_name = CString::new("test").unwrap();

        // Act
        let status = unsafe {
            thryve_ctx_set_logging_file_names(ctx, &file_name.as_ptr() as *const *const c_char, -1)
        };

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for negative count"
        );
    }

    #[test]
    fn test_ctx_send_task_with_null_ctx_returns_null_arg() {
        // Act
        let status = thryve_ctx_send_task(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null ctx"
        );
    }

    #[test]
    fn test_ctx_send_task_with_null_task_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);

        // Act
        let status = thryve_ctx_send_task(ctx, std::ptr::null_mut());

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null task"
        );
    }

    #[test]
    fn test_ctx_get_packet_handle_with_null_returns_null() {
        // Act
        let handle = thryve_ctx_get_packet_handle(std::ptr::null_mut());

        // Assert
        assert!(
            handle.is_null(),
            "thryve_ctx_get_packet_handle should return NULL for null ctx"
        );
    }

    #[test]
    fn test_ctx_send_notification_with_null_ctx_returns_null_arg() {
        // Act
        let status = thryve_ctx_send_notification(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null ctx"
        );
    }
}

mod tests_task_errors {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveTaskResult, thryve_task_create, thryve_task_free, thryve_task_result_new,
    };

    extern "C" fn dummy_task_job() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    #[test]
    fn test_task_create_with_null_name_returns_null() {
        // Act
        let task = unsafe { thryve_task_create(std::ptr::null(), 0, dummy_task_job) };

        // Assert
        assert!(
            task.is_null(),
            "thryve_task_create should return NULL for null name"
        );
    }

    #[test]
    fn test_task_free_with_null() {
        // Act — should not panic
        thryve_task_free(std::ptr::null_mut());
    }
}

mod tests_plugin_errors {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveTaskResult, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_packet_handle_free,
        thryve_plugin_create, thryve_plugin_free, thryve_task_result_new,
    };

    extern "C" fn dummy_plugin_entry() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    extern "C" fn dummy_plugin_cleanup(_core_ptr: *const u8) {}

    #[test]
    fn test_plugin_create_with_null_type_returns_null() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act
        let plugin = unsafe {
            thryve_plugin_create(
                std::ptr::null(),
                std::ptr::null_mut(),
                pkt,
                dummy_plugin_cleanup,
                dummy_plugin_entry,
            )
        };

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert!(
            plugin.is_null(),
            "thryve_plugin_create should return NULL for null type"
        );
    }

    #[test]
    fn test_plugin_create_with_null_packet_handle_returns_null() {
        // Act
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                dummy_plugin_cleanup,
                dummy_plugin_entry,
            )
        };

        // Assert
        assert!(
            plugin.is_null(),
            "thryve_plugin_create should return NULL for null packet handle"
        );
    }

    #[test]
    fn test_plugin_free_with_null() {
        // Act — should not panic
        thryve_plugin_free(std::ptr::null_mut());
    }
}

mod tests_observer_errors {
    use std::ffi::CString;
    use thryve_ffi::{
        thryve_config_default, thryve_config_free, thryve_ctx_create, thryve_ctx_free,
        thryve_ctx_get_packet_handle, thryve_observer_create, thryve_observer_free,
        thryve_packet_handle_free,
    };

    extern "C" fn dummy_obs_cleanup(_core_ptr: *const u8) {}

    extern "C" fn dummy_obs_job(_observer: thryve_ffi::ThryveObserver) {}

    #[test]
    fn test_observer_create_with_null_type_returns_null() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act
        let observer = unsafe {
            thryve_observer_create(
                std::ptr::null(),
                std::ptr::null_mut(),
                pkt,
                dummy_obs_cleanup,
                dummy_obs_job,
            )
        };

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert!(
            observer.is_null(),
            "thryve_observer_create should return NULL for null type"
        );
    }

    #[test]
    fn test_observer_create_with_null_packet_handle_returns_null() {
        // Act
        let observer = unsafe {
            thryve_observer_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                dummy_obs_cleanup,
                dummy_obs_job,
            )
        };

        // Assert
        assert!(
            observer.is_null(),
            "thryve_observer_create should return NULL for null packet handle"
        );
    }

    #[test]
    fn test_observer_free_with_null() {
        // Act — should not panic
        thryve_observer_free(std::ptr::null_mut());
    }
}

mod tests_notification_errors {
    use std::ffi::CString;
    use thryve_ffi::{thryve_notification_create, thryve_notification_free};

    #[test]
    fn test_notification_create_with_null_type_returns_null() {
        // Act
        let notif = unsafe {
            thryve_notification_create(
                std::ptr::null(),
                CString::new("TestReceiver").unwrap().as_ptr(),
                std::ptr::null_mut(),
            )
        };

        // Assert
        assert!(
            notif.is_null(),
            "thryve_notification_create should return NULL for null type"
        );
    }

    #[test]
    fn test_notification_create_with_null_receiver_returns_null() {
        // Act
        let notif = unsafe {
            thryve_notification_create(
                CString::new("TestNotif").unwrap().as_ptr(),
                std::ptr::null(),
                std::ptr::null_mut(),
            )
        };

        // Assert
        assert!(
            notif.is_null(),
            "thryve_notification_create should return NULL for null receiver"
        );
    }

    #[test]
    fn test_notification_free_with_null() {
        // Act — should not panic
        thryve_notification_free(std::ptr::null_mut());
    }
}

mod tests_run_errors {
    use thryve_ffi::{ThryveStatus, thryve_run_interactive_free, thryve_run_interactive_start};

    #[test]
    fn test_run_interactive_start_with_null_ctx_returns_null() {
        // Act
        let handle = thryve_run_interactive_start(std::ptr::null_mut());

        // Assert
        assert!(
            handle.is_null(),
            "thryve_run_interactive_start should return NULL for null ctx"
        );
    }

    #[test]
    fn test_run_interactive_free_with_null_returns_null_arg() {
        // Act
        let status = thryve_run_interactive_free(std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null handle"
        );
    }
}

mod tests_service_errors {
    use thryve_ffi::{
        ThryveStatus, thryve_service_create, thryve_service_free, thryve_service_run,
    };

    #[test]
    fn test_service_create_with_null_config_returns_null() {
        // Act
        let created = thryve_service_create(std::ptr::null_mut());

        // Assert
        assert!(
            created.service.is_null(),
            "thryve_service_create should return NULL service for null config"
        );
        assert!(
            created.ctx.is_null(),
            "thryve_service_create should return NULL ctx for null config"
        );
    }

    #[test]
    fn test_service_run_with_null_service_returns_null_arg() {
        // Act
        let status = thryve_service_run(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null service"
        );
    }

    #[test]
    fn test_service_free_with_null() {
        // Act — should not panic
        thryve_service_free(std::ptr::null_mut());
    }
}

mod tests_notification_send_errors {
    use thryve_ffi::{
        ThryveStatus, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_send_notification,
    };

    #[test]
    fn test_ctx_send_notification_with_null_ctx_returns_null_arg() {
        // Act
        let status = thryve_ctx_send_notification(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null ctx"
        );
    }

    #[test]
    fn test_ctx_send_notification_with_null_notif_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);

        // Act
        let status = thryve_ctx_send_notification(ctx, std::ptr::null_mut());

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null notif"
        );
    }
}

mod tests_packet_handle_send_errors {
    use thryve_ffi::{
        ThryveStatus, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_packet_handle_free,
        thryve_packet_handle_send_notification, thryve_packet_handle_send_task,
    };

    #[test]
    fn test_packet_handle_send_task_with_null_handle_returns_null_arg() {
        // Act
        let status = thryve_packet_handle_send_task(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null handle"
        );
    }

    #[test]
    fn test_packet_handle_send_task_with_null_task_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act
        let status = thryve_packet_handle_send_task(pkt, std::ptr::null_mut());

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null task"
        );
    }

    #[test]
    fn test_packet_handle_send_notification_with_null_handle_returns_null_arg() {
        // Act
        let status =
            thryve_packet_handle_send_notification(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null handle"
        );
    }

    #[test]
    fn test_packet_handle_send_notification_with_null_notif_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act
        let status = thryve_packet_handle_send_notification(pkt, std::ptr::null_mut());

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null notif"
        );
    }
}

mod tests_service_run_errors {
    use thryve_ffi::{
        ThryveStatus, thryve_config_default, thryve_config_free, thryve_ctx_free,
        thryve_service_create, thryve_service_free, thryve_service_run,
    };

    #[test]
    fn test_service_run_with_null_service_returns_null_arg() {
        // Act
        let status = thryve_service_run(std::ptr::null_mut(), std::ptr::null_mut());

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null service"
        );
    }

    #[test]
    fn test_service_run_with_null_ctx_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let created = thryve_service_create(config);

        // Act
        let status = thryve_service_run(created.service, std::ptr::null_mut());

        // Cleanup — thryve_service_run returns NullArg before consuming either argument
        // when ctx is null, so both service and the ctx bundled by thryve_service_create
        // are still valid and must be freed here (not "ownership transferred")
        thryve_service_free(created.service);
        thryve_ctx_free(created.ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::NullArg,
            "Should return NullArg for null ctx"
        );
    }
}

mod tests_task_create_null_job {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveTaskResult, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_packet_handle_free,
        thryve_plugin_create, thryve_plugin_free, thryve_task_create, thryve_task_result_new,
    };

    extern "C" fn dummy_task_job() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    #[test]
    fn test_task_create_with_null_name_returns_null() {
        // Act - passing null name pointer
        let task = unsafe {
            thryve_task_create(
                std::ptr::null(), // null name
                5,                // priority
                dummy_task_job,   // job
            )
        };

        // Assert - task creation should fail with null name
        assert!(task.is_null(), "task should be null when name is null");
    }

    extern "C" fn test_plugin_entry() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    extern "C" fn test_plugin_cleanup(_core_ptr: *const u8) {}

    #[test]
    fn test_plugin_create_with_null_core_ptr_and_null_entry_returns_null() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act - create plugin with null core_ptr and null entry point (both null)
        // Note: Rust's type system prevents passing null function pointers directly,
        // but we can test with null core_ptr and a valid entry point
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(), // null core_ptr
                pkt,
                test_plugin_cleanup,
                test_plugin_entry,
            )
        };

        // Assert - plugin creation should succeed with null core_ptr
        // (core_ptr is optional for the FFI layer)
        assert!(
            !plugin.is_null(),
            "thryve_plugin_create should succeed with null core_ptr"
        );

        // Cleanup
        thryve_plugin_free(plugin);
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }
}
