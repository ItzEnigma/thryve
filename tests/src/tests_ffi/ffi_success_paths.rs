// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! FFI success path tests — covering functions not tested by error_handling or lifecycle tests.
//!
//! These tests verify the success paths of FFI functions that are not covered
//! by the existing error handling or lifecycle tests.
//!
//! Key pattern: Use thryve_ctx_get_packet_handle to get the engine's packet handle
//! which already has receivers attached, preventing channel send blocking.

mod tests_ffi_ctx_set_logging_file_names_success {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveStatus, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_set_logging_file_names,
    };

    #[test]
    fn test_ctx_set_logging_file_names_success() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let file1 = CString::new("Plugin1").unwrap();
        let file2 = CString::new("Plugin2").unwrap();
        let files = [file1.as_ptr(), file2.as_ptr()];

        // Act
        let status = unsafe { thryve_ctx_set_logging_file_names(ctx, files.as_ptr(), 2) };

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(status, ThryveStatus::Ok);
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_one_file() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let file = CString::new("SingleFile").unwrap();

        // Act
        let status = unsafe {
            thryve_ctx_set_logging_file_names(ctx, &file.as_ptr() as *const *const i8, 1)
        };

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(status, ThryveStatus::Ok);
    }

    #[test]
    fn test_ctx_set_logging_file_names_with_invalid_utf8_adds_none_of_them() {
        // Arrange — a valid name, then one that isn't UTF-8
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let invalid_utf8 = c"\xFF\xFE";
        let names = [c"ImgCompression".as_ptr(), invalid_utf8.as_ptr()];

        // Act
        let status = unsafe { thryve_ctx_set_logging_file_names(ctx, names.as_ptr(), 2) };
        let error = unsafe { std::ffi::CStr::from_ptr(thryve_ffi::thryve_last_error()) }
            .to_string_lossy()
            .into_owned();
        let added = unsafe { &*(ctx as *const thryve_utils::cfg::ctx::Ctx) }
            .logging_file_names
            .len();

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert — refused, and the valid name wasn't added either
        assert_eq!(status, ThryveStatus::StrConvError);
        assert_eq!(error, "file_names[1] isn't valid UTF-8");
        assert_eq!(added, 0);
    }
}

mod tests_ffi_task_create_success {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveTaskResult, thryve_task_create, thryve_task_free, thryve_task_result_new,
    };

    extern "C" fn dummy_task_job(result: *mut ThryveTaskResult, _user_data: *mut std::ffi::c_void) {
        unsafe {
            *result =
                thryve_task_result_new(c"Success".as_ptr(), c"Done".as_ptr(), c"Main".as_ptr())
        };
    }

    #[test]
    fn test_task_create_with_valid_name() {
        // Arrange
        let name = CString::new("TestTask").unwrap();

        // Act
        let task = unsafe {
            thryve_task_create(
                name.as_ptr(),
                5,
                Some(dummy_task_job),
                std::ptr::null_mut(),
                None,
            )
        };

        // Cleanup
        thryve_task_free(task);
    }

    #[test]
    fn test_task_create_with_different_priorities() {
        // Arrange
        let name = CString::new("TestTask").unwrap();

        // Act — create tasks with different priorities
        let task_low = unsafe {
            thryve_task_create(
                name.as_ptr(),
                0,
                Some(dummy_task_job),
                std::ptr::null_mut(),
                None,
            )
        };
        thryve_task_free(task_low);

        let task_mid = unsafe {
            thryve_task_create(
                name.as_ptr(),
                7,
                Some(dummy_task_job),
                std::ptr::null_mut(),
                None,
            )
        };
        thryve_task_free(task_mid);

        let task_high = unsafe {
            thryve_task_create(
                name.as_ptr(),
                14,
                Some(dummy_task_job),
                std::ptr::null_mut(),
                None,
            )
        };
        thryve_task_free(task_high);
    }

    #[test]
    fn test_task_free_success() {
        // Arrange
        let name = CString::new("TestTask").unwrap();
        let task = unsafe {
            thryve_task_create(
                name.as_ptr(),
                5,
                Some(dummy_task_job),
                std::ptr::null_mut(),
                None,
            )
        };

        // Act — free the task
        thryve_task_free(task);
    }
}

mod tests_ffi_plugin_create_success {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveTaskResult, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_packet_handle_free,
        thryve_plugin_create, thryve_plugin_free, thryve_task_result_new,
    };

    extern "C" fn dummy_plugin_entry(
        result: *mut ThryveTaskResult,
        _core_ptr: *mut std::ffi::c_void,
    ) {
        unsafe {
            *result =
                thryve_task_result_new(c"Success".as_ptr(), c"Done".as_ptr(), c"Main".as_ptr())
        };
    }

    extern "C" fn dummy_plugin_cleanup(_core_ptr: *const std::ffi::c_void) -> i32 {
        thryve_ffi::ThryveStatus::Ok as i32
    }

    #[test]
    fn test_plugin_create_with_valid_args() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                Some(dummy_plugin_cleanup),
                Some(dummy_plugin_entry),
            )
        };

        // Assert
        assert!(!plugin.is_null(), "plugin should not be null");

        // Cleanup
        thryve_plugin_free(plugin);
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_plugin_create_with_core_ptr() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);
        let core_data = CString::new("CoreData").unwrap();
        let core_ptr = core_data.as_ptr() as *mut std::ffi::c_void;

        // Act
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                core_ptr,
                pkt,
                Some(dummy_plugin_cleanup),
                Some(dummy_plugin_entry),
            )
        };

        // Assert
        assert!(!plugin.is_null(), "plugin should not be null");

        // Cleanup
        thryve_plugin_free(plugin);
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
        // core_data is still valid - don't free it
    }

    #[test]
    fn test_plugin_free_success() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                Some(dummy_plugin_cleanup),
                Some(dummy_plugin_entry),
            )
        };

        // Act
        thryve_plugin_free(plugin);

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }
}

mod tests_ffi_notification_create_success {
    use std::ffi::CString;
    use thryve_ffi::{thryve_notification_create, thryve_notification_free};

    #[test]
    fn test_notification_create_with_valid_args() {
        // Arrange
        let notif_type = CString::new("TestNotifType").unwrap();
        let notif_receiver = CString::new("TestReceiver").unwrap();

        // Act
        let notif = unsafe {
            thryve_notification_create(
                notif_type.as_ptr(),
                notif_receiver.as_ptr(),
                std::ptr::null_mut(),
            )
        };

        // Assert
        assert!(!notif.is_null(), "notification should not be null");

        // Cleanup
        thryve_notification_free(notif);
    }

    #[test]
    fn test_notification_create_with_core_ptr() {
        // Arrange
        let notif_type = CString::new("TestNotifType").unwrap();
        let notif_receiver = CString::new("TestReceiver").unwrap();
        let core_data = CString::new("CoreData").unwrap();
        let core_ptr = core_data.as_ptr() as *mut std::ffi::c_void;

        // Act
        let notif = unsafe {
            thryve_notification_create(notif_type.as_ptr(), notif_receiver.as_ptr(), core_ptr)
        };

        // Assert
        assert!(!notif.is_null(), "notification should not be null");

        // Cleanup
        thryve_notification_free(notif);
        // core_data is still valid - don't free it
    }

    #[test]
    fn test_notification_free_success() {
        // Arrange
        let notif_type = CString::new("TestNotifType").unwrap();
        let notif_receiver = CString::new("TestReceiver").unwrap();

        let notif = unsafe {
            thryve_notification_create(
                notif_type.as_ptr(),
                notif_receiver.as_ptr(),
                std::ptr::null_mut(),
            )
        };

        // Act
        thryve_notification_free(notif);
    }
}

mod tests_ffi_observer_create_success {
    use std::ffi::CString;
    use thryve_ffi::{
        thryve_config_default, thryve_config_free, thryve_ctx_create, thryve_ctx_free,
        thryve_ctx_get_packet_handle, thryve_observer_create, thryve_observer_free,
        thryve_packet_handle_free,
    };

    extern "C" fn dummy_obs_cleanup(_core_ptr: *const std::ffi::c_void) {}
    extern "C" fn dummy_obs_job(_observer: thryve_ffi::ThryveObserver) {}

    #[test]
    fn test_observer_create_with_valid_args() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act
        let observer = unsafe {
            thryve_observer_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                Some(dummy_obs_cleanup),
                Some(dummy_obs_job),
            )
        };

        // Assert
        assert!(!observer.is_null(), "observer should not be null");

        // Cleanup
        thryve_observer_free(observer);
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_observer_create_with_core_ptr() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);
        let core_data = CString::new("CoreData").unwrap();
        let core_ptr = core_data.as_ptr() as *mut std::ffi::c_void;

        // Act
        let observer = unsafe {
            thryve_observer_create(
                CString::new("TestType").unwrap().as_ptr(),
                core_ptr,
                pkt,
                Some(dummy_obs_cleanup),
                Some(dummy_obs_job),
            )
        };

        // Assert
        assert!(!observer.is_null(), "observer should not be null");

        // Cleanup
        thryve_observer_free(observer);
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
        // core_data is still valid - don't free it
    }

    #[test]
    fn test_observer_free_success() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        let observer = unsafe {
            thryve_observer_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                Some(dummy_obs_cleanup),
                Some(dummy_obs_job),
            )
        };

        // Act
        thryve_observer_free(observer);

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }
}

mod tests_ffi_service_create_success {
    use thryve_ffi::{
        thryve_config_default, thryve_config_free, thryve_ctx_free, thryve_service_create,
        thryve_service_free,
    };

    #[test]
    fn test_service_create_with_valid_config() {
        // Arrange
        let config = thryve_config_default();

        // Act — returns the service bundled with its matching ctx
        let created = thryve_service_create(config);

        // Cleanup
        thryve_service_free(created.service);
        thryve_ctx_free(created.ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_service_free_success() {
        // Arrange
        let config = thryve_config_default();
        let created = thryve_service_create(config);

        // Act
        thryve_service_free(created.service);

        // Cleanup
        thryve_ctx_free(created.ctx);
        thryve_config_free(config);
    }
}

mod tests_ffi_service_create_ctx_wiring {
    // These verify the ctx bundled by thryve_service_create is a fully-wired, usable
    // Ctx — not a placeholder — behaving exactly like one from thryve_ctx_create.
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveStatus, ThryveTaskResult, thryve_config_default, thryve_config_free,
        thryve_ctx_add_plugin, thryve_ctx_free, thryve_ctx_get_packet_handle,
        thryve_packet_handle_free, thryve_plugin_create, thryve_service_create,
        thryve_service_free, thryve_task_result_new,
    };

    extern "C" fn dummy_entry(result: *mut ThryveTaskResult, _core_ptr: *mut std::ffi::c_void) {
        unsafe {
            *result =
                thryve_task_result_new(c"Success".as_ptr(), c"Done".as_ptr(), c"Main".as_ptr())
        };
    }

    extern "C" fn dummy_cleanup(_core_ptr: *const std::ffi::c_void) -> i32 {
        thryve_ffi::ThryveStatus::Ok as i32
    }

    #[test]
    fn test_service_create_ctx_has_connected_packet_handle() {
        // Arrange
        let config = thryve_config_default();
        let created = thryve_service_create(config);

        // Act
        let pkt = thryve_ctx_get_packet_handle(created.ctx);

        // Assert
        assert!(
            !pkt.is_null(),
            "ctx returned by thryve_service_create should have a connected packet handle"
        );

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_service_free(created.service);
        thryve_ctx_free(created.ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_service_create_ctx_accepts_plugin() {
        // Arrange
        let config = thryve_config_default();
        let created = thryve_service_create(config);
        let pkt = thryve_ctx_get_packet_handle(created.ctx);

        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                Some(dummy_cleanup),
                Some(dummy_entry),
            )
        };

        // Act
        let status = thryve_ctx_add_plugin(created.ctx, plugin);

        // Assert
        assert_eq!(
            status,
            ThryveStatus::Ok,
            "ctx returned by thryve_service_create should accept plugins like any other ctx"
        );

        // Cleanup — plugin now owned by ctx, don't free it separately
        thryve_packet_handle_free(pkt);
        thryve_service_free(created.service);
        thryve_ctx_free(created.ctx);
        thryve_config_free(config);
    }
}

mod tests_ffi_service_run_success {
    use thryve_ffi::{
        ThryveStatus, thryve_config_default, thryve_config_free, thryve_service_create,
        thryve_service_run,
    };

    #[test]
    fn test_service_run_outside_service_manager_returns_invalid_state() {
        // Arrange
        let config = thryve_config_default();
        let created = thryve_service_create(config);

        // thryve_service_run starts the engine before it connects to the service
        // manager, and that startup installs a process-wide panic hook that calls
        // std::process::exit(1) on ANY panic. That hook has no test-scoping — left
        // in place, it silently breaks every #[should_panic] test that runs
        // afterward in this same process (the whole binary aborts instead of the
        // panic being caught). Save and restore the hook around this call to keep
        // that side effect contained to this test.
        let prev_hook = std::panic::take_hook();

        // Act — service_run takes ownership of both service and ctx. A test
        // process isn't started by the Windows service manager, so connecting to
        // it fails, and the engine that was already started is stopped again.
        let status = thryve_service_run(created.service, created.ctx);

        std::panic::set_hook(prev_hook);

        // Cleanup — service_run already freed service and ctx
        thryve_config_free(config);

        // Assert
        assert_eq!(status, ThryveStatus::InvalidState);
    }
}

mod tests_ffi_ctx_send_task_success {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveStatus, ThryveTaskResult, thryve_config_default, thryve_config_free,
        thryve_ctx_create, thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_ctx_send_task,
        thryve_packet_handle_free, thryve_task_create, thryve_task_result_new,
    };

    extern "C" fn dummy_task_job(result: *mut ThryveTaskResult, _user_data: *mut std::ffi::c_void) {
        unsafe {
            *result =
                thryve_task_result_new(c"Success".as_ptr(), c"Done".as_ptr(), c"Main".as_ptr())
        };
    }

    #[test]
    fn test_ctx_send_task_success() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);
        let task = unsafe {
            thryve_task_create(
                CString::new("TestTask").unwrap().as_ptr(),
                5,
                Some(dummy_task_job),
                std::ptr::null_mut(),
                None,
            )
        };

        // Act — send task (this may or may not succeed depending on whether
        // the engine is running, but the function should return Ok)
        let status = thryve_ctx_send_task(ctx, task);

        // Cleanup — ctx_send_task takes ownership of task
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(status, ThryveStatus::Ok);
    }
}

mod tests_ffi_ctx_send_notification_success {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveStatus, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_ctx_send_notification,
        thryve_notification_create, thryve_packet_handle_free,
    };

    #[test]
    fn test_ctx_send_notification_success() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);
        let notif = unsafe {
            thryve_notification_create(
                CString::new("TestNotif").unwrap().as_ptr(),
                CString::new("TestReceiver").unwrap().as_ptr(),
                std::ptr::null_mut(),
            )
        };

        // Act — send notification (this may or may not succeed depending on whether
        // the engine is running, but the function should return Ok)
        let status = thryve_ctx_send_notification(ctx, notif);

        // Cleanup — ctx_send_notification takes ownership of notif
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(status, ThryveStatus::Ok);
    }
}

mod tests_ffi_packet_handle_send_task_success {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveStatus, ThryveTaskResult, thryve_config_default, thryve_config_free,
        thryve_ctx_create, thryve_ctx_free, thryve_ctx_get_packet_handle,
        thryve_packet_handle_free, thryve_packet_handle_send_task, thryve_task_create,
        thryve_task_result_new,
    };

    extern "C" fn dummy_task_job(result: *mut ThryveTaskResult, _user_data: *mut std::ffi::c_void) {
        unsafe {
            *result =
                thryve_task_result_new(c"Success".as_ptr(), c"Done".as_ptr(), c"Main".as_ptr())
        };
    }

    #[test]
    fn test_packet_handle_send_task_success() {
        // Arrange - use engine's packet handle which has receivers attached
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);
        let task = unsafe {
            thryve_task_create(
                CString::new("TestTask").unwrap().as_ptr(),
                5,
                Some(dummy_task_job),
                std::ptr::null_mut(),
                None,
            )
        };

        // Act — send task via packet handle
        let status = thryve_packet_handle_send_task(pkt, task);

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(status, ThryveStatus::Ok);
    }
}

mod tests_ffi_packet_handle_send_notification_success {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveStatus, thryve_config_default, thryve_config_free, thryve_ctx_create,
        thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_notification_create,
        thryve_packet_handle_free, thryve_packet_handle_send_notification,
    };

    #[test]
    fn test_packet_handle_send_notification_success() {
        // Arrange - use engine's packet handle which has receivers attached
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);
        let notif = unsafe {
            thryve_notification_create(
                CString::new("TestNotif").unwrap().as_ptr(),
                CString::new("TestReceiver").unwrap().as_ptr(),
                std::ptr::null_mut(),
            )
        };

        // Act — send notification via packet handle
        let status = thryve_packet_handle_send_notification(pkt, notif);

        // Cleanup
        thryve_packet_handle_free(pkt);
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(status, ThryveStatus::Ok);
    }
}

mod tests_ffi_plugin_clean_up_success {
    use std::sync::Arc;
    use thryve_ffi::PluginLogicAdapter;
    use thryve_utils::traits::plugin_logic::PlgLogic;

    // Track if cleanup was called
    static mut CLEANUP_CALLED: bool = false;

    extern "C" fn test_plugin_cleanup(_core_ptr: *const std::ffi::c_void) -> i32 {
        unsafe {
            CLEANUP_CALLED = true;
        }
        thryve_ffi::ThryveStatus::Ok as i32
    }

    #[test]
    fn test_plugin_logic_adapter_clean_up_calls_callback() {
        // Arrange
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut std::ffi::c_void;
        let adapter = PluginLogicAdapter::new("TestType", core_ptr, Some(test_plugin_cleanup));

        // Reset the flag
        unsafe {
            CLEANUP_CALLED = false;
        }

        // Act
        assert!(
            adapter.clean_up().is_ok(),
            "a ThryveStatus_Ok callback should succeed"
        );

        // Assert — cleanup callback should have been called
        unsafe {
            assert!(CLEANUP_CALLED, "clean_up should have called the callback");
        }

        // Cleanup: drop the core pointer
        unsafe {
            let _ = Arc::from_raw(core_ptr as *const u8);
        }
    }
}

mod tests_ffi_observer_clean_up_success {
    use crossbeam::channel::bounded;
    use std::sync::Arc;
    use thryve_ffi::ObserverLogicAdapter;
    use thryve_utils::bases::observer::Observer;
    use thryve_utils::extras::jobs::ObsJob;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::traits::observer_logic::ObsLogic;

    // Track if cleanup was called
    static mut OBS_CLEANUP_CALLED: bool = false;

    extern "C" fn test_obs_cleanup(_core_ptr: *const std::ffi::c_void) {
        unsafe {
            OBS_CLEANUP_CALLED = true;
        }
    }

    #[test]
    fn test_observer_logic_adapter_clean_up_calls_callback() {
        // Arrange
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut std::ffi::c_void;
        let adapter = ObserverLogicAdapter::new(core_ptr, Some(test_obs_cleanup));

        // Reset the flag
        unsafe {
            OBS_CLEANUP_CALLED = false;
        }

        // Create a mock observer to test clean_up
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Arc::new(
            Observer::new(
                "testType",
                obs_core,
                obs_job,
                PacketHandle::new(
                    bounded(10).0,
                    bounded(10).0,
                    crossbeam::channel::unbounded().0,
                    false,
                ),
            )
            .unwrap(),
        );

        // Act
        adapter.clean_up(observer);

        // Assert — cleanup callback should have been called
        unsafe {
            assert!(
                OBS_CLEANUP_CALLED,
                "clean_up should have called the callback"
            );
        }

        // Cleanup
        unsafe {
            let _ = Arc::from_raw(core_ptr as *const u8);
        }
    }

    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        fn clean_up(&self, _observer: Arc<Observer>) {}
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
}

mod tests_ffi_plugin_logic_adapter_as_any {
    use std::sync::Arc;
    use thryve_ffi::PluginLogicAdapter;
    use thryve_utils::traits::plugin_logic::PlgLogic;

    extern "C" fn dummy_cleanup(_core_ptr: *const std::ffi::c_void) -> i32 {
        thryve_ffi::ThryveStatus::Ok as i32
    }

    #[test]
    fn test_plugin_logic_adapter_as_any_returns_self() {
        // Arrange
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut std::ffi::c_void;
        let adapter = PluginLogicAdapter::new("TestType", core_ptr, Some(dummy_cleanup));

        // Act
        let any_ref = adapter.as_any();

        // Assert — should be able to downcast to PluginLogicAdapter
        assert!(any_ref.downcast_ref::<PluginLogicAdapter>().is_some());

        // Cleanup
        unsafe {
            let _ = Arc::from_raw(core_ptr as *const u8);
        }
    }
}

mod tests_ffi_observer_adapter_as_any {
    use thryve_ffi::ObserverLogicAdapter;
    use thryve_utils::traits::observer_logic::ObsLogic;

    extern "C" fn dummy_cleanup(_core_ptr: *const std::ffi::c_void) {}

    #[test]
    fn test_observer_adapter_as_any_returns_self() {
        // Arrange
        let core_ptr = std::ptr::null_mut();
        let adapter = ObserverLogicAdapter::new(core_ptr, Some(dummy_cleanup));

        // Act
        let any_ref = adapter.as_any();

        // Assert — should be able to downcast to ObserverLogicAdapter
        assert!(any_ref.downcast_ref::<ObserverLogicAdapter>().is_some());
    }
}

mod tests_ffi_notification_adapter_as_any {
    use std::sync::Arc;
    use thryve_ffi::NotificationLogicAdapter;
    use thryve_utils::traits::notif_logic::NotifLogic;

    #[test]
    fn test_notification_logic_adapter_as_any_returns_self() {
        // Arrange
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut std::ffi::c_void;
        let adapter = NotificationLogicAdapter::new(core_ptr);

        // Act
        let any_ref = adapter.as_any();

        // Assert — should be able to downcast to NotificationLogicAdapter
        assert!(any_ref.downcast_ref::<NotificationLogicAdapter>().is_some());

        // Cleanup
        unsafe {
            let _ = Arc::from_raw(core_ptr as *const u8);
        }
    }
}
