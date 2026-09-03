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
    fn test_ctx_set_logging_file_names_with_invalid_utf8() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        // Create a C string with invalid UTF-8 (3 bytes + null terminator)
        let invalid_utf8_bytes = b"\xFF\xFE\x00";
        let invalid_path = std::ffi::CStr::from_bytes_with_nul(invalid_utf8_bytes).unwrap();

        // Act
        let status = unsafe {
            thryve_ctx_set_logging_file_names(ctx, &invalid_path.as_ptr() as *const *const i8, 1)
        };

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert — should still succeed, just won't add the file
        assert_eq!(status, ThryveStatus::Ok);
    }
}

mod tests_ffi_task_create_success {
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
    fn test_task_create_with_valid_name() {
        // Arrange
        let name = CString::new("TestTask").unwrap();

        // Act
        let task = unsafe { thryve_task_create(name.as_ptr(), 5, dummy_task_job) };

        // Cleanup
        thryve_task_free(task);
    }

    #[test]
    fn test_task_create_with_different_priorities() {
        // Arrange
        let name = CString::new("TestTask").unwrap();

        // Act — create tasks with different priorities
        let task_low = unsafe { thryve_task_create(name.as_ptr(), 0, dummy_task_job) };
        thryve_task_free(task_low);

        let task_mid = unsafe { thryve_task_create(name.as_ptr(), 7, dummy_task_job) };
        thryve_task_free(task_mid);

        let task_high = unsafe { thryve_task_create(name.as_ptr(), 14, dummy_task_job) };
        thryve_task_free(task_high);
    }

    #[test]
    fn test_task_free_success() {
        // Arrange
        let name = CString::new("TestTask").unwrap();
        let task = unsafe { thryve_task_create(name.as_ptr(), 5, dummy_task_job) };

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

    extern "C" fn dummy_plugin_entry() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    extern "C" fn dummy_plugin_cleanup(_core_ptr: *const u8) {}

    #[test]
    fn test_plugin_create_with_valid_args() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Act
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestPlugin").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                dummy_plugin_cleanup,
                dummy_plugin_entry,
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
        let core_ptr = core_data.as_ptr() as *mut u8;

        // Act
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestPlugin").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                core_ptr,
                pkt,
                dummy_plugin_cleanup,
                dummy_plugin_entry,
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
                CString::new("TestPlugin").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                dummy_plugin_cleanup,
                dummy_plugin_entry,
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
        let core_ptr = core_data.as_ptr() as *mut u8;

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

    extern "C" fn dummy_obs_cleanup(_core_ptr: *const u8) {}
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
                CString::new("TestObserver").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                dummy_obs_cleanup,
                dummy_obs_job,
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
        let core_ptr = core_data.as_ptr() as *mut u8;

        // Act
        let observer = unsafe {
            thryve_observer_create(
                CString::new("TestObserver").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                core_ptr,
                pkt,
                dummy_obs_cleanup,
                dummy_obs_job,
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
                CString::new("TestObserver").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                dummy_obs_cleanup,
                dummy_obs_job,
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

    extern "C" fn dummy_entry() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
    }

    extern "C" fn dummy_cleanup(_core_ptr: *const u8) {}

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
                CString::new("TestPlugin").unwrap().as_ptr(),
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                dummy_cleanup,
                dummy_entry,
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
    fn test_service_run_returns_ok() {
        // Arrange
        let config = thryve_config_default();
        let created = thryve_service_create(config);

        // thryve_service_run internally calls init_panic_hook(), which installs a
        // process-wide panic hook that calls std::process::exit(1) on ANY
        // panic. That hook has no test-scoping — left in place, it silently
        // breaks every #[should_panic] test that runs afterward in this same
        // process (the whole binary aborts instead of the panic being
        // caught). Save and restore the hook around this call to keep that
        // side effect contained to this test.
        let prev_hook = std::panic::take_hook();

        // Act — service_run takes ownership of both service and ctx
        let status = thryve_service_run(created.service, created.ctx);

        std::panic::set_hook(prev_hook);

        // Cleanup — service_run already freed service and ctx
        thryve_config_free(config);

        // Assert
        assert_eq!(status, ThryveStatus::Ok);
    }
}

mod tests_ffi_ctx_send_task_success {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveStatus, ThryveTaskResult, thryve_config_default, thryve_config_free,
        thryve_ctx_create, thryve_ctx_free, thryve_ctx_get_packet_handle, thryve_ctx_send_task,
        thryve_packet_handle_free, thryve_task_create, thryve_task_result_new,
    };

    extern "C" fn dummy_task_job() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
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
                dummy_task_job,
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

    extern "C" fn dummy_task_job() -> ThryveTaskResult {
        thryve_task_result_new(
            CString::new("Success").unwrap().as_ptr(),
            CString::new("Done").unwrap().as_ptr(),
            CString::new("Main").unwrap().as_ptr(),
        )
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
                dummy_task_job,
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

    extern "C" fn test_plugin_cleanup(_core_ptr: *const u8) {
        unsafe {
            CLEANUP_CALLED = true;
        }
    }

    #[test]
    fn test_plugin_logic_adapter_clean_up_calls_callback() {
        // Arrange
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut u8;
        let adapter = PluginLogicAdapter::new(core_ptr, test_plugin_cleanup);

        // Reset the flag
        unsafe {
            CLEANUP_CALLED = false;
        }

        // Act
        adapter.clean_up();

        // Assert — cleanup callback should have been called
        unsafe {
            assert!(CLEANUP_CALLED, "clean_up should have called the callback");
        }

        // Cleanup: drop the core pointer
        unsafe {
            let _ = Arc::from_raw(core_ptr);
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

    extern "C" fn test_obs_cleanup(_core_ptr: *const u8) {
        unsafe {
            OBS_CLEANUP_CALLED = true;
        }
    }

    #[test]
    fn test_observer_logic_adapter_clean_up_calls_callback() {
        // Arrange
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut u8;
        let adapter = ObserverLogicAdapter::new(core_ptr, test_obs_cleanup);

        // Reset the flag
        unsafe {
            OBS_CLEANUP_CALLED = false;
        }

        // Create a mock observer to test clean_up
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run = ObsJob::new(move |_obs: Arc<Observer>| {});
        let observer = Arc::new(Observer::new(
            "test",
            "testType",
            obs_core,
            fn_to_run,
            PacketHandle::new(bounded(10).0, bounded(10).0, false),
        ));

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
            let _ = Arc::from_raw(core_ptr);
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

    extern "C" fn dummy_cleanup(_core_ptr: *const u8) {}

    #[test]
    fn test_plugin_logic_adapter_as_any_returns_self() {
        // Arrange
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut u8;
        let adapter = PluginLogicAdapter::new(core_ptr, dummy_cleanup);

        // Act
        let any_ref = adapter.as_any();

        // Assert — should be able to downcast to PluginLogicAdapter
        assert!(any_ref.downcast_ref::<PluginLogicAdapter>().is_some());

        // Cleanup
        unsafe {
            let _ = Arc::from_raw(core_ptr);
        }
    }
}

mod tests_ffi_observer_adapter_as_any {
    use thryve_ffi::ObserverLogicAdapter;
    use thryve_utils::traits::observer_logic::ObsLogic;

    extern "C" fn dummy_cleanup(_core_ptr: *const u8) {}

    #[test]
    fn test_observer_adapter_as_any_returns_self() {
        // Arrange
        let core_ptr = std::ptr::null_mut();
        let adapter = ObserverLogicAdapter::new(core_ptr, dummy_cleanup);

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
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut u8;
        let adapter = NotificationLogicAdapter::new(core_ptr);

        // Act
        let any_ref = adapter.as_any();

        // Assert — should be able to downcast to NotificationLogicAdapter
        assert!(any_ref.downcast_ref::<NotificationLogicAdapter>().is_some());

        // Cleanup
        unsafe {
            let _ = Arc::from_raw(core_ptr);
        }
    }
}
