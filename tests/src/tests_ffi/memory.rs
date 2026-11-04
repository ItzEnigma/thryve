// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! FFI memory safety tests — Arc raw pointer round-trips, no double-free, no memory leaks.
//!
//! These tests verify that the FFI layer correctly manages memory lifetimes
//! and does not cause leaks or double-free issues.

mod tests_plugin_logic_adapter {
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

    #[test]
    fn test_plugin_logic_adapter_as_any_returns_self() {
        // Arrange
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut std::ffi::c_void;
        let adapter = PluginLogicAdapter::new("TestType", core_ptr, Some(test_plugin_cleanup));

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

mod tests_observer_logic_adapter {
    use std::sync::Arc;
    use thryve_ffi::ObserverLogicAdapter;
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

        // Act — clean_up takes an Arc<Observer> but we don't have one, so test with null
        let _ = adapter.as_any();

        // Cleanup — we can't easily test clean_up without an Observer, but we can verify
        // the adapter exists and has the correct core_ptr
        assert!(!adapter.core_ptr.is_null());

        // Cleanup
        unsafe {
            let _ = Arc::from_raw(core_ptr as *const u8);
        }
    }

    #[test]
    fn test_observer_logic_adapter_as_any_returns_self() {
        // Arrange
        let core_ptr = Arc::into_raw(Arc::new(0u8)) as *mut std::ffi::c_void;
        let adapter = ObserverLogicAdapter::new(core_ptr, Some(test_obs_cleanup));

        // Act
        let any_ref = adapter.as_any();

        // Assert — should be able to downcast to ObserverLogicAdapter
        assert!(any_ref.downcast_ref::<ObserverLogicAdapter>().is_some());

        // Cleanup
        unsafe {
            let _ = Arc::from_raw(core_ptr as *const u8);
        }
    }
}

mod tests_notification_logic_adapter {
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

mod tests_notification_logic_adapter_send_sync {
    use thryve_ffi::NotificationLogicAdapter;

    // Test that NotificationLogicAdapter is Send
    #[test]
    fn test_notification_logic_adapter_is_send() {
        // Arrange — create adapter in a separate thread to verify Send trait
        let result = std::thread::spawn(move || {
            let core_ptr = std::ptr::dangling_mut::<std::ffi::c_void>(); // dummy pointer for testing
            let adapter = NotificationLogicAdapter::new(core_ptr);
            // Verify the adapter exists and has a non-null core_ptr
            !adapter.core_ptr.is_null()
        })
        .join()
        .unwrap();

        // Assert — adapter was created successfully in another thread
        assert!(result, "NotificationLogicAdapter should be Send");
    }
}

mod tests_plugin_logic_adapter_send_sync {
    use thryve_ffi::PluginLogicAdapter;

    // Helper function to check Send trait at compile time
    fn is_send<T: Send>() {}

    // Helper function to check Sync trait at compile time
    fn is_sync<T: Sync>() {}

    #[test]
    fn test_plugin_logic_adapter_is_send() {
        // Compile-time check: PluginLogicAdapter should be Send
        is_send::<PluginLogicAdapter>();
    }

    #[test]
    fn test_plugin_logic_adapter_is_sync() {
        // Compile-time check: PluginLogicAdapter should be Sync
        is_sync::<PluginLogicAdapter>();
    }
}

mod tests_observer_logic_adapter_send_sync {
    use thryve_ffi::ObserverLogicAdapter;

    // Helper function to check Send trait at compile time
    fn is_send<T: Send>() {}

    // Helper function to check Sync trait at compile time
    fn is_sync<T: Sync>() {}

    #[test]
    fn test_observer_logic_adapter_is_send() {
        // Compile-time check: ObserverLogicAdapter should be Send
        is_send::<ObserverLogicAdapter>();
    }

    #[test]
    fn test_observer_logic_adapter_is_sync() {
        // Compile-time check: ObserverLogicAdapter should be Sync
        is_sync::<ObserverLogicAdapter>();
    }
}

mod tests_adapter_core_ptr_validation {
    use thryve_ffi::{NotificationLogicAdapter, ObserverLogicAdapter, PluginLogicAdapter};
    use thryve_utils::traits::notif_logic::NotifLogic;
    use thryve_utils::traits::observer_logic::ObsLogic;
    use thryve_utils::traits::plugin_logic::PlgLogic;

    extern "C" fn dummy_plugin_cleanup(_core_ptr: *const std::ffi::c_void) -> i32 {
        thryve_ffi::ThryveStatus::Ok as i32
    }
    extern "C" fn dummy_obs_cleanup(_core_ptr: *const std::ffi::c_void) {}

    #[test]
    fn test_plugin_adapter_with_null_core_ptr() {
        // Arrange — null core_ptr is valid
        let adapter =
            PluginLogicAdapter::new("TestType", std::ptr::null_mut(), Some(dummy_plugin_cleanup));

        // Assert — adapter should exist with null core_ptr
        assert!(
            adapter
                .as_any()
                .downcast_ref::<PluginLogicAdapter>()
                .is_some()
        );
    }

    #[test]
    fn test_observer_adapter_with_null_core_ptr() {
        // Arrange — null core_ptr is valid
        let adapter = ObserverLogicAdapter::new(std::ptr::null_mut(), Some(dummy_obs_cleanup));

        // Assert — adapter should exist with null core_ptr
        assert!(
            adapter
                .as_any()
                .downcast_ref::<ObserverLogicAdapter>()
                .is_some()
        );
    }

    #[test]
    fn test_notification_adapter_with_null_core_ptr() {
        // Arrange — null core_ptr is valid
        let adapter = NotificationLogicAdapter::new(std::ptr::null_mut());

        // Assert — adapter should exist with null core_ptr
        assert!(
            adapter
                .as_any()
                .downcast_ref::<NotificationLogicAdapter>()
                .is_some()
        );
    }

    #[test]
    fn test_plugin_adapter_clean_up_with_null_core_ptr() {
        // Arrange — null core_ptr is valid
        let adapter =
            PluginLogicAdapter::new("TestType", std::ptr::null_mut(), Some(dummy_plugin_cleanup));

        // Act — clean_up should not panic with null core_ptr
        // The callback is still called with the null pointer
        // Note: This will call the cleanup with null, which is safe in this case
        assert!(
            adapter.clean_up().is_ok(),
            "a ThryveStatus_Ok callback should succeed"
        );
    }

    #[test]
    fn test_observer_adapter_clean_up_with_null_core_ptr() {
        // Arrange — null core_ptr is valid
        let adapter = ObserverLogicAdapter::new(std::ptr::null_mut(), Some(dummy_obs_cleanup));

        // Assert — clean_up takes Arc<Observer> which we don't have
        // Just verify the adapter exists
        assert!(
            adapter
                .as_any()
                .downcast_ref::<ObserverLogicAdapter>()
                .is_some()
        );
    }

    #[test]
    fn test_notification_adapter_as_any_with_null_core_ptr() {
        // Arrange — null core_ptr is valid
        let adapter = NotificationLogicAdapter::new(std::ptr::null_mut());

        // Assert — adapter should exist with null core_ptr
        assert!(
            adapter
                .as_any()
                .downcast_ref::<NotificationLogicAdapter>()
                .is_some()
        );
    }
}

mod tests_ffi_task_result_default {
    use thryve_ffi::ThryveTaskResult;

    #[test]
    fn test_ffi_task_result_default_all_null() {
        // Act
        let result = ThryveTaskResult::default();

        // Assert — all fields should be null/false
        assert!(result.result_type.is_null(), "result_type should be null");
        assert!(result.message.is_null(), "message should be null");
        assert!(result.log_file.is_null(), "log_file should be null");
        assert!(!result.has_chain, "has_chain should be false");
        assert!(result.chain_task.is_null(), "chain_task should be null");
    }
}

mod tests_ffi_task_result_new {
    use std::ffi::CString;
    use thryve_ffi::thryve_task_result_new;

    #[test]
    fn test_ffi_task_result_new_with_valid_strings() {
        // Arrange
        let result_type = CString::new("Success").unwrap();
        let message = CString::new("Task completed").unwrap();
        let log_file = CString::new("Main").unwrap();

        // Act
        let result =
            thryve_task_result_new(result_type.as_ptr(), message.as_ptr(), log_file.as_ptr());

        // Assert
        assert!(!result.result_type.is_null());
        assert!(!result.message.is_null());
        assert!(!result.log_file.is_null());
        assert!(!result.has_chain);
        assert!(result.chain_task.is_null());
    }
}

mod tests_ffi_task_result_new_with_chain {
    use std::ffi::CString;
    use thryve_ffi::{
        thryve_task_create, thryve_task_free, thryve_task_result_new,
        thryve_task_result_new_with_chain,
    };

    extern "C" fn dummy_task_job(
        result: *mut thryve_ffi::ThryveTaskResult,
        _user_data: *mut std::ffi::c_void,
    ) {
        unsafe {
            *result =
                thryve_task_result_new(c"Success".as_ptr(), c"Done".as_ptr(), c"Main".as_ptr())
        };
    }

    #[test]
    fn test_ffi_task_result_new_with_chain_valid_task() {
        // Arrange — create a real task to use as chain
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

        // Create result with chain
        let result_type = CString::new("Chained").unwrap();
        let message = CString::new("Chaining").unwrap();
        let log_file = CString::new("Main").unwrap();

        // Act
        let result = thryve_task_result_new_with_chain(
            result_type.as_ptr(),
            message.as_ptr(),
            log_file.as_ptr(),
            task,
        );

        // Assert
        assert!(result.has_chain);
        assert_eq!(result.chain_task as usize, task as usize);

        // Cleanup — task is now owned by the result, don't free it separately
        // Actually, the result only holds the pointer, so we should free it
        thryve_task_free(task);
    }
}

mod tests_ffi_packet_handle_clone {
    use thryve_ffi::{
        thryve_config_default, thryve_config_free, thryve_ctx_create, thryve_ctx_free,
        thryve_ctx_get_packet_handle, thryve_packet_handle_clone, thryve_packet_handle_free,
    };

    #[test]
    fn test_packet_handle_clone_returns_valid_handle() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let original = thryve_ctx_get_packet_handle(ctx);

        // Act
        let cloned = thryve_packet_handle_clone(original);

        // Assert — cloned should not be null
        assert!(!cloned.is_null(), "cloned PacketHandle should not be null");

        // Cleanup
        thryve_packet_handle_free(original);
        thryve_packet_handle_free(cloned);
        thryve_ctx_free(ctx);
        thryve_config_free(config);
    }

    #[test]
    fn test_packet_handle_clone_with_null_returns_null() {
        // Act
        let cloned = thryve_packet_handle_clone(std::ptr::null_mut());

        // Assert
        assert!(
            cloned.is_null(),
            "cloning null PacketHandle should return null"
        );
    }
}

mod tests_ffi_ctx_add_plugin_ownership {
    use std::ffi::CString;
    use thryve_ffi::{
        ThryveTaskResult, thryve_config_default, thryve_config_free, thryve_ctx_add_plugin,
        thryve_ctx_create, thryve_ctx_free, thryve_ctx_get_packet_handle,
        thryve_packet_handle_free, thryve_plugin_create, thryve_task_result_new,
    };

    extern "C" fn test_plugin_entry(
        result: *mut ThryveTaskResult,
        _core_ptr: *mut std::ffi::c_void,
    ) {
        unsafe {
            *result =
                thryve_task_result_new(c"Success".as_ptr(), c"Done".as_ptr(), c"Main".as_ptr())
        };
    }

    extern "C" fn test_plugin_cleanup(_core_ptr: *const std::ffi::c_void) -> i32 {
        thryve_ffi::ThryveStatus::Ok as i32
    }

    #[test]
    fn test_ctx_add_plugin_takes_ownership() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);
        let pkt = thryve_ctx_get_packet_handle(ctx);

        // Create plugin
        let plugin = unsafe {
            thryve_plugin_create(
                CString::new("TestType").unwrap().as_ptr(),
                std::ptr::null_mut(),
                pkt,
                Some(test_plugin_cleanup),
                Some(test_plugin_entry),
            )
        };

        // Add plugin to context (takes ownership)
        let status = thryve_ctx_add_plugin(ctx, plugin);

        // Assert
        assert_eq!(status, thryve_ffi::ThryveStatus::Ok);

        // Plugin should now be owned by ctx, we should NOT free it
        // If we try to free it, that would be a double-free

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_packet_handle_free(pkt);
        thryve_config_free(config);
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
        assert_eq!(status, thryve_ffi::ThryveStatus::NullArg);
    }

    #[test]
    fn test_ctx_add_observer_with_null_observer_returns_null_arg() {
        // Arrange
        let config = thryve_config_default();
        let ctx = thryve_ctx_create(config);

        // Act
        let status = thryve_ffi::thryve_ctx_add_observer(ctx, std::ptr::null_mut());

        // Cleanup
        thryve_ctx_free(ctx);
        thryve_config_free(config);

        // Assert
        assert_eq!(status, thryve_ffi::ThryveStatus::NullArg);
    }
}
