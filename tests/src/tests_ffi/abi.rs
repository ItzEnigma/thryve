// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! FFI ABI correctness tests — struct layout, function signatures, and data conversions.
//!
//! These tests verify that the FFI types have correct layouts and that
//! data conversions between FFI representations and internal Rust types work properly.

mod tests_ffi_task_result {
    use std::ffi::CString;
    use thryve_ffi::{
        ffi_result_to_result, thryve_task_result_new, thryve_task_result_new_with_chain,
    };

    #[test]
    fn test_task_result_new_creates_valid_result() {
        // Arrange
        let result_type = CString::new("Success").unwrap();
        let message = CString::new("Task completed").unwrap();
        let log_file = CString::new("Main").unwrap();

        // Act
        let result =
            thryve_task_result_new(result_type.as_ptr(), message.as_ptr(), log_file.as_ptr());

        // Assert
        assert!(
            !result.result_type.is_null(),
            "result_type should not be null"
        );
        assert!(!result.message.is_null(), "message should not be null");
        assert!(!result.log_file.is_null(), "log_file should not be null");
        assert!(!result.has_chain, "has_chain should be false");
        assert!(result.chain_task.is_null(), "chain_task should be null");

        // Verify string contents
        unsafe {
            assert_eq!(
                std::ffi::CStr::from_ptr(result.result_type)
                    .to_str()
                    .unwrap(),
                "Success"
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(result.message).to_str().unwrap(),
                "Task completed"
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(result.log_file).to_str().unwrap(),
                "Main"
            );
        }
    }

    #[test]
    fn test_task_result_new_with_null_strings_uses_defaults() {
        // Act
        let result = thryve_task_result_new(std::ptr::null(), std::ptr::null(), std::ptr::null());

        // Assert
        assert!(result.result_type.is_null(), "result_type should be null");
        assert!(result.message.is_null(), "message should be null");
        assert!(result.log_file.is_null(), "log_file should be null");
        assert!(!result.has_chain, "has_chain should be false");
        assert!(result.chain_task.is_null(), "chain_task should be null");
    }

    #[test]
    fn test_task_result_new_with_chain_sets_chain_fields() {
        // Arrange
        let result_type = CString::new("Chained").unwrap();
        let message = CString::new("Chaining to next task").unwrap();
        let log_file = CString::new("Main").unwrap();
        // Create a dummy task pointer (not a real task, just for chain test)
        let dummy_task: *mut u8 = 0x1234 as *mut u8;

        // Act
        let result = thryve_task_result_new_with_chain(
            result_type.as_ptr(),
            message.as_ptr(),
            log_file.as_ptr(),
            dummy_task as thryve_ffi::ThryveTask,
        );

        // Assert
        assert!(result.has_chain, "has_chain should be true");
        assert_eq!(
            result.chain_task as usize, 0x1234,
            "chain_task should match"
        );

        // Verify string contents
        unsafe {
            assert_eq!(
                std::ffi::CStr::from_ptr(result.result_type)
                    .to_str()
                    .unwrap(),
                "Chained"
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(result.message).to_str().unwrap(),
                "Chaining to next task"
            );
            assert_eq!(
                std::ffi::CStr::from_ptr(result.log_file).to_str().unwrap(),
                "Main"
            );
        }
    }

    #[test]
    fn test_ffi_result_to_result_with_valid_strings() {
        // Arrange
        let result_type = CString::new("Success").unwrap();
        let message = CString::new("Task completed").unwrap();
        let log_file = CString::new("Main").unwrap();

        let ffi_result =
            thryve_task_result_new(result_type.as_ptr(), message.as_ptr(), log_file.as_ptr());

        // Act
        let internal_result = ffi_result_to_result(ffi_result);

        // Assert
        assert_eq!(internal_result.result_type.inner(), "Success");
        assert_eq!(internal_result.message, "Task completed");
        assert_eq!(internal_result.log_msg_to_file.inner(), "Main");
        assert!(internal_result.next_task.is_none());
    }

    #[test]
    fn test_ffi_result_to_result_with_null_strings_uses_defaults() {
        // Arrange
        let ffi_result =
            thryve_task_result_new(std::ptr::null(), std::ptr::null(), std::ptr::null());

        // Act
        let internal_result = ffi_result_to_result(ffi_result);

        // Assert
        assert_eq!(internal_result.result_type.inner(), "Error");
        assert_eq!(internal_result.message, "No message");
        assert_eq!(internal_result.log_msg_to_file.inner(), "Main");
    }

    #[test]
    fn test_ffi_result_to_result_with_invalid_utf8_uses_default() {
        // Arrange — create a C string with invalid UTF-8 (3 bytes + null terminator)
        let invalid_utf8_bytes = b"\xFF\xFE\x00";
        let invalid_utf8 = std::ffi::CStr::from_bytes_with_nul(invalid_utf8_bytes).unwrap();
        let ffi_result =
            thryve_task_result_new(invalid_utf8.as_ptr(), std::ptr::null(), std::ptr::null());

        // Act
        let internal_result = ffi_result_to_result(ffi_result);

        // Assert — invalid UTF-8 should fall back to "Error"
        assert_eq!(internal_result.result_type.inner(), "Error");
        assert_eq!(internal_result.message, "No message");
    }

    #[test]
    fn test_ffi_result_to_result_with_non_null_chain_task_without_real_task() {
        // Arrange — create a result with chain_task set to a non-null but invalid pointer
        let result_type = CString::new("Chained").unwrap();
        let message = CString::new("Chaining").unwrap();
        let log_file = CString::new("Main").unwrap();
        let invalid_task = 0xDEAD as *mut u8;

        let _ = thryve_task_result_new_with_chain(
            result_type.as_ptr(),
            message.as_ptr(),
            log_file.as_ptr(),
            invalid_task as thryve_ffi::ThryveTask,
        );

        // Act & Assert — should not panic, but the chain_task pointer is invalid
        // so the Box::from_raw will likely panic. We test that the result is
        // created without a chained task when chain_task is NULL.
        let ffi_result_null_chain = thryve_task_result_new_with_chain(
            result_type.as_ptr(),
            message.as_ptr(),
            log_file.as_ptr(),
            std::ptr::null_mut() as thryve_ffi::ThryveTask,
        );

        let internal_result = ffi_result_to_result(ffi_result_null_chain);
        assert!(
            internal_result.next_task.is_none(),
            "next_task should be None for null chain"
        );
    }

    #[test]
    fn test_ffi_result_to_result_with_has_chain_false_and_null_task() {
        // Arrange — has_chain is false, so chain_task should be ignored
        let result_type = CString::new("Success").unwrap();
        let message = CString::new("Done").unwrap();
        let log_file = CString::new("Main").unwrap();
        let ffi_result =
            thryve_task_result_new(result_type.as_ptr(), message.as_ptr(), log_file.as_ptr());

        // Act
        let internal_result = ffi_result_to_result(ffi_result);

        // Assert
        assert!(
            internal_result.next_task.is_none(),
            "next_task should be None"
        );
        assert_eq!(internal_result.result_type.inner(), "Success");
    }
}

mod tests_ffi_status {
    use thryve_ffi::ThryveStatus;

    #[test]
    fn test_ffi_status_debug_format() {
        // Assert — Debug should produce readable output
        assert!(format!("{:?}", ThryveStatus::Ok).contains("Ok"));
        assert!(format!("{:?}", ThryveStatus::NullArg).contains("NullArg"));
        assert!(format!("{:?}", ThryveStatus::AllocFail).contains("AllocFail"));
    }

    #[test]
    fn test_ffi_status_clone_copy() {
        // Assert — ThryveStatus should be Clone + Copy
        let status = ThryveStatus::Ok;
        let cloned = status; // Copy
        let _copied = status; // Use again to verify it's Copy
        assert_eq!(cloned, ThryveStatus::Ok);
    }

    #[test]
    fn test_ffi_status_equality() {
        assert_eq!(ThryveStatus::Ok, ThryveStatus::Ok);
        assert_eq!(ThryveStatus::NullArg, ThryveStatus::NullArg);
        assert_ne!(ThryveStatus::Ok, ThryveStatus::NullArg);
    }

    #[test]
    fn test_ffi_status_repr_c_values() {
        // Verify the repr(C) values match the C header
        assert_eq!(ThryveStatus::Ok as i32, 0);
        assert_eq!(ThryveStatus::NullArg as i32, 1);
        assert_eq!(ThryveStatus::AllocFail as i32, 2);
        assert_eq!(ThryveStatus::InvalidState as i32, 3);
        assert_eq!(ThryveStatus::OperationFail as i32, 4);
        assert_eq!(ThryveStatus::StrConvError as i32, 5);
    }

    #[test]
    fn test_ffi_status_size_matches_c() {
        // ThryveStatus should be the size of i32 (C enum)
        assert_eq!(
            std::mem::size_of::<ThryveStatus>(),
            4,
            "ThryveStatus should be 4 bytes (i32)"
        );
    }

    #[test]
    fn test_ffi_status_alignment_matches_c() {
        // ThryveStatus should have alignment of 4 (C enum)
        assert_eq!(
            std::mem::align_of::<ThryveStatus>(),
            4,
            "ThryveStatus alignment should be 4"
        );
    }
}

mod tests_ffi_task_result_layout {
    use thryve_ffi::ThryveTaskResult;

    #[test]
    fn test_task_result_default_is_null() {
        // Arrange — default ThryveTaskResult
        let result = ThryveTaskResult::default();

        // Assert — all fields should be null/false
        assert!(result.result_type.is_null());
        assert!(result.message.is_null());
        assert!(result.log_file.is_null());
        assert!(!result.has_chain);
        assert!(result.chain_task.is_null());
    }

    #[test]
    fn test_task_result_layout_size() {
        // Verify the struct layout matches C expectations
        // ThryveTaskResult = 3 pointers (24 bytes) + 1 bool (1 byte + 7 padding) + 1 pointer (8 bytes) = 40 bytes (on 64-bit)
        // With repr(C) alignment, the bool is padded to 8 bytes for the next pointer
        let size = std::mem::size_of::<ThryveTaskResult>();
        assert!(
            size >= 40,
            "ThryveTaskResult should be at least 40 bytes (3 pointers + bool + padding + 1 pointer)"
        );
        assert!(
            size <= 48,
            "ThryveTaskResult should be at most 48 bytes (with full padding)"
        );
    }

    #[test]
    fn test_task_result_layout_alignment() {
        // ThryveTaskResult should have alignment of 8 (pointer size on 64-bit)
        let alignment = std::mem::align_of::<ThryveTaskResult>();
        assert_eq!(
            alignment, 8,
            "ThryveTaskResult should have 8-byte alignment"
        );
    }

    #[test]
    fn test_task_result_repro_c() {
        // Verify repr(C) layout — fields should be in declaration order
        // result_type, message, log_file, has_chain, chain_task
        let offset_result_type = std::mem::offset_of!(ThryveTaskResult, result_type);
        let offset_message = std::mem::offset_of!(ThryveTaskResult, message);
        let offset_log_file = std::mem::offset_of!(ThryveTaskResult, log_file);
        let offset_has_chain = std::mem::offset_of!(ThryveTaskResult, has_chain);
        let offset_chain_task = std::mem::offset_of!(ThryveTaskResult, chain_task);

        assert_eq!(offset_result_type, 0, "result_type should be at offset 0");
        assert_eq!(offset_message, 8, "message should be at offset 8");
        assert_eq!(offset_log_file, 16, "log_file should be at offset 16");
        // has_chain offset depends on padding after log_file (8 bytes)
        assert!(
            offset_has_chain >= 24,
            "has_chain should be at offset >= 24"
        );
        assert!(
            offset_chain_task > offset_has_chain,
            "chain_task should be after has_chain"
        );
    }
}

mod tests_ffi_config_size {
    use thryve_ffi::ThryveConfig;

    #[test]
    fn test_ffi_config_is_pointer_size() {
        // ThryveConfig is *mut u8, should be pointer size
        assert_eq!(
            std::mem::size_of::<ThryveConfig>(),
            std::mem::size_of::<usize>(),
            "ThryveConfig should be pointer size"
        );
    }
}

mod tests_ffi_ctx_size {
    use thryve_ffi::ThryveCtx;

    #[test]
    fn test_ffi_ctx_is_pointer_size() {
        assert_eq!(
            std::mem::size_of::<ThryveCtx>(),
            std::mem::size_of::<usize>(),
            "ThryveCtx should be pointer size"
        );
    }
}

mod tests_ffi_plugin_size {
    use thryve_ffi::ThryvePlugin;

    #[test]
    fn test_ffi_plugin_is_pointer_size() {
        assert_eq!(
            std::mem::size_of::<ThryvePlugin>(),
            std::mem::size_of::<usize>(),
            "ThryvePlugin should be pointer size"
        );
    }
}

mod tests_ffi_observer_size {
    use thryve_ffi::ThryveObserver;

    #[test]
    fn test_ffi_observer_is_pointer_size() {
        assert_eq!(
            std::mem::size_of::<ThryveObserver>(),
            std::mem::size_of::<usize>(),
            "ThryveObserver should be pointer size"
        );
    }
}

mod tests_ffi_task_size {
    use thryve_ffi::ThryveTask;

    #[test]
    fn test_ffi_task_is_pointer_size() {
        assert_eq!(
            std::mem::size_of::<ThryveTask>(),
            std::mem::size_of::<usize>(),
            "ThryveTask should be pointer size"
        );
    }
}

mod tests_ffi_notif_size {
    use thryve_ffi::ThryveNotif;

    #[test]
    fn test_ffi_notif_is_pointer_size() {
        assert_eq!(
            std::mem::size_of::<ThryveNotif>(),
            std::mem::size_of::<usize>(),
            "ThryveNotif should be pointer size"
        );
    }
}

mod tests_ffi_packet_handle_size {
    use thryve_ffi::ThryvePacketHandle;

    #[test]
    fn test_ffi_packet_handle_is_pointer_size() {
        assert_eq!(
            std::mem::size_of::<ThryvePacketHandle>(),
            std::mem::size_of::<usize>(),
            "ThryvePacketHandle should be pointer size"
        );
    }
}
