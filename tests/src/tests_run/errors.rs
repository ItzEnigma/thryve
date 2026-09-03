// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_thryve_run_error {
    use thryve_run::errors::ThryveRunError;

    #[test]
    fn test_from_string_creates_service_command_variant() {
        // Arrange
        let msg = String::from("something went wrong");

        // Act
        let err: ThryveRunError = ThryveRunError::from(msg.clone());

        // Assert
        match err {
            ThryveRunError::ServiceCommand(inner) => assert_eq!(inner, msg),
            other => panic!("Expected ServiceCommand variant, got {:?}", other),
        }
    }

    #[test]
    fn test_from_str_slice_creates_service_command_variant() {
        // Arrange
        let msg = "static str error";

        // Act
        let err: ThryveRunError = ThryveRunError::from(msg);

        // Assert
        match err {
            ThryveRunError::ServiceCommand(inner) => assert_eq!(inner, msg),
            other => panic!("Expected ServiceCommand variant, got {:?}", other),
        }
    }

    #[test]
    fn test_from_string_display_message_includes_text() {
        // Arrange
        let err: ThryveRunError = ThryveRunError::from(String::from("boom"));

        // Act
        let display = format!("{}", err);

        // Assert
        assert!(display.contains("boom"));
        assert!(display.contains("Service command error"));
    }

    #[test]
    fn test_from_str_slice_display_message_includes_text() {
        // Arrange
        let err: ThryveRunError = ThryveRunError::from("kaboom");

        // Act
        let display = format!("{}", err);

        // Assert
        assert!(display.contains("kaboom"));
        assert!(display.contains("Service command error"));
    }
}
