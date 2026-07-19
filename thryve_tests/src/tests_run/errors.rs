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
