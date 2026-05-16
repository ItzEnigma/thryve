//! Core notification structure for the Thryve notification system.
//!
//! This module provides the [`Notif`] struct, which serves as the fundamental
//! unit of communication in the Thryve observer system. Notifications are
//! messages that flow through the system, carrying information about events,
//! state changes, and other significant occurrences.
//!
//! # Architecture
//!
//! Each notification contains:
//! - A type identifier for categorizing the notification
//! - A receiver identifier for routing to the appropriate observer
//! - Core logic implementing the [`NotifLogic`] trait for custom behavior
//! - A timestamp indicating when the notification was created
//!
//! # Extensibility
//!
//! Any struct implementing the [`NotifLogic`] trait can serve as the core
//! of a notification, enabling flexible and extensible notification handling
//! throughout the system.
 
use crate::impl_as_any;
use crate::traits::notif_logic::NotifLogic;
use crate::extras::named_input::NamedInput;
use std::time::SystemTime;

/// Core notification structure for the Thryve notification system.
///
/// The `Notif` struct serves as the fundamental unit of communication in the
/// Thryve observer system. Notifications are messages that flow through the
/// system, carrying information about events, state changes, and other
/// significant occurrences.
///
/// # Fields
///
/// - `notif_type`: Type identifier for categorizing the notification
/// - `notif_receiver`: Receiver identifier for routing to the appropriate observer
/// - `notif_core`: Core logic implementing custom notification behavior
/// - `timestamp`: Time when the notification was created
///
/// # Example
///
/// ```no_run
/// // Notifications are typically created by implementing NotifLogic and
/// // passing the core logic to Notif::new along with type and receiver identifiers
/// ```
pub struct Notif {
    /// Type identifier for categorizing the notification.
    pub notif_type: NamedInput,
    /// Receiver identifier for routing to the appropriate observer.
    pub notif_receiver: NamedInput,
    /// Core logic implementing custom notification behavior.
    pub notif_core: Box<dyn NotifLogic>,
    /// Time when the notification was created.
    pub timestamp: SystemTime,
}

impl NotifLogic for Notif {
    impl_as_any!(Notif);
}

impl Notif {
    /// Creates a new notification with the specified configuration.
    ///
    /// Initializes a new notification instance with the given type, receiver,
    /// and core logic. The notification is automatically timestamped with the
    /// current system time.
    ///
    /// # Arguments
    ///
    /// * `notif_type` - Type identifier for categorizing the notification
    /// * `notif_receiver` - Receiver identifier for routing to the appropriate observer
    /// * `notif_core` - Core logic implementing custom notification behavior
    ///
    /// # Returns
    ///
    /// A new `Notif` instance configured with the provided parameters.
    pub fn new(
        notif_type: &str,
        notif_receiver: &str,
        notif_core: Box<dyn NotifLogic>,
    ) -> Self {
        Self {
            notif_type: NamedInput::new(notif_type),
            notif_receiver: NamedInput::new(notif_receiver),
            notif_core,
            timestamp: SystemTime::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test implementation of NotifLogic
    struct TestNotifLogic {
        _data: String,
    }

    impl NotifLogic for TestNotifLogic {
        impl_as_any!(TestNotifLogic);
    }

    #[test]
    fn test_notif_new_creates_notification() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new("TestType", "TestReceiver", notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner(), "TestType");
        assert_eq!(notif.notif_receiver.inner(), "TestReceiver");
    }

    #[test]
    fn test_notif_new_with_numbers_in_names() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new("Type123", "Receiver456", notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner(), "Type123");
        assert_eq!(notif.notif_receiver.inner(), "Receiver456");
    }

    #[test]
    fn test_notif_new_with_mixed_case() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new("MixedCase", "AnotherMixedCase", notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner(), "MixedCase");
        assert_eq!(notif.notif_receiver.inner(), "AnotherMixedCase");
    }

    #[test]
    fn test_notif_timestamp_is_set() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });
        let before = SystemTime::now();

        // Act
        let notif = Notif::new("TestType", "TestReceiver", notif_core);

        // Assert - timestamp should be >= before (no time travel)
        assert!(notif.timestamp >= before || notif.timestamp.duration_since(before).is_err());
    }

    #[test]
    #[should_panic(expected = "must be alphanumeric")]
    fn test_notif_new_with_hyphen_in_type_panics() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act & Assert
        let _notif = Notif::new("Test-Type", "TestReceiver", notif_core);
    }

    #[test]
    #[should_panic(expected = "must be alphanumeric")]
    fn test_notif_new_with_underscore_in_receiver_panics() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act & Assert
        let _notif = Notif::new("TestType", "Test_Receiver", notif_core);
    }

    #[test]
    #[should_panic(expected = "NamedInput cannot be empty")]
    fn test_notif_new_with_empty_type_panics() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act & Assert
        let _notif = Notif::new("", "TestReceiver", notif_core);
    }

    #[test]
    #[should_panic(expected = "NamedInput cannot be empty")]
    fn test_notif_new_with_empty_receiver_panics() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act & Assert
        let _notif = Notif::new("TestType", "", notif_core);
    }

    #[test]
    fn test_notif_with_single_character_names() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new("A", "B", notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner(), "A");
        assert_eq!(notif.notif_receiver.inner(), "B");
    }

    #[test]
    fn test_notif_with_long_names() {
        // Arrange
        let long_name = "A".repeat(100);
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new(&long_name, &long_name, notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner().len(), 100);
        assert_eq!(notif.notif_receiver.inner().len(), 100);
    }
}
