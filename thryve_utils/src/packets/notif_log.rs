//! Logging notification implementation for the Thryve notification system.
//!
//! This module provides the [`Log`] struct, which implements the core logic
//! for logging notifications. Log notifications carry messages that should
//! be written to log files, along with metadata about which file to use.
//!
//! # Usage
//!
//! Log notifications are created using the factory methods on [`Log`]:
//! - [`Log::log`] for regular log messages
//! - [`Log::err`] for error messages (automatically routed to "Error" log)
//!
//! The logger observer processes these notifications and writes them to
//! the appropriate files with timestamps.

use std::sync::Arc;
use std::{time::SystemTime};
use chrono::{DateTime, Local};
use std::fs::File;
use std::io::Write;

use crate::bases::notif::Notif;
use crate::impl_as_any;
use crate::traits::notif_logic::NotifLogic;
use crate::extras::named_input::NamedInput;

/// Core logic for logging notifications.
///
/// The `Log` struct implements the [`NotifLogic`] trait and carries
/// a message to be written to a log file. It is used by the logger
/// observer to persist notification messages.
///
/// # Fields
///
/// - `message`: The log message content
/// - `logfile`: The name of the log file to write to
///
/// # Example
///
/// ```no_run
/// use thryve_utils::packets::notif_log::Log;
///
/// // Create a regular log notification
/// let notif = Log::log("Something happened".to_string(), "MyModule");
///
/// // Create an error notification
/// let error_notif = Log::err("Something went wrong".to_string());
/// ```
pub struct Log {
    /// The log message content.
    pub message: String,
    /// The name of the log file to write to.
    pub logfile: NamedInput,
}

impl NotifLogic for Log {
    impl_as_any!(Log);
}

impl Log {
    /// Creates a new log notification with the specified message and log file.
    ///
    /// Creates a notification of type "Log" with the given message,
    /// targeted at the "Logging" observer. The notification will be
    /// written to the specified log file when processed.
    ///
    /// # Arguments
    ///
    /// * `message` - The log message content
    /// * `logfile` - The name of the log file to write to
    ///
    /// # Returns
    ///
    /// A new [`Notif`] containing the log notification.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::notif_log::Log;
    ///
    /// let notif = Log::log("Processing complete".to_string(), "Main");
    /// ```
    pub fn log(message: String, logfile: &str) -> Notif {
        let temp = Box::new(Log {
            message: message,
            logfile: NamedInput::new(logfile),
        });

        Notif::new("Log", "Logging", temp)
    }

    /// Creates a new error log notification.
    ///
    /// Creates a notification of type "Error" with the given message,
    /// automatically routed to the "Error" log file. This allows
    /// error messages to be separated from regular logs for easier
    /// identification and analysis.
    ///
    /// # Arguments
    ///
    /// * `message` - The error message content
    ///
    /// # Returns
    ///
    /// A new [`Notif`] containing the error notification.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::notif_log::Log;
    ///
    /// let error_notif = Log::err("Connection failed".to_string());
    /// ```
    pub fn err(message: String) -> Notif {
        let temp = Box::new(Log {
            message: message,
            logfile: NamedInput::new("Error"),
        });

        Notif::new("Error", "Logging", temp)
    }

    /// Writes the log message to the specified file.
    ///
    /// Formats and writes the log message with a timestamp, notification
    /// type, and message content to the provided file handle.
    ///
    /// # Arguments
    ///
    /// * `timestamp` - When the notification was created
    /// * `notif_type` - The type of notification (e.g., "Log", "Error")
    /// * `file` - The file handle to write to
    ///
    /// # Example
    ///
    /// Output format:
    /// ```text
    /// Log_Creation: 2024-01-15 10:30:45, [ LOGGING ], Event_Type: Log, Message: Hello
    /// ```
    pub fn display(&self, timestamp: SystemTime, notif_type: &str, file: Arc<File>) {
        let datetime: DateTime<Local> = timestamp.clone().into();
        writeln!(
            &*file,
            "Log_Creation: {}, [ LOGGING ], Event_Type: {}, Message: {}",
            datetime.format("%Y-%m-%d %H:%M:%S"),
            notif_type,
            self.message
        )
        .unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_creates_notification_with_correct_type() {
        // Arrange
        let message = "Test message".to_string();
        let log_file = "TestLog";
        
        // Act
        let notif = Log::log(message, log_file);
        
        // Assert
        assert_eq!(notif.notif_type.inner(), "Log");
        assert_eq!(notif.notif_receiver.inner(), "Logging");
    }

    #[test]
    fn test_log_stores_message_correctly() {
        // Arrange
        let message = "My log message".to_string();
        
        // Act
        let notif = Log::log(message.clone(), "Main");
        
        // Assert - downcast to Log to access message
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.message, "My log message");
    }

    #[test]
    fn test_log_stores_logfile_correctly() {
        // Arrange
        let log_file = "CustomLog";
        
        // Act
        let notif = Log::log("Message".to_string(), log_file);
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.logfile.inner(), "CustomLog");
    }

    #[test]
    fn test_err_creates_error_notification() {
        // Arrange
        let error_message = "Something went wrong".to_string();
        
        // Act
        let notif = Log::err(error_message);
        
        // Assert
        assert_eq!(notif.notif_type.inner(), "Error");
        assert_eq!(notif.notif_receiver.inner(), "Logging");
    }

    #[test]
    fn test_err_uses_error_logfile() {
        // Arrange & Act
        let notif = Log::err("Error message".to_string());
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.logfile.inner(), "Error");
    }

    #[test]
    fn test_log_with_empty_message() {
        // Arrange & Act
        let notif = Log::log("".to_string(), "Main");
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.message, "");
    }

    #[test]
    fn test_log_with_special_characters() {
        // Arrange
        let message = "Error: Connection failed! @#$%".to_string();
        
        // Act
        let notif = Log::log(message.clone(), "Main");
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.message, "Error: Connection failed! @#$%");
    }

    #[test]
    fn test_log_with_unicode() {
        // Arrange
        let message = "成功！Успех! Éxito!".to_string();
        
        // Act
        let notif = Log::log(message.clone(), "Main");
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.message, "成功！Успех! Éxito!");
    }

    #[test]
    fn test_log_invalid_logfile_panics() {
        // Arrange & Act - Log file must be alphanumeric
        assert!(std::panic::catch_unwind(|| {
            let _ = Log::log("msg".to_string(), "Invalid/File");
        }).is_err());
    }

    #[test]
    fn test_timestamp_is_set_on_notification() {
        // Arrange
        let before = SystemTime::now();
        
        // Act
        let notif = Log::log("Message".to_string(), "Main");
        
        // Assert
        assert!(notif.timestamp >= before);
    }
}
