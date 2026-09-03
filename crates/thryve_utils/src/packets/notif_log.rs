// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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

use chrono::{DateTime, Local};
use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::SystemTime;

use crate::bases::notif::Notif;
use crate::extras::named_input::NamedInput;
use crate::impl_as_any;
use crate::traits::notif_logic::NotifLogic;

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
            message,
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
            message,
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
        let datetime: DateTime<Local> = timestamp.into();
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
