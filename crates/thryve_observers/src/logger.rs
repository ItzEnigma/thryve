// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Logger observer implementation for the Thryve notification system.
//!
//! This module provides the [`LoggerObserver`] struct, which implements a
//! file-based logging observer for the Thryve system. The logger observer
//! receives notifications and writes them to timestamped log files organized
//! by module type.
//!
//! # Architecture
//!
//! The logger observer:
//! - Writes to the log files that startup (`init_directories`, in `thryve_run`) opened, in a
//!   folder named after the time the run started
//! - Maintains a hashmap of log files keyed by log-file name
//! - Processes notifications in a continuous loop
//! - Writes formatted log entries with timestamps
//!
//! # Log Directory Structure
//!
//! ```text
//! log/
//!   <timestamp>/
//!     Main.log          - General log messages
//!     Error.log         - Error messages
//!     Debug.log         - Debug messages (debug mode only)
//!     <Module>.log      - One per log-file name in the config
//! ```
//!
//! # Usage
//!
//! The logger observer is typically created during initialization and
//! registered with the observer manager. All other components send
//! log notifications via the [`PacketHandle`](thryve_utils::packets::packet_handle::PacketHandle).
//!
//! # Example
//!
//! ```ignore
//! use thryve_observers::logger::LoggerObserver;
//!
//! // `ctx` has been through `init_directories`, which opened the log files.
//! let logger = LoggerObserver::new(&ctx)?;
//! ```

use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    sync::{Arc, atomic::Ordering},
};

use thryve_utils::extras::log_format::{log_line, write_line};
use thryve_utils::packets::notif_log::Log;
use thryve_utils::traits::{observer_logic::ObsLogic, queue::Queue};
use thryve_utils::{bases::notif::Notif, cfg::ctx::Ctx, impl_as_any};
use thryve_utils::{bases::observer::Observer, extras::named_input::LogFileNamedInput};
use thryve_utils::{errors::StartupError, extras::jobs::ObsJob};

/// Core logic for the logger observer.
///
/// The `LoggerObserver` struct implements the [`ObsLogic`] trait and provides
/// file-based logging functionality for the Thryve system. It maintains a
/// hashmap of log files keyed by log-file name and processes incoming
/// notifications by writing them to the appropriate files.
///
/// # Fields
///
/// - `_currentdir`: The current executable directory (unused, kept for potential future use)
/// - `error_log_file`: `Error.log`, where redirected lines and notes go
/// - `files`: HashMap mapping log-file names to their log file handles
///
/// # Example
///
/// ```ignore
/// use thryve_observers::logger::LoggerObserver;
///
/// let logger = LoggerObserver::new(&ctx)?;
/// ```
pub struct LoggerObserver {
    /// The current executable directory (unused, kept for potential future use).
    _currentdir: String,
    /// `Error.log`, where redirected lines and notes go. It's looked up once, in
    /// [`new`](Self::new), so writing to it has nothing to unwrap. The file map keeps it too, so
    /// that lines sent to the log file `Error` find it there.
    error_log_file: Arc<File>,

    /// HashMap mapping log-file names, in upper case, to their log file handles.
    files: HashMap<LogFileNamedInput, Arc<File>>,
}

impl ObsLogic for LoggerObserver {
    /// Gracefully shuts down the logger observer.
    ///
    /// Logs a cleanup message and processes all remaining notifications
    /// in the observer's queue before terminating. This ensures that
    /// no log messages are lost during shutdown.
    ///
    /// # Arguments
    ///
    /// * `observer` - The observer being shut down
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Called automatically during engine shutdown
    /// observer.clean_up();
    /// ```
    fn clean_up(&self, observer: Arc<Observer>) {
        // At shutdown this runs on the engine's thread, after the logger's own thread has been
        // joined, so it's the only writer: the lines still queued come out in order, the
        // engine's "Engine Finalized" last.
        let notif = Log::log("Logger Obs Cleaning Up...".to_string(), "Main");
        self.handle_log(notif);

        while !observer.obs_queue.main_queue.is_empty() {
            if let Some(notif) = observer.obs_queue.main_queue.pop() {
                self.handle_log(notif);
            }
        }

        for (_, mut file) in self.files.clone() {
            // Non-fatal: flush failure means data might not be written, but we continue
            let _ = file.flush();
        }
    }

    impl_as_any!(LoggerObserver);
}

impl LoggerObserver {
    /// Creates the logger observer from the log files opened at startup.
    ///
    /// Takes the log files that `init_directories` opened from the context and wraps
    /// the logger in an [`Observer`] of type `Logging`.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The application context, after `init_directories` has run
    ///
    /// # Returns
    ///
    /// A new [`Observer`] instance configured for logging.
    ///
    /// # Log Files Created
    ///
    /// - `Main.log` - General log messages
    /// - `Error.log` - Error messages
    /// - `Debug.log` - Debug messages (debug mode only)
    /// - `<Module>.log` - One per log-file name registered in the config
    ///
    /// # Errors
    ///
    /// Returns [`StartupError::LogFileFetch`] if the context has no `Error.log`, that is, if
    /// `init_directories` hasn't run.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use thryve_observers::logger::LoggerObserver;
    ///
    /// let logger = LoggerObserver::new(&ctx)?;
    /// ```
    #[allow(clippy::new_ret_no_self)] // factory returns the Observer wrapper by design
    pub fn new(ctx: &Ctx) -> Result<Observer, StartupError> {
        let dir_data = ctx.meta_data.dir_data.clone();
        let err_file_key = &LogFileNamedInput::new_err();

        let err_file = dir_data
            .log_hash_map
            .get(err_file_key)
            .ok_or(StartupError::LogFileFetch)?;

        // Create an instance of the `LoggerObserver` struct with the initialized
        // log files and return an `Observer` instance that encapsulates its logic.
        let logger = Arc::new(LoggerObserver {
            _currentdir: dir_data.exe_dir.to_string_lossy().to_string(),
            error_log_file: err_file.clone(),
            files: dir_data.log_hash_map,
        });

        let logger_job = ObsJob::new(move |logger: Arc<Observer>| {
            let logger_as_any = logger.obs_core.as_any();
            if let Some(logger_core) = logger_as_any.downcast_ref::<LoggerObserver>() {
                logger_core.log(logger.clone());
            }
        });

        // Return a new observer instance with the specified type and name.
        Ok(Observer::new_logging(
            logger,
            logger_job,
            ctx.packet_handle(),
        ))
    }

    /// Main processing loop for the logger observer.
    ///
    /// Continuously monitors the observer's notification queue and processes
    /// incoming log notifications. Stops when the observer's exit flag is set during engine
    /// shutdown, or when its queue is finalized. Whatever is still queued then is written by
    /// the logger's `clean_up`.
    ///
    /// # Arguments
    ///
    /// * `observer` - The observer instance containing the notification queue
    ///
    /// # Lifecycle
    ///
    /// 1. Checks if the exit flag is set; if so, stops
    /// 2. Pops the next notification from the queue; if the queue was finalized, stops
    /// 3. Calls `handle_log` to write the notification to the appropriate file
    /// 4. Repeats until shutdown
    pub fn log(&self, observer: Arc<Observer>) {
        loop {
            // Check if the exit flag is set to true, indicating that the engine
            // is shutting down. If so, stop: the logger's `clean_up` writes what's left.
            if observer.exit_flag.load(Ordering::SeqCst) {
                break;
            }

            // Pop a notification from the observer's queue and handle it by
            // calling the `handle_log` method.
            let notif_opt = observer.obs_queue.pop();

            if let Some(notif) = notif_opt {
                self.handle_log(notif);
            } else {
                break;
            }
        }

        let notif = Log::log("Logger Obs Thread Exited...".to_string(), "Main");
        self.handle_log(notif);
    }

    /// Processes a notification and writes it to the appropriate log file.
    ///
    /// Downcasts the notification to a [`Log`] type and writes the message to the
    /// log file it asks for, looked up ignoring letter case. If no file with that
    /// name was registered at startup, the message goes to `Error.log` instead,
    /// behind a note naming the requested file. Empty messages are skipped.
    ///
    /// A notification that isn't a [`Log`] can't be displayed. It's dropped, with a note in
    /// `Error.log` naming its type, and the logger keeps going. A line whose file can't be
    /// written goes to stderr instead.
    ///
    /// # Arguments
    ///
    /// * `notif` - The notification to process
    pub fn handle_log(&self, notif: Notif) {
        // Attempt to downcast the notification's core logic to a `Log` type
        // to access its specific fields and functionality.
        let notif_as_any = notif.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<Log>();

        // Extract the notification type and timestamp for logging purposes.
        let notif_type = notif.notif_type();
        let timestamp = notif.timestamp;

        // If the notification is successfully downcasted to a `Log` type,
        // extract the log file name from the notification and log the message
        // to the appropriate file.
        if let Some(notif) = notif_casted {
            if notif.message.is_empty() {
                return;
            }

            // Write the message to its log file. An unregistered log file isn't
            // fatal: the message goes to Error.log instead, behind a note naming
            // the file that was asked for (escaped, since it's unchecked text).
            match self
                .files
                .get(notif.logfile().to_ascii_uppercase().as_str())
            {
                Some(file) => notif.display(timestamp, notif_type, None, file.clone()),
                None => {
                    let note = format!("[log file {:?} is not registered]", notif.logfile());
                    let error_file = self.error_log_file.clone();
                    notif.display(timestamp, notif_type, Some(&note), error_file.clone());
                }
            }
        }
        // If the notification cannot be downcasted to a `Log` type, the logger
        // can't display it. That isn't fatal: it's dropped, with a note in
        // Error.log naming its type (escaped, like the log-file note above).
        else {
            let line = log_line(
                timestamp,
                "LOGGING",
                "Error",
                &format!(
                    "[dropped a notification of type {notif_type:?}: the logger only takes log notifications]"
                ),
            );
            let error_file = self.error_log_file.clone();
            write_line(&error_file, &line);
        }
    }
}
