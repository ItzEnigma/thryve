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
//! - Creates a log directory with a timestamp on initialization
//! - Maintains a hashmap of log files keyed by module type
//! - Processes notifications in a continuous loop
//! - Writes formatted log entries with timestamps
//!
//! # Log Directory Structure
//!
//! ```text
//! log/
//!   2024_01_15_10_30/
//!     Main.log          - General log messages
//!     Error.log         - Error messages
//!     <Module>.log      - Module-specific logs
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
//! ```no_run
//! use thryve_observers::logger::LoggerObserver;
//! use thryve_utils::extras::named_input::NamedInput;
//!
//! let file_names = vec![NamedInput::new("Main"), NamedInput::new("Error")];
//! let logger = LoggerObserver::new(file_names, packet_handle);
//! ```

use std::{collections::HashMap, fs::File, io::Write, sync::{Arc, atomic::Ordering}};

use thryve_utils::{bases::notif::Notif, cfg::ctx::Ctx, impl_as_any};
use thryve_utils::bases::observer::Observer;
use thryve_utils::packets::notif_log::Log;
use thryve_utils::extras::{jobs::ObsJob, named_input::NamedInput};
use thryve_utils::traits::{observer_logic::ObsLogic , queue::Queue};

/// Core logic for the logger observer.
///
/// The `LoggerObserver` struct implements the [`ObsLogic`] trait and provides
/// file-based logging functionality for the Thryve system. It maintains a
/// hashmap of log files keyed by module type and processes incoming
/// notifications by writing them to the appropriate files.
///
/// # Fields
///
/// - `_currentdir`: The current executable directory (unused, kept for potential future use)
/// - `files`: HashMap mapping module types to their log file handles
///
/// # Example
///
/// ```no_run
/// use thryve_observers::logger::LoggerObserver;
/// use thryve_utils::extras::named_input::NamedInput;
///
/// let file_names = vec![NamedInput::new("Main"), NamedInput::new("Error")];
/// let logger = LoggerObserver::new(file_names, packet_handle);
/// ```
pub struct LoggerObserver {
    /// The current executable directory (unused, kept for potential future use).
    _currentdir: String,
    /// HashMap mapping module types to their log file handles.
    files: HashMap<NamedInput, Arc<File>>,
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
    /// ```no_run
    /// // Called automatically during engine shutdown
    /// observer.clean_up();
    /// ```
    fn clean_up(&self, observer: Arc<Observer>) {
        let notif = Log::log("Logger Obs Cleaning Up...".to_string(), "Main");
        self.handle_log(notif);

        while observer.obs_queue.main_queue.len() != 0 {
            if let Some(notif) = observer.obs_queue.main_queue.pop(){

                self.handle_log(notif);
            }
        };

        for (_ , mut file) in self.files.clone() {
            // Non-fatal: flush failure means data might not be written, but we continue
            let _ = file.flush();
        }
    }

    impl_as_any!(LoggerObserver);
}

impl LoggerObserver {
    /// Creates a new logger observer with the specified log files.
    ///
    /// Initializes a new logger observer by creating a timestamped log directory
    /// and opening log files for each specified module type. Always creates
    /// "Main.log" and "Error.log" files in addition to any custom log files.
    ///
    /// # Arguments
    ///
    /// * `file_names` - Vector of module types for which to create log files
    /// * `tx` - Packet handle for communication with the engine
    ///
    /// # Returns
    ///
    /// A new [`Observer`] instance configured for logging.
    ///
    /// # Log Files Created
    ///
    /// - `Main.log` - General log messages
    /// - `Error.log` - Error messages
    /// - `<Module>.log` - Module-specific logs for each name in `file_names`
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_observers::logger::LoggerObserver;
    /// use thryve_utils::extras::named_input::NamedInput;
    ///
    /// let file_names = vec![NamedInput::new("Main"), NamedInput::new("Error")];
    /// let logger = LoggerObserver::new(file_names, packet_handle);
    /// ```
    pub fn new(ctx: &Ctx) -> Observer {
        let dir_data = ctx.meta_data.dir_data.clone();

        // Create an instance of the `LoggerObserver` struct with the initialized
        // log files and return an `Observer` instance that encapsulates its logic.
        let logger = Arc::new(LoggerObserver {
            _currentdir: dir_data.exe_dir.to_string_lossy().to_string(),
            files: dir_data.log_hash_map,
        });

        let logger_core = ObsJob::new(move |logger: Arc<Observer>| {
            let logger_as_any = logger.obs_core.as_any();
            if let Some(logger_core) = logger_as_any.downcast_ref::<LoggerObserver>() {
                logger_core.log(logger.clone());
            }
        });

        // Return a new observer instance with the specified type and name.
        Observer::new("Logger", "Logging", logger, logger_core, ctx.packet_handle())
    }

    /// Main processing loop for the logger observer.
    ///
    /// Continuously monitors the observer's notification queue and processes
    /// incoming log notifications. Exits gracefully when the observer's exit
    /// flag is set during engine shutdown.
    ///
    /// # Arguments
    ///
    /// * `observer` - The observer instance containing the notification queue
    ///
    /// # Lifecycle
    ///
    /// 1. Checks if the exit flag is set; if so, logs exit message and breaks
    /// 2. Pops the next notification from the queue
    /// 3. Calls `handle_log` to write the notification to the appropriate file
    /// 4. Repeats until shutdown
    pub fn log(&self, observer: Arc<Observer>) {
        loop {
            // Check if the exit flag is set to true, indicating that the engine
            // is shutting down. If so, log a message and break the loop to exit
            // the thread gracefully.
            if observer.exit_flag.load(Ordering::SeqCst) == true {
                let notif = Log::log("Logger Obs Thread Exited...".to_string(), "Main");
                observer.obs_queue.push(notif);
                return;
            }

            // Pop a notification from the observer's queue and handle it by
            // calling the `handle_log` method.
            let notif = observer.obs_queue.pop();

            // pop() can wake here because the queue was finalized while this
            // thread was blocked inside it, in which case it returns a
            // sentinel value rather than a real notification -- recheck
            // before handing it to handle_log (which expects a real Log).
            if observer.exit_flag.load(Ordering::SeqCst) == true {
                let notif = Log::log("Logger Obs Thread Exited...".to_string(), "Main");
                observer.obs_queue.push(notif);
                return;
            }

            self.handle_log(notif);
        }
    }

    /// Processes a notification and writes it to the appropriate log file.
    ///
    /// Downcasts the notification to a [`Log`] type, extracts the target
    /// log file name, and writes the formatted message to the corresponding
    /// file. Panics if the log file is not found or if the notification
    /// type is unsupported.
    ///
    /// # Arguments
    ///
    /// * `notif` - The notification to process
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The specified log file does not exist in the internal hashmap
    /// - The notification cannot be downcast to a `Log` type
    pub fn handle_log(&self, notif: Notif) {
        // Attempt to downcast the notification's core logic to a `Log` type
        // to access its specific fields and functionality.
        let notif_as_any = notif.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<Log>();

        // Extract the notification type and timestamp for logging purposes.
        let notif_type = notif.notif_type.inner();
        let timestamp = notif.timestamp;

        // If the notification is successfully downcasted to a `Log` type,
        // extract the log file name from the notification and log the message
        // to the appropriate file.
        if let Some(notif) = notif_casted {
            let logfile = &notif.logfile;

            let file = self.files.get(logfile);

            // If the specified log file does not exist in the hashmap, panic
            // with an error message.
            if file.is_none() {
                assert!(
                    false,
                    "Logger Observer Cannot Find Requested Log File. Got {}",
                    logfile
                );
            }
            
            if notif.message == ""{
                return;
            }

            // Log the message to the appropriate file by calling the `display`
            // method of the `Log` notification.
            notif.display(timestamp, notif_type, file.unwrap().clone());
        }

        // If the notification cannot be downcasted to a `Log` type, it means
        // that the logger observer received an unsupported notification type.
        else {
            assert!(
                false,
                "Logger Observer Received Unsupported Notification Type."
            );
        }
    }
}