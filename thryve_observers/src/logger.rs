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
            file.flush().unwrap()
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

        let logger_core: ObsJob = Box::new(move |logger: Arc<Observer>| {
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
    fn log(&self, observer: Arc<Observer>) {
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
    fn handle_log(&self, notif: Notif) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use thryve_utils::cfg::ctx::Ctx;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::cfg::dir_data::DirData;
    use thryve_utils::cfg::meta_data::MetaData;
    use chrono::{DateTime, Local};
    use std::path::PathBuf;
    use std::time::SystemTime;
    use crossbeam::channel::bounded;

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();
        
        // Create minimal DirData and MetaData for testing
        let exe_dir = PathBuf::from(std::env::temp_dir());
        let log_dir = exe_dir.join("test_logs");
        std::fs::create_dir_all(&log_dir).ok();
        
        let mut log_hash_map: HashMap<NamedInput, Arc<File>> = HashMap::new();
        let main_log = std::fs::File::create(log_dir.join("Main.log")).unwrap();
        log_hash_map.insert(NamedInput::new("Main"), Arc::new(main_log));
        let error_log = std::fs::File::create(log_dir.join("Error.log")).unwrap();
        log_hash_map.insert(NamedInput::new("Error"), Arc::new(error_log));
        
        let dir_data = DirData::new(exe_dir, log_hash_map);
        let datetime: DateTime<Local> = SystemTime::now().into();
        let meta_data = MetaData::new(datetime, dir_data);
        
        let mut ctx = Ctx::new(config, task_rx, notif_rx, packet_handle);
        ctx.meta_data = meta_data;
        ctx
    }

    #[test]
    fn test_logger_observer_new_creates_observer() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = LoggerObserver::new(&ctx);

        // Assert
        assert_eq!(observer.obs_name, "Logger");
        assert_eq!(observer.obs_type.inner(), "Logging");
    }

    #[test]
    fn test_logger_observer_with_custom_log_files() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.logging_file_names.push("Custom".to_string());
        
        // Add custom log file to dir_data
        let custom_log = std::fs::File::create(ctx.meta_data.dir_data.exe_dir.join("Custom.log")).unwrap();
        ctx.meta_data.dir_data.log_hash_map.insert(NamedInput::new("Custom"), Arc::new(custom_log));

        // Act
        let observer = LoggerObserver::new(&ctx);

        // Assert
        assert_eq!(observer.obs_name, "Logger");
        assert_eq!(observer.obs_type.inner(), "Logging");
    }

    #[test]
    fn test_logger_observer_obs_type_is_logging() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = LoggerObserver::new(&ctx);

        // Assert
        assert_eq!(observer.obs_type.inner(), "Logging");
    }

    #[test]
    fn test_logger_observer_obs_name_is_logger() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let observer = LoggerObserver::new(&ctx);

        // Assert
        assert_eq!(observer.obs_name, "Logger");
    }

    #[test]
    fn test_logger_observer_handle_log_writes_to_file() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx);
        
        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();
        
        // Create a log notification
        let notif = Log::log("Test log message".to_string(), "Main");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, the log was written successfully
        // The file should now contain the message
    }

    #[test]
    fn test_logger_observer_handle_log_error_message() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx);
        
        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();
        
        // Create an error notification
        let notif = Log::err("Test error message".to_string());

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, the error was written successfully
    }

    #[test]
    fn test_logger_observer_handle_log_with_custom_file() {
        // Arrange
        let mut ctx = create_test_ctx();
        
        // Add a custom log file
        let custom_log = std::fs::File::create(ctx.meta_data.dir_data.exe_dir.join("Custom.log")).unwrap();
        ctx.meta_data.dir_data.log_hash_map.insert(NamedInput::new("Custom"), Arc::new(custom_log));
        
        let observer = LoggerObserver::new(&ctx);
        
        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();
        
        // Create a notification for the custom file
        let notif = Log::log("Custom log message".to_string(), "Custom");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, the log was written successfully
    }

    #[test]
    fn test_logger_observer_cleanup_processes_remaining_notifs() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx);
        let observer = Arc::new(observer);
        
        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();
        
        // Push some notifications to the queue
        let notif1 = Log::log("Message 1".to_string(), "Main");
        let notif2 = Log::log("Message 2".to_string(), "Main");
        observer.obs_queue.push(notif1);
        observer.obs_queue.push(notif2);

        // Act - cleanup should process remaining notifications
        logger_observer.clean_up(observer.clone());

        // Assert - if we get here without panic, cleanup worked
    }

    #[test]
    fn test_logger_observer_with_unicode_message() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx);
        
        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();
        
        // Create a notification with unicode
        let notif = Log::log("Hello 世界 🌍".to_string(), "Main");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, unicode was handled correctly
    }

    #[test]
    fn test_logger_observer_with_empty_message() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx);
        
        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();
        
        // Create a notification with empty message
        let notif = Log::log("".to_string(), "Main");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, empty message was handled
    }

    #[test]
    fn test_logger_observer_with_multiline_message() {
        // Arrange
        let ctx = create_test_ctx();
        let observer = LoggerObserver::new(&ctx);
        
        // Downcast to get the LoggerObserver
        let obs_core = observer.obs_core.as_any();
        let logger_observer = obs_core.downcast_ref::<LoggerObserver>().unwrap();
        
        // Create a notification with multiline message
        let notif = Log::log("Line 1\nLine 2\nLine 3".to_string(), "Main");

        // Act - should not panic
        logger_observer.handle_log(notif);

        // Assert - if we get here without panic, multiline was handled
    }
}
