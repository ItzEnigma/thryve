//! Utility modules for the Thryve system.
//!
//! This module contains helper types and utilities used throughout
//! the Thryve codebase, including:
//!
//! - [`named_input`]: Type-safe string wrapper for developer inputs
//! - [`jobs`]: Type aliases for task and observer job closures

pub mod named_input {
    //! Type-safe wrapper for developer-provided names.
    //!
    //! This module provides the [`NamedInput`] type, which wraps a `String`
    //! and ensures that developer-provided names are always alphanumeric.
    //! This validation prevents security issues that could arise from
    //! special characters in names used throughout the system.
    //!
    //! # Security
    //!
    //! The alphanumeric requirement ensures that names can be safely used
    //! in file paths, log messages, and other contexts where special
    //! characters could cause issues.

    use std::fmt::Display;

    /// Type-safe wrapper for alphanumeric names.
    ///
    /// This struct wraps a `String` and ensures that the contained
    /// value is always alphanumeric. This validation prevents
    /// security issues that could arise from special characters
    /// in names used throughout the system.
    ///
    /// # Fields
    ///
    /// The inner string is private and can only be accessed via
    /// the [`inner`](NamedInput::inner) method.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::extras::named_input::NamedInput;
    ///
    /// let name = NamedInput::new("ValidName123");
    /// println!("Name: {}", name.inner());
    ///
    /// // This would panic:
    /// // let invalid = NamedInput::new("Invalid-Name!");
    /// ```
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct NamedInput(String);

    impl NamedInput {
        /// Creates a new `NamedInput` from an alphanumeric string.
        ///
        /// Validates that the input contains only alphanumeric characters.
        /// Panics with a descriptive error message if validation fails.
        ///
        /// # Arguments
        ///
        /// * `s` - The string to wrap (must be alphanumeric)
        ///
        /// # Panics
        ///
        /// Panics if the string contains non-alphanumeric characters.
        ///
        /// # Example
        ///
        /// ```no_run
        /// use thryve_utils::extras::named_input::NamedInput;
        ///
        /// let name = NamedInput::new("ValidName");
        /// assert_eq!(name.inner(), "ValidName");
        ///
        /// // Panics:
        /// // let invalid = NamedInput::new("Invalid-Name");
        /// ```
        pub fn new(s: &str) -> Self {
            assert!(
                !s.is_empty(),
                "NamedInput cannot be empty."
            );
            assert!(
                s.chars().all(|c| c.is_alphanumeric()),
                "NamedInput must be alphanumeric. Got '{}'",
                s
            );
            NamedInput(s.to_owned())
        }

        /// Returns a reference to the inner string.
        ///
        /// # Returns
        ///
        /// A string slice reference to the wrapped value.
        ///
        /// # Example
        ///
        /// ```no_run
        /// use thryve_utils::extras::named_input::NamedInput;
        ///
        /// let name = NamedInput::new("MyName");
        /// assert_eq!(name.inner(), "MyName");
        /// ```
        pub fn inner(&self) -> &str {
            &self.0
        }
    }

    impl Display for NamedInput {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
}

pub mod jobs {
    //! Type aliases for job closures.
    //!
    //! This module defines type aliases for the closure types used
    //! as jobs in the Thryve system. These aliases simplify the
    //! type signatures throughout the codebase.
    //!
    //! # Job Types
    //!
    //! - [`TaskJob`]: Closure for task execution
    //! - [`ObsJob`]: Closure for observer initialization
    use std::sync::Arc;

    use crate::bases::observer::Observer;
    use crate::res::Result;

    /// Type alias for task execution closures.
    ///
    /// A `TaskJob` is a closure that takes no arguments and returns
    /// a [`Result`](crate::res::Result). Tasks are executed by worker
    /// threads, and the result indicates success or failure along with
    /// optional follow-up actions.
    ///
    /// # Traits
    ///
    /// - `FnOnce`: Executed exactly once
    /// - `Send`: Can be sent to another thread
    /// - `Sync`: Can be shared between threads
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::extras::jobs::TaskJob;
    /// use thryve_utils::res::Result;
    ///
    /// let job: TaskJob = Box::new(|| {
    ///     // Task execution logic
    ///     Result::new("Success", "Done".to_string(), "Main")
    /// });
    ///
    /// let result = job();
    /// ```
    pub type TaskJob = Box<dyn FnOnce() -> Result + Send + Sync>;

    /// Type alias for observer initialization closures.
    ///
    /// An `ObsJob` is a closure that takes an [`Arc<Observer>`](crate::bases::observer::Observer)
    /// and returns nothing. This closure is used to initialize observers
    /// with their specific logic, typically starting a loop that processes
    /// notifications until shutdown.
    ///
    /// # Typical Pattern
    ///
    /// ```no_run
    /// use thryve_utils::extras::jobs::ObsJob;
    /// use thryve_utils::bases::observer::Observer;
    /// use std::sync::Arc;
    ///
    /// let job: ObsJob = Box::new(move |observer: Arc<Observer>| {
    ///     // Downcast observer core to concrete type
    ///     // Start notification processing loop
    ///     // Clean up on exit
    /// });
    /// ```
    ///
    /// # Traits
    ///
    /// - `Fn`: Can be called multiple times (though typically called once)
    /// - `Send`: Can be sent to another thread
    /// - `Sync`: Can be shared between threads
    pub type ObsJob = Box<dyn Fn(Arc<Observer>) -> () + Send + Sync>;
}

pub mod panic_hook{
    use std::panic;
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::time::SystemTime;
    use chrono::{DateTime, Local};

    use crate::cfg::dir_data::DirData;

    /// Initializes a custom panic hook that writes panic information to the Error.log file.
    ///
    /// This should be called after `init_directories()` has been called, so that
    /// the panic hook can access the log directory path. The panic hook writes
    /// directly to the Error.log file in the same directory as the logger observer,
    /// ensuring that panics are captured even when running as a Windows service.
    pub fn init_panic_hook() {
        panic::set_hook(Box::new(|panic_info| {
            // Format panic message
            let message = panic_info.payload().downcast_ref::<&str>()
                .map(|s| *s)
                .or_else(|| panic_info.payload().downcast_ref::<String>()
                    .map(|s| s.as_str()))
                .unwrap_or("Unknown panic");
            
            // Format location
            let location = panic_info.location()
                .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
                .unwrap_or_else(|| "unknown location".to_string());
            
            // Format timestamp
            let timestamp: DateTime<Local> = SystemTime::now().into();

            // Get the log directory path from the static
            let log_dir = match DirData::get_log_dir_path() {
                Some(path) => path,
                None => return, // Log directory not initialized yet, skip file logging
            };
            
            // Open Error.log for appending
            let error_file_path = log_dir.join("Error.log");
            
            if let Ok(mut file) = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&error_file_path)
            {   
                // Write to file
                let _ = writeln!(file, "[PANIC] {}: {} at {}", timestamp.format("%Y-%m-%d %H:%M:%S%.6f"), message, location);
                let _ = file.flush();
            }
            
            // Also print to stderr (for non-service mode debugging)
            eprintln!("[PANIC] {}: {} at {}", timestamp.format("%Y-%m-%d %H:%M:%S%.6f"), message, location);
            std::process::exit(1);
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for named_input module
    mod named_input_tests {
        use super::*;

        #[test]
        fn test_named_input_new_valid_alphanumeric() {
            // Arrange
            let valid_name = "ValidName123";
            
            // Act
            let result = named_input::NamedInput::new(valid_name);
            
            // Assert
            assert_eq!(result.inner(), "ValidName123");
        }

        #[test]
        #[should_panic(expected = "NamedInput cannot be empty")]
        fn test_named_input_new_empty_string() {
            // Arrange
            let empty_name = "";
            
            // Act & Assert - empty string should panic
            let _result = named_input::NamedInput::new(empty_name);
        }

        #[test]
        fn test_named_input_new_letters_only() {
            // Arrange
            let letters_only = "HelloWorld";
            
            // Act
            let result = named_input::NamedInput::new(letters_only);
            
            // Assert
            assert_eq!(result.inner(), "HelloWorld");
        }

        #[test]
        fn test_named_input_new_numbers_only() {
            // Arrange
            let numbers_only = "123456";
            
            // Act
            let result = named_input::NamedInput::new(numbers_only);
            
            // Assert
            assert_eq!(result.inner(), "123456");
        }

        #[test]
        fn test_named_input_new_mixed_case() {
            // Arrange
            let mixed_case = "CamelCase123";
            
            // Act
            let result = named_input::NamedInput::new(mixed_case);
            
            // Assert
            assert_eq!(result.inner(), "CamelCase123");
        }

        #[test]
        #[should_panic(expected = "NamedInput must be alphanumeric")]
        fn test_named_input_new_with_hyphen_panics() {
            // Arrange
            let invalid_name = "Invalid-Name";
            
            // Act & Assert
            let _ = named_input::NamedInput::new(invalid_name);
        }

        #[test]
        #[should_panic(expected = "NamedInput must be alphanumeric")]
        fn test_named_input_new_with_underscore_panics() {
            // Arrange
            let invalid_name = "Invalid_Name";
            
            // Act & Assert
            let _ = named_input::NamedInput::new(invalid_name);
        }

        #[test]
        #[should_panic(expected = "NamedInput must be alphanumeric")]
        fn test_named_input_new_with_space_panics() {
            // Arrange
            let invalid_name = "Invalid Name";
            
            // Act & Assert
            let _ = named_input::NamedInput::new(invalid_name);
        }

        #[test]
        #[should_panic(expected = "NamedInput must be alphanumeric")]
        fn test_named_input_new_with_special_char_panics() {
            // Arrange
            let invalid_name = "Invalid@Name";
            
            // Act & Assert
            let _ = named_input::NamedInput::new(invalid_name);
        }

        #[test]
        #[should_panic(expected = "NamedInput must be alphanumeric")]
        fn test_named_input_new_with_dot_panics() {
            // Arrange
            let invalid_name = "Invalid.Name";
            
            // Act & Assert
            let _ = named_input::NamedInput::new(invalid_name);
        }

        #[test]
        fn test_named_input_inner_returns_correct_value() {
            // Arrange
            let name = named_input::NamedInput::new("TestName");
            
            // Act
            let inner = name.inner();
            
            // Assert
            assert_eq!(inner, "TestName");
        }

        #[test]
        fn test_named_input_display_impl() {
            // Arrange
            let name = named_input::NamedInput::new("DisplayTest");
            
            // Act
            let display_str = format!("{}", name);
            
            // Assert
            assert_eq!(display_str, "DisplayTest");
        }

        #[test]
        fn test_named_input_clone() {
            // Arrange
            let original = named_input::NamedInput::new("CloneTest");
            
            // Act
            let cloned = original.clone();
            
            // Assert
            assert_eq!(cloned.inner(), original.inner());
            assert_eq!(cloned, original);
        }

        #[test]
        fn test_named_input_debug_impl() {
            // Arrange
            let name = named_input::NamedInput::new("DebugTest");
            
            // Act
            let debug_str = format!("{:?}", name);
            
            // Assert
            assert!(debug_str.contains("NamedInput"));
        }

        #[test]
        fn test_named_input_partial_eq() {
            // Arrange
            let name1 = named_input::NamedInput::new("EqualTest");
            let name2 = named_input::NamedInput::new("EqualTest");
            let name3 = named_input::NamedInput::new("Different");
            
            // Act & Assert
            assert_eq!(name1, name2);
            assert_ne!(name1, name3);
        }

        #[test]
        fn test_named_input_hash_consistency() {
            // Arrange
            let name1 = named_input::NamedInput::new("HashTest");
            let name2 = named_input::NamedInput::new("HashTest");
            
            // Act
            use std::collections::HashSet;
            let mut set = HashSet::new();
            set.insert(name1);
            
            // Assert - equal items should have same hash
            assert!(set.contains(&name2));
        }
    }

    // Tests for jobs module - type aliases don't need extensive testing
    // but we can verify they compile and work correctly
    mod jobs_tests {

        use super::*;
        use std::sync::Arc;

        #[test]
        fn test_task_job_type_alias_compiles() {
            // Arrange & Act - just verify the type alias works
            let _job: jobs::TaskJob = Box::new(|| {
                crate::res::Result::new("Test", "Test message".to_string(), "Main")
            });
            
            // Assert - if we got here, the type alias is valid
        }

        #[test]
        fn test_task_job_execution() {
            // Arrange
            let job: jobs::TaskJob = Box::new(|| {
                crate::res::Result::new("Success", "Completed".to_string(), "Main")
            });
            
            // Act
            let result = job();
            
            // Assert
            assert_eq!(result.result_type.inner(), "Success");
            assert_eq!(result.message, "Completed");
        }

        #[test]
        fn test_obs_job_type_alias_compiles() {
            // Arrange & Act - verify the type alias works
            let _job: jobs::ObsJob = Box::new(move |_observer: Arc<crate::bases::observer::Observer>| {
                // Observer initialization logic
            });
            
            // Assert - if we got here, the type alias is valid
        }

        #[test]
        fn test_obs_job_execution() {
            // Arrange
            let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let executed_clone = Arc::clone(&executed);
            
            let job: jobs::ObsJob = Box::new(move |_observer: Arc<crate::bases::observer::Observer>| {
                executed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            });
            
            // Create a minimal observer for testing
            let (task_tx, _) = crossbeam::channel::unbounded::<crate::packets::task::Task>();
            let (notif_tx, _) = crossbeam::channel::unbounded::<crate::bases::notif::Notif>();
            let packet_handle = crate::packets::packet_handle::PacketHandle::new(task_tx, notif_tx);
            
            let observer = Arc::new(crate::bases::observer::Observer::new(
                "TestObserver",
                "TestType",
                Arc::new(TestObsLogic),
                job,
                packet_handle,
            ));
            
            // Act - execute the fn_to_run
            (observer.fn_to_run)(observer.clone());
            
            // Assert
            assert!(executed.load(std::sync::atomic::Ordering::SeqCst));
        }
    }

    // Helper struct for observer tests
    struct TestObsLogic;
    
    impl crate::traits::observer_logic::ObsLogic for TestObsLogic {
        fn clean_up(&self, _observer: std::sync::Arc<crate::bases::observer::Observer>) {
            // No-op for testing
        }
        
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    }
}