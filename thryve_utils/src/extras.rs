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
