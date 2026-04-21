pub mod named_input{
    //! This module is a wrapper around a String to ensure that developer input namings are always alphanumeric.
    //! This is important for security reasons, as namings from developer inputs are used in various places where special characters could cause issues.
    //! This ensures developer freedom in naming, while maintaining a secure environment
    use std::fmt::Display;

    
    // Wraps a String and ensures it is alphanumeric.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct NamedInput(String);
    
    impl NamedInput {
        /// Creates a new `NamedInput` from a string slice, ensuring that it is alphanumeric.
        /// If the input string contains non-alphanumeric characters, it will panic with an assertion error.
        pub fn new(s: &str) -> Self {
            assert!(s.chars().all(|c| c.is_alphanumeric()), "NamedInput must be alphanumeric. Got '{}'", s);
            NamedInput(s.to_owned())
        }
    
        /// Returns a reference to the inner string of the `NamedInput`.
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
    //! This module defines types for Tasks and Observer jobs.
    //! These are just aliases for boxed closures that can be sent across threads, allowing for flexible and dynamic job definitions.
    use std::sync::Arc;

    use crate::bases::observer::Observer;
    use crate::res::Result;

    /// Is a function that takes no parameters and returns a `res::Result`.
    /// Instead of having to type the entire type every time, a type alias is defined.
    pub type TaskJob = Box<dyn FnOnce() -> Result + Send + Sync>;

    /// Is a function that takes an `Arc<Observer>` as a parameter and returns nothing.
    /// This is used to initialize the given Observer with it's respective logic.
    /// The function typically begins by downcasting the Observer's logic to the appropriate type.
    /// After downcasting, a function of the core logic of the Observer is executed.
    /// Typically, the core logic function is a loop that continuously pops notifications from the Observer's queue and handles them accordingly.
    /// This loop continues until the Observer is finalized, at which point the `clean_up` function of the Observer's logic is called.
    pub type ObsJob = Box<dyn Fn(Arc<Observer>) -> () + Send + Sync>;
}