pub mod named_input{
    // This module is a wrapper around a String to ensure that developer input namings are always alphanumeric.
    // This is important for security reasons, as namings from developer inputs are used in various places where special characters could cause issues.
    // This ensures developer freedom in naming, while maintaining a secure environment
    use std::fmt::Display;

    
    // The struct that wraps a String and ensures it is alphanumeric.
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct NamedInput(String);
    
    impl NamedInput {
        // Creates a new NamedInput if the input string is alphanumeric.
        // Panics if the input string contains non-alphanumeric characters.
        // @param s: &str
        // @return NamedInput
        pub fn new(s: &str) -> Self {
            assert!(s.chars().all(|c| c.is_alphanumeric()), "NamedInput must be alphanumeric. Got '{}'", s);
            NamedInput(s.to_owned())
        }
    
        // Returns a reference to the inner string.
        // @return &str
        pub fn inner(&self) -> &str {
            &self.0
        }
    }
    
    impl Display for NamedInput {
        // Formats the NamedInput for display.
        // @param f: the formatter
        // @return std::fmt::Result
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
}

pub mod jobs {
    use std::sync::Arc;

    use crate::bases::observer::Observer;
    use crate::res::Result;

    // A TaskJob is a function that takes no parameters and returns a Result.
    // Instead of having to type the entire type every time, a type alias is defined.
    pub type TaskJob = Box<dyn FnOnce() -> Result + Send + Sync>;

    pub type ObsJob = Box<dyn Fn(Arc<Observer>) -> () + Send + Sync>;
}