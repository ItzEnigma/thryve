// This file is a wrapper around a String to ensure that developer input namings are always alphanumeric.
// This is important for security reasons, as namings from developer inputs are used in various places where special characters could cause issues.
// This ensures developer freedom in naming, while maintaining a secure environment
use std::fmt::Display;

// The struct that wraps a String and ensures it is alphanumeric.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ANNamingInput(String);

impl ANNamingInput {
    // Creates a new ANNamingInput if the input string is alphanumeric.
    // Panics if the input string contains non-alphanumeric characters.
    // @param s: &str
    // @return ANNamingInput
    pub fn new(s: &str) -> Self {
        assert!(s.chars().all(|c| c.is_alphanumeric()), "ANNamingInput must be alphanumeric. Got '{}'", s);
        ANNamingInput(s.to_owned())
    }

    // Returns a reference to the inner string.
    // @return &str
    pub fn get_inner(&self) -> &str {
        &self.0
    }
}

impl ANNamingInput {
    // A special naming input representing "fin".
    // This is used in various places to indicate the end of a process or sequence.
    pub (crate) fn fin() -> Self {
        ANNamingInput("fin".to_string())
    }
}

impl Display for ANNamingInput {
    // Formats the ANNamingInput for display.
    // @param f: the formatter
    // @return std::fmt::Result
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}