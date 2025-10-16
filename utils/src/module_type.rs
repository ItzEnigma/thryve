use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModuleType(String);

impl ModuleType {
    pub fn new(s: &str) -> Self {
        assert!(s.chars().all(|c| c.is_alphanumeric()));
        ModuleType(s.to_owned())
    }

    pub fn get_module_type(&self) -> &str {
        &self.0
    }
}

impl Display for ModuleType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}