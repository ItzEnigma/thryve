//! Task call site representation for graph visualization.
//!
//! This module provides the [`TaskCallSite`] struct, which captures
//! information about where a task is called from in the source code.
//! This information is used to build a topology graph showing task
//! relationships and call hierarchies.
//!
//! # Usage
//!
//! Call sites are automatically captured when graph visualization is
//! enabled in the debugging configuration. The engine captures stack
//! traces at task creation points and extracts relevant information
//! about the calling location.
//!
//! # Example
//!
//! ```no_run
//! use thryve_utils::graphs::callsite::TaskCallSite;
//! use std::path::PathBuf;
//!
//! let callsite = TaskCallSite::new(
//!     PathBuf::from("src/my_module.rs"),
//!     "my_crate::my_module".to_string(),
//!     42,
//!     "MyTask".to_string(),
//! );
//!
//! let fingerprint = callsite.fingerprint();
//! ```

use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;
use serde::{Serialize, Deserialize};

/// Represents a call site in the source code where a task is created.
///
/// A `TaskCallSite` captures the location in the source code where a
/// task is instantiated, including the file path, module path, line
/// number, and task name. This information is used to build a topology
/// graph that visualizes task relationships and call hierarchies.
///
/// # Fields
///
/// - `file_path`: The path to the source file where the task is created
/// - `module_path`: The Rust module path (e.g., "my_crate::my_module")
/// - `line_number`: The line number in the source file
/// - `callsite_name`: The name of the task being created
/// - `this_callsite_fingerprint`: A unique identifier for this call site
/// - `current_executing_task_fingerprint`: The fingerprint of the parent task
///
/// # Example
///
/// ```no_run
/// use thryve_utils::graphs::callsite::TaskCallSite;
/// use std::path::PathBuf;
///
/// let callsite = TaskCallSite::new(
///     PathBuf::from("src/my_module.rs"),
///     "my_crate::my_module".to_string(),
///     42,
///     "MyTask".to_string(),
/// );
/// ```
#[derive(Debug, Clone, Hash, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TaskCallSite {
    /// The path to the source file where the task is created.
    pub file_path: PathBuf,
    /// The Rust module path (e.g., "my_crate::my_module").
    pub module_path: String,
    /// The line number in the source file.
    pub line_number: u32,
    /// The name of the task being created.
    pub callsite_name: String,
    /// A unique identifier for this call site, computed from the struct fields.
    pub this_callsite_fingerprint: String,
    /// The fingerprint of the parent task that created this task.
    pub current_executing_task_fingerprint: String,
}

impl TaskCallSite {
    /// Creates a new `TaskCallSite` with the specified information.
    ///
    /// Initializes a new call site with the given file path, module path,
    /// line number, and task name. The fingerprint fields are initialized
    /// to empty strings and should be set after creation.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the source file
    /// * `module_path` - The Rust module path
    /// * `line_num` - The line number in the source file
    /// * `name` - The name of the task
    ///
    /// # Returns
    ///
    /// A new `TaskCallSite` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::callsite::TaskCallSite;
    /// use std::path::PathBuf;
    ///
    /// let callsite = TaskCallSite::new(
    ///     PathBuf::from("src/my_module.rs"),
    ///     "my_crate::my_module".to_string(),
    ///     42,
    ///     "MyTask".to_string(),
    /// );
    /// ```
    pub fn new(file_path: PathBuf, module_path: String, line_num: u32, name: String) -> Self {
        let absolute_path = file_path;

        Self {
            file_path: absolute_path,
            module_path: module_path,
            line_number: line_num,
            callsite_name: name,
            this_callsite_fingerprint: String::default(),
            current_executing_task_fingerprint: String::default(),
        }
    }

    /// Computes a unique fingerprint for this call site.
    ///
    /// Generates a string representation of the hash of all fields in
    /// the `TaskCallSite` struct. This fingerprint can be used to uniquely
    /// identify this call site across the application. The `this_callsite_fingerprint`
    /// and `current_executing_task_fingerprint` fields are still the default `String`
    /// value when this function is called. The `String` returned from this function
    /// is then placed into the `this_callsite_fingerprint` field of the `TaskCallSite`.
    ///
    /// # Returns
    ///
    /// A string representation of the hash value.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::graphs::callsite::TaskCallSite;
    /// use std::path::PathBuf;
    ///
    /// let callsite = TaskCallSite::new(
    ///     PathBuf::from("src/my_module.rs"),
    ///     "my_crate::my_module".to_string(),
    ///     42,
    ///     "MyTask".to_string(),
    /// );
    ///
    /// let fingerprint = callsite.fingerprint();
    /// println!("Call site fingerprint: {}", fingerprint);
    /// ```
    pub fn fingerprint(&self) -> String {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);

        let fingerprint = hasher.finish().to_string();

        fingerprint
    }
}
