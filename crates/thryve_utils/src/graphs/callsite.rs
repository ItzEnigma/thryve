// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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
//! enabled in the debugging configuration. `PacketHandle::send_task` and
//! `TaskResult::chain` record the file and line they were called from, which
//! the compiler passes in (`#[track_caller]`).
//!
//! # Example
//!
//! ```no_run
//! use thryve_utils::graphs::callsite::TaskCallSite;
//! use std::path::PathBuf;
//!
//! let callsite = TaskCallSite::new(
//!     PathBuf::from("src/my_module.rs"),
//!     42,
//!     "MyTask".to_string(),
//! );
//!
//! let fingerprint = callsite.fingerprint();
//! ```

use serde::{Deserialize, Serialize};
use std::fmt::Display;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::PathBuf;

/// Represents a call site in the source code where a task is sent.
///
/// A `TaskCallSite` captures the location in the source code where a
/// task is sent or chained: the file path, the line number, and the task
/// name. File, line and name identify a call site, and the topology graph
/// labels its nodes with them.
///
/// # Fields
///
/// - `file_path`: The path to the source file, as the compiler saw it
/// - `line_number`: The line number in the source file
/// - `callsite_name`: The name of the task being sent
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
///     42,
///     "MyTask".to_string(),
/// );
/// ```
#[derive(Debug, Clone, Hash, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TaskCallSite {
    /// The path to the source file where the task is created.
    pub file_path: PathBuf,
    /// The line number in the source file.
    pub line_number: u32,
    /// The name of the task being created.
    pub callsite_name: String,
    /// A unique identifier for this call site, computed from the struct fields.
    pub this_callsite_fingerprint: String,
    /// The fingerprint of the parent task that created this task.
    pub current_executing_task_fingerprint: String,
}

impl Display for TaskCallSite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "callsite name: {}, file: {}, line: {}, task fingerprint: {}",
            self.callsite_name,
            self.file_path.display(),
            self.line_number,
            self.this_callsite_fingerprint,
        )
    }
}

impl TaskCallSite {
    /// Creates a new `TaskCallSite` with the specified information.
    ///
    /// Initializes a new call site with the given file path, line number,
    /// and task name. The fingerprint fields are initialized
    /// to empty strings and should be set after creation.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The path to the source file
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
    ///     42,
    ///     "MyTask".to_string(),
    /// );
    /// ```
    pub fn new(file_path: PathBuf, line_num: u32, name: String) -> Self {
        let absolute_path = file_path;

        Self {
            file_path: absolute_path,
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

        hasher.finish().to_string()
    }
}
