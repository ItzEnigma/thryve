// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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
    #[derive(Clone, PartialEq, Eq, Hash, Default)]
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
    //! Job types for task and observer initialization.
    //!
    //! This module defines the [`TaskJob`] and [`ObsJob`] types used
    //! as jobs in the Thryve system. These types simplify the
    //! type signatures throughout the codebase.
    //!
    //! # Job Types
    //!
    //! - [`TaskJob`]: Wrapper for task execution closures
    //! - [`ObsJob`]: Closure for observer initialization
    use std::sync::Arc;

    use crate::bases::observer::Observer;
    use crate::res;

    /// A task execution closure wrapper.
    ///
    /// A `TaskJob` is a wrapper around a closure that takes no arguments and returns
    /// a [`Result`](crate::res::Result). Tasks are executed by worker
    /// threads, and the result indicates success or failure along with
    /// optional follow-up actions.
    ///
    /// This type is a newtype wrapper around `Box<dyn FnOnce() -> Result + Send + Sync>`
    /// to allow implementing `Default` (since the orphan rules prevent
    /// implementing traits for type aliases).
    ///
    /// # Traits
    ///
    /// - `FnOnce`: Can be called exactly once (via [`call_once`](TaskJob::call_once))
    /// - `Send`: Can be sent to another thread
    /// - `Sync`: Can be shared between threads
    /// - `Default`: Returns an error function as a placeholder
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::extras::jobs::TaskJob;
    /// use thryve_utils::res::Result;
    ///
    /// let job: TaskJob = TaskJob::new(|| {
    ///     // Task execution logic
    ///     Result::new("Success", "Done".to_string(), "Main")
    /// });
    ///
    /// let result = job.call_once(());
    /// ```
    pub struct TaskJob {
        /// The inner closure that executes the task.
        inner: Box<dyn FnOnce() -> res::Result + Send + Sync>,
    }

    impl TaskJob {
        /// Creates a new `TaskJob` from a closure.
        ///
        /// # Arguments
        ///
        /// * `closure` - A closure that takes no arguments and returns a `Result`
        ///
        /// # Returns
        ///
        /// A new `TaskJob` wrapping the closure.
        pub fn new(closure: impl FnOnce() -> res::Result + Send + Sync + 'static) -> Self {
            TaskJob {
                inner: Box::new(closure),
            }
        }

        /// Calls the task job and returns the result.
        ///
        /// This consumes the `TaskJob` and executes the inner closure.
        /// After calling this method, the `TaskJob` can no longer be used.
        ///
        /// # Returns
        ///
        /// The [`Result`](crate::res::Result) returned by the closure.
        ///
        /// # Example
        ///
        /// ```no_run
        /// use thryve_utils::extras::jobs::TaskJob;
        /// use thryve_utils::res::Result;
        ///
        /// let job: TaskJob = TaskJob::new(|| {
        ///     Result::new("Success", "Done".to_string(), "Main")
        /// });
        ///
        /// let result = job.call_once(());
        /// ```
        pub fn call_once(self) -> res::Result {
            (self.inner)()
        }
    }

    impl Default for TaskJob {
        /// Returns a default error function as a placeholder.
        ///
        /// This function is used as a replacement for `plg_entry_point` after
        /// the original function has been extracted. If this function is ever
        /// executed, it indicates a serious error in the plugin loading process.
        ///
        /// # Returns
        ///
        /// A `TaskJob` that returns an error result indicating a critical failure.
        fn default() -> Self {
            TaskJob {
                inner: Box::new(|| {
                    res::Result::new(
                        "BIGERR",
                        "If This Is Ever Called, Something is Wrong".to_string(),
                        "Main",
                    )
                }),
            }
        }
    }

    impl From<Box<dyn FnOnce() -> res::Result + Send + Sync>> for TaskJob {
        /// Converts a `Box<dyn FnOnce() -> Result + Send + Sync>` into a `TaskJob`.
        ///
        /// This implementation allows seamless migration from the old type alias
        /// pattern to the new struct pattern. Code using `Box::new(|| { ... })`
        /// can continue to work with `TaskJob::from(boxed_closure)` or via
        /// the `Into` trait.
        fn from(boxed: Box<dyn FnOnce() -> res::Result + Send + Sync>) -> Self {
            TaskJob { inner: boxed }
        }
    }

    /// A wrapper for observer initialization closures.
    ///
    /// An `ObsJob` wraps a closure that takes an [`Arc<Observer>`](crate::bases::observer::Observer)
    /// and returns nothing. This closure is used to initialize observers
    /// with their specific logic, typically starting a loop that processes
    /// notifications until shutdown.
    ///
    /// This type is a newtype wrapper around `Box<dyn Fn(Arc<Observer>) + Send + Sync>`
    /// to allow implementing `Default` (since the orphan rules prevent
    /// implementing traits for type aliases).
    ///
    /// # Traits
    ///
    /// - `Fn`: Can be called multiple times (though typically called once)
    /// - `Send`: Can be sent to another thread
    /// - `Sync`: Can be shared between threads
    /// - `Default`: Returns an empty closure as a placeholder
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::extras::jobs::ObsJob;
    /// use thryve_utils::bases::observer::Observer;
    /// use std::sync::Arc;
    ///
    /// let job: ObsJob = ObsJob::new(|observer: Arc<Observer>| {
    ///     // Downcast observer core to concrete type
    ///     // Start notification processing loop
    ///     // Clean up on exit
    /// });
    /// ```
    pub struct ObsJob {
        /// The inner closure that executes the observer job.
        inner: Box<dyn Fn(Arc<Observer>) + Send + Sync>,
    }

    impl ObsJob {
        /// Creates a new `ObsJob` from a closure.
        ///
        /// # Arguments
        ///
        /// * `closure` - A closure that takes an `Arc<Observer>` and returns nothing
        ///
        /// # Returns
        ///
        /// A new `ObsJob` wrapping the closure.
        pub fn new(closure: impl Fn(Arc<Observer>) + Send + Sync + 'static) -> Self {
            ObsJob {
                inner: Box::new(closure),
            }
        }

        /// Calls the observer job and executes the closure.
        ///
        /// This consumes the `ObsJob` and executes the inner closure.
        /// After calling this method, the `ObsJob` can no longer be used.
        ///
        /// # Arguments
        ///
        /// * `observer` - The observer to pass to the closure
        pub fn call_once(&self, observer: Arc<Observer>) {
            (self.inner)(observer);
        }
    }

    impl Default for ObsJob {
        /// Returns a default `ObsJob` that does nothing.
        fn default() -> Self {
            ObsJob {
                inner: Box::new(|_observer: Arc<Observer>| {}),
            }
        }
    }

    impl From<Box<dyn Fn(Arc<Observer>) + Send + Sync>> for ObsJob {
        /// Converts a boxed closure into an `ObsJob`.
        ///
        /// This implementation allows seamless migration from the old type alias
        /// pattern to the new struct pattern. Code using `Box::new(move |observer| { ... })`
        /// can continue to work with `ObsJob::from(boxed_closure)`.
        fn from(boxed: Box<dyn Fn(Arc<Observer>) + Send + Sync>) -> Self {
            ObsJob { inner: boxed }
        }
    }
}

pub mod panic_hook {
    use chrono::{DateTime, Local};
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::panic;
    use std::time::SystemTime;

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
            let message = panic_info
                .payload()
                .downcast_ref::<&str>()
                .copied()
                .or_else(|| {
                    panic_info
                        .payload()
                        .downcast_ref::<String>()
                        .map(|s| s.as_str())
                })
                .unwrap_or("Unknown panic");

            // Format location
            let location = panic_info
                .location()
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
                let _ = writeln!(
                    file,
                    "[PANIC] {}: {} at {}",
                    timestamp.format("%Y-%m-%d %H:%M:%S%.6f"),
                    message,
                    location
                );
                let _ = file.flush();
            }

            // Also print to stderr (for non-service mode debugging)
            eprintln!(
                "[PANIC] {}: {} at {}",
                timestamp.format("%Y-%m-%d %H:%M:%S%.6f"),
                message,
                location
            );
            let _ = std::io::stderr().flush();
            std::process::exit(1);
        }));
    }
}

/// Thread-local storage for the currently executing task's fingerprint.
///
/// This module provides thread-local storage for tracking which task is
/// currently executing on each thread. This is essential for building
/// the topology graph, as it allows the system to determine the parent
/// task when a new task is created.
///
/// # Usage
///
/// The current task fingerprint is set when a task begins execution
/// and cleared when it completes. When a new task is created via
/// [`PacketHandle::send_task`](crate::packets::packet_handle::PacketHandle::send_task),
/// the current fingerprint is read and attached to the new task's
/// call site as the `current_executing_task_fingerprint`.
pub mod thread_local {
    use std::cell::RefCell;

    thread_local! {
        /// Thread-local storage for the currently executing task's fingerprint.
        static CURRENT_TASK_FINGERPRINT: RefCell<String> = RefCell::new(String::default());
    }

    /// Sets the current task fingerprint for the calling thread.
    ///
    /// This should be called when a task begins execution to track
    /// which task is currently running on this thread.
    ///
    /// # Arguments
    ///
    /// * `fingerprint` - The fingerprint of the task being executed
    pub fn tl_set_current_task_fingerprint(fingerprint: String) {
        CURRENT_TASK_FINGERPRINT.with(|data| {
            *data.borrow_mut() = fingerprint;
        });
    }

    /// Gets the current task fingerprint for the calling thread.
    ///
    /// Returns the fingerprint of the task currently executing on
    /// this thread, or an empty string if no task is executing.
    ///
    /// # Returns
    ///
    /// The fingerprint string, or an empty string if not set
    pub fn tl_get_current_task_fingerprint() -> String {
        CURRENT_TASK_FINGERPRINT.with(|data| data.borrow().clone())
    }
}

/// Stack trace analysis for call site extraction.
///
/// This module provides functionality to analyze stack traces and extract
/// call site information for graph visualization. When a task is created,
/// a backtrace is captured and this module processes it to determine:
///
/// - The source file where the task was created
/// - The module path within the crate
/// - The line number of creation
///
/// # How It Works
///
/// 1. A backtrace is captured at the point of task creation
/// 2. The backtrace frames are analyzed to find the actual caller
/// 3. Symbol information is extracted (file path, line number, symbol name)
/// 4. The module path is parsed from the symbol name
/// 5. A [`TaskCallSite`](crate::graphs::callsite::TaskCallSite) is constructed
///
/// # Frame Skipping
///
/// The first 4 frames are skipped to bypass internal Thryve function calls
/// and reach the actual user code that created the task.
pub mod stack {
    use std::path::PathBuf;

    use backtrace::Backtrace;

    use crate::graphs::callsite::TaskCallSite;

    /// Extracts call site information from a backtrace.
    ///
    /// This function analyzes the provided backtrace to determine where
    /// a task was created in the source code. It skips internal Thryve
    /// frames and extracts file path, module path, line number, and
    /// symbol name information.
    ///
    /// # Arguments
    ///
    /// * `backtrace` - The backtrace to analyze
    /// * `name` - The name of the task being created
    ///
    /// # Returns
    ///
    /// A [`TaskCallSite`](crate::graphs::callsite::TaskCallSite) containing
    /// the extracted call site information.
    ///
    /// # Panics
    ///
    /// Panics if the backtrace cannot be properly analyzed or if the
    /// expected frame structure is not found.
    pub fn get_task_data(backtrace: Backtrace, name: String) -> TaskCallSite {
        // println!("\n=== ALL FRAMES ===");
        // for (i, frame) in backtrace.frames().iter().enumerate() {
        //     println!("Frame {}: {:?}", i, frame);
        //     if let Some(sym) = frame.symbols().first() {
        //         if let Some(name) = sym.name() {
        //             println!("  Symbol: {}", name);
        //         }
        //         if let Some(file) = sym.filename() {
        //             println!("  File: {}", file.display());
        //         }
        //         if let Some(line) = sym.lineno() {
        //             println!("  Line: {}", line);
        //         }
        //     }
        // }
        // println!("==================\n");

        let mut counter: u8 = 0;

        // Iterate through frames to find the actual caller
        for frame in backtrace.frames() {
            if counter != 4 {
                counter += 1;
                continue;
            }

            // Get symbol information for this frame
            if let Some(symbol) = frame.symbols().first()
                && let Some(filename) = symbol.filename()
            {
                // filename is &Path, convert to string and extract just the filename
                let filename = PathBuf::from(filename.to_string_lossy().to_string());

                if let Some(line_num) = symbol.lineno() {
                    let symbol_name = symbol
                        .name()
                        .map(|n| n.to_string())
                        .unwrap_or_else(|| "unknown".to_string());

                    // Parse the module path (everything before the last ::function_name)
                    let module_path = extract_module_path(&symbol_name);

                    return TaskCallSite::new(filename, module_path, line_num, name);
                }
            }
        }

        panic!("This function: get_task_data got called in a very wrong place");
    }

    fn extract_module_path(symbol_name: &str) -> String {
        // Remove common suffixes
        let cleaned = symbol_name
            .replace("::{{closure}}", "")
            .replace("::{{closure}}::{{closure}}", "")
            .replace("::{anonymous}", "");

        // Split by :: and take all but the last component (the function/method name)
        let parts: Vec<&str> = cleaned.split("::").collect();

        if parts.len() > 1 {
            parts[..parts.len() - 1].join("::")
        } else {
            cleaned.to_string()
        }
    }
}

pub mod dir_paths {
    use std::{
        env::current_exe,
        fs::create_dir_all,
        path::{Path, PathBuf},
        process::Command,
    };

    use crate::errors::ThryveUtilsError;
    use crate::extras::misc::get_current_date_time;

    #[derive(Clone, PartialEq, Eq, Hash)]
    pub enum DirName {
        Log,
        Topology,
    }

    /// Gets the current executable's directory.
    ///
    /// # Returns
    ///
    /// * `Ok(PathBuf)` - The directory path
    /// * `Err(ThryveUtilsError)` - If `current_exe()` fails
    pub fn get_current_exec_dir() -> Result<PathBuf, ThryveUtilsError> {
        let exe_dir = current_exe()
            .map_err(ThryveUtilsError::Io)?
            .parent()
            .ok_or_else(|| {
                ThryveUtilsError::Io(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Executable has no parent directory",
                ))
            })?
            .to_path_buf();

        Ok(exe_dir)
    }

    /// Creates a directory based on the given `DirName`.
    ///
    /// # Arguments
    ///
    /// * `file_path` - The type of directory to create
    ///
    /// # Returns
    ///
    /// * `Ok(PathBuf)` - The created directory path
    /// * `Err(ThryveUtilsError)` - If directory creation fails
    pub fn create_dir(file_path: DirName) -> Result<PathBuf, ThryveUtilsError> {
        let current_root_dir = get_current_exec_dir()?;

        match file_path {
            DirName::Log => {
                // Create a log directory with a timestamp to store the log files.
                let datetime = get_current_date_time();
                let log_dir_path = current_root_dir
                    .join("log")
                    .join(datetime.format("%Y_%m_%d_%H_%M").to_string());
                create_dir_all(&log_dir_path).map_err(|e| {
                    ThryveUtilsError::DirectoryCreation(format!(
                        "Failed to create log directory: {}\n{e}",
                        log_dir_path.display()
                    ))
                })?;

                Ok(log_dir_path)
            }

            DirName::Topology => {
                let topology_dir_path = current_root_dir.join("topology");
                create_dir_all(&topology_dir_path).map_err(|e| {
                    ThryveUtilsError::DirectoryCreation(format!(
                        "Failed to create topology directory: {}\n{e}",
                        topology_dir_path.display()
                    ))
                })?;

                Ok(topology_dir_path)
            }
        }
    }

    /// Gets the workspace root directory by running `cargo locate-project`.
    ///
    /// # Panics
    ///
    /// This function panics if:
    /// - The cargo command fails to execute
    /// - The output is not valid UTF-8
    /// - The cargo.toml path has no parent directory
    pub fn get_workspace_root() -> PathBuf {
        // Look up the cargo executable used to build the project
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());

        let output = Command::new(cargo)
            .arg("locate-project")
            .arg("--workspace")
            .arg("--message-format=plain")
            .output()
            .expect("Failed to execute cargo command");

        let stdout = str::from_utf8(&output.stdout).expect("Invalid UTF-8 output");
        let cargo_toml_path = Path::new(stdout.trim());

        // Pop "Cargo.toml" off the path to get the root directory
        cargo_toml_path.parent().unwrap().to_path_buf()
    }
}

pub mod misc {
    use chrono::{DateTime, Local};
    use std::time::SystemTime;

    pub fn get_current_date_time() -> DateTime<Local> {
        SystemTime::now().into()
    }
}
