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
    //! Checked names for tasks, plugins, observers, notifications and log files.
    //!
    //! - [`NamedInput`] holds a general name: not empty and without control characters. Names end up
    //!   in log lines and thread names, and a NUL in a thread name makes spawning panic.
    //! - [`LogFileNamedInput`] holds a log-file name, which becomes part of a file path: ASCII letters
    //!   and digits only, and not a Windows device name such as `CON`.
    //!
    //! Both types can only be built through constructors that check, so holding one means the name
    //! was checked. Thryve's own fixed names have constructors that take no text and can't fail.

    use std::{borrow::Borrow, fmt::Display};

    use crate::errors::{NameInputType, NamingError};

    /// A checked general name: not empty and without control characters.
    ///
    /// # Example
    ///
    /// ```
    /// use thryve_utils::errors::NameInputType;
    /// use thryve_utils::extras::named_input::NamedInput;
    ///
    /// let name = NamedInput::new("Valid-Name 1", NameInputType::TaskName).unwrap();
    /// assert_eq!(name.inner(), "Valid-Name 1");
    ///
    /// assert!(NamedInput::new("", NameInputType::TaskName).is_err());
    /// ```
    #[derive(Clone, PartialEq, Eq, Hash)]
    pub struct NamedInput(String);

    impl NamedInput {
        /// Checks `s` and wraps it.
        ///
        /// # Arguments
        ///
        /// * `s` - The name to check
        /// * `kind` - What the name is for; the error reports it
        ///
        /// # Errors
        ///
        /// - [`NamingError::NameEmpty`] if `s` is empty;
        /// - [`NamingError::NameContainsControlChar`] if `s` contains a control character.
        ///
        /// # Example
        ///
        /// ```
        /// use thryve_utils::errors::{NameInputType, NamingError};
        /// use thryve_utils::extras::named_input::NamedInput;
        ///
        /// let name = NamedInput::new("ValidName", NameInputType::PlgType)?;
        /// assert_eq!(name.inner(), "ValidName");
        /// # Ok::<(), NamingError>(())
        /// ```
        pub fn new(s: &str, kind: NameInputType) -> Result<Self, NamingError> {
            if s.is_empty() {
                return Err(NamingError::NameEmpty { kind });
            }

            if s.chars().any(|c| c.is_control()) {
                return Err(NamingError::NameContainsControlChar {
                    name: s.to_string(),
                    kind,
                });
            }

            Ok(NamedInput(s.to_owned()))
        }

        /// Returns a reference to the inner string.
        ///
        /// # Returns
        ///
        /// A string slice reference to the wrapped value.
        ///
        /// # Example
        ///
        /// ```
        /// use thryve_utils::errors::NameInputType;
        /// use thryve_utils::extras::named_input::NamedInput;
        ///
        /// let name = NamedInput::new("MyName", NameInputType::TaskName).unwrap();
        /// assert_eq!(name.inner(), "MyName");
        /// ```
        pub fn inner(&self) -> &str {
            &self.0
        }
    }

    // Thryve's own fixed names. They take no text, so they can't fail.
    impl NamedInput {
        /// `TopologyHandler`: the topology handler's observer type, and the receiver of topology
        /// notifications.
        pub fn new_topo_handler() -> Self {
            NamedInput(String::from("TopologyHandler"))
        }

        /// `AddNode`: the type of the notification that adds a node to the task graph.
        pub fn new_topo_add_node() -> Self {
            NamedInput(String::from("AddNode"))
        }

        /// `AddEdge`: the type of the notification that adds an edge to the task graph.
        pub fn new_topo_add_edge() -> Self {
            NamedInput(String::from("AddEdge"))
        }

        /// `Logging`: the logger's observer type, and the receiver of log notifications.
        pub fn new_logging() -> Self {
            NamedInput(String::from("Logging"))
        }

        /// `Log`: the type of a regular log notification, from
        /// [`Log::log`](crate::packets::notif_log::Log::log).
        pub fn new_logging_log() -> Self {
            NamedInput(String::from("Log"))
        }

        /// `Error`: the type of an error log notification, from
        /// [`Log::err`](crate::packets::notif_log::Log::err).
        pub fn new_logging_err() -> Self {
            NamedInput(String::from("Error"))
        }

        /// `Empty`: the name of the placeholder task from
        /// [`Task::new_empty`](crate::packets::task::Task::new_empty).
        pub fn new_empty() -> Self {
            NamedInput(String::from("Empty"))
        }

        /// `DefTask`: the name of the default task, from
        /// [`Task::new_default`](crate::packets::task::Task::new_default).
        pub fn new_def_task() -> Self {
            NamedInput(String::from("DefTask"))
        }
    }

    /// Lets maps keyed by `NamedInput` be searched with plain text. Sound because `Hash` and `Eq`
    /// come straight from the inner `String`.
    impl Borrow<str> for NamedInput {
        fn borrow(&self) -> &str {
            self.inner()
        }
    }

    impl Display for NamedInput {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    /// A checked log-file name: ASCII letters and digits only.
    ///
    /// It's the key the logger looks files up by. The user's names are stored in uppercase, and so are
    /// the built-in `MAIN`, `ERROR` and `DEBUG`, so a lookup ignores letter case once the requested name
    /// is uppercased too. The files on disk keep their normal spelling.
    #[derive(Clone, PartialEq, Eq, Hash)]
    pub struct LogFileNamedInput(String);

    impl LogFileNamedInput {
        /// Checks only the form of `s`: not empty, ASCII letters and digits only.
        ///
        /// # Errors
        ///
        /// [`NamingError::NameEmpty`] or [`NamingError::LogFileNameASCII`].
        pub fn new(s: &str) -> Result<Self, NamingError> {
            validate_input(s)?;

            Ok(LogFileNamedInput(s.to_owned()))
        }

        /// Checks a log-file name the user registers at startup.
        ///
        /// On top of the form check in [`new`](Self::new), the name can't be one of Thryve's own files
        /// (`Main`, `Error`, `Debug`) or a Windows device name, both compared ignoring case.
        ///
        /// # Errors
        ///
        /// The errors of [`new`](Self::new), [`NamingError::LogFileNameReservedThryve`] or
        /// [`NamingError::LogFileNameReserved`].
        pub fn new_register(s: &str) -> Result<Self, NamingError> {
            validate_input(s)?;

            if s.eq_ignore_ascii_case("Main")
                || s.eq_ignore_ascii_case("Error")
                || s.eq_ignore_ascii_case("Debug")
            {
                return Err(NamingError::LogFileNameReservedThryve {
                    name: s.to_string(),
                });
            }

            if is_device_name(s) {
                return Err(NamingError::LogFileNameReserved {
                    name: s.to_string(),
                });
            }

            Ok(LogFileNamedInput(s.to_owned()))
        }

        /// The key of Thryve's own Main.log.
        pub fn new_main() -> Self {
            Self(String::from("MAIN"))
        }

        /// The key of Thryve's own Error.log.
        pub fn new_err() -> Self {
            Self(String::from("ERROR"))
        }

        /// The key of Thryve's own Debug.log.
        pub fn new_debug() -> Self {
            Self(String::from("DEBUG"))
        }

        /// Returns a reference to the inner string.
        ///
        /// # Returns
        ///
        /// A string slice reference to the wrapped value.
        ///
        /// # Example
        ///
        /// ```
        /// use thryve_utils::extras::named_input::LogFileNamedInput;
        ///
        /// assert_eq!(LogFileNamedInput::new_main().inner(), "MAIN");
        /// ```
        pub fn inner(&self) -> &str {
            &self.0
        }
    }

    impl Borrow<str> for LogFileNamedInput {
        fn borrow(&self) -> &str {
            self.inner()
        }
    }

    /// The form check every log-file name gets: not empty, and ASCII letters and digits only.
    ///
    /// Allowing nothing else also rules out path separators, dots, spaces, and the superscript
    /// digits that Windows accepts in device names.
    fn validate_input(s: &str) -> Result<(), NamingError> {
        if s.is_empty() {
            return Err(NamingError::NameEmpty {
                kind: NameInputType::LogFileName,
            });
        }

        if !s.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(NamingError::LogFileNameASCII {
                name: s.to_string(),
            });
        }

        Ok(())
    }

    /// Returns whether `s` is a Windows reserved device name, ignoring case: `CON`, `PRN`, `AUX`,
    /// `NUL`, or `COM`/`LPT` followed by one digit.
    ///
    /// Older Windows versions treat such a name as the device even with an extension (`NUL.log` is
    /// the null device), so a log file with that name would lose its lines. The superscript forms
    /// (`COM¹`) need no check here, because log-file names are ASCII only.
    fn is_device_name(s: &str) -> bool {
        let is_port = s.len() == 4
            && s.get(..3)
                .is_some_and(|p| p.eq_ignore_ascii_case("COM") || p.eq_ignore_ascii_case("LPT"))
            && s.as_bytes()[3].is_ascii_digit();

        is_port
            || ["CON", "PRN", "AUX", "NUL"]
                .iter()
                .any(|d| s.eq_ignore_ascii_case(d))
    }

    impl Display for LogFileNamedInput {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }
}

pub mod jobs {
    //! Job types for tasks and observers.
    //!
    //! # Job Types
    //!
    //! - `TaskJob`: the boxed closure a task runs. It's internal to `thryve_utils`:
    //!   [`Task::new`](crate::packets::task::Task::new) and
    //!   [`Plugin::new`](crate::bases::plugin::Plugin::new) take a plain closure and build it.
    //! - [`ObsJob`]: Closure for observer initialization
    use std::sync::Arc;

    use crate::bases::observer::Observer;
    use crate::res::{SealedHandoff, TaskResult};

    /// The closure a task runs, boxed: it takes no arguments and returns a [`TaskResult`].
    ///
    /// Internal to `thryve_utils`. [`Task::new`](crate::packets::task::Task::new),
    /// `Task::new_init` and [`Plugin::new`](crate::bases::plugin::Plugin::new) take a plain closure
    /// and wrap it in one, so the code that writes tasks never names this type. A worker runs it
    /// once, through [`call_once`](TaskJob::call_once).
    ///
    /// It's a newtype around `Box<dyn FnOnce() -> TaskResult + Send + Sync>` so that it can
    /// implement `Default`: the placeholder that reports `BIGERR` if it ever runs, which means a
    /// job was taken twice.
    pub(crate) struct TaskJob {
        /// The inner closure that executes the task.
        inner: Box<dyn FnOnce() -> TaskResult + Send>,
    }

    impl TaskJob {
        /// Creates a new `TaskJob` from a closure.
        ///
        /// # Arguments
        ///
        /// * `closure` - A closure that takes no arguments and returns a `TaskResult`
        ///
        /// # Returns
        ///
        /// A new `TaskJob` wrapping the closure.
        pub fn new(closure: impl FnOnce() -> TaskResult + Send + 'static) -> Self {
            TaskJob {
                inner: Box::new(closure),
            }
        }

        /// A job that does nothing and reports nothing: its result has an empty type, message and log
        /// file.
        pub fn new_empty() -> Self {
            TaskJob {
                inner: Box::new(|| TaskResult::new("", "".to_string(), "")),
            }
        }

        /// Calls the task job and returns the result.
        ///
        /// This consumes the `TaskJob` and executes the inner closure.
        /// After calling this method, the `TaskJob` can no longer be used.
        ///
        /// # Returns
        ///
        /// The [`TaskResult`] returned by the closure.
        pub fn call_once(self) -> TaskResult {
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
                    TaskResult::new(
                        "BIGERR",
                        "If This Is Ever Called, Something is Wrong".to_string(),
                        "Main",
                    )
                }),
            }
        }
    }

    impl From<Box<dyn FnOnce() -> TaskResult + Send>> for TaskJob {
        /// Converts a `Box<dyn FnOnce() -> TaskResult + Send + Sync>` into a `TaskJob`.
        ///
        /// Lets callers construct a `TaskJob` directly from a boxed closure via
        /// `TaskJob::from(boxed_closure)` or the `Into` trait, without going
        /// through [`TaskJob::new`].
        fn from(boxed: Box<dyn FnOnce() -> TaskResult + Send>) -> Self {
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
        /// Lets callers construct an `ObsJob` directly from a boxed closure via
        /// `ObsJob::from(boxed_closure)`, without going through [`ObsJob::new`].
        fn from(boxed: Box<dyn Fn(Arc<Observer>) + Send + Sync>) -> Self {
            ObsJob { inner: boxed }
        }
    }

    /// What a task runs: a job it can run now, one that waits for another task's handoff, or a
    /// join's, which waits for several.
    pub(crate) enum Job {
        /// Runs as soon as a worker takes the task.
        TaskReady(TaskJob),
        /// A follow-up declared with `then`: the engine holds it until the task it waits on
        /// finishes, then gives it that task's sealed handoff, which makes it ready.
        TaskWait {
            /// The id of the task it waits on.
            on: u64,
            /// Whether its priority was inherited from the task it waits on (`then`), rather than
            /// given (`then_with_priority`). An inherited priority is capped below realtime when
            /// the task becomes ready.
            inherits_priority: bool,
            /// Takes the sealed handoff of the task it waits on, and returns this task's result.
            input_job: Box<dyn FnOnce(SealedHandoff) -> TaskResult + Send>,
        },
        /// A join declared with `join`: the engine fills its slots as the tasks it waits on
        /// finish, then makes it ready, once every slot is full or as soon as one task fails.
        JoinWait {
            /// One slot per task, in the order the handles were given: the task's id, and its
            /// sealed handoff once it has arrived.
            slots: Vec<(u64, Option<SealedHandoff>)>,
            /// Whether its priority was inherited from the tasks it waits on (`join`: the highest
            /// of theirs), rather than given (`join_with_priority`). An inherited priority is
            /// capped below realtime when the task becomes ready.
            inherits_priority: bool,
            /// How many slots are still empty.
            remaining_inputs: usize,
            /// Takes the slots' handoffs, in order, some of them still empty if a task failed
            /// first, and returns this task's result.
            input_job: Box<dyn FnOnce(Vec<Option<SealedHandoff>>) -> TaskResult + Send>,
        },
    }
}

/// The one line format that every line in Thryve's log files uses, and the one way to write it.
///
/// The logger observer, [`Ctx::e_log`](crate::cfg::ctx::Ctx::e_log), the topology handler and the
/// panic hook all build their lines with [`log_line`](log_format::log_line), so `Main.log`,
/// `Error.log` and the other log files read as one format, whoever wrote a line. They write them
/// with [`write_line`](log_format::write_line), so a line whose file can't be written goes to
/// stderr instead of being lost.
///
/// Public only because Thryve's other crates use it; it isn't part of the public API.
#[doc(hidden)]
pub mod log_format {
    use chrono::{DateTime, Local};
    use std::fs::File;
    use std::io::{self, Write};
    use std::time::SystemTime;

    /// Formats one log line.
    ///
    /// `source` says who wrote the line: `LOGGING` for the logger observer, `STARTUP` for a
    /// failed startup written by [`Ctx::e_log`](crate::cfg::ctx::Ctx::e_log), `TOPOLOGY` for the
    /// topology handler, and `PANIC` for the panic hook. `event_type` says what kind of line it
    /// is, such as `Log`, `Error` or `Panic`. The line ends with a newline, so it can be written
    /// with a single call.
    ///
    /// Control characters in `source`, `event_type` and `message` are escaped (a newline becomes
    /// `\n`, an escape character `\u{1b}`). So one entry stays one line, and a terminal showing
    /// the log runs no escape codes hidden in a message. Everything else, other alphabets
    /// included, is kept as it is.
    ///
    /// ```text
    /// Log_Creation: 2024-01-15 10:30:45, [ STARTUP ], Event_Type: Error, Message: failed to start up: ...
    /// ```
    ///
    /// ```
    /// use std::time::SystemTime;
    /// use thryve_utils::extras::log_format::log_line;
    ///
    /// let line = log_line(SystemTime::now(), "LOGGING", "Log", "first\nsecond\u{1b}[31m");
    /// assert!(line.ends_with("Message: first\\nsecond\\u{1b}[31m\n"));
    /// assert_eq!(line.lines().count(), 1);
    /// ```
    pub fn log_line(
        timestamp: SystemTime,
        source: &str,
        event_type: &str,
        message: &str,
    ) -> String {
        let datetime: DateTime<Local> = timestamp.into();
        format!(
            "Log_Creation: {}, [ {} ], Event_Type: {}, Message: {}\n",
            datetime.format("%Y-%m-%d %H:%M:%S"),
            escape_control(source),
            escape_control(event_type),
            escape_control(message),
        )
    }

    /// Escapes the control characters in `text` and keeps everything else as it is.
    fn escape_control(text: &str) -> String {
        let mut escaped = String::with_capacity(text.len());

        for c in text.chars() {
            if c.is_control() {
                escaped.extend(c.escape_default());
            } else {
                escaped.push(c);
            }
        }

        escaped
    }

    /// Writes one finished line to `file`.
    ///
    /// If the write fails (for example, because the disk is full), the line goes to stderr
    /// instead, behind a note with the reason. So the line isn't lost, and whoever reads stderr
    /// can fix the cause:
    ///
    /// ```text
    /// [could not write to its log file: There is not enough space on the disk. (os error 112)] Log_Creation: ...
    /// ```
    pub fn write_line(file: &File, line: &str) {
        let mut writer = file;

        if let Err(e) = writer.write_all(line.as_bytes()) {
            write_stderr(&format!("[could not write to its log file: {e}] {line}"));
        }
    }

    /// Writes `text` to stderr, and ignores a failure.
    ///
    /// It's the last place left to report anything, so a failure here has nowhere to go. Unlike
    /// `eprint!`, which panics when stderr can't be written, this can't fail. In the Windows
    /// service, stderr goes nowhere.
    pub fn write_stderr(text: &str) {
        let _ = io::stderr().lock().write_all(text.as_bytes());
    }
}

/// Thryve's panic hook.
///
/// `init_panic_hook` replaces Rust's default panic hook with one that writes each panic, with
/// its time and location, to `Error.log` and to stderr, and then ends the process with exit code 1.
pub mod panic_hook {
    use std::fs::OpenOptions;
    use std::io::Write;
    use std::panic;
    use std::time::SystemTime;

    use super::log_format::{log_line, write_stderr};
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

            // One line in the logger's format, written to both Error.log and stderr.
            let line = log_line(
                SystemTime::now(),
                "PANIC",
                "Panic",
                &format!("{message} at {location}"),
            );

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
                let _ = file.write_all(line.as_bytes());
            }

            // Also print to stderr (for non-service mode debugging). `eprint!` would panic if
            // stderr can't be written, and a panic inside the panic hook aborts the process.
            write_stderr(&line);
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
    use std::cell::{Cell, RefCell};

    thread_local! {
        /// Thread-local storage for the currently executing task's fingerprint.
        static CURRENT_TASK_FINGERPRINT: RefCell<String> = RefCell::new(String::default());

        static WORKER_PARK_IDX: Cell<usize> = const { Cell::new(0) };
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

    /// Records the current thread's slot in the worker registry.
    ///
    /// A worker thread must call this once, with its own index, before it takes tasks: the queues
    /// mark that slot while the worker is parked waiting for work.
    pub fn tl_set_worker_park_idx(idx: usize) {
        WORKER_PARK_IDX.with(|c| c.set(idx));
    }

    /// Returns the current thread's slot in the worker registry, as set by
    /// [`tl_set_worker_park_idx`]. It's 0 on a thread that never set it.
    pub fn tl_get_worker_park_idx() -> usize {
        WORKER_PARK_IDX.with(Cell::get)
    }
}
/// Finding the executable's folder, and creating the folders Thryve keeps next to it.
pub mod dir_paths {
    use crate::{errors::StartupError, extras::misc::get_current_date_time};
    use std::{
        env::current_exe,
        fs::create_dir_all,
        path::{Path, PathBuf},
        process::Command,
    };

    /// The folders Thryve creates next to the executable.
    #[derive(Clone, PartialEq, Eq, Hash)]
    pub enum DirName {
        /// `log/<timestamp>`: this run's log files. Each run gets a new folder.
        Log,
        /// `topology`: where graph mode writes the task graph.
        Topology,
    }

    /// Gets the current executable's directory.
    ///
    /// # Returns
    ///
    /// * `Ok(PathBuf)` - The directory path
    /// * `Err(StartupError::RootFolder)` - If `current_exe()` fails; the OS error is its source
    /// * `Err(StartupError::NoParentFolder)` - If the executable has no parent directory
    pub fn get_current_exec_dir() -> Result<PathBuf, StartupError> {
        let exe_dir = current_exe()
            .map_err(|e| StartupError::RootFolder { source: e })?
            .parent()
            .ok_or(StartupError::NoParentFolder)?
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
    /// * `Err(StartupError)` - Any error from [`get_current_exec_dir`], or
    ///   `LogDirCreation` / `TopologyDirCreation` if the directory can't be created (the OS error is its source)
    pub fn create_dir(file_path: DirName) -> Result<PathBuf, StartupError> {
        let current_root_dir = get_current_exec_dir()?;

        match file_path {
            DirName::Log => {
                // Create a log directory with a timestamp to store the log files.
                let datetime = get_current_date_time();
                let log_dir_path = current_root_dir
                    .join("log")
                    .join(datetime.format("%Y_%m_%d_%H_%M").to_string());
                create_dir_all(&log_dir_path).map_err(|e| StartupError::LogDirCreation {
                    path: log_dir_path.clone(),
                    source: e,
                })?;

                Ok(log_dir_path)
            }

            DirName::Topology => {
                let topology_dir_path = current_root_dir.join("topology");
                create_dir_all(&topology_dir_path).map_err(|e| {
                    StartupError::TopologyDirCreation {
                        path: topology_dir_path.clone(),
                        source: e,
                    }
                })?;

                Ok(topology_dir_path)
            }
        }
    }

    /// Gets the workspace root directory by running `cargo locate-project`.
    ///
    /// Graph mode uses it to show file paths relative to the workspace.
    ///
    /// # Returns
    ///
    /// The folder that holds the workspace's `Cargo.toml`, or `None` if it can't be found:
    /// cargo can't be run, the program doesn't run inside a cargo project (a deployed program,
    /// for example), or cargo's answer can't be read.
    pub fn get_workspace_root() -> Option<PathBuf> {
        // Look up the cargo executable used to build the project
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());

        let output = Command::new(cargo)
            .arg("locate-project")
            .arg("--workspace")
            .arg("--message-format=plain")
            .output()
            .ok()?;

        // Outside a cargo project, cargo fails and prints nothing to stdout.
        if !output.status.success() {
            return None;
        }

        let stdout = str::from_utf8(&output.stdout).ok()?;

        // Pop "Cargo.toml" off the path to get the root directory
        Path::new(stdout.trim()).parent().map(Path::to_path_buf)
    }
}

/// Small helpers that don't fit anywhere else.
pub mod misc {
    use chrono::{DateTime, Local};
    use std::time::SystemTime;

    /// Returns the current local date and time.
    pub fn get_current_date_time() -> DateTime<Local> {
        SystemTime::now().into()
    }
}
