// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Error types for the Thryve framework.
//!
//! Thryve reports its own failures with [`ThryveError`]. Each top-level case says what kind of
//! failure happened, which tells the caller what they can do about it. The inner enums
//! ([`StartupError`], [`ConfigError`], [`NamingError`], [`EngineNotRunningError`]) carry the
//! details: which step failed, which path or name was involved, what was dropped, and the
//! underlying cause.
//!
//! An error either returns its cause from [`source`](std::error::Error::source) or prints it in its
//! message, never both, so walking the chain prints every cause exactly once. `{}` prints only the
//! top line; [`get_err`] and [`get_err_oneline`] render the whole chain:
//!
//! ```text
//! failed to start up
//!   caused by: failed to create the log directory 'C:\app\log\2026_09_26_14_02'
//!   caused by: Access is denied. (os error 5)
//! ```

use std::{error::Error, fmt::Display, io, path::PathBuf};

/// Every failure Thryve itself can report.
///
/// The case says what kind of failure it is; the error inside carries the details and the cause.
/// The enum is `#[non_exhaustive]`: new cases can be added without breaking callers, so a `match`
/// on it needs a wildcard arm.
///
/// # Example
///
/// ```
/// use thryve_utils::errors::{NameInputType, NamingError, ThryveError, get_err};
///
/// let err = ThryveError::from(NamingError::NameEmpty {
///     kind: NameInputType::TaskName,
/// });
/// assert_eq!(
///     get_err(&err),
///     "failed naming check\n  caused by: task name must not be empty"
/// );
/// ```
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ThryveError {
    /// Starting Thryve failed: a folder, a log file or a thread couldn't be created, or something
    /// was registered twice.
    #[error("failed to start up")]
    Startup(#[from] StartupError),

    /// The configuration file couldn't be loaded: it couldn't be read, didn't parse, or has
    /// invalid values. [`ConfigError`] says which, and lists every invalid value.
    #[error("failed to properly configure Thryve")]
    Config(#[from] ConfigError),

    /// A name failed its check, for example an empty task name or a log-file name with a space.
    #[error("failed naming check")]
    Naming(#[from] NamingError),

    /// A task or notification was sent after the engine was gone, and was dropped.
    /// [`EngineNotRunningError`] says which of the two it was.
    #[error("engine is not running")]
    EngineNotRunning(#[from] EngineNotRunningError),

    #[error("An Error Occured while finalizing")]
    Finalization(#[from] FinalizationError),
}

/// Why starting Thryve failed.
///
/// Returned by the startup steps (creating the folders and log files, starting the threads) and
/// carried by [`ThryveError::Startup`]. The cases caused by an I/O failure keep the [`io::Error`]
/// as their [`source`](std::error::Error::source), so the OS error code isn't lost.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StartupError {
    /// The path of the running executable couldn't be read.
    #[error("failed to get the exe directory")]
    RootFolder {
        /// The error from [`std::env::current_exe`].
        source: io::Error,
    },

    /// The executable's path has no parent folder, so there's nowhere to put Thryve's folders.
    #[error("executable has no parent folder")]
    NoParentFolder,

    /// The `topology` folder used by graph mode couldn't be created.
    #[error("failed to create topology directory '{}'" , .path.display())]
    TopologyDirCreation {
        /// The folder that couldn't be created.
        path: PathBuf,
        /// The error from creating it.
        source: io::Error,
    },

    /// This run's timestamped log folder couldn't be created.
    #[error("failed to create the log directory '{}'" , .path.display())]
    LogDirCreation {
        /// The folder that couldn't be created.
        path: PathBuf,
        /// The error from creating it.
        source: io::Error,
    },

    /// A plugin type, an observer type or a log-file name was registered more than once.
    ///
    /// Log-file names are compared ignoring letter case, because on Windows `Payments.log` and
    /// `payments.log` are the same file.
    #[error("{kind} {name:?} cannot be registered more than once")]
    RegisterDuplicate {
        /// The name that was registered again.
        name: String,
        /// What the name is for: a plugin type, an observer type or a log-file name.
        kind: NameInputType,
    },

    /// A log file couldn't be opened for appending.
    #[error("failed to open the log file '{}'" , .path.display())]
    LogFileOpen {
        /// The log file's path.
        path: PathBuf,
        /// The error from opening it.
        source: io::Error,
    },

    /// One of Thryve's threads (the engine thread, a worker, an observer or the notification
    /// sorter) couldn't be started.
    #[error("failed to start thread: '{thread_name}'")]
    ThreadInit {
        /// The name the thread would have had.
        thread_name: String,
        /// The error from spawning it.
        source: io::Error,
    },

    /// The engine thread ended before it was handed its engine.
    ///
    /// The engine thread only waits until then, so this shouldn't happen. If it does, the set-up
    /// engine is stopped again, so no thread is left running.
    #[error("the engine thread ended before it received the engine")]
    EngineThreadEnded,

    /// The Windows service couldn't connect to the service manager, usually because the process
    /// wasn't started by it (for example, it was run from a console).
    ///
    /// The engine was already set up by then; it's stopped again before this is returned.
    #[error("the service couldn't connect to the service control manager: {error}")]
    Service {
        /// The service crate's reason. It's plain text rather than an error value, so it's part
        /// of the message instead of a [`source`](std::error::Error::source).
        error: String,
    },

    /// The logger was created before `Error.log` was opened.
    ///
    /// The logger keeps `Error.log` in its own field, so it checks for it once, when it's
    /// created. The startup sequence opens the log files first, so this shouldn't happen.
    #[error("failed to fetch a log file from startup map")]
    LogFileFetch,

    /// The topology handler was created before the topology folder was recorded.
    ///
    /// The handler keeps the folder in its own field, so it checks for it once, when it's
    /// created. The startup sequence creates the folder first in graph mode, so this shouldn't
    /// happen.
    #[error("failed to fetch a directory path")]
    DirFetch,
}

/// Why a name was rejected.
///
/// General names (task names, plugin and observer types, notification types and receivers) must
/// not be empty or contain control characters. Log-file names are stricter, because they become
/// file names: ASCII letters and digits only, not a Windows device name, and not one of Thryve's
/// own files. Messages show a rejected name escaped (`{:?}`), so control characters stay visible.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NamingError {
    /// A log-file name contains something other than ASCII letters and digits.
    #[error("log-file name {name:?} may contain only ASCII letters and digits")]
    LogFileNameASCII {
        /// The rejected name.
        name: String,
    },

    /// A log-file name is a Windows device name (`CON`, `PRN`, `AUX`, `NUL`, or `COM`/`LPT`
    /// followed by one digit), compared ignoring case.
    ///
    /// Older Windows versions treat such a name as the device even with an extension, so a log file
    /// called `NUL.log` would silently lose its lines.
    #[error("log-file name {name:?} is a reserved device name")]
    LogFileNameReserved {
        /// The rejected name.
        name: String,
    },

    /// A log-file name is one of Thryve's own files (`Main`, `Error`, `Debug`), compared ignoring
    /// case.
    #[error("log-file name {name:?} is already reserved by Thryve")]
    LogFileNameReservedThryve {
        /// The rejected name.
        name: String,
    },

    /// A name is empty.
    #[error("{kind} must not be empty")]
    NameEmpty {
        /// What the name is for.
        kind: NameInputType,
    },

    /// A general name contains a control character, such as a newline or a NUL.
    #[error("{kind} {name:?} cannot contain control characters")]
    NameContainsControlChar {
        /// The rejected name.
        name: String,
        /// What the name is for.
        kind: NameInputType,
    },
}

/// Why a configuration file couldn't be loaded.
///
/// Returned by [`Config::load_config_from_file`](crate::cfg::config::Config::load_config_from_file)
/// and carried by [`ThryveError::Config`]. Loading never falls back to the defaults; the caller
/// decides that.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The file isn't valid TOML, or a key is missing, unknown or of the wrong type. TOML stops at
    /// the first such problem; its message, the [`source`](std::error::Error::source), gives the
    /// line and column.
    #[error("failed to parse config file '{}'", .path.display())]
    FileParse {
        /// The configuration file.
        path: PathBuf,
        /// The error from parsing it.
        source: toml::de::Error,
    },

    /// The file couldn't be read, for example because it doesn't exist.
    #[error("failed to read config file '{}'" , .path.display())]
    FileRead {
        /// The configuration file.
        path: PathBuf,
        /// The error from reading it.
        source: io::Error,
    },

    /// The file parsed, but some values would break the engine. The message lists every one of
    /// them, one per line.
    #[error("invalid config file '{}':\n{}" , .path.display(), violation)]
    ConfigViolation {
        /// The configuration file.
        path: PathBuf,
        /// Every invalid value found.
        violation: ConfigViolations,
    },
}

/// What was dropped because the engine was gone when it was sent.
///
/// [`PacketHandle::send_task`](crate::packets::packet_handle::PacketHandle::send_task) and
/// [`send_notification`](crate::packets::packet_handle::PacketHandle::send_notification) return
/// it inside [`ThryveError::EngineNotRunning`]. The engine never restarts, so the task or
/// notification isn't handed back.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum EngineNotRunningError {
    /// A task was sent and dropped.
    #[error("failed to send task to the engine")]
    Task,

    /// A notification was sent and dropped.
    #[error("failed to send notification to the engine")]
    Notif,
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum FinalizationError {
    /// A task was sent and dropped.
    #[error("plugin {} failed to clean up successfully", plugin_type)]
    PluginCleanup { plugin_type: String },
}

/// What a name is for: a task name, a plugin type, and so on.
///
/// Errors carry it so their message can say which name failed. It prints in plain words, for
/// example `task name` or `log-file name`.
#[derive(Debug)]
#[non_exhaustive]
pub enum NameInputType {
    /// The name of a task.
    TaskName,
    /// The type of a plugin, which identifies it.
    PlgType,
    /// The type of an observer, which identifies it and routes notifications to it.
    ObsType,
    /// The type of a notification.
    NotifType,
    /// The type of the observer a notification is sent to.
    NotifReceiver,
    /// The name of a log file.
    LogFileName,
}

impl Display for NameInputType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TaskName => write!(f, "task name"),
            Self::PlgType => write!(f, "plugin type"),
            Self::ObsType => write!(f, "observer type"),
            Self::NotifType => write!(f, "notification type"),
            Self::NotifReceiver => write!(f, "notification receiver"),
            Self::LogFileName => write!(f, "log-file name"),
        }
    }
}

/// Every invalid value found in one configuration file.
///
/// It prints one rule per line, indented, so that [`ConfigError::ConfigViolation`]'s message lists
/// them all.
#[derive(Debug)]
pub struct ConfigViolations {
    /// The broken rules, in the order they were checked.
    pub types: Vec<RuleError>,
}

impl Display for ConfigViolations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = String::from("");
        for err_type in self.types.iter() {
            s += &format!("  {}\n", err_type)
        }

        write!(f, "{s}")
    }
}

/// One configuration value that would break the engine.
///
/// Each case holds the value found in the file, then the limit it broke. It prints as
/// `threads.worker_count is 0, must be at least 1`.
#[derive(Debug)]
#[non_exhaustive]
pub enum RuleError {
    /// `engine.idle_wait` is below its minimum.
    IdleWait(u64, usize),
    /// `engine.shutdown_grace_period` is below its minimum.
    ShutdownGracePeriod(usize, usize),
    /// `threads.worker_count` is below its minimum; with no workers, no task would ever run.
    WorkerCount(usize, usize),
    /// `pipes.task_pipe_capacity` is below its minimum.
    TaskPipeCap(usize, usize),
    /// `pipes.tasks_processed_per_turn` is below its minimum.
    TasksProcessedPerTurn(usize, usize),
    /// `pipes.notif_pipe_capacity` is below its minimum.
    NotifPipeCap(usize, usize),
    /// `pipes.notifs_processed_per_turn` is below its minimum.
    NotifsProcessedPerTurn(usize, usize),
    /// `pipes.res_pipe_capacity` is below its minimum.
    ResultPipeCap(usize, usize),
    /// `pipes.res_processed_per_turn` is below its minimum.
    ResultsProcessedPerTurn(usize, usize),
    /// `tasks.aging_interval` is below its minimum.
    AgingInterval(u64, usize),
    /// `tasks.max_priority` isn't below its limit.
    MaxPriority(u8, usize),
}

impl Display for RuleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IdleWait(is, should_be) => write!(
                f,
                "engine.idle_wait is {is:?}, must be at least {should_be:?}"
            ),
            Self::ShutdownGracePeriod(is, should_be) => write!(
                f,
                "engine.shutdown_grace_period is {is:?}, must be at least {should_be:?}"
            ),
            Self::WorkerCount(is, should_be) => write!(
                f,
                "threads.worker_count is {is:?}, must be at least {should_be:?}"
            ),
            Self::TaskPipeCap(is, should_be) => write!(
                f,
                "pipes.task_pipe_capacity is {is:?}, must be at least {should_be:?}"
            ),
            Self::TasksProcessedPerTurn(is, should_be) => write!(
                f,
                "pipes.tasks_processed_per_turn is {is:?}, must be at least {should_be:?}"
            ),
            Self::NotifPipeCap(is, should_be) => write!(
                f,
                "pipes.notif_pipe_capacity is {is:?}, must be at least {should_be:?}"
            ),
            Self::NotifsProcessedPerTurn(is, should_be) => write!(
                f,
                "pipes.notifs_processed_per_turn is {is:?}, must be at least {should_be:?}"
            ),
            Self::ResultPipeCap(is, should_be) => write!(
                f,
                "pipes.res_pipe_capacity is {is:?}, must be at least {should_be:?}"
            ),
            Self::ResultsProcessedPerTurn(is, should_be) => write!(
                f,
                "pipes.res_processed_per_turn is {is:?}, must be at least {should_be:?}"
            ),
            Self::AgingInterval(is, should_be) => write!(
                f,
                "tasks.aging_interval is {is:?}, must be at least {should_be:?}"
            ),
            Self::MaxPriority(is, should_be) => write!(
                f,
                "tasks.max_priority is {is:?}, must be less than {should_be:?}"
            ),
        }
    }
}

/// Renders an error and its whole chain of causes, one cause per line.
///
/// `{}` prints only an error's own message. This follows [`source`](Error::source) down the chain
/// and puts each cause on its own indented line after `caused by:`. It returns the text rather
/// than printing it, so the caller decides where it goes: a log line, stderr or an FFI buffer.
///
/// # Example
///
/// ```
/// use std::io;
/// use thryve_utils::errors::{StartupError, ThryveError, get_err};
///
/// let err = ThryveError::from(StartupError::RootFolder {
///     source: io::Error::other("access denied"),
/// });
/// assert_eq!(
///     get_err(&err),
///     "failed to start up\n  caused by: failed to get the exe directory\n  caused by: access denied"
/// );
/// ```
pub fn get_err(err: &dyn Error) -> String {
    let mut string = format!("{err}");
    let mut current_source = err.source();

    while let Some(source) = current_source {
        string += format!("\n  caused by: {source}").as_str();

        current_source = source.source();
    }

    string
}

/// Renders an error and its whole chain of causes on one line, joined by `: `.
///
/// The one-line form suits log lines and short messages; [`get_err`] gives the multi-line form.
///
/// # Example
///
/// ```
/// use std::io;
/// use thryve_utils::errors::{StartupError, ThryveError, get_err_oneline};
///
/// let err = ThryveError::from(StartupError::RootFolder {
///     source: io::Error::other("access denied"),
/// });
/// assert_eq!(
///     get_err_oneline(&err),
///     "failed to start up: failed to get the exe directory: access denied"
/// );
/// ```
pub fn get_err_oneline(err: &dyn Error) -> String {
    let mut string = format!("{err}");
    let mut current_source = err.source();

    while let Some(source) = current_source {
        string += format!(": {source}").as_str();

        current_source = source.source();
    }

    string
}
