// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Configuration structures for the Thryve Task Manager application.
//!
//! This module defines the configuration hierarchy for the Thryve system,
//! providing types for where Thryve keeps its files, service behavior, engine
//! parameters, thread pool settings, pipe capacities, task management, and
//! debugging options.
//!
//! # Loading Configuration
//!
//! Configuration can be loaded from a TOML file using
//! [`Config::load_config_from_file`], or default values can be used via
//! [`Config::load_def_config`].
//!
//! Loading a file never falls back to the defaults. A file must set every key of every section;
//! a missing key, an unknown or misspelled key, or a value of the wrong type is a parse error with
//! its line and column. After parsing, every value is checked, and a file with invalid values is
//! rejected with an error that lists all of them. Whether to use the defaults instead is the
//! caller's choice. The `Default` values documented on each field are what
//! [`Config::load_def_config`] uses.
//!
//! ```toml
//! [init]
//! root_dir = ""
//!
//! [service]
//! can_stop = true
//! can_pause = true
//!
//! [engine]
//! idle_wait = 100
//! shutdown_grace_period = 10
//!
//! [threads]
//! worker_count = 4
//!
//! [pipes]
//! task_pipe_capacity = 1024
//! tasks_processed_per_turn = 500
//! notif_pipe_capacity = 1024
//! notifs_processed_per_turn = 500
//! res_pipe_capacity = 1024
//! res_processed_per_turn = 500
//!
//! [tasks]
//! max_priority = 4
//! aging_interval = 3
//!
//! [debugging]
//! is_debugging = false
//! wait_before_execution = 10
//! visual_graph = false
//! calculate_metrics = false
//! log_tasks = false
//! ```
//!
//! `init.root_dir` names the root folder: the folder that holds each run's logs and, in graph
//! mode, the task graph. Empty means the program's own folder. Anything else must be an absolute
//! path to a folder that already exists, such as `D:\ThryveData`. Write a Windows path in single
//! quotes (`root_dir = 'D:\ThryveData'`); inside double quotes, TOML reads each backslash as the
//! start of an escape.
//!
//! # Configuration Hierarchy
//!
//! - [`Config`]: Root configuration structure
//! - [`InitConfig`]: Where Thryve keeps its files (the root folder)
//! - [`ServiceConfig`]: Service behavior (pause/stop capabilities)
//! - [`EngineConfig`]: Engine tick rate and sleep behavior
//! - [`ThreadConfig`]: Worker thread count
//! - [`PipeConfig`]: Channel capacities for inter-component communication
//! - [`TaskConfig`]: Task priority and aging parameters
//! - [`DebugConfig`]: Debugging mode and wait times

use serde::Deserialize;

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::errors::{ConfigError, ConfigViolations, RuleError, ThryveError};

/// Root configuration structure for the Thryve Task Manager application.
///
/// This structure aggregates all configuration subsystems and serves as the
/// primary configuration container passed throughout the application.
///
/// # Fields
///
/// - `init`: Where Thryve keeps its files
/// - `service`: Service behavior configuration
/// - `engine`: Engine execution parameters
/// - `threads`: Thread pool configuration
/// - `pipes`: Channel capacity settings
/// - `tasks`: Task management parameters
/// - `debugging`: Debugging mode configuration
///
/// # Example
///
/// ```no_run
/// use thryve_utils::cfg::config::Config;
/// use thryve_utils::errors::get_err;
///
/// // Load from file; falling back to the defaults is the caller's choice
/// let config = Config::load_config_from_file("config.toml").unwrap_or_else(|e| {
///     eprintln!("{}", get_err(&e));
///     Config::load_def_config()
/// });
///
/// // Or use default configuration
/// let default_config = Config::load_def_config();
/// ```
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// Where Thryve keeps its files.
    pub init: InitConfig,
    /// Service behavior configuration.
    pub service: ServiceConfig,
    /// Engine execution parameters.
    pub engine: EngineConfig,
    /// Thread pool configuration.
    pub threads: ThreadConfig,
    /// Channel capacity settings.
    pub pipes: PipeConfig,
    /// Task management parameters.
    pub tasks: TaskConfig,
    /// Debugging mode configuration.
    pub debugging: DebugConfig,
}

impl Default for Config {
    /// Provides default configuration values for all subsystems.
    ///
    /// Returns a `Config` instance with all fields set to their respective
    /// default values, suitable for basic operation without external configuration.
    fn default() -> Self {
        Self {
            init: Default::default(),
            service: Default::default(),
            engine: Default::default(),
            threads: Default::default(),
            pipes: Default::default(),
            tasks: Default::default(),
            debugging: Default::default(),
        }
    }
}

impl Config {
    /// Loads the default configuration.
    ///
    /// Returns a `Config` instance with all fields set to their default
    /// values. This is useful when no external configuration file is
    /// available or desired.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::config::Config;
    ///
    /// let config = Config::load_def_config();
    /// ```
    pub fn load_def_config() -> Config {
        let config: Config = Default::default();
        config
    }

    /// Loads configuration from a TOML file.
    ///
    /// Reads and parses the file, then checks every value. It never falls back to the defaults:
    /// if the caller wants them after an error, it asks for them with [`Config::load_def_config`].
    ///
    /// The file must set every key of every section. The checks reject values that would break
    /// the engine, and all failing values are reported together:
    ///
    /// - `engine.idle_wait`, `engine.shutdown_grace_period`, `threads.worker_count`,
    ///   `tasks.aging_interval`, and the three `pipes` capacities and per-turn limits must be at
    ///   least 1;
    /// - `tasks.max_priority` must be 14 or lower;
    /// - `init.root_dir` must be empty, or an absolute path to a folder that already exists.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the TOML configuration file
    ///
    /// # Returns
    ///
    /// The `Config` loaded from the file.
    ///
    /// # Errors
    ///
    /// Returns [`ThryveError::Config`] with:
    /// - [`ConfigError::FileRead`] if the file can't be read;
    /// - [`ConfigError::FileParse`] if it isn't valid TOML, or a key is missing, unknown or of the
    ///   wrong type. TOML stops at the first such problem, and its message gives the line and
    ///   column;
    /// - [`ConfigError::ConfigViolation`] if it parses but has invalid values. The message lists
    ///   every one of them.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::config::Config;
    ///
    /// let config = Config::load_config_from_file("config.toml")?;
    /// # Ok::<(), thryve_utils::errors::ThryveError>(())
    /// ```
    pub fn load_config_from_file(path: impl AsRef<Path>) -> Result<Config, ThryveError> {
        // Reading and parsing: a missing, unknown or mistyped key fails here.
        let path: PathBuf = path.as_ref().to_path_buf();
        let config = read_config_file(path.clone())?;

        let mut violations = Vec::new();

        // Each value as read, then its limit: at least 1 for each, and below 15 for
        // `max_priority`. The root folder has no limit; it's checked as a path.
        let init_dir = config.init.root_dir.clone();
        let init_dir_path_is = Path::new(&init_dir);
        let idle_wait_is = config.engine.idle_wait;
        let shutdown_grace_period_is = config.engine.shutdown_grace_period;
        let worker_count_is = config.threads.worker_count;
        let task_pipe_cap_is = config.pipes.task_pipe_capacity;
        let tasks_processed_per_turn_is = config.pipes.tasks_processed_per_turn;
        let notif_pipe_cap_is = config.pipes.notif_pipe_capacity;
        let notifs_processed_per_turn_is = config.pipes.notifs_processed_per_turn;
        let res_pipe_cap_is = config.pipes.res_pipe_capacity;
        let res_processed_per_turn_is = config.pipes.res_processed_per_turn;
        let max_priority_is = config.tasks.max_priority;
        let aging_interval_is = config.tasks.aging_interval;

        let idle_wait_should_be: usize = 1;
        let shutdown_grace_period_should_be: usize = 1;
        let worker_count_should_be: usize = 1;
        let task_pipe_cap_should_be: usize = 1;
        let tasks_processed_per_turn_should_be: usize = 1;
        let notif_pipe_cap_should_be: usize = 1;
        let notifs_processed_per_turn_should_be: usize = 1;
        let res_pipe_cap_should_be: usize = 1;
        let res_processed_per_turn_should_be: usize = 1;
        let max_priority_should_be: usize = 15;
        let aging_interval_should_be: usize = 1;

        // Every check runs, so that all the failing values are reported together.

        // The root folder: empty means the program's own folder; anything else must be an
        // absolute path to a folder that already exists.
        if !init_dir_path_is.is_empty()
            && (!init_dir_path_is.is_absolute() || !init_dir_path_is.is_dir())
        {
            violations.push(RuleError::InitDir(init_dir));
        }

        // The engine's waits.
        if idle_wait_is < idle_wait_should_be as u64 {
            violations.push(RuleError::IdleWait(idle_wait_is, idle_wait_should_be));
        }

        if shutdown_grace_period_is < shutdown_grace_period_should_be {
            violations.push(RuleError::ShutdownGracePeriod(
                shutdown_grace_period_is,
                shutdown_grace_period_should_be,
            ));
        }

        // The plugin workers.
        if worker_count_is < worker_count_should_be {
            violations.push(RuleError::WorkerCount(
                worker_count_is,
                worker_count_should_be,
            ));
        }

        // The three bounded mailboxes: each one's capacity and per-turn limit.
        if task_pipe_cap_is < task_pipe_cap_should_be {
            violations.push(RuleError::TaskPipeCap(
                task_pipe_cap_is,
                task_pipe_cap_should_be,
            ));
        }
        if tasks_processed_per_turn_is < tasks_processed_per_turn_should_be {
            violations.push(RuleError::TasksProcessedPerTurn(
                tasks_processed_per_turn_is,
                tasks_processed_per_turn_should_be,
            ));
        }

        if notif_pipe_cap_is < notif_pipe_cap_should_be {
            violations.push(RuleError::NotifPipeCap(
                notif_pipe_cap_is,
                notif_pipe_cap_should_be,
            ));
        }

        if notifs_processed_per_turn_is < notifs_processed_per_turn_should_be {
            violations.push(RuleError::NotifsProcessedPerTurn(
                notifs_processed_per_turn_is,
                notifs_processed_per_turn_should_be,
            ));
        }

        if res_pipe_cap_is < res_pipe_cap_should_be {
            violations.push(RuleError::ResultPipeCap(
                res_pipe_cap_is,
                res_pipe_cap_should_be,
            ));
        }

        if res_processed_per_turn_is < res_processed_per_turn_should_be {
            violations.push(RuleError::ResultsProcessedPerTurn(
                res_processed_per_turn_is,
                res_processed_per_turn_should_be,
            ));
        }

        // The task queue: its priorities and its ageing.
        if max_priority_is >= max_priority_should_be as u8 {
            violations.push(RuleError::MaxPriority(
                max_priority_is,
                max_priority_should_be,
            ));
        }

        if aging_interval_is < aging_interval_should_be as u64 {
            violations.push(RuleError::AgingInterval(
                aging_interval_is,
                aging_interval_should_be,
            ));
        }

        // Any failure rejects the whole file, with the list.
        if !violations.is_empty() {
            return Err(ConfigError::ConfigViolation {
                path,
                violation: ConfigViolations { types: violations },
            }
            .into());
        }

        Ok(config)
    }
}

/// Where Thryve keeps its files.
///
/// Thryve keeps each run's logs (`log/<timestamp>/`) and, in graph mode, the task graph
/// (`topology/topology_export.ron`) under one folder, the root folder. Without one, that's the
/// program's own folder. A dev can name another, such as `D:\ThryveData`, to keep a service's
/// files apart from the program. `Topology.exe`, which shows the graph, stays next to the program
/// either way.
///
/// # Fields
///
/// - `root_dir`: The root folder, or empty for the program's own folder
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct InitConfig {
    /// The root folder: empty for the program's own folder, or an absolute path to a folder
    /// that already exists. A relative path is refused when the config is loaded: it would
    /// depend on the working folder, which for a Windows service is System32.
    ///
    /// In the file, write a Windows path in single quotes (`root_dir = 'D:\ThryveData'`); inside
    /// double quotes, TOML reads each backslash as the start of an escape.
    ///
    /// Default: `""`
    pub root_dir: String,
}

impl Default for InitConfig {
    /// Provides the default: no root folder, so everything stays in the program's own folder.
    fn default() -> Self {
        Self {
            root_dir: String::from(""),
        }
    }
}

/// Service behavior configuration.
///
/// Controls whether the service can be stopped or paused during runtime.
/// These settings determine which commands the service control manager
/// can issue to the running service.
///
/// # Fields
///
/// - `can_stop`: Whether the service accepts stop commands
/// - `can_pause`: Whether the service accepts pause/resume commands
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    /// Whether the service accepts stop commands.
    ///
    /// Default: `true`
    pub can_stop: bool,
    /// Whether the service accepts pause/resume commands.
    ///
    /// Default: `true`
    pub can_pause: bool,
}

impl Default for ServiceConfig {
    /// Provides default service behavior configuration.
    ///
    /// Returns a `ServiceConfig` with both stop and pause capabilities enabled.
    fn default() -> Self {
        Self {
            can_stop: true,
            can_pause: true,
        }
    }
}

/// Engine execution parameters.
///
/// Sets how long an idle engine waits before it looks at its loop's flags again, and how long
/// the shutdown waits for tasks that are still running.
///
/// # Fields
///
/// - `idle_wait`: Milliseconds an idle engine waits for something to arrive
/// - `shutdown_grace_period`: Seconds the shutdown waits for running tasks
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct EngineConfig {
    /// Milliseconds an idle engine waits for something to arrive.
    ///
    /// When a turn finds nothing, the engine waits on its mailboxes, and anything that arrives
    /// wakes it at once, so this doesn't delay any work. It only sets how often an idle engine
    /// returns to its loop, which is when the loop sees flags set from outside, such as the
    /// service's Stop, Pause and Resume. Lower values notice those sooner but wake an idle engine
    /// more often.
    ///
    /// Must be at least 1.
    ///
    /// Default: `100` milliseconds
    pub idle_wait: u64,

    /// Seconds the shutdown waits for tasks that are still running.
    ///
    /// The engine keeps emptying its mailboxes while it waits, so a task that is only slow, or
    /// blocked on a full mailbox, still finishes. A thread still busy when the time runs out is
    /// left running on its own: `Error.log` names it and what it was running, and the shutdown's
    /// summary of lost work counts it as still running. Rust can't stop a thread, so it ends with
    /// the process (in FFI, it keeps running in the host process). The plugins aren't cleaned up
    /// then, because one of them may still be in use.
    ///
    /// One deadline covers the plugin workers and then the plugins' cleanups. The observers'
    /// cleanup gets one more second, even when a stuck task has used up the whole grace period,
    /// because only an observer's cleanup stops its thread. So the whole shutdown can take the
    /// grace period plus about one second: keep it at least that much below how long the
    /// service manager waits for a service to stop.
    ///
    /// Must be at least 1: idle workers need a moment to stop.
    ///
    /// Default: `10` seconds
    pub shutdown_grace_period: usize,
}

impl Default for EngineConfig {
    /// Provides default engine execution parameters.
    ///
    /// Returns an `EngineConfig` with a 100 ms idle wait and a 10 s shutdown grace period.
    fn default() -> Self {
        Self {
            idle_wait: 100,
            shutdown_grace_period: 10,
        }
    }
}

/// Thread pool configuration.
///
/// Specifies the number of worker threads available for executing tasks.
/// More threads allow greater parallelism but consume more system resources.
///
/// # Fields
///
/// - `worker_count`: Number of worker threads in the pool
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct ThreadConfig {
    /// Number of worker threads for task execution.
    ///
    /// Each worker thread can process one task at a time. Setting this
    /// to 0 disables the thread pool entirely.
    ///
    /// Default: `4` worker threads
    pub worker_count: usize,
}

impl Default for ThreadConfig {
    /// Provides default thread pool configuration.
    ///
    /// Returns a `ThreadConfig` with 4 worker threads.
    fn default() -> Self {
        Self { worker_count: 4 }
    }
}

/// Channel capacity settings.
///
/// Defines the buffer sizes for the communication channels between
/// components, and how much the engine takes from each one per turn of its loop. Larger
/// capacities allow more items to queue but use more memory.
///
/// # Fields
///
/// - `task_pipe_capacity`: Buffer size for task channel
/// - `tasks_processed_per_turn`: Most tasks the engine takes per turn
/// - `notif_pipe_capacity`: Buffer size for notification channel
/// - `notifs_processed_per_turn`: Most notifications the engine takes per turn
/// - `res_pipe_capacity`: Buffer size for result channel
/// - `res_processed_per_turn`: Most results the engine takes per turn
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct PipeConfig {
    /// Buffer size for the task channel.
    ///
    /// Default: `1024`
    pub task_pipe_capacity: usize,
    /// The most tasks the engine takes from the task channel in one turn of its loop.
    ///
    /// The rest wait for the next turn, so a steady stream of tasks can't keep the engine from
    /// the other channels. Must be at least 1.
    ///
    /// Default: `500`
    pub tasks_processed_per_turn: usize,
    /// Buffer size for the notification channel.
    ///
    /// Default: `1024`
    pub notif_pipe_capacity: usize,
    /// The most notifications the engine takes from the notification channel in one turn of its
    /// loop.
    ///
    /// The rest wait for the next turn, so a steady stream of notifications can't keep the
    /// engine from the other channels. Must be at least 1.
    ///
    /// Default: `500`
    pub notifs_processed_per_turn: usize,
    /// Buffer size for the result channel.
    ///
    /// Default: `1024`
    pub res_pipe_capacity: usize,
    /// The most results the engine takes from the result channel in one turn of its loop.
    ///
    /// The rest wait for the next turn, so a steady stream of results can't keep the engine from
    /// the other channels. Must be at least 1.
    ///
    /// Default: `500`
    pub res_processed_per_turn: usize,
}

impl Default for PipeConfig {
    /// Provides default channel capacity settings.
    ///
    /// Returns a `PipeConfig` with all channels set to 1024 items, and the engine taking at most
    /// 500 from each per turn.
    fn default() -> Self {
        Self {
            task_pipe_capacity: 1024,
            tasks_processed_per_turn: 500,
            notif_pipe_capacity: 1024,
            notifs_processed_per_turn: 500,
            res_pipe_capacity: 1024,
            res_processed_per_turn: 500,
        }
    }
}

/// Task management parameters.
///
/// Controls task priority levels and the aging mechanism that prevents
/// lower-priority tasks from starving indefinitely.
///
/// # Fields
///
/// - `max_priority`: Highest valid priority level (0-14)
/// - `aging_interval`: Seconds before priority boost is applied
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct TaskConfig {
    /// Maximum valid task priority level.
    ///
    /// Tasks with priority higher than this value are clamped down.
    /// Valid range is 0-14 (inclusive).
    ///
    /// Default: `4` (priorities 0-4)
    pub max_priority: u8,
    /// Seconds a task must wait before receiving a priority boost.
    ///
    /// This aging mechanism prevents lower-priority tasks from
    /// starving when higher-priority tasks continuously arrive.
    ///
    /// Default: `3` seconds
    pub aging_interval: u64,
}

impl Default for TaskConfig {
    /// Provides default task management parameters.
    ///
    /// Returns a `TaskConfig` with max priority 4 and 3-second aging.
    fn default() -> Self {
        Self {
            max_priority: 4,
            aging_interval: 3,
        }
    }
}

/// Debugging mode configuration.
///
/// Enables special debugging behavior, including a wait period before
/// task execution that allows developers to attach debuggers or
/// monitoring tools.
///
/// # Fields
///
/// - `is_debugging`: Whether debugging mode is enabled
/// - `wait_before_execution`: Seconds to wait before starting execution
#[derive(Debug, Deserialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct DebugConfig {
    /// Whether debugging mode is enabled.
    ///
    /// When true, other attributes in the `DebugConfig` can be set
    /// to be enabled.
    ///
    /// Default: `false`
    pub is_debugging: bool,
    /// Seconds to wait before task execution in debugging mode.
    ///
    /// This delay only applies when `is_debugging` is true.
    ///
    /// Default: `10` seconds
    pub wait_before_execution: u64,
    /// Whether tracking for visual graph creation is enabled.
    ///
    /// When true, an observer will be loaded to keep track of
    /// the graphs. Hash calculations and call-stack frames will
    /// be tracked too.
    ///
    /// Default: `false`
    pub visual_graph: bool,
    /// Whether tracking metrics is enabled.
    ///
    /// When true, the engine will populate metrics arrays
    /// to be batched for metrics calculations every few cycles.
    ///
    /// Default: `false`
    pub calculate_metrics: bool,
    /// Whether logging task IDs is enabled.
    ///
    /// When true, notifications for when tasks have finished executing
    /// will be sent to log the finished task.
    ///
    /// Default: `false`
    pub log_tasks: bool,
}

impl Default for DebugConfig {
    /// Provides default debugging configuration.
    ///
    /// Returns a `DebugConfig` with debugging disabled and a 10-second
    /// wait period configured.
    fn default() -> Self {
        Self {
            is_debugging: false,
            wait_before_execution: 10,
            visual_graph: false,
            calculate_metrics: false,
            log_tasks: false,
        }
    }
}

/// Reads and parses a TOML configuration file.
///
/// Internal helper that reads the file contents and deserializes them
/// into a `Config` structure. It doesn't check the values;
/// [`Config::load_config_from_file`] does that afterwards.
///
/// # Arguments
///
/// * `path` - Path to the TOML configuration file
///
/// # Returns
///
/// * `Ok(Config)` - Parsed configuration on success
/// * `Err(ConfigError::FileRead)` - The file couldn't be read
/// * `Err(ConfigError::FileParse)` - The contents aren't valid TOML, or a key is missing, unknown
///   or of the wrong type
fn read_config_file(path: PathBuf) -> Result<Config, ConfigError> {
    let contents = fs::read_to_string(path.clone()).map_err(|e| ConfigError::FileRead {
        path: path.clone(),
        source: e,
    })?;

    let config: Config =
        toml::from_str(&contents).map_err(|e| ConfigError::FileParse { path, source: e })?;

    Ok(config)
}
