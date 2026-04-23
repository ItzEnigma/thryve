//! Configuration structures for the Thryve Task Manager application.
//!
//! This module defines the configuration hierarchy for the Thryve system,
//! providing types for service behavior, engine parameters, thread pool
//! settings, pipe capacities, task management, and debugging options.
//!
//! # Loading Configuration
//!
//! Configuration can be loaded from a TOML file using [`load_config_from_file`],
//! or default values can be used via [`load_def_config`]. When loading from a
//! file, any missing or invalid values fall back to defaults.
//!
//! # Configuration Hierarchy
//!
//! - [`Config`]: Root configuration structure
//! - [`SerivceConfig`]: Service behavior (pause/stop capabilities)
//! - [`EngineConfig`]: Engine tick rate and sleep behavior
//! - [`ThreadConfig`]: Worker thread count
//! - [`PipeConfig`]: Channel capacities for inter-component communication
//! - [`TaskConfig`]: Task priority and aging parameters
//! - [`DebugConfig`]: Debugging mode and wait times

use serde::Deserialize;

use std::fs;

/// Root configuration structure for the Thryve Task Manager application.
///
/// This structure aggregates all configuration subsystems and serves as the
/// primary configuration container passed throughout the application.
///
/// # Fields
///
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
///
/// // Load from file with fallback to defaults
/// let config = Config::load_config_from_file("config.toml");
///
/// // Or use default configuration
/// let default_config = Config::load_def_config();
/// ```
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(default)]
pub struct Config {
    /// Service behavior configuration.
    pub service: SerivceConfig,
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
            service: Default::default(),
            engine: Default::default(),
            threads: Default::default(),
            pipes: Default::default(),
            tasks: Default::default(),
            debugging: Default::default(),
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
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(default)]
pub struct SerivceConfig {
    /// Whether the service accepts stop commands.
    ///
    /// Default: `true`
    pub can_stop: bool,
    /// Whether the service accepts pause/resume commands.
    ///
    /// Default: `true`
    pub can_pause: bool,
}

impl Default for SerivceConfig {
    /// Provides default service behavior configuration.
    ///
    /// Returns a `SerivceConfig` with both stop and pause capabilities enabled.
    fn default() -> Self {
        Self {
            can_stop: true,
            can_pause: true,
        }
    }
}

/// Engine execution parameters.
///
/// Controls the engine's sleep behavior when idle, balancing responsiveness
/// with CPU usage. When no tasks are available, the engine sleeps for
/// the configured duration before checking again.
///
/// # Fields
///
/// - `sleep_tick_rate`: Milliseconds to sleep when idle
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(default)]
pub struct EngineConfig {
    /// Milliseconds to sleep when the engine has no tasks.
    ///
    /// This prevents the engine from consuming CPU while waiting for
    /// new work. Lower values increase responsiveness but use more CPU.
    ///
    /// Default: `4` milliseconds
    pub sleep_tick_rate: u64,
}

impl Default for EngineConfig {
    /// Provides default engine execution parameters.
    ///
    /// Returns an `EngineConfig` with a 4ms idle sleep duration.
    fn default() -> Self {
        Self {
            sleep_tick_rate: 4,
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
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(default)]
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
        Self {
            worker_count: 4,
        }
    }
}

/// Channel capacity settings.
///
/// Defines the buffer sizes for the communication channels between
/// components. Larger capacities allow more items to queue but use
/// more memory.
///
/// # Fields
///
/// - `task_pipe_capacity`: Buffer size for task channel
/// - `notif_pipe_capacity`: Buffer size for notification channel
/// - `res_pipe_capacity`: Buffer size for result channel
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(default)]
pub struct PipeConfig {
    /// Buffer size for the task channel.
    ///
    /// Default: `1024`
    pub task_pipe_capacity: usize,
    /// Buffer size for the notification channel.
    ///
    /// Default: `1024`
    pub notif_pipe_capacity: usize,
    /// Buffer size for the result channel.
    ///
    /// Default: `1024`
    pub res_pipe_capacity: usize,
}

impl Default for PipeConfig {
    /// Provides default channel capacity settings.
    ///
    /// Returns a `PipeConfig` with all channels set to 1024 items.
    fn default() -> Self {
        Self {
            task_pipe_capacity: 1024,
            notif_pipe_capacity: 1024,
            res_pipe_capacity: 1024,
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
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(default)]
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
#[derive(Debug, Deserialize, Clone, Copy)]
#[serde(default)]
pub struct DebugConfig {
    /// Whether debugging mode is enabled.
    ///
    /// When true, the engine waits before executing tasks, allowing
    /// developers to set up monitoring or debugging tools.
    ///
    /// Default: `false`
    pub is_debugging: bool,
    /// Seconds to wait before task execution in debugging mode.
    ///
    /// This delay only applies when `is_debugging` is true.
    ///
    /// Default: `10` seconds
    pub wait_before_execution: u64,
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
        }
    }
}

/// Loads the default configuration.
///
/// Returns a `Config` instance with all fields set to their default
/// values. This is useful when no external configuration file is
/// available or desired.
///
/// # Example
///
/// ```no_run
/// use thryve_utils::cfg::config::load_def_config;
///
/// let config = load_def_config();
/// ```
pub fn load_def_config() -> Config {
    let config: Config = Default::default();
    config
}

/// Loads configuration from a TOML file.
///
/// Attempts to parse the specified file as TOML configuration. If the
/// file cannot be read or parsed, returns the default configuration
/// instead of failing.
///
/// # Arguments
///
/// * `path` - Path to the TOML configuration file
///
/// # Returns
///
/// A `Config` instance loaded from the file, or defaults on error.
///
/// # Panics
///
/// Panics if `max_priority` exceeds 15, as this would break internal
/// assumptions about priority encoding.
///
/// # Example
///
/// ```no_run
/// use thryve_utils::cfg::config::load_config_from_file;
///
/// let config = load_config_from_file("config.toml");
/// ```
pub fn load_config_from_file(path: &str) -> Config {
    let config = match read_config_file(path) {
        Ok(cfg) => cfg,
        Err(_) => load_def_config(),
    };

    assert!(config.tasks.max_priority < 15, "Maximum Task Priority cannot exceed 15");

    config
}

/// Reads and parses a TOML configuration file.
///
/// Internal helper that reads the file contents and deserializes them
/// into a `Config` structure.
///
/// # Arguments
///
/// * `path` - Path to the TOML configuration file
///
/// # Returns
///
/// * `Ok(Config)` - Parsed configuration on success
/// * `Err(Box<dyn Error>)` - File I/O or parsing error
fn read_config_file(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(path)?;
    let config: Config = toml::from_str(&contents)?;
    Ok(config)
}
