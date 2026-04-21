//! This module defines the configuration structures for the Task Manager application.
//! The configuration can be loaded from a TOML file.
//! Default values are provided for all configuration options.
//! Only values that are explicitly set in the configuration file will override the defaults.

use serde::Deserialize;

use std::fs;

/// Is the main configuration structure for the Task Manager application.
#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct Config {
    pub service: SerivceConfig,
    pub engine: EngineConfig,
    pub threads: ThreadConfig,
    pub pipes: PipeConfig,
    pub tasks: TaskConfig,
    pub debugging: DebugConfig
}
impl Default for Config {
    /// Provides default configuration values for the Task Manager application.
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

/// Defines the configuration options related to the service behavior of the Task Manager application.
#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct SerivceConfig {
    /// Determines whether the service can be stopped or paused or not.
    /// Default is true for both.
    pub can_stop: bool,
    pub can_pause: bool,
}

impl Default for SerivceConfig {
    /// Provides default configuration values for the service behavior of the Task Manager application.
    fn default() -> Self {
        Self {
            can_stop: true,
            can_pause: true,
        }
    }
}

/// Defines the configuration options related to the engine behavior of the Task Manager application.
#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct EngineConfig {
    /// Is how much time the engine sleeps only when it does not have tasks on its hand so it doesn't occupy the thread when it doesn't need to.
    /// Default is 4 milliseconds.
    pub sleep_tick_rate: u64,
}

impl Default for EngineConfig {
    /// Provides default configuration values for the engine behavior of the Task Manager application.
    fn default() -> Self {
        Self {
            sleep_tick_rate: 4,
        }
    }
}

/// Defines the configuration options related to the worker threads of the Task Manager application.
#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct ThreadConfig {
    // Number of worker threads that will be executing tasks.
    // Default is 4 worker threads.
    pub worker_count: usize,
}

impl Default for ThreadConfig {
    /// Provides default configuration values for the worker threads of the Task Manager application.
    fn default() -> Self {
        Self {
            worker_count: 4,
        }
    }
}

/// Defines the configuration options related to the pipes of the Task Manager application.
#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct PipeConfig {
    /// Capacity of task, notif, and result pipes.
    /// Default is 1024 for each of the pipes.
    pub task_pipe_capacity: usize,
    pub notif_pipe_capacity: usize,
    pub res_pipe_capacity: usize,
}

impl Default for PipeConfig {
    /// Provides default configuration values for the pipes of the Task Manager application.
    fn default() -> Self {
        Self {
            task_pipe_capacity: 1024,
            notif_pipe_capacity: 1024,
            res_pipe_capacity: 1024,
        }
    }
}

/// Defines the configuration options related to the tasks of the Task Manager application.
#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct TaskConfig {
    /// Maximum priority of task in the program.
    /// Default is max priority 4: (0 until 4)
    pub max_priority: u8,

    /// The time in seconds after which the priority of a task becomes higher to execute so that it is not starved
    /// Default is 3 seconds
    pub aging_interval: u64,
}

impl Default for TaskConfig {
    /// Provides default configuration values for the task behavior of the Task Manager application.
    fn default() -> Self {
        Self {
            max_priority: 4,
            aging_interval: 3,
        }
    }
}

/// Defines the configuration options related to the debugging behavior of the Task Manager application.
#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct DebugConfig{
    /// Boolean to check whether to run logic with debugging options:
        /// Until now, the logic debugging is just a timer that starts after all modules are loaded and waits a specific time before tasks are executed by the workers.
            /// This is typically used to allow you to setup some stuff to monitor execution from the beginning of your modules.
            /// Default is false: debugging mode is off.
            //// This is to be removed later and replaced by user input to allow for more developer friendly behavior.
    pub is_debugging: bool,

    /// The time to wait before executing.
    /// Default number is 10 seconds.
    pub wait_before_execution: u64
}

impl Default for DebugConfig {
    /// Provides default configuration values for the debugging options of the Task Manager application.
    fn default() -> Self {
        Self {
            is_debugging: false,
            wait_before_execution: 10,
        }
    }
}

/// Loads the default configuration values for the Task Manager application.
pub fn load_def_config() -> Config{
    let config: Config = Default::default();

    config
}

/// Loads the configuration from a TOML file at the specified path.
/// If the file cannot be read or parsed, the default configuration will be used instead.
pub fn load_config_from_file(path: &str) -> Config{
    let config = match read_config_file(path) {
        Ok(cfg) => cfg,
        Err(_) => load_def_config(),
    };

    assert!(config.tasks.max_priority < 15 , "Maximum Task Priority cannot exceed 15");

    config
}

/// Reads the configuration from a TOML file and parses it into a `Config` struct.
/// If the file cannot be read or parsed, an error will be returned.
fn read_config_file(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(path)?;
    let config: Config = toml::from_str(&contents)?;
    Ok(config)
}