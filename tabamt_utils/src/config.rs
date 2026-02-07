use serde::Deserialize;

use std::fs;

#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct Config {
    pub service: SerivceConfig,
    pub engine: EngineConfig,
    pub threads: ThreadConfig,
    pub tasks: TaskConfig,
    pub debugging: DebugConfig
}
impl Default for Config {
    fn default() -> Self {
        Self {
            service: Default::default(),
            engine: Default::default(),
            threads: Default::default(),
            tasks: Default::default(),
            debugging: Default::default(),
        }
    }
}

#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct SerivceConfig {
    // Determines whether the service can be stopped or paused or not.
    // default is true for both.
    pub can_stop: bool,
    pub can_pause: bool,
}

impl Default for SerivceConfig {
    fn default() -> Self {
        Self {
            can_stop: true,
            can_pause: true,
        }
    }
}

#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct EngineConfig {
    // How much time the engine sleeps only when it does not have tasks on its hand so it doesn't occupy the thread when it doesn't need to.
    // default is 4 milliseconds.
    pub sleep_tick_rate: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            sleep_tick_rate: 4,
        }
    }
}


#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct ThreadConfig {
    // Number of worker threads that will be executing tasks.
    // Default is 4 worker threads.
    pub worker_count: usize,
}

impl Default for ThreadConfig {
    fn default() -> Self {
        Self {
            worker_count: 4,
        }
    }
}


#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct TaskConfig {
    // Maximum priority of task in the program.
    // Default is max priority 4: (0 until 4)
    pub max_priority: u8,

    // The time in seconds after which the priority of a task becomes higher to execute so that it is not starved
    // Default is 3 seconds
    pub aging_interval: u64,
}

impl Default for TaskConfig {
    fn default() -> Self {
        Self {
            max_priority: 4,
            aging_interval: 3,
        }
    }
}

#[derive(Debug, Deserialize , Clone , Copy)]
#[serde(default)]
pub struct DebugConfig{
    // Boolean to check whether to run logic with debugging options:
        // Until now, the logic debugging is just a timer that starts after all modules are loaded and waits a specific time before tasks are executed by the workers.
            // This is typically used to allow you to setup some stuff to monitor execution from the beginning of your modules.
            // Default is false: debugging mode is off.
            //// This is to be removed later and replaced by user input to allow for more developer friendly behavior.
    pub is_debugging: bool,

    // The time to wait before executing.
    // Default number is 10 seconds.
    pub wait_before_execution: u64
}

impl Default for DebugConfig {
    fn default() -> Self {
        Self {
            is_debugging: false,
            wait_before_execution: 10,
        }
    }
}

pub fn load_def_config() -> Config{
    let config: Config = Default::default();

    config
}

pub fn load_config_from_file(path: &str) -> Config{
    let config = match read_config_file(path) {
        Ok(cfg) => cfg,
        Err(_) => load_def_config(),
    };

    assert!(config.tasks.max_priority < 15 , "Maximum Task Priority cannot exceed 15");

    config
}

fn read_config_file(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let contents = fs::read_to_string(path)?;
    let config: Config = toml::from_str(&contents)?;
    Ok(config)
}