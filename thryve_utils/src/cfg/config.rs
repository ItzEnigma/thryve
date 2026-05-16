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

impl Config{
    /// Loads the default configuration.
    ///
    /// Returns a `Config` instance with all fields set to their default
    /// values. This is useful when no external configuration file is
    /// available or desired.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::Config;
    ///
    /// let config = Config::load_def_config();
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
    /// use thryve_utils::cfg::config::Config;
    ///
    /// let config = Config::load_config_from_file("config.toml");
    /// ```
    pub fn load_config_from_file(path: &str) -> Config {
        let config = match read_config_file(path) {
            Ok(cfg) => cfg,
            Err(_) => Config::load_def_config(),
        };
    
        assert!(config.tasks.max_priority < 15, "Maximum Task Priority cannot exceed 15");
    
        config
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

#[cfg(test)]
mod tests {
    use super::*;

    // Tests for Config
    #[test]
    fn test_config_default_values() {
        // Arrange & Act
        let config = Config::default();
        
        // Assert - verify all default values
        assert_eq!(config.service.can_stop, true);
        assert_eq!(config.service.can_pause, true);
        assert_eq!(config.engine.sleep_tick_rate, 4);
        assert_eq!(config.threads.worker_count, 4);
        assert_eq!(config.pipes.task_pipe_capacity, 1024);
        assert_eq!(config.pipes.notif_pipe_capacity, 1024);
        assert_eq!(config.pipes.res_pipe_capacity, 1024);
        assert_eq!(config.tasks.max_priority, 4);
        assert_eq!(config.tasks.aging_interval, 3);
        assert_eq!(config.debugging.is_debugging, false);
        assert_eq!(config.debugging.wait_before_execution, 10);
    }

    #[test]
    fn test_config_clone() {
        // Arrange
        let config1 = Config::default();
        
        // Act
        let config2 = config1.clone();
        
        // Assert
        assert_eq!(config1.service.can_stop, config2.service.can_stop);
        assert_eq!(config1.engine.sleep_tick_rate, config2.engine.sleep_tick_rate);
    }

    // Tests for SerivceConfig
    #[test]
    fn test_service_config_default() {
        // Arrange & Act
        let config = SerivceConfig::default();
        
        // Assert
        assert_eq!(config.can_stop, true);
        assert_eq!(config.can_pause, true);
    }

    #[test]
    fn test_service_config_clone() {
        // Arrange
        let config1 = SerivceConfig { can_stop: false, can_pause: true };
        
        // Act
        let config2 = config1.clone();
        
        // Assert
        assert_eq!(config1.can_stop, config2.can_stop);
        assert_eq!(config1.can_pause, config2.can_pause);
    }

    // Tests for EngineConfig
    #[test]
    fn test_engine_config_default() {
        // Arrange & Act
        let config = EngineConfig::default();
        
        // Assert
        assert_eq!(config.sleep_tick_rate, 4);
    }

    #[test]
    fn test_engine_config_with_custom_value() {
        // Arrange & Act
        let config = EngineConfig { sleep_tick_rate: 10 };
        
        // Assert
        assert_eq!(config.sleep_tick_rate, 10);
    }

    // Tests for ThreadConfig
    #[test]
    fn test_thread_config_default() {
        // Arrange & Act
        let config = ThreadConfig::default();
        
        // Assert
        assert_eq!(config.worker_count, 4);
    }

    #[test]
    fn test_thread_config_with_zero_workers() {
        // Arrange & Act
        let config = ThreadConfig { worker_count: 0 };
        
        // Assert
        assert_eq!(config.worker_count, 0);
    }

    // Tests for PipeConfig
    #[test]
    fn test_pipe_config_default() {
        // Arrange & Act
        let config = PipeConfig::default();
        
        // Assert
        assert_eq!(config.task_pipe_capacity, 1024);
        assert_eq!(config.notif_pipe_capacity, 1024);
        assert_eq!(config.res_pipe_capacity, 1024);
    }

    #[test]
    fn test_pipe_config_with_custom_values() {
        // Arrange & Act
        let config = PipeConfig {
            task_pipe_capacity: 2048,
            notif_pipe_capacity: 512,
            res_pipe_capacity: 256,
        };
        
        // Assert
        assert_eq!(config.task_pipe_capacity, 2048);
        assert_eq!(config.notif_pipe_capacity, 512);
        assert_eq!(config.res_pipe_capacity, 256);
    }

    // Tests for TaskConfig
    #[test]
    fn test_task_config_default() {
        // Arrange & Act
        let config = TaskConfig::default();
        
        // Assert
        assert_eq!(config.max_priority, 4);
        assert_eq!(config.aging_interval, 3);
    }

    #[test]
    fn test_task_config_with_min_priority() {
        // Arrange & Act
        let config = TaskConfig { max_priority: 0, aging_interval: 3 };
        
        // Assert
        assert_eq!(config.max_priority, 0);
    }

    #[test]
    fn test_task_config_with_max_valid_priority() {
        // Arrange & Act
        let config = TaskConfig { max_priority: 14, aging_interval: 3 };
        
        // Assert
        assert_eq!(config.max_priority, 14);
    }

    // Tests for DebugConfig
    #[test]
    fn test_debug_config_default() {
        // Arrange & Act
        let config = DebugConfig::default();
        
        // Assert
        assert_eq!(config.is_debugging, false);
        assert_eq!(config.wait_before_execution, 10);
    }

    #[test]
    fn test_debug_config_enabled() {
        // Arrange & Act
        let config = DebugConfig {
            is_debugging: true,
            wait_before_execution: 30,
        };
        
        // Assert
        assert_eq!(config.is_debugging, true);
        assert_eq!(config.wait_before_execution, 30);
    }

    // Tests for load_def_config
    #[test]
    fn test_load_def_config_returns_defaults() {
        // Arrange & Act
        let config = Config::load_def_config();
        
        // Assert
        assert_eq!(config.threads.worker_count, 4);
        assert_eq!(config.engine.sleep_tick_rate, 4);
    }

    // Tests for load_config_from_file
    #[test]
    fn test_load_config_from_file_nonexistent_returns_defaults() {
        // Arrange
        let nonexistent_path = "/nonexistent/path/config.toml";
        
        // Act
        let config = Config::load_config_from_file(nonexistent_path);
        
        // Assert - should return defaults
        assert_eq!(config.threads.worker_count, 4);
    }

    #[test]
    fn test_load_config_from_file_with_valid_content() {
        // Arrange - create a temp file
        use std::fs::File;
        use std::io::Write;
        
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_config.toml");
        
        let toml_content = r#"
            [threads]
            worker_count = 8
            
            [engine]
            sleep_tick_rate = 10
        "#;
        
        let mut file = File::create(&file_path).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();
        
        // Act
        let config = Config::load_config_from_file(file_path.to_str().unwrap());
        
        // Assert
        assert_eq!(config.threads.worker_count, 8);
        assert_eq!(config.engine.sleep_tick_rate, 10);
        
        // Cleanup
        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    fn test_load_config_from_file_partial_content_uses_defaults() {
        // Arrange - create a temp file with partial config
        use std::fs::File;
        use std::io::Write;
        
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_config_partial.toml");
        
        let toml_content = r#"
            [threads]
            worker_count = 16
        "#;
        
        let mut file = File::create(&file_path).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();
        
        // Act
        let config = Config::load_config_from_file(file_path.to_str().unwrap());
        
        // Assert - specified value used, others default
        assert_eq!(config.threads.worker_count, 16);
        assert_eq!(config.engine.sleep_tick_rate, 4); // default
        
        // Cleanup
        let _ = std::fs::remove_file(&file_path);
    }

    #[test]
    #[should_panic(expected = "Maximum Task Priority cannot exceed 15")]
    fn test_load_config_from_file_invalid_priority_panics() {
        // Arrange - create a temp file with invalid priority
        use std::fs::File;
        use std::io::Write;
        
        let temp_dir = std::env::temp_dir();
        let file_path = temp_dir.join("test_config_invalid.toml");
        
        let toml_content = r#"
            [tasks]
            max_priority = 20
        "#;
        
        let mut file = File::create(&file_path).unwrap();
        file.write_all(toml_content.as_bytes()).unwrap();
        
        // Act & Assert - should panic
        let _ = Config::load_config_from_file(file_path.to_str().unwrap());
        
        // Cleanup
        let _ = std::fs::remove_file(&file_path);
    }
}
