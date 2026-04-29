//! Directory data structures for the Thryve system.
//!
//! This module provides the [`DirData`] struct, which encapsulates directory
//! paths and log file handles for the Thryve system. It manages the executable
//! directory, log directory path, and a hashmap of log files organized by
//! module type.
//!
//! # Purpose
//!
//! The `DirData` struct centralizes directory and file management, ensuring
//! that log files are created in the correct location and can be accessed
//! throughout the system lifecycle.
//!
//! # Static State
//!
//! The log directory path is stored in a static `OnceLock` for global access,
//! enabling the panic hook to write to log files even when the main context
//! is not available.
 
use std::{path::PathBuf, collections::HashMap, sync::{Arc, OnceLock}, fs::File};
 
use crate::extras::named_input::NamedInput;
 
/// Static storage for the log directory path, accessible globally.
///
/// This `OnceLock` is initialized during system startup and provides
/// access to the log directory path from anywhere in the system,
/// including the panic hook.
static LOG_DIR_PATH: OnceLock<PathBuf> = OnceLock::new();
 
/// Directory data structure for the Thryve system.
///
/// The `DirData` struct encapsulates directory paths and log file handles
/// for the Thryve system. It manages the executable directory, log directory
/// path, and a hashmap of log files organized by module type.
///
/// # Fields
///
/// - `exe_dir`: Path to the executable directory
/// - `log_dir_path`: Path to the timestamped log directory
/// - `log_hash_map`: HashMap mapping module types to their log file handles
///
/// # Example
///
/// ```no_run
/// use thryve_utils::cfg::dir_data::DirData;
///
/// // DirData is typically created during initialization
/// let dir_data = DirData::new(exe_dir, log_dir_path, log_hash_map);
/// ```
#[derive(Clone)]
pub struct DirData {
    /// Path to the executable directory.
    pub exe_dir: PathBuf,
    /// Path to the timestamped log directory.
    pub log_dir_path: PathBuf,
    /// HashMap mapping module types to their log file handles.
    pub log_hash_map: HashMap<NamedInput, Arc<File>>,
}
 
impl Default for DirData {
    /// Creates a default `DirData` instance with empty paths and hashmap.
    ///
    /// # Returns
    ///
    /// A new `DirData` with all fields set to their default (empty) values.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::dir_data::DirData;
    ///
    /// let dir_data = DirData::default();
    /// ```
    fn default() -> Self {
        Self {
            exe_dir: PathBuf::new(),
            log_dir_path: PathBuf::new(),
            log_hash_map: HashMap::new(),
        }
    }
}
 
impl DirData {
    /// Creates a new `DirData` instance with the specified paths and log files.
    ///
    /// Initializes a new `DirData` with the given executable directory,
    /// log directory path, and hashmap of log files. Also stores the log
    /// directory path in the static `LOG_DIR_PATH` for global access.
    ///
    /// # Arguments
    ///
    /// * `exe_dir` - Path to the executable directory
    /// * `log_dir_path` - Path to the timestamped log directory
    /// * `log_hash_map` - HashMap mapping module types to their log file handles
    ///
    /// # Returns
    ///
    /// A new `DirData` instance with the specified configuration.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::dir_data::DirData;
    ///
    /// let dir_data = DirData::new(exe_dir, log_dir_path, log_hash_map);
    /// ```
    pub fn new(
        exe_dir: PathBuf,
        log_dir_path: PathBuf,
        log_hash_map: HashMap<NamedInput, Arc<File>>,
    ) -> Self {
        let _ = LOG_DIR_PATH.set(log_dir_path.clone());
        Self { exe_dir, log_dir_path, log_hash_map }
    }
 
    /// Returns the log directory path from the static storage.
    ///
    /// Provides global access to the log directory path, which is
    /// essential for the panic hook to write to log files even when
    /// the main context is not available.
    ///
    /// # Returns
    ///
    /// A clone of the log directory path if it has been initialized,
    /// or `None` if initialization has not occurred yet.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::dir_data::DirData;
    ///
    /// if let Some(log_dir) = DirData::get_log_dir_path() {
    ///     println!("Log directory: {:?}", log_dir);
    /// }
    /// ```
    pub fn get_log_dir_path() -> Option<PathBuf> {
        LOG_DIR_PATH.get().cloned()
    }
}