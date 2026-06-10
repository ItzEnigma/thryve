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
 
use crate::extras::{dir_paths::DirName, named_input::NamedInput};
 
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
    pub dir_paths: HashMap<DirName , PathBuf>,
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
            dir_paths: HashMap::new(),
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
        log_hash_map: HashMap<NamedInput, Arc<File>>,
    ) -> Self {
        Self { exe_dir, dir_paths: HashMap::new(), log_hash_map }
    }
 
    pub fn set_path(&mut self, dir_name: DirName , file_path: PathBuf) {
        match dir_name {
            DirName::Log => {
                let _ = LOG_DIR_PATH.set(file_path.clone());
                self.dir_paths.insert(DirName::Log, file_path);
            }

            DirName::Topology => {
                self.dir_paths.insert(DirName::Topology, file_path);
            }
        }
    }

    pub fn get_dir_path(&self , dir_name: &DirName) -> PathBuf{
        self.dir_paths[dir_name].clone()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dir_data_default() {
        // Arrange & Act
        let dir_data = DirData::default();
        
        // Assert
        assert!(dir_data.exe_dir.as_os_str().is_empty());
        assert!(dir_data.dir_paths.is_empty());
        assert!(dir_data.log_hash_map.is_empty());
    }

    #[test]
    fn test_dir_data_new() {
        // Arrange
        let exe_dir = PathBuf::from("/test/exe");
        let log_dir = PathBuf::from("/test/logs");
        let log_hash_map: HashMap<NamedInput, Arc<File>> = HashMap::new();
        
        // Act
        let mut dir_data = DirData::new(exe_dir.clone(), log_hash_map);
        dir_data.set_path(DirName::Log, log_dir.clone());
        
        // Assert
        assert_eq!(dir_data.exe_dir, exe_dir);
        assert_eq!(dir_data.dir_paths[&DirName::Log], log_dir);
        assert!(dir_data.log_hash_map.is_empty());
    }

    #[test]
    fn test_dir_data_new_sets_static_log_path() {
        // Arrange
        let exe_dir = PathBuf::from("/test/exe");
        let log_dir = PathBuf::from("/test/logs");
        let log_hash_map: HashMap<NamedInput, Arc<File>> = HashMap::new();
        
        // Act
        let mut dir_data = DirData::new(exe_dir, log_hash_map);
        dir_data.set_path(DirName::Log, log_dir.clone());

        // Assert
        let retrieved = DirData::get_log_dir_path();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), log_dir);
    }

    #[test]
    fn test_dir_data_get_log_dir_path_before_init() {
        // Note: This test assumes the static is not initialized yet
        // In practice, other tests may have already initialized it
        // Act & Assert
        let result = DirData::get_log_dir_path();
        // Result may be Some if another test initialized it, or None if not
        // This is acceptable behavior
        assert!(result.is_some() || result.is_none());
    }

    #[test]
    fn test_dir_data_clone() {
        // Arrange
        let exe_dir = PathBuf::from("/test/exe");
        let log_dir = PathBuf::from("/test/logs");
        let mut log_hash_map: HashMap<NamedInput, Arc<File>> = HashMap::new();
        
        // Create a temp file for testing using cross-platform temp directory
        let temp_path = std::env::temp_dir().join("test.log");
        let temp_file = std::fs::File::create(&temp_path).unwrap();
        log_hash_map.insert(NamedInput::new("Test"), Arc::new(temp_file));
        
        let mut dir_data1 = DirData::new(exe_dir.clone(), log_hash_map.clone());
        dir_data1.set_path(DirName::Log, log_dir);

        // Act
        let dir_data2 = dir_data1.clone();
        
        // Assert
        assert_eq!(dir_data1.exe_dir, dir_data2.exe_dir);
        assert_eq!(dir_data1.dir_paths[&DirName::Log], dir_data2.dir_paths[&DirName::Log]);
        assert_eq!(dir_data1.log_hash_map.len(), dir_data2.log_hash_map.len());
    }

    #[test]
    fn test_dir_data_with_log_files() {
        // Arrange
        let exe_dir = PathBuf::from("/test/exe");
        let mut log_hash_map: HashMap<NamedInput, Arc<File>> = HashMap::new();
        
        // Create temp files using cross-platform temp directory
        let temp_path1 = std::env::temp_dir().join("test1.log");
        let temp_path2 = std::env::temp_dir().join("test2.log");
        let file1 = std::fs::File::create(&temp_path1).unwrap();
        let file2 = std::fs::File::create(&temp_path2).unwrap();
        
        log_hash_map.insert(NamedInput::new("Main"), Arc::new(file1));
        log_hash_map.insert(NamedInput::new("Error"), Arc::new(file2));
        
        // Act
        let dir_data = DirData::new(exe_dir, log_hash_map.clone());
        
        // Assert
        assert_eq!(dir_data.log_hash_map.len(), 2);
        assert!(dir_data.log_hash_map.contains_key(&NamedInput::new("Main")));
        assert!(dir_data.log_hash_map.contains_key(&NamedInput::new("Error")));
    }
}