//! Metadata structure for the Thryve system.
//!
//! This module provides the [`MetaData`] struct, which encapsulates
//! initialization metadata including the current timestamp and directory
//! data. It serves as a container for information collected during
//! system startup.
//!
//! # Purpose
//!
//! The `MetaData` struct bundles together the initialization timestamp
//! and directory configuration, providing a single source of truth for
//! metadata needed throughout the system lifecycle.
//!
//! # Contents
//!
//! - `datetime`: The local time when the system was initialized
//! - `dir_data`: Directory paths and log file handles
 
use std::time::SystemTime;
 
use chrono::{DateTime, Local};
 
use crate::cfg::{dir_data::DirData, metrics::Metrics};
 
/// Metadata structure for the Thryve system.
///
/// The `MetaData` struct encapsulates initialization metadata including
/// the current timestamp and directory data. It serves as a container
/// for information collected during system startup.
///
/// # Fields
///
/// - `datetime`: The local time when the system was initialized
/// - `dir_data`: Directory paths and log file handles
///
/// # Example
///
/// ```no_run
/// use thryve_utils::cfg::meta_data::MetaData;
///
/// // MetaData is typically created during initialization
/// let meta_data = MetaData::new(datetime, dir_data);
/// ```
#[derive(Clone)]
pub struct MetaData {
    /// The local time when the system was initialized.
    pub datetime: DateTime<Local>,
    /// Directory paths and log file handles.
    pub dir_data: DirData,
    /// Metrics for debugging and calculations.
    pub metrics: Metrics,
}
 
impl Default for MetaData {
    /// Creates a default `MetaData` instance with the current time.
    ///
    /// Initializes a new `MetaData` with the current system time and
    /// a default `DirData` instance.
    ///
    /// # Returns
    ///
    /// A new `MetaData` with the current timestamp and empty directory data.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::meta_data::MetaData;
    ///
    /// let meta_data = MetaData::default();
    /// ```
    fn default() -> Self {
        Self {
            datetime: SystemTime::now().into(),
            dir_data: DirData::default(),
            metrics: Metrics::default(),
        }
    }
}
 
impl MetaData {
    /// Creates a new `MetaData` instance with the specified timestamp and directory data.
    ///
    /// Initializes a new `MetaData` with the given local time and directory
    /// data configuration.
    ///
    /// # Arguments
    ///
    /// * `datetime` - The local time when the system was initialized
    /// * `dir_data` - Directory paths and log file handles
    ///
    /// # Returns
    ///
    /// A new `MetaData` instance with the specified configuration.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::meta_data::MetaData;
    ///
    /// let meta_data = MetaData::new(datetime, dir_data);
    /// ```
    pub fn new(datetime: DateTime<Local>, dir_data: DirData) -> Self {
        let metrics = Metrics::default();

        Self { datetime, dir_data, metrics}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_meta_data_default() {
        // Arrange & Act
        let meta_data = MetaData::default();
        
        // Assert
        assert!(meta_data.datetime <= chrono::Local::now());
        assert!(meta_data.dir_data.exe_dir.as_os_str().is_empty());
        assert!(meta_data.dir_data.log_dir_path.as_os_str().is_empty());
    }

    #[test]
    fn test_meta_data_new() {
        // Arrange
        let datetime = chrono::Local::now();
        let dir_data = DirData::default();
        
        // Act
        let meta_data = MetaData::new(datetime, dir_data.clone());
        
        // Assert
        assert_eq!(meta_data.datetime, datetime);
        assert_eq!(meta_data.dir_data.exe_dir, dir_data.exe_dir);
    }

    #[test]
    fn test_meta_data_clone() {
        // Arrange
        let datetime = chrono::Local::now();
        let dir_data = DirData::default();
        let meta_data1 = MetaData::new(datetime, dir_data);
        
        // Act
        let meta_data2 = meta_data1.clone();
        
        // Assert
        assert_eq!(meta_data1.datetime, meta_data2.datetime);
        assert_eq!(meta_data1.dir_data.exe_dir, meta_data2.dir_data.exe_dir);
    }

    #[test]
    fn test_meta_data_with_custom_dir_data() {
        // Arrange
        let datetime = chrono::Local::now();
        let exe_dir = std::path::PathBuf::from("/custom/exe");
        let log_dir = std::path::PathBuf::from("/custom/logs");
        let dir_data = DirData::new(exe_dir.clone(), log_dir.clone(), std::collections::HashMap::new());
        
        // Act
        let meta_data = MetaData::new(datetime, dir_data.clone());
        
        // Assert
        assert_eq!(meta_data.dir_data.exe_dir, exe_dir);
        assert_eq!(meta_data.dir_data.log_dir_path, log_dir);
    }

    #[test]
    fn test_meta_data_datetime_is_current() {
        // Arrange
        let before = chrono::Local::now();
        
        // Act
        let meta_data = MetaData::default();
        
        // Assert
        let after = chrono::Local::now();
        assert!(meta_data.datetime >= before);
        assert!(meta_data.datetime <= after);
    }
}