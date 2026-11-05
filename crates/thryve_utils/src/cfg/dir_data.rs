// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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

use std::{
    collections::HashMap,
    fs::File,
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use crate::extras::{dir_paths::DirName, named_input::LogFileNamedInput};

/// Static storage for the log directory path, accessible globally.
///
/// This `OnceLock` is initialized during system startup and provides
/// access to the log directory path from anywhere in the system,
/// including the panic hook.
static LOG_DIR_PATH: OnceLock<PathBuf> = OnceLock::new();

/// Directory data structure for the Thryve system.
///
/// The `DirData` struct encapsulates directory paths and log file handles
/// for the Thryve system. It holds the root folder, the paths of Thryve's
/// folders under it, and a hashmap of log files organized by module type.
///
/// # Fields
///
/// - `exe_dir`: The root folder: the folder `init.root_dir` names, or the executable's
///   folder when it's empty
/// - `dir_paths`: The paths of Thryve's folders (this run's log folder, and the topology
///   folder in graph mode)
/// - `log_hash_map`: HashMap mapping module types to their log file handles
///
/// # Example
///
/// ```ignore
/// use thryve_utils::cfg::dir_data::DirData;
///
/// // DirData is typically created during initialization
/// let dir_data = DirData::new(root_dir, log_hash_map);
/// ```
#[derive(Clone)]
pub struct DirData {
    /// The root folder: the folder `init.root_dir` names, or the executable's folder when it's
    /// empty.
    pub exe_dir: PathBuf,
    /// The paths of Thryve's folders, by name.
    pub dir_paths: HashMap<DirName, PathBuf>,
    /// HashMap mapping module types to their log file handles.
    pub log_hash_map: HashMap<LogFileNamedInput, Arc<File>>,
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
    /// Initializes a new `DirData` with the given root folder and hashmap of
    /// log files. The folders' paths are recorded afterwards with
    /// [`set_path`](Self::set_path).
    ///
    /// # Arguments
    ///
    /// * `exe_dir` - The root folder (`init.root_dir`, or the executable's folder when it's empty)
    /// * `log_hash_map` - HashMap mapping module types to their log file handles
    ///
    /// # Returns
    ///
    /// A new `DirData` instance with the specified configuration.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use thryve_utils::cfg::dir_data::DirData;
    ///
    /// let dir_data = DirData::new(root_dir, log_hash_map);
    /// ```
    pub fn new(exe_dir: PathBuf, log_hash_map: HashMap<LogFileNamedInput, Arc<File>>) -> Self {
        Self {
            exe_dir,
            dir_paths: HashMap::new(),
            log_hash_map,
        }
    }

    /// Records where one of Thryve's folders is.
    ///
    /// The log folder is also stored in a global, so the panic hook can find `Error.log` without a
    /// context. That global is set only once: a later call changes only this `DirData`.
    pub fn set_path(&mut self, dir_name: DirName, file_path: PathBuf) {
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

    /// Returns the path of one of Thryve's folders, or `None` if no path was recorded for
    /// `dir_name` with [`set_path`](Self::set_path).
    ///
    /// Not every folder always exists (the topology folder only in graph mode), so the caller
    /// decides what a missing one means. Thryve's own callers check once, when they're created,
    /// and fail startup if a folder they need is missing.
    pub fn get_dir_path(&self, dir_name: &DirName) -> Option<PathBuf> {
        self.dir_paths.get(dir_name).cloned()
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
