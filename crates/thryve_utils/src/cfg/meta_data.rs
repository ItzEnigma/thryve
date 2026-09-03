// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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

        Self {
            datetime,
            dir_data,
            metrics,
        }
    }
}
