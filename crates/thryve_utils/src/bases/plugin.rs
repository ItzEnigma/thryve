// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Core plugin structure for the Thryve plugin system.
//!
//! This module provides the [`Plugin`] struct, which encapsulates all state and
//! behavior required for a plugin to extend the functionality of the Thryve system.
//! Plugins are modular components that can be dynamically loaded and executed
//! without modifying the core codebase.
//!
//! # Architecture
//!
//! Each plugin maintains:
//! - A unique name and type identifier for routing and debugging
//! - Core logic implementing the [`PlgLogic`] trait for custom behavior
//! - A packet handle for communicating with the engine
//! - An initialization function that runs when the plugin is loaded
//!
//! # Lifecycle
//!
//! Plugins are initialized with an `plg_entry_point` closure that performs
//! the main task when the plugin is first loaded. This function typically
//! chains calls to other functions in the plugin, enabling a flexible and
//! modular design. Plugins can be gracefully shut down by calling the
//! [`clean_up`](PluginB::clean_up) method.

use std::sync::Arc;

use crate::extras::{jobs::TaskJob, named_input::NamedInput};
use crate::packets::packet_handle::PacketHandle;
use crate::traits::plugin_logic::{PlgLogic, PluginB};

/// Core plugin structure for the Thryve plugin system.
///
/// The `Plugin` struct encapsulates all state and behavior required for a
/// plugin to extend the functionality of the Thryve system. Plugins are
/// modular components that can be dynamically loaded and executed without
/// modifying the core codebase.
///
/// # Fields
///
/// - `plg_name`: Unique identifier for the plugin
/// - `plg_type`: Type classifier for the plugin
/// - `plg_core`: Core logic implementing custom plugin behavior
/// - `tx`: Packet handle for communicating with the engine
/// - `plg_entry_point`: Initialization function executed when the plugin loads
///
/// # Example
///
/// ```no_run
/// // Plugins are typically created by implementing PlgLogic and
/// // passing the core logic to Plugin::new along with an initialization function
/// ```
pub struct Plugin {
    /// Unique identifier for the plugin.
    pub plg_name: String,
    /// Type classifier for the plugin.
    pub plg_type: NamedInput,
    /// Core logic implementing custom plugin behavior.
    pub plg_core: Arc<dyn PlgLogic>,
    /// Packet handle for communicating with the engine.
    pub tx: PacketHandle,
    /// Initialization function executed when the plugin loads.
    plg_entry_point: TaskJob,
}

impl PluginB for Plugin {
    /// Gracefully shuts down the plugin.
    ///
    /// Delegates cleanup to the plugin's core logic, allowing each plugin
    /// to perform its own resource deallocation and finalization.
    fn clean_up(&self) {
        let x = Arc::clone(&self.plg_core);
        x.clean_up();
    }
}

impl Plugin {
    /// Creates a new plugin with the specified configuration.
    ///
    /// Initializes a new plugin instance with the given name, type, core logic,
    /// packet handle, and initialization function. The initialization function
    /// will be executed when the plugin is first loaded by the engine.
    ///
    /// # Arguments
    ///
    /// * `plg_name` - Unique identifier for the plugin
    /// * `plg_type` - Type classifier for the plugin
    /// * `plg_core` - Core logic implementing custom plugin behavior
    /// * `tx` - Packet handle for communicating with the engine
    /// * `job` - Initialization function executed when the plugin loads
    ///
    /// # Returns
    ///
    /// A new `Plugin` instance configured with the provided parameters.
    pub fn new(
        plg_name: &str,
        plg_type: &str,
        plg_core: Arc<dyn PlgLogic>,
        tx: PacketHandle,
        job: TaskJob,
    ) -> Self {
        if plg_type.is_empty() {
            panic!("The field `plg_type` in the Plugin cannot be empty");
        }

        Self {
            plg_name: plg_name.to_string(),
            plg_type: NamedInput::new(plg_type),
            plg_core,
            tx,
            plg_entry_point: job,
        }
    }

    /// Extracts and returns the initialization function.
    ///
    /// Returns the `plg_entry_point` closure and replaces it with a default
    /// error function. This ensures the initialization function is only executed
    /// once when the plugin is first loaded.
    ///
    /// # Returns
    ///
    /// The initialization function closure, or a default error function if
    /// called multiple times.
    pub fn plg_entry_point(&mut self) -> TaskJob {
        std::mem::take(&mut self.plg_entry_point)
    }
}
