// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Topology handler observer for graph visualization.
//!
//! This module provides the [`TopologyHandlerObserver`] struct, which is an
//! observer that receives topology modification notifications and maintains
//! a graph of task relationships. When the engine shuts down, it exports
//! the graph to a RON file and launches a GUI viewer.
//!
//! # Architecture
//!
//! The topology handler observer:
//!
//! 1. Receives `TopologyModifier` notifications from the engine
//! 2. Updates an internal `TaskTopology` graph with new nodes and edges
//! 3. On cleanup, exports the graph to a RON file
//! 4. Launches an external GUI viewer to visualize the topology
//!
//! # Usage
//!
//! The topology handler is automatically initialized when graph visualization
//! is enabled in the debugging configuration. It receives notifications
//! about task creation points and builds a complete picture of the task
//! call hierarchy.
//!
//! # Example
//!
//! ```ignore
//! use thryve_observers::topology_handler::TopologyHandlerObserver;
//!
//! // Typically created by the engine during initialization
//! let observer = TopologyHandlerObserver::new(ctx);
//! ```

use std::{
    io::Write,
    path::PathBuf,
    process::Command,
    sync::{Arc, RwLock, atomic::Ordering},
};

use ron::ser::PrettyConfig;

use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::extras::{dir_paths::DirName, jobs::ObsJob};
use thryve_utils::graphs::graph::TaskTopology;
use thryve_utils::impl_as_any;
use thryve_utils::packets::{notif_log::Log, notif_topology::TopologyModifier};
use thryve_utils::traits::{observer_logic::ObsLogic, queue::Queue};
use thryve_utils::{
    bases::{notif::Notif, observer::Observer},
    extras::dir_paths::get_workspace_root,
};

/// Observer that maintains and visualizes the task topology graph.
///
/// The `TopologyHandlerObserver` receives notifications about task creation
/// points and maintains a graph representing the call hierarchy. On shutdown,
/// it exports the graph to a RON file and launches a GUI viewer.
///
/// # Fields
///
/// - `topology`: The task topology graph (protected by a read-write lock)
/// - `topology_dir`: The directory where topology files are stored
///
/// # Example
///
/// ```ignore
/// use thryve_observers::topology_handler::TopologyHandlerObserver;
///
/// let observer = TopologyHandlerObserver::new(ctx);
/// ```
pub struct TopologyHandlerObserver {
    /// The task topology graph (protected by a read-write lock).
    pub topology: RwLock<TaskTopology>,
    /// The directory where topology files are stored.
    pub topology_dir: PathBuf,
}

impl ObsLogic for TopologyHandlerObserver {
    /// Performs cleanup when the observer is shutting down.
    ///
    /// This method:
    /// 1. Processes any remaining notifications in the queue
    /// 2. Exports the topology graph to a RON file
    /// 3. Clears the graph
    /// 4. Launches the GUI viewer
    ///
    /// # Arguments
    ///
    /// * `obs` - The observer being cleaned up
    fn clean_up(&self, obs: Arc<Observer>) {
        while !obs.obs_queue.main_queue.is_empty() {
            if let Some(notif) = obs.obs_queue.main_queue.pop() {
                self.handle_entry(notif);
            }
        }

        let current_dir = self.topology_dir.clone();

        // Export topology to RON file
        // RwLock write failure is fatal - indicates poisoned lock
        let mut topology_write = self
            .topology
            .write()
            .unwrap_or_else(|e| panic!("Topology RwLock poisoned: {}. Cannot export topology.", e));

        // Configure pretty-printing options
        let pretty_config = PrettyConfig::new()
            .depth_limit(8)
            .indentor(String::from("    "))
            .new_line(String::from("\n"));

        // Serialize to RON string
        // Serialization failure is fatal - topology data is corrupted
        let ron_string = ron::ser::to_string_pretty(&topology_write.graph, pretty_config)
            .expect("Failed to serialize topology to RON - topology data may be corrupted");

        // Write to file
        let file_path = current_dir.join("topology_export.ron");

        // File write failure is fatal - cannot store topology data
        std::fs::write(&file_path, ron_string).expect("Failed to write topology file");

        topology_write.graph.clear();

        self.visualize_gui();
    }

    impl_as_any!(TopologyHandlerObserver);
}

impl TopologyHandlerObserver {
    /// Creates a new topology handler observer.
    ///
    /// Initializes a new observer that will receive topology modification
    /// notifications and maintain the task topology graph.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The application context
    ///
    /// # Returns
    ///
    /// A new `Observer` instance configured for topology handling.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use thryve_observers::topology_handler::TopologyHandlerObserver;
    ///
    /// let observer = TopologyHandlerObserver::new(ctx);
    /// ```
    #[allow(clippy::new_ret_no_self)] // factory returns the Observer wrapper by design
    pub fn new(ctx: &Ctx) -> Observer {
        let topo_handler = Arc::new(Self {
            topology: RwLock::new(TaskTopology::default()),
            topology_dir: ctx.meta_data.dir_data.get_dir_path(&DirName::Topology),
        });

        let topo_handler_job = ObsJob::new(move |topo_handler: Arc<Observer>| {
            let topo_handler_as_any = topo_handler.obs_core.as_any();
            if let Some(topo_handler_core) =
                topo_handler_as_any.downcast_ref::<TopologyHandlerObserver>()
            {
                topo_handler_core.topology_modification(topo_handler.clone());
            }
        });

        // Return a new observer instance with the specified type and name.
        Observer::new(
            "Topology",
            "TopologyHandler",
            topo_handler,
            topo_handler_job,
            ctx.packet_handle(),
        )
    }

    /// Main loop for processing topology modification notifications.
    ///
    /// This method runs in a dedicated thread, continuously receiving
    /// notifications and updating the topology graph accordingly.
    ///
    /// # Arguments
    ///
    /// * `observer` - The observer instance
    fn topology_modification(&self, observer: Arc<Observer>) {
        loop {
            if observer.exit_flag.load(Ordering::SeqCst) {
                let notif = Log::log("Topology Handler Obs Thread Exited...".to_string(), "Main");
                observer.tx.send_notification(notif);

                return;
            }

            let notif = observer.obs_queue.pop();

            // pop() can wake here because the queue was finalized while this
            // thread was blocked inside it, in which case it returns a
            // sentinel value rather than a real notification -- recheck
            // before handing it to handle_entry (which expects a real
            // TopologyModifier and will panic on anything else).
            if observer.exit_flag.load(Ordering::SeqCst) {
                let notif = Log::log("Topology Handler Obs Thread Exited...".to_string(), "Main");
                observer.tx.send_notification(notif);

                return;
            }

            self.handle_entry(notif);
        }
    }

    /// Handles an incoming topology modification notification.
    ///
    /// Processes the notification by adding nodes or edges to the
    /// topology graph based on the action type.
    ///
    /// # Arguments
    ///
    /// * `notif` - The notification to handle
    pub fn handle_entry(&self, notif: Notif) {
        if notif.notif_receiver.inner() != "TopologyHandler" {
            return;
        }

        // Attempt to downcast the notification's core logic to a `TopologyModifier` type
        // to access its specific fields and functionality.
        let notif_as_any = notif.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<TopologyModifier>();

        // If the notification is successfully downcasted to a `TopologyModifier` type,
        // extract the log file name from the notification and log the message
        // to the appropriate file.
        // Downcast failure is fatal - indicates a protocol violation
        let mut notif_core = notif_casted.unwrap_or_else(|| panic!("Notification Sent to TopologyHandler but is meant for someone else. Intended receiver: {}", notif.notif_receiver.inner())).clone();

        let from_fingerprint = notif_core.from_fingerprint.as_str();
        let to_fingerprint = notif_core.to_fingerprint.as_str();

        // RwLock write failure is fatal - indicates poisoned lock
        let mut topology_ref = self
            .topology
            .write()
            .expect("Topology RwLock poisoned - cannot process topology modifier");

        if topology_ref.workspace_root == PathBuf::default() {
            topology_ref.workspace_root = get_workspace_root();
        }

        match notif_core.action.as_str() {
            "AddNode" => {
                // strip_prefix returns None if the prefix isn't found - this is not fatal,
                // we just use the original path
                notif_core.graph_node.callsite.file_path = notif_core
                    .graph_node
                    .callsite
                    .file_path
                    .strip_prefix(topology_ref.workspace_root.clone())
                    .unwrap_or(&notif_core.graph_node.callsite.file_path)
                    .to_path_buf();

                let node_fingerprint = notif_core.graph_node.fingerprint.clone();

                if !topology_ref.contains_node(&node_fingerprint) {
                    let node_index = topology_ref.graph.add_node(notif_core.graph_node);
                    topology_ref
                        .graph_node_indices
                        .insert(node_fingerprint, node_index);
                }
            }

            "AddEdge" => {
                let temp_from = topology_ref.contains_node(from_fingerprint);
                let temp_to = topology_ref.contains_node(to_fingerprint);

                // Missing node is a fatal error - indicates graph corruption
                if !temp_from || !temp_to {
                    panic!(
                        "Cannot add edge because a node is missing. This indicates graph corruption. from={}, to={}",
                        from_fingerprint, to_fingerprint
                    );
                }

                let temp_from = topology_ref.get_node_index(from_fingerprint);
                let temp_to = topology_ref.get_node_index(to_fingerprint);

                if !topology_ref.contains_edge(
                    notif_core.from_fingerprint.as_str(),
                    notif_core.to_fingerprint.as_str(),
                ) {
                    topology_ref.graph.add_edge(temp_from, temp_to, 1);
                } else {
                    let edge_index = topology_ref.get_edge(from_fingerprint, to_fingerprint);
                    topology_ref.graph[edge_index] += 1;
                }
            }
            _ => {}
        }
    }
}
impl TopologyHandlerObserver {
    /// Launches the GUI viewer for topology visualization.
    ///
    /// Spawns an external process (`Topology.exe`) that reads
    /// the exported RON file and displays the topology graph.
    pub fn visualize_gui(&self) {
        if let Some(current_execution_dir) = self.topology_dir.clone().parent() {
            // Path conversion failure is fatal - indicates corrupted path data
            let command = format!(
                "{}\\Topology.exe",
                current_execution_dir
                    .to_str()
                    .expect("Topology directory path contains invalid UTF-8")
            );
            println!("Command: {}", command);
            // Stdout flush failure is fatal - indicates corrupted stdout
            std::io::stdout().flush().expect("Failed to flush stdout");

            if PathBuf::from(command.clone()).exists() {
                // Process spawn failure is fatal - cannot visualize topology
                #[allow(clippy::zombie_processes)]
                // detached GUI viewer, not meant to block shutdown
                Command::new(command).spawn().expect("Failed to spawn GUI");
                println!("Process spawned and detached!");
            } else {
                println!("Failed to spawn GUI");
            }
        } else {
            // Path operations - parent() returns None if no parent, to_str() fails on invalid UTF-8
            // These are non-fatal - we log the error and continue
            if let Some(parent) = self.topology_dir.clone().parent() {
                if let Some(path_str) = parent.to_str() {
                    println!("{}\\Topology.exe", path_str);
                } else {
                    println!("Topology directory path contains invalid UTF-8");
                }
            } else {
                println!("Failed to get topology directory parent path");
            }
        }
    }
}
