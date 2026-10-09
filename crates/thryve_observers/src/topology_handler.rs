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
//! # Failures
//!
//! Graph mode is a debugging aid, so a failure here never stops the engine:
//!
//! - When the graph would be wrong from some point on (an edge to a node that isn't in the
//!   graph, a call site that wasn't found, a poisoned lock), graph mode turns off until the
//!   program restarts. The graph ends with a failure node that says why (see
//!   [`TopologyNode::failure`]), and it's still exported.
//! - Other failures are handled on the spot and reported in `Error.log`: a notification that
//!   isn't a topology modifier is dropped; without a workspace root, file paths stay absolute;
//!   a failed export or viewer launch is reported, and the shutdown goes on.
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
//! let observer = TopologyHandlerObserver::new(ctx)?;
//! ```

use std::{
    fs::File,
    io::{self, Write},
    path::PathBuf,
    process::Command,
    sync::{
        Arc, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::SystemTime,
};

use ron::ser::PrettyConfig;

use thryve_utils::extras::{
    dir_paths::DirName,
    jobs::ObsJob,
    log_format::{log_line, write_line, write_stderr},
    named_input::LogFileNamedInput,
};
use thryve_utils::graphs::graph::{TaskTopology, TopologyNode};
use thryve_utils::impl_as_any;
use thryve_utils::packets::{
    notif_log::Log, notif_topology::TopologyModifier, packet_handle::PacketHandle,
};
use thryve_utils::traits::{observer_logic::ObsLogic, queue::Queue};
use thryve_utils::{
    bases::{notif::Notif, observer::Observer},
    extras::dir_paths::get_workspace_root,
};
use thryve_utils::{cfg::ctx::Ctx, errors::StartupError};

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
/// let observer = TopologyHandlerObserver::new(ctx)?;
/// ```
pub struct TopologyHandlerObserver {
    /// The task topology graph (protected by a read-write lock).
    pub topology: RwLock<TaskTopology>,
    /// The directory where topology files are stored.
    pub topology_dir: PathBuf,
    /// The context's packet handle, used for the graph-mode switch that every handle shares.
    packet_handle: PacketHandle,
    /// `Error.log`, where the handler reports what went wrong. `None` when the context has no
    /// log files; the reports then go to stderr.
    error_log: Option<Arc<File>>,
    /// Set once the workspace root couldn't be found, so that the lookup isn't retried and the
    /// warning is written once.
    workspace_root_missing: AtomicBool,
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
    /// The graph is exported even if graph mode turned off: what was recorded before the failure
    /// is the useful part, and the failure node shows where it ended. A failed export is
    /// reported in `Error.log`, and the shutdown goes on.
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

        // A poisoned lock is taken back, so what was recorded is still exported, ending with
        // a failure node.
        let mut topology_write = match self.topology.write() {
            Ok(topology_write) => topology_write,
            Err(poisoned) => {
                let mut topology_write = poisoned.into_inner();
                self.graph_failed(
                    &mut topology_write,
                    "the topology lock was poisoned".to_string(),
                    "",
                );
                topology_write
            }
        };

        // Configure pretty-printing options
        let pretty_config = PrettyConfig::new()
            .depth_limit(8)
            .indentor(String::from("    "))
            .new_line(String::from("\n"));

        // Serialize to RON string
        let ron_string = match ron::ser::to_string_pretty(&topology_write.graph, pretty_config) {
            Ok(ron_string) => ron_string,
            Err(e) => {
                self.report(&format!("could not export the topology graph: {e}"));
                topology_write.graph.clear();
                return;
            }
        };

        // Write to file
        let file_path = current_dir.join("topology_export.ron");

        if let Err(e) = std::fs::write(&file_path, ron_string) {
            self.report(&format!(
                "could not write the topology graph to '{}': {e}",
                file_path.display()
            ));
            topology_write.graph.clear();
            return;
        }

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
    /// # Errors
    ///
    /// Returns [`StartupError::DirFetch`] if the context has no topology folder, that is, if
    /// `init_directories` didn't run in graph mode.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use thryve_observers::topology_handler::TopologyHandlerObserver;
    ///
    /// let observer = TopologyHandlerObserver::new(ctx)?;
    /// ```
    #[allow(clippy::new_ret_no_self)] // factory returns the Observer wrapper by design
    pub fn new(ctx: &Ctx) -> Result<Observer, StartupError> {
        let topo_handler = Arc::new(Self {
            topology: RwLock::new(TaskTopology {
                enabled: true,
                ..TaskTopology::default()
            }),

            topology_dir: ctx
                .meta_data
                .dir_data
                .get_dir_path(&DirName::Topology)
                .ok_or(StartupError::DirFetch)?,

            packet_handle: ctx.packet_handle(),
            error_log: ctx
                .meta_data
                .dir_data
                .log_hash_map
                .get(&LogFileNamedInput::new_err())
                .cloned(),
            workspace_root_missing: AtomicBool::new(false),
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
        Ok(Observer::new_topo_handler(
            topo_handler,
            topo_handler_job,
            ctx.packet_handle(),
        ))
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
                break;
            }

            let notif_opt = observer.obs_queue.pop();

            if let Some(notif) = notif_opt {
                self.handle_entry(notif);
            } else {
                break;
            }
        }

        let notif = Log::log("Topology Handler Obs Thread Exited...".to_string(), "Main");
        observer.tx.send_status_log(notif)
    }

    /// Handles an incoming topology modification notification.
    ///
    /// Processes the notification by adding nodes or edges to the
    /// topology graph based on the action type.
    ///
    /// Nothing here stops the engine:
    ///
    /// - A notification that isn't a topology modifier is dropped, with a note in `Error.log`.
    /// - An edge to a node that isn't in the graph, or a poisoned lock, turns graph mode off. The
    ///   graph ends with a failure node, and a warning goes to `Error.log`.
    /// - After the failure node, every notification is ignored, so the graph ends where it failed.
    ///
    /// # Arguments
    ///
    /// * `notif` - The notification to handle
    pub fn handle_entry(&self, notif: Notif) {
        if notif.notif_receiver() != "TopologyHandler" {
            return;
        }

        // A notification that isn't a topology modifier can't change the graph. It's dropped,
        // with a note, and graph mode stays on.
        let Some(notif_core) = notif.notif_core.as_any().downcast_ref::<TopologyModifier>() else {
            self.report(&format!(
                "[dropped a notification of type {:?}: the topology handler only takes topology notifications]",
                notif.notif_type()
            ));
            return;
        };
        let mut notif_core = notif_core.clone();

        // A poisoned lock means a thread panicked while changing the graph, so the graph may be
        // half-changed. It's taken back and ends with a failure node.
        let mut topology_ref = match self.topology.write() {
            Ok(topology_ref) => topology_ref,
            Err(poisoned) => {
                let mut topology_ref = poisoned.into_inner();
                self.graph_failed(
                    &mut topology_ref,
                    "the topology lock was poisoned".to_string(),
                    "",
                );
                return;
            }
        };

        // After the failure node, nothing more is recorded.
        if !topology_ref.enabled {
            return;
        }

        match notif_core.action.as_str() {
            "AddNode" => {
                // Paths are shown relative to the workspace. If the workspace root is unknown,
                // or the path isn't inside it, the full path stays.
                if let Some(workspace_root) = self.workspace_root(&mut topology_ref)
                    && let Ok(relative_path) = notif_core
                        .graph_node
                        .callsite
                        .file_path
                        .strip_prefix(&workspace_root)
                {
                    notif_core.graph_node.callsite.file_path = relative_path.to_path_buf();
                }

                let node_fingerprint = notif_core.graph_node.fingerprint.clone();

                if !topology_ref.contains_node(&node_fingerprint) {
                    let node_index = topology_ref.graph.add_node(notif_core.graph_node);
                    topology_ref
                        .graph_node_indices
                        .insert(node_fingerprint, node_index);
                }
            }

            "AddEdge" => {
                let from = notif_core.from_fingerprint.as_str();
                let to = notif_core.to_fingerprint.as_str();

                match (topology_ref.node_index(from), topology_ref.node_index(to)) {
                    (Some(from_index), Some(to_index)) => {
                        match topology_ref.graph.find_edge(from_index, to_index) {
                            Some(edge_index) => {
                                if let Some(count) = topology_ref.graph.edge_weight_mut(edge_index)
                                {
                                    *count += 1;
                                }
                            }
                            None => {
                                topology_ref.graph.add_edge(from_index, to_index, 1);
                            }
                        }
                    }

                    // An edge to a node that isn't in the graph means the graph is wrong from
                    // here on. The failure node hangs off the end that exists.
                    (from_index, to_index) => {
                        let at = if from_index.is_some() {
                            from
                        } else if to_index.is_some() {
                            to
                        } else {
                            ""
                        };
                        let reason = format!(
                            "an edge from {from:?} to {to:?} refers to a node that isn't in the graph"
                        );

                        self.graph_failed(&mut topology_ref, reason, at);
                    }
                }
            }
            _ => {}
        }
    }

    /// Turns graph mode off after a failure found here, and records it as the failure node, with
    /// an edge to it from the node at `at` when that node is in the graph. Recording then stops,
    /// so the failure is the graph's last node.
    ///
    /// Only the first failure counts: it writes the warning to `Error.log` and becomes the failure
    /// node. The switch is shared with every packet handle, so senders stop recording call sites
    /// too.
    ///
    /// # Arguments
    ///
    /// * `topology` - The graph, behind its lock
    /// * `reason` - Why graph mode turned off
    /// * `at` - The fingerprint of the node where the failure happened, or empty if unknown
    fn graph_failed(&self, topology: &mut TaskTopology, reason: String, at: &str) {
        if self.packet_handle.turn_graph_mode_off() {
            self.report(&format!("graph mode turned off: {reason}"));
        }

        if !topology.enabled {
            return;
        }

        let failure_index = topology.graph.add_node(TopologyNode::failure(reason));
        topology
            .graph_node_indices
            .insert(TopologyNode::FAILURE_FINGERPRINT.to_string(), failure_index);

        if let Some(at_index) = topology.node_index(at) {
            topology.graph.add_edge(at_index, failure_index, 1);
        }

        topology.enabled = false;
    }

    /// Returns the workspace root, looking it up the first time.
    ///
    /// If the lookup fails (for example, outside a cargo project), the graph keeps full file
    /// paths. The warning is written once, and the lookup isn't retried.
    fn workspace_root(&self, topology: &mut TaskTopology) -> Option<PathBuf> {
        if topology.workspace_root != PathBuf::default() {
            return Some(topology.workspace_root.clone());
        }

        if self.workspace_root_missing.load(Ordering::SeqCst) {
            return None;
        }

        match get_workspace_root() {
            Some(workspace_root) => {
                topology.workspace_root = workspace_root.clone();
                Some(workspace_root)
            }
            None => {
                self.workspace_root_missing.store(true, Ordering::SeqCst);
                self.report(
                    "could not find the workspace root, so the graph keeps full file paths",
                );
                None
            }
        }
    }

    /// Writes one of the handler's reports to `Error.log`, or to stderr if there's no
    /// `Error.log`.
    ///
    /// The handler writes directly instead of sending the line to the logger: most of its
    /// reports happen during shutdown, when the engine no longer reads its channels.
    fn report(&self, message: &str) {
        let line = log_line(SystemTime::now(), "TOPOLOGY", "Error", message);

        match &self.error_log {
            Some(error_log) => write_line(error_log, &line),
            None => write_stderr(&line),
        }
    }
}

impl TopologyHandlerObserver {
    /// Launches the GUI viewer for topology visualization.
    ///
    /// Spawns an external process (`Topology.exe`) that reads
    /// the exported RON file and displays the topology graph.
    ///
    /// Nothing here stops the shutdown: a viewer that can't be started is reported in
    /// `Error.log`, and a missing `Topology.exe` is only mentioned on stdout.
    pub fn visualize_gui(&self) {
        let mut stdout = io::stdout();

        let Some(current_execution_dir) = self.topology_dir.parent() else {
            let _ = writeln!(stdout, "Failed to get topology directory parent path");
            return;
        };

        let Some(current_execution_dir) = current_execution_dir.to_str() else {
            self.report(
                "the topology folder's path isn't valid UTF-8, so Topology.exe can't be started",
            );
            return;
        };

        let command = format!("{current_execution_dir}\\Topology.exe");
        let _ = writeln!(stdout, "Command: {command}");
        let _ = stdout.flush();

        if PathBuf::from(&command).exists() {
            // Detached GUI viewer, not meant to block shutdown
            #[allow(clippy::zombie_processes)]
            let spawned = Command::new(&command).spawn();

            match spawned {
                Ok(_) => {
                    let _ = writeln!(stdout, "Process spawned and detached!");
                }
                Err(e) => self.report(&format!("could not start Topology.exe: {e}")),
            }
        } else {
            let _ = writeln!(stdout, "Failed to spawn GUI");
        }
    }
}
