// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Core observer structure for the Thryve observer system.
//!
//! This module provides the [`Observer`] struct, which encapsulates all state and
//! behavior required for an observer to participate in the notification processing
//! pipeline. Observers are long-running components that continuously monitor and
//! respond to notifications from various parts of the system.
//!
//! # Architecture
//!
//! Each observer maintains:
//! - A unique name and type identifier for routing and debugging
//! - A dedicated notification queue for receiving messages
//! - Core logic implementing the [`ObsLogic`] trait for custom behavior
//! - An exit flag for graceful shutdown coordination
//! - A packet handle for communicating with the engine
//!
//! # Lifecycle
//!
//! Observers are initialized with a function closure that typically runs a loop,
//! processing notifications until the exit flag is set. When shutdown is requested,
//! the exit flag signals the loop to terminate and resources are cleaned up via
//! the [`clean_up`](Observer::clean_up) method.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::extras::{jobs::ObsJob, named_input::NamedInput};
use crate::packets::packet_handle::PacketHandle;
use crate::queues::seg_queue::QChild;
use crate::traits::observer_logic::{ObsB, ObsLogic};
use crate::traits::queue::Queue;

/// Core observer structure for the Thryve observer system.
///
/// The `Observer` struct encapsulates all state and behavior required for an
/// observer to participate in the notification processing pipeline. Observers
/// are long-running components that continuously monitor and respond to
/// notifications from various parts of the system.
///
/// # Fields
///
/// - `obs_name`: Unique identifier for the observer
/// - `obs_type`: Type classifier for routing notifications
/// - `obs_queue`: Dedicated queue for receiving notifications
/// - `obs_core`: Core logic implementing custom observer behavior
/// - `exit_flag`: Atomic flag for graceful shutdown coordination
/// - `tx`: Packet handle for communicating with the engine
/// - `fn_to_run`: Initialization closure that starts the observer's main loop
///
/// # Example
///
/// ```no_run
/// // Observers are typically created by implementing ObsLogic and
/// // passing the core logic to Observer::new along with a run function
/// ```
pub struct Observer {
    /// Unique identifier for the observer.
    pub obs_name: String,
    /// Type classifier for routing notifications to the correct observer.
    pub obs_type: NamedInput,
    /// Dedicated queue for receiving notifications.
    pub obs_queue: QChild,
    /// Core logic implementing custom observer behavior.
    pub obs_core: Arc<dyn ObsLogic>,
    /// Atomic flag for graceful shutdown coordination.
    pub exit_flag: AtomicBool,
    /// Packet handle for communicating with the engine.
    pub tx: PacketHandle,
    /// Initialization closure that starts the observer's main loop.
    pub fn_to_run: ObsJob,
}

impl ObsB for Observer {}

impl Observer {
    /// Creates a new observer with the specified configuration.
    ///
    /// Initializes a new observer instance with the given name, type, core logic,
    /// run function, and packet handle. The observer starts with its exit flag
    /// set to `false`, indicating it is ready to process notifications.
    ///
    /// # Arguments
    ///
    /// * `obs_name` - Unique identifier for the observer
    /// * `obs_type` - Type classifier for routing notifications
    /// * `obs_core` - Core logic implementing custom observer behavior
    /// * `func` - Closure that runs the observer's main processing loop
    /// * `tx` - Packet handle for communicating with the engine
    ///
    /// # Returns
    ///
    /// A new `Observer` instance configured with the provided parameters.
    pub fn new(
        obs_name: &str,
        obs_type: &str,
        obs_core: Arc<dyn ObsLogic>,
        func: ObsJob,
        tx: PacketHandle,
    ) -> Self {
        if obs_type.is_empty() {
            panic!("The field `obs_type` in the Observer cannot be empty");
        }

        Self {
            obs_name: obs_name.to_string(),
            obs_type: NamedInput::new(obs_type),
            // Capacity 1: this observer's own dedicated thread (run via
            // `run_loop`) is the only thread that ever pops from obs_queue.
            obs_queue: QChild::new(1, tx.clone()),
            obs_core,
            exit_flag: AtomicBool::new(false),
            tx,
            fn_to_run: func,
        }
    }

    /// Gracefully shuts down the observer.
    ///
    /// Sets the exit flag to signal the observer's main loop to terminate,
    /// invokes the core logic's cleanup method, and finalizes the notification
    /// queue to ensure all pending notifications are processed.
    ///
    /// This method should be called during engine shutdown to ensure proper
    /// resource cleanup and orderly termination of the observer.
    pub fn clean_up(self: &Arc<Self>) {
        self.exit_flag.store(true, Ordering::SeqCst);
        self.obs_queue.finalize(Some(self.clone()));
        self.obs_core.clean_up(self.clone());
    }

    /// Extracts and returns the initialization function.
    ///
    /// Returns the `fn_to_run` closure and replaces it with a default
    /// error function. This ensures the initialization function is only executed
    /// once when the observer is first loaded.
    ///
    /// # Returns
    ///
    /// The initialization function closure, or a default error function if
    /// called multiple times.
    pub fn run_loop(&self, arc_obs: Arc<Observer>) {
        // Take the ObsJob and replace it with a default one (similar to Task::process)
        self.fn_to_run.call_once(arc_obs);
    }
}
