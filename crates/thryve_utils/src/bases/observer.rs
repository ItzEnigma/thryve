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

use crate::bases::notif::Notif;
use crate::errors::{NameInputType, NamingError};
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
/// - `obs_type`: Type classifier for routing notifications; it identifies the observer and must be unique
/// - `obs_queue`: Dedicated queue for receiving notifications
/// - `obs_core`: Core logic implementing custom observer behavior
/// - `exit_flag`: Atomic flag for graceful shutdown coordination
/// - `tx`: Packet handle for communicating with the engine
/// - `obs_job`: Initialization closure that starts the observer's main loop
///
/// # Example
///
/// An observer's core logic implements [`ObsLogic`]; [`Observer::new`] adds its type and the
/// loop its thread runs.
///
/// ```no_run
/// use std::sync::Arc;
/// use thryve_utils::bases::observer::Observer;
/// use thryve_utils::extras::jobs::ObsJob;
/// use thryve_utils::impl_as_any;
/// use thryve_utils::packets::packet_handle::PacketHandle;
/// use thryve_utils::traits::observer_logic::ObsLogic;
/// use thryve_utils::traits::queue::Queue;
///
/// // A core with nothing to clean up.
/// struct Metrics;
///
/// impl ObsLogic for Metrics {
///     impl_as_any!(Metrics);
///
///     fn clean_up(&self, _observer: Arc<Observer>) {}
/// }
///
/// # let (task_tx, _) = crossbeam::channel::bounded(1);
/// # let (notif_tx, _) = crossbeam::channel::bounded(1);
/// # let handle = PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
/// // The loop takes each notification sent to "Metrics", until the shutdown finalizes the queue.
/// let job = ObsJob::new(|observer: Arc<Observer>| {
///     while let Some(_notif) = observer.obs_queue.pop() {
///         // Handle the notification here.
///     }
/// });
/// let observer = Observer::new("Metrics", Arc::new(Metrics), job, handle)?;
/// # Ok::<(), thryve_utils::errors::NamingError>(())
/// ```
pub struct Observer {
    /// Type classifier for routing notifications to the correct observer.
    obs_type: NamedInput,
    /// Dedicated queue for receiving notifications.
    pub obs_queue: QChild,
    /// Core logic implementing custom observer behavior.
    pub obs_core: Arc<dyn ObsLogic>,
    /// Atomic flag for graceful shutdown coordination.
    pub exit_flag: AtomicBool,
    /// Packet handle for communicating with the engine.
    pub tx: PacketHandle,
    /// Initialization closure that starts the observer's main loop.
    pub obs_job: ObsJob,
}

impl ObsB for Observer {}

impl Observer {
    /// Creates a new observer with the specified configuration.
    ///
    /// Initializes a new observer instance with the given type, core logic,
    /// run function, and packet handle. The observer starts with its exit flag
    /// set to `false`, indicating it is ready to process notifications.
    ///
    /// # Arguments
    ///
    /// * `obs_type` - Type classifier for routing notifications; it identifies the observer and must be unique
    /// * `obs_core` - Core logic implementing custom observer behavior
    /// * `obs_job`  - Closure that runs the observer's main processing loop
    /// * `tx` - Packet handle for communicating with the engine
    ///
    /// # Returns
    ///
    /// A new `Observer` instance configured with the provided parameters.
    ///
    /// # Errors
    ///
    /// Returns [`NamingError::NameEmpty`] if `obs_type` is empty, or
    /// [`NamingError::NameContainsControlChar`] if it contains a control character. The
    /// error's kind is `NameInputType::ObsType`.
    pub fn new(
        obs_type: &str,
        obs_core: Arc<dyn ObsLogic>,
        obs_job: ObsJob,
        tx: PacketHandle,
    ) -> Result<Self, NamingError> {
        Ok(Self {
            obs_type: NamedInput::new(obs_type, NameInputType::ObsType)?,
            // Capacity 1: this observer's own dedicated thread (run via
            // `run_loop`) is the only thread that ever pops from obs_queue.
            obs_queue: QChild::new(1, tx.clone()),
            obs_core,
            exit_flag: AtomicBool::new(false),
            tx,
            obs_job,
        })
    }

    /// Gracefully shuts down the observer.
    ///
    /// Sets the exit flag to signal the observer's main loop to terminate, then invokes the
    /// core logic's cleanup method, which decides what happens to the notifications still in
    /// the queue. Then it finalizes the queue, which wakes the observer's thread so it stops,
    /// and hands back whatever the cleanup left in the queue.
    ///
    /// This method should be called during engine shutdown, after the notification sorter has
    /// stopped, so that nothing new reaches the queue.
    ///
    /// # Arguments
    ///
    /// * `notifs` - Receives the notifications the cleanup left in the queue
    /// * `status_reps` - Receives the queue's "Obs Queue Finalized" line
    ///
    /// # Returns
    ///
    /// How many notifications the cleanup left in the queue.
    pub fn clean_up(
        self: &Arc<Self>,
        notifs: &mut Vec<Notif>,
        status_reps: &mut Vec<Notif>,
    ) -> usize {
        // A loop that checks the flag between notifications (the built-in observers' loops do)
        // stops after the one it's on.
        self.exit_flag.store(true, Ordering::SeqCst);
        // The observer's own cleanup: it decides what happens to what's still queued.
        self.obs_core.clean_up(self.clone());
        // Wakes the thread if it's waiting in `pop`, which then returns `None`, so the loop ends
        // even if nothing new arrives.
        self.obs_queue.finalize(notifs, status_reps)
    }

    /// Stops the observer without running its cleanup, for a shutdown that's out of time.
    ///
    /// Like [`clean_up`](Self::clean_up), but the core logic's cleanup is skipped: it sets the
    /// exit flag, then finalizes the queue, which wakes the observer's thread so it stops, and
    /// hands back what was still queued. The observer manager calls it for an observer whose
    /// turn comes after the shutdown's deadline.
    ///
    /// # Arguments
    ///
    /// * `notifs` - Receives the notifications still in the queue
    /// * `status_reps` - Receives the queue's "Obs Queue Finalized" line
    ///
    /// # Returns
    ///
    /// How many notifications were still in the queue.
    pub fn clean_up_after_grace_period(
        self: &Arc<Self>,
        notifs: &mut Vec<Notif>,
        status_reps: &mut Vec<Notif>,
    ) -> usize {
        // As in `clean_up`, without the observer's own cleanup in between.
        self.exit_flag.store(true, Ordering::SeqCst);
        self.obs_queue.finalize(notifs, status_reps)
    }

    /// Extracts and returns the initialization function.
    ///
    /// Returns the `obs_job` closure and replaces it with a default
    /// error function. This ensures the initialization function is only executed
    /// once when the observer is first loaded.
    ///
    /// # Returns
    ///
    /// The initialization function closure, or a default error function if
    /// called multiple times.
    pub fn run_loop(&self, arc_obs: Arc<Observer>) {
        // Take the ObsJob and replace it with a default one (similar to Task::process)
        self.obs_job.call_once(arc_obs);
    }

    /// Returns the observer's type, which identifies it and routes notifications to it.
    pub fn obs_type(&self) -> &str {
        self.obs_type.inner()
    }

    /// Returns the observer's type as a checked name.
    ///
    /// For Thryve's own crates, which key their maps by checked names. Not part of the public API.
    #[doc(hidden)]
    pub fn obs_type_input(&self) -> &NamedInput {
        &self.obs_type
    }
}

impl Observer {
    /// Creates the topology handler's observer, of type `TopologyHandler`.
    ///
    /// The type is a fixed name, so this can't fail. Use [`Observer::new`] for any other observer.
    pub fn new_topo_handler(
        obs_core: Arc<dyn ObsLogic>,
        obs_job: ObsJob,
        tx: PacketHandle,
    ) -> Self {
        let obs_type = NamedInput::new_topo_handler();
        Self {
            obs_type,
            // Capacity 1: this observer's own dedicated thread (run via
            // `run_loop`) is the only thread that ever pops from obs_queue.
            obs_queue: QChild::new(1, tx.clone()),
            obs_core,
            exit_flag: AtomicBool::new(false),
            tx,
            obs_job,
        }
    }

    /// Creates the logger's observer, of type `Logging`.
    ///
    /// The type is a fixed name, so this can't fail. Use [`Observer::new`] for any other observer.
    pub fn new_logging(obs_core: Arc<dyn ObsLogic>, obs_job: ObsJob, tx: PacketHandle) -> Self {
        let obs_type = NamedInput::new_logging();
        Self {
            obs_type,
            // Capacity 1: this observer's own dedicated thread (run via
            // `run_loop`) is the only thread that ever pops from obs_queue.
            obs_queue: QChild::new(1, tx.clone()),
            obs_core,
            exit_flag: AtomicBool::new(false),
            tx,
            obs_job,
        }
    }
}
