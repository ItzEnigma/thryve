// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Packet handle for engine communication.
//!
//! This module provides the [`PacketHandle`] struct, which serves as the
//! exclusive interface for plugins and observers to communicate with the
//! engine. It encapsulates the sender ends of crossbeam channels for
//! tasks and notifications.
//!
//! # Architecture
//!
//! The packet handle ensures that:
//! - Plugins and observers cannot directly access the engine
//! - All communication flows through controlled channels
//! - The engine remains isolated from plugin/observer internals
//! - Multithreaded communication is safe and efficient
//!
//! # Usage
//!
//! The packet handle is created by the engine during initialization and
//! passed to modules. Since it only contains two channel senders,
//! cloning is inexpensive, making it easy to distribute throughout
//! the system.

use std::panic::Location;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::bases::notif::Notif;
use crate::errors::{EngineNotRunningError, ThryveError};
use crate::extras::thread_local;
use crate::graphs::callsite::TaskCallSite;
use crate::packets::task::Task;
use crate::traits::packets::Packet;

use crossbeam::channel::Sender;

/// Handle for sending packets to the engine.
///
/// The `PacketHandle` encapsulates the sender ends of crossbeam channels
/// for tasks and notifications. It serves as the exclusive interface for
/// plugins and observers to communicate with the engine, ensuring that:
///
/// - The engine remains isolated from plugin/observer internals
/// - All communication flows through controlled channels
/// - Multithreaded communication is safe and efficient
///
/// # Cloning
///
/// The packet handle implements `Clone` because it only contains channel
/// senders and the graph-mode switch. Cloning is inexpensive and allows the handle to be easily
/// distributed throughout the system. Every clone shares the same switch, so when graph mode
/// turns off, it turns off for all of them.
///
/// # Example
///
/// ```no_run
/// # use crossbeam::channel::bounded;
/// # use thryve_utils::extras::jobs::TaskJob;
/// # use thryve_utils::packets::notif_log::Log;
/// # use thryve_utils::res::TaskResult;
/// use thryve_utils::packets::packet_handle::PacketHandle;
/// use thryve_utils::packets::task::Task;
///
/// # let (task_tx, _task_rx) = bounded(1);
/// # let (notif_tx, _notif_rx) = bounded(1);
/// # let handle = PacketHandle::new(task_tx, notif_tx, false);
/// // Send a task to the engine
/// handle.send_task(Task::new("MyTask", 0, TaskJob::new(|| {
///     TaskResult::new("", "".to_string(), "")
/// })).expect("valid task name"))?;
///
/// // Send a notification to the engine
/// # let my_notification = Log::log("hello".to_string(), "Main");
/// handle.send_notification(my_notification)?;
/// # Ok::<(), thryve_utils::errors::ThryveError>(())
/// ```
#[derive(Clone)]
pub struct PacketHandle {
    /// Sender for tasks.
    task_tx: Sender<Task>,
    /// Sender for notifications.
    notif_tx: Sender<Notif>,
    /// Whether graph mode is on. Shared by every clone of this handle, so one failure turns
    /// graph mode off everywhere.
    is_graph_visualization_enabled: Arc<AtomicBool>,
}

impl PacketHandle {
    /// Creates a new packet handle with the specified channel senders.
    ///
    /// Initializes a new `PacketHandle` with the given task and
    /// notification senders. These channels are created by the engine
    /// during initialization and passed to modules.
    ///
    /// When graph visualization is enabled, the packet handle will
    /// automatically capture call site information (file path, module
    /// path, line number) for each task sent through [`send_task`](Self::send_task).
    /// This information is used to build a topology graph showing task
    /// relationships and call hierarchies.
    ///
    /// # Arguments
    ///
    /// * `task_tx` - Sender for tasks
    /// * `notif_tx` - Sender for notifications
    /// * `is_graph_vis_enabled` - Whether graph visualization is enabled.
    ///   When true, call site information is captured for each task, until graph mode fails
    ///   and turns off.
    ///
    /// # Returns
    ///
    /// A new `PacketHandle` configured with the provided senders.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// use thryve_utils::packets::packet_handle::PacketHandle;
    ///
    /// # let (task_sender, _task_rx) = bounded(1);
    /// # let (notif_sender, _notif_rx) = bounded(1);
    /// // Typically created by the engine during initialization
    /// let handle = PacketHandle::new(task_sender, notif_sender, false);
    /// ```
    pub fn new(task_tx: Sender<Task>, notif_tx: Sender<Notif>, is_graph_vis_enabled: bool) -> Self {
        Self {
            task_tx,
            notif_tx,
            is_graph_visualization_enabled: Arc::new(AtomicBool::new(is_graph_vis_enabled)),
        }
    }

    /// Sends a task to the engine.
    ///
    /// Transmits the given task through the task channel to the engine
    /// for processing. The task will be queued and executed by worker
    /// threads according to its priority.
    ///
    /// ## Graph Visualization
    ///
    /// When graph visualization is enabled (via the `is_graph_vis_enabled`
    /// parameter in [`PacketHandle::new`](Self::new)), this method automatically:
    ///
    /// 1. Records the call site: the file and line of the call to this method
    /// 2. Computes a unique fingerprint for this call site
    /// 3. Retrieves the fingerprint of the currently executing task (parent)
    /// 4. Attaches this call site information to the task
    ///
    /// This information is used by the topology handler (`TopologyHandlerObserver`, in `thryve_observers`)
    /// to build a topology graph showing task creation relationships.
    ///
    /// The compiler passes the call site in (`#[track_caller]`), so it's always found and exact,
    /// whatever the build's optimization level or debug info. A function of yours that wraps this
    /// method is the call site for every task it sends; mark the wrapper `#[track_caller]` too, and
    /// its own caller becomes the call site.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to send
    ///
    /// # Errors
    ///
    /// Returns [`ThryveError::EngineNotRunning`] ([`EngineNotRunningError::Task`]) when the engine
    /// is gone: it has stopped, or it failed to start. The task is dropped.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::extras::jobs::TaskJob;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// # use thryve_utils::res::TaskResult;
    /// use thryve_utils::packets::task::Task;
    ///
    /// # let (task_tx, _task_rx) = bounded(1);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let handle = PacketHandle::new(task_tx, notif_tx, false);
    /// handle.send_task(Task::new("MyTask", 0, TaskJob::new(|| {
    ///     // Task logic here
    ///     TaskResult::new("", "".to_string(), "")
    /// })).expect("valid task name"))?;
    /// # Ok::<(), thryve_utils::errors::ThryveError>(())
    /// ```
    #[track_caller]
    pub fn send_task(&self, mut task: Task) -> Result<(), ThryveError> {
        if self.graph_mode_on() {
            let caller = Location::caller();
            let file_path = PathBuf::from(caller.file().to_string());
            let line_num = caller.line();
            let task_name = task.task_name().to_string();

            let mut callsite = TaskCallSite::new(file_path, line_num, task_name);

            let child_fingerprint = callsite.fingerprint();
            let current_task_fingerprint = thread_local::tl_get_current_task_fingerprint();

            callsite.this_callsite_fingerprint = child_fingerprint;
            callsite.current_executing_task_fingerprint = current_task_fingerprint;

            task.set_callsite(callsite.clone());
        }

        // A send fails only once the engine is gone (its receiving end was dropped). The error
        // holds the task, so dropping the error drops the task.
        self.task_tx.send(task).map_err(|e| {
            drop(e);
            EngineNotRunningError::Task
        })?;

        Ok(())
    }

    /// Sends a notification to the engine.
    ///
    /// Transmits the given notification through the notification channel
    /// to the engine for processing. The notification will be routed to
    /// the appropriate observer based on its receiver field.
    ///
    /// # Arguments
    ///
    /// * `notif` - The notification to send
    ///
    /// # Errors
    ///
    /// Returns [`ThryveError::EngineNotRunning`] ([`EngineNotRunningError::Notif`]) when the
    /// engine is gone: it has stopped, or it failed to start. The notification is dropped.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// use thryve_utils::packets::notif_log::Log;
    ///
    /// # let (task_tx, _task_rx) = bounded(1);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let handle = PacketHandle::new(task_tx, notif_tx, false);
    /// handle.send_notification(Log::log("Hello".to_string(), "Main"))?;
    /// # Ok::<(), thryve_utils::errors::ThryveError>(())
    /// ```
    pub fn send_notification(&self, notif: Notif) -> Result<(), ThryveError> {
        // A send fails only once the engine is gone (its receiving end was dropped). The error
        // holds the notification, so dropping the error drops the notification.
        self.notif_tx.send(notif).map_err(|e| {
            drop(e);
            EngineNotRunningError::Notif
        })?;

        Ok(())
    }

    /// Sends one of Thryve's own status lines, such as "ThreadPool Finalized", to the engine.
    ///
    /// Unlike [`send_notification`](Self::send_notification), a failed send is dropped without an
    /// error, by design: the line has nowhere else to go, and a status line must never stop a
    /// shutdown halfway. Every such line goes through this one method, so changing where they go
    /// is a change in one place.
    ///
    /// It's public only because Thryve's other crates use it. Plugins and observers use
    /// [`send_notification`](Self::send_notification).
    ///
    /// # Arguments
    ///
    /// * `notif` - The status line
    #[doc(hidden)]
    pub fn send_status(&self, notif: Notif) {
        let _ = self.send_notification(notif);
    }

    /// Returns whether graph mode is on.
    ///
    /// For Thryve's own crates. Not part of the public API.
    #[doc(hidden)]
    pub fn graph_mode_on(&self) -> bool {
        self.is_graph_visualization_enabled.load(Ordering::SeqCst)
    }

    /// Turns graph mode off for every clone of this handle, until the program restarts.
    ///
    /// Returns `true` if this call turned it off, so that only the first failure is reported.
    /// For Thryve's own crates. Not part of the public API.
    #[doc(hidden)]
    pub fn turn_graph_mode_off(&self) -> bool {
        self.is_graph_visualization_enabled
            .swap(false, Ordering::SeqCst)
    }
}
