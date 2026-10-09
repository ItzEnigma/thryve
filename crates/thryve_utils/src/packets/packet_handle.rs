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
//! tasks, notifications and Thryve's own status messages. Typed tasks are
//! sent with [`PacketHandle::send_adv_task`], which returns the handle
//! their follow-ups are declared on.
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
//! passed to modules. Since it only holds channel senders and a shared
//! switch, cloning is inexpensive, making it easy to distribute
//! throughout the system.

use std::panic::Location;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::bases::notif::Notif;
use crate::errors::{EngineNotRunningError, ThryveError};
use crate::extras::thread_local;
use crate::graphs::callsite::TaskCallSite;
use crate::packets::task::{AdvTask, AdvTaskHandle, Task};
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
/// # use thryve_utils::packets::notif_log::Log;
/// # use thryve_utils::res::TaskResult;
/// use thryve_utils::packets::packet_handle::PacketHandle;
/// use thryve_utils::packets::task::Task;
///
/// # let (task_tx, _task_rx) = bounded(1);
/// # let (notif_tx, _notif_rx) = bounded(1);
/// # let handle = PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
/// // Send a task to the engine
/// handle.send_task(Task::new("MyTask", 0, || {
///     TaskResult::new("", "".to_string(), "")
/// }).expect("valid task name"))?;
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
    /// Sender for Thryve's own messages to the engine: status lines, and the releases of dropped
    /// task handles. Its channel has no capacity limit, so a send never waits, even while the
    /// engine isn't reading.
    status_tx: Sender<StatusMsg>,
    /// Whether graph mode is on. Shared by every clone of this handle, so one failure turns
    /// graph mode off everywhere.
    is_graph_visualization_enabled: Arc<AtomicBool>,
}

impl PacketHandle {
    /// Creates a new packet handle with the specified channel senders.
    ///
    /// Initializes a new `PacketHandle` with the given task, notification
    /// and status senders. These channels are created by the engine
    /// during initialization and passed to modules.
    ///
    /// When graph visualization is enabled, the packet handle will
    /// automatically capture call site information (file path and line
    /// number) for each task sent through [`send_task`](Self::send_task).
    /// This information is used to build a topology graph showing task
    /// relationships and call hierarchies.
    ///
    /// # Arguments
    ///
    /// * `task_tx` - Sender for tasks
    /// * `notif_tx` - Sender for notifications
    /// * `status_tx` - Sender for Thryve's own messages: status lines, and the releases of
    ///   dropped task handles. Its channel should be unbounded, so that sending on it never waits
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
    /// # use crossbeam::channel::{bounded, unbounded};
    /// use thryve_utils::packets::packet_handle::PacketHandle;
    ///
    /// # let (task_sender, _task_rx) = bounded(1);
    /// # let (notif_sender, _notif_rx) = bounded(1);
    /// # let (status_sender, _status_rx) = unbounded();
    /// // Typically created by the engine during initialization
    /// let handle = PacketHandle::new(task_sender, notif_sender, status_sender, false);
    /// ```
    pub fn new(
        task_tx: Sender<Task>,
        notif_tx: Sender<Notif>,
        status_tx: Sender<StatusMsg>,
        is_graph_vis_enabled: bool,
    ) -> Self {
        Self {
            task_tx,
            notif_tx,
            status_tx,
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
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// # use thryve_utils::res::TaskResult;
    /// use thryve_utils::packets::task::Task;
    ///
    /// # let (task_tx, _task_rx) = bounded(1);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let handle = PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
    /// handle.send_task(Task::new("MyTask", 0, || {
    ///     // Task logic here
    ///     TaskResult::new("", "".to_string(), "")
    /// }).expect("valid task name"))?;
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
    /// # let handle = PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
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
    /// Status lines go through the status mailbox, which has no capacity limit, so this never
    /// waits, even while the engine isn't reading (during startup, a pause or a shutdown) or is
    /// the sender itself. That's safe because Thryve sends a fixed, small number of them; your plugins'
    /// traffic stays on the bounded channels. The engine reads this channel like the others and
    /// routes the lines to the logger.
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
    pub fn send_status_log(&self, notif: Notif) {
        self.send_status_helper(StatusMsg::LogLine(notif));
    }

    /// Tells the engine that the handle of the task `handle_id` is gone without a follow-up, so
    /// nobody will take the task's result: the engine lets it go, and writes an error to
    /// `Error.log`.
    ///
    /// [`AdvTaskHandle`]'s `drop` sends it, unless `then` used the handle. It goes through the
    /// status mailbox because a handle can be dropped anywhere, even on the engine thread, which
    /// must never wait on one of its own bounded mailboxes. A failed send is dropped without an
    /// error: the engine is gone, and the result with it.
    ///
    /// For Thryve's own crates. Not part of the public API.
    ///
    /// # Arguments
    ///
    /// * `handle_id` - The id of the task whose handle was dropped
    #[doc(hidden)]
    pub fn send_status_release(&self, handle_id: u64) {
        self.send_status_helper(StatusMsg::HandleRelease(handle_id));
    }

    /// Sends on the status mailbox. A failed send means the engine is gone, so it's dropped.
    fn send_status_helper(&self, status_msg: StatusMsg) {
        let _ = self.status_tx.send(status_msg);
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

impl PacketHandle {
    /// Sends a typed task ([`AdvTask`]) to the engine.
    ///
    /// It's queued and run like any other task. When it finishes, its result reaches the engine
    /// as a handoff. It returns the task's handle: [`then`](AdvTaskHandle::then) on it declares a
    /// follow-up, which gets the result. The engine keeps the result while the handle exists, so
    /// `then` can come after the task finished. Once the handle is dropped without a follow-up,
    /// an error is written to `Error.log` and a value is dropped.
    ///
    /// Like [`send_task`](Self::send_task), it passes your call's file and line on to graph mode
    /// (`#[track_caller]`).
    ///
    /// # Errors
    ///
    /// Returns [`ThryveError::EngineNotRunning`] ([`EngineNotRunningError::Task`]) when the engine
    /// is gone: it has stopped, or it failed to start. The task is dropped.
    #[track_caller]
    pub fn send_adv_task<T, E>(
        &self,
        adv_task: AdvTask<T, E>,
    ) -> Result<AdvTaskHandle<T, E>, ThryveError>
    where
        T: Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        let task = adv_task.into_task();
        let handle = AdvTaskHandle::new(&task, self.clone());

        self.send_task(task)?;

        Ok(handle)
    }
}

/// What the status mailbox carries: Thryve's own messages to the engine, whose senders never
/// wait.
///
/// For Thryve's own crates. Not part of the public API.
#[doc(hidden)]
pub enum StatusMsg {
    /// A status line, such as "ThreadPool Finalized", for the logger.
    LogLine(Notif),
    /// The handle of the task with this id was dropped without a follow-up, so nobody will take
    /// the task's result.
    HandleRelease(u64),
}
