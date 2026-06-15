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

use crate::bases::notif::Notif;
use crate::extras::stack::get_task_data;
use crate::extras::thread_local;
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
/// senders. Cloning is inexpensive and allows the handle to be easily
/// distributed throughout the system.
///
/// # Example
///
/// ```no_run
/// use thryve_utils::packets::packet_handle::PacketHandle;
/// use thryve_utils::packets::task::Task;
///
/// // Send a task to the engine
/// handle.send_task(Task::new("MyTask", 0, Box::new(|| /* ... */)));
///
/// // Send a notification to the engine
/// handle.send_notification(my_notification);
/// ```
#[derive(Clone)]
pub struct PacketHandle {
    /// Sender for tasks.
    task_tx: Sender<Task>,
    /// Sender for notifications.
    notif_tx: Sender<Notif>,
    /// Bool for whether graph visualization is enabled or not.
    is_graph_visualization_enabled: bool,
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
    ///   When true, call site information is captured for each task.
    ///
    /// # Returns
    ///
    /// A new `PacketHandle` configured with the provided senders.
    ///
    /// # Example
    ///
    /// ```no_run
    /// // Typically created by the engine during initialization
    /// let handle = PacketHandle::new(task_sender, notif_sender, false);
    /// ```
    pub fn new(task_tx: Sender<Task>, notif_tx: Sender<Notif>, is_graph_vis_enabled: bool) -> Self {
        Self {
            task_tx: task_tx,
            notif_tx: notif_tx,
            is_graph_visualization_enabled: is_graph_vis_enabled
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
    /// 1. Captures a stack backtrace to determine the call site
    /// 2. Extracts file path, module path, and line number information
    /// 3. Computes a unique fingerprint for this call site
    /// 4. Retrieves the fingerprint of the currently executing task (parent)
    /// 5. Attaches this call site information to the task
    ///
    /// This information is used by the [`TopologyHandlerObserver`](crate::topology_handler::TopologyHandlerObserver)
    /// to build a topology graph showing task creation relationships.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to send
    ///
    /// # Panics
    ///
    /// Panics if the task cannot be sent to the engine.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::task::Task;
    ///
    /// handle.send_task(Task::new("MyTask", 0, Box::new(|| {
    ///     // Task logic here
    ///     Ok(())
    /// })));
    /// ```
    pub fn send_task(&self, mut task: Task) {
        if self.is_graph_visualization_enabled{
            // Capture backtrace
            let backtrace = backtrace::Backtrace::new();
            let mut callsite = get_task_data(backtrace , task.task_name.to_string());

            let child_fingerprint = callsite.fingerprint();
            let current_task_fingerprint = thread_local::tl_get_current_task_fingerprint();

            callsite.this_callsite_fingerprint = child_fingerprint;
            callsite.current_executing_task_fingerprint = current_task_fingerprint;

            task.set_callsite(callsite.clone());
        }

        let _ = self.task_tx.send(task)
        .unwrap_or_else(|e| panic!("Failed to send task to engine: {}" , e));
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
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::notif_log::Log;
    ///
    /// handle.send_notification(Log::log("Hello".to_string(), "Main"));
    /// ```
    pub fn send_notification(&self, notif: Notif) {
        let _ = self.notif_tx.send(notif)
        .unwrap_or_else(|e| panic!("Failed to send notification to engine: {}", e));
    }
}