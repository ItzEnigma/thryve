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
use crate::packets::task::Task;

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
}

impl PacketHandle {
    /// Creates a new packet handle with the specified channel senders.
    ///
    /// Initializes a new `PacketHandle` with the given task and
    /// notification senders. These channels are created by the engine
    /// during initialization and passed to modules.
    ///
    /// # Arguments
    ///
    /// * `task_tx` - Sender for tasks
    /// * `notif_tx` - Sender for notifications
    ///
    /// # Returns
    ///
    /// A new `PacketHandle` configured with the provided senders.
    ///
    /// # Example
    ///
    /// ```no_run
    /// // Typically created by the engine during initialization
    /// let handle = PacketHandle::new(task_sender, notif_sender);
    /// ```
    pub fn new(task_tx: Sender<Task>, notif_tx: Sender<Notif>) -> Self {
        Self {
            task_tx: task_tx,
            notif_tx: notif_tx,
        }
    }
 
    /// Sends a task to the engine.
    ///
    /// Transmits the given task through the task channel to the engine
    /// for processing. The task will be queued and executed by worker
    /// threads according to its priority.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to send
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
    pub fn send_task(&self, task: Task) {
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
