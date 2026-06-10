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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packets::notif_log::Log;
    use crate::res::Result;

    #[test]
    fn test_packet_handle_new_creates_handle() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);

        // Act
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Assert - handle is created successfully (no panic)
        drop(handle);
        // Verify channels are still functional
        assert!(task_rx.try_recv().is_err()); // Empty
        assert!(notif_rx.try_recv().is_err()); // Empty
    }

    #[test]
    fn test_send_task_sends_to_channel() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);
        let task = Task::new("TestTask", 0, Box::new(|| Result::new("Success", "Done".to_string(), "Main")));

        // Act
        handle.send_task(task);

        // Assert
        let received = task_rx.recv().unwrap();
        assert_eq!(received.task_name.inner(), "TestTask");
        // Notification channel should be empty
        assert!(notif_rx.try_recv().is_err());
    }

    #[test]
    fn test_send_notification_sends_to_channel() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);
        let notif = Log::log("Test message".to_string(), "Main");

        // Act
        handle.send_notification(notif);

        // Assert
        let received = notif_rx.recv().unwrap();
        assert_eq!(received.notif_type.inner(), "Log");
        // Task channel should be empty
        assert!(task_rx.try_recv().is_err());
    }

    #[test]
    fn test_packet_handle_clone() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, _) = crossbeam::channel::bounded::<Notif>(10);
        let handle1 = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        let handle2 = handle1.clone();

        // Assert - both handles can send
        let task1 = Task::new("Task1", 0, Box::new(|| Result::new("Success", "Done".to_string(), "Main")));
        let task2 = Task::new("Task2", 0, Box::new(|| Result::new("Success", "Done".to_string(), "Main")));
        
        handle1.send_task(task1);
        handle2.send_task(task2);

        let received1 = task_rx.recv().unwrap();
        let received2 = task_rx.recv().unwrap();
        
        assert!(received1.task_name.inner() == "Task1" || received1.task_name.inner() == "Task2");
        assert!(received2.task_name.inner() == "Task1" || received2.task_name.inner() == "Task2");
    }

    #[test]
    fn test_send_multiple_tasks() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        for i in 0..5 {
            let task = Task::new(&format!("Task{}", i), 0, Box::new(|| Result::new("Success", "Done".to_string(), "Main")));
            handle.send_task(task);
        }

        // Assert
        for _ in 0..5 {
            let received = task_rx.recv().unwrap();
            assert!(received.task_name.inner().starts_with("Task"));
        }
        assert!(task_rx.try_recv().is_err()); // Channel empty
        assert!(notif_rx.try_recv().is_err()); // Notification channel empty
    }

    #[test]
    fn test_send_multiple_notifications() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        for i in 0..5 {
            let notif = Log::log(format!("Message {}", i), "Main");
            handle.send_notification(notif);
        }

        // Assert
        for _ in 0..5 {
            let received = notif_rx.recv().unwrap();
            assert_eq!(received.notif_type.inner(), "Log");
        }
        assert!(notif_rx.try_recv().is_err()); // Channel empty
        assert!(task_rx.try_recv().is_err()); // Task channel empty
    }

    #[test]
    fn test_send_task_and_notification_interleaved() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act - send interleaved
        handle.send_task(Task::new("T1", 0, Box::new(|| Result::new("Success", "Done".to_string(), "Main"))));
        handle.send_notification(Log::log("N1".to_string(), "Main"));
        handle.send_task(Task::new("T2", 0, Box::new(|| Result::new("Success", "Done".to_string(), "Main"))));
        handle.send_notification(Log::log("N2".to_string(), "Main"));

        // Assert - tasks go to task channel
        assert_eq!(task_rx.recv().unwrap().task_name.inner(), "T1");
        assert_eq!(task_rx.recv().unwrap().task_name.inner(), "T2");
        assert!(task_rx.try_recv().is_err());

        // Notifications go to notification channel
        assert_eq!(notif_rx.recv().unwrap().notif_type.inner(), "Log");
        assert_eq!(notif_rx.recv().unwrap().notif_type.inner(), "Log");
        assert!(notif_rx.try_recv().is_err());
    }

    #[test]
    fn test_send_task_with_different_priorities() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, _) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        handle.send_task(Task::new("Low", 0, Box::new(|| Result::new("Success", "Done".to_string(), "Main"))));
        handle.send_task(Task::new("Medium", 50, Box::new(|| Result::new("Success", "Done".to_string(), "Main"))));
        handle.send_task(Task::new("High", 100, Box::new(|| Result::new("Success", "Done".to_string(), "Main"))));

        // Assert
        let t1 = task_rx.recv().unwrap();
        let t2 = task_rx.recv().unwrap();
        let t3 = task_rx.recv().unwrap();
        
        let priorities: Vec<u8> = vec![t1.base_priority, t2.base_priority, t3.base_priority];
        assert!(priorities.contains(&0));
        assert!(priorities.contains(&50));
        assert!(priorities.contains(&100));
    }

    #[test]
    fn test_send_error_notification() {
        // Arrange
        let (task_tx, _) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        let notif = Log::err("Error occurred".to_string());
        handle.send_notification(notif);

        // Assert
        let received = notif_rx.recv().unwrap();
        assert_eq!(received.notif_type.inner(), "Error");
    }

    #[test]
    #[should_panic(expected = "NamedInput cannot be empty")]
    fn test_packet_handle_with_empty_task_name() {
        // Arrange
        let (task_tx, _task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, _notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act & Assert - should panic with empty name
        let task = Task::new("", 0, Box::new(|| Result::new("Success", "Done".to_string(), "Main")));
        handle.send_task(task);
    }

    #[test]
    fn test_packet_handle_with_unicode_message() {
        // Arrange
        let (task_tx, _) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        let notif = Log::log("Hello 世界 🌍".to_string(), "Main");
        handle.send_notification(notif);

        // Assert
        let received = notif_rx.recv().unwrap();
        assert_eq!(received.notif_type.inner(), "Log");
    }
}
