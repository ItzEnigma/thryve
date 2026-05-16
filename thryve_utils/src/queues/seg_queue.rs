//! Segmented queue implementation for notification handling.
//!
//! This module provides a lock-free segmented queue implementation using
//! crossbeam's [`SegQueue`] for efficient notification processing in the
//! observer system. The segmented design allows multiple threads to push
//! and pop notifications concurrently without locking overhead.
//!
//! # Use Case
//!
//! This queue is specifically designed for the observer system, where
//! notifications flow from the engine to various observers. The lock-free
//! design minimizes contention in this high-throughput path.
//!
//! # Features
//!
//! - Lock-free concurrent access via SegQueue
//! - Pause/resume support for controlled shutdown
//! - Integration with the observer logging system

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use crossbeam::queue::SegQueue;

use crate::bases::observer::Observer;
use crate::bases::{notif::Notif, queue::QueueBase};
use crate::packets::{notif_log::Log, packet_handle::PacketHandle};
use crate::traits::queue::Queue;

/// Type alias for the notification segmented queue.
///
/// Uses crossbeam's `SegQueue` for lock-free concurrent access,
/// wrapped in [`QueueBase`] for common queue functionality.
pub type QChild = QueueBase<SegQueue<Notif>, Notif>;

impl Queue<Notif> for QChild {
    /// Pushes a notification into the queue.
    ///
    /// Inserts the notification into the segmented queue and notifies
    /// one waiting thread if the queue is not paused. The operation
    /// is lock-free and thread-safe.
    ///
    /// # Arguments
    ///
    /// * `value` - The notification to push
    fn push(&self, value: Notif) {
        self.main_queue.push(value);

        if self.pause_flag.load(Ordering::SeqCst) == false {
            self.condition_var.notify_one();
        }
    }

    /// Pops a notification from the queue.
    ///
    /// Blocks until a notification is available or the queue is
    /// finalized. Returns a special notification indicating exit
    /// when the queue is being shut down.
    ///
    /// # Returns
    ///
    /// The next notification, or an exit notification on shutdown.
    fn pop(&self) -> Notif {
        loop {
            // Check for shutdown.
            if self.exit_flag.load(Ordering::SeqCst) == true {
                break;
            }

            // Wait if paused.
            else if self.pause_flag.load(Ordering::SeqCst) == true {
                let _unused = self.condition_var.wait(self.gate.lock().unwrap()).unwrap();
            }

            // Try to pop a notification.
            if let Some(notif) = self.main_queue.pop() {
                return notif;
            }

            // Wait for notification.
            let _unused = self.condition_var.wait(self.gate.lock().unwrap()).unwrap();
        }

        // Return exit notification on shutdown.
        Log::log(format!("Exit Flag Detected! Obs Thread Exiting..."), "Main")
    }

    /// Finalizes the queue for shutdown.
    ///
    /// Drains remaining notifications to the logger, sets the exit
    /// flag, and notifies all waiting threads.
    ///
    /// # Arguments
    ///
    /// * `logger` - The logger observer for draining notifications
    fn finalize(&self, logger: Option<Arc<Observer>>) {
        // Drain remaining notifications to logger.
        while self.main_queue.len() != 0 {
            let notif = self.main_queue.pop().unwrap();
            logger.as_ref().unwrap().obs_queue.push(notif);
        }

        self.exit_flag.store(true, Ordering::SeqCst);
        self.condition_var.notify_all();

        let notif = Log::log("Obs Queue Finalized".to_string(), "Main");
        logger.unwrap().obs_queue.push(notif);
    }

    /// Pauses the queue.
    ///
    /// Sets the pause flag, causing `pop()` to block until resumed.
    fn pause(&self) {
        self.pause_flag.store(true, Ordering::SeqCst);

        let notif = Log::log("Obs Queue Paused".to_string(), "Main");
        self.packet_handle.send_notification(notif);
    }

    /// Resumes the queue.
    ///
    /// Clears the pause flag and notifies waiting threads.
    fn resume(&self) {
        self.pause_flag.store(false, Ordering::SeqCst);

        let notif = Log::log("Obs Queue continuing".to_string(), "Main");
        self.packet_handle.send_notification(notif);
    }
}

impl QChild {
    /// Creates a new segmented notification queue.
    ///
    /// Initializes a new `QChild` with the given packet handle.
    /// The queue is ready to accept notifications immediately.
    ///
    /// # Arguments
    ///
    /// * `packet_handle` - Handle for sending notifications
    ///
    /// # Returns
    ///
    /// A new `QChild` instance.
    pub fn new(packet_handle: PacketHandle) -> Self {
        Self {
            task_config: None,
            main_queue: SegQueue::new(),
            gate: Mutex::new(()),
            condition_var: Condvar::new(),
            exit_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
            packet_handle: packet_handle,
            _marker: std::marker::PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packets::packet_handle::PacketHandle;
    use crate::packets::task::Task;
    use crate::packets::notif_log::Log;
    use crate::bases::observer::Observer;
    use crate::traits::observer_logic::ObsLogic;
    use crossbeam::channel::bounded;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        crate::impl_as_any!(TestObsLogic);
        fn clean_up(&self, _observer: Arc<Observer>) {
            // No-op for testing
        }
    }

    #[test]
    fn test_seg_queue_new_creates_queue() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);

        // Act
        let queue = QChild::new(packet_handle);

        // Assert
        assert!(queue.exit_flag.load(Ordering::SeqCst) == false);
        assert!(queue.pause_flag.load(Ordering::SeqCst) == false);
        assert!(queue.task_config.is_none());
    }

    #[test]
    fn test_seg_queue_push_and_pop() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle);
        let notif = Log::log("test message".to_string(), "Main");

        // Act - push notification
        queue.push(notif);

        // Act - pop notification
        let popped = queue.pop();
        
        // Attempt to downcast the notification's core logic to a `Log` type
        // to access its specific fields and functionality.
        let notif_as_any = popped.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<Log>();

        // Assert
        assert_eq!(notif_casted.unwrap().message, "test message");
    }

    #[test]
    fn test_seg_queue_push_multiple_pop_multiple() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle);
        let mut count = 5;

        // Act - push multiple notifications
        for i in 0..count {
            let notif = Log::log(format!("message {}", i), "Main");
            queue.push(notif);
        }

        // Act - pop all notifications
        loop {
            if count == 0 {
                queue.exit_flag.store(true, Ordering::SeqCst);
            }
            
            let notif = queue.pop();


            if queue.exit_flag.load(Ordering::SeqCst) == true {
                // Attempt to downcast the notification's core logic to a `Log` type
                // to access its specific fields and functionality.
                let notif_as_any = notif.notif_core.as_any();
                let notif_casted = notif_as_any.downcast_ref::<Log>();

                if notif_casted.unwrap().message == "Exit Flag Detected! Obs Thread Exiting..."{
                    break;
                }
            }
            count -= 1;
        }

        // Assert
        assert_eq!(count, 0);
    }

    #[test]
    fn test_seg_queue_pause_sets_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle);

        // Assert initially not paused
        assert!(queue.pause_flag.load(Ordering::SeqCst) == false);

        // Act
        queue.pause();

        // Assert
        assert!(queue.pause_flag.load(Ordering::SeqCst) == true);
    }

    #[test]
    fn test_seg_queue_resume_clears_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle);

        // Pause first
        queue.pause();
        assert!(queue.pause_flag.load(Ordering::SeqCst) == true);

        // Act
        queue.resume();

        // Assert
        assert!(queue.pause_flag.load(Ordering::SeqCst) == false);
    }

    #[test]
    fn test_seg_queue_finalize_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle);

        // Create a mock logger observer
        let (logger_task_tx, _logger_task_rx) = bounded::<Task>(10);
        let (logger_notif_tx, _logger_notif_rx) = bounded::<Notif>(10);
        let logger_packet_handle = PacketHandle::new(logger_task_tx, logger_notif_tx);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: crate::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let logger = Arc::new(Observer::new(
            "logger",
            "loggertype",
            obs_core,
            fn_to_run,
            logger_packet_handle,
        ));

        // Assert initially not exiting
        assert!(queue.exit_flag.load(Ordering::SeqCst) == false);

        // Act
        queue.finalize(Some(logger));

        // Assert
        assert!(queue.exit_flag.load(Ordering::SeqCst) == true);
    }

    #[test]
    fn test_seg_queue_pop_returns_exit_notification_when_exit_flag_set() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle);

        // Set exit flag
        queue.exit_flag.store(true, Ordering::SeqCst);

        // Act
        let notif = queue.pop();

        // Attempt to downcast the notification's core logic to a `Log` type
        // to access its specific fields and functionality.
        let notif_as_any = notif.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<Log>();

        // Assert
        assert_eq!(notif_casted.unwrap().message, "Exit Flag Detected! Obs Thread Exiting...");
    }

    #[test]
    fn test_seg_queue_push_notifies_condition_var() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle);
        let notif = Log::log("test".to_string(), "Main");

        // Act - push should notify (queue not paused)
        queue.push(notif);

        // pop to verify notification was queued
        let popped = queue.pop();

        // Attempt to downcast the notification's core logic to a `Log` type
        // to access its specific fields and functionality.
        let notif_as_any = popped.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<Log>();

        // Assert
        assert_eq!(notif_casted.unwrap().message, "test");
    }
}
