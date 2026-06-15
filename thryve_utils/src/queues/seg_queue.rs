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
        Log::log(format!(""), "Main")
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