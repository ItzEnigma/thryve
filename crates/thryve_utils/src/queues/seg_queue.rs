// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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
//!
//! # Thread Safety
//!
//! `main_queue` is a lock-free [`SegQueue`], so `push()` and the fast path of
//! `pop()` never touch a lock at all. When `pop()` finds nothing, it
//! registers on a [`WorkerRegistry`] (see that module's docs for the
//! invariants this depends on), spins briefly via [`Backoff`], then parks --
//! no mutex or condvar anywhere in the path.

use crossbeam::queue::SegQueue;
use crossbeam::utils::Backoff;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::bases::observer::Observer;
use crate::bases::{notif::Notif, queue::QueueBase};
use crate::extras::thread_local;
use crate::packets::{notif_log::Log, packet_handle::PacketHandle};
use crate::threads::thread_registry::WorkerRegistry;
use crate::traits::queue::Queue;

/// Type alias for the notification segmented queue.
///
/// Uses crossbeam's `SegQueue` for lock-free concurrent access,
/// wrapped in [`QueueBase`] for common queue functionality.
pub type QChild = QueueBase<SegQueue<Notif>, Notif, ()>;

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

        if !self.pause_flag.load(Ordering::SeqCst) {
            self.worker_registry.notify_one();
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
            if self.exit_flag.load(Ordering::SeqCst) {
                break;
            }
            // Wait if paused.
            else if self.pause_flag.load(Ordering::SeqCst) {
                // Mutex lock failure indicates poisoned mutex - fatal error.
                // Condvar wait failure also indicates poisoned mutex - fatal error.
                let idx = thread_local::tl_get_worker_park_idx();

                self.worker_registry.set_idle(idx);
                thread::park();
                self.worker_registry.clear_idle(idx);
            }

            // Try to pop a notification.
            if let Some(notif) = self.main_queue.pop() {
                return notif;
            }

            let idx = thread_local::tl_get_worker_park_idx();
            self.worker_registry.set_idle(idx);

            // 2. Medium Path: Spin backoff before committing to sleep
            let backoff = Backoff::new();

            while !backoff.is_completed() {
                // Try to pop a notification.
                if let Some(notif) = self.main_queue.pop() {
                    self.worker_registry.clear_idle(idx);
                    return notif;
                }

                // snooze(), not spin(): only snooze() advances Backoff past
                // its spin-only phase, so is_completed() can ever become true
                // and this loop can fall through to a real park() below.
                backoff.snooze();
            }

            // Check for shutdown.
            // Wait for notification.
            if !self.exit_flag.load(Ordering::SeqCst) {
                thread::park();
                self.worker_registry.clear_idle(idx);
            }
        }

        // Return exit notification on shutdown.
        Log::log(String::new(), "Main")
    }

    /// Finalizes the queue for shutdown.
    ///
    /// Sets the exit flag and wakes every waiting thread, so a blocked `pop` returns. Then it
    /// sends an "Obs Queue Finalized" line to `logger`, if there is one.
    ///
    /// # Arguments
    ///
    /// * `logger` - The observer that receives the final line, or `None` to skip it (for
    ///   example, when a half-built engine is finalized after a failed startup and has no
    ///   logger)
    fn finalize(&self, logger: Option<Arc<Observer>>) {
        self.exit_flag.store(true, Ordering::SeqCst);
        self.worker_registry.notify_all();

        if let Some(logger) = &logger {
            let notif = Log::log("Obs Queue Finalized".to_string(), "Main");
            logger.obs_queue.push(notif);
        }
    }

    /// Pauses the queue.
    ///
    /// Sets the pause flag, causing `pop()` to block until resumed.
    fn pause(&self) {
        self.pause_flag.store(true, Ordering::SeqCst);

        let notif = Log::log("Obs Queue Paused".to_string(), "Main");
        self.packet_handle.send_status(notif)
    }

    /// Resumes the queue.
    ///
    /// Clears the pause flag and notifies waiting threads.
    fn resume(&self) {
        self.pause_flag.store(false, Ordering::SeqCst);

        let notif = Log::log("Obs Queue continuing".to_string(), "Main");
        self.packet_handle.send_status(notif)
    }

    fn exit_flag(&self) -> Arc<AtomicBool> {
        self.exit_flag.clone()
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
    pub fn new(q_size: usize, packet_handle: PacketHandle) -> Self {
        Self {
            task_config: Arc::new(()),
            main_queue: SegQueue::new(),
            worker_registry: Arc::new(WorkerRegistry::new(q_size)),
            exit_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
            packet_handle,
            _marker: std::marker::PhantomData,
        }
    }
}
