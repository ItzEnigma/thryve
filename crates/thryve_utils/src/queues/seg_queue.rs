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
//! - Pause/resume support, and a finalize for the shutdown
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
    /// Waits until a notification is available or the queue is finalized.
    ///
    /// # Returns
    ///
    /// The next notification, or `None` once the queue is finalized, even if notifications are
    /// still queued.
    fn pop(&self) -> Option<Notif> {
        let idx = thread_local::tl_get_worker_park_idx();

        loop {
            if self.is_exit(idx) {
                break;
            }

            // Fast path: take the oldest notification, if there is one.
            let notif = self.main_queue.pop();

            if notif.is_some() {
                return notif;
            }

            // Registered as idle before it looks again, so a push from now on wakes it.
            self.worker_registry.set_idle(idx);

            // Medium path: spin with backoff before committing to sleep.
            let backoff = Backoff::new();

            while !backoff.is_completed() {
                // Try to pop a notification.
                let notif = self.main_queue.pop();
                if notif.is_some() {
                    self.worker_registry.clear_idle(idx);
                    return notif;
                }

                // snooze(), not spin(): only snooze() advances Backoff past
                // its spin-only phase, so is_completed() can ever become true
                // and this loop can fall through to a real park() below.
                backoff.snooze();
            }

            // Slow path: unless the queue is shutting down, park until a push, a resume or the
            // shutdown wakes this thread, then look again.
            if !self.exit_flag.load(Ordering::SeqCst) {
                thread::park();
                self.worker_registry.clear_idle(idx);
            }
        }

        None
    }

    /// Finalizes the queue for shutdown.
    ///
    /// Moves every notification still queued into `notifs`, oldest first, then sets the exit
    /// flag and wakes every waiting thread, so a blocked `pop` returns `None`. The "Obs Queue
    /// Finalized" line goes into `status_reps`.
    ///
    /// For the notification sorter's queue, the observer manager joins the sorter first: it
    /// routes what's left, so nothing is moved here.
    ///
    /// # Arguments
    ///
    /// * `notifs` - Receives the notifications that were still queued
    /// * `status_reps` - Receives the "Obs Queue Finalized" line
    ///
    /// # Returns
    ///
    /// How many notifications were still queued.
    fn finalize(&self, notifs: &mut Vec<Notif>, status_reps: &mut Vec<Notif>) -> usize {
        // The queue is emptied first, then the flag is set and every waiting thread woken: its
        // `pop` returns `None`, so its loop ends.
        let mut left_over = 0;
        while !self.main_queue.is_empty() {
            if let Some(entry) = self.main_queue.pop() {
                notifs.push(entry);
                left_over += 1;
            }
        }

        self.exit_flag.store(true, Ordering::SeqCst);
        self.worker_registry.notify_all();

        let notif = Log::log("Obs Queue Finalized".to_string(), "Main");
        status_reps.push(notif);

        left_over
    }

    /// Pauses the queue.
    ///
    /// Sets the pause flag, causing `pop()` to block until resumed. Except for the startup pause
    /// (`is_start`), sends "Obs Queue Paused".
    fn pause(&self, is_start: bool) {
        self.pause_flag.store(true, Ordering::SeqCst);

        if !is_start {
            let notif = Log::log("Obs Queue Paused".to_string(), "Main");
            self.packet_handle.send_status_log(notif)
        }
    }

    /// Resumes the queue.
    ///
    /// Clears the pause flag and wakes the parked workers. Except when the startup pause ends
    /// (`is_start`), sends "Obs Queue continuing".
    fn resume(&self, is_start: bool) {
        self.pause_flag.store(false, Ordering::SeqCst);

        if !is_start {
            let notif = Log::log("Obs Queue continuing".to_string(), "Main");
            self.packet_handle.send_status_log(notif);
        }

        self.worker_registry.notify_all();
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
    /// * `q_size` - How many threads call `pop` on this queue, which sizes its worker registry:
    ///   1 for the sorter's queue and for an observer's own queue
    /// * `packet_handle` - Handle for sending the queue's status lines ("Obs Queue Paused", and
    ///   so on)
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
