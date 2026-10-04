// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Trait for queue implementations.
//!
//! This module defines the [`Queue`] trait, which specifies the interface
//! for all queue implementations in the Thryve system. This trait enables
//! generic implementations of thread pools that can work with different
//! queue types (priority queues, segmented queues, etc.).
//!
//! # Purpose
//!
//! The `Queue` trait allows the thread pool implementation to be
//! generalized, enabling a single implementation to handle different
//! queue types. This promotes code reuse and simplifies maintenance.
//!
//! # Queue Types
//!
//! - [`PQChild`](crate::queues::priority_queue::PQChild): Priority queue for tasks
//! - [`QChild`](crate::queues::seg_queue::QChild): Segmented queue for notifications

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::bases::notif::Notif;

/// Trait for queue implementations.
///
/// The `Queue` trait specifies the interface for all queue implementations
/// in the Thryve system. It enables generic implementations of thread pools
/// that can work with different queue types.
///
/// # Type Parameters
///
/// - `T`: The type of items stored in the queue
///
/// # Trait Bounds
///
/// - `Send`: Queues can be transferred between threads
/// - `Sync`: Queues can be shared between threads
/// - `'static`: Queues have no non-'static references
///
/// # Queue Types
///
/// - [`PQChild`](crate::queues::priority_queue::PQChild): Priority queue for tasks
/// - [`QChild`](crate::queues::seg_queue::QChild): Segmented queue for notifications
///
/// # Example
///
/// ```no_run
/// # use crossbeam::channel::bounded;
/// # use thryve_utils::packets::notif_log::Log;
/// # use thryve_utils::packets::packet_handle::PacketHandle;
/// # use thryve_utils::queues::seg_queue::QChild;
/// use thryve_utils::traits::queue::Queue;
///
/// # let (task_tx, _task_rx) = bounded(1);
/// # let (notif_tx, _notif_rx) = bounded(1);
/// # let handle = PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
/// // Using a segmented queue
/// let sq = QChild::new(1, handle);
/// # let my_notification = Log::log("hello".to_string(), "Main");
/// sq.push(my_notification);
/// let notif = sq.pop();
/// ```
pub trait Queue<T>: Send + Sync + 'static {
    /// Adds an item to the queue.
    ///
    /// Inserts the given value into the queue. The item will be
    /// available for retrieval via `pop()` according to the queue's
    /// ordering semantics (FIFO, priority-based, etc.).
    ///
    /// # Arguments
    ///
    /// * `value` - The item to add to the queue
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::packets::notif_log::Log;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// # use thryve_utils::queues::seg_queue::QChild;
    /// # use thryve_utils::traits::queue::Queue;
    /// #
    /// # let (task_tx, _task_rx) = bounded(1);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let queue = QChild::new(1, PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false));
    /// # let my_item = Log::log("hello".to_string(), "Main");
    /// queue.push(my_item);
    /// ```
    fn push(&self, value: T);

    /// Removes and returns the next item from the queue.
    ///
    /// Retrieves the next item according to the queue's ordering semantics. While the queue is
    /// empty or paused, it waits: it never returns just because the queue is empty for a moment.
    ///
    /// # Returns
    ///
    /// `Some(item)`, or `None` once the queue is finalized (its exit flag is set). `None` comes
    /// back as soon as the flag is set, even if items are still queued: what happens to them is
    /// the caller's choice. For example, the notification sorter routes them before it stops.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// # use thryve_utils::queues::seg_queue::QChild;
    /// # use thryve_utils::traits::queue::Queue;
    /// #
    /// # let (task_tx, _task_rx) = bounded(1);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let queue = QChild::new(1, PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false));
    /// while let Some(item) = queue.pop() {
    ///     // Handle the item
    /// #   let _ = item;
    /// }
    /// // The queue was finalized
    /// ```
    fn pop(&self) -> Option<T>;

    /// Finalizes the queue for shutdown.
    ///
    /// Sets the exit flag and wakes every thread waiting in [`pop`](Self::pop), which then
    /// returns `None`. Every item still queued is moved into `packets`, in the order `pop` would
    /// have returned them, and the queue's final status line goes into `status_reps`, for the
    /// logger to write at the end of the shutdown.
    ///
    /// # Arguments
    ///
    /// * `packets` - Receives the items that were still queued
    /// * `status_reps` - Receives the queue's final status line ("Task Queue Finalized" or "Obs
    ///   Queue Finalized")
    ///
    /// # Returns
    ///
    /// How many items were still queued.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// # use thryve_utils::queues::seg_queue::QChild;
    /// # use thryve_utils::traits::queue::Queue;
    /// #
    /// # let (task_tx, _task_rx) = bounded(1);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let queue = QChild::new(1, PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false));
    /// let mut left_over = Vec::new();
    /// let mut status_reps = Vec::new();
    /// let count = queue.finalize(&mut left_over, &mut status_reps);
    /// ```
    fn finalize(&self, packets: &mut Vec<T>, status_reps: &mut Vec<Notif>) -> usize;

    /// Pauses the queue.
    ///
    /// Causes `pop()` to block until `resume()` is called. This is
    /// used for controlled shutdown and debugging.
    ///
    /// # Arguments
    ///
    /// * `is_start` - `true` only for the engine's startup pause, which isn't logged; any other
    ///   pause sends a status line
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// # use thryve_utils::queues::seg_queue::QChild;
    /// # use thryve_utils::traits::queue::Queue;
    /// #
    /// # let (task_tx, _task_rx) = bounded(1);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let queue = QChild::new(1, PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false));
    /// queue.pause(false);
    /// // pop() will block here
    /// queue.resume(false);
    /// ```
    fn pause(&self, is_start: bool);

    /// Resumes the queue.
    ///
    /// Unblocks any threads waiting in `pop()` due to a pause.
    ///
    /// # Arguments
    ///
    /// * `is_start` - `true` only when the engine ends its startup pause, which isn't logged; any
    ///   other resume sends a status line
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// # use thryve_utils::queues::seg_queue::QChild;
    /// # use thryve_utils::traits::queue::Queue;
    /// #
    /// # let (task_tx, _task_rx) = bounded(1);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let queue = QChild::new(1, PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false));
    /// queue.resume(false);
    /// // pop() will now continue
    /// ```
    fn resume(&self, is_start: bool);

    /// Returns the queue's own shutdown flag.
    ///
    /// This is the flag [`pop`](Self::pop) checks before it looks for an item and after it
    /// wakes. Thread pools adopt it as their own shutdown signal rather than allocating a
    /// separate one, so a single store stops both the pool's loop and every `pop`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// # use thryve_utils::queues::seg_queue::QChild;
    /// # use thryve_utils::traits::queue::Queue;
    /// #
    /// # let (task_tx, _task_rx) = bounded(1);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let queue = QChild::new(1, PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false));
    /// let shared_flag = queue.exit_flag();
    /// ```
    fn exit_flag(&self) -> Arc<AtomicBool>;
}
