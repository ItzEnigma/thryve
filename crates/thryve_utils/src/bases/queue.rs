// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Base queue structure for the Thryve queue system.
//!
//! This module provides the [`QueueBase`] struct, which serves as the foundation
//! for all queue implementations in the Thryve system. It provides common
//! functionality and fields that are shared among all queue types.
//!
//! # Architecture
//!
//! The [`QueueBase`] struct is designed to be embedded in specific queue
//! implementations, providing:
//! - Task configuration for queue behavior
//! - A main queue for storing items
//! - A lock-free registry for parking and waking idle worker threads
//! - Control flags for pause/resume and exit functionality
//! - A packet handle for communication with the engine
//!
//! # Synchronization model
//!
//! Idle threads register themselves in the [`WorkerRegistry`]'s lock-free
//! bitset, spin briefly, then park via [`std::thread::park`]; wakers scan the
//! bitset and call [`Thread::unpark`](std::thread::Thread::unpark) directly --
//! no mutex or condvar is involved. See [`WorkerRegistry`]'s own docs for the
//! correctness invariants this depends on, in particular why a thread must
//! register *before* its spin/backoff window rather than only right before
//! parking.
//!
//! # Usage
//!
//! Queue implementations embed [`QueueBase`] to inherit its functionality,
//! then implement the [`Queue`](crate::traits::queue::Queue) trait to provide type-specific behavior.

use std::{
    marker::PhantomData,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
};

use crate::threads::thread_registry::WorkerRegistry;
use crate::{packets::packet_handle::PacketHandle, traits::packets::Packet};

/// Base structure for all queue implementations in the Thryve system.
///
/// The `QueueBase` struct provides common functionality and fields that are
/// shared among all queue types. It is designed to be embedded in specific
/// queue implementations to inherit its functionality.
///
/// # Type Parameters
///
/// - `Q`: The underlying queue type (e.g., `SkipMap`, `SegQueue`)
/// - `P`: The packet type stored in the queue
/// - `C`: The queue's configuration: [`TaskConfig`](crate::cfg::config::TaskConfig) for the task
///   queue, `()` for the observer queue, which has none. Each queue has exactly what it uses, so
///   nothing has to be unwrapped.
///
/// # Fields
///
/// - `task_config`: The queue's configuration (see `C`)
/// - `main_queue`: The underlying queue storing items
/// - `worker_registry`: Lock-free registry for parking and waking idle workers
/// - `exit_flag`: Atomic flag indicating whether the queue should terminate
/// - `pause_flag`: Atomic flag indicating whether the queue is paused
/// - `packet_handle`: Handle for communicating with the engine
///
/// # Example
///
/// ```ignore
/// // Queue implementations embed QueueBase to inherit common functionality
/// pub type MyQueue<T> = QueueBase<SomeQueueType, T, ()>;
/// ```
pub struct QueueBase<Q, P, C> {
    /// The queue's configuration: the task settings for the task queue, `()` for the observer
    /// queue.
    pub task_config: Arc<C>,
    /// The underlying queue storing items.
    pub main_queue: Q,
    /// Lock-free registry idle workers park on and wakers scan to unpark one
    /// (or all) of them; sized to exactly the number of threads that call
    /// [`pop`](crate::traits::queue::Queue::pop) on this specific queue instance.
    pub worker_registry: Arc<WorkerRegistry>,
    /// Atomic flag indicating whether the queue should terminate.
    pub exit_flag: Arc<AtomicBool>,
    /// Atomic flag indicating whether the queue is paused.
    pub pause_flag: Arc<AtomicBool>,
    /// Handle for communicating with the engine.
    pub packet_handle: PacketHandle,
    /// Phantom data to maintain ownership of the packet type.
    pub _marker: PhantomData<P>,
}

impl<Q, P: Packet, C> QueueBase<Q, P, C> {
    /// Returns whether the queue is shutting down, after waiting out a pause.
    ///
    /// While the queue is paused, the calling worker parks until the queue is resumed or shut
    /// down. Both queues' `pop` call this first.
    ///
    /// # Arguments
    ///
    /// * `idx` - The calling worker's index in the queue's worker registry
    pub fn is_exit(&self, idx: usize) -> bool {
        while self.is_paused_and_running() {
            self.worker_registry.set_idle(idx);

            // Look again now that this worker is marked idle: a `resume` or `finalize` from
            // before the mark shows up here, and one from after it finds the mark and wakes this
            // worker. Either way, the worker can't sleep through it.
            if self.is_paused_and_running() {
                thread::park();
            }

            self.worker_registry.clear_idle(idx);
        }

        self.exit_flag.load(Ordering::SeqCst)
    }

    /// Whether the queue is paused and not shutting down. `finalize` sets the exit flag but leaves
    /// the pause flag as it is, so a worker waiting out a pause must watch both.
    fn is_paused_and_running(&self) -> bool {
        self.pause_flag.load(Ordering::SeqCst) && !self.exit_flag.load(Ordering::SeqCst)
    }
}
