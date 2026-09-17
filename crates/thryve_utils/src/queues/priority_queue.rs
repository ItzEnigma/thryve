// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Priority queue implementation for task scheduling.
//!
//! This module provides a priority queue implementation using a [`SkipMap`]
//! that orders tasks by their effective priority, which combines base priority
//! with an aging mechanism to prevent starvation of lower-priority tasks.
//!
//! # Priority Calculation
//!
//! The effective priority of a task is calculated as:
//!
//! ```text
//! effective_priority = min(base_priority + aging_boost, max_priority)
//! ```
//!
//! Where `aging_boost` increases based on how long the task has been waiting.
//!
//! # Starvation Prevention
//!
//! Tasks that have aged significantly (reaching 80% of max priority) are
//! prioritized over newer tasks, even if those newer tasks have higher
//! base priority. This ensures fair scheduling over time.
//!
//! # Thread Safety
//!
//! `main_queue` is a lock-free [`SkipMap`], so `push()`'s insert and the fast
//! path of `pop()` never touch a lock at all. When `pop()` finds nothing, it
//! registers on a [`WorkerRegistry`] (see that module's docs for the
//! invariants this depends on), spins briefly via
//! [`Backoff`], then parks -- no mutex or condvar
//! anywhere in the path.

use crossbeam::atomic::AtomicCell;
use crossbeam::utils::Backoff;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use crate::bases::observer::Observer;
use crate::bases::queue::QueueBase;
use crate::cfg::config::{Config, TaskConfig};
use crate::extras::jobs::TaskJob;
use crate::extras::thread_local;
use crate::packets::{notif_log::Log, packet_handle::PacketHandle, task::Task};
use crate::res::Result;
use crate::threads::thread_registry::WorkerRegistry;
use crate::traits::queue::Queue;

/// Type alias for the priority queue child structure.
///
/// Uses a `SkipMap` to maintain task ordering by priority, with
/// `AtomicCell` wrappers enabling lock-free concurrent access.
pub type PQChild<T> = QueueBase<SkipMap<Priority, AtomicCell<Option<Box<EnqueuedTask>>>>, T>;

/// Wrapper for a task that tracks enqueue time for aging.
///
/// This structure wraps a [`Task`] and records when it was added
/// to the queue, enabling the aging mechanism that prevents starvation.
pub struct EnqueuedTask {
    /// The wrapped task.
    pub(crate) task: Task,
    /// Time when the task was enqueued.
    enq_at: Instant,
}

/// Priority calculation for task ordering.
///
/// The `Priority` struct encapsulates all information needed to
/// calculate a task's effective priority, combining base priority
/// with aging to ensure fair scheduling.
///
/// # Ordering
///
/// Tasks are ordered by:
/// 1. Effective priority (base + aging boost)
/// 2. Enqueue time (FIFO for equal priority)
/// 3. Task ID (tiebreaker)
pub struct Priority {
    /// Task configuration for priority calculations.
    pub task_config: Arc<TaskConfig>,
    /// The task's base priority level.
    pub base_priority: u8,
    /// Time when the task was enqueued.
    pub enq_at: Instant,
    /// Unique task identifier for tiebreaking.
    pub task_id: u64,
}

impl Queue<Task> for PQChild<Task> {
    /// Inserts a task into the priority queue.
    ///
    /// Wraps the task in an [`EnqueuedTask`] to track enqueue time,
    /// calculates its priority, and inserts it into the `SkipMap`.
    /// Notifies waiting threads if the queue is not paused.
    ///
    /// # Panics
    ///
    /// Panics if `task_config` is `None`, which should never happen if
    /// the queue was properly initialized with [`PQChild::new`](Self::new).
    ///
    /// # Arguments
    ///
    /// * `task` - The task to insert
    fn push(&self, task: Task) {
        let task_config = self
            .task_config
            .as_ref()
            .expect("PQChild task_config must be Some - queue was not properly initialized");

        // Wrap the task to track enqueue time for aging.
        let mut enq_task = EnqueuedTask {
            task,
            enq_at: Instant::now(),
        };
        enq_task.task.clamp_task_priority(task_config.max_priority);

        // Create priority struct for ordering.
        let priority = Priority {
            task_config: self.task_config.clone().unwrap(),
            base_priority: enq_task.task.base_priority,
            enq_at: enq_task.enq_at,
            task_id: enq_task.task.id,
        };

        // Insert into SkipMap with AtomicCell for lock-free access.
        self.main_queue
            .insert(priority, AtomicCell::new(Some(Box::new(enq_task))));

        // Notify waiting threads if not paused.
        if !self.pause_flag.load(Ordering::SeqCst) {
            self.worker_registry.notify_one();
        }
    }

    /// Removes and returns the highest priority task.
    ///
    /// Blocks until a task is available or the queue is finalized.
    /// Implements aging-based priority boosting to prevent starvation
    /// of lower-priority tasks.
    ///
    /// # Returns
    ///
    /// The highest priority task, or a special "Fin" task when
    /// the queue is being finalized.
    fn pop(&self) -> Task {
        loop {
            // Check for shutdown.
            if self.exit_flag.load(Ordering::SeqCst) {
                break;
            }
            // Wait if paused.
            else if self.pause_flag.load(Ordering::SeqCst) {
                let idx = thread_local::tl_get_worker_park_idx();

                self.worker_registry.set_idle(idx);
                thread::park();
                self.worker_registry.clear_idle(idx);
            }

            // Try to pop a task.
            if let Some(entry) = self.main_queue.pop_front() {
                let mut wrapper = entry.value().take();

                // Atomically extract the task.
                if let Some(moved_task) = wrapper.take() {
                    return moved_task.task;
                }
            }

            let idx = thread_local::tl_get_worker_park_idx();
            self.worker_registry.set_idle(idx);

            // 2. Medium Path: Spin backoff before committing to sleep
            let backoff = Backoff::new();

            while !backoff.is_completed() {
                // Try to pop a task.
                if let Some(entry) = self.main_queue.pop_front() {
                    let mut wrapper = entry.value().take();

                    // Atomically extract the task.
                    if let Some(moved_task) = wrapper.take() {
                        self.worker_registry.clear_idle(idx);
                        return moved_task.task;
                    }
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

        // Return special "Fin" task on shutdown.
        Task::new("Fin", 0, TaskJob::new(Result::fin))
    }

    /// Finalizes the queue for shutdown.
    ///
    /// Clears all pending tasks, sets the exit flag, and notifies
    /// all waiting threads to wake up and terminate.
    ///
    /// # Arguments
    ///
    /// * `_` - Logger (unused for task queue)
    fn finalize(&self, _: Option<Arc<Observer>>) {
        self.main_queue.clear();
        self.exit_flag.store(true, Ordering::SeqCst);
        self.worker_registry.notify_all();

        let notif = Log::log("Task Queue Finalized".to_string(), "Main");
        self.packet_handle.send_notification(notif);
    }

    /// Pauses the queue.
    ///
    /// Sets the pause flag, causing `pop()` to block until resumed.
    fn pause(&self) {
        self.pause_flag.store(true, Ordering::SeqCst);

        let notif = Log::log("Task Queue Paused".to_string(), "Main");
        self.packet_handle.send_notification(notif);
    }

    /// Resumes the queue.
    ///
    /// Clears the pause flag and notifies waiting threads.
    fn resume(&self) {
        self.pause_flag.store(false, Ordering::SeqCst);

        let notif = Log::log("Task Queue continuing".to_string(), "Main");
        self.packet_handle.send_notification(notif);
    }

    fn exit_flag(&self) -> Arc<AtomicBool> {
        self.exit_flag.clone()
    }
}

impl PQChild<Task> {
    /// Creates a new priority queue.
    ///
    /// Initializes a new priority queue with the given configuration
    /// and packet handle. The queue is ready to accept tasks immediately.
    ///
    /// # Arguments
    ///
    /// * `config` - Application configuration
    /// * `packet_handle` - Handle for sending notifications
    ///
    /// # Returns
    ///
    /// A new `PQChild` instance.
    pub fn new(config: &Config, packet_handle: PacketHandle) -> Self {
        Self {
            task_config: Some(Arc::new(config.tasks)),
            main_queue: SkipMap::new(),
            worker_registry: Arc::new(WorkerRegistry::new(config.threads.worker_count)),
            exit_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
            packet_handle,
            _marker: std::marker::PhantomData,
        }
    }
}

impl Priority {
    /// Calculates the effective priority with aging boost.
    ///
    /// The effective priority combines the base priority with a boost
    /// based on how long the task has been waiting. This prevents
    /// starvation of lower-priority tasks.
    ///
    /// # Returns
    ///
    /// The effective priority, clamped to `max_priority`.
    pub fn effective_priority(&self) -> u8 {
        let waited = self.enq_at.elapsed();
        let aging_boost = (waited.as_secs_f64()
            / Duration::from_secs(self.task_config.aging_interval).as_secs_f64())
            as u8;

        (self.base_priority + aging_boost).min(self.task_config.max_priority)
    }
}

impl PartialEq for Priority {
    fn eq(&self, other: &Self) -> bool {
        self.base_priority == other.base_priority
            && self.enq_at == other.enq_at
            && self.task_id == other.task_id
    }
}
impl Eq for Priority {}

impl Ord for Priority {
    /// Orders priorities with starvation prevention.
    ///
    /// Tasks that have aged to 80% of max priority or higher are
    /// prioritized over newer tasks (more like realtime tasks).
    /// There are also checks made that prevent starvation.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let threshold = (self.task_config.max_priority as f32 * 0.8) as u8;

        let self_is_realtime = self.base_priority >= threshold;
        let other_is_realtime = other.base_priority >= threshold;

        // Realtime tasks come first
        if self_is_realtime && !other_is_realtime {
            return std::cmp::Ordering::Less;
        }
        if other_is_realtime && !self_is_realtime {
            return std::cmp::Ordering::Greater;
        }

        // Normal ordering
        other
            .effective_priority()
            .cmp(&self.effective_priority())
            .then_with(|| self.enq_at.cmp(&other.enq_at))
            .then_with(|| self.task_id.cmp(&other.task_id))
    }
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
