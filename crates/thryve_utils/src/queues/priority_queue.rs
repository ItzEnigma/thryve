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
//! Where `aging_boost` grows by one for every `aging_interval` seconds the task has been waiting.
//!
//! # Realtime and Starvation Prevention
//!
//! A task whose base priority is at least 80% of `max_priority` is realtime, and realtime tasks
//! always come before the others. Among the rest, ageing raises a waiting task's effective
//! priority, so a low-priority task that has waited long enough comes before newer tasks with a
//! higher base priority. Ageing never makes a task realtime.
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

use crate::bases::notif::Notif;
use crate::bases::queue::QueueBase;
use crate::cfg::config::{Config, TaskConfig};
use crate::extras::thread_local;
use crate::packets::{notif_log::Log, packet_handle::PacketHandle, task::Task};
use crate::threads::thread_registry::WorkerRegistry;
use crate::traits::queue::Queue;

/// Type alias for the priority queue child structure.
///
/// Uses a `SkipMap` to maintain task ordering by priority, with
/// `AtomicCell` wrappers enabling lock-free concurrent access.
pub type PQChild<T> =
    QueueBase<SkipMap<Priority, AtomicCell<Option<Box<EnqueuedTask>>>>, T, TaskConfig>;

/// The lowest base priority that counts as realtime: 80% of `max_priority`, rounded down.
///
/// Realtime tasks always come before the others, and ageing never makes a task realtime (see
/// [`Priority`]'s ordering). With a `max_priority` of 0 or 1 the threshold is 0, so every level is
/// realtime.
pub(crate) fn realtime_threshold(max_priority: u8) -> u8 {
    (max_priority as f32 * 0.8) as u8
}

/// The highest priority a follow-up can inherit: the level just below realtime.
///
/// A follow-up that inherits its dependency's priority must not become realtime that way: a
/// realtime chain would keep every other task waiting, because ageing never lifts a task past
/// realtime ones. `None` when every level is realtime (a `max_priority` of 0 or 1), so there's
/// nothing below realtime to cap to.
pub(crate) fn inherited_priority_cap(max_priority: u8) -> Option<u8> {
    realtime_threshold(max_priority).checked_sub(1)
}

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
/// 1. Realtime first (base priority at least 80% of `max_priority`)
/// 2. Effective priority (base + aging boost)
/// 3. Enqueue time (FIFO for equal priority)
/// 4. Task ID (tiebreaker)
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
    /// # Arguments
    ///
    /// * `task` - The task to insert
    fn push(&self, task: Task) {
        let task_config = self.task_config.clone();

        // Wrap the task to track enqueue time for aging.
        let mut enq_task = EnqueuedTask {
            task,
            enq_at: Instant::now(),
        };
        enq_task.task.clamp_task_priority(task_config.max_priority);

        // Create priority struct for ordering.
        let priority = Priority {
            task_config,
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
    /// Waits until a task is available or the queue is finalized.
    /// Implements aging-based priority boosting to prevent starvation
    /// of lower-priority tasks.
    ///
    /// # Returns
    ///
    /// The highest priority task, or `None` once the queue is finalized.
    fn pop(&self) -> Option<Task> {
        let idx = thread_local::tl_get_worker_park_idx();

        loop {
            if self.is_exit(idx) {
                break;
            }

            // Fast path: take the first task, if there is one.
            if let Some(entry) = self.main_queue.pop_front() {
                let mut wrapper = entry.value().take();

                // Atomically extract the task.
                if let Some(moved_task) = wrapper.take() {
                    return Some(moved_task.task);
                }
            }

            // Registered as idle before it looks again, so a push from now on wakes it.
            self.worker_registry.set_idle(idx);

            // Medium path: spin with backoff before committing to sleep.
            let backoff = Backoff::new();

            while !backoff.is_completed() {
                // Try to pop a task.
                if let Some(entry) = self.main_queue.pop_front() {
                    let mut wrapper = entry.value().take();

                    // Atomically extract the task.
                    if let Some(moved_task) = wrapper.take() {
                        self.worker_registry.clear_idle(idx);
                        return Some(moved_task.task);
                    }
                }
                // snooze(), not spin(): only snooze() advances Backoff past
                // its spin-only phase, so is_completed() can ever become true
                // and this loop can fall through to a real park() below.
                backoff.snooze();
            }

            // Slow path: unless the queue is shutting down, park until a push, a resume or the
            // shutdown wakes this worker, then look again.
            if !self.exit_flag.load(Ordering::SeqCst) {
                thread::park();
                self.worker_registry.clear_idle(idx);
            }
        }

        None
    }

    /// Finalizes the queue for shutdown.
    ///
    /// Sets the exit flag and wakes every waiting worker, so a blocked `pop` returns `None`.
    /// Then it moves every task still queued into `tasks`, highest priority first; those tasks
    /// never run. The "Task Queue Finalized" line goes into `status_reps`.
    ///
    /// # Arguments
    ///
    /// * `tasks` - Receives the tasks that were still queued
    /// * `status_reps` - Receives the "Task Queue Finalized" line
    ///
    /// # Returns
    ///
    /// How many tasks were still queued.
    fn finalize(&self, tasks: &mut Vec<Task>, status_reps: &mut Vec<Notif>) -> usize {
        // The flag first, so the workers stop taking tasks (one already past its check may still
        // take one) while the queue is emptied below.
        self.exit_flag.store(true, Ordering::SeqCst);
        self.worker_registry.notify_all();

        let mut left_over = 0;
        while !self.main_queue.is_empty() {
            if let Some(entry) = self.main_queue.pop_front() {
                let mut wrapper = entry.value().take();

                if let Some(enq_task) = wrapper.take() {
                    tasks.push(enq_task.task);
                    left_over += 1;
                }
            }
        }

        let notif = Log::log("Task Queue Finalized".to_string(), "Main");
        status_reps.push(notif);

        left_over
    }

    /// Pauses the queue.
    ///
    /// Sets the pause flag, causing `pop()` to block until resumed. Except for the startup pause
    /// (`is_start`), sends "Task Queue Paused".
    fn pause(&self, is_start: bool) {
        self.pause_flag.store(true, Ordering::SeqCst);

        if !is_start {
            let notif = Log::log("Task Queue Paused".to_string(), "Main");
            self.packet_handle.send_status_log(notif)
        }
    }

    /// Resumes the queue.
    ///
    /// Clears the pause flag and wakes the parked workers. Except when the startup pause ends
    /// (`is_start`), sends "Task Queue continuing".
    fn resume(&self, is_start: bool) {
        self.pause_flag.store(false, Ordering::SeqCst);

        if !is_start {
            let notif = Log::log("Task Queue continuing".to_string(), "Main");
            self.packet_handle.send_status_log(notif);
        }

        self.worker_registry.notify_all();
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
    /// * `packet_handle` - Handle for sending the queue's status lines ("Task Queue Paused", and
    ///   so on)
    ///
    /// # Returns
    ///
    /// A new `PQChild` instance.
    pub fn new(config: &Config, packet_handle: PacketHandle) -> Self {
        Self {
            task_config: Arc::new(config.tasks),
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
    /// The effective priority, clamped to `max_priority`. However long a task waits, the sum
    /// can't overflow: it stops at the top instead of wrapping around to a low priority.
    pub fn effective_priority(&self) -> u8 {
        let waited = self.enq_at.elapsed();
        let aging_boost = (waited.as_secs_f64()
            / Duration::from_secs(self.task_config.aging_interval).as_secs_f64())
            as u8;

        self.base_priority
            .saturating_add(aging_boost)
            .min(self.task_config.max_priority)
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
    /// Orders priorities so that the task to run first is the smallest.
    ///
    /// A realtime task (base priority at least 80% of `max_priority`) comes before any other,
    /// whatever their ages. Otherwise the higher effective priority (base plus ageing, which is
    /// what prevents starvation) comes first, then the task enqueued first, then the lower id.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let threshold = realtime_threshold(self.task_config.max_priority);

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
