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
//! The queue is designed for concurrent access, using atomic operations
//! and condition variables to coordinate between producers and consumers.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Instant, Duration};
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;

use crate::bases::observer::Observer;
use crate::cfg::config::{Config, TaskConfig};
use crate::packets::{notif_log::Log, packet_handle::PacketHandle, task::Task};
use crate::bases::queue::QueueBase;
use crate::traits::queue::Queue;
use crate::res::Result;

/// Type alias for the priority queue child structure.
///
/// Uses a `SkipMap` to maintain task ordering by priority, with
/// `AtomicCell` wrappers enabling lock-free concurrent access.
pub type PQChild<T> = QueueBase<SkipMap<Priority, AtomicCell<Option<EnqueuedTask>>>, T>;

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
    task_config: Arc<TaskConfig>,
    /// The task's base priority level.
    base_priority: u8,
    /// Time when the task was enqueued.
    enq_at: Instant,
    /// Unique task identifier for tiebreaking.
    task_id: u64,
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
        // Wrap the task to track enqueue time for aging.
        let mut enq_task = EnqueuedTask { task, enq_at: Instant::now() };
        enq_task.task.check_task_priority(self.task_config.clone().unwrap().max_priority);

        // Create priority struct for ordering.
        let priority = Priority {
            task_config: self.task_config.clone().unwrap(),
            base_priority: enq_task.task.base_priority,
            enq_at: enq_task.enq_at,
            task_id: enq_task.task.id,
        };

        // Insert into SkipMap with AtomicCell for lock-free access.
        self.main_queue.insert(priority, AtomicCell::new(Some(enq_task)));

        // Notify waiting threads if not paused.
        if self.pause_flag.load(Ordering::SeqCst) == false {
            self.condition_var.notify_one();
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
    /// The highest priority task, or a special "fin" task when
    /// the queue is being finalized.
    fn pop(&self) -> Task {
        loop {
            // Check for shutdown.
            if self.exit_flag.load(Ordering::SeqCst) == true {
                break;
            }

            // Wait if paused.
            else if self.pause_flag.load(Ordering::SeqCst) == true {
                let _unused = self.condition_var.wait(self.gate.lock().unwrap()).unwrap();
            }

            // Try to pop a task.
            if let Some(entry) = self.main_queue.pop_front() {
                let mut wrapper = entry.value().take();

                // Atomically extract the task.
                if let Some(moved_task) = wrapper.take() {
                    return moved_task.task;
                }
            }

            // Wait for notification.
            let _unused = self.condition_var.wait(self.gate.lock().unwrap()).unwrap();
        }

        // Return special "fin" task on shutdown.
        Task::new("fin", 0, Box::new(|| Result::new("fin", "".to_string(), "Main")))
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
        self.condition_var.notify_all();

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
            gate: Mutex::new(()),
            condition_var: Condvar::new(),
            exit_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
            packet_handle: packet_handle,
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
    pub(crate) fn effective_priority(&self) -> u8 {
        let waited = self.enq_at.elapsed();
        let aging_boost = (waited.as_secs_f64() / Duration::from_secs(self.task_config.aging_interval).as_secs_f64()) as u8;

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
    /// prioritized over newer tasks, preventing starvation.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Starvation prevention: aged tasks jump ahead.
        if other.base_priority >= (self.task_config.max_priority as f32 * 0.8) as u8 {
            return std::cmp::Ordering::Greater;
        }
        other.effective_priority()
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
