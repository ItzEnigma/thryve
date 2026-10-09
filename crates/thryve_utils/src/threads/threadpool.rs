// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Thread pool for task and notification processing.
//!
//! This module provides the [`ThreadPool`] struct, which manages a pool
//! of worker threads that process items from a queue. The thread pool
//! is generic over both the queue type and the packet type, enabling
//! a single implementation to handle both plugin tasks and observer
//! notifications.
//!
//! # Architecture
//!
//! The thread pool uses std::thread for persistent worker threads and supports:
//! - Configurable number of worker threads
//! - Pause/resume functionality
//! - Graceful shutdown via exit flags
//! - Result channel for task completion reporting
//!
//! # Usage
//!
//! Two main thread pools exist in the engine:
//! 1. Plugin thread pool: Processes tasks using [`PQChild`]
//! 2. Observer thread pool: Processes notifications using [`QChild`]

use crossbeam::atomic::AtomicCell;
use crossbeam::channel::Sender;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use crate::bases::notif::Notif;
use crate::bases::observer::Observer;
use crate::errors::StartupError;
use crate::extras::thread_local::{self, tl_set_current_task_fingerprint};
use crate::packets::task::Task;
use crate::packets::{notif_log::Log, packet_handle::PacketHandle};
use crate::queues::priority_queue::PQChild;
use crate::queues::seg_queue::QChild;
use crate::res::TaskResult;
use crate::traits::{packets::Packet, queue::Queue};

/// A thread's slot for the name of what it's working on, empty when it's idle.
///
/// A plugin worker swaps a task's name in before it runs the task and takes it back out after;
/// the plugins' clean-up thread does the same with each plugin's type. When the shutdown's grace
/// period runs out, the engine takes the name out of every thread that hasn't finished, so the
/// log can say what it was stuck on.
///
/// It's lock-free: whoever takes the name out owns it, so no one ever reads a name that someone
/// else could be freeing. `Option<Box<String>>` is one pointer wide, which `AtomicCell` moves with
/// a single atomic instruction. A wider value, such as `Option<Box<str>>`, would make it fall back
/// to an internal lock.
pub type TaskSlot = Arc<AtomicCell<Option<Box<String>>>>;

/// Thread pool for processing tasks and notifications.
///
/// The `ThreadPool` struct manages a pool of worker threads that
/// process items from a queue. It is generic over both the queue
/// type and packet type, enabling a single implementation to
/// handle both plugin tasks and observer notifications.
///
/// # Type Parameters
///
/// - `Q`: The queue type (e.g., [`PQChild`] or [`QChild`])
/// - `P`: The packet type (e.g., [`Task`] or [`Notif`])
///
/// # Fields
///
/// - `num_workers`: Number of worker threads to spawn
/// - `pool_exit_flag`: Flag for graceful shutdown
/// - `pool_pause_flag`: Flag for pause/resume
/// - `main_queue`: The queue being processed
/// - `packet_handle`: Handle for sending notifications
///
/// The plugin pool's workers need a sender for task results and the observer pool's don't, so
/// the pool doesn't store one: [`run_threads_plugins`](Self::run_threads_plugins) takes it.
///
/// # Example
///
/// ```ignore
/// // Thread pools are typically created by the engine
/// let mut pool = ThreadPool::new(worker_count, queue, packet_handle);
/// pool.run_threads_plugins(result_sender)?; // For plugin tasks
/// // or
/// pool.run_thread_obs(observer)?; // For observers
/// ```
pub struct ThreadPool<Q, P> {
    /// Number of worker threads to spawn.
    pub num_workers: usize,
    /// The worker threads, each with the slot naming the task it's running (see [`TaskSlot`]).
    pub workers: Vec<(JoinHandle<()>, TaskSlot)>,

    /// Flag for graceful shutdown.
    pub pool_exit_flag: Arc<AtomicBool>,
    /// Flag for pause/resume.
    pub pool_pause_flag: Arc<AtomicBool>,
    /// The queue being processed.
    pub main_queue: Arc<Q>,
    /// Handle for sending tasks and notifications.
    pub packet_handle: PacketHandle,
    /// Phantom data for the packet type.
    _marker: PhantomData<fn() -> P>,
}

impl<Q, P: Packet> ThreadPool<Q, P>
where
    Q: Queue<P>,
{
    /// Creates a new thread pool.
    ///
    /// Initializes a new thread pool with the specified number of
    /// worker threads, queue and packet handle. If `num_workers` is 0,
    /// no thread pool is created.
    ///
    /// # Arguments
    ///
    /// * `num_workers` - Number of worker threads (0 disables the pool)
    /// * `queue` - The queue to process
    /// * `packet_handle` - Handle for sending notifications
    ///
    /// # Returns
    ///
    /// A new `ThreadPool` instance.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Thread pools are typically created by the engine
    /// let pool = ThreadPool::new(4, queue, packet_handle);
    /// ```
    pub fn new(num_workers: usize, queue: Q, packet_handle: PacketHandle) -> Self {
        // Adopt the queue's own shutdown flag rather than allocating a
        // separate one -- see Queue::exit_flag's doc comment for why two
        // independent flags here is a shutdown race waiting to happen.
        let pool_exit_flag = queue.exit_flag();

        Self {
            num_workers,
            workers: Vec::new(),
            pool_exit_flag,
            pool_pause_flag: Arc::new(AtomicBool::new(false)),
            main_queue: Arc::new(queue),
            packet_handle,
            _marker: PhantomData,
        }
    }

    /// Submits a packet to the thread pool.
    ///
    /// Pushes the given packet into the main queue for processing
    /// by worker threads.
    ///
    /// # Arguments
    ///
    /// * `obj` - The packet to submit
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.submit_task(my_task);
    /// ```
    pub fn submit_task(&self, obj: P) {
        self.main_queue.push(obj);
    }

    /// Starts worker threads for processing plugin tasks.
    ///
    /// Spawns worker threads that continuously pop tasks from the
    /// main queue and process them. Each task's result is sent
    /// back through the result channel. A worker stops once the pool exit flag is set, after
    /// finishing the task it's running, and sends a "Plugin Thread Exiting..." status line.
    ///
    /// # Arguments
    ///
    /// * `res_tx` - Sender for the task results, which the engine reads. Every worker gets a
    ///   clone.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError::ThreadInit`] if a worker thread can't be started. Workers
    /// started before the failing one keep running.
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.run_threads_plugins(result_sender)?;
    /// // Worker threads now processing tasks
    /// ```
    pub fn run_threads_plugins(
        &mut self,
        res_tx: Arc<Sender<(u64, TaskResult)>>,
    ) -> Result<(), StartupError> {
        for i in 0..self.num_workers {
            let queue = Arc::clone(&self.main_queue);
            let exit_flag = Arc::clone(&self.pool_exit_flag);
            let tx = self.packet_handle.clone();
            let res_tx = res_tx.clone();
            let task_slot = Arc::new(AtomicCell::new(None));
            let task_slot_clone = task_slot.clone();

            let thread_name = format!("plugin-worker-{}", i);
            let handle = thread::Builder::new()
                .name(thread_name.clone())
                .spawn(move || {
                    thread_local::tl_set_worker_park_idx(i);
                    loop {
                        // The pool's exit flag is the queue's own; the shutdown sets it.
                        if exit_flag.load(Ordering::SeqCst) {
                            break;
                        }

                        // Waits for a task. `None` once the queue is finalized, even with tasks
                        // still in it: the engine takes those back, and they never run.
                        let obj_opt = queue.pop();

                        if let Some(obj) = obj_opt {
                            tl_set_current_task_fingerprint(
                                obj.get_callsite().this_callsite_fingerprint,
                            );

                            // The slot holds the task's name while it runs, so if the shutdown
                            // gives up waiting for this worker, `Error.log` can name the task.
                            if let Some(task_name) = obj.get_task_name() {
                                task_slot_clone.swap(Some(Box::new(task_name)));
                            }

                            if let Some(res_container) = obj.process() {
                                let _ = task_slot_clone.take();
                                // Waits while the result mailbox is full. At shutdown the engine
                                // keeps reading it until this worker has finished.
                                // Non-fatal: if result send fails, log it and continue
                                if let Err(e) = res_tx.send(res_container) {
                                    let notif =
                                        Log::log(format!("Failed to send result: {}", e), "Error");
                                    tx.send_status_log(notif);
                                }
                            }
                        } else {
                            break;
                        }
                    }

                    // The engine reads the status mailbox until the worker has finished, and once
                    // more after joining it, so this line isn't lost.
                    let notif = Log::log("Plugin Thread Exiting...".to_string(), "Main");
                    tx.send_status_log(notif);
                })
                .map_err(|e| StartupError::ThreadInit {
                    thread_name,
                    source: e,
                })?;

            self.workers.push((handle, task_slot));
        }

        Ok(())
    }

    /// Starts a worker thread for processing an observer.
    ///
    /// Spawns a single thread that runs the given observer's
    /// initialization function. This function typically starts
    /// the observer's main notification processing loop.
    ///
    /// # Arguments
    ///
    /// * `obs` - The observer to run
    ///
    /// # Errors
    ///
    /// Returns [`StartupError::ThreadInit`] if the observer's thread can't be started.
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.run_thread_obs(logger_observer)?;
    /// // Observer thread now processing notifications
    /// ```
    pub fn run_thread_obs(&mut self, obs: Arc<Observer>) -> Result<(), StartupError> {
        let arc_obs = Arc::clone(&obs);

        let thread_name = obs.obs_type().to_string();

        let handle = thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                arc_obs.run_loop(arc_obs.clone());
            })
            .map_err(|e| StartupError::ThreadInit {
                thread_name,
                source: e,
            })?;

        self.workers.push((handle, Arc::new(AtomicCell::new(None))));

        Ok(())
    }

    /// Returns the number of worker threads.
    ///
    /// # Returns
    ///
    /// The number of worker threads configured for this pool.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let worker_count = pool.num_workers();
    /// println!("Pool has {} workers", worker_count);
    /// ```
    pub fn num_workers(&self) -> usize {
        self.num_workers
    }

    /// Returns a clone of the pool exit flag.
    ///
    /// # Returns
    ///
    /// An `Arc<AtomicBool>` indicating whether the pool should exit.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let exit_flag = pool.pool_exit_flag();
    /// if exit_flag.load(Ordering::SeqCst) {
    ///     // Pool is shutting down
    /// }
    /// ```
    pub fn pool_exit_flag(&self) -> Arc<AtomicBool> {
        self.pool_exit_flag.clone()
    }

    /// Pauses the thread pool.
    ///
    /// Sets the pause flag and pauses the main queue, causing
    /// worker threads to block until resumed.
    ///
    /// # Arguments
    ///
    /// * `is_start` - `true` only for the engine's startup pause, which the queue doesn't log
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.pause(false);
    /// // Worker threads block
    /// pool.resume(false);
    /// // Worker threads continue
    /// ```
    pub fn pause(&mut self, is_start: bool) {
        self.pool_pause_flag.store(true, Ordering::SeqCst);
        self.main_queue.pause(is_start)
    }

    /// Resumes the thread pool.
    ///
    /// Clears the pause flag and resumes the main queue,
    /// unblocking worker threads.
    ///
    /// # Arguments
    ///
    /// * `is_start` - `true` only when the engine ends its startup pause, which the queue doesn't
    ///   log
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.resume(false);
    /// // Worker threads continue
    /// ```
    pub fn resume(&mut self, is_start: bool) {
        self.pool_pause_flag.store(false, Ordering::SeqCst);
        self.main_queue.resume(is_start)
    }

    /// Joins every worker thread and adds the pool's "ThreadPool Finalized" line.
    ///
    /// A join waits for its thread to finish, so call this once the workers have stopped. The
    /// engine waits for its plugin workers itself, emptying its mailboxes meanwhile (a worker can
    /// be blocked on a full one): it takes their handles out of `workers` first, so for it this
    /// only adds the line. A pool whose workers never started has nothing to join either.
    ///
    /// # Arguments
    ///
    /// * `status_reps` - Receives the "ThreadPool Finalized" line
    pub fn stop_workers(&mut self, status_reps: &mut Vec<Notif>) {
        // A worker that panicked was already reported by the panic hook. Its join error is
        // dropped, so that the other workers are still joined and the shutdown goes on.
        for (handle, _) in self.workers.drain(..) {
            let _ = handle.join();
        }

        let notif = Log::log("ThreadPool Finalized".to_string(), "Main");

        status_reps.push(notif);
    }
}

impl ThreadPool<PQChild<Task>, Task> {
    /// Stops the plugin thread pool.
    ///
    /// Sets the exit flag and finalizes the task queue, which wakes every parked worker and hands
    /// back the tasks that were still queued; those never run. A worker that's running a task
    /// finishes it, then stops. Nothing is joined here: the engine waits for the workers while it
    /// empties its mailboxes, then joins them.
    ///
    /// # Arguments
    ///
    /// * `tasks` - Receives the tasks that were still queued
    /// * `status_reps` - Receives the queue's "Task Queue Finalized" line
    ///
    /// # Returns
    ///
    /// How many tasks were still queued.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut tasks = Vec::new();
    /// let mut status_reps = Vec::new();
    /// let never_ran = pool.finalize_plg(&mut tasks, &mut status_reps);
    /// ```
    pub fn finalize_plg(&mut self, tasks: &mut Vec<Task>, status_reps: &mut Vec<Notif>) -> usize {
        // The pool's exit flag is the queue's own: a worker checks it before taking a task, and
        // finalizing the queue wakes the waiting workers so they see it.
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize(tasks, status_reps)
    }
}

impl ThreadPool<QChild, Notif> {
    /// Finalizes the observer thread pool's queue, the one the notification sorter reads.
    ///
    /// Call it after the sorter has stopped. The observer manager sets the pool's exit flag,
    /// wakes the sorter and joins it, and the sorter routes everything it still holds before it
    /// stops. So the queue is normally empty here, and anything this hands back was never
    /// routed.
    ///
    /// It doesn't join the observer threads: the observers are told to stop later, by their
    /// cleanup. The engine waits for their threads after that, and joins the logger's last.
    ///
    /// # Arguments
    ///
    /// * `notifs` - Receives any notifications still queued
    /// * `status_reps` - Receives the queue's "Obs Queue Finalized" line
    ///
    /// # Returns
    ///
    /// How many notifications were still queued.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let never_routed = pool.finalize_obs(&mut notifs, &mut status_reps);
    /// ```
    pub fn finalize_obs(&mut self, notifs: &mut Vec<Notif>, status_reps: &mut Vec<Notif>) -> usize {
        self.main_queue.finalize(notifs, status_reps)
    }
}
