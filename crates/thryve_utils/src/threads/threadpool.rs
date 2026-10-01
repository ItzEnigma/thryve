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
//! 1. Plugin thread pool: Processes tasks using [`PQChild`](crate::queues::priority_queue::PQChild)
//! 2. Observer thread pool: Processes notifications using [`QChild`](crate::queues::seg_queue::QChild)

use crossbeam::channel::Sender;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};

use crate::bases::observer::Observer;
use crate::errors::StartupError;
use crate::extras::thread_local::{self, tl_set_current_task_fingerprint};
use crate::packets::{notif_log::Log, packet_handle::PacketHandle};
use crate::res::TaskResult;
use crate::traits::{packets::Packet, queue::Queue};

/// Thread pool for processing tasks and notifications.
///
/// The `ThreadPool` struct manages a pool of worker threads that
/// process items from a queue. It is generic over both the queue
/// type and packet type, enabling a single implementation to
/// handle both plugin tasks and observer notifications.
///
/// # Type Parameters
///
/// - `Q`: The queue type (e.g., [`PQChild`](crate::queues::priority_queue::PQChild) or [`QChild`](crate::queues::seg_queue::QChild))
/// - `P`: The packet type (e.g., [`Task`](crate::packets::task::Task) or [`Notif`](crate::bases::notif::Notif))
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
    /// Collection of worker thread handles for lifecycle management.
    pub workers: Vec<JoinHandle<()>>,
    /// Flag for graceful shutdown.
    pub pool_exit_flag: Arc<AtomicBool>,
    /// Flag for pause/resume.
    pub pool_pause_flag: Arc<AtomicBool>,
    /// The queue being processed.
    pub main_queue: Arc<Q>,
    /// Handle for sending notifications.
    packet_handle: PacketHandle,
    /// Phantom data for the packet type.
    _marker: PhantomData<P>,
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
    /// back through the result channel. Threads exit when the
    /// pool exit flag is set.
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

            let thread_name = format!("plugin-worker-{}", i);
            let handle = thread::Builder::new()
                .name(thread_name.clone())
                .spawn(move || {
                    thread_local::tl_set_worker_park_idx(i);
                    loop {
                        if exit_flag.load(Ordering::SeqCst) {
                            let notif = Log::log("Plugin Thread Exiting...".to_string(), "Main");
                            tx.send_status(notif);
                            break;
                        }

                        let obj = queue.pop();

                        // pop() can wake here because the queue was finalized
                        // while this thread was blocked inside it, in which
                        // case it returns a sentinel value rather than a real
                        // task -- recheck before treating it as real work.
                        if exit_flag.load(Ordering::SeqCst) {
                            let notif = Log::log("Plugin Thread Exiting...".to_string(), "Main");
                            tx.send_status(notif);
                            break;
                        }

                        tl_set_current_task_fingerprint(
                            obj.get_callsite().this_callsite_fingerprint,
                        );

                        if let Some(res_container) = obj.process() {
                            // Non-fatal: if result send fails, log it and continue
                            if let Err(e) = res_tx.send(res_container) {
                                let notif =
                                    Log::log(format!("Failed to send result: {}", e), "Error");
                                tx.send_status(notif);
                            }
                        }
                    }
                })
                .map_err(|e| StartupError::ThreadInit {
                    thread_name,
                    source: e,
                })?;

            self.workers.push(handle);
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

        self.workers.push(handle);

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

    /// Adds a worker thread handle to the internal collection.
    ///
    /// Stores the provided `JoinHandle` in the workers vector for
    /// later lifecycle management (e.g., joining during shutdown).
    ///
    /// # Arguments
    ///
    /// * `thread_handle` - The join handle of the spawned thread
    ///
    /// # Example
    ///
    /// ```ignore
    /// let handle = std::thread::spawn(|| { /* work */ });
    /// pool.add_worker(handle);
    /// ```
    pub fn add_worker(&mut self, thread_handle: JoinHandle<()>) {
        self.workers.push(thread_handle);
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

    /// Finalizes the plugin thread pool.
    ///
    /// Sets the exit flag, finalizes the main queue (which wakes any parked worker), joins every
    /// worker thread, and sends a notification indicating completion. This should be called
    /// during engine shutdown. A pool whose workers were never started has nothing to join.
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.finalize_plg();
    /// // Worker threads will exit gracefully
    /// ```
    pub fn finalize_plg(&mut self) {
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize(None);

        // A worker that panicked was already reported by the panic hook. Its join error is
        // dropped, so that the other workers are still joined and the shutdown goes on.
        for handle in self.workers.drain(..) {
            let _ = handle.join();
        }

        let notif = Log::log("ThreadPool Finalized".to_string(), "Main");
        self.packet_handle.send_status(notif)
    }

    /// Finalizes the observer thread pool.
    ///
    /// Sets the exit flag (which tells the notification sorter to stop), finalizes the main
    /// queue (which wakes the sorter), and sends a notification to the logger. This should be
    /// called during engine shutdown.
    ///
    /// It doesn't join any thread. The observers are told to stop only later, by their
    /// `clean_up`, so a join here would wait forever. The observer manager joins the sorter
    /// right after this call, and the engine joins the observer threads at the very end.
    ///
    /// # Arguments
    ///
    /// * `logger` - The logger observer for the final notifications, or `None` if it was never
    ///   loaded; the status lines are skipped then
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.finalize_obs(Some(logger.clone()));
    /// // The sorter will exit gracefully
    /// ```
    pub fn finalize_obs(&mut self, logger: Option<Arc<Observer>>) {
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize(logger.clone());

        if let Some(logger) = &logger {
            let notif = Log::log("ThreadPool Finalized".to_string(), "Main");
            logger.obs_queue.push(notif);
        }
    }

    /// Pauses the thread pool.
    ///
    /// Sets the pause flag and pauses the main queue, causing
    /// worker threads to block until resumed.
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.pause();
    /// // Worker threads block
    /// pool.resume();
    /// // Worker threads continue
    /// ```
    pub fn pause(&mut self) {
        self.pool_pause_flag.store(true, Ordering::SeqCst);
        self.main_queue.pause()
    }

    /// Resumes the thread pool.
    ///
    /// Clears the pause flag and resumes the main queue,
    /// unblocking worker threads.
    ///
    /// # Example
    ///
    /// ```ignore
    /// pool.resume();
    /// // Worker threads continue
    /// ```
    pub fn resume(&mut self) {
        self.pool_pause_flag.store(false, Ordering::SeqCst);
        self.main_queue.resume()
    }
}
