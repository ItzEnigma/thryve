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

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::marker::PhantomData;
use std::thread::{self, JoinHandle};
use crossbeam::channel::Sender;

use crate::bases::observer::Observer;
use crate::packets::{notif_log::Log, packet_handle::PacketHandle};
use crate::traits::{packets::Packet, queue::Queue};
use crate::res;

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
/// - `res_tx`: Optional result sender for task completion
/// - `packet_handle`: Handle for sending notifications
///
/// # Example
///
/// ```no_run
/// // Thread pools are typically created by the engine
/// let pool = ThreadPool::new(worker_count, queue, packet_handle, result_sender);
/// pool.run_threads_plugins(); // For plugin tasks
/// // or
/// pool.run_thread_obs(observer); // For observers
/// ```
pub struct ThreadPool<Q, P> {
    /// Number of worker threads to spawn.
    num_workers: usize,
    /// Collection of worker thread handles for lifecycle management.
    workers: Vec<JoinHandle<()>>,
    /// Flag for graceful shutdown.
    pool_exit_flag: Arc<AtomicBool>,
    /// Flag for pause/resume.
    pool_pause_flag: Arc<AtomicBool>,
    /// The queue being processed.
    pub main_queue: Arc<Q>,
    /// Optional result sender for task completion.
    res_tx: Option<Arc<Sender<(u64, res::Result)>>>,
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
    /// worker threads, queue, packet handle, and optional result
    /// sender. If `num_workers` is 0, no thread pool is created.
    ///
    /// # Arguments
    ///
    /// * `num_workers` - Number of worker threads (0 disables the pool)
    /// * `queue` - The queue to process
    /// * `packet_handle` - Handle for sending notifications
    /// * `res_tx` - Optional result sender for task completion
    ///
    /// # Returns
    ///
    /// A new `ThreadPool` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// // Thread pools are typically created by the engine
    /// let pool = ThreadPool::new(4, queue, packet_handle, result_sender);
    /// ```
    pub fn new(
        num_workers: usize,
        queue: Q,
        packet_handle: PacketHandle,
        res_tx: Option<Arc<Sender<(u64, res::Result)>>>,
    ) -> Self {
        Self {
            num_workers,
            workers: Vec::new(),
            pool_exit_flag: Arc::new(AtomicBool::new(false)),
            pool_pause_flag: Arc::new(AtomicBool::new(false)),
            main_queue: Arc::new(queue),
            res_tx: res_tx,
            packet_handle: packet_handle,
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
    /// ```no_run
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
    /// # Example
    ///
    /// ```no_run
    /// pool.run_threads_plugins();
    /// // Worker threads now processing tasks
    /// ```
    pub fn run_threads_plugins(&mut self) {        
        for i in 0..self.num_workers {
            let queue = Arc::clone(&self.main_queue);
            let res_tx = self.res_tx.clone().expect("Result Sender Has to be Present!");
            let exit_flag = Arc::clone(&self.pool_exit_flag);
            let tx = self.packet_handle.clone();

            let thread_name = format!("plugin-worker-{}", i);
            let handle = thread::Builder::new()
                .name(thread_name)
                .spawn(move || {
                    loop {
                        if exit_flag.load(Ordering::SeqCst) == true {
                            let notif = Log::log("Plugin Thread Exiting...".to_string(), "Main");
                            tx.send_notification(notif);
                            break;
                        }

                        let obj = queue.pop();

                        if let Some(res_container) = obj.process().take() {
                            let _ = res_tx.send(res_container);
                        }
                    }
                })
                .unwrap_or_else(|e| panic!("Failed to spawn plugin worker thread: {}" , e));

            self.workers.push(handle);
        }
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
    /// # Example
    ///
    /// ```no_run
    /// pool.run_thread_obs(logger_observer);
    /// // Observer thread now processing notifications
    /// ```
    pub fn run_thread_obs(&mut self, obs: Arc<Observer>) {
        let arc_obs = Arc::clone(&obs);

        let handle = thread::Builder::new()
            .name(format!("{}" , obs.obs_type))
            .spawn(move || {
                (arc_obs.fn_to_run)(arc_obs.clone());
            })
            .unwrap_or_else(|e| panic!("Failed to spawn observer worker thread: {}", e));

        self.workers.push(handle);
    }

    /// Returns the number of worker threads.
    ///
    /// # Returns
    ///
    /// The number of worker threads configured for this pool.
    ///
    /// # Example
    ///
    /// ```no_run
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
    /// ```no_run
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
    /// ```no_run
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
    /// Sets the exit flag, finalizes the main queue, and sends
    /// a notification indicating completion. This should be called
    /// during engine shutdown.
    ///
    /// # Example
    ///
    /// ```no_run
    /// pool.finalize_plg();
    /// // Worker threads will exit gracefully
    /// ```
    pub fn finalize_plg(&mut self) {
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize(None);

        let notif = Log::log("ThreadPool Finalized".to_string(), "Main");
        self.packet_handle.send_notification(notif);
    }

    /// Finalizes the observer thread pool.
    ///
    /// Sets the exit flag, finalizes the main queue with the
    /// logger, and sends a notification. This should be called
    /// during engine shutdown.
    ///
    /// # Arguments
    ///
    /// * `logger` - The logger observer for draining notifications
    ///
    /// # Example
    ///
    /// ```no_run
    /// pool.finalize_obs(logger.clone());
    /// // Observer threads will exit gracefully
    /// ```
    pub fn finalize_obs(&mut self, logger: Arc<Observer>) {
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize(Some(logger.clone()));

        let notif = Log::log("ThreadPool Finalized".to_string(), "Main");
        logger.obs_queue.push(notif);
    }

    /// Pauses the thread pool.
    ///
    /// Sets the pause flag and pauses the main queue, causing
    /// worker threads to block until resumed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// pool.pause();
    /// // Worker threads block
    /// pool.resume();
    /// // Worker threads continue
    /// ```
    pub fn pause(&mut self) {
        self.pool_pause_flag.store(true, Ordering::SeqCst);
        self.main_queue.pause();
    }

    /// Resumes the thread pool.
    ///
    /// Clears the pause flag and resumes the main queue,
    /// unblocking worker threads.
    ///
    /// # Example
    ///
    /// ```no_run
    /// pool.resume();
    /// // Worker threads continue
    /// ```
    pub fn resume(&mut self) {
        self.pool_pause_flag.store(false, Ordering::SeqCst);
        self.main_queue.resume();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg;
use crate::packets::packet_handle::PacketHandle;
    use crate::packets::task::Task;
    use crate::bases::notif::Notif;
    use crate::bases::observer::Observer;
    use crate::queues::priority_queue::PQChild;
use crate::traits::observer_logic::ObsLogic;
    use crate::queues::seg_queue::QChild;
    use crossbeam::channel::bounded;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        crate::impl_as_any!(TestObsLogic);
        fn clean_up(&self, _observer: Arc<Observer>) {
            // No-op for testing
        }
    }

    #[test]
    fn test_thread_pool_new_creates_pool() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle.clone());

        // Act
        let pool = ThreadPool::new(2, queue, packet_handle, None);

        // Assert
        assert_eq!(pool.num_workers(), 2);
        assert!(pool.workers.is_empty());
        assert!(pool.pool_exit_flag.load(Ordering::SeqCst) == false);
        assert!(pool.pool_pause_flag.load(Ordering::SeqCst) == false);
    }

    #[test]
    fn test_thread_pool_new_with_zero_workers() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle.clone());

        // Act
        let pool = ThreadPool::new(0, queue, packet_handle, None);

        // Assert
        assert_eq!(pool.num_workers(), 0);
    }

    #[test]
    fn test_thread_pool_submit_task_pushes_to_queue() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle.clone());
        let pool = ThreadPool::new(1, queue, packet_handle, None);
        let notif = Log::log("test_message".to_string(), "Main");

        // Act
        pool.submit_task(notif);

        // Set exit flag and try to pop
        pool.main_queue.exit_flag.store(true, Ordering::SeqCst);
        let popped = pool.main_queue.pop();

        // Assert - should have popped our notification
        assert_eq!(popped.notif_type.inner(), "Log");
    }

    #[test]
    fn test_thread_pool_num_workers_returns_correct_value() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle.clone());

        // Act
        let pool = ThreadPool::new(5, queue, packet_handle, None);

        // Assert
        assert_eq!(pool.num_workers(), 5);
    }

    #[test]
    fn test_thread_pool_pool_exit_flag_returns_clone() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle.clone());
        let pool = ThreadPool::new(1, queue, packet_handle, None);

        // Act
        let flag1 = pool.pool_exit_flag();
        let flag2 = pool.pool_exit_flag();

        // Assert - both should be false initially
        assert!(flag1.load(Ordering::SeqCst) == false);
        assert!(flag2.load(Ordering::SeqCst) == false);
    }

    #[test]
    fn test_thread_pool_pause_sets_flags() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Assert initially not paused
        assert!(pool.pool_pause_flag.load(Ordering::SeqCst) == false);

        // Act
        pool.pause();

        // Assert
        assert!(pool.pool_pause_flag.load(Ordering::SeqCst) == true);
    }

    #[test]
    fn test_thread_pool_resume_clears_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Pause first
        pool.pause();
        assert!(pool.pool_pause_flag.load(Ordering::SeqCst) == true);

        // Act
        pool.resume();

        // Assert
        assert!(pool.pool_pause_flag.load(Ordering::SeqCst) == false);
    }

    #[test]
    fn test_thread_pool_finalize_plg_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let cfg = cfg::config::Config::default();
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = PQChild::new(&cfg , packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Assert initially not exiting
        assert!(pool.pool_exit_flag.load(Ordering::SeqCst) == false);

        // Act
        pool.finalize_plg();

        // Assert
        assert!(pool.pool_exit_flag.load(Ordering::SeqCst) == true);
    }

    #[test]
    fn test_thread_pool_finalize_obs_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Create a mock logger observer
        let (logger_task_tx, _logger_task_rx) = bounded::<Task>(10);
        let (logger_notif_tx, _logger_notif_rx) = bounded::<Notif>(10);
        let logger_packet_handle = PacketHandle::new(logger_task_tx, logger_notif_tx);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: crate::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let logger = Arc::new(Observer::new(
            "logger",
            "loggerType",
            obs_core,
            fn_to_run,
            logger_packet_handle,
        ));

        // Assert initially not exiting
        assert!(pool.pool_exit_flag.load(Ordering::SeqCst) == false);

        // Act
        pool.finalize_obs(logger);

        // Assert
        assert!(pool.pool_exit_flag.load(Ordering::SeqCst) == true);
    }

    #[test]
    fn test_thread_pool_add_worker() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let queue = QChild::new(packet_handle.clone());
        let mut pool = ThreadPool::new(1, queue, packet_handle, None);

        // Create a dummy thread handle
        let handle = std::thread::spawn(|| {});

        // Assert initially empty
        assert!(pool.workers.is_empty());

        // Act
        pool.add_worker(handle);

        // Assert
        assert_eq!(pool.workers.len(), 1);
    }
}
