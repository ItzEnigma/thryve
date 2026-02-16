// This file should have the threadpool.
// The pool can only be accessed by the engine.
// Any tasks to be submitted to the pool will be submitted from the plugin/observer, to the engine via the channels, to the pool.

use rayon;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc , Mutex};

use crate::any_naming_input::ANNamingInput;
use crate::config::Config;
use crate::notif_log::NotifLog;
use crate::packet_handle::PacketHandle;
use crate::task_queue::TaskQueue;
use crate::task::Task;
use crate::res_bullitin::ResBullitin;

// The struct containing the threadpool.
pub struct ThreadPool {
    main_pool: rayon::ThreadPool,                   // The main pool that will be created by Rayon                                                  | no permissions.
    pool_exit_flag: Arc<AtomicBool>,                      // A thread-safe shared reference to the flag to indicate the application is shutting down      | no permissions.
    pool_pause_flag: Arc<AtomicBool>,                     // A thread-safe shared reference to the flag to indicate the application is shutting down      | no permissions.
    main_queue: Arc<TaskQueue>,                     // A thread-safe shared reference to the priority Queue holding the tasks                       | no permissions.
    res_bullitin: Arc<Mutex<ResBullitin>>,          // A thread-safe shared reference to a mutex holding the result bullitin.                       | read only.
    packet_handle: PacketHandle                     // The packet handle that will be used to send notifications to the engine.                     | no permissions.
}

impl ThreadPool {
    // Create a new threadpool with the given size.
    // @param size: The number of threads in the pool.
    // @param packet_handle: The packet handle that will be used to send notifications to the engine.
    // @return: A new threadpool.
    pub fn new(config: &Config , packet_handle: PacketHandle) -> Self {
        Self {
            main_pool: rayon::ThreadPoolBuilder::new()
                .num_threads(config.threads.worker_count)
                .build()
                .unwrap(),
            pool_exit_flag: Arc::new(AtomicBool::new(false)),
            pool_pause_flag: Arc::new(AtomicBool::new(false)),
            main_queue: Arc::new(TaskQueue::new(config , packet_handle.clone())),
            res_bullitin: Arc::new(Mutex::new(ResBullitin::new())),
            packet_handle: packet_handle,

        }
    }

    // Submit a task to the pool.
    // This adds the task to the priority queue.
    // @param task: The task to be submitted.
    pub fn submit_task(&self , task: Task) {
        self.main_queue.push(task);
    }

    // This initializes the threads in the pool.
    // Each thread will continuously check the queue for new tasks.
    pub fn run_threads(&self) {
        let queue = Arc::clone(&self.main_queue);
        let results = Arc::clone(&self.res_bullitin);
        let exit_flag = Arc::clone(&self.pool_exit_flag);
        
        // Spawn threads in the pool.
        // The spawn_broadcast method will spawn a copy of the closure in each thread in the pool.
        // Rayon's thread pool will manage the threads and scheduling and ensure they are running.
        self.main_pool.spawn_broadcast(move |_| {
            loop {
                // Check if the exit flag is set.
                // If it is, break the loop and exit the thread.
                if exit_flag.load(Ordering::SeqCst) == true {
                    break
                }

                // Check the queue for new tasks.
                // If a task is found, execute it and store the result in the result bullitin.
                let task = queue.pop();

                // If the code reaches here, it means either the queue has been finalized, or a task was captured.
                let res_container = task.run();

                // Store the result in the result bullitin.
                let mut res_bullitin_lock = results.lock().unwrap();
                res_bullitin_lock.insert(res_container);
            }
        });
    }

    // Finalize the threadpool.
    // This will set the exit flag to true and finalize the queue.
    pub fn finalize(&mut self) {
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize();

        // Send a notification to the engine that the threadpool has been finalized.
        // This is mainly for logging purposes.
        let notif = NotifLog::log("ThreadPool Finalized".to_string() , ANNamingInput::new("Main"));
        self.packet_handle.send_notification(Box::new(notif));
    }

    // A function to pause the threadpool.
    // This will set the pause flag to true and pause the queue.
    pub fn pause(&mut self){
        self.pool_pause_flag.store(true, Ordering::SeqCst);
        self.main_queue.pause();
    }

    // A function to continue the threadpool.
    // This will set the pause flag to false and continue the queue.
    pub fn resume(&mut self){
        self.pool_pause_flag.store(false, Ordering::SeqCst);
        self.main_queue.resume();
    }
}

impl ThreadPool {
    // Get a clone of the shared reference to the result bullitin.
    // This allows the engine to access the results of the tasks executed in the pool.
    // @return: A clone of the shared reference to the result bullitin.
    pub fn res_bullitin(&self) -> Arc<Mutex<ResBullitin>>{
        self.res_bullitin.clone()
    }
}