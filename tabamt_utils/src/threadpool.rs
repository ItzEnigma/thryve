// This file should have the threadpool.
// The pool can only be accessed by the engine.
// Any tasks to be submitted to the pool will be submitted from the plugin/observer, to the engine via the channels, to the pool.

use rayon;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::marker::PhantomData;
use crossbeam::channel::Sender;

use crate::bases::observer::Observer;
use crate::packets::{notif_log::Log , packet_handle::PacketHandle};
use crate::traits::{packets::Packet , queue::Queue};
use crate::res;

// The struct containing the threadpool.
pub struct ThreadPool <Q , P> {
    main_pool: Option<rayon::ThreadPool>,                   // The main pool that will be created by Rayon                                                  | no permissions.
    pool_exit_flag: Arc<AtomicBool>,                      // A thread-safe shared reference to the flag to indicate the application is shutting down      | no permissions.
    pool_pause_flag: Arc<AtomicBool>,                     // A thread-safe shared reference to the flag to indicate the application is shutting down      | no permissions.
    pub main_queue: Arc<Q>,                     // A thread-safe shared reference to the priority Queue holding the tasks                       | no permissions.
    res_tx: Option<Arc<Sender<(u64 , res::Result)>>>,          // A thread-safe shared reference to a mutex holding the result bullitin.                       | read only.
    packet_handle: PacketHandle,                     // The packet handle that will be used to send notifications to the engine.                     | no permissions.

    _marker: PhantomData<P>
}

impl <Q , P: Packet> ThreadPool<Q , P> 
where
    Q: Queue<P>
{
    // Create a new threadpool with the given size.
    // @param size: The number of threads in the pool.
    // @param packet_handle: The packet handle that will be used to send notifications to the engine.
    // @return: A new threadpool.
    pub fn new(num_workers: usize , queue: Q , packet_handle: PacketHandle , res_tx: Option<Arc<Sender<(u64 , res::Result)>>>) -> Self {
        let mut pool: Option<rayon::ThreadPool> = None;
        if num_workers != 0 {
            pool = Some(rayon::ThreadPoolBuilder::new()
                .num_threads(num_workers)
                .build()
                .unwrap());
        }

        Self {
            main_pool: pool,
            pool_exit_flag: Arc::new(AtomicBool::new(false)),
            pool_pause_flag: Arc::new(AtomicBool::new(false)),
            main_queue: Arc::new(queue),
            res_tx: res_tx,
            packet_handle: packet_handle,
            _marker: PhantomData
        }
    }

    // Submit a task to the pool.
    // This adds the task to the priority queue.
    // @param task: The task to be submitted.
    pub fn submit_task(&self , obj: P) {
        self.main_queue.push(obj);
    }

    // This initializes the threads in the pool.
    // Each thread will continuously check the queue for new tasks.
    pub fn run_threads_plugins(&self) {
        let queue = Arc::clone(&self.main_queue);
        let res_tx = self.res_tx.clone().expect("Result Sender Has to be Present!");
        let exit_flag = Arc::clone(&self.pool_exit_flag);
        let tx = self.packet_handle.clone();
        
        // Spawn threads in the pool.
        // The spawn_broadcast method will spawn a copy of the closure in each thread in the pool.
        // Rayon's thread pool will manage the threads and scheduling and ensure they are running.
        
        if let Some(pool) = &self.main_pool{
            pool.spawn_broadcast(move |_| {
                loop {
                    // Check if the exit flag is set.
                    // If it is, break the loop and exit the thread.
                    if exit_flag.load(Ordering::SeqCst) == true {
                        let notif = Log::log("Plugin Thread Exiting...".to_string(), "Main");
                        tx.send_notification(notif);
                        break
                    }
    
                    // Check the queue for new tasks.
                    // If a task is found, execute it and store the result in the result bullitin.
                    let obj = queue.pop();
    
                    // If the code reaches here, it means either the queue has been finalized, or a task was captured.                
                    if let Some(res_container) = obj.process().take() {
                        let _ = res_tx.send(res_container);
                    }
    
                    // Store the result in the result bullitin.
                }
            });
        }
    }

    pub fn run_thread_obs(&self , obs: Arc<Observer>) {
        let arc_obs = Arc::clone(&obs);

        if let Some(pool) = &self.main_pool{
            pool.spawn(move || {
                (arc_obs.fn_to_run)(arc_obs.clone());
            });
        }
        

        // Spawn threads in the pool.
    }

    pub fn main_pool(&self) -> &Option<rayon::ThreadPool> {
        &self.main_pool
    }

    pub fn pool_exit_flag(&self) -> Arc<AtomicBool> {
        self.pool_exit_flag.clone()
    }

    // Finalize the threadpool.
    // This will set the exit flag to true and finalize the queue.
    pub fn finalize_plg(&mut self) {
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize(None);
        // Send a notification to the engine that the threadpool has been finalized.
        // This is mainly for logging purposes.
        let notif = Log::log("ThreadPool Finalized".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }

    pub fn finalize_obs(&mut self , logger: Arc<Observer>){
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize(Some(logger.clone()));
        // Send a notification to the engine that the threadpool has been finalized.
        // This is mainly for logging purposes.
        let notif = Log::log("ThreadPool Finalized".to_string() , "Main");
        logger.obs_queue.push(notif);
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