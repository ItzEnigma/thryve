//! This module contains the `ThreadPool` struct, which is responsible for managing a pool of worker threads that process tasks from a queue.
//! There are two main types of `ThreadPool`s in the engine: one for processing Plugin Tasks and one for processing Observer Notifs.
//! The `ThreadPool` struct is generic over the type of queue it uses and the type of packets it processes.
//! This allows for flexibility in how tasks and notifications are managed and processed.
use rayon;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::marker::PhantomData;
use crossbeam::channel::Sender;

use crate::bases::observer::Observer;
use crate::packets::{notif_log::Log , packet_handle::PacketHandle};
use crate::traits::{packets::Packet , queue::Queue};
use crate::res;

/// The `ThreadPool` struct manages a pool of worker threads that process tasks from a queue.
pub struct ThreadPool <Q , P> {
    main_pool: Option<rayon::ThreadPool>,                   
    pool_exit_flag: Arc<AtomicBool>,                      
    pool_pause_flag: Arc<AtomicBool>,                     
    pub main_queue: Arc<Q>,                     
    res_tx: Option<Arc<Sender<(u64 , res::Result)>>>,          
    packet_handle: PacketHandle,                     

    _marker: PhantomData<P>
}

impl <Q , P: Packet> ThreadPool<Q , P> 
where
    Q: Queue<P>
{
    /// Creates a new `ThreadPool` with the specified number of worker threads, a queue for tasks, a `PacketHandle` for communication, and an optional result sender for sending results back to the engine.
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

    /// Submits a task to the thread pool by pushing it into the main queue.
    pub fn submit_task(&self , obj: P) {
        self.main_queue.push(obj);
    }

    /// Runs the worker threads for processing Plugin Tasks.
    /// Each worker thread continuously pops Tasks from the main queue and processes them until the `pool_exit_flag` is set.
    /// If a Task produces a result, it is sent back to the engine using the `res_tx`.
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
                }
            });
        }
    }

    /// Runs the worker thread for processing Observer Notifs.
    /// This method runs the same base logic for all observers to initialize them.
    /// It then calls the given observer's main function that should contain the main loop for the observer's thread.
    pub fn run_thread_obs(&self , obs: Arc<Observer>) {
        let arc_obs = Arc::clone(&obs);

        if let Some(pool) = &self.main_pool{
            pool.spawn(move || {
                (arc_obs.fn_to_run)(arc_obs.clone());
            });
        }        
    }

    /// Returns a reference to the main thread pool, if it exists.
    pub fn main_pool(&self) -> &Option<rayon::ThreadPool> {
        &self.main_pool
    }

    /// Returns a clone of the `pool_exit_flag`, which is an `Arc<AtomicBool>` that indicates whether the thread pool should exit.
    pub fn pool_exit_flag(&self) -> Arc<AtomicBool> {
        self.pool_exit_flag.clone()
    }

    /// finalizes the plugin thread pool by setting the exit flag and finalizing the main queue.
    pub fn finalize_plg(&mut self) {
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize(None);
        
        let notif = Log::log("ThreadPool Finalized".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }

    /// finalizes the observer thread pool by setting the exit flag and finalizing the main queue.
    pub fn finalize_obs(&mut self , logger: Arc<Observer>){
        self.pool_exit_flag.store(true, Ordering::SeqCst);
        self.main_queue.finalize(Some(logger.clone()));
        
        let notif = Log::log("ThreadPool Finalized".to_string() , "Main");
        logger.obs_queue.push(notif);
    }

    /// Pauses the thread pool by setting the `pool_pause_flag` and pausing the main queue.
    pub fn pause(&mut self){
        self.pool_pause_flag.store(true, Ordering::SeqCst);
        self.main_queue.pause();
    }

    /// Resumes the thread pool by clearing the `pool_pause_flag` and resuming the main queue.
    pub fn resume(&mut self){
        self.pool_pause_flag.store(false, Ordering::SeqCst);
        self.main_queue.resume();
    }
}