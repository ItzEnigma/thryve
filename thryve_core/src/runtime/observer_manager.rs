//! This module defines the `ObserverManager` struct, which is responsible for managing observers and their associated threads in the engine.
//! It provides functionality to load observers, forward notifications to the appropriate observer queues, and finalize the observer manager during engine shutdown.
//! The `ObserverManager` maintains a mapping of observer types to their corresponding `Observer` instances and manages a thread pool for processing notifications.


use thryve_utils::bases::notif::Notif;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::bases::observer::Observer;
use thryve_utils::packets::notif_log::Log;
use thryve_utils::queues::seg_queue::QChild;
use thryve_utils::threadpool::ThreadPool;
use thryve_utils::traits::queue::Queue;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::{collections::HashMap};

/// Takes a hashmap of observer types to their corresponding `Observer` instances
/// It manages a thread pool for processing notifications.
/// It provides methods to load observers, forward notifications to the appropriate observer queues, and finalize the observer manager.
/// The `threadpool` has a minimum of 2 workers and a maximum equal to the number of observers plus 2.
/// The minimum of 2 workers is there because of the sorter thread, see [ObserverManager::init_obs_sorter_thread], and the logging observer thread.
/// This sorter thread is responsible for sorting incoming notifications and forwarding them to the correct observer queues.
pub struct ObserverManager {
    observers: HashMap<NamedInput , Arc<Observer>>,     
    pub threadpool: ThreadPool<QChild , Notif>,
}

impl ObserverManager {
    
    /// Creates the manager of observers with the provided `Ctx`, initializing its components.
    /// The `ObserverManager` is initialized with the provided context, which contains configuration and communication channels.
    /// The thread pool is created with a number of workers equal to the number of observers plus 2.
    /// It uses a `QChild` queue for managing notifications.
    pub fn new(ctx: &Ctx) -> Self{
        let queue = QChild::new(ctx.packet_handle());
        let num_workers = ctx.observers.len() + 2;

        ObserverManager {
            observers: HashMap::new(),
            threadpool: ThreadPool::new(num_workers, queue, ctx.packet_handle(), None),
        }
    }

    /// Initializes the observer sorter thread responsible for sorting incoming notifications and forwarding them to the correct observer queues.
    /// The thread will exit gracefully when the engine signals for shutdown by setting the exit flag.
    pub fn init_obs_sorter_thread(&self) {
        if let Some(pool) = self.threadpool.main_pool(){
            let mut arc_observers: HashMap<NamedInput , Arc<Observer>> = HashMap::new();
            let queue = Arc::clone(&self.threadpool.main_queue);
            let exit_flag = Arc::clone(&self.threadpool.pool_exit_flag());
            
            // Populate the `arc_observers` hashmap with the observer types and their corresponding `Arc<Observer>` instances for use in the sorter thread.
            for (_ , observer) in &self.observers{
                arc_observers.insert(NamedInput::new(observer.obs_type.inner()) ,Arc::clone(observer));
            }

            // Spawn the sorter thread in the thread pool
            // It will continuously check for new notifications and forward them to the appropriate observer queues until the exit flag is set to true.
            pool.spawn(move || {
                loop {
                    
                    // Check if the exit flag is set to true, indicating that the engine is shutting down.
                    // If so, log a message and break the loop to exit the thread gracefully.
                    if exit_flag.load(Ordering::SeqCst) == true {
                        let notif = Log::log("Obs Sorter Thread Finalized...".to_string(), "Main");
                        arc_observers.get(&NamedInput::new("Logging")).unwrap().obs_queue.push(notif);
                        break
                    }

                    
                    // Check for new notifications in the main queue and forward them to the appropriate observer queues based on the notification's receiver type.
                    let obj = queue.pop();
                    let _ = arc_observers.get(&obj.notif_receiver).unwrap().obs_queue.push(obj);

                }
            });
        }
    }

    /// Checks if an observer of the specified `obs_type` exists in the `observers` hashmap.
    fn check_observer(&self , obs_type: &NamedInput) -> bool{
        if self.observers.contains_key(obs_type) {
            return true;
        }
        return false;
    }
    
    /// Forwards a given `Notif` to the main queue of the thread pool.
    /// It first checks if the observer specified in the notification's receiver field exists in the `observers` hashmap.
    /// If the observer exists, it pushes the notification to the main queue of the thread pool for processing by the sorter thread.
    /// If the observer does not exist, it panics with an error message indicating that the observer manager cannot find the requested observer.
    pub fn forward_notif(&mut self , notif: Notif) {
        assert!(self.check_observer(&notif.notif_receiver), "Observer Manager Cannot Find Requested Observer. Got {}" , &notif.notif_receiver);

        self.threadpool.main_queue.push(notif);
    }

    /// Is a helper method that allows the engine to log error messages to the logging observer's queue.
    /// It pushes a log notification with the error `message`.
    pub fn notif_err(&mut self , message: &str){
        self.threadpool.main_queue.push(Log::err(message.to_string()));
    }

    /// Loads a new `Observer` into the `ObserverManager`, starting its associated thread and adding it to the `observers` hashmap.
    pub fn load_observer(&mut self , observer: Observer){
        let x = Arc::new(observer);
        self.threadpool.run_thread_obs(Arc::clone(&x));
        self.observers.insert(NamedInput::new(x.obs_type.inner()) , x);
    }
    
    
    /// Finalizes the observer manager by setting the exit flag, finalizing the thread pool, processing any remaining notifications, and cleaning up resources.
    /// It returns an `Arc<Observer>` instance of the logging observer to log the final messages from the engine shutdown process.
    pub fn finalize(&mut self) -> Arc<Observer>{
        let logger = self.observers.remove(&NamedInput::new("Logging")).unwrap();
        self.threadpool.finalize_obs(logger.clone());
        
        // Clean up all remaining observers by draining the `observers` hashmap and calling the `clean_up` method on each observer instance.
        if !self.observers.is_empty(){
            for (_ , observer) in self.observers.drain(){
                observer.clean_up();
            }
        }
        
        let notif = Log::log("Observer manager Finalized".to_string() , "Main");
        logger.obs_queue.push(notif);
        
            
        logger
    }
}


impl ObserverManager {
    /// Returns a vector of all observer names.
    pub fn observer_names(&self) -> Vec<&str>{
        let mut vector: Vec<&str> = Vec::new();
        for obs in self.observers.keys(){
            vector.push(obs.inner());
        }
    
        vector
    }
}