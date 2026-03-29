use tabamt_utils::bases::notif::Notif;
use tabamt_utils::cfg::ctx::Ctx;
use tabamt_utils::extras::named_input::NamedInput;
use tabamt_utils::bases::observer::Observer;
use tabamt_utils::packets::notif_log::Log;
use tabamt_utils::queues::seg_queue::QChild;
use tabamt_utils::threadpool::ThreadPool;
use tabamt_utils::traits::queue::Queue;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::{collections::HashMap};

// Manages observers and forwards notifications to them.
// Handles loading and finalizing observers.
// Ensures that notifications are sent to the correct observer based on their type.
// It stores observers in a HashMap for efficient access.
// This makes the engine modular, but still capable of directly accessing public facing functions of the observers if needed.
pub struct ObserverManager {
    observers: HashMap<NamedInput , Arc<Observer>>,      // Key: Observer type specified by the developer, Value: Observer | read only
    pub threadpool: ThreadPool<QChild , Notif>,
}

impl ObserverManager {
    // Creates a new ObserverManager with an empty observer list.
    // @return: A new instance of ObserverManager.
    pub fn new(ctx: &Ctx) -> Self{
        let queue = QChild::new(ctx.packet_handle());
        let num_workers = ctx.observers.len() + 2;

        ObserverManager {
            observers: HashMap::new(),
            threadpool: ThreadPool::new(num_workers, queue, ctx.packet_handle(), None),
        }
    }

    pub fn init_obs_sorter_thread(&self) {
        if let Some(pool) = self.threadpool.main_pool(){
            let mut arc_observers: HashMap<NamedInput , Arc<Observer>> = HashMap::new();
            let queue = Arc::clone(&self.threadpool.main_queue);
            let exit_flag = Arc::clone(&self.threadpool.pool_exit_flag());
            
            for (_ , observer) in &self.observers{
                arc_observers.insert(NamedInput::new(observer.obs_type.inner()) ,Arc::clone(observer));
            }

            pool.spawn(move || {
                loop {
                    // Check if the exit flag is set.
                    // If it is, break the loop and exit the thread.
                    if exit_flag.load(Ordering::SeqCst) == true {
                        let notif = Log::log("Obs Sorter Thread Finalized...".to_string(), "Main");
                        arc_observers.get(&NamedInput::new("Logging")).unwrap().obs_queue.push(notif);
                        break
                    }

                    // Check the queue for new tasks.
                    // If a task is found, execute it and store the result in the result bullitin.
                    let obj = queue.pop();
                    let _ = arc_observers.get(&obj.notif_receiver).unwrap().obs_queue.push(obj);

                }
            });
        }
    }

    // Checks if an observer of the specified type exists in the manager.
    // This is used internally to ensure that notifications are sent to valid observers.
    // @param obs_type: The type of the observer to check for.
    // @return: true if the observer exists, false otherwise.
    fn check_observer(&self , obs_type: &NamedInput) -> bool{
        if self.observers.contains_key(obs_type) {
            return true;
        }
        return false;
    }
    
    // Forwards a notification to the appropriate observer based on its type.
    // Asserts that the observer exists before attempting to send the notification.
    // @param notif: The notification to be forwarded.
    pub fn forward_notif(&mut self , notif: Notif) {
        assert!(self.check_observer(&notif.notif_receiver), "Observer Manager Cannot Find Requested Observer. Got {}" , &notif.notif_receiver);

        self.threadpool.main_queue.push(notif);
    }

    // Convenience method to send an error notification to the logging observer.
    // This is a shortcut for sending error messages to the logger instead of specifying known parameters.
    // @param message: The error message to be logged.
    pub fn notif_err(&mut self , message: &str){
        self.threadpool.main_queue.push(Log::err(message.to_string()));
    }

    // Loads a new observer into the manager.
    // This allows the system to dynamically add observers at runtime.
    // @param observer: The observer to be loaded.
    pub fn load_observer(&mut self , observer: Observer){
        let x = Arc::new(observer);
        self.threadpool.run_thread_obs(Arc::clone(&x));
        self.observers.insert(NamedInput::new(x.obs_type.inner()) , x);
    }
    
    // Finalizes the observer manager by clearing all observers and sending a final notification.
    // This is typically called during system shutdown to ensure that resources are cleaned up properly.
    pub fn finalize(&mut self) -> Arc<Observer>{
        let logger = self.observers.remove(&NamedInput::new("Logging")).unwrap();
        self.threadpool.finalize_obs(logger.clone());
        
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
    // A function to get the names of all loaded observers.
    // This can be useful for debugging or monitoring purposes.
    // @return: A vector of observer names.
    pub fn observer_names(&self) -> Vec<&str>{
        let mut vector: Vec<&str> = Vec::new();
        for obs in self.observers.keys(){
            vector.push(obs.inner());
        }
    
        vector
    }
}