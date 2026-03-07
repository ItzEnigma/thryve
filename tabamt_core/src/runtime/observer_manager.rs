use tabamt_utils::cfg::ctx::Ctx;
use tabamt_utils::extras::named_input::NamedInput;
use tabamt_utils::bases::observer_base::ObsBase;
use tabamt_utils::packets::notif_log::NotifLog;
use tabamt_utils::queues::seg_queue::QChild;
use tabamt_utils::threadpool::ThreadPool;
use tabamt_utils::traits::{notif_ex::NotifEX , queue::Queue};

use std::{collections::HashMap};

// Manages observers and forwards notifications to them.
// Handles loading and finalizing observers.
// Ensures that notifications are sent to the correct observer based on their type.
// It stores observers in a HashMap for efficient access.
// This makes the engine modular, but still capable of directly accessing public facing functions of the observers if needed.
pub struct ObserverManager {
    observers: HashMap<NamedInput , ObsBase>,      // Key: Observer type specified by the developer, Value: Observer | read only
    pub threadpool: ThreadPool<QChild , Box<dyn NotifEX>>,
}

impl ObserverManager {
    // Creates a new ObserverManager with an empty observer list.
    // @return: A new instance of ObserverManager.
    pub fn new(ctx: &Ctx) -> Self{
        let queue = QChild::new(&ctx.config, ctx.packet_handle());

        ObserverManager {
            observers: HashMap::new(),
            threadpool: ThreadPool::new(&ctx.config, queue, ctx.packet_handle(), None),
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
    pub fn forward_notif(&mut self , notif: Box<dyn NotifEX>) {
        assert!(self.check_observer(notif.obs_receiver()), "Observer Manager Cannot Find Requested Observer. Got {}" , notif.obs_receiver());

        self.threadpool.main_queue.push(notif);
    }

    // Convenience method to send an error notification to the logging observer.
    // This is a shortcut for sending error messages to the logger instead of specifying known parameters.
    // @param message: The error message to be logged.
    pub fn notif_err(&mut self , message: &str){
        self.threadpool.main_queue.push(NotifLog::err(message.to_string()));
    }

    // Loads a new observer into the manager.
    // This allows the system to dynamically add observers at runtime.
    // @param observer: The observer to be loaded.
    pub fn load_observer(&mut self , mut observer: ObsBase){
        self.threadpool.run_thread_obs(observer.fn_to_run());
        self.observers.insert(NamedInput::new(observer.obs_type.inner()) , observer);
    }
    
    // Finalizes the observer manager by clearing all observers and sending a final notification.
    // This is typically called during system shutdown to ensure that resources are cleaned up properly.
    pub fn finalize(&mut self) -> ObsBase{
        let notif = NotifLog::log("Observer manager Finalized".to_string() , "Main");
        self.forward_notif(notif);

        
        let logger = self.observers.remove(&NamedInput::new("Logging")).unwrap();
        self.observers.clear();
            
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