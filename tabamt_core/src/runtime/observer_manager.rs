use tabamt_utils::{any_naming_input::ANNamingInput, notif_log::NotifLog , notif::Notif , observer::Observer};

use std::{collections::HashMap};

// Manages observers and forwards notifications to them.
// Handles loading and finalizing observers.
// Ensures that notifications are sent to the correct observer based on their type.
// It stores observers in a HashMap for efficient access.
// This makes the engine modular, but still capable of directly accessing public facing functions of the observers if needed.
pub struct ObserverManager {
    observers: HashMap<ANNamingInput , Box<dyn Observer>>      // Key: Observer type specified by the developer, Value: Observer | read only
}

impl ObserverManager {
    // Creates a new ObserverManager with an empty observer list.
    // @return: A new instance of ObserverManager.
    pub fn new() -> Self{
        ObserverManager {
            observers: HashMap::new()
        }
    }

    // Checks if an observer of the specified type exists in the manager.
    // This is used internally to ensure that notifications are sent to valid observers.
    // @param obs_type: The type of the observer to check for.
    // @return: true if the observer exists, false otherwise.
    fn check_observer(&self , obs_type: &ANNamingInput) -> bool{
        if self.observers.contains_key(obs_type) {
            return true;
        }
        return false;
    }
    
    // Forwards a notification to the appropriate observer based on its type.
    // Asserts that the observer exists before attempting to send the notification.
    // @param notif: The notification to be forwarded.
    pub fn forward_notif(&mut self , notif: Box<dyn Notif>) {
        assert!(self.check_observer(notif.get_send_to()), "Observer Manager Cannot Find Requested Observer. Got {}" , notif.get_send_to());

        self.observers
        .get_mut(notif.get_send_to())
        .unwrap()
        .consume(notif);
    }

    // Convenience method to send an error notification to the logging observer.
    // This is a shortcut for sending error messages to the logger instead of specifying known parameters.
    // @param message: The error message to be logged.
    pub fn notif_err(&mut self , message: &str){
        self.observers
            .get_mut(&ANNamingInput::new("Logging"))
            .unwrap()
            .consume(Box::new(NotifLog::err(message.to_string())));
    }

    // Loads a new observer into the manager.
    // This allows the system to dynamically add observers at runtime.
    // @param observer: The observer to be loaded.
    pub fn load_observer(&mut self , observer: Box<dyn Observer>){
        self.observers.insert(observer.get_observer_type().clone() , observer);
    }
    
    // Finalizes the observer manager by clearing all observers and sending a final notification.
    // This is typically called during system shutdown to ensure that resources are cleaned up properly.
    pub fn finalize(&mut self) -> Box<dyn Observer>{
        let notif = NotifLog::log("Observer manager Finalized".to_string() , ANNamingInput::new("Main"));
        self.forward_notif(Box::new(notif));

        
        let logger = self.observers.remove(&ANNamingInput::new("Logging")).unwrap();
        self.observers.clear();
            
        logger
    }
}


impl ObserverManager {
    // A function to get the names of all loaded observers.
    // This can be useful for debugging or monitoring purposes.
    // @return: A vector of observer names.
    pub fn get_observer_names(&self) -> Vec<&str>{
        let mut vector: Vec<&str> = Vec::new();
        for obs in self.observers.keys(){
            vector.push(obs.get_inner());
        }
    
        vector
    }

    // A function to retrieve a specific observer by its type.
    // @param obs_type: The type of the observer to retrieve.
    // @return: An Option containing a reference to the observer if found, or None if not found.
    fn get_observer(&mut self , obs_type: &ANNamingInput) -> Option<&Box<dyn Observer>>{
        // Assert that the observer exists before attempting to retrieve it.
        assert!(self.check_observer(obs_type), "Observer Manager Cannot Find Requested Observer. Got {}" , obs_type);
        self.observers.get(obs_type)
    }
}