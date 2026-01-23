// Implementation for the observer pattern.
// Observers can be loggers, audits, email sendings, etc.... Each needs to be implemented alone.

use crate::{notif::Notif, any_naming_input::ANNamingInput};

// The trait that needs to be implemented by any observer created.
// It needs to be Send + Sync in order to be used in a multithreaded context.
pub trait Observer: Send + Sync{
    // Function to consume a notification.
    // @param notif: A boxed dynamic notification object.
    fn consume(&mut self , notif: Box<dyn Notif>);

    // Getter methods for observer properties.

    // Get the name of the observer.
    // @return A reference to the name of the observer as a string slice.
    fn observer_name(&self) -> &str;
    
    // Get the type of the observer.
    // @return A reference to the type of the observer as an ANNamingInput.
    fn observer_type(&self) -> &ANNamingInput;
}