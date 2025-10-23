// Implementation for the observer pattern.
// Observers can be loggers, audits, email sendings, etc.... Each needs to be implemented alone.

use crate::{notification::Notification, any_naming_input::ANNamingInput};

// The trait that needs to be implemented by any observer created.
// It needs to be Send + Sync in order to be used in a multithreaded context.
pub trait Observer: Send + Sync{
    fn consume(&mut self , notif: Notification);
    fn get_observer_name(&self) -> &str;
    fn get_observer_type(&self) -> &ANNamingInput;
}