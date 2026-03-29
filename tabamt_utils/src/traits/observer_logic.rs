// Implementation for the observer pattern.
// Observers can be loggers, audits, email sendings, etc.... Each needs to be implemented alone.

use std::sync::Arc;

use crate::bases::observer::Observer;

// The trait that needs to be implemented by any observer created.
// It needs to be Send + Sync in order to be used in a multithreaded context.
pub trait ObsB: Send + Sync{}

pub trait ObsLogic: Send + Sync + 'static {
    fn clean_up(&self , observer: Arc<Observer>);

    fn as_any(&self) -> &dyn std::any::Any;
}