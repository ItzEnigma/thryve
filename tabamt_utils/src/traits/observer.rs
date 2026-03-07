// Implementation for the observer pattern.
// Observers can be loggers, audits, email sendings, etc.... Each needs to be implemented alone.

// The trait that needs to be implemented by any observer created.
// It needs to be Send + Sync in order to be used in a multithreaded context.
pub trait ObsB: Send + Sync{}

pub trait Observer: Send + Sync {
    fn clean_up(&self);
}