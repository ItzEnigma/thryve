// The trait that all plugins must implement.
// A compilation error will occur if a plugin does not implement the Plugin trait and is tried to be loaded by the engine.
// This trait defines the basic functions that all plugins must have.
// This will allow the engine to interact with the plugins in a generic way.
// Each plugin will have its own implementation of these functions.
// All plugins must be Send + Sync to be used in a multithreaded context.
pub trait PluginB {
    fn clean_up(&self);
}

pub trait Plugin: Send + Sync {
    fn clean_up(&self);
}