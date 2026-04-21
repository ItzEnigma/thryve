//! This module defines the `PlgLogic` trait for Plugin objects' logic.
//! This trait defines the behavior and functionality of Plugin objects in the system.
//! It provides a way to handle different types of Plugins polymorphically, allowing for flexible and extensible Plugin handling.

/// `PluginB` is a marker trait for Plugin objects.
/// It is used to indicate that a struct is a Plugin object and can be used as a Plugin in the system.
/// This trait is only used in the Plugin struct.
pub trait PluginB {
    /// Shuts down the plugin gracefully.
    fn clean_up(&self);
}

/// Is a trait for Plugin objects' logic.
/// It is used to indicate that a struct is a core logic of the Plugin object.
pub trait PlgLogic: Send + Sync {
    fn clean_up(&self);

    fn as_any(&self) -> &dyn std::any::Any;
}