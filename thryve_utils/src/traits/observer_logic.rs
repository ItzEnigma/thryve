//! This module defines the `ObsLogic` trait for Observer objects' logic.
//! This trait defines the behavior and functionality of Observer objects in the system.
//! It provides a way to handle different types of Observers polymorphically, allowing for flexible and extensible Observer handling.
use std::sync::Arc;

use crate::bases::observer::Observer;


/// Is a marker trait for observer objects.
/// It is used to indicate that a struct is an observer object and can be used as an observer in the system.
/// This trait is only used in the Observer struct.
pub trait ObsB: Send + Sync{}

/// Is a trait for Observer objects' logic.
/// It is used to indicate that a struct is a core logic of the Observer object.
pub trait ObsLogic: Send + Sync + 'static {
    /// Shuts down the given Observer gracefully.
    fn clean_up(&self , observer: Arc<Observer>);

    /// Gets a reference to the object as a dynamic Any type.
    /// This is useful for downcasting to the concrete type later.
    fn as_any(&self) -> &dyn std::any::Any;
}