//! This module defines the `NotifLogic` trait for Notif objects' logic.
//! This trait defines the behavior and functionality of Notif objects in the system.
//! It provides a way to handle different types of Notifs polymorphically, allowing for flexible and extensible Notif handling.
//! This allows for polymorphic handling of different Notif types.
use crate::traits::packets::Packet;

/// Is a trait for Notif objects' logic.
/// It is used to indicate that a struct is a core logic of the Notif object.
pub trait NotifLogic: Packet{
    /// gets a reference to the object as a dynamic Any type.
    /// This is useful for downcasting to the concrete type later.
    fn as_any(&self) -> &dyn std::any::Any;
}

impl <T:NotifLogic> Packet for T{}