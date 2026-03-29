// A trait for notification objects.
// Each notification struct must implement this trait to provide access to its properties.
// This allows for polymorphic handling of different notification types.
use crate::traits::packets::Packet;

pub trait NotifLogic: Packet{
    // A function to get a reference to the object as a dynamic Any type.
    // This is useful for downcasting to the concrete type later.
    // @return A reference to the object as a dynamic Any type.
    fn as_any(&self) -> &dyn std::any::Any;
}

impl <T:NotifLogic> Packet for T{}