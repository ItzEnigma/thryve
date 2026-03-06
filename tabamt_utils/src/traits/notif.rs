// A trait for notification objects.
// Each notification struct must implement this trait to provide access to its properties.
// This allows for polymorphic handling of different notification types.

use std::time::SystemTime;

use crate::extras::named_input::NamedInput;
use crate::traits::packets::Packet;

pub trait Notif: Packet{
    // A function to get a reference to the object as a dynamic Any type.
    // This is useful for downcasting to the concrete type later.
    // @return A reference to the object as a dynamic Any type.
    fn as_any(&self) -> &dyn std::any::Any;

    // Getter methods for notification properties.

    // Get the event type of the notification.
    // @return A reference to the event type as an NamedInput.
    fn event_type(&self) -> &NamedInput;

    // Get the time stamp of the notification.
    // @return A reference to the time stamp as a SystemTime.
    fn time_stamp(&self) -> &SystemTime;

    // Get the sender of the notification.
    // @return A reference to whom should the notification be sent as an NamedInput.
    fn obs_receiver(&self) -> &NamedInput;
}

impl <T:Notif> Packet for T{}