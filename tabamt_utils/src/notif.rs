// A trait for notification objects.
// Each notification struct must implement this trait to provide access to its properties.
// This allows for polymorphic handling of different notification types.

use std::time::SystemTime;

use crate::any_naming_input::ANNamingInput;

pub trait Notif: Send + Sync {
    // A function to get a reference to the object as a dynamic Any type.
    // This is useful for downcasting to the concrete type later.
    // @return A reference to the object as a dynamic Any type.
    fn as_any(&self) -> &dyn std::any::Any;

    // Getter methods for notification properties.

    // Get the event type of the notification.
    // @return A reference to the event type as an ANNamingInput.
    fn event_type(&self) -> &ANNamingInput;

    // Get the time stamp of the notification.
    // @return A reference to the time stamp as a SystemTime.
    fn time_stamp(&self) -> &SystemTime;

    // Get the sender of the notification.
    // @return A reference to whom should the notification be sent as an ANNamingInput.
    fn obs_receiver(&self) -> &ANNamingInput;
}