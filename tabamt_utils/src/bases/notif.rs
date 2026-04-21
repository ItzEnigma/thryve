//! This module contains the `Notif` struct, which is the core of the notification system in TabAMT.
//! 
//! The `Notif` struct holds information about a notification,
//! including its type, receiver, core logic, and timestamp.
//! 
//! Any struct that implements the NotifLogic trait can be used as the core of a `Notif`,
//! allowing for flexible and extensible notification handling.

use crate::impl_as_any;
use crate::traits::notif_logic::NotifLogic;
use crate::extras::named_input::NamedInput;
use std::time::SystemTime;

/// Holds information about a notification.
/// Any struct that implements NotifLogic can be used as the core of a `Notif`.
pub struct Notif{
    pub notif_type: NamedInput,              
    pub notif_receiver: NamedInput,                 
    pub notif_core: Box<dyn NotifLogic>,
    pub timestamp: SystemTime,  
}

impl NotifLogic for Notif {
    impl_as_any!(Notif);
}

impl Notif {
    /// Creates a new `Notif` struct with the given notification type, notification receiver, and core logic of the notification.
    pub fn new(notif_type: &str , notif_receiver: &str , notif_core: Box<dyn NotifLogic>) -> Self {
        Self {
            notif_type: NamedInput::new(notif_type),
            notif_receiver: NamedInput::new(notif_receiver),
            notif_core,
            timestamp: SystemTime::now(),
        }
    }
}