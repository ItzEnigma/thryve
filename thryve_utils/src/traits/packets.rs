//! This module defines the `Packet` trait for Tasks and Notifs.
//! A `Packet` is a unit of data that can be sent through channels to be processed by the engine.
//! The `Packet` allows for the generalization of the Threadpool implementation.
//! This enables a single implementation of the Threadpool to handle different types of packets, making the codebase cleaner and easier to maintain.
use crate::res;

/// `Packet` is a trait for packets that allow for the generalization of the Threadpool implementation.
pub trait Packet: Send + Sync + 'static{
    /// Processes the `packet` and returns an option containing the result as a tuple of a u64 and a `res::Result`.
    /// A default implementation is provided that returns `None`, but it can be overridden to provide processing logic for different types of packets.
    /// This is only used for Task packets, as Notif packets are processed differently and do not return a result.
    fn process(self) -> Option<(u64 , res::Result)>
    where 
        Self: Sized
    {
        None
    }
}