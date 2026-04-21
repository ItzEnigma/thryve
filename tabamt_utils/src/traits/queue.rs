//! This module contains the definition of the `Queue` trait, which is used to generalize the behavior of queues in the framework.
//! The `Queue` trait defines the basic operations that a queue should support.

use std::sync::Arc;

use crate::bases::observer::Observer;

/// Is a trait for queues to allow for the generalization of the Threadpool implementation.
/// This trait defines the behavior and functionality of queues, allowing for different types of queues to be used interchangeably.
pub trait Queue<T>: Send + Sync + 'static {
    /// Adds a `value` to the queue.
    fn push(&self, value: T);

    /// Removes and returns the next value from the queue.
    fn pop(&self) -> T;
    
    /// Finalizes the queue, allowing for any necessary cleanup or finalization logic to be executed.
    fn finalize(&self , logger: Option<Arc<Observer>>);
    
    /// Pauses the queue, allowing for any necessary logic to be executed when the queue is paused.
    fn pause(&self);
    
    /// Resumes the queue, allowing for any necessary logic to be executed when the queue is resumed.
    fn resume(&self);
}