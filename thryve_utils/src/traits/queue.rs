//! Trait for queue implementations.
//!
//! This module defines the [`Queue`] trait, which specifies the interface
//! for all queue implementations in the Thryve system. This trait enables
//! generic implementations of thread pools that can work with different
//! queue types (priority queues, segmented queues, etc.).
//!
//! # Purpose
//!
//! The `Queue` trait allows the thread pool implementation to be
//! generalized, enabling a single implementation to handle different
//! queue types. This promotes code reuse and simplifies maintenance.
//!
//! # Queue Types
//!
//! - [`PQChild`](crate::queues::priority_queue::PQChild): Priority queue for tasks
//! - [`QChild`](crate::queues::seg_queue::QChild): Segmented queue for notifications

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use crate::bases::observer::Observer;

/// Trait for queue implementations.
///
/// The `Queue` trait specifies the interface for all queue implementations
/// in the Thryve system. It enables generic implementations of thread pools
/// that can work with different queue types.
///
/// # Type Parameters
///
/// - `T`: The type of items stored in the queue
///
/// # Trait Bounds
///
/// - `Send`: Queues can be transferred between threads
/// - `Sync`: Queues can be shared between threads
/// - `'static`: Queues have no non-'static references
///
/// # Queue Types
///
/// - [`PQChild`](crate::queues::priority_queue::PQChild): Priority queue for tasks
/// - [`QChild`](crate::queues::seg_queue::QChild): Segmented queue for notifications
///
/// # Example
///
/// ```no_run
/// use thryve_utils::traits::queue::Queue;
///
/// // Using a priority queue
/// let pq = /* ... */;
/// pq.push(my_task);
/// let task = pq.pop();
///
/// // Using a segmented queue
/// let sq = /* ... */;
/// sq.push(my_notification);
/// let notif = sq.pop();
/// ```
pub trait Queue<T>: Send + Sync + 'static {
    /// Adds an item to the queue.
    ///
    /// Inserts the given value into the queue. The item will be
    /// available for retrieval via `pop()` according to the queue's
    /// ordering semantics (FIFO, priority-based, etc.).
    ///
    /// # Arguments
    ///
    /// * `value` - The item to add to the queue
    ///
    /// # Example
    ///
    /// ```no_run
    /// queue.push(my_item);
    /// ```
    fn push(&self, value: T);

    /// Removes and returns the next item from the queue.
    ///
    /// Retrieves the next item according to the queue's ordering
    /// semantics. This method may block if the queue is empty and
    /// not finalized.
    ///
    /// # Returns
    ///
    /// The next item from the queue, or a special sentinel value
    /// when the queue is being finalized.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let item = queue.pop();
    /// ```
    fn pop(&self) -> T;

    /// Finalizes the queue for shutdown.
    ///
    /// Prepares the queue for termination, performing any necessary
    /// cleanup. After this call, `pop()` will return sentinel values
    /// to signal consumers to exit.
    ///
    /// # Arguments
    ///
    /// * `logger` - Optional logger observer for draining notifications
    ///
    /// # Example
    ///
    /// ```no_run
    /// queue.finalize(Some(logger.clone()));
    /// ```
    fn finalize(&self, logger: Option<Arc<Observer>>);

    /// Pauses the queue.
    ///
    /// Causes `pop()` to block until `resume()` is called. This is
    /// used for controlled shutdown and debugging.
    ///
    /// # Example
    ///
    /// ```no_run
    /// queue.pause();
    /// // pop() will block here
    /// queue.resume();
    /// ```
    fn pause(&self);

    /// Resumes the queue.
    ///
    /// Unblocks any threads waiting in `pop()` due to a pause.
    ///
    /// # Example
    ///
    /// ```no_run
    /// queue.resume();
    /// // pop() will now continue
    /// ```
    fn resume(&self);

    /// Returns the queue's own shutdown flag.
    ///
    /// This is the same flag `pop()` checks internally before blocking and
    /// after waking. Consumers (thread pools) should adopt this flag as
    /// their own shutdown signal rather than allocating a separate one --
    /// otherwise a worker can pass its own pre-`pop()` shutdown check just
    /// before the queue's flag flips, block inside `pop()`, and then treat
    /// the sentinel value `pop()` returns on shutdown as real work.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let shared_flag = queue.exit_flag();
    /// ```
    fn exit_flag(&self) -> Arc<AtomicBool>;
}
