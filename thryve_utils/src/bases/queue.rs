//! Base queue structure for the Thryve queue system.
//!
//! This module provides the [`QueueBase`] struct, which serves as the foundation
//! for all queue implementations in the Thryve system. It provides common
//! functionality and fields that are shared among all queue types.
//!
//! # Architecture
//!
//! The `QueueBase` struct is designed to be embedded in specific queue
//! implementations, providing:
//! - Task configuration for queue behavior
//! - A main queue for storing items
//! - Synchronization primitives for thread-safe access
//! - Control flags for pause/resume and exit functionality
//! - A packet handle for communication with the engine
//!
//! # Usage
//!
//! Queue implementations embed `QueueBase` to inherit its functionality,
//! then implement the [`Queue`] trait to provide type-specific behavior.

use std::{marker::PhantomData, sync::{Arc, Condvar, Mutex, atomic::AtomicBool}};

use crate::cfg::config::TaskConfig;
use crate::packets::packet_handle::PacketHandle;

/// Base structure for all queue implementations in the Thryve system.
///
/// The `QueueBase` struct provides common functionality and fields that are
/// shared among all queue types. It is designed to be embedded in specific
/// queue implementations to inherit its functionality.
///
/// # Type Parameters
///
/// - `Q`: The underlying queue type (e.g., `SkipMap`, `SegQueue`)
/// - `P`: The packet type stored in the queue
///
/// # Fields
///
/// - `task_config`: Optional configuration for task-related queue behavior
/// - `main_queue`: The underlying queue storing items
/// - `gate`: Mutex for synchronizing access to the condition variable
/// - `condition_var`: Condition variable for waiting on queue state changes
/// - `exit_flag`: Atomic flag indicating whether the queue should terminate
/// - `pause_flag`: Atomic flag indicating whether the queue is paused
/// - `packet_handle`: Handle for communicating with the engine
///
/// # Example
///
/// ```no_run
/// // Queue implementations embed QueueBase to inherit common functionality
/// pub type MyQueue<T> = QueueBase<SomeQueueType, T>;
/// ```
pub struct QueueBase<Q, P> {
    /// Optional configuration for task-related queue behavior.
    pub task_config: Option<Arc<TaskConfig>>,
    /// The underlying queue storing items.
    pub main_queue: Q,
    /// Mutex for synchronizing access to the condition variable.
    pub gate: Mutex<()>,
    /// Condition variable for waiting on queue state changes.
    pub condition_var: Condvar,
    /// Atomic flag indicating whether the queue should terminate.
    pub exit_flag: Arc<AtomicBool>,
    /// Atomic flag indicating whether the queue is paused.
    pub pause_flag: Arc<AtomicBool>,
    /// Handle for communicating with the engine.
    pub packet_handle: PacketHandle,
    /// Phantom data to maintain ownership of the packet type.
    pub _marker: PhantomData<P>,
}
