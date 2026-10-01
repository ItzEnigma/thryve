// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Trait for packet types in the Thryve system.
//!
//! This module defines the [`Packet`] trait, which serves as the common
//! interface for all packet types (tasks and notifications) in the system.
//! This trait enables generic implementations of thread pools and queues
//! that can handle different packet types uniformly.
//!
//! # Purpose
//!
//! The `Packet` trait allows the thread pool implementation to be
//! generalized, enabling a single implementation to handle both tasks
//! and notifications. This reduces code duplication and improves
//! maintainability.
//!
//! # Trait Bounds
//!
//! - `Send`: Packets can be transferred between threads
//! - `Sync`: Packets can be shared between threads
//! - `'static`: Packets have no non-'static references

use crate::{graphs::callsite::TaskCallSite, res::TaskResult};

/// Common trait for all packet types in the Thryve system.
///
/// The `Packet` trait serves as the interface for tasks and notifications,
/// enabling generic implementations of thread pools and queues. This trait
/// allows the system to handle different packet types uniformly.
///
/// # Trait Bounds
///
/// - `Send`: Packets can be transferred between threads
/// - `Sync`: Packets can be shared between threads
/// - `'static`: Packets have no non-'static references
///
/// # Example
///
/// ```no_run
/// use thryve_utils::traits::packets::Packet;
/// use thryve_utils::res::TaskResult;
///
/// pub struct MyPacket;
///
/// impl Packet for MyPacket {
///     fn process(self) -> Option<(u64, TaskResult)> {
///         Some((42, TaskResult::new("Ok", "Done".to_string(), "Main")))
///     }
/// }
/// ```
pub trait Packet: Send + Sync + 'static {
    /// Processes the packet and returns a result.
    ///
    /// This method provides the execution logic for packets. For tasks,
    /// it returns the task ID and result. For notifications, the default
    /// implementation returns `None` since notifications don't produce
    /// results in the same way.
    ///
    /// # Returns
    ///
    /// - `Some((id, result))`: For tasks, the ID and execution result
    /// - `None`: Default for notifications or if processing is not applicable
    ///
    /// # Example
    ///
    /// ```ignore
    /// use thryve_utils::traits::packets::Packet;
    /// use thryve_utils::res::TaskResult;
    ///
    /// // Task implementation returns Some
    /// let result = my_task.process();
    /// assert!(result.is_some());
    ///
    /// // Notification implementation returns None (default)
    /// let result = my_notification.process();
    /// assert!(result.is_none());
    /// ```
    fn process(self) -> Option<(u64, TaskResult)>
    where
        Self: Sized,
    {
        None
    }

    /// Sets the call site information for this packet.
    ///
    /// This method is used by the graph visualization system to attach
    /// call site information to packets. The call site includes the
    /// file path, line number, and fingerprints needed
    /// to build the topology graph.
    ///
    /// # Arguments
    ///
    /// * `callsite` - The call site information to attach
    ///
    /// # Default Implementation
    ///
    /// The default implementation does nothing. Types that need call
    /// site tracking should override this method.
    fn set_callsite(&mut self, _callsite: TaskCallSite) {}

    /// Returns the call site information for this packet.
    ///
    /// This method provides access to call site information that was
    /// attached via [`set_callsite`](Self::set_callsite). This is used
    /// by the graph visualization system to track task creation points.
    ///
    /// # Returns
    ///
    /// The call site information if available, or a default empty
    /// `TaskCallSite`.
    ///
    /// # Default Implementation
    ///
    /// The default implementation returns an empty `TaskCallSite`.
    /// Types that support call site tracking should override this method.
    fn get_callsite(&self) -> TaskCallSite {
        TaskCallSite::default()
    }
}
