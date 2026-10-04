// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Core notification structure for the Thryve notification system.
//!
//! This module provides the [`Notif`] struct, which serves as the fundamental
//! unit of communication in the Thryve observer system. Notifications are
//! messages that flow through the system, carrying information about events,
//! state changes, and other significant occurrences.
//!
//! # Architecture
//!
//! Each notification contains:
//! - A type identifier for categorizing the notification
//! - A receiver identifier for routing to the appropriate observer
//! - Core logic implementing the [`NotifLogic`] trait for custom behavior
//! - A timestamp indicating when the notification was created
//!
//! # Extensibility
//!
//! Any struct implementing the [`NotifLogic`] trait can serve as the core
//! of a notification, enabling flexible and extensible notification handling
//! throughout the system.
use chrono::{DateTime, Local};

use crate::errors::{NameInputType, NamingError};
use crate::extras::named_input::NamedInput;
use crate::graphs::callsite::TaskCallSite;
use crate::traits::notif_logic::NotifLogic;
use crate::traits::packets::Packet;
use std::fmt::Display;
use std::time::SystemTime;

/// Core notification structure for the Thryve notification system.
///
/// The `Notif` struct serves as the fundamental unit of communication in the
/// Thryve observer system. Notifications are messages that flow through the
/// system, carrying information about events, state changes, and other
/// significant occurrences.
///
/// # Graph Visualization
///
/// When graph visualization is enabled, notifications can carry call site
/// information via the [`callsite`](Notif::callsite) field. This allows
/// notifications to be traced back to their creation point in the source code,
/// similar to tasks.
///
/// # Fields
///
/// - `notif_type`: Type identifier for categorizing the notification
/// - `callsite`: Optional call site information for graph visualization
/// - `notif_receiver`: Receiver identifier for routing to the appropriate observer
/// - `notif_core`: Core logic implementing custom notification behavior
/// - `timestamp`: Time when the notification was created
///
/// # Example
///
/// A notification's core logic implements [`NotifLogic`]; [`Notif::new`] adds its type and the
/// observer it's for.
///
/// ```no_run
/// use thryve_utils::bases::notif::Notif;
/// use thryve_utils::impl_as_any;
/// use thryve_utils::traits::notif_logic::NotifLogic;
///
/// // A core with nothing more to carry.
/// struct Measurement;
///
/// impl NotifLogic for Measurement {
///     impl_as_any!(Measurement);
/// }
///
/// // A "Measurement", for the observer of type "Metrics".
/// let notif = Notif::new("Measurement", "Metrics", Box::new(Measurement))?;
/// # Ok::<(), thryve_utils::errors::NamingError>(())
/// ```
pub struct Notif {
    /// Type identifier for categorizing the notification.
    notif_type: NamedInput,
    /// Optional call site information for graph visualization.
    ///
    /// This field is populated when graph visualization is enabled
    /// and contains information about where this notification was created
    /// in the source code.
    pub callsite: Option<TaskCallSite>,
    /// Receiver identifier for routing to the appropriate observer.
    notif_receiver: NamedInput,
    /// Core logic implementing custom notification behavior.
    pub notif_core: Box<dyn NotifLogic>,
    /// Time when the notification was created.
    pub timestamp: SystemTime,
}

impl Display for Notif {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let datetime: DateTime<Local> = self.timestamp.into();
        write!(
            f,
            "notification type: {}, notification receiver: {}, creation time: {}",
            self.notif_type,
            self.notif_receiver,
            datetime.format("%Y-%m-%d %H:%M:%S"),
        )
    }
}

impl Packet for Notif {
    /// Sets the call site information for this notification.
    ///
    /// This method is called by the packet handling system when graph
    /// visualization is enabled. It attaches call site information
    /// (file path, line number, fingerprints) to the
    /// notification for topology graph construction.
    ///
    /// # Arguments
    ///
    /// * `callsite` - The call site information to attach to this notification
    fn set_callsite(&mut self, callsite: TaskCallSite) {
        self.callsite = Some(callsite);
    }

    /// Returns the call site information for this notification.
    ///
    /// This method provides access to the call site information that
    /// was attached when graph visualization is enabled.
    ///
    /// # Returns
    ///
    /// The call site information if available, or a default empty
    /// `TaskCallSite` if no call site was captured.
    fn get_callsite(&self) -> TaskCallSite {
        self.callsite.clone().unwrap_or_default()
    }
}

impl Notif {
    /// Creates a new notification with the specified configuration.
    ///
    /// Initializes a new notification instance with the given type, receiver,
    /// and core logic. The notification is automatically timestamped with the
    /// current system time.
    ///
    /// # Arguments
    ///
    /// * `notif_type` - Type identifier for categorizing the notification
    /// * `notif_receiver` - Receiver identifier for routing to the appropriate observer
    /// * `notif_core` - Core logic implementing custom notification behavior
    ///
    /// # Returns
    ///
    /// A new `Notif` instance configured with the provided parameters.
    ///
    /// # Errors
    ///
    /// Returns [`NamingError::NameEmpty`] if `notif_type` or `notif_receiver` is empty, or
    /// [`NamingError::NameContainsControlChar`] if one of them contains a control character. The
    /// error's kind (`NameInputType::NotifType` or `NameInputType::NotifReceiver`) says which one.
    pub fn new(
        notif_type: &str,
        notif_receiver: &str,
        notif_core: Box<dyn NotifLogic>,
    ) -> Result<Self, NamingError> {
        Ok(Self {
            notif_type: NamedInput::new(notif_type, NameInputType::NotifType)?,
            callsite: None,
            notif_receiver: NamedInput::new(notif_receiver, NameInputType::NotifReceiver)?,
            notif_core,
            timestamp: SystemTime::now(),
        })
    }

    /// Returns the size of the notification struct in bytes.
    ///
    /// Heap memory owned by its fields, such as the names and the core logic, isn't included.
    pub fn size_in_bytes(&self) -> usize {
        std::mem::size_of_val(self)
    }

    /// Returns the notification's type.
    pub fn notif_type(&self) -> &str {
        self.notif_type.inner()
    }

    /// Returns the type of the observer this notification is routed to.
    pub fn notif_receiver(&self) -> &str {
        self.notif_receiver.inner()
    }
}

impl Notif {
    /// Creates one of Thryve's own notifications from names that are already checked.
    ///
    /// Thryve's notifications use fixed names, built with `NamedInput`'s no-text constructors, so
    /// this can't fail:
    ///
    /// - log notifications from [`Log::log`](crate::packets::notif_log::Log::log) and
    ///   [`Log::err`](crate::packets::notif_log::Log::err): type `Log` or `Error`, receiver
    ///   `Logging`;
    /// - topology notifications from
    ///   [`TopologyModifier`](crate::packets::notif_topology::TopologyModifier): type `AddNode` or
    ///   `AddEdge`, receiver `TopologyHandler`.
    ///
    /// Use [`Notif::new`] for any other notification: it checks the names and returns an error for
    /// a bad one.
    ///
    /// # Arguments
    ///
    /// * `notif_type` - The notification's type
    /// * `notif_receiver` - The type of the observer it's routed to
    /// * `notif_core` - Core logic implementing the notification's behavior
    pub fn new_internal(
        notif_type: NamedInput,
        notif_receiver: NamedInput,
        notif_core: Box<dyn NotifLogic>,
    ) -> Self {
        Self {
            notif_type,
            callsite: None,
            notif_receiver,
            notif_core,
            timestamp: SystemTime::now(),
        }
    }
}
