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

use crate::impl_as_any;
use crate::traits::notif_logic::NotifLogic;
use crate::extras::named_input::NamedInput;
use std::time::SystemTime;

/// Core notification structure for the Thryve notification system.
///
/// The `Notif` struct serves as the fundamental unit of communication in the
/// Thryve observer system. Notifications are messages that flow through the
/// system, carrying information about events, state changes, and other
/// significant occurrences.
///
/// # Fields
///
/// - `notif_type`: Type identifier for categorizing the notification
/// - `notif_receiver`: Receiver identifier for routing to the appropriate observer
/// - `notif_core`: Core logic implementing custom notification behavior
/// - `timestamp`: Time when the notification was created
///
/// # Example
///
/// ```no_run
/// // Notifications are typically created by implementing NotifLogic and
/// // passing the core logic to Notif::new along with type and receiver identifiers
/// ```
pub struct Notif {
    /// Type identifier for categorizing the notification.
    pub notif_type: NamedInput,
    /// Receiver identifier for routing to the appropriate observer.
    pub notif_receiver: NamedInput,
    /// Core logic implementing custom notification behavior.
    pub notif_core: Box<dyn NotifLogic>,
    /// Time when the notification was created.
    pub timestamp: SystemTime,
}

impl NotifLogic for Notif {
    impl_as_any!(Notif);
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
    pub fn new(
        notif_type: &str,
        notif_receiver: &str,
        notif_core: Box<dyn NotifLogic>,
    ) -> Self {
        Self {
            notif_type: NamedInput::new(notif_type),
            notif_receiver: NamedInput::new(notif_receiver),
            notif_core,
            timestamp: SystemTime::now(),
        }
    }
}
