// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Trait for notification core logic.
//!
//! This module defines the [`NotifLogic`] trait, which specifies the
//! interface for notification core logic implementations. Any struct
//! implementing this trait can serve as the core of a [`Notif`](crate::bases::notif::Notif),
//! enabling polymorphic handling of different notification types.
//!
//! # Purpose
//!
//! The `NotifLogic` trait allows the notification system to be extensible
//! without modifying core code. New notification types can be added by
//! implementing this trait, following the open/closed principle.
//!
//! # Implementation
//!
//! Implementors must provide:
//! - The `as_any` method for downcasting to concrete types
//! - All methods from the [`Packet`](crate::traits::packets::Packet) trait

use crate::traits::packets::Packet;

/// Trait for notification core logic implementations.
///
/// This trait specifies the interface for notification core logic.
/// Any struct implementing `NotifLogic` can serve as the core of
/// a [`Notif`](crate::bases::notif::Notif), enabling polymorphic
/// handling of different notification types.
///
/// # Trait Bounds
///
/// - `Packet`: All notification logic must also implement the
///   [`Packet`](crate::traits::packets::Packet) trait
///
/// # Example
///
/// ```no_run
/// use thryve_utils::traits::notif_logic::NotifLogic;
///
/// pub struct MyNotifLogic {
///     // fields...
/// }
///
/// // `Packet` is implemented automatically via NotifLogic's blanket impl -
/// // implementing it explicitly here would conflict with that blanket impl.
/// impl NotifLogic for MyNotifLogic {
///     fn as_any(&self) -> &dyn std::any::Any {
///         self
///     }
/// }
/// ```
pub trait NotifLogic: Packet {
    /// Returns a reference to `self` as `dyn Any`.
    ///
    /// This method enables downcasting to the concrete type after
    /// receiving a notification through a trait object. It is essential
    /// for the observer system to handle different notification types
    /// polymorphically.
    ///
    /// # Returns
    ///
    /// A reference to the object as a dynamic `Any` type.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use thryve_utils::traits::notif_logic::NotifLogic;
    /// #
    /// # struct MyNotifLogic;
    /// # impl NotifLogic for MyNotifLogic {
    /// #     fn as_any(&self) -> &dyn std::any::Any { self }
    /// # }
    /// #
    /// # let notif_core: Arc<dyn NotifLogic> = Arc::new(MyNotifLogic);
    /// // Downcasting example in an observer:
    /// let notif_as_any = notif_core.as_any();
    /// if let Some(concrete) = notif_as_any.downcast_ref::<MyNotifLogic>() {
    ///     // Access concrete type methods
    /// }
    /// ```
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Blanket implementation of `Packet` for all `NotifLogic` types.
///
/// This ensures that any type implementing `NotifLogic` automatically
/// satisfies the `Packet` trait bound, simplifying implementations.
impl<T: NotifLogic> Packet for T {}
