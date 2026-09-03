// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Traits for plugin core logic.
//!
//! This module defines the traits that specify the interface for plugin
//! core logic implementations. Any struct implementing these traits can
//! serve as the core of a [`Plugin`](crate::bases::plugin::Plugin),
//! enabling polymorphic handling of different plugin types.
//!
//! # Traits
//!
//! - [`PluginB`]: Marker trait for plugin objects with cleanup
//! - [`PlgLogic`]: Core logic trait for plugin behavior
//!
//! # Purpose
//!
//! These traits allow the plugin system to be extensible without
//! modifying core code. New plugin types can be added by implementing
//! these traits, following the open/closed principle.

/// Marker trait for plugin objects.
///
/// This trait indicates that a struct is a plugin object and can be
/// used as a plugin in the system. It provides the cleanup interface
/// for graceful plugin shutdown.
///
/// # Example
///
/// ```no_run
/// use thryve_utils::traits::plugin_logic::PluginB;
///
/// pub struct MyPlugin;
///
/// impl PluginB for MyPlugin {
///     fn clean_up(&self) {
///         // Cleanup logic here
///     }
/// }
/// ```
pub trait PluginB {
    /// Gracefully shuts down the plugin.
    ///
    /// This method is called during plugin shutdown to perform
    /// any necessary cleanup. Implementors should release resources,
    /// flush buffers, and perform any other termination tasks.
    ///
    /// # Example
    ///
    /// ```no_run
    /// fn clean_up(&self) {
    ///     // Release resources
    ///     // Flush buffers
    ///     // Close connections
    /// }
    /// ```
    fn clean_up(&self);
}

/// Core logic trait for plugin behavior.
///
/// This trait specifies the interface for plugin core logic.
/// Any struct implementing `PlgLogic` can serve as the core of
/// a [`Plugin`](crate::bases::plugin::Plugin), enabling
/// polymorphic handling of different plugin types.
///
/// # Trait Bounds
///
/// - `Send`: Plugin logic can be transferred between threads
/// - `Sync`: Plugin logic can be shared between threads
///
/// # Example
///
/// ```no_run
/// use thryve_utils::traits::plugin_logic::PlgLogic;
///
/// pub struct MyPluginLogic;
///
/// impl PlgLogic for MyPluginLogic {
///     fn clean_up(&self) {
///         // Cleanup logic here
///     }
///
///     fn as_any(&self) -> &dyn std::any::Any {
///         self
///     }
/// }
/// ```
pub trait PlgLogic: Send + Sync {
    /// Gracefully shuts down the plugin.
    ///
    /// This method is called during plugin shutdown to perform
    /// any necessary cleanup. Implementors should release resources,
    /// flush buffers, and perform any other termination tasks.
    ///
    /// # Example
    ///
    /// ```no_run
    /// fn clean_up(&self) {
    ///     // Release resources
    ///     // Flush buffers
    ///     // Close connections
    /// }
    /// ```
    fn clean_up(&self);

    /// Returns a reference to `self` as `dyn Any`.
    ///
    /// This method enables downcasting to the concrete type after
    /// receiving a plugin through a trait object. It is essential
    /// for the plugin system to handle different plugin types
    /// polymorphically.
    ///
    /// # Returns
    ///
    /// A reference to the object as a dynamic `Any` type.
    ///
    /// # Example
    ///
    /// ```no_run
    /// // Downcasting example:
    /// let plg_as_any = plugin.plg_core.as_any();
    /// if let Some(concrete) = plg_as_any.downcast_ref::<MyPluginLogic>() {
    ///     // Access concrete type methods
    /// }
    /// ```
    fn as_any(&self) -> &dyn std::any::Any;
}
