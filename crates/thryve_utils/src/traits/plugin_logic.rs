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

use crate::errors::ThryveError;

/// Marker trait for plugin objects.
///
/// This trait indicates that a struct is a plugin object and can be
/// used as a plugin in the system. It provides the cleanup interface
/// for graceful plugin shutdown.
///
/// # Example
///
/// ```no_run
/// use thryve_utils::errors::ThryveError;
/// use thryve_utils::traits::plugin_logic::PluginB;
///
/// pub struct MyPlugin;
///
/// impl PluginB for MyPlugin {
///     fn clean_up(&self) -> Result<(), ThryveError> {
///         // Cleanup logic here
///         Ok(())
///     }
/// }
/// ```
pub trait PluginB {
    /// Gracefully shuts down the plugin.
    ///
    /// Called once when the engine shuts down, after every task has stopped. Implementors should
    /// release resources, flush buffers, and perform any other termination tasks.
    ///
    /// # Errors
    ///
    /// Returns an error if the cleanup failed. See [`PlgLogic::clean_up`].
    ///
    /// # Example
    ///
    /// ```ignore
    /// fn clean_up(&self) -> Result<(), ThryveError> {
    ///     // Release resources
    ///     // Flush buffers
    ///     // Close connections
    ///     Ok(())
    /// }
    /// ```
    fn clean_up(&self) -> Result<(), ThryveError>;
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
/// use thryve_utils::errors::ThryveError;
/// use thryve_utils::traits::plugin_logic::PlgLogic;
///
/// pub struct MyPluginLogic;
///
/// impl PlgLogic for MyPluginLogic {
///     fn clean_up(&self) -> Result<(), ThryveError> {
///         // Cleanup logic here
///         Ok(())
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
    /// Called once when the engine shuts down, after every task has stopped. Implementors should
    /// release resources, flush buffers, and perform any other termination tasks.
    ///
    /// The plugins are cleaned up one after another on a thread of their own, while the engine
    /// keeps reading its mailboxes, so a `clean_up` may still send notifications.
    ///
    /// # Errors
    ///
    /// Return an error if the cleanup failed, usually
    /// [`FinalizationError::PluginCleanup`](crate::errors::FinalizationError::PluginCleanup) naming
    /// the plugin's type. Thryve writes it to `Error.log` and goes on with the other plugins and
    /// the shutdown.
    ///
    /// Return an error rather than panic: Thryve's panic hook ends the whole process, so a panic
    /// here skips every plugin still waiting for its `clean_up`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::any::Any;
    /// # use thryve_utils::errors::{FinalizationError, ThryveError};
    /// # use thryve_utils::traits::plugin_logic::PlgLogic;
    /// #
    /// # struct MyPluginLogic;
    /// # impl MyPluginLogic {
    /// #     fn flush_cache(&self) -> bool { true }
    /// # }
    /// # impl PlgLogic for MyPluginLogic {
    /// #     fn as_any(&self) -> &dyn Any { self }
    /// fn clean_up(&self) -> Result<(), ThryveError> {
    ///     if !self.flush_cache() {
    ///         return Err(FinalizationError::PluginCleanup {
    ///             plugin_type: "ImgCompression".to_string(),
    ///         }
    ///         .into());
    ///     }
    ///
    ///     Ok(())
    /// }
    /// # }
    /// ```
    fn clean_up(&self) -> Result<(), ThryveError>;

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
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// # use thryve_utils::traits::plugin_logic::PlgLogic;
    /// #
    /// # struct MyPluginLogic;
    /// # impl PlgLogic for MyPluginLogic {
    /// #     fn clean_up(&self) -> Result<(), thryve_utils::errors::ThryveError> { Ok(()) }
    /// #     fn as_any(&self) -> &dyn Any { self }
    /// # }
    /// #
    /// # let plg_core: Arc<dyn PlgLogic> = Arc::new(MyPluginLogic);
    /// // Downcasting example:
    /// let plg_as_any = plg_core.as_any();
    /// if let Some(concrete) = plg_as_any.downcast_ref::<MyPluginLogic>() {
    ///     // Access concrete type methods
    /// }
    /// ```
    fn as_any(&self) -> &dyn std::any::Any;
}
