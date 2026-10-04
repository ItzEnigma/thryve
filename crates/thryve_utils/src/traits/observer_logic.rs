// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Traits for observer core logic.
//!
//! This module defines the traits that specify the interface for observer
//! core logic implementations. Any struct implementing these traits can
//! serve as the core of an [`Observer`],
//! enabling polymorphic handling of different observer types.
//!
//! # Traits
//!
//! - [`ObsB`]: Marker trait for observer objects
//! - [`ObsLogic`]: Core logic trait for observer behavior
//!
//! # Purpose
//!
//! These traits allow the observer system to be extensible without
//! modifying core code. New observer types can be added by implementing
//! these traits, following the open/closed principle.

use std::sync::Arc;

use crate::bases::observer::Observer;

/// Marker trait for observer objects.
///
/// This trait indicates that a struct is an observer object and can be
/// used as an observer in the system. It enforces thread safety by
/// requiring `Send` and `Sync` bounds.
///
/// # Trait Bounds
///
/// - `Send`: Observer objects can be transferred between threads
/// - `Sync`: Observer objects can be shared between threads
///
/// # Example
///
/// ```no_run
/// use thryve_utils::traits::observer_logic::ObsB;
///
/// pub struct MyObserver;
///
/// impl ObsB for MyObserver {}
/// ```
pub trait ObsB: Send + Sync {}

/// Core logic trait for observer behavior.
///
/// This trait specifies the interface for observer core logic.
/// Any struct implementing `ObsLogic` can serve as the core of
/// an [`Observer`], enabling
/// polymorphic handling of different observer types.
///
/// # Trait Bounds
///
/// - `Send`: Observer logic can be transferred between threads
/// - `Sync`: Observer logic can be shared between threads
/// - `'static`: Observer logic has no non-'static references
///
/// # Example
///
/// ```no_run
/// use thryve_utils::traits::observer_logic::ObsLogic;
/// use thryve_utils::bases::observer::Observer;
/// use std::sync::Arc;
///
/// pub struct MyObserverLogic;
///
/// impl ObsLogic for MyObserverLogic {
///     fn clean_up(&self, observer: Arc<Observer>) {
///         // Cleanup logic here
///     }
///
///     fn as_any(&self) -> &dyn std::any::Any {
///         self
///     }
/// }
/// ```
pub trait ObsLogic: Send + Sync + 'static {
    /// Gracefully shuts down the observer.
    ///
    /// This method is called during observer shutdown to perform
    /// any necessary cleanup. Implementors should release resources,
    /// flush buffers, and perform any other termination tasks.
    ///
    /// # Arguments
    ///
    /// * `observer` - The observer being shut down
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::any::Any;
    /// # use std::sync::Arc;
    /// # use thryve_utils::bases::observer::Observer;
    /// # use thryve_utils::traits::observer_logic::ObsLogic;
    /// #
    /// # struct MyObserverLogic;
    /// # impl ObsLogic for MyObserverLogic {
    /// fn clean_up(&self, observer: Arc<Observer>) {
    ///     // Flush any pending notifications
    ///     while let Some(notif) = observer.obs_queue.main_queue.pop() {
    ///         // Process final notifications
    ///     }
    /// }
    /// #     fn as_any(&self) -> &dyn Any { self }
    /// # }
    /// ```
    fn clean_up(&self, observer: Arc<Observer>);

    /// Returns a reference to `self` as `dyn Any`.
    ///
    /// This method enables downcasting to the concrete type after
    /// receiving an observer through a trait object. It is essential
    /// for the observer system to handle different observer types
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
    /// # use thryve_utils::bases::observer::Observer;
    /// # use thryve_utils::traits::observer_logic::ObsLogic;
    /// #
    /// # struct MyObserverLogic;
    /// # impl ObsLogic for MyObserverLogic {
    /// #     fn clean_up(&self, _observer: Arc<Observer>) {}
    /// #     fn as_any(&self) -> &dyn Any { self }
    /// # }
    /// #
    /// # let obs_core: Arc<dyn ObsLogic> = Arc::new(MyObserverLogic);
    /// // Downcasting example:
    /// let obs_as_any = obs_core.as_any();
    /// if let Some(concrete) = obs_as_any.downcast_ref::<MyObserverLogic>() {
    ///     // Access concrete type methods
    /// }
    /// ```
    fn as_any(&self) -> &dyn std::any::Any;
}
