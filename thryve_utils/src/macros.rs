//! Macros for the Thryve system.
//!
//! This module contains macros that simplify common patterns and
//! reduce boilerplate code throughout the framework.
//!
//! # Macros
//!
//! - [`impl_as_any!`]: Implements the `as_any` method for downcasting

/// Implements the `as_any` method for a given type.
///
/// This macro generates an implementation of the `as_any` method
/// that returns `self` as `&dyn Any`, enabling downcasting to
/// the concrete type later.
///
/// # Usage
///
/// Place this macro inside a trait implementation block:
///
/// ```no_run
/// use thryve_utils::impl_as_any;
///
/// pub struct MyType;
///
/// pub trait MyTrait {
///     fn as_any(&self) -> &dyn std::any::Any;
/// }
///
/// impl MyTrait for MyType {
///     impl_as_any!(MyType);
/// }
/// ```
///
/// This expands to:
///
/// ```no_run
/// fn as_any(&self) -> &dyn std::any::Any {
///     self
/// }
/// ```
///
/// # Downcasting Example
///
/// ```no_run
/// let my_trait_obj: &dyn MyTrait = &MyType;
/// let as_any = my_trait_obj.as_any();
/// if let Some(concrete) = as_any.downcast_ref::<MyType>() {
///     // Access concrete type
/// }
/// ```
#[macro_export]
macro_rules! impl_as_any {
    ($t:ty) => {
        /// Returns a reference to the type as `&dyn Any`, allowing for downcasting to the specific type later.
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    };
}
