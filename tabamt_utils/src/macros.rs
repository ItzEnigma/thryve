//! This module contains macros that are used throughout the framework.
//! These macros are designed to simplify common patterns and reduce boilerplate code.

/// Implements the `as_any` method for a given type, allowing it to be downcasted to a specific type later.
#[macro_export]
macro_rules! impl_as_any {
    ($t:ty) => {
        /// Returns a reference to the type as `&dyn Any`, allowing for downcasting to the specific type later.
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
    };
}