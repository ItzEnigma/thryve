//! Error types for thryve_run.
//!
//! This module provides error handling for the run/application layer.
//! It uses `anyhow` for contextual error wrapping at the application level,
//! allowing errors from the core and utils layers to be propagated with
//! additional context about where they occurred.
//!
//! # Design Decisions
//!
//! - **Contextual errors** use `anyhow` for rich error messages with backtrace
//! - **Service-level errors** are represented as `Result` types
//! - **Fatal errors** (double-start, invalid state) are handled with `expect()`
//!   with descriptive messages that explain the application invariant violation

use anyhow::Result;
use thiserror::Error;

/// Errors that can occur in the thryve_run application layer.
///
/// These are application-level errors that wrap errors from lower layers
/// and add contextual information about where the error occurred.
#[derive(Error, Debug)]
pub enum ThryveRunError {
    /// Error from the core runtime layer.
    #[error("Core runtime error: {0}")]
    Core(#[from] anyhow::Error),

    /// Service command error.
    #[error("Service command error: {0}")]
    ServiceCommand(String),

    /// Engine initialization failure.
    #[error("Engine initialization failed: {0}")]
    EngineInit(String),

    /// Thread join failure during shutdown.
    #[error("Thread join error: {0}")]
    ThreadJoin(String),
}

/// Type alias for Result with ThryveRunError.
pub type ThryveResult<T> = Result<T, ThryveRunError>;

/// Converts a service command error into a ThryveRunError.
impl From<String> for ThryveRunError {
    fn from(msg: String) -> Self {
        ThryveRunError::ServiceCommand(msg)
    }
}

/// Converts a string slice into a ThryveRunError.
impl From<&str> for ThryveRunError {
    fn from(msg: &str) -> Self {
        ThryveRunError::ServiceCommand(msg.to_string())
    }
}
