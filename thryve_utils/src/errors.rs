//! Error types for thryve_utils.
//!
//! This module provides the [`ThryveUtilsError`] enum, which represents errors
//! that can occur in the utility layer. Errors that are truly unrecoverable
//! (e.g., thread spawn failure, missing engine state) use `panic!()`, while
//! recoverable errors (e.g., IO failures, config errors) are represented
//! here as `Result` types.
//!
//! # Design Decisions
//!
//! - **Recoverable errors** are represented as `Result<_, ThryveUtilsError>`:
//!   - File I/O failures (log files, config)
//!   - Directory creation failures
//!   - Channel send failures (non-fatal)
//!   - Observer/plugin not found in HashMap
//! - **Fatal errors** are handled with `panic!()`:
//!   - Thread spawn failures (no way to continue without threads)
//!   - Missing sender in thread pool (indicates program logic error)
//!   - Missing channel receivers (indicates engine is in an invalid state)

use thiserror::Error;

/// Errors that can occur in the thryve_utils layer.
///
/// These are recoverable errors that should be propagated up through
/// the application and handled appropriately by the caller.
#[derive(Error, Debug)]
pub enum ThryveUtilsError {
    /// File or directory I/O error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// TOML configuration parsing error.
    #[error("Configuration parse error: {0}")]
    ConfigParse(#[from] toml::de::Error),

    /// Channel send failure (non-fatal, can be logged).
    #[error("Channel send failed: {0}")]
    ChannelSend(String),

    /// Channel receive failure (non-fatal, can be logged).
    #[error("Channel receive failed: {0}")]
    ChannelReceive(String),

    /// Observer or plugin not found in registry.
    #[error("Observer/plugin not found: {0}")]
    NotFound(String),

    /// Invalid operation due to missing required data.
    #[error("Invalid operation: {0}")]
    InvalidOperation(String),

    /// Mutex lock failure (indicates poisoned mutex).
    #[error("Mutex poisoned: {0}")]
    MutexPoisoned(String),

    /// Directory creation failure.
    #[error("Directory creation failed: {0}")]
    DirectoryCreation(String),

    /// Invalid UTF-8 conversion.
    #[error("Invalid UTF-8: {0}")]
    InvalidUtf8(String),
}


