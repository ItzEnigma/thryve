// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Error types for thryve_core.
//!
//! This module provides the [`ThryveCoreError`] enum, which represents errors
//! that can occur in the core runtime layer. It composes errors from thryve_utils
//! and adds engine-specific errors.
//!
//! # Design Decisions
//!
//! - **Recoverable errors** are represented as `Result<_, ThryveCoreError>`:
//!   - Observer plugin load failures
//!   - Engine startup failures
//!   - Task result validation errors
//! - **Fatal errors** are handled with `panic!()`:
//!   - Engine runtime state corruption
//!   - Missing required observer state

use thiserror::Error;
use thryve_utils::errors::ThryveUtilsError;

/// Errors that can occur in the thryve_core runtime layer.
///
/// These are recoverable errors that should be propagated up through
/// the application and handled appropriately by the caller.
#[derive(Error, Debug)]
pub enum ThryveCoreError {
    /// Error originating from the utils layer.
    #[error("Utils error: {0}")]
    Utils(#[from] ThryveUtilsError),

    /// Error from the observer manager.
    #[error("Observer manager error: {0}")]
    ObserverManager(String),

    /// Error from the plugin manager.
    #[error("Plugin manager error: {0}")]
    PluginManager(String),

    /// Error from the engine runtime.
    #[error("Engine runtime error: {0}")]
    EngineRuntime(String),

    /// Error from task execution.
    #[error("Task execution error: {0}")]
    TaskExecution(String),

    /// Error from result validation.
    #[error("Result validation error: {0}")]
    ResultValidation(String),
}
