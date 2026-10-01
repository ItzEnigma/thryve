// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Windows service mode execution for the Thryve engine.
//!
//! This module provides functionality for running the Thryve engine as a Windows service,
//! enabling the application to run in the background with system-level privileges and
//! automatic startup capabilities.
//!
//! # Overview
//!
//! The service mode provides two key operations:
//!
//! - **Service creation**: Constructs a [`Thryve`] service instance with the given configuration
//! - **Service execution**: Runs the service with the provided context
//!
//! # Key Functions
//!
//! - [`create_service`] - Creates a new Thryve service instance with the provided configuration
//! - [`try_run_as_service`] - Runs the Thryve service with the provided context
//!
//! # Example
//!
//! ```no_run
//! use thryve_utils::cfg::config::Config;
//! use thryve_run::run::run_service::{create_service, try_run_as_service};
//!
//! let config = Config::default();
//! let (service, ctx) = create_service(config);
//! try_run_as_service(service, ctx)?;
//! # Ok::<(), thryve_utils::errors::ThryveError>(())
//! ```

use crate::service::service::Thryve;
use thryve_utils::{
    cfg::{config::Config, ctx::Ctx},
    errors::ThryveError,
};

/// Creates a new Thryve service instance with the provided configuration.
///
/// This function constructs and initializes a [`Thryve`] service ready for execution
/// as a Windows service. It packages the service instance together with the context
/// required for its operation. Nothing is created on disk yet: the folders and log files are
/// created when the service runs.
///
/// # Arguments
///
/// * `config` - The configuration to use for initializing the service
///
/// # Returns
///
/// A tuple containing:
/// - A newly created [`Thryve`] service instance
/// - The context required for service execution
///
/// # Examples
///
/// ```no_run
/// use thryve_utils::cfg::config::Config;
/// use thryve_run::run::run_service::create_service;
///
/// let config = Config::default();
/// let (service, ctx) = create_service(config);
/// ```
pub fn create_service<'a>(config: Config) -> (Thryve<'a>, Ctx) {
    Thryve::new(config)
}

/// Runs the Thryve service with the provided context.
///
/// This is the primary entry point for executing the application as a Windows service. It
/// hands the service and the context to [`Thryve::run_service`], which sets the engine up,
/// connects to the service manager and blocks until the service stops.
///
/// # Arguments
///
/// * `svc` - The initialized Thryve service instance
/// * `ctx` - The context containing configuration and communication channels
///
/// # Errors
///
/// Returns the error of a failed setup (see
/// [`EngineInitialization::prepare`](crate::init::init::EngineInitialization::prepare)), or
/// [`StartupError::Service`](thryve_utils::errors::StartupError::Service) if the process wasn't
/// started by the service manager. Either way, every thread the setup started is stopped, and
/// the error is written to `Error.log` once the log folder exists.
///
/// # Examples
///
/// ```no_run
/// use thryve_utils::cfg::config::Config;
/// use thryve_run::run::run_service::{create_service, try_run_as_service};
///
/// let config = Config::default();
/// let (service, ctx) = create_service(config);
/// try_run_as_service(service, ctx)?;
/// # Ok::<(), thryve_utils::errors::ThryveError>(())
/// ```
pub fn try_run_as_service(mut svc: Thryve, ctx: Ctx) -> Result<(), ThryveError> {
    svc.run_service(ctx)
}
