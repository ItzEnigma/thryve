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
//! try_run_as_service(service, ctx);
//! ```

use crate::{init::init, service::service::Thryve};
use thryve_utils::{cfg::{config::Config, ctx::Ctx}, extras};

/// Creates a new Thryve service instance with the provided configuration.
///
/// This function constructs and initializes a [`Thryve`] service ready for execution
/// as a Windows service. It packages the service instance together with the context
/// required for its operation. Required directories are created.
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
pub fn create_service<'a> (config: Config) -> (Thryve<'a> , Ctx) {
    let (svc, mut ctx) = Thryve::new(config);
    init::init_directories(&mut ctx);
    extras::panic_hook::init_panic_hook();

    (svc , ctx)
}

/// Runs the Thryve service with the provided context.
///
/// This is the primary entry point for executing the application as a Windows service.
/// The function delegates the actual service execution to the provided [`Thryve`] instance.
///
/// # Arguments
///
/// * `svc` - The initialized Thryve service instance
/// * `ctx` - The context containing configuration and communication channels
///
/// # Examples
///
/// ```no_run
/// use thryve_utils::cfg::config::Config;
/// use thryve_run::run::run_service::{create_service, try_run_as_service};
///
/// let config = Config::default();
/// let (service, ctx) = create_service(config);
/// try_run_as_service(service, ctx);
/// ```
pub fn try_run_as_service(mut svc: Thryve, ctx: Ctx){
    svc.run_service(ctx);
}