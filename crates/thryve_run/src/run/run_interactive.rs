// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Interactive mode execution for the Thryve engine.
//!
//! This module provides functionality for running the Thryve engine in interactive mode,
//! which is the primary execution path when the application is launched manually
//! (e.g., double-clicking an executable or running from a command prompt).
//!
//! # Overview
//!
//! The interactive mode handles the complete lifecycle of the engine:
//!
//! - **Initialization**: Sets up communication channels and creates the execution context
//! - **Engine startup**: Sets up the engine with observers and plugins, through the shared
//!   startup sequence ([`EngineInitialization`])
//! - **Main loop**: Processes notifications and tasks until a `Fin` task ends it
//! - **Cleanup**: Performs finalization before exit
//!
//! # Key Functions
//!
//! - [`summon_requirements`] - Creates the communication channels and context required for execution
//! - [`run_interactive`] - The main entry point for interactive mode execution
//!
//! # Example
//!
//! ```no_run
//! use thryve_utils::cfg::config::Config;
//! use thryve_run::run::run_interactive::{summon_requirements, run_interactive};
//!
//! let config = Config::default();
//! let ctx = summon_requirements(config);
//! let handle = run_interactive(ctx)?;
//! let _ = handle.join();
//! # Ok::<(), thryve_utils::errors::ThryveError>(())
//! ```

use crossbeam::channel::{bounded, unbounded};
use thryve_core::runtime::engine::Engine;
use thryve_utils::errors::ThryveError;

use crate::init::init::EngineInitialization;
use std::thread::JoinHandle;
use thryve_utils::cfg::{config::Config, ctx::Ctx};
use thryve_utils::packets::packet_handle::PacketHandle;

/// Runs the Thryve engine in interactive mode.
///
/// This is the primary entry point for running the engine when launched manually
/// (e.g., double-clicking an executable or running from a command prompt).
///
/// All the setup happens on the caller's thread, before this function returns, through the
/// shared startup sequence ([`EngineInitialization`]):
///
/// 1. Spawns the engine thread, which waits for its engine
/// 2. Creates the log folders and files, and installs the panic hook
/// 3. Starts the worker threads, the logger and, in graph mode, the topology handler
/// 4. Loads all configured observers and plugins
/// 5. Starts the observer sorter thread for notification routing
/// 6. Hands the engine to its thread, which applies any debugging wait time and then runs the
///    main loop
///
/// The main loop processes notifications and tasks until a task result of type `Fin` (or
/// `FinErr`) finalizes the engine. The engine thread then ends, so joining the returned handle
/// waits for the engine to stop.
///
/// # Arguments
///
/// * `ctx` - The context containing configuration, observers, and plugins to load
///
/// # Returns
///
/// The engine thread's handle.
///
/// # Errors
///
/// Returns the error of the first startup step that fails. See
/// [`EngineInitialization::prepare`] for the list. A failed start stops every thread it started,
/// and writes its error to `Error.log` once the log folder exists.
///
/// # Examples
///
/// ```no_run
/// use thryve_utils::cfg::config::Config;
/// use thryve_run::run::run_interactive::{run_interactive, summon_requirements};
///
/// let ctx = summon_requirements(Config::load_def_config());
/// let handle = run_interactive(ctx)?;
/// let _ = handle.join();
/// # Ok::<(), thryve_utils::errors::ThryveError>(())
/// ```
pub fn run_interactive(ctx: Ctx) -> Result<JoinHandle<()>, ThryveError> {
    EngineInitialization::prepare(ctx, engine_loop)?.complete_and_run()
}

/// Interactive mode's loop, run on the engine thread.
///
/// Processes tasks and notifications until the engine's exit flag is set. Only
/// [`Engine::finalize`] sets it, and a `Fin` task result calls that, so the loop needs no
/// finalize of its own.
fn engine_loop(mut eng: Engine) {
    while !eng.exit_flag {
        eng.run_once();
    }
}

/// Creates the communication channels and context required for engine execution.
///
/// This function sets up the bounded channels for task and notification communication
/// based on the capacities specified in the provided configuration, then constructs
/// and returns a fully initialized [`Ctx`] instance.
///
/// # Arguments
///
/// * `config` - The configuration containing pipe capacity settings and other parameters
///
/// # Returns
///
/// A new [`Ctx`] instance containing:
/// - The provided configuration
/// - A bounded receiver for tasks
/// - A bounded receiver for notifications
/// - A [`PacketHandle`] with the corresponding transmitters
///
/// # Panics
///
/// Panics if the channel creation fails due to invalid capacity values.
///
/// # Examples
///
/// ```no_run
/// use thryve_utils::cfg::config::Config;
/// use thryve_run::run::run_interactive::summon_requirements;
///
/// let config = Config::default();
/// let ctx = summon_requirements(config);
/// ```
pub fn summon_requirements(config: Config) -> Ctx {
    let (task_tx, task_rx) = bounded(config.pipes.task_pipe_capacity);
    let (notif_tx, notif_rx) = bounded(config.pipes.notif_pipe_capacity);
    let (status_tx, status_rx) = unbounded();

    if config.debugging.is_debugging {
        Ctx::new(
            config,
            task_rx,
            notif_rx,
            status_rx,
            PacketHandle::new(task_tx, notif_tx, status_tx, config.debugging.visual_graph),
        )
    } else {
        Ctx::new(
            config,
            task_rx,
            notif_rx,
            status_rx,
            PacketHandle::new(task_tx, notif_tx, status_tx, false),
        )
    }
}
