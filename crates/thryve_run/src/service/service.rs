// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Windows service implementation for the Thryve engine.
//!
//! This module provides the core functionality for running the Thryve engine as a
//! Windows service, enabling background execution with system-level integration.
//!
//! # Overview
//!
//! The [`Thryve`] struct encapsulates a Windows service instance and manages its
//! lifecycle, including start, stop, pause, and resume operations. It coordinates
//! with the Thryve engine to process tasks and notifications while responding to
//! service control manager commands.
//!
//! # Service Lifecycle
//!
//! 1. **Creation**: A new [`Thryve`] instance is created with a [`Config`]
//! 2. **Setup**: The engine is set up through the shared startup sequence
//!    ([`EngineInitialization`]) before the service connects to anything, so a failed setup is
//!    returned as an error
//! 3. **Registration**: The service registers with the Windows Service Control Manager
//! 4. **Execution**: The service listens for commands and manages the engine lifecycle
//! 5. **Termination**: The service gracefully shuts down the engine on stop commands
//!
//! # Supported Commands
//!
//! - **Start**: Hands the prepared engine to its thread, which starts processing. A second
//!   `Start` is logged and ignored
//! - **Stop**: Gracefully shuts down the engine and terminates the service
//! - **Pause**: Suspends engine processing while maintaining state
//! - **Resume**: Resumes engine processing after a pause
//!
//! # Example
//!
//! ```no_run
//! use thryve_utils::cfg::config::Config;
//! use thryve_run::service::service::Thryve;
//!
//! let config = Config::default();
//! let (mut service, ctx) = Thryve::new(config);
//! service.run_service(ctx)?;
//! # Ok::<(), thryve_utils::errors::ThryveError>(())
//! ```

use crossbeam::channel::{bounded, unbounded};
use std::sync::{
    Arc, Mutex, PoisonError,
    atomic::{AtomicBool, Ordering},
};
use thryve_core::runtime::engine::Engine;
use windows_services::{Command, Service};

use thryve_utils::packets::notif_log::Log;
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::{
    cfg::{config::Config, ctx::Ctx},
    errors::{StartupError, ThryveError},
};

use crate::init::init::EngineInitialization;

/// A Windows service wrapper for the Thryve engine.
///
/// This struct encapsulates a Windows service instance and manages the lifecycle
/// of the Thryve engine when running as a service. It handles service control
/// commands (start, stop, pause, resume) and coordinates engine execution.
///
/// # Fields
///
/// - `is_running`: Atomic flag indicating whether the service is currently running
/// - `is_paused`: Atomic flag indicating whether the service is currently paused
/// - `service_instance`: The underlying Windows service instance
///
/// # Lifetime Parameter
///
/// - `'a`: The lifetime of the service instance, tied to the Windows service handle
///
/// # Example
///
/// ```no_run
/// use thryve_utils::cfg::config::Config;
/// use thryve_run::service::service::Thryve;
///
/// let config = Config::default();
/// let (mut service, ctx) = Thryve::new(config);
/// service.run_service(ctx)?;
/// # Ok::<(), thryve_utils::errors::ThryveError>(())
/// ```
pub struct Thryve<'a> {
    /// Whether the engine should keep running. Clearing it stops the engine loop.
    pub is_running: Arc<AtomicBool>,
    /// Whether the engine should be paused. The engine loop pauses or resumes to match it.
    pub is_paused: Arc<AtomicBool>,
    /// The Windows service this wraps.
    pub service_instance: Service<'a>,
}

impl<'a> Thryve<'a> {
    /// Creates a new Thryve service instance with the given configuration.
    ///
    /// This constructor initializes the Windows service with the specified capabilities
    /// (pause/stop) based on the configuration, creates the communication channels
    /// for task and notification pipes, and returns both the service instance and
    /// the context required for engine execution.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration containing service capabilities and pipe settings
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A new [`Thryve`] service instance configured with the specified capabilities
    /// - A [`Ctx`] instance with initialized communication channels. Its packet handle refuses
    ///   tasks and notifications until [`run_service`](Self::run_service) has made the engine
    ///   (see [`PacketHandle::refuse_sends`]); send from the plugins' init tasks, or from other
    ///   threads once the service runs
    ///
    /// # Service Capabilities
    ///
    /// The service capabilities are configured based on the `config.service` settings:
    /// - If `can_pause` is true, the service supports pause/resume commands
    /// - If `can_stop` is true, the service supports stop commands
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::config::Config;
    /// use thryve_run::service::service::Thryve;
    ///
    /// let config = Config::default();
    /// let (service, ctx) = Thryve::new(config);
    /// ```
    pub fn new(config: Config) -> (Self, Ctx) {
        // Create the channels that will be used to send tasks and notifications to the engine.
        let (task_tx, task_rx) = bounded(config.pipes.task_pipe_capacity);
        let (notif_tx, notif_rx) = bounded(config.pipes.notif_pipe_capacity);
        let (status_tx, status_rx) = unbounded();

        // Initialize the service struct with the created channels and the service instance.
        let mut svc: Thryve<'a> = Thryve {
            is_running: Arc::new(AtomicBool::new(false)),
            is_paused: Arc::new(AtomicBool::new(false)),
            service_instance: Service::new(),
        };

        // Set the service capabilities based on the configuration.
        if config.service.can_pause {
            svc.service_instance.can_pause();
        }

        if config.service.can_stop {
            svc.service_instance.can_stop();
        }

        // Create the context that will be passed to the engine, containing the configuration, channels, and packet handle.
        let ctx = if config.debugging.is_debugging {
            let packet_handle =
                PacketHandle::new(task_tx, notif_tx, status_tx, config.debugging.visual_graph);
            packet_handle.refuse_sends();

            Ctx::new(config.clone(), task_rx, notif_rx, status_rx, packet_handle)
        } else {
            let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
            packet_handle.refuse_sends();

            Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle)
        };

        // Return the service instance and the context.
        (svc, ctx)
    }

    /// Runs the Thryve service and manages its execution lifecycle.
    ///
    /// This is the primary entry point for service execution. It sets the engine up first,
    /// then registers the service with the Windows Service Control Manager and blocks, listening
    /// for control commands, until the service stops. The engine runs in a dedicated thread to
    /// ensure the service remains responsive to control commands at all times.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context containing configuration, observers, plugins, and communication channels
    ///
    /// # Engine Lifecycle
    ///
    /// Before the service connects to the service manager, the shared startup sequence
    /// ([`EngineInitialization::prepare`]) sets the engine up on this thread:
    /// 1. Spawns the engine thread, which waits for its engine
    /// 2. Creates the log folders and files, and installs the panic hook
    /// 3. Starts the worker threads, the logger and, in graph mode, the topology handler
    /// 4. Loads all configured observers and plugins
    /// 5. Starts the observer sorter thread
    ///
    /// On stop, the engine:
    /// 1. Exits the main processing loop
    /// 2. Performs finalization and cleanup
    ///
    /// # Behavior
    ///
    /// The service responds to the following commands:
    ///
    /// - **Start**: Sets the running flag, then hands the prepared engine to its thread, which
    ///   begins processing tasks and notifications. A second `Start` finds no engine left to hand
    ///   over; it's logged and ignored
    /// - **Stop**: Clears the running flag, then waits for the engine thread, which finalizes the
    ///   engine before it ends. Windows sees the service as stopped only after that
    /// - **Pause**: Sets the pause flag, causing the engine to suspend processing
    /// - **Resume**: Clears the pause flag, allowing the engine to resume processing
    ///
    /// A `Fin` task result also ends the engine loop; the engine is then already finalized.
    ///
    /// # Errors
    ///
    /// - Returns the error of the first setup step that fails, before connecting to the service
    ///   manager. See [`EngineInitialization::prepare`] for the list.
    /// - Returns [`StartupError::Service`] if the process wasn't started by the service manager.
    ///   The prepared engine is then stopped again.
    ///
    /// Either way, no Thryve thread is left running, and the error is written to `Error.log` once
    /// the log folder exists.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::config::Config;
    /// use thryve_run::service::service::Thryve;
    ///
    /// let config = Config::default();
    /// let (mut service, ctx) = Thryve::new(config);
    /// service.run_service(ctx)?;
    /// # Ok::<(), thryve_utils::errors::ThryveError>(())
    /// ```
    pub fn run_service(&mut self, ctx: Ctx) -> Result<(), ThryveError> {
        let packet_handle = ctx.packet_handle();

        // Cloning the atomic flags for running and paused states to be used in the service command handling.
        let is_running: Arc<AtomicBool> = self.is_running.clone();
        let pause_flag: Arc<AtomicBool> = self.is_paused.clone();

        // A second pair of clones for the engine loop. All of them point at the same two flags,
        // so the loop sees every change the commands make.
        let is_running_clone = is_running.clone();
        let pause_flag_clone = pause_flag.clone();

        // The engine thread's handle, stored by Start so that Stop can join it.
        let engine_thread: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> =
            Arc::new(Mutex::new(None));
        let engine_thread_clone = engine_thread.clone();

        // The prepared engine. Start takes it out of the slot; if the service manager can't be
        // reached, run_service takes it back out to abort it.
        let eng_init_slot: Arc<Mutex<Option<EngineInitialization>>> = Arc::new(Mutex::new(Some(
            EngineInitialization::prepare(ctx, move |eng| {
                engine_loop(eng, is_running_clone, pause_flag_clone)
            })?,
        )));
        let eng_init_slot_clone = eng_init_slot.clone();

        // The service instance will call the closure whenever a command is received.
        // Start hands the prepared engine to its thread, Stop clears the running flag and waits
        // for the thread, and Pause and Resume flip the pause flag.
        // The engine will run until the is_running flag is set to false.
        // The engine will finalize itself when the is_running flag is set to false.
        let service_run = self
            .service_instance
            .run(move |_service, command| match command {
                Command::Start => {
                    is_running.store(true, Ordering::SeqCst);

                    if let Some(eng_init) = eng_init_slot_clone
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take()
                    {
                        if let Ok(handle) = eng_init.complete_and_run() {
                            // Store the thread handle in the engine_thread variable so that we can join it later when stopping the service.
                            *engine_thread_clone
                                .lock()
                                .unwrap_or_else(PoisonError::into_inner) = Some(handle);
                        }
                    } else {
                        packet_handle.send_status_log(Log::err(
                            "service 'start' called multiple times".to_string(),
                        ));
                    }
                }

                // When the stop command is received, the is_running flag is set to false to signal the engine to stop running and finalize itself.
                Command::Stop => {
                    // Set the is_running flag to false to signal the engine to stop running and finalize itself.
                    is_running.store(false, Ordering::SeqCst);
                    if let Some(handle) = engine_thread
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .take()
                    {
                        // Thread join failure is a fatal error - the engine thread panicked
                        if let Err(e) = handle.join() {
                            // Thread join error indicates the engine thread panicked
                            // This is a critical failure that requires immediate attention
                            packet_handle.send_status_log(Log::err(format!(
                                "Engine thread join error (thread panicked): {:?}",
                                e
                            )));
                        }
                    }
                }

                // When the pause command is received, the pause flag is set to true to signal the engine to pause its execution.
                Command::Pause => {
                    pause_flag.store(true, Ordering::SeqCst);
                }

                // When the resume command is received, the pause flag is set to false to signal the engine to resume its execution.
                Command::Resume => {
                    pause_flag.store(false, Ordering::SeqCst);
                }
                _ => {}
            });

        if let Err(error) = service_run {
            let error = StartupError::Service {
                error: error.to_string(),
            }
            .into();
            if let Some(eng_init) = eng_init_slot
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .take()
            {
                eng_init.abort(&error);
            }

            return Err(error);
        }

        Ok(())
    }
}

/// The service's loop, run on the engine thread.
///
/// Runs until `Stop` clears `is_running`, or a `Fin` task result finalizes the engine (which sets
/// its exit flag). On every turn it matches the engine to `pause_flag`: it pauses or resumes the
/// engine when the flag changes, and sleeps while paused. At the end it finalizes the engine,
/// unless a `Fin` task already did.
///
/// # Arguments
///
/// * `eng` - The engine, handed over by `Start`
/// * `is_running` - Cleared by `Stop`
/// * `pause_flag` - Set by `Pause`, cleared by `Resume`
///
/// Public, but hidden from the docs, only so that the tests can run it: the service itself needs
/// the Windows service manager.
#[doc(hidden)]
pub fn engine_loop(mut eng: Engine, is_running: Arc<AtomicBool>, pause_flag: Arc<AtomicBool>) {
    // The engine will run in a loop until the is_running flag is set to false.
    while !eng.exit_flag && is_running.load(Ordering::SeqCst) {
        // Check the pause flag and pause or resume the engine accordingly.
        let engine_is_paused = eng.pause_flag;
        let is_pause_flag_loaded = pause_flag.load(Ordering::SeqCst);

        // If the pause flag is loaded and the engine is not paused, the engine gets paused.
        if is_pause_flag_loaded && !engine_is_paused {
            eng.pause(false);
        }
        // If the pause flag is not loaded and the engine is paused, the engine gets resumed.
        else if !is_pause_flag_loaded && engine_is_paused {
            eng.resume(false);
        }

        // Run the engine's main loop once. This will process any tasks or notifications that are in the channels.
        eng.run_once();
    }

    // Finalize the engine by performing any necessary cleanup operations before shutting down.
    // Only `Stop` gets here (a `Fin` has already finalized the engine). Someone asked for this
    // stop, so the summary of lost work, if any, goes to `Main.log`.
    if !eng.exit_flag {
        eng.finalize(true);
    }
}
