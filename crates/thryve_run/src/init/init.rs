// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Startup for the Thryve engine.
//!
//! [`EngineInitialization`] is the startup sequence that every run mode shares. Interactive mode
//! and the Windows service use it today, and a Linux service can use it later. It spawns the
//! engine thread, creates the log folders and files, installs the panic hook, and starts every
//! thread the engine needs. If a step fails, it stops what it started and returns the error. The
//! other functions here are the individual steps.
//!
//! # Example
//!
//! ```no_run
//! use thryve_core::runtime::engine::Engine;
//! use thryve_run::init::init::{init_directories, init_logger};
//! use thryve_run::run::run_interactive::summon_requirements;
//! use thryve_utils::cfg::config::Config;
//!
//! let mut ctx = summon_requirements(Config::load_def_config());
//! init_directories(&mut ctx)?;
//! let mut engine = Engine::new(&ctx);
//! init_logger(&ctx, &mut engine)?;
//! # Ok::<(), thryve_utils::errors::ThryveError>(())
//! ```

use crossbeam::channel::Sender;
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    sync::Arc,
    thread::{self, JoinHandle},
    time::Duration,
};
use thryve_core::runtime::engine::Engine;
use thryve_observers::{logger::LoggerObserver, topology_handler::TopologyHandlerObserver};
use thryve_utils::{
    cfg::ctx::Ctx,
    errors::{NameInputType, StartupError, ThryveError},
    extras::{
        self,
        dir_paths::{DirName, create_dir},
        named_input::LogFileNamedInput,
    },
};
use thryve_utils::{
    cfg::{dir_data::DirData, meta_data::MetaData},
    extras::{dir_paths::get_current_exec_dir, misc::get_current_date_time},
};

/// An engine that is set up and ready to start: the startup sequence every run mode shares.
///
/// Starting the engine takes three steps:
///
/// 1. [`prepare`](Self::prepare) spawns the engine thread, which waits for its engine. Then it
///    creates the engine and sets everything up on the caller's thread. The engine starts
///    paused, so the worker threads already run but no plugin task does.
/// 2. [`complete_and_run`](Self::complete_and_run) hands the engine to that thread, which waits
///    out the debug wait (when there is one), resumes the engine, and starts the mode's loop.
///    Only then do the plugins' init tasks run.
/// 3. `abort`, instead of step 2, stops a prepared engine that will never start. It's
///    crate-internal: `prepare` calls it when a step fails, and the Windows service when it can't
///    connect to the service manager.
///
/// Interactive mode calls `complete_and_run` right after `prepare`. The Windows service prepares
/// before it connects to the service manager, and completes when the service manager sends
/// `Start`. A Linux service can do the same later.
///
/// A value of this type owns running threads, so it must end in either `complete_and_run` or
/// `abort`. Just dropping it leaves those threads running, which is why it's `#[must_use]`.
///
/// # Example
///
/// ```no_run
/// use thryve_run::init::init::EngineInitialization;
/// use thryve_run::run::run_interactive::summon_requirements;
/// use thryve_utils::cfg::config::Config;
///
/// let ctx = summon_requirements(Config::load_def_config());
/// let prepared = EngineInitialization::prepare(ctx, |mut eng| {
///     while !eng.exit_flag {
///         eng.run_once();
///     }
/// })?;
/// let handle = prepared.complete_and_run()?;
/// let _ = handle.join();
/// # Ok::<(), thryve_utils::errors::ThryveError>(())
/// ```
#[must_use]
pub struct EngineInitialization {
    /// The engine thread, waiting for `eng_thread_tx` to hand it the engine.
    eng_thread_handle: JoinHandle<()>,
    /// Hands the engine to the engine thread. Dropping it without sending tells the thread to end.
    eng_thread_tx: Sender<Engine>,
    /// The set-up engine. Its threads are already running, but its loop hasn't started.
    eng: Engine,
    /// The context, kept so that a later failure can still be written to `Error.log`.
    ctx: Ctx,
}

impl EngineInitialization {
    /// Spawns the engine thread, creates the engine and sets everything up.
    ///
    /// The engine thread starts first, waiting for its engine. That way a failed setup can
    /// always stop it again. Then, on the caller's thread, this function:
    ///
    /// 1. creates the log folders and files, and installs the panic hook;
    /// 2. starts the worker threads, the logger and, in graph mode, the topology handler;
    /// 3. loads the context's observers and plugins;
    /// 4. starts the notification sorter.
    ///
    /// If a step fails, the error is written to `Error.log` (once the log folder exists),
    /// everything already started is stopped, the engine thread is joined, and the error is
    /// returned. No Thryve thread is left running.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context with the configuration, the observers, the plugins and the log-file
    ///   names
    /// * `func` - The run mode's loop. It runs on the engine thread once the engine arrives, after
    ///   the debug wait (`wait_before_execution`) if debugging is on. If the engine never arrives,
    ///   it never runs.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError::ThreadInit`] if the engine thread or any of Thryve's threads can't
    /// be started. It also returns every error of the setup steps: the folder and log-file errors
    /// of [`init_directories`], [`NamingError`](thryve_utils::errors::NamingError) for a bad
    /// log-file name, and [`StartupError::RegisterDuplicate`] for a plugin type, an observer type
    /// or a log-file name used twice.
    pub fn prepare(
        mut ctx: Ctx,
        func: impl FnOnce(Engine) + Send + 'static,
    ) -> Result<Self, ThryveError> {
        let builder = std::thread::Builder::new();
        let (tx, rx) = crossbeam::channel::bounded::<Engine>(1);

        let handle = builder
            .name(String::from("Engine Thread"))
            .spawn(move || {
                if let Ok(mut eng) = rx.recv() {
                    // Handle any debugging wait time if specified in the configuration.
                    if ctx.config.debugging.is_debugging {
                        thread::sleep(Duration::from_secs(
                            ctx.config.debugging.wait_before_execution,
                        ));
                    }

                    eng.resume(true);
                    func(eng)
                }
            })
            .map_err(|e| {
                let error = StartupError::ThreadInit {
                    thread_name: "Engine Thread".to_string(),
                    source: e,
                }
                .into();

                ctx.e_log(&error);

                error
            })?;

        let mut eng = Engine::new(&ctx);

        let init_result = init_sequence(&mut eng, &mut ctx);

        let initialization_sequence = EngineInitialization {
            eng_thread_handle: handle,
            eng_thread_tx: tx,
            eng,
            ctx,
        };

        if let Err(e) = init_result {
            initialization_sequence.abort(&e);
            return Err(e);
        }

        Ok(initialization_sequence)
    }

    /// Hands the engine to the engine thread, which starts the run mode's loop.
    ///
    /// Returns the engine thread's handle. Joining it waits until the loop has ended.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError::EngineThreadEnded`] if the engine thread ended before it received
    /// the engine. Its code only waits, so this shouldn't happen. If it does, the engine is taken
    /// back and stopped the same way as when a setup step fails.
    pub fn complete_and_run(mut self) -> Result<JoinHandle<()>, ThryveError> {
        if let Err(e) = self.eng_thread_tx.send(self.eng) {
            let error: ThryveError = StartupError::EngineThreadEnded.into();
            self.eng = e.0;

            self.abort(&error);

            return Err(error);
        };

        Ok(self.eng_thread_handle)
    }

    /// Stops a prepared engine that will never start.
    ///
    /// Writes `error` to `Error.log` first, so that the reason isn't lost even if the cleanup
    /// hangs. Then it closes the channel (the waiting engine thread ends), finalizes the engine
    /// (which stops every thread it started) and joins the engine thread.
    ///
    /// [`prepare`](Self::prepare) calls this when a setup step fails. The Windows service calls it
    /// when it can't connect to the service manager.
    ///
    /// # Arguments
    ///
    /// * `error` - Why the engine won't start
    pub(crate) fn abort(mut self, error: &ThryveError) {
        self.ctx.e_log(error);

        drop(self.eng_thread_tx);
        // Not the service's Stop, so the summary of lost work, if any, goes to `Error.log`.
        self.eng.finalize(false);
        let _ = self.eng_thread_handle.join();
    }
}

/// Runs the setup steps of [`EngineInitialization::prepare`], in order, and stops at the first
/// error.
///
/// The order matters:
///
/// - The folders and log files come first, because the logger needs them.
/// - The logger and the topology handler load before the context's observers, so the duplicate
///   check rejects a user observer typed `Logging`, or `TopologyHandler` in graph mode.
/// - The sorter starts last, because it copies the observer map when it starts.
///
/// This function doesn't clean up after an error. `prepare` does, through
/// [`EngineInitialization::abort`].
fn init_sequence(eng: &mut Engine, ctx: &mut Ctx) -> Result<(), ThryveError> {
    // Initialize the logging directories and panic hook.
    init_directories(ctx)?;
    extras::panic_hook::init_panic_hook();

    // Start the thread pool for the plugin manager to allow plugins to run
    // their threads and perform their tasks.
    eng.plugin_manager
        .threadpool
        .run_threads_plugins(eng.plugin_manager.res_tx.clone())?;

    // Initialize the logger observer with the specified log file names.
    init_logger(ctx, eng)?;

    if ctx.config.debugging.is_debugging && ctx.config.debugging.visual_graph {
        init_topology_handler(ctx, eng)?;
    }

    let len_obs_tasks = ctx.observers.len();
    let len_plugin_tasks = ctx.plugins.len();

    // Load the observers and plugins into the engine if there are any specified in the context.
    if len_obs_tasks > 0 {
        for obs in ctx.observers() {
            eng.obs_manager.load_observer(obs)?;
        }
    }

    // Load the plugins into the engine if there are any specified in the context. Each init task
    // goes through the engine's usual per-task step (metrics, graph node) and waits in the queue:
    // the engine is still paused, so it runs only once the engine starts.
    if len_plugin_tasks > 0 {
        let is_graph_mode_on =
            eng.config.debugging.is_debugging && eng.config.debugging.visual_graph;

        for plugin in ctx.plugins() {
            println!("loaded plugin: {}", plugin.plg_type());
            let task_init =
                eng.plugin_manager
                    .load_plugin(&ctx.config, plugin, is_graph_mode_on)?;
            eng.accept_task(task_init);
        }
    }

    // Initialize the observer sorter thread responsible for sorting incoming notifications and forwarding them to the correct observer queues.
    eng.obs_manager.init_obs_sorter_thread()?;

    Ok(())
}

/// Initializes the logger observer with the specified log file names.
///
/// Creates a [`LoggerObserver`] instance
/// with the given log file names and loads it into the engine's observer
/// manager. This enables logging functionality throughout the system.
///
/// # Arguments
///
/// * `ctx` - The application context containing the packet handle and the log files
/// * `eng` - Mutable reference to the engine to load the observer into
///
/// # Errors
///
/// Returns [`StartupError::LogFileFetch`] if the context has no `Error.log` (`init_directories`
/// hasn't run), or [`StartupError::ThreadInit`] if the logger's thread can't be started.
///
/// # Example
///
/// ```no_run
/// use thryve_core::runtime::engine::Engine;
/// use thryve_run::init::init::{init_directories, init_logger};
/// use thryve_run::run::run_interactive::summon_requirements;
/// use thryve_utils::cfg::config::Config;
///
/// let mut ctx = summon_requirements(Config::load_def_config());
/// init_directories(&mut ctx)?;
/// let mut engine = Engine::new(&ctx);
/// init_logger(&ctx, &mut engine)?;
/// # Ok::<(), thryve_utils::errors::ThryveError>(())
/// ```
pub fn init_logger(ctx: &Ctx, eng: &mut Engine) -> Result<(), ThryveError> {
    // Create a new instance of the `LoggerObserver` struct, passing the
    // checked log file names and the packet handle from the context for
    // communication.
    let logger = LoggerObserver::new(ctx)?;

    // Load the logger observer into the engine's observer manager to make it
    // available for handling notifications and logging messages.
    eng.obs_manager.load_observer(logger)?;

    Ok(())
}

/// Initializes the topology handler observer used by graph mode.
///
/// Creates a [`TopologyHandlerObserver`]
/// and loads it into the engine's observer manager. The startup sequence
/// ([`EngineInitialization::prepare`]) calls this only when both `is_debugging` and
/// `visual_graph` are enabled, after [`init_directories`] has created the topology directory.
///
/// # Arguments
///
/// * `ctx` - The application context containing the packet handle and the topology directory
/// * `eng` - Mutable reference to the engine to load the observer into
///
/// # Errors
///
/// Returns [`StartupError::DirFetch`] if the context has no topology folder (`init_directories`
/// didn't run in graph mode), or [`StartupError::ThreadInit`] if the topology handler's thread
/// can't be started.
pub fn init_topology_handler(ctx: &Ctx, eng: &mut Engine) -> Result<(), ThryveError> {
    let topology_handler = TopologyHandlerObserver::new(ctx)?;

    eng.obs_manager.load_observer(topology_handler)?;

    Ok(())
}

/// Initializes the directory structure and log files for the Thryve system.
///
/// Creates the necessary directory structure and log files required for the
/// system to operate. This includes:
///
/// 1. Determining the executable directory
/// 2. Creating a timestamped log directory
/// 3. Creating Main.log and Error.log files
/// 4. Creating additional log files based on configuration
/// 5. Storing the directory data in the context
///
/// # Arguments
///
/// * `ctx` - Mutable reference to the application context to store metadata
///
/// # Directory Structure
///
/// Creates the following structure:
///
/// ```text
/// <exe_dir>/
///   log/
///     <timestamp>/
///       Main.log
///       Error.log
///       Debug.log     (only if debug-mode is enabled in the Config)
///       <Module>.log  (for each configured module)
/// ```
///
/// # Log Files Created
///
/// - `Main.log`: General log messages
/// - `Error.log`: Error messages
/// - `<Module>.log`: Module-specific logs for each name in `ctx.logging_file_names`
///
/// # Errors
///
/// Returns a [`StartupError`] if:
/// - the executable's directory can't be found (`RootFolder`, `NoParentFolder`);
/// - the log directory can't be created (`LogDirCreation`);
/// - a log file can't be opened (`LogFileOpen`);
/// - graph mode is on and the topology directory can't be created (`TopologyDirCreation`).
///
/// Folders and files created before the failing step are left on disk.
///
/// # Example
///
/// ```no_run
/// use thryve_run::init::init::init_directories;
/// use thryve_run::run::run_interactive::summon_requirements;
/// use thryve_utils::cfg::config::Config;
///
/// let mut ctx = summon_requirements(Config::load_def_config());
/// init_directories(&mut ctx)?;
/// # Ok::<(), thryve_utils::errors::ThryveError>(())
/// ```
pub fn init_directories(ctx: &mut Ctx) -> Result<(), ThryveError> {
    // Create log files for each module type and store them in a hashmap for easy access.
    let mut temp_hash_map: HashMap<LogFileNamedInput, Arc<File>> = HashMap::new();
    let mut temp_validation_hash_map: HashMap<LogFileNamedInput, String> = HashMap::new();

    for name in ctx.logging_file_names.iter() {
        let log_file_name = LogFileNamedInput::new_register(&name.to_ascii_uppercase())?;

        if temp_validation_hash_map.contains_key(&log_file_name) {
            return Err(ThryveError::Startup(StartupError::RegisterDuplicate {
                name: name.to_string(),
                kind: NameInputType::LogFileName,
            }));
        }

        temp_validation_hash_map.insert(log_file_name, name.to_string());
    }

    let log_dir_path = create_dir(DirName::Log)?;

    // Create a main log file and an error log file for general logging and
    // error logging respectively.
    let log_file_path_main = log_dir_path.join("Main.log");
    temp_hash_map.insert(
        LogFileNamedInput::new_main(),
        Arc::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_file_path_main)
                .map_err(|e| StartupError::LogFileOpen {
                    path: log_file_path_main.clone(),
                    source: e,
                })?,
        ),
    );

    let log_file_path_err = log_dir_path.join("Error.log");
    temp_hash_map.insert(
        LogFileNamedInput::new_err(),
        Arc::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&log_file_path_err)
                .map_err(|e| StartupError::LogFileOpen {
                    path: log_file_path_err.clone(),
                    source: e,
                })?,
        ),
    );

    // Create a debug log file if debug-mode is enabled in the Config.
    if ctx.config.debugging.is_debugging {
        let log_file_path_debug = log_dir_path.join("Debug.log");
        temp_hash_map.insert(
            LogFileNamedInput::new_debug(),
            Arc::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_file_path_debug)
                    .map_err(|e| StartupError::LogFileOpen {
                        path: log_file_path_debug.clone(),
                        source: e,
                    })?,
            ),
        );
    }

    // Create log files for each supplied file name and store them in the hashmap.
    for (key, value) in temp_validation_hash_map {
        let log_file_path = log_dir_path.join(format!("{}.log", value));

        temp_hash_map.insert(
            key,
            Arc::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&log_file_path)
                    .map_err(|e| StartupError::LogFileOpen {
                        path: log_file_path.clone(),
                        source: e,
                    })?,
            ),
        );
    }

    let current_exec_dir = get_current_exec_dir()?;
    let mut dir_data = DirData::new(current_exec_dir, temp_hash_map);
    dir_data.set_path(DirName::Log, log_dir_path);

    if ctx.config.debugging.is_debugging && ctx.config.debugging.visual_graph {
        let topology_dir_path = create_dir(DirName::Topology)?;
        dir_data.set_path(DirName::Topology, topology_dir_path);
    }

    let meta_data = MetaData::new(get_current_date_time(), dir_data);

    ctx.meta_data = meta_data;

    Ok(())
}
