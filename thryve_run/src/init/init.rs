//! Initialization module for the Thryve engine.
//!
//! This module provides functions for initializing the Thryve engine
//! and its components. It handles the creation of the engine instance,
//! startup of the plugin thread pool, and initialization of the logger
//! observer.
//!
//! # Initialization Flow
//!
//! 1. Create a new [`Engine`](thryve_core::runtime::engine::Engine) instance
//! 2. Start the plugin thread pool for task processing
//! 3. Initialize the logger observer for logging functionality
//!
//! # Example
//!
//! ```no_run
//! use thryve_run::init::init_engine;
//!
//! let engine = init_engine(&ctx);
//! ```

use std::{collections::HashMap, fs::{File, OpenOptions}, sync::Arc};
use thryve_core::runtime::engine::Engine;
use thryve_utils::{cfg::{dir_data::DirData, meta_data::MetaData}, extras::{dir_paths::get_current_exec_dir, misc::get_current_date_time, named_input::NamedInput}};
use thryve_utils::{cfg::ctx::Ctx, extras::dir_paths::{create_dir, DirName}};
use thryve_observers::{logger::LoggerObserver, topology_handler::TopologyHandlerObserver};

/// Initializes the Thryve engine with the given context.
///
/// Creates a new [`Engine`](thryve_core::runtime::engine::Engine) instance,
/// starts the plugin thread pool for task processing, and initializes
/// the logger observer for logging functionality.
///
/// # Arguments
///
/// * `ctx` - The application context containing configuration and resources
///
/// # Returns
///
/// A new [`Engine`](thryve_core::runtime::engine::Engine) instance ready for execution.
///
/// # Initialization Steps
///
/// 1. Creates a new engine instance from the context
/// 2. Starts the plugin thread pool for processing tasks
/// 3. Initializes the logger observer with configured log file names
///
/// # Example
///
/// ```no_run
/// use thryve_run::init::init_engine;
///
/// let engine = init_engine(&ctx);
/// ```
pub fn init_engine(ctx: &Ctx) -> Engine {
    // Create a new instance of the `Engine` struct.
    let mut eng = Engine::new(ctx);

    // Start the thread pool for the plugin manager to allow plugins to run
    // their threads and perform their tasks.
    eng.plugin_manager.threadpool.run_threads_plugins();

    // Initialize the logger observer with the specified log file names.
    init_logger(ctx, &mut eng);

    if ctx.config.debugging.is_debugging && ctx.config.debugging.visual_graph{
        init_topology_handler(ctx, &mut eng);
    }

    eng
}

/// Initializes the logger observer with the specified log file names.
///
/// Creates a [`LoggerObserver`](thryve_observers::logger::LoggerObserver) instance
/// with the given log file names and loads it into the engine's observer
/// manager. This enables logging functionality throughout the system.
///
/// # Arguments
///
/// * `ctx` - The application context containing the packet handle
/// * `eng` - Mutable reference to the engine to load the observer into
/// * `file_names` - Vector of log file names to create
///
/// # Example
///
/// ```no_run
/// use thryve_run::init::init_logger;
///
/// init_logger(&ctx, &mut engine, vec!["Main".to_string(), "Error".to_string()]);
/// ```
pub fn init_logger(ctx: &Ctx, eng: &mut Engine) {
    // Create a new instance of the `LoggerObserver` struct, passing the
    // checked log file names and the packet handle from the context for
    // communication.
    let logger = LoggerObserver::new(ctx);

    // Load the logger observer into the engine's observer manager to make it
    // available for handling notifications and logging messages.
    eng.obs_manager.load_observer(logger);
}

pub fn init_topology_handler(ctx: &Ctx, eng: &mut Engine){
    let topology_handler = TopologyHandlerObserver::new(ctx);

    eng.obs_manager.load_observer(topology_handler);
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
/// # Panics
///
/// Panics if:
/// - Cannot determine the executable path
/// - Cannot create the log directory
/// - Cannot create any of the log files
///
/// # Example
///
/// ```no_run
/// use thryve_run::init::init_directories;
///
/// init_directories(&mut ctx);
/// ```
pub fn init_directories(ctx: &mut Ctx) {
    let log_dir_path = create_dir(DirName::Log);

    // Create log files for each module type and store them in a hashmap for easy access.
    let mut temp_hash_map: HashMap<NamedInput, Arc<File>> = HashMap::new();

    // Create a main log file and an error log file for general logging and
    // error logging respectively.
    let log_file_path_main = log_dir_path.join("Main.log");
    temp_hash_map.insert(
        NamedInput::new("Main"),
        Arc::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_file_path_main)
                .unwrap(),
        ),
    );
 
    let log_file_path_err = log_dir_path.join("Error.log");
    temp_hash_map.insert(
        NamedInput::new("Error"),
        Arc::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_file_path_err)
                .unwrap(),
        ),
    );

    // Create a debug log file if debug-mode is enabled in the Config.
    if ctx.config.debugging.is_debugging {
        let log_file_path_debug = log_dir_path.join("Debug.log");
        temp_hash_map.insert(
            NamedInput::new("Debug"),
            Arc::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_file_path_debug)
                    .unwrap(),
            ),
        );
    }

    let file_names = ctx.logging_file_names.clone();
 
    // Create log files for each supplied file name and store them in the hashmap.
    for name in file_names {
        let log_file_path = log_dir_path.join(format!("{}.log", name));
 
        temp_hash_map.insert(
            NamedInput::new(name.as_str()),
            Arc::new(
                OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(log_file_path)
                    .unwrap(),
            ),
        );
    }
  
    let mut dir_data = DirData::new(get_current_exec_dir(), temp_hash_map);
    dir_data.set_path(DirName::Log, log_dir_path);

    if ctx.config.debugging.is_debugging && ctx.config.debugging.visual_graph{
        let topology_dir_path = create_dir(DirName::Topology);
        dir_data.set_path(DirName::Topology, topology_dir_path);
    }

    let meta_data = MetaData::new(get_current_date_time(), dir_data);
  
    ctx.meta_data = meta_data;
}