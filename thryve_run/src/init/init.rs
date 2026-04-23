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

use thryve_core::runtime::engine::Engine;

use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::cfg::ctx::Ctx;
use thryve_observers::logger::LoggerObserver;

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
    init_logger(ctx, &mut eng, ctx.logging_file_names.clone());

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
pub fn init_logger(ctx: &Ctx, eng: &mut Engine, mut file_names: Vec<String>) {
    let mut file_names_checked = vec![];

    // Convert the vector of log file names from `String` to `NamedInput` and
    // store them in a new vector for use in the logger observer.
    while !file_names.is_empty() {
        file_names_checked.push(NamedInput::new(
            file_names[file_names.len() - 1].as_str(),
        ));
        file_names.pop();
    }

    // Create a new instance of the `LoggerObserver` struct, passing the
    // checked log file names and the packet handle from the context for
    // communication.
    let logger = LoggerObserver::new(file_names_checked, ctx.packet_handle());

    // Load the logger observer into the engine's observer manager to make it
    // available for handling notifications and logging messages.
    eng.obs_manager.load_observer(logger);
}