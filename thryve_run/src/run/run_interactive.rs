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
//! - **Engine startup**: Initializes the engine with observers and plugins
//! - **Main loop**: Processes notifications and tasks until shutdown
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
//! run_interactive(ctx);
//! ```

use crossbeam::channel::bounded;
use thryve_utils::extras;

use std::{thread, time::Duration};
use thryve_utils::cfg::{config::Config, ctx::Ctx};
use thryve_utils::packets::packet_handle::PacketHandle;
use crate::init::init::{self, init_engine};

/// Runs the Thryve engine in interactive mode.
///
/// This is the primary entry point for running the engine when launched manually
/// (e.g., double-clicking an executable or running from a command prompt).
///
/// The function performs the following operations:
///
/// 1. Initializes the engine with the provided context
/// 2. Loads all configured observers and plugins
/// 3. Starts the observer sorter thread for notification routing
/// 4. Enters the main processing loop until shutdown is signaled
/// 5. Performs cleanup operations before exiting
///
/// # Arguments
///
/// * `ctx` - The context containing configuration, observers, and plugins to load
///
/// # Behavior
///
/// On the first iteration of the main loop, the function:
/// - Loads all observers specified in the context
/// - Loads all plugins specified in the context
/// - Initializes the observer sorter thread
/// - Applies any debugging wait time if enabled in configuration
///
/// The main loop continues processing notifications and tasks until the engine
/// sets its exit flag to `true`, at which point the engine is finalized and
/// the function returns.
///
/// # Examples
///
/// ```no_run
/// use thryve_utils::cfg::{config::Config, ctx::Ctx};
/// use thryve_run::run::run_interactive::run_interactive;
///
/// let config = Config::default();
/// let ctx = Ctx::new(config);
/// run_interactive(ctx);
/// ```
pub fn run_interactive(mut ctx: Ctx) {
    println!("Running manually (double-click, cmd, etc.)");
    println!("Hello from the interactive mode!");

    // Initialize the engine by creating a new instance of the engine struct and setting up the logger observer.
    let mut eng = init_engine(&ctx);
    
    let mut state: u8 = 0;
    let len_obs_tasks = ctx.observers.len();
    let len_plugin_tasks = ctx.plugins.len();
    
    // Main loop for processing notifications and tasks until the engine signals for shutdown by setting the exit flag to true.
    // The loop also handles the initial setup of the logger observer and the observer sorter thread during the first iteration.
    // During the first iteration, it loads the observers and plugins into the engine, initializes the observer sorter thread, and handles any debugging wait time if specified in the configuration.
    while eng.exit_flag == false {
        // During the first iteration of the loop, perform the initial setup of the logger observer and the observer sorter thread.
        if state == 0{
            state += 1;
            
            // Load the observers and plugins into the engine if there are any specified in the context.
            if len_obs_tasks > 0 {
                for obs in ctx.observers(){
                    eng.obs_manager.load_observer(obs);
                }
            }

            // Load the plugins into the engine if there are any specified in the context.
            if len_plugin_tasks > 0 {
                for plugin in ctx.plugins(){
                    eng.plugin_manager.load_plugin(plugin);
                }
            }
            
            // Initialize the observer sorter thread responsible for sorting incoming notifications and forwarding them to the correct observer queues.
            eng.obs_manager.init_obs_sorter_thread();
            
            // Handle any debugging wait time if specified in the configuration.
            if ctx.config.debugging.is_debugging{
                thread::sleep(Duration::from_secs(ctx.config.debugging.wait_before_execution));
            }
        }

        eng.run_once();
    }

    // Finalize the engine by performing any necessary cleanup operations before shutting down.
    eng.finalize();
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
pub fn summon_requirements(config: Config) -> Ctx{
    
    let (task_tx , task_rx) = bounded(config.pipes.task_pipe_capacity);
    let (notif_tx , notif_rx) = bounded(config.pipes.notif_pipe_capacity);
    
    let mut ctx = Ctx::new(config, task_rx, notif_rx, PacketHandle::new(task_tx , notif_tx));
    init::init_directories(&mut ctx);
    extras::panic_hook::init_panic_hook();

    ctx
}