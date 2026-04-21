use crossbeam::channel::bounded;

use std::{thread, time::Duration};
use tabamt_utils::cfg::{config::Config, ctx::Ctx};
use tabamt_utils::packets::packet_handle::PacketHandle;
use crate::init::init::init_engine;

/// Is the entry point for running the Engine in interactive mode.
/// It takes the Ctx and initializes the engine, loads Observers and Plugins, and starts the main loop for processing notifications and tasks.
/// The function also handles the initial setup of the LoggerObserver and the observer sorter thread.
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
        eng.run_once();

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
    }

    // Finalize the engine by performing any necessary cleanup operations before shutting down.
    eng.finalize();
}

/// The `summon_requirements` function is responsible for creating the necessary communication channels and context.
/// It inserts the given Config into the context and returns the initialized Ctx.
pub fn summon_requirements(config: Config) -> Ctx{
    
    let (task_tx , task_rx) = bounded(config.pipes.task_pipe_capacity);
    let (notif_tx , notif_rx) = bounded(config.pipes.notif_pipe_capacity);
    
    Ctx::new(config, task_rx, notif_rx, PacketHandle::new(task_tx , notif_tx))
}