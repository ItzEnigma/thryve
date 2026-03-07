use crossbeam::channel::bounded;

use std::{thread, time::Duration};
use tabamt_utils::cfg::{config::Config, ctx::Ctx};
use tabamt_utils::packets::packet_handle::PacketHandle;
use crate::init::init::init_engine;

// The main function if the app will be running without the SCM: the main interactive entrypoint to the program
// @param max_threads: an optional usize indicating the maximum number of threads the service can handle at once.
// @param packet_handle: PacketHandle - the packet handle to be used by the engine.
// @param task_rx: Receiver<Task> - the receiver part of the channel to be handed to the engine.
// @param notif_rx: Receiver<Box<dyn NotifEX>> - the receiver part of the notification channel to be handed to the engine.
// @param obs_init_tasks: a vector of Tasks that initialize observers.
// @param plugin_init_tasks: a vector of Tasks that initialize plugins.
// @param logging_file_names: a vector of NamedInput representing the names of the log files to be created.
pub fn run_interactive(mut ctx: Ctx) {
    println!("Running manually (double-click, cmd, etc.)");
    println!("Hello from the interactive mode!");

    let mut eng = init_engine(&ctx);
    
    let mut state: u8 = 0;
    let len_obs_tasks = ctx.observers.len();
    let len_plugin_tasks = ctx.plugins.len();
    
    while eng.exit_flag == false {
        // is_Running the engine once to process any tasks in the queue.
        // The first time this is called, it will also initialize the logger.
        // This is important as the logger needs to be initialized before any other module.
        // Once the logger is initialized, the state is incremented to move to the next step.
        eng.run_once();

        if state == 0{
            // The logger is now initialized.
            // Increment the state to move to the next step.
            state += 1;
            
            // Initializing the observers if there are any.
            // This needs to be done after the logger is initialized.
            if len_obs_tasks > 0 {
                for obs in ctx.observers(){
                    eng.obs_manager.load_observer(obs);
                }
            }

            // Initializing the plugins if there are any.
            // This needs to be done after the observers are initialized.
            if len_plugin_tasks > 0 {
                for plugin in ctx.plugins(){
                    eng.plugin_manager.load_plugin(plugin);
                }

                // init::init_plugins(ctx.plugins(), ctx.packet_handle());
            }
            
            // The engine is now fully initialized.
            // This thread sleep is only here to allow in debugging to open the log file and see the initialization messages.
            if ctx.config.debugging.is_debugging{
                thread::sleep(Duration::from_secs(ctx.config.debugging.wait_before_execution));
            }
        }
    }
}

pub fn summon_requirements(config: Config) -> Ctx{
    // Creating the channel that will be used to send tasks to the engine.
    let (task_tx , task_rx) = bounded(config.pipes.task_pipe_capacity);
    let (notif_tx , notif_rx) = bounded(config.pipes.notif_pipe_capacity);
    

    Ctx::new(config, task_rx, notif_rx, PacketHandle::new(task_tx , notif_tx))
}