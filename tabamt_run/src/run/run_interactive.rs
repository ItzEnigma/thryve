use std::{sync::mpsc::{self, Receiver}, thread, time::Duration};
use tabamt_core::runtime::engine::Engine;
use tabamt_utils::{any_naming_input::ANNamingInput, notification::Notification, packet_handle::PacketHandle, task::Task};
use crate::init::init;

// The main function if the app will be running without the SCM: the main interactive entrypoint to the program
// @param obs_init_tasks: a vector of Tasks that initialize observers.
// @param plugin_init_tasks: a vector of Tasks that initialize plugins.
// @param logging_file_names: a vector of ANNamingInput representing the names of the log files to be created.
pub fn run_interactive(obs_init_tasks: Vec<Task> , plugin_init_tasks: Vec<Task> , logging_file_names: Vec<ANNamingInput>) {
    println!("Running manually (double-click, cmd, etc.)");
    println!("Hello from the interactive mode!");

    let (packet_handle , task_rx , notif_rx) = setup_environment();

    // Creating the engine instance.
    let mut eng = Engine::new(task_rx , notif_rx , packet_handle.clone());
    let len_obs_tasks = obs_init_tasks.len();
    let len_plugin_tasks = plugin_init_tasks.len(); 

    // This spawns the threads in the thread pool and makes them wait for tasks.
    eng.get_thread_pool().run_threads();

    // Initializing the logger first.
    // The initialization function takes ownership of the vector.
    init::init_logger(packet_handle.clone() , logging_file_names);
    
    let mut state: u8 = 0;
    
    // Taking ownership of the observers and plugins vectors.
    let mut obs_init_tasks_clone = Some(obs_init_tasks);
    let mut plugin_init_tasks_clone = Some(plugin_init_tasks);

    while eng.get_exit_flag() == false {
        // is_Running the engine once to process any tasks in the queue.
        // The first time this is called, it will also initialize the logger.
        // This is important as the logger needs to be initialized before any other module.
        // Once the logger is initialized, the state is incremented to move to the next step.
        let logger_loaded = eng.run_once();

        if state == 0 && logger_loaded.unwrap_or(false) == true {
            // The logger is now initialized.
            // Increment the state to move to the next step.
            state += 1;
            
            // Initializing the observers if there are any.
            // This needs to be done after the logger is initialized.\
            if len_obs_tasks > 0 {
                // The option is taken here to move the vector into the init function, meaning the option is unwrapped to reveal the vector.
                if let Some(obs) = obs_init_tasks_clone.take() {
                    // The initialization function takes ownership of the vector.
                    init::init(obs, packet_handle.clone());
                }
            }
        }
        
        // Once the observers are initialized, the plugins can be initialized.
        // This needs to be done after the observers are initialized.
        if state == 1{
            // The observers are now initialized.
            // Increment the state to move to the next step.
            state += 1;
            
            // Initializing the plugins if there are any.
            // This needs to be done after the observers are initialized.
            if len_plugin_tasks > 0 {
                // The option is taken here to move the vector into the init function, meaning the option is unwrapped to reveal the vector.
                if let Some(plugins) = plugin_init_tasks_clone.take() {
                    // The initialization function takes ownership of the vector.
                    init::init(plugins, packet_handle.clone());
                }
            }
            
            // The engine is now fully initialized.
            // This thread sleep is only here to allow in debugging to open the log file and see the initialization messages.
            // It can and should be removed in production.
            thread::sleep(Duration::from_secs(10));
        }
    }
}

fn setup_environment() -> (PacketHandle , Receiver<Task> , Receiver<Notification>) {
    // Creating the channel that will be used to send tasks to the engine.
    let (task_tx , task_rx) = mpsc::channel();
    let (notif_tx , notif_rx) = mpsc::channel();
    
    let packet_handle = PacketHandle::new(task_tx, notif_tx);

    (packet_handle , task_rx , notif_rx)
}