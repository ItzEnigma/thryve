use tabamt_core::runtime::engine::Engine;

use tabamt_utils::extras::named_input::NamedInput;
use tabamt_utils::cfg::ctx::Ctx;
use tabamt_utils::packets::packet_handle::PacketHandle;
use tabamt_utils::packets::task::{Job, Task};
use tabamt_utils::res::{self};

use tabamt_observers::logger::LoggerObserver;

pub fn init_engine(ctx: &Ctx) -> Engine {
    // Creating the engine instance.
    let eng = Engine::new(&ctx);

    // This spawns the threads in the thread pool and makes them wait for tasks.
    eng.plugin_manager.threadpool.run_threads_plugins();

    // Initializing the logger first.
    // The initialization function takes ownership of the vector.
    init_logger(ctx.packet_handle() , ctx.logging_file_names.clone());

    eng
}

// This function initializes the logger observer.
// @param packet_handle: the PacketHandle to send the initialization task to the engine.
// @param file_names: a vector of NamedInput representing the names of the log files to be created.
pub fn init_logger(packet_handle: PacketHandle , file_names: Vec<String>){
    // The logging observer type is always "Logging".
    let logging_type = NamedInput::new("Logging");
    load_logger(packet_handle, logging_type , file_names);
}

// This function creates and sends a task to load the logger observer.
// This is just a helper function for init_logger.
// @param packet_handle: the PacketHandle to send the task to the engine.
// @param obs_type: the NamedInput representing the type of the observer to be loaded (here it should always be "Logging").
// @param file_names: a vector of NamedInput representing the names of the log files to be created.
fn load_logger(packet_handle: PacketHandle , obs_type: NamedInput , mut file_names: Vec<String>){
    let mut file_names_checked = vec![];

    while !file_names.is_empty(){
        file_names_checked.push(NamedInput::new(file_names[0].as_str()));
        file_names.pop();
    }

    // The job to load the logger observer.
    let job_obs_load: Job = Box::new(move || {
        // If the observer type is "Logging", create a new LoggerObserver with the provided file names.
        if obs_type.inner() == "Logging" {
            LoggerObserver::new(file_names_checked)
        }

        // If the observer type is not "Logging", return an error result.
        else{
            res::Result::new("ObserverInit", "[CORE] Failed To Load Logging Observer.".to_string() , "Main")
        }
    });

    // Create a task with the job to load the logger observer and send it to the engine via the PacketHandle.
    let task = Task::new("Init", 1, job_obs_load);
    packet_handle.send_task(task);
}