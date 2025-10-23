use utils::{res::{self}, task::{Job, Task, TaskName}, packet_handle::PacketHandle, any_naming_input::ANNamingInput};
use observers::logger::LoggerObserver;

// This function initializes observers, except for the logger, and plugins.
// @param vector_init_tasks: a vector of Tasks that initialize observers/plugins.
// @param packet_handle: the PacketHandle to send the initialization tasks to the engine.
pub fn init(vector_init_tasks: Vec<Task> , packet_handle: PacketHandle){
    // For each initialization function in the vector, create a task and send it to the engine via the PacketHandle.
    for task in vector_init_tasks{
        packet_handle.send_task(task);
    }
    
}

// This function initializes the logger observer.
// @param packet_handle: the PacketHandle to send the initialization task to the engine.
// @param file_names: a vector of ANNamingInput representing the names of the log files to be created.
pub fn init_logger(packet_handle: PacketHandle , file_names: Vec<ANNamingInput>){
    // The logging observer type is always "Logging".
    let logging_type = ANNamingInput::new("Logging");
    load_logger(packet_handle, logging_type , file_names);
}

// This function creates and sends a task to load the logger observer.
// This is just a helper function for init_logger.
// @param packet_handle: the PacketHandle to send the task to the engine.
// @param obs_type: the ANNamingInput representing the type of the observer to be loaded (here it should always be "Logging").
// @param file_names: a vector of ANNamingInput representing the names of the log files to be created.
fn load_logger(packet_handle: PacketHandle , obs_type: ANNamingInput , file_names: Vec<ANNamingInput>){
    // The job to load the logger observer.
    let job_obs_load: Job = Box::new(move || {
        // If the observer type is "Logging", create a new LoggerObserver with the provided file names.
        if obs_type.get_module_type() == "Logging" {
            LoggerObserver::new(file_names)
        }

        // If the observer type is not "Logging", return an error result.
        else{
            res::Result::new(ANNamingInput::new("ObserverInit"), "[CORE] Failed To Load Logging Observer.".to_string() , ANNamingInput::new("Main"))
        }
    });

    // Create a task with the job to load the logger observer and send it to the engine via the PacketHandle.
    let task = Task::new(TaskName::Init, 1, job_obs_load);
    packet_handle.send_task(task);
}