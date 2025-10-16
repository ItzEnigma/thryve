use crate::service::service::TaBaMT;
use utils::{any_naming_input::ANNamingInput, task::Job};

// The main function if the app will be running as a service: the main service entrypoint to the program
// What is written in this function is actually just a sample of what can be done.
// In this sample, 10 instances of "TestPlugin" are created to do the same "Job".
// The engine then loads them and runs all the tasks within each instance of "TestPlugin".
pub fn try_run_as_service(can_pause: Option<bool> , can_stop: Option<bool> , obs_init_functions: Vec<Job> , plugin_init_functions: Vec<Job> , logging_file_names: Vec<ANNamingInput>) -> Result<(), bool>{
    // This is an instance of the service that will be running.
    // The "svc" is the struct holding the instance.
    // The "rx" is an "mpsc::Receiver<Task>" part of a channel which will be passed to the engine when it is created.
    let (mut svc , task_rx, notif_rx) = TaBaMT::new(can_pause, can_stop);

    // Then the service itself will run.
    // The run_service will take ownership of th receiver, the observers vector, the plugins vector, and the log-file-names vector.
    let is_running = svc.run_service(task_rx , notif_rx , obs_init_functions , plugin_init_functions , logging_file_names);

    // If the app is not started with the SCM, return to start interactively (as opened with the CMD/double-clicked/etc...).
    match is_running {
        Ok(_) => Ok(()),
        Err(_) => Err(false),
    }
}