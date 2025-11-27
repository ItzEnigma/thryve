use std::sync::mpsc::Receiver;

use crate::service::service::TaBaMT;
use tabamt_utils::{any_naming_input::ANNamingInput, notif::Notif, task::Task};

// The main function if the app will be running as a service: the main service entrypoint to the program.
// @param can_pause: an optional boolean indicating if the service can be paused.
// @param can_stop: an optional boolean indicating if the service can be stopped.
// @param obs_init_tasks: a vector of Tasks that initialize observers.
// @param plugin_init_tasks: a vector of Tasks that initialize plugins.
// @param logging_file_names: a vector of ANNamingInput representing the names of the log files to be created.
pub fn try_run_as_service(mut svc: TaBaMT, task_rx: Receiver<Task> , notif_rx: Receiver<Box<dyn Notif>>, obs_init_tasks: Vec<Task> , plugin_init_tasks: Vec<Task> , logging_file_names: Vec<ANNamingInput>){
    // Then the service itself will run.
    // The run_service will take ownership of th receiver, the observers vector, the plugins vector, and the log-file-names vector.
    svc.run_service(task_rx , notif_rx , obs_init_tasks , plugin_init_tasks , logging_file_names);
}

pub fn create_service<'a> (can_pause: Option<bool> , can_stop: Option<bool>) -> (TaBaMT<'a> , Receiver<Task> , Receiver<Box<dyn Notif>>) {
    // This is an instance of the service that will be running.
    // The "svc" is the struct holding the instance.
    // The "rx" is an "mpsc::Receiver<Task>" part of a channel which will be passed to the engine when it is created.
    TaBaMT::new(can_pause, can_stop)
}