use crate::service::service::TaBaMT;
use tabamt_utils::cfg::{config::Config, ctx::Ctx};

// The main function if the app will be running as a service: the main service entrypoint to the program.
// @param can_pause: an optional boolean indicating if the service can be paused.
// @param can_stop: an optional boolean indicating if the service can be stopped.
// @param obs_init_tasks: a vector of Tasks that initialize observers.
// @param plugin_init_tasks: a vector of Tasks that initialize plugins.
// @param logging_file_names: a vector of NamedInput representing the names of the log files to be created.
pub fn try_run_as_service(mut svc: TaBaMT, ctx: Ctx){
    // Then the service itself will run.
    // The run_service will take ownership of th receiver, the observers vector, the plugins vector, and the log-file-names vector.
    svc.run_service(ctx);
}

// Function to create a new service instance.
// @param max_threads: an optional usize indicating the maximum number of threads the service can handle at once.
// @param can_pause: an optional boolean indicating if the service can be paused.
// @param can_stop: an optional boolean indicating if the service can be stopped.
// @return: a tuple containing the service instance, the task receiver, and the notification receiver.
pub fn create_service<'a> (config: Config) -> (TaBaMT<'a> , Ctx) {
    // This is an instance of the service that will be running.
    // The "svc" is the struct holding the instance.
    // The "rx" is an "mpsc::Receiver<Task>" part of a channel which will be passed to the engine when it is created.
    TaBaMT::new(config)
}