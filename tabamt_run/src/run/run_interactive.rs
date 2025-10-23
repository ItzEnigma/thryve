use tabamt_utils::{any_naming_input::ANNamingInput, task::Task};

// The main function if the app will be running without the SCM: the main interactive entrypoint to the program
// @param obs_init_tasks: a vector of Tasks that initialize observers.
// @param plugin_init_tasks: a vector of Tasks that initialize plugins.
// @param logging_file_names: a vector of ANNamingInput representing the names of the log files to be created.
pub fn run_interactive(obs_init_tasks: Vec<Task> , plugin_init_tasks: Vec<Task> , logging_file_names: Vec<ANNamingInput>) {
    println!("Running manually (double-click, cmd, etc.)");
    println!("Hello from the interactive mode!");
}