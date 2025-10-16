use utils::{any_naming_input::ANNamingInput, task::Job};

// The main function if the app will be running without the SCM: the main interactive entrypoint to the program
pub fn run_interactive(obs_init_functions: Vec<Job> , plugin_init_functions: Vec<Job> , logging_file_names: Vec<ANNamingInput>) {
    println!("Running manually (double-click, cmd, etc.)");
    println!("Hello from the interactive mode!");
}