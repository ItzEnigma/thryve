use thryve_core::runtime::engine::Engine;

use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::cfg::ctx::Ctx;
use thryve_observers::logger::LoggerObserver;

/// Initializes the engine by creating a new instance of the `Engine` struct with the given `Ctx` and setting up the logger observer.
/// The `Engine` is then returned.
pub fn init_engine(ctx: &Ctx) -> Engine {
    // Create a new instance of the `Engine` struct.
    let mut eng = Engine::new(&ctx);

    // Start the thread pool for the plugin manager to allow plugins to run their threads and perform their tasks.
    eng.plugin_manager.threadpool.run_threads_plugins();

    // Initialize the logger observer with the specified log file names.
    init_logger(ctx , &mut eng , ctx.logging_file_names.clone());

    eng
}

/// Initializes the logger observer by creating an instance of the LoggerObserver struct with the given Ctx and the specified `file_names`.
/// It then adds the loaded logger observer to the provided ['Engine'] in its ObserverManager to enable logging functionality.
pub fn init_logger(ctx: &Ctx , eng: &mut Engine , mut file_names: Vec<String>){
    let mut file_names_checked = vec![];

    // Convert the vector of log file names from `String` to `NamedInput` and store them in a new vector for use in the logger observer.
    while !file_names.is_empty(){
        file_names_checked.push(NamedInput::new(file_names[file_names.len() - 1].as_str()));
        file_names.pop();
    }

    // Create a new instance of the `LoggerObserver` struct, passing the checked log file names and the packet handle from the context for communication.
    let logger = LoggerObserver::new(file_names_checked , ctx.packet_handle());

    // Load the logger observer into the engine's observer manager to make it available for handling notifications and logging messages.
    eng.obs_manager.load_observer(logger);
}