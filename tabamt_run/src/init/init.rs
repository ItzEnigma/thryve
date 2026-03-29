use tabamt_core::runtime::engine::Engine;

use tabamt_utils::extras::named_input::NamedInput;
use tabamt_utils::cfg::ctx::Ctx;
use tabamt_observers::logger::LoggerObserver;

pub fn init_engine(ctx: &Ctx) -> Engine {
    // Creating the engine instance.
    let mut eng = Engine::new(&ctx);

    // This spawns the threads in the thread pool and makes them wait for tasks.
    eng.plugin_manager.threadpool.run_threads_plugins();

    // Initializing the logger first.
    // The initialization function takes ownership of the vector.
    init_logger(ctx , &mut eng , ctx.logging_file_names.clone());

    eng
}

// This function initializes the logger observer.
// @param packet_handle: the PacketHandle to send the initialization task to the engine.
// @param file_names: a vector of NamedInput representing the names of the log files to be created.
pub fn init_logger(ctx: &Ctx , eng: & mut Engine , mut file_names: Vec<String>){
    let mut file_names_checked = vec![];

    while !file_names.is_empty(){
        file_names_checked.push(NamedInput::new(file_names[file_names.len() - 1].as_str()));
        file_names.pop();
    }

    let logger = LoggerObserver::new(file_names_checked , ctx.packet_handle());
    eng.obs_manager.load_observer(logger);
}