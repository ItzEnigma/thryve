// This file contains all the logic related to the service itself.
// It serves as the ground basis that will instanciate the service, initialize it, manage it, and finalize it.

use windows_services::{ Command, Service};
use crossbeam::channel::bounded;
use std::{sync::{atomic::{AtomicBool, Ordering} , Arc, Mutex}, thread, time::Duration};

use tabamt_utils::cfg::{config::Config, ctx::Ctx};
use tabamt_utils::debug_logging::log;
use tabamt_utils::packets::packet_handle::PacketHandle;

use crate::init::init::init_engine;

// The main struct holding the service instance and service information.
pub struct TaBaMT <'a>{
    is_running: Arc<AtomicBool>,        // Thread-safe shared reference to a flag for maintaining engine runs until finalized               | No Permissions.
    is_paused: Arc<AtomicBool>,         // Thread-safe shared reference to a flag for checking whether the service is paused or not         | No Permissions.
    service_instance: Service<'a>,      // The service instance                                                                             | No Permissions.
}

// Implementing the functional methods.
// These will be the functions that will allow for the creation of a service and is_running the service.
impl <'a> TaBaMT <'a> {

    // Creating a new service instance.
    // @param max_threads: Option<usize> - the maximum number of threads the service can handle at once.
    // @param svc_name: &str - the name of the service.
    // @param can_pause: Option<bool> - flag to allow the service to be paused, default: false.
    // @param can_stop: Option<bool> - flag to allow the service to be stopped, default: false.
    // @return: (Self, Receiver<Task>, Receiver<Notification>) - a tuple containing the service instance and the receiver parts of both channels.
    pub fn new(config: Config) -> (Self , Ctx)  {
        
        // Creating the channel that will be used to send tasks to the engine.
        let (task_tx , task_rx) = bounded(config.pipes.task_pipe_capacity);
        let (notif_tx , notif_rx) = bounded(config.pipes.notif_pipe_capacity);
        
        // Creating the service instance.
        let mut svc: TaBaMT<'a> = TaBaMT{
            is_running: Arc::new(AtomicBool::new(false)),
            is_paused: Arc::new(AtomicBool::new(false)),
            service_instance: Service::new(),
        };
        
        
        if config.service.can_pause {
            // If the service can be paused, then set the service instance to be pausable.
            svc.service_instance.can_pause();
        }
        
        if config.service.can_stop {
            // If the service can be stopped, then set the service instance to be stoppable.
            svc.service_instance.can_stop();
        }
        
        let ctx = Ctx::new(config, task_rx, notif_rx, PacketHandle::new(task_tx , notif_tx));
        (svc , ctx)
    }

    // is_Running the service.
    // This function will take ownership of the receiver part of the channel, the observers vector, and the plugins vector.
    // It checks for the start command and runs the engine in a separate thread.
    // It also checks for the stop command and stops the engine.
    // @param task_rx: Receiver<Task> The receiver part of the channel to be handed to the engine.
    // @param notif_rx: Receiver<Box<dyn NotifEX>> The receiver part of the notification channel to be handed to the engine.
    // @param obs_init_tasks: Vec<Task> A vector of tasks to initialize observers
    // @param plugin_init_tasks: Vec<Task> A vector of tasks to initialize
    // @param logging_file_names: Vec<NamedInput> A vector of log file names to initialize the logger with.
    // return: -
    pub fn run_service(&mut self , ctx: Ctx){
        // Cloning the is_running flag and the receiver to be used in the thread.
        let is_running: Arc<AtomicBool> = self.is_running.clone();
        let pause_flag: Arc<AtomicBool> = self.is_paused.clone();

        let engine_thread: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::new(Mutex::new(None));
        let engine_thread_clone = engine_thread.clone();

        let len_obs_tasks = ctx.observers.len();
        let len_plugin_tasks = ctx.plugins.len();
        let mut ctx = Some(ctx);

        // is_Running the service instance.
        // The service instance will call the closure whenever a command is received.
        // The closure will check for the command and run or stop the engine accordingly.
        // The engine will run in a separate thread.
        // The engine will run until the is_running flag is set to false.
        // The engine will finalize itself when the is_running flag is set to false.
        let _ = self.service_instance.run(move |_service, command| match command {
            Command::Start => {
                // Setting the is_running flag to true.
                is_running.store(true, Ordering::SeqCst);
                let is_running_clone = is_running.clone();
                let pause_flag_clone = pause_flag.clone();

                let mut ctx = Some(ctx.take().expect("Start Called More Than Once"));
                
                // Spawning the thread that will run the engine.
                let handle = thread::spawn(move || {
                    let mut ctx = ctx.take().expect("Start Called More Than Once");
                    let mut eng = init_engine(&ctx);
                    
                    let mut state: u8 = 0;
                    let mut eng_paused = false;

                    // is_Running the engine until the is_running flag is set to false.
                    while is_running_clone.load(Ordering::SeqCst) {
                        // Logic for pausing. Just looping until the service is stopped or until the service is continued.
                        let is_pause_flag_loaded = pause_flag_clone.load(Ordering::SeqCst);
                        
                        if eng_paused && is_pause_flag_loaded{
                            thread::sleep(Duration::from_millis(500));
                            continue;
                        }
                        
                        if is_pause_flag_loaded{    
                            eng.pause();
                            eng_paused = true;
                        }

                        // When the service resumes, turn the paused flag off.
                        else if eng_paused {
                            eng.resume();
                            eng_paused = false;
                        } 
                        
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
                            // This needs to be done after the logger is initialized.
                            if len_obs_tasks > 0{
                                for obs in ctx.observers(){
                                    eng.obs_manager.load_observer(obs);
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
                                for plugin in ctx.plugins(){
                                    eng.plugin_manager.load_plugin(plugin);
                                }
                            }
                            
                            // The engine is now fully initialized.
                            // This thread sleep is only here to allow in debugging to open the log file and see the initialization messages.
                            if ctx.config.debugging.is_debugging{
                                thread::sleep(Duration::from_secs(ctx.config.debugging.wait_before_execution));
                            }
                        }
                    }

                    // Finalizing the engine.
                    // This will stop all threads in the thread pool and wait for them to finish.
                    // This is only called when the is_running flag is set to false.
                    eng.finalize();
                });

                *engine_thread_clone.lock().unwrap() = Some(handle);

            }
            
            Command::Stop => {
                // Setting the is_running flag to false.
                // This will cause the engine to finalize itself in the next iteration of the loop above and stop all threads in the thread pool.
                is_running.store(false, Ordering::SeqCst);

                if let Some(handle) = engine_thread.lock().unwrap().take() {
                    if let Err(e) = handle.join() {
                        log(format!("Engine thread join error: {:?}", e));
                    }
                }
            }
            
            Command::Pause => {
                pause_flag.store(true, Ordering::SeqCst);
            }
            
            Command::Resume => {
                pause_flag.store(false, Ordering::SeqCst);
            }
            _ => {}
        });
    }
}
