// This file contains all the logic related to the service itself.
// It serves as the ground basis that will instanciate the service, initialize it, manage it, and finalize it.

use windows_services::{ Command, Service};
use tabamt_core::runtime::engine::Engine;
use std::{sync::{atomic::{AtomicBool, Ordering}, mpsc::{self, Receiver}, Arc, Mutex}, thread, time::Duration};

use tabamt_utils::{any_naming_input::ANNamingInput, debug_logging::log, notif::Notif, packet_handle::PacketHandle, task::Task};
use crate::init::init;

// The main struct holding the service instance and service information.
pub struct TaBaMT <'a>{
    is_running: Arc<AtomicBool>,        // Thread-safe shared reference to a flag for maintaining engine runs until finalized               | No Permissions.
    is_paused: Arc<AtomicBool>,         // Thread-safe shared reference to a flag for checking whether the service is paused or not         | No Permissions.
    service_instance: Service<'a>,      // The service instance                                                                             | No Permissions.
    can_pause: bool,                    // flag allows service to be paused                                                                 | Read Only.
    can_stop: bool,                     // flag allows service to be stopped                                                                | Read Only.
    packet_handle: PacketHandle         // The packet handle to send tasks and notifications                                                | No Permissions.
}

// Getters and Setters
impl <'a> TaBaMT <'a> {
    // Getter to get the is_running flag of the service
    // @return: bool - the is_running flag of the service
    pub fn _get_can_pause(&self) -> bool{
        self.can_pause
    }

    // Getter to get the can_stop flag of the service
    // @return: bool - the can_stop flag of the service
    pub fn _get_can_stop(&self) -> bool{
        self.can_stop
    }

    // Getter to get a clone of the task handle
    // @return: PacketHandle - a clone of the task handle
    pub fn get_packet_handle(&self) -> PacketHandle {
        self.packet_handle.clone()
    }
}

// Implementing the functional methods.
// These will be the functions that will allow for the creation of a service and is_running the service.
impl <'a> TaBaMT <'a> {

    // Creating a new service instance.
    // @param svc_name: &str - the name of the service.
    // @param can_pause: Option<bool> - flag to allow the service to be paused, default: false.
    // @param can_stop: Option<bool> - flag to allow the service to be stopped, default: false.
    // @return: (Self, Receiver<Task>, Receiver<Notification>) - a tuple containing the service instance and the receiver parts of both channels.
    pub fn new(can_pause: Option<bool> , can_stop: Option<bool>) -> (Self , Receiver<Task> , Receiver<Box<dyn Notif>>) {

        // Creating the channel that will be used to send tasks to the engine.
        let (task_tx , task_rx) = mpsc::channel();
        let (notif_tx , notif_rx) = mpsc::channel();

        // Creating the service instance.
        let mut svc: TaBaMT<'a> = TaBaMT{
            is_running: Arc::new(AtomicBool::new(false)),
            is_paused: Arc::new(AtomicBool::new(false)),
            service_instance: Service::new(),
            can_pause: can_pause.unwrap_or_else(|| true),
            can_stop: can_stop.unwrap_or_else(|| true),
            packet_handle: PacketHandle::new(task_tx , notif_tx),
        };


        if svc.can_pause {
            // If the service can be paused, then set the service instance to be pausable.
            svc.service_instance.can_pause();
        }

        if svc.can_stop {
            // If the service can be stopped, then set the service instance to be stoppable.
            svc.service_instance.can_stop();
        }

        (svc , task_rx , notif_rx)
    }

    // is_Running the service.
    // This function will take ownership of the receiver part of the channel, the observers vector, and the plugins vector.
    // It checks for the start command and runs the engine in a separate thread.
    // It also checks for the stop command and stops the engine.
    // @param rx: Receiver<Task> The receiver part of the channel to be handed to the engine.
    // @param obs_init_tasks: Vec<Task> A vector of tasks to initialize observers
    // @param plugin_init_tasks: Vec<Task> A vector of tasks to initialize
    // return: -
    pub fn run_service(&mut self , task_rx: Receiver<Task> , notif_rx: Receiver<Box<dyn Notif>>, obs_init_tasks: Vec<Task> , plugin_init_tasks: Vec<Task> , logging_file_names: Vec<ANNamingInput>){
        // Cloning the is_running flag and the receiver to be used in the thread.
        let is_running: Arc<AtomicBool> = self.is_running.clone();
        let pause_flag: Arc<AtomicBool> = self.is_paused.clone();
        let rx = Arc::new(Mutex::new(Some((task_rx , notif_rx))));
        
        // Cloning the task handle to be used in the thread.
        let packet_handle = self.packet_handle.clone();

        // Storing the lengths of the observers and plugins vectors.
        let len_obs_tasks = obs_init_tasks.len();
        let len_plugin_tasks = plugin_init_tasks.len();

        // Wrapping the observers, plugins, and log-file-name vectors in Options to allow for taking ownership in the thread.
        let mut obs_init_tasks: Option<Vec<Task>> = Some(obs_init_tasks);
        let mut plugin_init_tasks: Option<Vec<Task>> = Some(plugin_init_tasks);
        let mut logging_file_names: Option<Vec<ANNamingInput>> = Some(logging_file_names);

        let engine_thread: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::new(Mutex::new(None));
        let engine_thread_clone = engine_thread.clone();

        // is_Running the service instance.
        // The service instance will call the closure whenever a command is received.
        // The closure will check for the command and run or stop the engine accordingly.
        // The engine will run in a separate thread.
        // The engine will run until the is_running flag is set to false.
        // The engine will finalize itself when the is_running flag is set to false.
        let _ = self.service_instance.run(move |_service, command| match command {
            Command::Start => {
                // Cloning the observers, plugins and log-file-name vectors to be used in the thread.
                let mut obs_init_tasks_clone: Vec<Task> = Vec::new();
                if let Some(obs_clone) = obs_init_tasks.take() {
                    obs_init_tasks_clone = Vec::from(obs_clone);
                }

                let mut plugin_init_tasks_clone: Vec<Task> = Vec::new();
                if let Some(plugins_clone) = plugin_init_tasks.take() {
                    plugin_init_tasks_clone = Vec::from(plugins_clone);
                }

                let mut logging_file_names_clone: Vec<ANNamingInput> = Vec::new();
                if let Some(file_names_clone) = logging_file_names.take() {
                    logging_file_names_clone = Vec::from(file_names_clone);
                }

                // If the receiver is available, then run the engine in a separate thread.
                if let Some(rec) = rx.lock().unwrap().take() {
                    // wait_for_debugger("TestService");

                    // Setting the is_running flag to true.
                    is_running.store(true, Ordering::SeqCst);
                    let is_running_clone = is_running.clone();
                    let pause_flag_clone = pause_flag.clone();
                    
                    // Cloning the task handle to be used in the thread.
                    let t_handle_clone = packet_handle.clone();

                    // Spawning the thread that will run the engine.
                    let handle = thread::spawn(move || {
                        // Taking ownership of the observers and plugins vectors.
                        let mut obs_init_tasks_clone = Some(obs_init_tasks_clone);
                        let mut plugin_init_tasks_clone = Some(plugin_init_tasks_clone);
                        let mut logging_file_names_clone = Some(logging_file_names_clone);
                        
                        // Creating the engine instance.
                        let mut eng = Engine::new(rec.0 , rec.1 , t_handle_clone.clone());

                        // This spawns the threads in the thread pool and makes them wait for tasks.
                        eng.get_thread_pool().run_threads();

                        // Initializing the logger first.
                        if let Some(file_names) = logging_file_names_clone.take() {
                            // The initialization function takes ownership of the vector.
                            init::init_logger(t_handle_clone.clone() , file_names);
                        }
                        
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
                                eng.cont();
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
                                    // The option is taken here to move the vector into the init function, meaning the option is unwrapped to reveal the vector.
                                    if let Some(obs) = obs_init_tasks_clone.take() {
                                        // The initialization function takes ownership of the vector.
                                        init::init(obs, t_handle_clone.clone());
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
                                    // The option is taken here to move the vector into the init function, meaning the option is unwrapped to reveal the vector.
                                    if let Some(plugins) = plugin_init_tasks_clone.take() {
                                        // The initialization function takes ownership of the vector.
                                        init::init(plugins, t_handle_clone.clone());
                                    }
                                }
                                
                                // The engine is now fully initialized.
                                // This thread sleep is only here to allow in debugging to open the log file and see the initialization messages.
                                // It can and should be removed in production.
                                thread::sleep(Duration::from_secs(10));
                            }
                        }

                        // Finalizing the engine.
                        // This will stop all threads in the thread pool and wait for them to finish.
                        // This is only called when the is_running flag is set to false.
                        eng.finalize();
                    });

                    *engine_thread_clone.lock().unwrap() = Some(handle);
                }

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
