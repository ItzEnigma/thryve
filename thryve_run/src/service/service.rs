//! Windows service implementation for the Thryve engine.
//!
//! This module provides the core functionality for running the Thryve engine as a
//! Windows service, enabling background execution with system-level integration.
//!
//! # Overview
//!
//! The [`Thryve`] struct encapsulates a Windows service instance and manages its
//! lifecycle, including start, stop, pause, and resume operations. It coordinates
//! with the Thryve engine to process tasks and notifications while responding to
//! service control manager commands.
//!
//! # Service Lifecycle
//!
//! 1. **Creation**: A new [`Thryve`] instance is created with a [`Config`]
//! 2. **Registration**: The service registers with the Windows Service Control Manager
//! 3. **Execution**: The service listens for commands and manages the engine lifecycle
//! 4. **Termination**: The service gracefully shuts down the engine on stop commands
//!
//! # Supported Commands
//!
//! - **Start**: Initializes and runs the engine in a dedicated thread
//! - **Stop**: Gracefully shuts down the engine and terminates the service
//! - **Pause**: Suspends engine processing while maintaining state
//! - **Resume**: Resumes engine processing after a pause
//!
//! # Example
//!
//! ```no_run
//! use thryve_utils::cfg::config::Config;
//! use thryve_run::service::service::Thryve;
//!
//! let config = Config::default();
//! let (mut service, ctx) = Thryve::new(config);
//! service.run_service(ctx);
//! ```

use windows_services::{ Command, Service};
use crossbeam::channel::bounded;
use std::{sync::{atomic::{AtomicBool, Ordering} , Arc, Mutex}, thread, time::Duration};

use thryve_utils::cfg::{config::Config, ctx::Ctx};
use thryve_utils::debug_logging::log;
use thryve_utils::packets::packet_handle::PacketHandle;

use crate::init::init::init_engine;

/// A Windows service wrapper for the Thryve engine.
///
/// This struct encapsulates a Windows service instance and manages the lifecycle
/// of the Thryve engine when running as a service. It handles service control
/// commands (start, stop, pause, resume) and coordinates engine execution.
///
/// # Fields
///
/// - `is_running`: Atomic flag indicating whether the service is currently running
/// - `is_paused`: Atomic flag indicating whether the service is currently paused
/// - `service_instance`: The underlying Windows service instance
///
/// # Lifetime Parameter
///
/// - `'a`: The lifetime of the service instance, tied to the Windows service handle
///
/// # Example
///
/// ```no_run
/// use thryve_utils::cfg::config::Config;
/// use thryve_run::service::service::Thryve;
///
/// let config = Config::default();
/// let (mut service, ctx) = Thryve::new(config);
/// service.run_service(ctx);
/// ```
pub struct Thryve <'a>{
    is_running: Arc<AtomicBool>,
    is_paused: Arc<AtomicBool>,
    service_instance: Service<'a>,
}



impl <'a> Thryve <'a> {

    /// Creates a new Thryve service instance with the given configuration.
    ///
    /// This constructor initializes the Windows service with the specified capabilities
    /// (pause/stop) based on the configuration, creates the communication channels
    /// for task and notification pipes, and returns both the service instance and
    /// the context required for engine execution.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration containing service capabilities and pipe settings
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A new [`Thryve`] service instance configured with the specified capabilities
    /// - A [`Ctx`] instance with initialized communication channels
    ///
    /// # Service Capabilities
    ///
    /// The service capabilities are configured based on the `config.service` settings:
    /// - If `can_pause` is true, the service supports pause/resume commands
    /// - If `can_stop` is true, the service supports stop commands
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::config::Config;
    /// use thryve_run::service::service::Thryve;
    ///
    /// let config = Config::default();
    /// let (service, ctx) = Thryve::new(config);
    /// ```
    pub fn new(config: Config) -> (Self , Ctx)  {
        
        // Create the channels that will be used to send tasks and notifications to the engine.
        let (task_tx , task_rx) = bounded(config.pipes.task_pipe_capacity);
        let (notif_tx , notif_rx) = bounded(config.pipes.notif_pipe_capacity);
        
        // Initialize the service struct with the created channels and the service instance.
        let mut svc: Thryve<'a> = Thryve{
            is_running: Arc::new(AtomicBool::new(false)),
            is_paused: Arc::new(AtomicBool::new(false)),
            service_instance: Service::new(),
        };
        
        // Set the service capabilities based on the configuration.
        if config.service.can_pause {
            svc.service_instance.can_pause();
        }
        
        if config.service.can_stop {
            svc.service_instance.can_stop();
        }
        
        // Create the context that will be passed to the engine, containing the configuration, channels, and packet handle.
        let ctx = Ctx::new(config, task_rx, notif_rx, PacketHandle::new(task_tx , notif_tx));
        
        // Return the service instance and the context.
        (svc , ctx)
    }

    /// Runs the Thryve service and manages its execution lifecycle.
    ///
    /// This is the primary entry point for service execution. It registers the service
    /// with the Windows Service Control Manager and begins listening for control commands.
    /// The engine runs in a dedicated thread to ensure the service remains responsive
    /// to control commands at all times.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The context containing configuration, observers, plugins, and communication channels
    ///
    /// # Behavior
    ///
    /// The service responds to the following commands:
    ///
    /// - **Start**: Spawns the engine in a dedicated thread, loads observers and plugins,
    ///   and begins processing tasks and notifications
    /// - **Stop**: Signals the engine to shut down, waits for the engine thread to complete,
    ///   and performs cleanup
    /// - **Pause**: Sets the pause flag, causing the engine to suspend processing
    /// - **Resume**: Clears the pause flag, allowing the engine to resume processing
    ///
    /// # Engine Lifecycle
    ///
    /// On start, the engine:
    /// 1. Initializes with the provided context
    /// 2. Loads all configured observers
    /// 3. Loads all configured plugins
    /// 4. Starts the observer sorter thread
    /// 5. Enters the main processing loop
    ///
    /// On stop, the engine:
    /// 1. Exits the main processing loop
    /// 2. Performs finalization and cleanup
    ///
    /// # Panics
    ///
    /// Panics if the Start command is received more than once without a corresponding Stop.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::cfg::config::Config;
    /// use thryve_run::service::service::Thryve;
    ///
    /// let config = Config::default();
    /// let (mut service, ctx) = Thryve::new(config);
    /// service.run_service(ctx);
    /// ```
    pub fn run_service(&mut self , ctx: Ctx){
        // Cloneing the atomic flags for running and paused states to be used in the service command handling.
        let is_running: Arc<AtomicBool> = self.is_running.clone();
        let pause_flag: Arc<AtomicBool> = self.is_paused.clone();

        // Create a thread handle that will be used to run the engine in a separate thread.
        let engine_thread: Arc<Mutex<Option<std::thread::JoinHandle<()>>>> = Arc::new(Mutex::new(None));
        let engine_thread_clone = engine_thread.clone();
        
        let len_obs_tasks = ctx.observers.len();
        let len_plugin_tasks = ctx.plugins.len();
        let mut ctx = Some(ctx);

        // The service instance will call the closure whenever a command is received.
        // The closure will check for the command and run or stop the engine accordingly.
        // The engine will run in a separate thread.
        // The engine will run until the is_running flag is set to false.
        // The engine will finalize itself when the is_running flag is set to false.
        let _ = self.service_instance.run(move |_service, command| match command {
            Command::Start => {
                
                is_running.store(true, Ordering::SeqCst);
                let is_running_clone = is_running.clone();
                let pause_flag_clone = pause_flag.clone();

                let mut ctx = Some(ctx.take().expect("Start Called More Than Once"));
                
                // Spawn a new thread to run the engine so that the service can continue to listen for commands.
                let handle = thread::spawn(move || {
                    let mut ctx = ctx.take().expect("Start Called More Than Once");
                    let mut eng = init_engine(&ctx);
                    
                    let mut state: u8 = 0;

                    // The engine will run in a loop until the is_running flag is set to false.
                    while is_running_clone.load(Ordering::SeqCst) {
                        // Check the pause flag and pause or resume the engine accordingly.
                        let engine_is_paused = eng.pause_flag;
                        let is_pause_flag_loaded = pause_flag_clone.load(Ordering::SeqCst);
                        
                        // If the engine is already paused and the pause flag is still loaded, we just wait and continue the loop without calling run_once.
                        if engine_is_paused && is_pause_flag_loaded{
                            thread::sleep(Duration::from_millis(500));
                            continue;
                        }
                        
                        // If the pause flag is loaded and the engine is not paused, the engine gets paused.
                        if is_pause_flag_loaded && !engine_is_paused{
                            eng.pause();
                        }

                        // If the pause flag is not loaded and the engine is paused, the engine gets resumed.
                        else if !is_pause_flag_loaded && engine_is_paused {
                            eng.resume();
                        }

                        // During the first iteration of the loop, perform the initial setup of the logger observer and the observer sorter thread.
                        if state == 0 {
                            state += 1;
                            
                            // Load the observers and plugins into the engine if there are any specified in the context.
                            if len_obs_tasks > 0{
                                for obs in ctx.observers(){
                                    eng.obs_manager.load_observer(obs);
                                }
                            }
  
                            // Load the plugins into the engine if there are any specified in the context.
                            if len_plugin_tasks > 0 {
                                for plugin in ctx.plugins(){
                                    eng.plugin_manager.load_plugin(&ctx.config , plugin);
                                }
                            }
                            
                            // Initialize the observer sorter thread responsible for sorting incoming notifications and forwarding them to the correct observer queues.
                            eng.obs_manager.init_obs_sorter_thread();

                            // Handle any debugging wait time if specified in the configuration.
                            if ctx.config.debugging.is_debugging{
                                thread::sleep(Duration::from_secs(ctx.config.debugging.wait_before_execution));
                            }
                        }

                        // Run the engine's main loop once. This will process any tasks or notifications that are in the channels.
                        eng.run_once();
                    }
                    
                    // Finalize the engine by performing any necessary cleanup operations before shutting down.
                    eng.finalize();
                });
                
                // Store the thread handle in the engine_thread variable so that we can join it later when stopping the service.
                *engine_thread_clone.lock().unwrap() = Some(handle);
            }
            
            // When the stop command is received, the is_running flag is set to false to signal the engine to stop running and finalize itself.
            Command::Stop => {
                // Set the is_running flag to false to signal the engine to stop running and finalize itself.
                is_running.store(false, Ordering::SeqCst);

                if let Some(handle) = engine_thread.lock().unwrap().take() {
                    if let Err(e) = handle.join() {
                        log(format!("Engine thread join error: {:?}", e));
                    }
                }
            }
            
            // When the pause command is received, the pause flag is set to true to signal the engine to pause its execution.
            Command::Pause => {
                pause_flag.store(true, Ordering::SeqCst);
            }
            
            // When the resume command is received, the pause flag is set to false to signal the engine to resume its execution.
            Command::Resume => {
                pause_flag.store(false, Ordering::SeqCst);
            }
            _ => {}
        });
    }
}
