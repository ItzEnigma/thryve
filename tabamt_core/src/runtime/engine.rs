//! The Engine is the core component of the runtime, responsible for managing the execution flow of tasks and notifications.
//! It maintains the main loop that checks for new tasks, processes notifications, and handles results from task executions.
//! The Engine also manages the lifecycle of the application, allowing for graceful shutdowns and pauses.

use crate::runtime::{observer_manager::ObserverManager, plugin_manager::PluginManager};

use tabamt_utils::bases::notif::Notif;
use tabamt_utils::cfg::ctx::Ctx;
use tabamt_utils::packets::{notif_log::Log , task::Task};
use tabamt_utils::res;
use tabamt_utils::traits::queue::Queue;

use std::{sync::Arc, thread, time::Duration};
use crossbeam::channel::{Receiver , bounded };

/// Encapsulates the core functionality of the runtime, managing the execution flow of tasks and notifications.
pub struct Engine {
    /// Defines how many milliseconds the engine should sleep when idle before the next iteration.
    sleep_tick_rate: u64,

    /// Manages observers and their notifications.
    pub obs_manager: ObserverManager,

    /// Manages plugins and the lifecycle of their tasks.
    pub plugin_manager: PluginManager,
    
    /// Receives tasks from the service to be executed by plugins.
    task_rx: Arc<Receiver<Task>>,

    /// Receives notifications from observers to be processed by the engine.
    notif_rx: Arc<Receiver<Notif>>,

    /// Receives results from plugin executions to be handled by the engine.
    res_rx: Receiver<(u64 , res::Result)>,

    /// Indicates whether the engine should exit, allowing for graceful shutdowns.
    pub exit_flag: bool,

    /// Indicates whether the engine is paused, allowing for temporary halts in execution.
    pause_flag: bool,

}

impl Engine {
    /// Creates a new instance of the Engine, initializing its components and setting up communication channels.
    /// The Engine is initialized with a `Ctx`, which contains configuration and communication channels.
    pub fn new(ctx: &Ctx) -> Self {
        let (res_tx , res_rx) = bounded(ctx.config.pipes.res_pipe_capacity);

        Self {
            sleep_tick_rate: ctx.config.engine.sleep_tick_rate,
            plugin_manager: PluginManager::new(ctx , res_tx),
            obs_manager: ObserverManager::new(ctx),
            task_rx: ctx.task_rx.clone(),
            notif_rx: ctx.notif_rx.clone(),
            res_rx: res_rx,
            exit_flag: false,
            pause_flag: false,
        }
    }
}

impl Engine {
    /// Runs a single iteration of the engine's main loop, checking for new tasks, processing notifications, and handling results.
    /// If the engine is paused or marked for exit, it will only process notifications and skip task execution.
    /// This method is called repeatedly in the main loop of the service to keep the engine responsive to incoming tasks and notifications.
    /// The engine will sleep for a configured duration when idle to prevent excessive CPU usage while waiting for new tasks or notifications.
    pub fn run_once(&mut self){
        // If the engine is marked for exit or is currently paused, it will only check for notifications and skip task execution.
        if self.exit_flag || self.pause_flag{
            self.check_notifs();
            return
        }
        
        // Check for new tasks, notifications, and results.
        // If none are received, the engine will sleep for a configured duration before the next iteration.
        let received_tasks = self.check_tasks();
        let received_notifs = self.check_notifs();
        let received_results = self.check_results();
        
        if !received_tasks && !received_notifs && !received_results {
            thread::sleep(Duration::from_millis(self.sleep_tick_rate));
        }
    }

    /// Checks for new tasks from the service and submits them to the plugin manager for execution.
    fn check_tasks(&self) -> bool {
        let mut received_tasks = false;

        // While there are tasks available in the task receiver, pop them and submit them to the plugin manager's thread pool for execution.
        while let Ok(task) = self.task_rx.try_recv() {
            received_tasks = true;
            self.plugin_manager.threadpool.submit_task(task);
        }
        received_tasks
    }

    /// Checks for new notifications from observers and forwards them to the observer manager.
    fn check_notifs(&mut self) -> bool {
        let mut received_notifs = false;

        // While there are notifications available in the notification receiver, pop them and forward them to the observer manager for processing.
        while let Ok(notif) = self.notif_rx.try_recv() {
            received_notifs = true;
            self.obs_manager.forward_notif(notif);
        }
        received_notifs
    }
    
    /// Checks for new results from plugin executions and handles them accordingly, including submitting any next tasks and logging messages.
    pub fn check_results(&mut self) -> bool {
        let mut received_results = false;

        // While there are results available in the result receiver, pop them and handle them using the `handle_result` method, which processes the result and submits any next tasks if necessary.
        while let Ok(task_result) = self.res_rx.try_recv(){
            received_results = true;
            self.handle_result(task_result);
        };

        received_results
    }

    /// Finalizes the engine by setting the exit flag, finalizing the plugin manager, processing any remaining notifications, and cleaning up resources.
    pub fn finalize(&mut self) {
        // Set the exit flag to true to signal all components to begin shutdown procedures.
        self.exit_flag = true.into();

        // Finalize the plugin manager, which will stop all plugin threads and clean up resources related to plugin execution.
        self.plugin_manager.finalize();
        
        // Process any remaining notifications to ensure all observers have a chance to clean up and log final messages before the engine fully shuts down.
        self.run_once();

        // Finalize the observer manager and clean up resources, ensuring that all logs are flushed and any necessary cleanup is performed before the engine exits.
        // Takes Ownership of the logger to ensure it is properly cleaned up after finalization.
        let logger = self.obs_manager.finalize();
        
        let notif = Log::log("Engine Finalized".to_string() , "Main");
        logger.obs_queue.push(notif);
        
        let notif = Log::log("Service Finalized Successfully".to_string() , "Main");
        logger.obs_queue.push(notif);
        
        logger.clean_up();

    }

    /// to be documented when properly implemented.
    pub fn pause(&mut self) {
        
        self.plugin_manager.threadpool.pause();
        
        
        self.pause_flag = true;
        
        
        let notif = Log::log("Engine Paused".to_string() , "Main");
        self.obs_manager.forward_notif(notif);
        
        
        self.run_once();
        
    }

    /// to be documented when properly implemented.
    pub fn resume(&mut self) {
        
        self.plugin_manager.threadpool.resume();
        
        
        self.pause_flag = false;

        
        let notif = Log::log("Engine continued".to_string() , "Main");
        self.obs_manager.forward_notif(notif);
    }
}

impl Engine {
    /// Handles the result of a task execution, including processing the result message, submitting any next tasks, and logging relevant information.
    fn handle_result(&mut self , task_result: (u64 , res::Result)) {
        
        let result = task_result.1;

        // If the result contains a next task to be executed, submit it to the plugin manager's thread pool for execution.
        if let Some(next_task) = result.next_task{
            self.plugin_manager.threadpool.submit_task(next_task);
        }

        let finished_notif = Log::log(format!("Finished Task ID: {}." , task_result.0) , "Main");
        self.obs_manager.forward_notif(finished_notif);

        
        // Process the result message and log it to the appropriate logfile, as specified in the result.
        let task_res_message = result.message;
        let log_file = result.log_msg_to_file.inner();
        if task_res_message != "".to_string(){
            let finished_notif = Log::log(task_res_message , log_file);
            self.obs_manager.forward_notif(finished_notif);
        }
        
        // If the result indicates a call for shutdown (either successfully or with an error) and the engine is not already marked for exit, call the `finalize` method to begin shutdown procedures.
        let res_type = result.result_type.inner().to_lowercase();
        if (res_type == "fin" || res_type == "finerr") && !self.exit_flag {
            self.finalize();
        }
    }
}