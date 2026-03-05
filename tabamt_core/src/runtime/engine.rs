use crate::runtime::{observer_manager::ObserverManager, plugin_manager::PluginManager};

use tabamt_utils::cfg::ctx::Ctx;
use tabamt_utils::traits::notif::Notif;
use tabamt_utils::packets::{notif_log::NotifLog , task::Task};
use tabamt_utils::res;

use std::{sync::Arc, thread, time::Duration};
use crossbeam::channel::{Receiver , bounded };
// The Engine struct is responsible for managing the overall runtime of the application.
// It holds the ObserverManager, PluginManager, and ThreadPool, and coordinates their interactions.
pub struct Engine {
    // ----CONFIG START
    sleep_tick_rate: u64,
    // ----CONFIG END

    pub obs_manager: ObserverManager,               // Manages observers and forwards notifications to them.    | Read Only.
    pub plugin_manager: PluginManager,              // Manages plugins and their tasks.                         | Read Only.
    

    task_rx: Arc<Receiver<Task>>,              // Receives tasks to be executed.                           | No Permissions.
    notif_rx: Arc<Receiver<Box<dyn Notif>>>,   // Receives notifications to be processed.                  | No Permissions.
    res_rx: Receiver<(u64 , res::Result)>,

    pub exit_flag: bool,                            // Flag to indicate if the engine should exit.              | Read Only.
    pause_flag: bool,                           // Flag to indicate if the engine should pause.             | No Permissions.

}

impl Engine {
    // Creates a new Engine instance with the provided task and notification receivers, and packet handle.
    // @param max_threads: The maximum number of threads in the thread pool.
    // @param task_rx: Receiver for incoming tasks.
    // @param notif_rx: Receiver for incoming notifications.
    // @param packet_handle: Packet handle for sending tasks and notifications.
    // @return: A new Engine instance.
    pub fn new(ctx: &Ctx) -> Self {
        let (res_tx , res_rx) = bounded(ctx.config.pipes.res_pipe_capacity);

        Self {
            sleep_tick_rate: ctx.config.engine.sleep_tick_rate,
            plugin_manager: PluginManager::new(ctx , res_tx),
            obs_manager: ObserverManager::new(),
            task_rx: ctx.task_rx.clone(),
            notif_rx: ctx.notif_rx.clone(),
            res_rx: res_rx,
            exit_flag: false,
            pause_flag: false,
        }
    }
}

impl Engine {
    // Runs the engine, sending tasks and processing notifications until the exit flag is set.
    // This function will not block even when continuously check for new tasks and notifications,
    // It will only run once and return an Option<bool>.
    // the return value is Some(true) only when the logger is loaded, None otherwise.
    // The main loop in the run_service should call this function repeatedly until the application exits.
    pub fn run_once(&mut self) -> Option<bool>{

        // If the exit flag is set, check for any remaining notifications before final shutdown and return None.
        if self.exit_flag || self.pause_flag{
            self.check_notifs();

            return None
        }

        // Continuously check for new tasks, notifications, and results.
        // If none are received, sleep for a short duration to avoid busy-waiting.
        // Return Some(true) if the logger is loaded, None otherwise.
        // This ensures that the engine remains responsive without consuming excessive CPU resources.
        let received_tasks = self.check_tasks();
        let received_notifs = self.check_notifs();
        let received_results = self.check_results();
        
        // If no tasks, notifications, or results were received, sleep for a short duration to avoid busy-waiting.
        //// duration to be implemented as a config later.
        if !received_tasks && !received_notifs && received_results == 0 {
            thread::sleep(Duration::from_millis(self.sleep_tick_rate));
        }
        else if received_results == 2 {
            return Some(true)
        }
        
        return None
        
    }

    // Checks for new tasks from the task receiver and submits them to the thread pool.
    // @return: true if any tasks were received and submitted, false otherwise.
    fn check_tasks(&self) -> bool {
        let mut received_tasks = false;

        // Try to receive tasks without blocking. If tasks are available, submit them to the thread pool.
        while let Ok(task) = self.task_rx.try_recv() {
            received_tasks = true;
            self.plugin_manager.threadpool.submit_task(task);
        }
        received_tasks
    }

    // Checks for new notifications from the notification receiver and forwards them to the observer manager.
    // @return: true if any notifications were received and forwarded, false otherwise.
    fn check_notifs(&mut self) -> bool {
        let mut received_notifs = false;

        // Try to receive notifications without blocking. If notifications are available, forward them to the observer manager.
        while let Ok(notif) = self.notif_rx.try_recv() {
            received_notifs = true;
            self.obs_manager.forward_notif(notif);
        }
        received_notifs
    }
    
    pub fn check_results(&mut self) -> u8 {
        let mut received_results = 0;

        // Try to get results without blocking. If results are available, process them.
        while let Ok(task_result) = self.res_rx.try_recv(){
            received_results = 1;

            // Handle the results and check if the logger is loaded.
            // The firt time this function runs, it will return Some(true) if the logger is loaded.
            // The next time it runs, ret should be none None.
            let ret = self.handle_result(task_result);
            
            // If the logger is loaded, return Some(true) immediately.
            if ret.unwrap_or(false) == true {return 2}
        };

        received_results
    }

    // Finalizes the engine, ensuring all components are properly shut down.
    // This includes finalizing the plugin manager, thread pool, the pool's queue, and observer manager.
    pub fn finalize(&mut self) {
        // Set the exit flag to true to indicate that the engine should stop running.

        self.exit_flag = true.into();
        self.plugin_manager.finalize();
        self.plugin_manager.threadpool.finalize();
        
        // Run the engine once more to process any remaining notifications before shutdown.
        // This ensures that all the finalization messages are logged.
        // Tasks will not be processed as the exit flag is set.
        
        self.run_once();

        // The observer manager is finalized last to ensure all notifications are logged before shutdown.
        let mut logger = self.obs_manager.finalize();
        
        let notif = NotifLog::log("Engine Finalized".to_string() , "Main");
        logger.consume(notif);
        
        let notif = NotifLog::log("Service Finalized Successfully".to_string() , "Main");
        logger.consume(notif);
    }


    // Pauses the engine, stopping task processing and notifying observers.
    pub fn pause(&mut self) {
        // Pause the thread pool to stop processing tasks.
        self.plugin_manager.threadpool.pause();
        
        // Set the pause flag to true to indicate that the engine is paused.
        self.pause_flag = true;
        
        // Forward a notification to the observer_manager that the engine is paused.
        let notif = NotifLog::log("Engine Paused".to_string() , "Main");
        self.obs_manager.forward_notif(notif);
        
        // Run the engine once more to process any remaining notifications before pausing.
        self.run_once();
        
    }

    // Continues the engine, resuming task processing and notifying observers.
    pub fn resume(&mut self) {
        // Continue the thread pool to resume processing tasks.
        self.plugin_manager.threadpool.resume();
        
        // Set the pause flag to false to indicate that the engine is running.
        self.pause_flag = false;

        // Forward a notification to the observer_manager that the engine is continued.
        let notif = NotifLog::log("Engine continued".to_string() , "Main");
        self.obs_manager.forward_notif(notif);
    }
}

impl Engine {
    // Handles the results of completed tasks, forwarding notifications and loading new observers or plugins as needed.
    // @param task_results: A vector of tuples containing task IDs and their corresponding results.
    // @return: An Option<bool> indicating if the logger is loaded (Some(true)) or not (None).
    fn handle_result(&mut self , task_result: (u64 , res::Result)) -> Option<bool> {
        let mut ret: Option<bool> = None;

        // Iterate over each task result and process it.
        let result = task_result.1;
        
        // If the task result includes a new observer, load it into the observer manager.
        // If the observer is of type "Logging", set ret to Some(true) to indicate that the logger is loaded.
        if let Some(obs) = result.observer{
            let observer_type = obs.observer_type().clone();
            if observer_type.inner() == "Logging" {
                ret = Some(true);
            }

            self.obs_manager.load_observer(obs);
        }

        // If the task result includes a new plugin, load it into the plugin manager.
        else if let Some(plugin) = result.plugin{
            self.plugin_manager.load_plugin(plugin);
        }

        // If the task result includes a next task to be executed, submit it to the thread pool.
        if let Some(next_task) = result.next_task{
            self.plugin_manager.threadpool.submit_task(next_task);
        }

        // Forward a notification indicating that the task has finished.
        let finished_notif = NotifLog::log(format!("Finished Task ID: {}." , task_result.0) , "Main");
        self.obs_manager.forward_notif(finished_notif);

        // If the task result includes a message to be logged, forward it to the appropriate observer.
        // The message will be logged to the file specified in the task result.
        let task_res_message = result.message;
        let log_file = result.log_msg_to_file.inner();
        if task_res_message != "".to_string(){
            let finished_notif = NotifLog::log(task_res_message , log_file);
            self.obs_manager.forward_notif(finished_notif);
        }

        // If the result type is "Fin", finalize the engine.
        let res_type = result.result_type.inner().to_lowercase();
        if (res_type == "fin" || res_type == "finerr") && !self.exit_flag {
            self.finalize();
            return ret
        }
        return ret
    }
}