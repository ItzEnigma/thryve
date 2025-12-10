use crate::runtime::{observer_manager::ObserverManager, plugin_manager::PluginManager};
use tabamt_utils::{any_naming_input::ANNamingInput, notif_log::NotifLog , notif::Notif , packet_handle::PacketHandle, res, res_bullitin::ResBullitin, task::Task, threadpool::ThreadPool};
use std::{sync::{mpsc , Arc , Mutex}, thread, time::Duration};

// The Engine struct is responsible for managing the overall runtime of the application.
// It holds the ObserverManager, PluginManager, and ThreadPool, and coordinates their interactions.
pub struct Engine {
    obs_manager: ObserverManager,               // Manages observers and forwards notifications to them.    | Read Only.
    plugin_manager: PluginManager,              // Manages plugins and their tasks.                         | Read Only.
    threadpool: ThreadPool,                     // Manages a pool of threads for executing tasks.           | Read Only.

    task_rx: mpsc::Receiver<Task>,              // Receives tasks to be executed.                           | No Permissions.
    notif_rx: mpsc::Receiver<Box<dyn Notif>>,     // Receives notifications to be processed.                  | No Permissions.

    exit_flag: bool,                            // Flag to indicate if the engine should exit.              | No Permissions.
    pause_flag: bool,                           // Flag to indicate if the engine should pause.             | No Permissions.
}

impl Engine {
    // Creates a new Engine instance with the provided task and notification receivers, and packet handle.
    // @param task_rx: Receiver for incoming tasks.
    // @param notif_rx: Receiver for incoming notifications.
    // @param packet_handle: Packet handle for sending tasks and notifications.
    // @return: A new Engine instance.
    pub fn new(task_rx: mpsc::Receiver<Task> , notif_rx: mpsc::Receiver<Box<dyn Notif>> , packet_handle: PacketHandle) -> Self {
        Self {
            plugin_manager: PluginManager::new(packet_handle.clone()),
            obs_manager: ObserverManager::new(),
            threadpool: ThreadPool::new(4 , packet_handle.clone()),     // Default to 4 threads in the pool.
            task_rx: task_rx,
            notif_rx: notif_rx,
            exit_flag: false,
            pause_flag: false,
        }
    }

    // Getters for accessing the internal components of the Engine.
    // @return: References to the ObserverManager, PluginManager, and ThreadPool.
    pub fn get_obs_manager(&self) -> &ObserverManager{
        &self.obs_manager
    }

    // @return: References to the ObserverManager, PluginManager, and ThreadPool.
    pub fn get_plugin_manager(&self) -> &PluginManager{
        &self.plugin_manager
    }

    // @return: References to the ObserverManager, PluginManager, and ThreadPool.
    pub fn get_thread_pool(&self) -> &ThreadPool {
        &self.threadpool
    }

    pub fn get_exit_flag(&self) -> bool {
        self.exit_flag
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
        let mut received_results = false;
        
        {
            // Check for completed task results from the thread pool.
            // If results are available, handle them and check if the logger is loaded.
            // The lock is needed to safely access the shared ResBullitin.
            let lock: Arc<Mutex<ResBullitin>> = self.threadpool.get_res_bullitin();
            let mut results = lock.lock().unwrap();

            // Try to get results without blocking. If results are available, process them.
            if let Ok(task_results) = results.try_get(){
                received_results = true;

                // Handle the results and check if the logger is loaded.
                // The firt time this function runs, it will return Some(true) if the logger is loaded.
                // The next time it runs, ret should be none None.
                let ret = self.handle_results(task_results);
                
                // If the logger is loaded, return Some(true) immediately.
                if ret.unwrap_or(false) == true {return ret}
            }
        }
        
        // If no tasks, notifications, or results were received, sleep for a short duration to avoid busy-waiting.
        //// duration to be implemented as a config later.
        if !received_tasks && !received_results && !received_notifs {
            thread::sleep(Duration::from_millis(10));
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
            self.threadpool.submit_task(task);
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
    
    // Finalizes the engine, ensuring all components are properly shut down.
    // This includes finalizing the plugin manager, thread pool, the pool's queue, and observer manager.
    pub fn finalize(&mut self) {
        // Set the exit flag to true to indicate that the engine should stop running.

        self.exit_flag = true.into();
        self.plugin_manager.finalize();
        self.threadpool.finalize();
        
        // Run the engine once more to process any remaining notifications before shutdown.
        // This ensures that all the finalization messages are logged.
        // Tasks will not be processed as the exit flag is set.
        
        self.run_once();

        // The observer manager is finalized last to ensure all notifications are logged before shutdown.
        let mut logger = self.obs_manager.finalize();
        
        let notif = NotifLog::log("Engine Finalized".to_string() , ANNamingInput::new("Main"));
        logger.consume(Box::new(notif));
        
        let notif = NotifLog::log("Service Finalized Successfully".to_string() , ANNamingInput::new("Main"));
        logger.consume(Box::new(notif));
    }

    pub fn pause(&mut self) {
        self.threadpool.pause();
        
        self.pause_flag = true;
        
        // Forward a notification to the observer_manager that the engine is paused.
        let notif = NotifLog::log("Engine Paused".to_string() , ANNamingInput::new("Main"));
        self.obs_manager.forward_notif(Box::new(notif));
        
        self.run_once();
        
    }

    pub fn cont(&mut self) {
        self.pause_flag = false;
        self.threadpool.cont();

        // Forward a notification to the observer_manager that the engine is continued.
        let notif = NotifLog::log("Engine continued".to_string() , ANNamingInput::new("Main"));
        self.obs_manager.forward_notif(Box::new(notif));
    }
}

impl Engine {
    // Handles the results of completed tasks, forwarding notifications and loading new observers or plugins as needed.
    // @param task_results: A vector of tuples containing task IDs and their corresponding results.
    // @return: An Option<bool> indicating if the logger is loaded (Some(true)) or not (None).
    fn handle_results(&mut self , task_results: Vec<(u64 , res::Result)>) -> Option<bool> {
        let mut ret: Option<bool> = None;

        // Iterate over each task result and process it.
        for task_tuple in task_results{
            let mut task_result = task_tuple.1;
            
            // If the task result includes a new observer, load it into the observer manager.
            // If the observer is of type "Logging", set ret to Some(true) to indicate that the logger is loaded.
            if let Some(obs) = task_result.get_obs(){
                let observer_type = obs.get_observer_type().clone();
                if observer_type == ANNamingInput::new("Logging") {
                    ret = Some(true);
                }

                self.obs_manager.load_observer(obs);
            }

            // If the task result includes a new plugin, load it into the plugin manager.
            else if let Some(plugin) = task_result.get_plugin(){
                self.plugin_manager.load_plugin(plugin);
            }

            // If the task result includes a next task to be executed, submit it to the thread pool.
            if let Some(next_task) = task_result.get_next_task(){
                self.threadpool.submit_task(next_task);
            }

            // Forward a notification indicating that the task has finished.
            let finished_notif = NotifLog::log(format!("Finished Task ID: {}." , task_tuple.0) , ANNamingInput::new("Main"));
            self.obs_manager.forward_notif(Box::new(finished_notif));

            // If the task result includes a message to be logged, forward it to the appropriate observer.
            // The message will be logged to the file specified in the task result.
            let task_res_message = task_result.get_message();
            let log_file = task_result.get_log_to_file();
            if task_res_message != "".to_string(){
                let finished_notif = NotifLog::log(task_res_message , log_file);
                self.obs_manager.forward_notif(Box::new(finished_notif));
            }

            // If the result type is "Fin", finalize the engine.
            if task_result.get_res_type().get_inner() == "Fin"{
                self.finalize();
                return ret
            }
        }
        return ret
    }
}