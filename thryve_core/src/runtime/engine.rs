//! Core engine for the Thryve runtime.
//!
//! This module provides the [`Engine`] struct, which is the central
//! component of the Thryve runtime. The engine manages the execution
//! flow of tasks and notifications, coordinating between the plugin
//! manager and observer manager.
//!
//! # Architecture
//!
//! The engine maintains a main loop that:
//! 1. Checks for new tasks from the service
//! 2. Processes notifications from observers
//! 3. Handles results from task executions
//! 4. Coordinates shutdown and pause/resume operations
//!
//! # Communication
//!
//! The engine communicates with other components via crossbeam channels:
//! - Task receiver: Receives tasks from the service
//! - Notification receiver: Receives notifications from observers
//! - Result receiver: Receives results from plugin executions

use crate::runtime::{observer_manager::ObserverManager, plugin_manager::PluginManager};

use thryve_utils::bases::notif::Notif;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::packets::{notif_log::Log, task::Task};
use thryve_utils::res;
use thryve_utils::traits::queue::Queue;

use std::{sync::Arc, thread, time::Duration};
use crossbeam::channel::{Receiver, bounded};

/// Core engine for the Thryve runtime.
///
/// The `Engine` struct is the central component of the Thryve runtime,
/// managing the execution flow of tasks and notifications. It coordinates
/// between the plugin manager and observer manager, handling the main
/// execution loop and lifecycle management.
///
/// # Fields
///
/// - `sleep_tick_rate`: Milliseconds to sleep when idle
/// - `obs_manager`: Manages observers and notifications
/// - `plugin_manager`: Manages plugins and tasks
/// - `task_rx`: Receiver for incoming tasks
/// - `notif_rx`: Receiver for incoming notifications
/// - `res_rx`: Receiver for task execution results
/// - `exit_flag`: Indicates whether the engine should shut down
/// - `pause_flag`: Indicates whether the engine is paused
///
/// # Example
///
/// ```no_run
/// // Engine is typically created and managed by the service
/// let engine = Engine::new(&ctx);
/// engine.run_once(); // Single iteration
/// ```
pub struct Engine {
    /// Milliseconds to sleep when idle.
    sleep_tick_rate: u64,
    /// Manages observers and notifications.
    pub obs_manager: ObserverManager,
    /// Manages plugins and tasks.
    pub plugin_manager: PluginManager,
    /// Receiver for incoming tasks.
    task_rx: Arc<Receiver<Task>>,
    /// Receiver for incoming notifications.
    notif_rx: Arc<Receiver<Notif>>,
    /// Receiver for task execution results.
    res_rx: Receiver<(u64, res::Result)>,
    /// Indicates whether the engine should shut down.
    pub exit_flag: bool,
    /// Indicates whether the engine is paused.
    pub pause_flag: bool,
}

impl Engine {
    /// Creates a new engine instance.
    ///
    /// Initializes a new engine with the given context, setting up
    /// communication channels and creating the observer and plugin
    /// managers.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The application context containing configuration
    ///
    /// # Returns
    ///
    /// A new `Engine` instance ready for execution.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let engine = Engine::new(&ctx);
    /// ```
    pub fn new(ctx: &Ctx) -> Self {
        let (res_tx, res_rx) = bounded(ctx.config.pipes.res_pipe_capacity);

        Self {
            sleep_tick_rate: ctx.config.engine.sleep_tick_rate,
            plugin_manager: PluginManager::new(ctx, res_tx),
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
    /// Runs a single iteration of the engine's main loop.
    ///
    /// Checks for new tasks, notifications, and results, processing
    /// each as they arrive. If the engine is paused or marked for
    /// exit, it only processes notifications. When idle, the engine
    /// sleeps for the configured duration to reduce CPU usage.
    ///
    /// # Example
    ///
    /// ```no_run
    /// while !engine.exit_flag {
    ///     engine.run_once();
    /// }
    /// ```
    pub fn run_once(&mut self) {
        if self.exit_flag || self.pause_flag {
            self.check_notifs();
            return;
        }

        let received_tasks = self.check_tasks();
        let received_notifs = self.check_notifs();
        let received_results = self.check_results();

        if !received_tasks && !received_notifs && !received_results {
            thread::sleep(Duration::from_millis(self.sleep_tick_rate));
        }
    }

    /// Checks for and processes new tasks.
    ///
    /// Receives all available tasks from the task receiver and
    /// submits them to the plugin manager's thread pool.
    ///
    /// # Returns
    ///
    /// `true` if any tasks were received and processed.
    fn check_tasks(&self) -> bool {
        let mut received_tasks = false;

        while let Ok(task) = self.task_rx.try_recv() {
            received_tasks = true;
            self.plugin_manager.threadpool.submit_task(task);
        }
        received_tasks
    }

    /// Checks for and processes new notifications.
    ///
    /// Receives all available notifications from the notification
    /// receiver and forwards them to the observer manager.
    ///
    /// # Returns
    ///
    /// `true` if any notifications were received and processed.
    fn check_notifs(&mut self) -> bool {
        let mut received_notifs = false;

        while let Ok(notif) = self.notif_rx.try_recv() {
            received_notifs = true;
            self.obs_manager.forward_notif(notif);
        }
        received_notifs
    }

    /// Checks for and processes new results.
    ///
    /// Receives all available results from the result receiver
    /// and handles them, which may include submitting follow-up
    /// tasks and logging completion messages.
    ///
    /// # Returns
    ///
    /// `true` if any results were received and processed.
    pub fn check_results(&mut self) -> bool {
        let mut received_results = false;

        while let Ok(task_result) = self.res_rx.try_recv() {
            received_results = true;
            self.handle_result(task_result);
        };

        received_results
    }

    /// Finalizes the engine and shuts down all components.
    ///
    /// Sets the exit flag, finalizes the plugin manager, processes
    /// any remaining notifications, and cleans up the observer
    /// manager. This should be called during service shutdown.
    ///
    /// # Example
    ///
    /// ```no_run
    /// engine.finalize();
    /// // All components shut down gracefully
    /// ```
    pub fn finalize(&mut self) {
        self.exit_flag = true.into();

        self.plugin_manager.finalize();

        self.run_once();

        let logger = self.obs_manager.finalize();

        let notif = Log::log("Engine Finalized".to_string(), "Main");
        logger.obs_queue.push(notif);

        let notif = Log::log("Service Finalized Successfully".to_string(), "Main");
        logger.obs_queue.push(notif);

        logger.clean_up();
    }

    /// Pauses the engine.
    ///
    /// Sets the pause flag and pauses the plugin manager's thread
    /// pool, causing task processing to halt until resumed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// engine.pause();
    /// // Task processing halted
    /// engine.resume();
    /// // Task processing resumed
    /// ```
    pub fn pause(&mut self) {
        self.plugin_manager.threadpool.pause();

        self.pause_flag = true;

        let notif = Log::log("Engine Paused".to_string(), "Main");
        self.obs_manager.forward_notif(notif);

        self.run_once();
    }

    /// Resumes the engine after a pause.
    ///
    /// Clears the pause flag and resumes the plugin manager's
    /// thread pool, allowing task processing to continue.
    ///
    /// # Example
    ///
    /// ```no_run
    /// engine.resume();
    /// // Task processing resumed
    /// ```
    pub fn resume(&mut self) {
        self.plugin_manager.threadpool.resume();

        self.pause_flag = false;

        let notif = Log::log("Engine continued".to_string(), "Main");
        self.obs_manager.forward_notif(notif);
    }
}

impl Engine {
    /// Handles a task execution result.
    ///
    /// Processes the result of a task execution, which may include:
    /// - Submitting follow-up tasks
    /// - Logging completion messages
    /// - Triggering shutdown on "fin" or "finerr" results
    ///
    /// # Arguments
    ///
    /// * `task_result` - Tuple of task ID and result
    fn handle_result(&mut self, task_result: (u64, res::Result)) {
        let result = task_result.1;

        if let Some(next_task) = result.next_task {
            self.plugin_manager.threadpool.submit_task(next_task);
        }

        let finished_notif = Log::log(format!("Finished Task ID: {}.", task_result.0), "Main");
        self.obs_manager.forward_notif(finished_notif);

        let task_res_message = result.message;
        let log_file = result.log_msg_to_file.inner();
        if task_res_message != "".to_string() {
            let finished_notif = Log::log(task_res_message, log_file);
            self.obs_manager.forward_notif(finished_notif);
        }

        let res_type = result.result_type.inner().to_lowercase();
        if (res_type == "fin" || res_type == "finerr") && !self.exit_flag {
            self.finalize();
        }
    }
}
