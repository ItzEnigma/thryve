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
use thryve_utils::cfg::metrics::Metrics;
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
    pub sleep_tick_rate: u64,
    /// Determines if the Config debug-mode is enabled.
    pub is_debug_enabled: bool,
    /// Determines if metric calculation is enabled.
    pub is_metric_calc_enabled: bool,
    /// Determines if tasks are logged.
    pub is_task_logging_enabled: bool,
    /// Keeps track of the metrics only if `is_debug_enabled` and `is_metric_calc_enabled` is true.
    pub metrics: Metrics,
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
            is_debug_enabled: ctx.config.debugging.is_debugging,
            is_metric_calc_enabled: ctx.config.debugging.calculate_metrics,
            is_task_logging_enabled: ctx.config.debugging.log_tasks,
            metrics: ctx.meta_data.metrics.clone(),
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
    fn check_tasks(&mut self) -> bool {
        let mut received_tasks = false;

        while let Ok(task) = self.task_rx.try_recv() {
            received_tasks = true;
            
            if self.is_debug_enabled && self.is_metric_calc_enabled {
                self.manage_metrics_tasks(&task);
            }

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
            
            if self.is_debug_enabled && self.is_metric_calc_enabled {
                self.manage_metrics_notifs(&notif);
            }
            
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

            if self.is_debug_enabled && self.is_metric_calc_enabled {
                self.manage_metrics_results(&task_result.1);
            }

            self.handle_result(task_result);
        };

        received_results
    }

    /// Collects size metrics for an incoming task.
    ///
    /// Stores the task size in the metrics batch buffer. When the batch
    /// is full (1024 entries), triggers metric calculation and sends
    /// a debug notification with the formatted results.
    ///
    /// # Arguments
    ///
    /// * `task` - Reference to the task being processed
    fn manage_metrics_tasks(&mut self, task: &Task) {
        let size = task.size_in_bytes();
        let cur_batch_size = self.metrics.cur_batch_count_task;

        self.metrics.task_size_batch[cur_batch_size] = size;
        self.metrics.cur_batch_count_task += 1;

        if self.metrics.cur_batch_count_task == self.metrics.task_size_batch.len() {
            let metrics_readable = self.metrics.calculate_metrics();

            let notif = Log::log(metrics_readable, "Debug");
            self.obs_manager.forward_notif(notif);
        }
    }

    /// Collects size metrics for a task execution result.
    ///
    /// Stores the result size in the metrics batch buffer. When the
    /// batch is full (1024 entries), triggers metric calculation and
    /// sends a debug notification with the formatted results.
    ///
    /// # Arguments
    ///
    /// * `task_result` - Reference to the result being processed
    fn manage_metrics_results(&mut self, task_result: &res::Result) {
        let size = task_result.size_in_bytes();
        let cur_batch_size = self.metrics.cur_batch_count_res;

        self.metrics.res_size_batch[cur_batch_size] = size;
        self.metrics.cur_batch_count_res += 1;

        if self.metrics.cur_batch_count_res == self.metrics.res_size_batch.len() {
            let metrics_readable = self.metrics.calculate_metrics();

            let notif = Log::log(metrics_readable, "Debug");
            self.obs_manager.forward_notif(notif);
        }
    }

    /// Collects size metrics for an incoming notification.
    ///
    /// Stores the notification size in the metrics batch buffer. When
    /// the batch is full (1024 entries), triggers metric calculation
    /// and sends a debug notification with the formatted results.
    ///
    /// # Arguments
    ///
    /// * `notif` - Reference to the notification being processed
    fn manage_metrics_notifs(&mut self, notif: &Notif) {
        let size = notif.size_in_bytes();
        let cur_batch_size = self.metrics.cur_batch_count_notif;

        self.metrics.notif_size_batch[cur_batch_size] = size;
        self.metrics.cur_batch_count_notif += 1;

        if self.metrics.cur_batch_count_notif == self.metrics.notif_size_batch.len() {
            let metrics_readable = self.metrics.calculate_metrics();

            let notif_metric = Log::log(metrics_readable, "Debug");
            self.obs_manager.forward_notif(notif_metric);
        }
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

        if self.is_debug_enabled && self.is_metric_calc_enabled {
            let metrics_readable = self.metrics.calculate_metrics();
            
            let notif = Log::log(metrics_readable , "Debug");
            logger.obs_queue.push(notif);
        }

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
    /// - Triggering shutdown on "Fin" or "FinErr" results
    ///
    /// # Arguments
    ///
    /// * `task_result` - Tuple of task ID and result
    fn handle_result(&mut self, task_result: (u64, res::Result)) {
        let result = task_result.1;

        if let Some(next_task) = result.next_task {
            if self.is_debug_enabled && self.is_metric_calc_enabled {
                self.manage_metrics_tasks(&next_task);
            }

            self.plugin_manager.threadpool.submit_task(next_task);
        }

        if self.is_debug_enabled && self.is_task_logging_enabled{
            let notif = Log::log(format!("Finished Task ID: {}.", task_result.0), "Main");

            if self.is_metric_calc_enabled {
                self.manage_metrics_notifs(&notif);
            }

            self.obs_manager.forward_notif(notif);
        }

        let task_res_message = result.message;

        let log_file = result.log_msg_to_file.inner();
        if task_res_message != "".to_string() {
            let notif = Log::log(task_res_message, log_file);

            if self.is_debug_enabled && self.is_metric_calc_enabled{
                self.manage_metrics_notifs(&notif);
            }
            
            self.obs_manager.forward_notif(notif);
        }

        let res_type = result.result_type.inner();
        if (res_type == "Fin" || res_type == "FinErr") && !self.exit_flag {
            self.finalize();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use thryve_utils::cfg::ctx::Ctx;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::bases::observer::Observer;
    use thryve_utils::traits::observer_logic::ObsLogic;
    use crossbeam::channel::bounded;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        thryve_utils::impl_as_any!(TestObsLogic);
        fn clean_up(&self, observer: Arc<Observer>) {
            while observer.obs_queue.main_queue.len() != 0 {
                let _notif = observer.obs_queue.pop();
            };
        }
    }

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
        let config = Config::default();
        Ctx::new(config, task_rx, notif_rx, packet_handle)
    }

    #[test]
    fn test_engine_new_creates_engine() {
        // Arrange
        let ctx = create_test_ctx();

        // Act
        let engine = Engine::new(&ctx);

        // Assert
        assert!(!engine.exit_flag);
        assert!(!engine.pause_flag);
        assert_eq!(engine.sleep_tick_rate, ctx.config.engine.sleep_tick_rate);
    }

    #[test]
    fn test_engine_run_once_with_exit_flag_only_checks_notifs() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.exit_flag = true;

        // Act - should not panic
        engine.run_once();

        // Assert - exit_flag should still be true
        assert!(engine.exit_flag);
    }

    #[test]
    fn test_engine_run_once_with_pause_flag_only_checks_notifs() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        engine.pause_flag = true;

        // Act - should not panic
        engine.run_once();

        // Assert - pause_flag should still be true
        assert!(engine.pause_flag);
    }

    #[test]
    fn test_engine_pause_sets_flag() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: thryve_utils::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("logger", "Logging", obs_core, fn_to_run, ctx.packet_handle());
        engine.obs_manager.load_observer(observer);

        // Assert initially not paused
        assert!(!engine.pause_flag);

        // Act
        engine.pause();

        // Assert
        assert!(engine.pause_flag);
    }

    #[test]
    fn test_engine_resume_clears_flag() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer first (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: thryve_utils::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("logger", "Logging", obs_core, fn_to_run, ctx.packet_handle());
        engine.obs_manager.load_observer(observer);

        engine.pause();

        // Assert paused
        assert!(engine.pause_flag);

        // Act
        engine.resume();

        // Assert
        assert!(!engine.pause_flag);
    }

    #[test]
    fn test_engine_check_results_with_empty_channel() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Act
        let received = engine.check_results();

        // Assert - no results available
        assert!(!received);
    }

    #[test]
    fn test_engine_check_tasks_with_empty_channel() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Act
        let received = engine.check_tasks();

        // Assert - no tasks available
        assert!(!received);
    }

    #[test]
    fn test_engine_check_notifs_with_empty_channel() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Act
        let received = engine.check_notifs();

        // Assert - no notifications available
        assert!(!received);
    }

    #[test]
    fn test_engine_finalize_sets_exit_flag() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for finalize)
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: thryve_utils::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("logger", "Logging", obs_core, fn_to_run, ctx.packet_handle());
        engine.obs_manager.load_observer(observer);

        // Assert initially not exiting
        assert!(!engine.exit_flag);

        // Act
        engine.finalize();

        // Assert
        assert!(engine.exit_flag);
    }

    #[test]
    fn test_engine_handle_result_with_fin_type_triggers_finalize() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: thryve_utils::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("logger", "Logging", obs_core, fn_to_run, ctx.packet_handle());
        engine.obs_manager.load_observer(observer);

        // Create a "Fin" result
        let fin_result = res::Result::new("Fin", "Finished".to_string(), "Main");

        // Assert initially not exiting
        assert!(!engine.exit_flag);

        // Act - handle the Fin result
        engine.handle_result((1, fin_result));

        // Assert - finalize should have been called
        assert!(engine.exit_flag);
    }

    #[test]
    fn test_engine_handle_result_with_finerr_type_triggers_finalize() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: thryve_utils::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("logger", "Logging", obs_core, fn_to_run, ctx.packet_handle());
        engine.obs_manager.load_observer(observer);

        // Create a "FinErr" result
        let finerr_result = res::Result::new("FinErr", "Error finished".to_string(), "Error");

        // Assert initially not exiting
        assert!(!engine.exit_flag);

        // Act - handle the FinErr result
        engine.handle_result((1, finerr_result));

        // Assert - finalize should have been called
        assert!(engine.exit_flag);
    }

    #[test]
    fn test_engine_handle_result_with_normal_type_does_not_finalize() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: thryve_utils::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("logger", "Logging", obs_core, fn_to_run, ctx.packet_handle());
        engine.obs_manager.load_observer(observer);

        // Create a normal result
        let normal_result = res::Result::new("normal", "Done".to_string(), "Main");

        // Act - handle the normal result
        engine.handle_result((1, normal_result));

        // Assert - should not have finalized
        assert!(!engine.exit_flag);
    }

    #[test]
    fn test_engine_handle_result_submits_next_task() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: thryve_utils::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("logger", "Logging", obs_core, fn_to_run, ctx.packet_handle());
        engine.obs_manager.load_observer(observer);

        // Create a next task
        let next_task = Task::new("nextTask", 5, Box::new(|| {res::Result::new("Test", "testinggg".to_string(), "Main")}));
        let result_with_next = res::Result::new("normal", "Done".to_string(), "Main").chain(next_task);

        // Act - handle the result with next task
        engine.handle_result((1, result_with_next));

        // Assert - the next task should have been submitted to the threadpool
        // We can verify this by checking that the task was sent (no panic means it worked)
        assert!(!engine.exit_flag);
    }

    #[test]
    fn test_engine_check_tasks_receives_and_submits() {
        // Arrange
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx.clone(), notif_tx);
        let config = Config::default();
        let ctx = Ctx::new(config, task_rx, notif_rx, packet_handle);
        let mut engine = Engine::new(&ctx);

        // Send a task
        let task = Task::new("testTask", 5, Box::new(|| {res::Result::new("Test", "testinggg".to_string(), "Main")}));
        task_tx.send(task).unwrap();

        // Act
        let received = engine.check_tasks();

        // Assert
        assert!(received);
    }

    #[test]
    fn test_engine_check_notifs_receives_and_forwards() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);

        // Load a logging observer (required for forward_notif)
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: thryve_utils::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("logger", "Logging", obs_core, fn_to_run, ctx.packet_handle());
        engine.obs_manager.load_observer(observer);

        // Send a notification
        let notif = Log::log("test notification".to_string(), "Main");
        ctx.packet_handle().send_notification(notif);

        // Act
        let received = engine.check_notifs();

        // Assert
        assert!(received);
    }

    #[test]
    fn test_engine_run_once_processes_all_channels() {
        // Arrange
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx.clone(), notif_tx.clone());
        let config = Config::default();
        let ctx = Ctx::new(config, task_rx, notif_rx, packet_handle);
        let mut engine = Engine::new(&ctx);

        // Load a logging observer
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: thryve_utils::extras::jobs::ObsJob = Box::new(move |_obs: Arc<Observer>| {});
        let observer = Observer::new("logger", "Logging", obs_core, fn_to_run, ctx.packet_handle());
        engine.obs_manager.load_observer(observer);

        // Send a task and notification
        let task = Task::new("testTask", 5, Box::new(|| {res::Result::new("Test", "testinggg".to_string(), "Main")}));
        task_tx.send(task).unwrap();
        let notif = Log::log("test".to_string(), "Main");
        notif_tx.send(notif).unwrap();

        // Act - run once should process both without blocking
        engine.run_once();

        // Assert - engine should still be running
        assert!(!engine.exit_flag);
        assert!(!engine.pause_flag);
    }

    // Tests for manage_metrics_tasks

    #[test]
    fn test_manage_metrics_tasks_stores_size_in_batch() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let task = Task::new("TestTask", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let expected_size = task.size_in_bytes();

        // Act
        engine.manage_metrics_tasks(&task);

        // Assert
        assert_eq!(engine.metrics.task_size_batch[0], expected_size);
        assert_eq!(engine.metrics.cur_batch_count_task, 1);
    }

    #[test]
    fn test_manage_metrics_tasks_increments_batch_count() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let task1 = Task::new("Task1", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let task2 = Task::new("Task2", 2, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));

        // Act
        engine.manage_metrics_tasks(&task1);
        engine.manage_metrics_tasks(&task2);

        // Assert
        assert_eq!(engine.metrics.cur_batch_count_task, 2);
        assert_eq!(engine.metrics.task_size_batch[0], task1.size_in_bytes());
        assert_eq!(engine.metrics.task_size_batch[1], task2.size_in_bytes());
    }

    #[test]
    fn test_manage_metrics_tasks_does_not_trigger_calculation_before_batch_full() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let task = Task::new("TestTask", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));

        // Act
        engine.manage_metrics_tasks(&task);

        // Assert - avg_task_size should remain 0.0 since batch is not full
        assert_eq!(engine.metrics.avg_task_size, 0.0);
    }

    // Tests for manage_metrics_results

    #[test]
    fn test_manage_metrics_results_stores_size_in_batch() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let result = res::Result::new("Success", "Done".to_string(), "Main");
        let expected_size = result.size_in_bytes();

        // Act
        engine.manage_metrics_results(&result);

        // Assert
        assert_eq!(engine.metrics.res_size_batch[0], expected_size);
        assert_eq!(engine.metrics.cur_batch_count_res, 1);
    }

    #[test]
    fn test_manage_metrics_results_increments_batch_count() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let result1 = res::Result::new("Type1", "Message1".to_string(), "Main");
        let result2 = res::Result::new("Type2", "Message2".to_string(), "Error");

        // Act
        engine.manage_metrics_results(&result1);
        engine.manage_metrics_results(&result2);

        // Assert
        assert_eq!(engine.metrics.cur_batch_count_res, 2);
        assert_eq!(engine.metrics.res_size_batch[0], result1.size_in_bytes());
        assert_eq!(engine.metrics.res_size_batch[1], result2.size_in_bytes());
    }

    #[test]
    fn test_manage_metrics_results_does_not_trigger_calculation_before_batch_full() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let result = res::Result::new("Success", "Done".to_string(), "Main");

        // Act
        engine.manage_metrics_results(&result);

        // Assert - avg_res_size should remain 0.0 since batch is not full
        assert_eq!(engine.metrics.avg_res_size, 0.0);
    }

    // Tests for manage_metrics_notifs

    #[test]
    fn test_manage_metrics_notifs_stores_size_in_batch() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let notif = Log::log("Test notification".to_string(), "Main");
        let expected_size = notif.size_in_bytes();

        // Act
        engine.manage_metrics_notifs(&notif);

        // Assert
        assert_eq!(engine.metrics.notif_size_batch[0], expected_size);
        assert_eq!(engine.metrics.cur_batch_count_notif, 1);
    }

    #[test]
    fn test_manage_metrics_notifs_increments_batch_count() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let notif1 = Log::log("Notification 1".to_string(), "Main");
        let notif2 = Log::log("Notification 2".to_string(), "Debug");

        // Act
        engine.manage_metrics_notifs(&notif1);
        engine.manage_metrics_notifs(&notif2);

        // Assert
        assert_eq!(engine.metrics.cur_batch_count_notif, 2);
        assert_eq!(engine.metrics.notif_size_batch[0], notif1.size_in_bytes());
        assert_eq!(engine.metrics.notif_size_batch[1], notif2.size_in_bytes());
    }

    #[test]
    fn test_manage_metrics_notifs_does_not_trigger_calculation_before_batch_full() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let notif = Log::log("Test notification".to_string(), "Main");

        // Act
        engine.manage_metrics_notifs(&notif);

        // Assert - avg_notif_size should remain 0.0 since batch is not full
        assert_eq!(engine.metrics.avg_notif_size, 0.0);
    }

    // Integration test for all three manage_metrics functions

    #[test]
    fn test_all_manage_metrics_functions_work_together() {
        // Arrange
        let ctx = create_test_ctx();
        let mut engine = Engine::new(&ctx);
        let task = Task::new("TestTask", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let result = res::Result::new("Success", "Done".to_string(), "Main");
        let notif = Log::log("Test notification".to_string(), "Main");

        // Act
        engine.manage_metrics_tasks(&task);
        engine.manage_metrics_results(&result);
        engine.manage_metrics_notifs(&notif);

        // Assert
        assert_eq!(engine.metrics.cur_batch_count_task, 1);
        assert_eq!(engine.metrics.cur_batch_count_res, 1);
        assert_eq!(engine.metrics.cur_batch_count_notif, 1);
        assert_eq!(engine.metrics.task_size_batch[0], task.size_in_bytes());
        assert_eq!(engine.metrics.res_size_batch[0], result.size_in_bytes());
        assert_eq!(engine.metrics.notif_size_batch[0], notif.size_in_bytes());
    }
}
