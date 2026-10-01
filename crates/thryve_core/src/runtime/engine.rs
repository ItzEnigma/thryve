// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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
use thryve_utils::graphs::graph::TopologyNode;
use thryve_utils::packets::notif_topology::TopologyModifier;
use thryve_utils::packets::{notif_log::Log, task::Task};
use thryve_utils::res::TaskResult;
use thryve_utils::traits::queue::Queue;

use crossbeam::channel::{Receiver, bounded};
use std::{sync::Arc, thread, time::Duration};

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
/// ```ignore
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
    /// Determines if graph visualization is enabled.
    pub is_graph_vis_enabled: bool,
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
    res_rx: Receiver<(u64, TaskResult)>,
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
    /// ```ignore
    /// let engine = Engine::new(&ctx);
    /// ```
    pub fn new(ctx: &Ctx) -> Self {
        let (res_tx, res_rx) = bounded(ctx.config.pipes.res_pipe_capacity);

        Self {
            sleep_tick_rate: ctx.config.engine.sleep_tick_rate,
            is_debug_enabled: ctx.config.debugging.is_debugging,
            is_metric_calc_enabled: ctx.config.debugging.calculate_metrics,
            is_task_logging_enabled: ctx.config.debugging.log_tasks,
            is_graph_vis_enabled: ctx.config.debugging.visual_graph,
            metrics: ctx.meta_data.metrics.clone(),
            plugin_manager: PluginManager::new(ctx, res_tx),
            obs_manager: ObserverManager::new(ctx),
            task_rx: ctx.task_rx.clone(),
            notif_rx: ctx.notif_rx.clone(),
            res_rx,
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
    /// ```ignore
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
    pub fn check_tasks(&mut self) -> bool {
        let mut received_tasks = false;

        while let Ok(task) = self.task_rx.try_recv() {
            received_tasks = true;

            if self.is_debug_enabled {
                if self.is_metric_calc_enabled {
                    self.manage_metrics_tasks(&task);
                }

                if self.is_graph_vis_enabled {
                    self.handle_graph_visualization(&task);
                }
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
    pub fn check_notifs(&mut self) -> bool {
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

            if self.exit_flag {
                break; // finalize() ran inside handle_result() above -- stop
                // draining more results into a torn-down observer manager
            }
        }

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
    pub fn manage_metrics_tasks(&mut self, task: &Task) {
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
    pub fn manage_metrics_results(&mut self, task_result: &TaskResult) {
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
    pub fn manage_metrics_notifs(&mut self, notif: &Notif) {
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
    /// In order, it:
    ///
    /// 1. sets the exit flag;
    /// 2. finalizes the plugin manager, which stops and joins the worker threads and cleans up
    ///    the plugins;
    /// 3. processes any remaining notifications;
    /// 4. finalizes the observer manager, which stops and joins the notification sorter and
    ///    cleans up the observers;
    /// 5. writes the final lines (and the metrics, when enabled) to the logger, then stops it;
    /// 6. joins every observer thread, the logger's included, so no engine thread is left.
    ///
    /// It also works on a half-built engine, which is how a failed startup stops what it
    /// started. Parts that never started are skipped, and without a logger the status lines are
    /// skipped too. A `Fin` task result calls this from inside [`run_once`](Self::run_once).
    ///
    /// Each observer's loop must return once its exit flag is set, or step 6 waits for it
    /// forever.
    ///
    /// # Example
    ///
    /// ```ignore
    /// engine.finalize();
    /// // All components shut down gracefully
    /// ```
    pub fn finalize(&mut self) {
        self.exit_flag = true;

        self.plugin_manager.finalize();

        self.run_once();

        let logger = self.obs_manager.finalize();

        if let Some(logger) = &logger {
            let notif = Log::log("Engine Finalized".to_string(), "Main");
            logger.obs_queue.push(notif);
        }

        if self.is_debug_enabled && self.is_metric_calc_enabled {
            let metrics_readable = self.metrics.calculate_metrics();

            if let Some(logger) = &logger {
                let notif = Log::log(metrics_readable, "Debug");
                logger.obs_queue.push(notif);
            }
        }

        if let Some(logger) = &logger {
            logger.clean_up(Some(logger.clone()));
        }

        for _ in 0..self.obs_manager.threadpool.workers.len() {
            if let Some(handle) = self.obs_manager.threadpool.workers.pop()
                && !handle.is_finished()
            {
                let res = handle.join();
                if res.is_err() {
                    continue;
                }
            }
        }
    }

    /// Pauses the engine.
    ///
    /// Sets the pause flag and pauses the plugin manager's thread
    /// pool, causing task processing to halt until resumed.
    ///
    /// # Example
    ///
    /// ```ignore
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
    /// ```ignore
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
    pub fn handle_result(&mut self, task_result: (u64, TaskResult)) {
        let mut result = task_result.1;

        if let Some(next_task) = result.next_task.take() {
            if self.is_debug_enabled && self.is_metric_calc_enabled {
                self.manage_metrics_tasks(&next_task);
            }

            if self.is_debug_enabled && self.is_graph_vis_enabled {
                self.handle_graph_visualization(&next_task);
            }

            self.plugin_manager.threadpool.submit_task(next_task);
        }

        if self.is_debug_enabled && self.is_task_logging_enabled {
            let notif = Log::log(format!("Finished Task ID: {}.", task_result.0), "Main");

            if self.is_metric_calc_enabled {
                self.manage_metrics_notifs(&notif);
            }

            self.obs_manager.forward_notif(notif);
        }

        let result_message = std::mem::take(&mut result.message);
        let result_type = result.result_type();
        let log_file = result.log_msg_to_file();

        if result_type.is_empty() {
            if !result_message.is_empty() {
                let notif = Log::err(String::from(
                    "result type cannot be empty if the result message is not empty",
                ));
                self.obs_manager.forward_notif(notif);
            } else if !log_file.is_empty() {
                let notif = Log::err(String::from(
                    "result type cannot be empty if the result should log to a file",
                ));
                self.obs_manager.forward_notif(notif);
            }
            return;
        }

        if !result_message.is_empty() {
            let notif = Log::log(result_message, log_file);

            if self.is_debug_enabled && self.is_metric_calc_enabled {
                self.manage_metrics_notifs(&notif);
            }

            self.obs_manager.forward_notif(notif);
        }

        if (result_type == "Fin" || result_type == "FinErr") && !self.exit_flag {
            self.finalize();
        }
    }

    /// Handles graph visualization for a task.
    ///
    /// When graph visualization is enabled, this method captures the
    /// call site information from tasks and sends notifications to
    /// the topology handler observer to build the task call graph.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to process for graph visualization
    pub fn handle_graph_visualization(&mut self, task: &Task) {
        if let Some(callsite) = task.callsite.clone() {
            let topo_node =
                TopologyNode::new(callsite.clone(), callsite.this_callsite_fingerprint.clone());
            let notif = TopologyModifier::add_node(topo_node);
            self.obs_manager.forward_notif(notif);

            if callsite.current_executing_task_fingerprint != String::default() {
                let notif = TopologyModifier::add_edge(
                    callsite.current_executing_task_fingerprint,
                    callsite.this_callsite_fingerprint,
                );
                self.obs_manager.forward_notif(notif);
            }
        }
    }
}
