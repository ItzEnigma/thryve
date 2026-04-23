//! Plugin manager for the Thryve runtime.
//!
//! This module provides the [`PluginManager`] struct, which is responsible
//! for managing plugins and their associated tasks in the engine. It
//! provides functionality to load plugins, retrieve plugin names, and
//! finalize the plugin manager by cleaning up resources and finalizing
//! the thread pool.
//!
//! # Architecture
//!
//! The plugin manager maintains:
//! - A mapping of plugin types to their instances
//! - A thread pool for processing tasks
//! - A priority queue for task management
//!
//! # Thread Pool
//!
//! The thread pool is configured with the number of workers specified
//! in the application configuration. It uses a [`PQChild`](thryve_utils::queues::priority_queue::PQChild)
//! queue for managing tasks with priority-based ordering.
//!
//! # Example
//!
//! ```no_run
//! let manager = PluginManager::new(&ctx, result_sender);
//! manager.load_plugin(my_plugin);
//! ```

use crossbeam::channel::Sender;

use thryve_utils::bases::plugin::Plugin;
use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::packets::{notif_log::Log , packet_handle::PacketHandle , task::Task};
use thryve_utils::traits::plugin_logic::PluginB;
use thryve_utils::res;
use thryve_utils::queues::priority_queue::PQChild;
use thryve_utils::threadpool::ThreadPool;

use std::{collections::HashMap, sync::Arc};

/// Manager for plugins in the Thryve runtime.
///
/// The `PluginManager` is responsible for managing plugins and their
/// associated tasks. It maintains a mapping of plugin types to their
/// instances and manages a thread pool for processing tasks.
///
/// # Fields
///
/// - `plugins`: Mapping of plugin types to instances
/// - `threadpool`: Thread pool for processing tasks
/// - `packet_handle`: Handle for sending tasks and notifications
///
/// # Thread Pool Configuration
///
/// The thread pool is configured with the number of workers specified
/// in the application configuration. It uses a [`PQChild`](thryve_utils::queues::priority_queue::PQChild)
/// queue for managing tasks with priority-based ordering.
///
/// # Example
///
/// ```no_run
/// let manager = PluginManager::new(&ctx, result_sender);
/// manager.load_plugin(my_plugin);
/// ```
pub struct PluginManager {
    /// Mapping of plugin types to instances.
    plugins: HashMap<NamedInput, Plugin>,
    /// Thread pool for processing tasks.
    pub threadpool: ThreadPool<PQChild<Task>, Task>,
    /// Handle for sending tasks and notifications.
    packet_handle: PacketHandle,
}

impl PluginManager {
    /// Creates a new plugin manager.
    ///
    /// Initializes a new plugin manager with the given context and
    /// result sender. The thread pool is created with the number of
    /// workers specified in the configuration.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The application context containing configuration
    /// * `res_tx` - Sender for task execution results
    ///
    /// # Returns
    ///
    /// A new `PluginManager` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let manager = PluginManager::new(&ctx, result_sender);
    /// ```
    pub fn new(ctx: &Ctx, res_tx: Sender<(u64, res::Result)>) -> Self {
        let queue = PQChild::new(&ctx.config, ctx.packet_handle());
        let mut num_workers = 0;

        if ctx.config.threads.worker_count != 0 {
            num_workers = ctx.config.threads.worker_count;
        }

        PluginManager {
            plugins: HashMap::new(),
            threadpool: ThreadPool::new(
                num_workers,
                queue,
                ctx.packet_handle(),
                Some(Arc::new(res_tx)),
            ),
            packet_handle: ctx.packet_handle(),
        }
    }

    /// Returns a vector of all plugin names.
    ///
    /// # Returns
    ///
    /// A vector of references to `NamedInput` representing the names
    /// of the loaded plugins.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let names = manager.plugin_names();
    /// for name in names {
    ///     println!("Plugin: {}", name);
    /// }
    /// ```
    pub fn plugin_names(&self) -> Vec<&NamedInput> {
        let mut vector: Vec<&NamedInput> = Vec::new();
        for plugin in self.plugins.keys() {
            vector.push(plugin);
        }

        vector
    }

    /// Loads a new plugin into the manager.
    ///
    /// Creates an initialization task for the given plugin and sends
    /// it to the engine for execution. The plugin is then inserted
    /// into the plugins hashmap, keyed by its type.
    ///
    /// # Arguments
    ///
    /// * `plugin` - The plugin to load
    ///
    /// # Example
    ///
    /// ```no_run
    /// manager.load_plugin(my_plugin);
    /// ```
    pub fn load_plugin(&mut self, mut plugin: Plugin) {
        let task_init = Task::new("Init", 0, plugin.initial_fn_to_run());
        self.packet_handle.send_task(task_init);
        self.plugins.insert(NamedInput::new(plugin.plg_type.inner()), plugin);
    }

    /// Finalizes the plugin manager.
    ///
    /// Cleans up resources used by the plugin manager and finalizes
    /// the thread pool. This includes:
    /// - Finalizing the thread pool to clean up worker threads
    /// - Calling `clean_up()` on each plugin for resource deallocation
    /// - Sending a finalization notification
    ///
    /// # Example
    ///
    /// ```no_run
    /// manager.finalize();
    /// // All plugins cleaned up, thread pool finalized
    /// ```
    pub fn finalize(&mut self) {
        // Finalize the thread pool, which will clean up worker threads
        // and any resources associated with it.
        self.threadpool.finalize_plg();

        // Clean up each plugin by calling their `clean_up` method.
        // This should handle any necessary resource deallocation or
        // finalization logic specific to the plugin.
        for (_, plugin) in self.plugins.drain() {
            plugin.clean_up();
        }

        let notif = Log::log("Plugin Manager Finalized".to_string(), "Main");
        self.packet_handle.send_notification(notif);
    }
}