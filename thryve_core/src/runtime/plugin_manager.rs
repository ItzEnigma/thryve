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
use thryve_utils::cfg::config::Config;
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
    /// * `congig` - The Config to get the max priority available
    ///
    /// # Example
    ///
    /// ```no_run
    /// manager.load_plugin(my_plugin);
    /// ```
    pub fn load_plugin(&mut self, config: &Config, mut plugin: Plugin) {
        let plg_type = format!("Init{}" , plugin.plg_type.inner());
        let task_init = Task::new(&plg_type, config.tasks.max_priority, plugin.plg_entry_point());
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

#[cfg(test)]
mod tests {
    use super::*;
    use thryve_utils::cfg::ctx::Ctx;
    use thryve_utils::cfg::config::Config;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::traits::plugin_logic::PlgLogic;
    use crossbeam::channel::bounded;

    // Test implementation of PlgLogic
    struct TestPlgLogic;
    impl PlgLogic for TestPlgLogic {
        thryve_utils::impl_as_any!(TestPlgLogic);
        fn clean_up(&self) {}
    }

    fn create_test_ctx() -> Ctx {
        let (task_tx, task_rx) = bounded::<Task>(10);
        let (notif_tx, notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();
        Ctx::new(config, task_rx, notif_rx, packet_handle)
    }

    #[test]
    fn test_plugin_manager_new_creates_manager() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, res::Result)>(10);

        // Act
        let manager = PluginManager::new(&ctx, res_tx);

        // Assert
        assert!(manager.plugin_names().is_empty());
    }

    #[test]
    fn test_plugin_manager_new_with_zero_workers() {
        // Arrange
        let mut ctx = create_test_ctx();
        ctx.config.threads.worker_count = 0;
        let (res_tx, _res_rx) = bounded::<(u64, res::Result)>(10);

        // Act
        let manager = PluginManager::new(&ctx, res_tx);

        // Assert
        assert_eq!(manager.threadpool.num_workers(), 0);
    }

    #[test]
    fn test_plugin_manager_plugin_names_returns_empty_when_no_plugins() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, res::Result)>(10);
        let manager = PluginManager::new(&ctx, res_tx);

        // Act
        let names = manager.plugin_names();

        // Assert
        assert!(names.is_empty());
    }

    #[test]
    fn test_plugin_manager_load_plugin() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, res::Result)>(10);
        let mut manager = PluginManager::new(&ctx, res_tx);
        
        let plg_core = Arc::new(TestPlgLogic);
        let job: thryve_utils::extras::jobs::TaskJob = Box::new(|| {
            res::Result::new("OK", "success".to_string(), "Main")
        });
        let plugin = Plugin::new(
            "test_plugin",
            "testType",
            plg_core,
            ctx.packet_handle(),
            job,
        );

        // Act
        manager.load_plugin(&ctx.config ,plugin);

        // Assert
        let names = manager.plugin_names();
        assert_eq!(names.len(), 1);
        assert_eq!(names[0].inner(), "testType");
    }

    #[test]
    fn test_plugin_manager_load_multiple_plugins() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, res::Result)>(10);
        let mut manager = PluginManager::new(&ctx, res_tx);

        // Load first plugin
        let plg_core1 = Arc::new(TestPlgLogic);
        let job1: thryve_utils::extras::jobs::TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));
        let plugin1 = Plugin::new("plugin1", "type1", plg_core1, ctx.packet_handle().clone(), job1);

        // Load second plugin
        let plg_core2 = Arc::new(TestPlgLogic);
        let job2: thryve_utils::extras::jobs::TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));
        let plugin2 = Plugin::new("plugin2", "type2", plg_core2, ctx.packet_handle().clone(), job2);

        // Act
        manager.load_plugin(&ctx.config , plugin1);
        manager.load_plugin(&ctx.config , plugin2);

        // Assert
        let names = manager.plugin_names();
        assert_eq!(names.len(), 2);
    }

    #[test]
    fn test_plugin_manager_finalize() {
        // Arrange
        let ctx = create_test_ctx();
        let (res_tx, _res_rx) = bounded::<(u64, res::Result)>(10);
        let mut manager = PluginManager::new(&ctx, res_tx);

        // Add a plugin
        let plg_core = Arc::new(TestPlgLogic);
        let job: thryve_utils::extras::jobs::TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));
        let plugin = Plugin::new("test_plugin", "testType", plg_core, ctx.packet_handle(), job);
        manager.load_plugin(&ctx.config, plugin);

        // Assert plugin was loaded
        assert_eq!(manager.plugin_names().len(), 1);

        // Act
        manager.finalize();

        // Assert - finalize should clean up plugins (though we can't directly verify this)
        // The test passes if finalize doesn't panic
    }
}