// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Plugin manager for the Thryve runtime.
//!
//! This module provides the [`PluginManager`] struct, which is responsible
//! for managing plugins and their associated tasks in the engine. It
//! provides functionality to load plugins, retrieve plugin names, stop
//! the thread pool at shutdown, and clean up the plugins on a thread of
//! their own.
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
//! in the application configuration. It uses a [`PQChild`]
//! queue for managing tasks with priority-based ordering.
//!
//! # Example
//!
//! ```ignore
//! let mut manager = PluginManager::new(&ctx, result_sender);
//! let task_init = manager.load_plugin(&ctx.config, my_plugin, false)?;
//! ```

use crossbeam::atomic::AtomicCell;
use crossbeam::channel::Sender;

use thryve_utils::bases::notif::Notif;
use thryve_utils::bases::plugin::Plugin;
use thryve_utils::cfg::config::Config;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::errors::{NameInputType, StartupError, ThryveError, get_err};
use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::extras::thread_local;
use thryve_utils::graphs::callsite::TaskCallSite;
use thryve_utils::packets::{notif_log::Log, task::Task};
use thryve_utils::queues::priority_queue::PQChild;
use thryve_utils::res::TaskResult;
use thryve_utils::threads::threadpool::{TaskSlot, ThreadPool};
use thryve_utils::traits::packets::Packet;
use thryve_utils::traits::plugin_logic::PluginB;

use std::panic::Location;
use std::path::PathBuf;
use std::thread::{self, JoinHandle};
use std::time::Instant;
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
/// in the application configuration. It uses a [`PQChild`]
/// queue for managing tasks with priority-based ordering.
///
/// # Example
///
/// ```ignore
/// let manager = PluginManager::new(&ctx, result_sender);
/// manager.load_plugin(my_plugin);
/// ```
pub struct PluginManager {
    /// Mapping of plugin types to instances.
    plugins: HashMap<NamedInput, Plugin>,
    /// Thread pool for processing tasks.
    pub threadpool: ThreadPool<PQChild<Task>, Task>,
    /// Sender for task results, handed to the worker threads when they start (see
    /// [`ThreadPool::run_threads_plugins`]).
    pub res_tx: Arc<Sender<(u64, TaskResult)>>,
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
    /// ```ignore
    /// let manager = PluginManager::new(&ctx, result_sender);
    /// ```
    pub fn new(ctx: &Ctx, res_tx: Sender<(u64, TaskResult)>) -> Self {
        let queue = PQChild::new(&ctx.config, ctx.packet_handle());
        let mut num_workers = 0;

        if ctx.config.threads.worker_count != 0 {
            num_workers = ctx.config.threads.worker_count;
        }

        PluginManager {
            plugins: HashMap::new(),
            threadpool: ThreadPool::new(num_workers, queue, ctx.packet_handle()),
            res_tx: Arc::new(res_tx),
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
    /// ```ignore
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
    /// Inserts the plugin into the plugins hashmap, keyed by its type, and returns its init task.
    /// The task isn't queued here: the caller hands it to
    /// [`Engine::accept_task`](crate::runtime::engine::Engine::accept_task), which records its
    /// metrics and graph node like any other task's, then queues it. Startup does this while the
    /// engine is still paused, so the task runs only once the engine starts.
    ///
    /// In graph mode the init task gets a call site here, so that it has a node in the graph and
    /// the tasks it sends or chains are linked to it. It points at this function, and it's named
    /// after the plugin's type, so each plugin gets its own node.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration, for the init task's priority (the highest one)
    /// * `plugin` - The plugin to load
    /// * `is_graph_mode_on` - Whether graph mode is on (debugging and `visual_graph` both on)
    ///
    /// # Errors
    ///
    /// Returns [`StartupError::RegisterDuplicate`] if a plugin of the same type is already loaded.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let task_init = manager.load_plugin(&config, my_plugin, false)?;
    /// engine.accept_task(task_init);
    /// ```
    pub fn load_plugin(
        &mut self,
        config: &Config,
        mut plugin: Plugin,
        is_graph_mode_on: bool,
    ) -> Result<Task, ThryveError> {
        let plg_type = plugin.plg_type_input().clone();
        let mut task_init = Task::new_init(
            plg_type.clone(),
            config.tasks.max_priority,
            plugin.plg_entry_point(),
        );

        if is_graph_mode_on {
            let caller = Location::caller();
            let file_path = PathBuf::from(caller.file().to_string());
            let line_num = caller.line();
            let task_name = task_init.task_name().to_string();

            let mut callsite = TaskCallSite::new(file_path, line_num, task_name);

            let child_fingerprint = callsite.fingerprint();
            let current_task_fingerprint = thread_local::tl_get_current_task_fingerprint();

            callsite.this_callsite_fingerprint = child_fingerprint;
            callsite.current_executing_task_fingerprint = current_task_fingerprint;

            task_init.set_callsite(callsite.clone());
        }

        if self.plugins.contains_key(&plg_type) {
            return Err(StartupError::RegisterDuplicate {
                name: plg_type.inner().to_string(),
                kind: NameInputType::PlgType,
            }
            .into());
        }

        self.plugins.insert(plg_type, plugin);

        Ok(task_init)
    }

    /// Stops the plugin manager's thread pool.
    ///
    /// Stops the workers (see [`ThreadPool::finalize_plg`]) and adds a "Plugin Manager
    /// Finalized" line. The workers aren't joined and the plugins aren't cleaned up here: the
    /// engine waits for the workers while it empties its mailboxes, then calls
    /// [`finalize_plugins`](Self::finalize_plugins).
    ///
    /// # Arguments
    ///
    /// * `tasks` - Receives the tasks that were still queued; they never run
    /// * `status_reps` - Receives the status lines
    ///
    /// # Returns
    ///
    /// How many tasks were still queued.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let never_ran = manager.finalize(&mut tasks, &mut status_reps);
    /// ```
    pub fn finalize(&mut self, tasks: &mut Vec<Task>, status_reps: &mut Vec<Notif>) -> usize {
        // Tells the workers to stop and wakes the waiting ones; the tasks still queued come
        // back here and never run. The workers aren't joined: the engine waits for them.
        let left_over_tasks_count = self.threadpool.finalize_plg(tasks, status_reps);

        let notif = Log::log("Plugin Manager Finalized".to_string(), "Main");
        status_reps.push(notif);

        left_over_tasks_count
    }

    /// Takes every loaded plugin out of the manager, for
    /// [`finalize_plugins`](Self::finalize_plugins).
    ///
    /// # Returns
    ///
    /// The plugins, in no particular order. The manager is left empty.
    fn finalize_get_plugins(&mut self) -> Vec<Plugin> {
        let mut plugins = Vec::new();
        for (_, plugin) in self.plugins.drain() {
            plugins.push(plugin);
        }

        plugins
    }

    /// Cleans up every plugin on a thread of its own, once the workers have stopped.
    ///
    /// The engine calls it after it has waited for the workers. It adds the pool's
    /// "ThreadPool Finalized" line, takes the plugins out of the manager, and starts the
    /// "plugin cleanup" thread, which runs each plugin's
    /// [`clean_up`](thryve_utils::traits::plugin_logic::PluginB::clean_up). The thread's slot
    /// holds the type of the plugin it's cleaning up.
    ///
    /// A failed cleanup is sent as a status line for `Error.log`, so the engine has to keep
    /// reading its mailboxes until the thread ends. It must anyway: a `clean_up` may send too,
    /// and wait while a mailbox is full.
    ///
    /// No plugin is cleaned up, and a status line says why, if a worker was left behind (its
    /// plugin may still be in use), if the deadline has already passed (the cleanups would be
    /// given up on at once, and the process could end in the middle of one), or if the thread
    /// can't be started. The manager is left empty either way.
    ///
    /// # Arguments
    ///
    /// * `status_reps` - Receives the pool's final line, and the reason when no plugin is
    ///   cleaned up
    /// * `grace_period_deadline` - The shutdown's deadline, which the engine's wait for the thread
    ///   shares
    /// * `did_handle_logic_succeed` - Whether every worker finished before the deadline
    ///
    /// # Returns
    ///
    /// The clean-up thread with its slot, for the engine to wait for, or nothing if no plugin is
    /// cleaned up.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let plugins_cleanup =
    ///     manager.finalize_plugins(&mut status_reps, grace_period_deadline, did_all_finish);
    /// ```
    pub fn finalize_plugins(
        &mut self,
        status_reps: &mut Vec<Notif>,
        grace_period_deadline: Instant,
        did_handle_logic_succeed: bool,
    ) -> Vec<(JoinHandle<()>, TaskSlot)> {
        // The workers are joined already; this adds the pool's final line.
        self.threadpool.stop_workers(status_reps);
        // The clean-up thread reports failures through the engine's status mailbox.
        let tx = self.threadpool.packet_handle.clone();

        // Taken out either way, so the manager is left empty; plugins that aren't cleaned up
        // are dropped at the end of this call.
        let plugins = self.finalize_get_plugins();

        if !did_handle_logic_succeed {
            // A worker left behind may still be running a task of one of these plugins.
            status_reps.push(Log::err(
                "plugin threads did not exit correctly, cannot cleanup plugins".to_string(),
            ));

            Vec::new()
        } else if Instant::now() >= grace_period_deadline {
            // Started now, the cleanups would be given up on at once, and the process could end
            // in the middle of one.
            status_reps.push(Log::err(
                "the shutdown grace period ran out before the plugins' cleanup could start, so no \
                 plugin was cleaned up"
                    .to_string(),
            ));

            Vec::new()
        } else {
            let worker_slot = Arc::new(AtomicCell::new(None));
            let worker_slot_clone = worker_slot.clone();
            let builder = thread::Builder::new().name(String::from("plugin cleanup"));
            let spawn_res = builder.spawn(move || {
                for plugin in plugins {
                    // The slot names the plugin being cleaned up, so if the engine gives up on
                    // this thread, `Error.log` can say whose cleanup didn't return.
                    worker_slot_clone.swap(Some(Box::new(plugin.plg_type().to_string())));

                    if let Err(e) = plugin.clean_up() {
                        let e_message = get_err(&e);
                        tx.send_status_log(Log::err(e_message));
                    }

                    let _ = worker_slot_clone.take();
                }
            });

            match spawn_res {
                Ok(handle) => vec![(handle, worker_slot)],
                Err(e) => {
                    status_reps.push(Log::err(format!(
                        "plugin cleanup thread failed to spawn: {e}"
                    )));

                    Vec::new()
                }
            }
        }
    }
}
