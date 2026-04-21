//! This module defines the `PluginManager` struct, which is responsible for managing plugins in the engine.
//! It provides functionality to load plugins, retrieve plugin names, and finalize the plugin manager by cleaning up resources and finalizing the thread pool.

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

/// Takes a hashmap of plugin types to their corresponding `Plugin` instances and manages a thread pool for processing tasks.
/// It provides methods to load plugins, retrieve plugin names, and finalize the plugin manager by cleaning up resources and finalizing the thread pool.
/// The thread pool is created with a number of workers specified in the configuration.
/// It uses a `PQChild` queue for managing tasks.
pub struct PluginManager {
    plugins: HashMap<NamedInput , Plugin>,      
    pub threadpool: ThreadPool<PQChild<Task> , Task>,                     
    packet_handle: PacketHandle,                            
}

impl PluginManager{
    /// Creates the manager of plugins with the provided `ctx` and Crossbeam channel.
    pub fn new(ctx: &Ctx , res_tx: Sender<(u64 , res::Result)>) -> Self{
        let queue = PQChild::new(&ctx.config, ctx.packet_handle());
        let mut num_workers = 0;

        if ctx.config.threads.worker_count != 0 {
            num_workers = ctx.config.threads.worker_count;
        }

        PluginManager{
            plugins: HashMap::new(),
            threadpool: ThreadPool::new(num_workers , queue , ctx.packet_handle() , Some(Arc::new(res_tx))),     
            packet_handle: ctx.packet_handle(),
        }
    }

    /// Returns a vector of references to `NamedInput` representing the names of the loaded plugins (the keys of the hashmap).
    pub fn plugin_names(&self) -> Vec<&NamedInput>{
        let mut vector: Vec<&NamedInput> = Vec::new();
        for plugin in self.plugins.keys(){
            vector.push(plugin);
        }

        vector
    }
    
    /// Creates a task to initialize the given `plugin` and sends the task to the engine using the `packet_handle`.
    /// It then inserts the `plugin` into the `plugins` hashmap with its type as the key.
    pub fn load_plugin(&mut self , mut plugin: Plugin){
        let task_init = Task::new("Init", 0, plugin.initial_fn_to_run());
        self.packet_handle.send_task(task_init);
        self.plugins.insert(NamedInput::new(plugin.plg_type.inner()) , plugin);
    }

    /// Is responsible for cleaning up resources used by the `PluginManager` and finalizing the thread pool.
    pub fn finalize(&mut self) {
        // Finalize the thread pool, which will clean up worker threads and any resources associated with it.
        self.threadpool.finalize_plg();
        
        // Clean up each plugin by calling their `clean_up` method.
        // It should handle any necessary resource deallocation or finalization logic specific to the plugin.
        for (_ , plugin) in self.plugins.drain(){
            plugin.clean_up();
        }

        let notif = Log::log("Plugin Manager Finalized".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }
}