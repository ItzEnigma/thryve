use crossbeam::channel::Sender;

use tabamt_utils::bases::plugin::Plugin;
use tabamt_utils::extras::named_input::NamedInput;
use tabamt_utils::cfg::ctx::Ctx;
use tabamt_utils::packets::{notif_log::Log , packet_handle::PacketHandle , task::Task};
use tabamt_utils::traits::plugin_logic::PluginB;
use tabamt_utils::res;
use tabamt_utils::queues::priority_queue::PQChild;
use tabamt_utils::threadpool::ThreadPool;

use std::{collections::HashMap, sync::Arc};

// Manages plugins and their interactions with the system.
// Handles loading and finalizing plugins.
// Ensures that plugins can be accessed and managed based on their type.
// It stores Plugins in a HashMap for efficient access.
// This makes the engine modular, but still capable of directly accessing public facing functions of the Plugins if needed.
pub struct PluginManager {
    plugins: HashMap<NamedInput , Plugin>,      // Key: Plugin type specified by the developer, Value: Plugin   | read only
    pub threadpool: ThreadPool<PQChild<Task> , Task>,                     // Manages a pool of threads for executing tasks.           | Read Only.
    packet_handle: PacketHandle,                            // Used to send notifications to the ObserverManager            | read only
}

impl PluginManager{
    // Creates a new PluginManager with an empty plugin list and a packet handle for communication with the ObserverManager.
    // @param tx: The PacketHandle used for sending notifications to the ObserverManager.
    // @return: A new instance of PluginManager.
    pub fn new(ctx: &Ctx , res_tx: Sender<(u64 , res::Result)>) -> Self{
        let queue = PQChild::new(&ctx.config, ctx.packet_handle());
        let mut num_workers = 0;

        if ctx.config.threads.worker_count != 0 {
            num_workers = ctx.config.threads.worker_count;
        }

        PluginManager{
            plugins: HashMap::new(),
            threadpool: ThreadPool::new(num_workers , queue , ctx.packet_handle() , Some(Arc::new(res_tx))),     // Default to 4 threads in the pool.
            packet_handle: ctx.packet_handle(),
        }
    }

    // Retrieves a list of all plugin types currently loaded in the manager.
    // This is useful for managing and interacting with the available plugins.
    // @return: A vector containing references to the types of all loaded plugins.
    pub fn plugin_names(&self) -> Vec<&NamedInput>{
        let mut vector: Vec<&NamedInput> = Vec::new();
        for plugin in self.plugins.keys(){
            vector.push(plugin);
        }

        vector
    }

    // Checks if a plugin of the specified type exists in the manager.
    // @param plugin_type: The type of the plugin to check for.
    // @return: true if the plugin exists, false otherwise.
    // fn check_plugin(&self , plugin_type: &NamedInput) -> bool{
    //     if self.plugins.contains_key(plugin_type) {
    //         return true;
    //     }
    //     return false;
    // }

    // Loads a new plugin into the manager.
    // This allows the system to dynamically add plugins at runtime.
    // @param plugin: The plugin to be loaded.
    pub fn load_plugin(&mut self , mut plugin: Plugin){
        let task_init = Task::new("Init", 0, plugin.initial_fn_to_run());
        self.packet_handle.send_task(task_init);
        self.plugins.insert(NamedInput::new(plugin.plg_type.inner()) , plugin);
    }

    // Finalizes the plugin manager by clearing all plugins and sending a final notification.
    // This is typically called during system shutdown to ensure that resources are cleaned up properly.
    pub fn finalize(&mut self) {
        self.threadpool.finalize_plg();
        
        for (_ , plugin) in self.plugins.drain(){
            plugin.clean_up();
        }

        let notif = Log::log("Plugin Manager Finalized".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }
}