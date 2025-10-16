use utils::{any_naming_input::ANNamingInput, notification::Notification, packet_handle::PacketHandle, plugin::Plugin};

use std::{collections::HashMap};

// Manages plugins and their interactions with the system.
// Handles loading and finalizing plugins.
// Ensures that plugins can be accessed and managed based on their type.
// It stores Plugins in a HashMap for efficient access.
// This makes the engine modular, but still capable of directly accessing public facing functions of the Plugins if needed.
pub struct PluginManager {
    plugins: HashMap<ANNamingInput , Box<dyn Plugin>>,     // Key: Plugin type specified by the developer, Value: Plugin   | read only
    packet_handle: PacketHandle,                        // Used to send notifications to the ObserverManager            | read only
}

impl PluginManager{
    // Creates a new PluginManager with an empty plugin list and a packet handle for communication with the ObserverManager.
    // @param tx: The PacketHandle used for sending notifications to the ObserverManager.
    // @return: A new instance of PluginManager.
    pub fn new(tx: PacketHandle) -> Self{
        PluginManager{
            plugins: HashMap::new(),        
            packet_handle: tx,
        }
    }

    // Retrieves a list of all plugin types currently loaded in the manager.
    // This is useful for managing and interacting with the available plugins.
    // @return: A vector containing references to the types of all loaded plugins.
    pub fn get_plugin_names(&self) -> Vec<&ANNamingInput>{
        let mut vector: Vec<&ANNamingInput> = Vec::new();
        for plugin in self.plugins.keys(){
            vector.push(plugin);
        }

        vector
    }

    // Checks if a plugin of the specified type exists in the manager.
    // @param plugin_type: The type of the plugin to check for.
    // @return: true if the plugin exists, false otherwise.
    fn check_plugin(&self , plugin_type: &ANNamingInput) -> bool{
        if self.plugins.contains_key(plugin_type) {
            return true;
        }
        return false;
    }

    // Loads a new plugin into the manager.
    // This allows the system to dynamically add plugins at runtime.
    // @param plugin: The plugin to be loaded.
    pub fn load_plugin(&mut self , plugin: Box<dyn Plugin>){
        self.plugins.insert(plugin.get_plugin_type().clone() , plugin);
    }

    // Finalizes the plugin manager by clearing all plugins and sending a final notification.
    // This is typically called during system shutdown to ensure that resources are cleaned up properly.
    pub fn finalize(&mut self) {
        self.plugins.clear();

        let notif = Notification::log("Plugin Manager Finalized".to_string() , ANNamingInput::new("Main"));
        self.packet_handle.send_notification(notif);
    }
}


impl PluginManager{
    // A function to retrieve a specific plugin by its type.
    // @param plugin_type: The type of the plugin to retrieve.
    // @return: An Option containing a reference to the plugin if found, or None if not found.
    fn get_plugin(&mut self , plugin_type: &ANNamingInput) -> Option<&Box<dyn Plugin>>{
        assert!(self.check_plugin(plugin_type), "Plugin Manager Cannot Find Requested Plugin. Got {}" , plugin_type);
        self.plugins.get(plugin_type)
    }
}