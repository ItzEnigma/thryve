// The trait that all plugins must implement.
// A compilation error will occur if a plugin does not implement the Plugin trait and is tried to be loaded by the engine.
// This trait defines the basic functions that all plugins must have.
// This will allow the engine to interact with the plugins in a generic way.
// Each plugin will have its own implementation of these functions.
// All plugins must be Send + Sync to be used in a multithreaded context.

use crate::packets::{notif_log::NotifLog , packet_handle::PacketHandle};
use crate::packets::task::Job;
use crate::extras::named_input::NamedInput;

pub trait Plugin: Send + Sync {
    // Getters for plugin metadata

    // Get the type of the plugin
    // @return The type of the plugin as an NamedInput
    fn plugin_type(&self) -> &NamedInput;

    // Get the name of the plugin
    // @return The name of the plugin as a &str
    fn plugin_name(&self) -> &str;

    // Get the version of the plugin
    // @return The version of the plugin as a &str
    fn packet_handle(&self) -> PacketHandle;

    // Core plugin functionality

    // Get the initial function to run when the plugin is loaded
    // @return The initial Job to be run
    fn initial_fn_to_run(&self) -> Job;

    fn clean_up(self: Box<Self>) {
        let p_type = self.plugin_type().inner();

        let notif = NotifLog::log(format!("Plugin - {} Has Cleaned Up Successfully." , p_type), "Main");
        self.packet_handle().send_notification(notif);
    }
}