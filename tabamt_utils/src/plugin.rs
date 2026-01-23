// The trait that all plugins must implement.
// A compilation error will occur if a plugin does not implement the Plugin trait and is tried to be loaded by the engine.
// This trait defines the basic functions that all plugins must have.
// This will allow the engine to interact with the plugins in a generic way.
// Each plugin will have its own implementation of these functions.
// All plugins must be Send + Sync to be used in a multithreaded context.

use crate::task::{ Job , Task };
use crate::packet_handle::PacketHandle;
use crate::any_naming_input::ANNamingInput;

pub trait Plugin: Send + Sync {
    // Getters for plugin metadata

    // Get the type of the plugin
    // @return The type of the plugin as an ANNamingInput
    fn plugin_type(&self) -> &ANNamingInput;

    // Get the name of the plugin
    // @return The name of the plugin as a &str
    fn plugin_name(&self) -> &str;

    // Get the version of the plugin
    // @return The version of the plugin as a &str
    fn packet_handle(&self) -> PacketHandle;

    // Core plugin functionality

    // Create a new task
    // @param task_name The name of the task as an ANNamingInput
    // @param task_priority The priority of the task as a u8
    // @param job The job to be executed as a Job
    // @return The created Task
    fn create_task(&self, task_name: ANNamingInput , task_priority: u8 , job: Job) -> Task;

    // Send a task to be executed
    // @param task The task to be sent as a Task
    fn send_task(&self, task: Task);

    // Get the initial function to run when the plugin is loaded
    // @return The initial Job to be run
    fn initial_fn_to_run(&self) -> Job;
}