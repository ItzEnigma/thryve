// The trait that all plugins must implement.
// A compilation error will occur if a plugin does not implement the Plugin trait and is tried to be loaded by the engine.
// This trait defines the basic functions that all plugins must have.
// This will allow the engine to interact with the plugins in a generic way.
// Each plugin will have its own implementation of these functions.
// All plugins must be Send + Sync to be used in a multithreaded context.

use crate::task::{ Job , Task , TaskName };
use crate::packet_handle::PacketHandle;
use crate::any_naming_input::ANNamingInput;

pub trait Plugin: Send + Sync {
    fn get_plugin_type(&self) -> &ANNamingInput;
    fn get_plugin_name(&self) -> &str;
    fn get_packet_handle(&self) -> PacketHandle;
    fn create_task(&self, task_name: TaskName , task_priority: u8 , job: Job) -> Task;
    fn send_task(&self, task: Task);
    fn initial_fn_to_run(&self) -> Job;
}