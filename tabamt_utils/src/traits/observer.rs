// Implementation for the observer pattern.
// Observers can be loggers, audits, email sendings, etc.... Each needs to be implemented alone.

use crate::packets::task::{Job, Task};
use crate::traits::notif::Notif;
use crate::extras::named_input::NamedInput;

// The trait that needs to be implemented by any observer created.
// It needs to be Send + Sync in order to be used in a multithreaded context.
pub trait Observer: Send + Sync{
    // Function to consume a notification.
    // @param notif: A boxed dynamic notification object.
    fn consume(&mut self , notif: Box<dyn Notif>);

    // Getter methods for observer properties.

    // Get the name of the observer.
    // @return A reference to the name of the observer as a string slice.
    fn observer_name(&self) -> &str;
    
    // Get the type of the observer.
    // @return A reference to the type of the observer as an NamedInput.
    fn observer_type(&self) -> &NamedInput;

    // Create a new task
    // @param task_name The name of the task as an NamedInput
    // @param task_priority The priority of the task as a u8
    // @param job The job to be executed as a Job
    // @return The created Task
    fn create_task(&self, task_name: &str , task_priority: u8 , job: Job) -> Task;

    // Send a task to be executed
    // @param task The task to be sent as a Task
    fn send_task(&self, task: Task);

    // Get the initial function to run when the plugin is loaded
    // @return The initial Job to be run
    fn initial_fn_to_run(&self) -> Job;
}