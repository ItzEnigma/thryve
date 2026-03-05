use std::sync::atomic::{AtomicU64 , Ordering};

use crate::{any_naming_input::NamedInput, traits::packets::Packet, res::{self, Result}};

// Atomic counter to generate unique ids for tasks.
// This is a global static variable that is safe to use across threads.
// It starts at 1 and increments by 1 for each new task.
static COUNTER: AtomicU64 = AtomicU64::new(1);

// A Job is a function that takes no parameters and returns a Result.
// Instead of having to type the entire type every time, a type alias is defined.
pub type Job = Box<dyn FnOnce() -> Result + Send + Sync>;

// A Task is a unit of work that can be executed by the thread pool.
// This will be the object that will be sent to the thread pool.
// No other object should be sent to the thread pool.
pub struct Task{
    pub id: u64,                    // Unique identifier for the task.                                          | read only.
    pub task_name: NamedInput,   // The name of the task.                                                    | read only.
    pub base_priority: u8,          // The priority of the task. 0-4 (0 is the lowest, 4 is the highest).       | read write.
    job: Job                        // The function that will be executed by the thread pool.                   | no permissions.
}

impl Packet for Task {
    // Run the task and return the result.
    // This will execute the function that was passed to the task.
    // It will also create a ResultBullitinContainer with the result of the function and the id of the task.
    // @return A ResultBullitinContainer with the result of the function and the id of the task.
    fn process(self) -> Option<(u64 , res::Result)>{
        let id = self.id;
        Some((id , (self.job)()))
    }
}

impl Task {
    // Create a new Task with the given parameters.
    // The priority is clamped between 0 and 4.
    // @param task_name: The name of the task.
    // @param task_priority: The priority of the task.
    // @param function_to_exec: The function that will be executed by the thread pool.
    // @return A new Task with the given parameters.
    pub fn new(task_name: &str , task_priority: u8 , function_to_exec: Job) -> Self {
        Task{ 
            id: next_id(),
            task_name: NamedInput::new(task_name),
            base_priority: task_priority,
            job: function_to_exec,
        }
    }
}

impl Task {
    pub fn check_task_priority(&mut self , max_task_priority: u8){
        if self.base_priority > max_task_priority {
            self.base_priority = max_task_priority;
        }
    }
}

// Generate a new unique id for a task.
// This uses an atomic counter to ensure that the id is unique across threads.
// @return A new unsigned long unique id for a task.
fn next_id() -> u64 {
    // Get the current value of the counter and increment it by 1.
    COUNTER.fetch_add(1, Ordering::Relaxed)
}