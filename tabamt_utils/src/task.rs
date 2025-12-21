use std::sync::atomic::{AtomicU64 , Ordering};

use crate::{any_naming_input::ANNamingInput, res::{self, Result}, res_bullitin_container::ResultBullitinContainer};

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
    id: u64,                    // Unique identifier for the task.                                          | read only.
    task_name: ANNamingInput,   // The name of the task.                                                    | read only.
    priority: u8,               // The priority of the task. 0-4 (0 is the lowest, 4 is the highest).       | no permissions.
    job: Job                    // The function that will be executed by the thread pool.                   | no permissions.
}

impl Task {
    // Create a new Task with the given parameters.
    // The priority is clamped between 0 and 4.
    // @param task_name: The name of the task.
    // @param task_priority: The priority of the task.
    // @param function_to_exec: The function that will be executed by the thread pool.
    // @return A new Task with the given parameters.
    pub fn new(task_name: ANNamingInput , task_priority: u8 , function_to_exec: Job) -> Self {
        let mut priority = task_priority;
        if priority > 4 {
            priority = 4;
        }

        Task{ 
            id: next_id(),
            task_name: task_name,
            priority: priority,
            job: function_to_exec,
        }
    }

    // Run the task and return the result.
    // This will execute the function that was passed to the task.
    // It will also create a ResultBullitinContainer with the result of the function and the id of the task.
    // @return A ResultBullitinContainer with the result of the function and the id of the task.
    pub (crate) fn run(self) -> ResultBullitinContainer{
        let id = *self.get_id();
        let task_result: res::Result = (self.job)();
        ResultBullitinContainer::new(id, task_result)
    }
}

impl Task {
    // Get the id of the task.
    // @return The id of the task.
    pub fn get_id(&self) -> &u64 {
        &self.id
    }

    // Get the name of the task.
    // @return The name of the task.
    pub fn get_task_name(&self) -> &ANNamingInput {
        &self.task_name
    }
}

impl PartialEq for Task {
    // Two tasks are equal if they have the same priority.
    // This is used to prioritize tasks in the thread pool.
    // @param other: The other task to compare to.
    // @return true if the tasks have the same priority, false otherwise.
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}
impl Eq for Task {}

impl Ord for Task {
    // Imagine task A has priority 4 and task B has priority 2.
    // When comparing A to B, it will actually compare B's priority to A's priority (2 compared to 4).
    // This results in A being considered "greater" than B.
    // Higher priority tasks (closer to 4) will be considered "greater".
    // Lower priority tasks (closer to 0) will be considered "less".
    // This creates a max-heap behavior in the priority queue.
    // @param other: The other task to compare to.
    // @return An Ordering indicating the relative priority of the tasks.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        other.priority.cmp(&self.priority)
    }
    
}

impl PartialOrd for Task {
    // This is needed to implement Ord.
    // It simply calls the cmp function.
    // @param other: The other task to compare to.
    // @return An Option containing the Ordering indicating the relative priority of the tasks.
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Generate a new unique id for a task.
// This uses an atomic counter to ensure that the id is unique across threads.
// @return A new unsigned long unique id for a task.
fn next_id() -> u64 {
    // Get the current value of the counter and increment it by 1.
    COUNTER.fetch_add(1, Ordering::Relaxed)
}