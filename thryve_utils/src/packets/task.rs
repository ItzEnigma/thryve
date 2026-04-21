//! This module defines what a `Task` is and how it works.
//! A `Task` is a packet that can be sent to the engine to be executed.
//! It has a name, a priority, and a function to execute.
//! The function to execute is a `TaskJob`, which is a type alias for a function that takes no arguments and returns a `res::Result`.
//! An `id` is also generated for each task to be able to track it and get the result of the task when it is executed.
//! The `Task` struct implements the `Packet` trait, which means that it can be processed by the engine when it is received through the task channel.

use std::sync::atomic::{AtomicU64 , Ordering};

use crate::extras::{named_input::NamedInput , jobs::TaskJob};
use crate::traits::packets::Packet;
use crate::res;

// Atomic counter to generate unique ids for tasks.
// This is a global static variable that is safe to use across threads.
// It starts at 1 and increments by 1 for each new task.
static COUNTER: AtomicU64 = AtomicU64::new(1);



/// Represents a task that can be sent to the Engine to be executed.
pub struct Task{
    pub id: u64,                    
    pub task_name: NamedInput,   
    pub base_priority: u8,          
    job: TaskJob                        
}

impl Packet for Task {
    fn process(self) -> Option<(u64 , res::Result)>{
        let id = self.id;
        Some((id , (self.job)()))
    }
}

impl Task {
    /// Creates a new task with the given name, priority, and function to execute.
    pub fn new(task_name: &str , task_priority: u8 , function_to_exec: TaskJob) -> Self {
        Task{ 
            id: next_id(),
            task_name: NamedInput::new(task_name),
            base_priority: task_priority,
            job: function_to_exec,
        }
    }
}

impl Task {
    /// Checks if the task's priority is higher than the maximum allowed priority.
    /// If so, it sets the task's priority to the maximum allowed priority.
    pub fn check_task_priority(&mut self , max_task_priority: u8){
        if self.base_priority > max_task_priority {
            self.base_priority = max_task_priority;
        }
    }
}

/// Generates a unique id for each task by incrementing the global atomic counter.
/// This function is thread-safe and can be called from multiple threads without any issues.
/// No locks are needed because the `fetch_add` method of `AtomicU64` is atomic and guarantees that each call will return a unique value.
fn next_id() -> u64 {
    // Get the current value of the counter and increment it by 1.
    COUNTER.fetch_add(1, Ordering::Relaxed)
}