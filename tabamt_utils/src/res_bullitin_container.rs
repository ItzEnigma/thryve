// This file wraps the result to be "hung" in the ResBullitin struct.

use crate::res::Result;

// A wrapper struct to hold the result of a task along with its ID.
pub struct ResultBullitinContainer {
    task_result: Result,
    task_id: u64,
}

// 
impl  ResultBullitinContainer {
    // Create a new ResultBullitinContainer with the given ID and result.
    // @param id: The ID of the task.
    // @param res: The result of the task.
    // @return A new ResultBullitinContainer with the given ID and result.
    pub fn new(id: u64 , res: Result) -> Self{
        ResultBullitinContainer { task_result: res, task_id: id }
    }

    // Get the result of the task.
    // @return The result of the task.
    pub fn get_task_result(self) -> Result{
        self.task_result
    }

    // Get the ID of the task.
    // @return The ID of the task.
    pub fn get_task_id(&self) -> u64{
        self.task_id
    }
}