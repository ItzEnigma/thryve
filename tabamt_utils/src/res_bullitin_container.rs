// This file wraps the result to be "hung" in the ResBullitin struct.

use crate::res::Result;

// A wrapper struct to hold the result of a task along with its ID.
pub struct ResultBullitinContainer {
    pub task_result: Result,    // The result of the task.  | Read Only.
    pub task_id: u64,           // The ID of the task.      | Read Only.
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
}