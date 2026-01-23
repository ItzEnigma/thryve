// The bulletin board that will store the results of the tasks.
// It will be used by the engine to get the results of the tasks.
// This system utilizes a realworld scenario to achieve full non-blocking multithreading.
// Students have exams to write (tasks) and when they finish, they submit their exams to the teacher.
// They then live their lives and do not wait for the teacher to grade their exams.
// When the teacher has time, they grade the exams and put the results on a bulletin board.
// The students can then check the bulletin board when they have time to see their results.

// The concept is very similar.
// The engine listens for tasks and submits them to the thread pool.
// The thread pool executes the tasks and when they are done, they submit the results to the bulletin board.
// The engine can then check the bulletin board later when it has time to see the results of the tasks.
// This way, the engine is never blocked and can continue to do other things while the tasks are being executed.
// Futures and async/await are not used in this system to keep it simple and easy to understand.
// It also allows for easy integration with existing synchronous code, and it allows for a custom implementation of task scheduling and execution.

use std::collections::HashMap;

use crate::{res::{self} , res_bullitin_container::ResultBullitinContainer};

// The errors that can occur when trying to get results from the bulletin board.
pub enum BullitinError {
    Empty,
}

// The bulletin board that will store the results of the tasks.
// It will be used by the engine to get the results of the tasks.
pub struct ResBullitin {
    results: HashMap<u64 , res::Result>,        // The results of the tasks. key: task id, value: result.   | no permissions.
}

impl ResBullitin {
    // Create a new empty bulletin board.
    // @return A new empty bulletin board.
    pub fn new() -> Self{
        Self { results: HashMap::new() }
    }

    // Insert a new result into the bulletin board.
    // It decomposes the ResultBullitinContainer into its components and stores them in the HashMap.
    // @param result_container: The result container to insert.
    pub fn insert(&mut self, result_container: ResultBullitinContainer) {
        self.results.insert(result_container.task_id(), result_container.task_result());
    }

    // Try to get all the results from the bulletin board.
    // If there are no results, it returns an error.
    // @return A vector of tuples containing the task id and the result, or an error if there are no results.
    pub fn try_get(&mut self) -> Result<Vec<(u64 , res::Result)> , BullitinError> {
        if self.results.is_empty(){
            return Err(BullitinError::Empty)
        }
        
        let results = self.results.drain().collect();
        Ok(results)
    }
}