//! Result type for task execution.
//!
//! This module defines the [`Result`] struct, which represents the outcome
//! of task execution in the Thryve system. Results serve as the mechanism
//! for tasks to communicate their completion status and trigger follow-up
//! actions.
//!
//! # Task Graph Model
//!
//! Think of tasks as nodes in a graph of operations. Results are the edges
//! that connect these nodes, carrying information about which node to
//! execute next. This design enables flexible task chaining and conditional
//! execution flows.
//!
//! # Result Types
//!
//! - Regular results: Terminal nodes that end execution
//! - Tasked results: Edges that chain to the next task
//! - Observer results: Results that spawn new observers
//! - Plugin results: Results that load new plugins

use std::fmt::Display;

use crate::bases::plugin::Plugin;
use crate::extras::named_input::NamedInput;
use crate::bases::observer::Observer;
use crate::packets::task::Task;

/// Result of task execution.
///
/// The `Result` struct represents the outcome of a task execution.
/// It serves as the mechanism for tasks to communicate their
/// completion status and trigger follow-up actions.
///
/// # Task Graph Model
///
/// Tasks are nodes in a graph of operations. Results are the edges
/// that connect these nodes, carrying information about which node
/// to execute next. This design enables flexible task chaining.
///
/// # Fields
///
/// - `result_type`: Type identifier for the result
/// - `message`: Human-readable result message
/// - `log_msg_to_file`: Log file name for the message
/// - `next_task`: Optional task to execute next
/// - `observer`: Optional observer to spawn
/// - `plugin`: Optional plugin to load
///
/// # Example
///
/// ```no_run
/// use thryve_utils::res::Result;
///
/// // Regular result (terminal)
/// let result = Result::new("Success", "Done".to_string(), "Main");
///
/// // Tasked result (chains to next task)
/// let next = Some(Task::new("NextTask", 1, Box::new(|| {
///     Result::new("Ok", "".to_string(), "Main")
/// })));
/// let chained = Result::new_tasked_res("Chained", "Moving on".to_string(), "Main", next);
/// ```
pub struct Result {
    /// Type identifier for the result.
    pub result_type: NamedInput,
    /// Human-readable result message.
    pub message: String,
    /// Log file name for the message.
    pub log_msg_to_file: NamedInput,
    /// Optional task to execute next.
    pub next_task: Option<Task>,
    /// Optional observer to spawn.
    pub observer: Option<Observer>,
    /// Optional plugin to load.
    pub plugin: Option<Plugin>,
}

impl Display for Result {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Message: {}", self.message)
    }
}

impl Result {
    /// Creates a new regular result.
    ///
    /// Creates a result that represents a terminal node in the task
    /// graph. No further tasks will be executed as a result of this
    /// outcome, effectively ending the execution flow for this branch.
    ///
    /// # Arguments
    ///
    /// * `res_type` - Type identifier for the result
    /// * `msg` - Human-readable result message
    /// * `logging_file` - Log file name for the message
    ///
    /// # Returns
    ///
    /// A new `Result` with no follow-up actions.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::res::Result;
    ///
    /// let result = Result::new("Success", "Task completed".to_string(), "Main");
    /// assert!(result.next_task.is_none());
    /// ```
    pub fn new(res_type: &str, msg: String, logging_file: &str) -> Self {
        Result {
            result_type: NamedInput::new(res_type),
            message: msg,
            log_msg_to_file: NamedInput::new(logging_file),
            next_task: None,
            observer: None,
            plugin: None,
        }
    }

    /// Chains a task to this result, creating a task edge in the execution graph.
    ///
    /// This method transforms a result into a task edge by attaching a
    /// `Task` that will be executed next. It enables fluent task chaining
    /// where the output of one task directly leads to the execution of
    /// another, forming a directed graph of operations.
    /// 
    /// **Note** that only one task can be chained at a time.
    ///
    /// # Behavior
    ///
    /// - Takes ownership of `self` and the provided `task`
    /// - Sets the `next_task` field to `Some(task)`
    /// - Returns the modified result
    ///
    /// # Arguments
    ///
    /// * `task` - The task to chain as the next execution step
    ///
    /// # Returns
    ///
    /// The modified `Result` with the task attached, enabling method chaining.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::res::Result;
    /// use thryve_utils::packets::task::Task;
    ///
    /// // Create a result and chain a follow-up task
    /// let next_task = Task::new("FollowUp", 1, Box::new(|| {
    ///     Result::new("Done", "Completed".to_string(), "Main")
    /// }));
    ///
    /// let chained_result = Result::new("Step1", "First step".to_string(), "Main")
    ///     .chain(next_task);
    ///
    /// assert!(chained_result.next_task.is_some());
    /// ```
    /// 
    /// ```no_run
    /// use thryve_utils::res::Result;
    /// use thryve_utils::packets::task::Task;
    ///
    /// // Create a result and chain a follow-up task
    /// let next_task_1 = Task::new("FollowUp2", 1, Box::new(|| {
    ///     Result::new("Done", "Completed".to_string(), "Main")
    /// }));
    /// 
    ///
    /// let next_task_2 = Task::new("FollowUp2", 1, Box::new(|| {
    ///     Result::new("Done", "Completed".to_string(), "Main")
    /// }));
    ///
    /// // A double chain should make next_task_2 overwrite next_task_1
    /// let chained_result = Result::new("Step1", "First step".to_string(), "Main")
    ///     .chain(next_task_1).chain(next_task_2);
    ///
    /// assert!(chained_result.next_task.task_name.inner() == "FollowUp2");
    /// ```
    pub fn chain(mut self , task: Task) -> Self {
        self.next_task = Some(task);
        self
    }

    /// Returns the size of this result in bytes.
    ///
    /// This method returns the memory footprint of the result struct
    /// itself, as determined by `std::mem::size_of_val`. This is
    /// useful for metrics tracking and memory profiling.
    ///
    /// # Returns
    ///
    /// The size of the result in bytes.
    ///
    /// # Note
    ///
    /// This returns the size of the struct itself, not the heap
    /// memory allocated by its fields (such as the `String` message
    /// or optional `Task`, `Observer`, or `Plugin` fields).
    pub fn size_in_bytes(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::packets::Packet;

    #[test]
    fn test_result_new_creates_regular_result() {
        // Arrange
        let result_type = "Success";
        let message = "Task completed successfully".to_string();
        let log_file = "Main";
        
        // Act
        let result = Result::new(result_type, message, log_file);
        
        // Assert
        assert_eq!(result.result_type.inner(), "Success");
        assert_eq!(result.message, "Task completed successfully");
        assert_eq!(result.log_msg_to_file.inner(), "Main");
        assert!(result.next_task.is_none());
        assert!(result.observer.is_none());
        assert!(result.plugin.is_none());
    }

    #[test]
    fn test_result_new_with_empty_message() {
        // Arrange
        let message = "".to_string();
        
        // Act
        let result = Result::new("Ok", message, "Main");
        
        // Assert
        assert_eq!(result.message, "");
    }

    #[test]
    fn test_result_new_with_special_characters_in_message() {
        // Arrange
        let message = "Error: Connection failed! @#$%".to_string();
        
        // Act
        let result = Result::new("Error", message, "Error");
        
        // Assert
        assert_eq!(result.message, "Error: Connection failed! @#$%");
    }

    #[test]
    fn test_result_chaining_with_task() {
        // Arrange
        let next_task = Task::new(
            "FollowUpTask",
            1,
            Box::new(|| Result::new("Ok", "Done".to_string(), "Main"))
        );
        
        // Act
        let result = Result::new(
            "Chained",
            "Continuing to next task".to_string(),
            "Main",
        ).chain(next_task);
        
        // Assert
        assert_eq!(result.result_type.inner(), "Chained");
        assert_eq!(result.message, "Continuing to next task");
        assert!(result.next_task.is_some());
        assert!(result.observer.is_none());
        assert!(result.plugin.is_none());
        
        // Verify the task details
        if let Some(task) = result.next_task {
            assert_eq!(task.task_name.inner(), "FollowUpTask");
            assert_eq!(task.base_priority, 1);
        }
    }

    #[test]
    fn test_result_display_impl() {
        // Arrange
        let result = Result::new("Test", "Display message".to_string(), "Main");
        
        // Act
        let display_str = format!("{}", result);
        
        // Assert
        assert_eq!(display_str, "Message: Display message");
    }

    #[test]
    fn test_result_display_with_empty_message() {
        // Arrange
        let result = Result::new("Test", "".to_string(), "Main");
        
        // Act
        let display_str = format!("{}", result);
        
        // Assert
        assert_eq!(display_str, "Message: ");
    }

    #[test]
    fn test_result_chaining_pattern() {
        // Arrange & Act - Simulate a task chain
        let task1_result = Result::new(
            "Step1",
            "First step done".to_string(),
            "Main",
        ).chain(
            Task::new("Step2", 2, Box::new(|| {
                Result::new(
                    "Step2",
                    "Second step done".to_string(),
                    "Main"
                ).chain(
                    Task::new("Step3", 3, Box::new(|| {
                        Result::new("Final", "All done".to_string(), "Main")
                    }))
                )
            }))
        );
        
        // Assert first result has a task
        assert!(task1_result.next_task.is_some());
        
        // Execute the chained task
        if let Some(task) = task1_result.next_task {
            let task2_result = task.process().unwrap().1;
            assert!(task2_result.next_task.is_some());
            
            // Execute the final task
            if let Some(final_task) = task2_result.next_task {
                let final_result = final_task.process().unwrap().1;
                assert!(final_result.next_task.is_none());
                assert_eq!(final_result.result_type.inner(), "Final");
            }
        }
    }

    #[test]
    fn test_result_with_unicode_message() {
        // Arrange
        let message = "成功！Успех! Éxito!".to_string();
        
        // Act
        let result = Result::new("Success", message, "Main");
        
        // Assert
        assert_eq!(result.message, "成功！Успех! Éxito!");
    }

    #[test]
    fn test_result_result_type_validation() {
        // Arrange & Act - These should all work (alphanumeric)
        let _r1 = Result::new("A", "msg".to_string(), "Main");
        let _r2 = Result::new("Z123", "msg".to_string(), "Main");
        let _r3 = Result::new("Type123", "msg".to_string(), "Main");
        
        // Assert - if we got here, all validations passed
    }

    #[test]
    #[should_panic(expected = "NamedInput must be alphanumeric")]
    fn test_result_invalid_result_type_panics() {
        // Arrange & Act - This should panic due to invalid result type
        let _ = Result::new("Invalid-Type", "msg".to_string(), "Main");
    }

    #[test]
    #[should_panic(expected = "NamedInput must be alphanumeric")]
    fn test_result_invalid_log_file_panics() {
        // Arrange & Act - This should panic due to invalid log file name
        let _ = Result::new("Valid", "msg".to_string(), "Invalid/File");
    }

    #[test]
    fn test_size_in_bytes_returns_struct_size() {
        // Arrange
        let result = Result::new("Success", "Done".to_string(), "Main");

        // Act
        let size = result.size_in_bytes();

        // Assert - size should match std::mem::size_of_val
        assert_eq!(size, std::mem::size_of_val(&result));
        assert!(size > 0);
    }

    #[test]
    fn test_size_in_bytes_consistent_across_instances() {
        // Arrange
        let result1 = Result::new("Type1", "Message1".to_string(), "Main");
        let result2 = Result::new("Type2", "Message2".to_string(), "Error");

        // Act
        let size1 = result1.size_in_bytes();
        let size2 = result2.size_in_bytes();

        // Assert - struct size should be the same regardless of field values
        assert_eq!(size1, size2);
    }

    #[test]
    fn test_size_in_bytes_matches_mem_size_of() {
        // Arrange
        let result = Result::new("MemSizeTest", "test message".to_string(), "Main");

        // Act
        let size = result.size_in_bytes();

        // Assert - should equal std::mem::size_of::<Result>()
        assert_eq!(size, std::mem::size_of::<Result>());
    }

    #[test]
    fn test_size_in_bytes_with_chained_result() {
        // Arrange
        let next_task = Task::new("NextTask", 1, Box::new(|| {
            Result::new("Ok", "Done".to_string(), "Main")
        }));
        let result = Result::new("Chained", "Chained message".to_string(), "Main")
            .chain(next_task);

        // Act
        let size = result.size_in_bytes();

        // Assert - size should still match std::mem::size_of_val
        assert_eq!(size, std::mem::size_of_val(&result));
        assert!(size > 0);
    }
}
