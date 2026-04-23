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

    /// Creates a new result with a follow-up task.
    ///
    /// Creates a result that represents an edge in the task graph,
    /// connecting to the next task to be executed. This enables
    /// task chaining and complex execution flows.
    ///
    /// # Arguments
    ///
    /// * `res_type` - Type identifier for the result
    /// * `msg` - Human-readable result message
    /// * `logging_file` - Log file name for the message
    /// * `next_task` - Optional task to execute next
    ///
    /// # Returns
    ///
    /// A new `Result` with the specified follow-up task.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::res::Result;
    /// use thryve_utils::packets::task::Task;
    ///
    /// let next = Some(Task::new("FollowUp", 1, Box::new(|| {
    ///     Result::new("Ok", "".to_string(), "Main")
    /// })));
    ///
    /// let result = Result::new_tasked_res("Chained", "Continuing".to_string(), "Main", next);
    /// assert!(result.next_task.is_some());
    /// ```
    pub fn new_tasked_res(
        res_type: &str,
        msg: String,
        logging_file: &str,
        next_task: Option<Task>,
    ) -> Self {
        Result {
            result_type: NamedInput::new(res_type),
            message: msg,
            log_msg_to_file: NamedInput::new(logging_file),
            next_task: next_task,
            observer: None,
            plugin: None,
        }
    }
}
