// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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

use crate::bases::observer::Observer;
use crate::bases::plugin::Plugin;
use crate::extras::named_input::NamedInput;
use crate::extras::stack::get_task_data;
use crate::extras::thread_local;
use crate::packets::task::Task;
use crate::traits::packets::Packet;

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
///
#[derive(Default)]
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
    pub fn chain(mut self, mut task: Task) -> Self {
        if thread_local::tl_get_current_task_fingerprint() != String::default() {
            let backtrace = backtrace::Backtrace::new();
            let mut callsite = get_task_data(backtrace, task.task_name.to_string());

            let child_fingerprint = callsite.fingerprint();
            let current_task_fingerprint = thread_local::tl_get_current_task_fingerprint();

            callsite.this_callsite_fingerprint = child_fingerprint;
            callsite.current_executing_task_fingerprint = current_task_fingerprint;

            task.set_callsite(callsite.clone());
        }

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

impl Result {
    pub fn fin() -> Self {
        Result {
            result_type: NamedInput::new("Fin"),
            message: String::from("finishing capture: starting exit now."),
            log_msg_to_file: NamedInput::new("Main"),
            next_task: None,
            observer: None,
            plugin: None,
        }
    }
}
