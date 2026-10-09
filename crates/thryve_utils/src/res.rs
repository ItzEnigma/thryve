// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! TaskResult — the result type for task execution.
//!
//! This module defines the [`TaskResult`] struct, which represents the outcome
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
//! - Handoff results: what an [`AdvTask`](crate::packets::task::AdvTask) or a follow-up returns.
//!   They carry the task's `Result<T, E>`, sealed, which the engine hands to the task's follow-up
//!   as a [`Handoff`], and no message. A forwarding task's carries the id of the task whose
//!   outcome becomes its own instead.

use std::any::Any;
use std::fmt::Display;
use std::panic::Location;
use std::path::PathBuf;

use crate::errors::{self, TaskError};
use crate::extras::thread_local;
use crate::graphs::callsite::TaskCallSite;
use crate::packets::task::Task;
use crate::traits::packets::Packet;

/// The result of a task's execution.
///
/// The `TaskResult` struct represents the outcome of a task execution.
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
/// - `handoff`: An [`AdvTask`](crate::packets::task::AdvTask)'s sealed result (private)
///
/// # Example
///
/// ```no_run
/// use thryve_utils::res::TaskResult;
/// use thryve_utils::packets::task::Task;
///
/// // Regular result (terminal)
/// let result = TaskResult::new("Success", "Done".to_string(), "Main");
///
/// // Tasked result (chains to next task)
/// let next_task = Task::new("NextTask", 1, || {
///     TaskResult::new("Ok", "".to_string(), "Main")
/// }).expect("valid task name");
/// let chained = TaskResult::new("Chained", "Moving on".to_string(), "Main")
///     .chain(next_task);
/// ```
///
#[derive(Default)]
pub struct TaskResult {
    /// Type identifier for the result. Empty means there's nothing to report.
    result_type: String,
    /// Human-readable result message.
    pub message: String,
    /// Log file name for the message.
    log_msg_to_file: String,
    /// Optional task to execute next.
    pub next_task: Option<Task>,
    /// What an [`AdvTask`](crate::packets::task::AdvTask) hands over: its sealed `Result<T, E>`.
    /// `None` for every other task.
    handoff: Option<SealedHandoff>,
}

impl Display for TaskResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "result type: {}, message: {}, next task: {}, handoff: {}",
            self.result_type,
            self.message,
            self.next_task
                .as_ref()
                .map_or("None", |task| task.task_name()),
            if let Some(handoff) = self.handoff.as_ref() {
                match handoff {
                    SealedHandoff::Finished { .. } => {
                        self.handoff_error_text().unwrap_or(String::from("a value"))
                    }
                    SealedHandoff::Forward { to } => format!("forwarded to task {to}"),
                }
            } else {
                String::from("None")
            }
        )
    }
}

impl From<TaskError> for TaskResult {
    fn from(value: TaskError) -> Self {
        Self {
            result_type: String::from("TaskError"),
            message: errors::get_err_oneline(&value),
            log_msg_to_file: String::from("Error"),
            next_task: None,
            handoff: None,
        }
    }
}

impl TaskResult {
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
    /// A new `TaskResult` with no follow-up actions.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::res::TaskResult;
    ///
    /// let result = TaskResult::new("Success", "Task completed".to_string(), "Main");
    /// assert!(result.next_task.is_none());
    /// ```
    pub fn new(res_type: &str, msg: String, logging_file: &str) -> Self {
        TaskResult {
            result_type: res_type.to_string(),
            message: msg,
            log_msg_to_file: logging_file.to_string(),
            next_task: None,
            handoff: None,
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
    /// The modified `TaskResult` with the task attached, enabling method chaining.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::res::TaskResult;
    /// use thryve_utils::packets::task::Task;
    ///
    /// // Create a result and chain a follow-up task
    /// let next_task = Task::new("FollowUp", 1, || {
    ///     TaskResult::new("Done", "Completed".to_string(), "Main")
    /// }).expect("valid task name");
    ///
    /// let chained_result = TaskResult::new("Step1", "First step".to_string(), "Main")
    ///     .chain(next_task);
    ///
    /// assert!(chained_result.next_task.is_some());
    /// ```
    ///
    /// ```no_run
    /// use thryve_utils::res::TaskResult;
    /// use thryve_utils::packets::task::Task;
    ///
    /// // Create a result and chain a follow-up task
    /// let next_task_1 = Task::new("FollowUp2", 1, || {
    ///     TaskResult::new("Done", "Completed".to_string(), "Main")
    /// }).expect("valid task name");
    ///
    ///
    /// let next_task_2 = Task::new("FollowUp2", 1, || {
    ///     TaskResult::new("Done", "Completed".to_string(), "Main")
    /// }).expect("valid task name");
    ///
    /// // A double chain should make next_task_2 overwrite next_task_1
    /// let chained_result = TaskResult::new("Step1", "First step".to_string(), "Main")
    ///     .chain(next_task_1).chain(next_task_2);
    ///
    /// assert!(chained_result.next_task.unwrap().task_name() == "FollowUp2");
    /// ```
    #[track_caller]
    pub fn chain(mut self, mut task: Task) -> Self {
        if thread_local::tl_get_current_task_fingerprint() != String::default() {
            let caller = Location::caller();
            let file_path = PathBuf::from(caller.file().to_string());
            let line_num = caller.line();
            let task_name = task.task_name().to_string();

            let mut callsite = TaskCallSite::new(file_path, line_num, task_name);

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
    /// memory allocated by its fields (such as the `String` message,
    /// the optional `Task` or the handoff's sealed result).
    pub fn size_in_bytes(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

impl TaskResult {
    /// A result of type `Fin`, which tells the engine to shut down.
    ///
    /// Its message is written to `Main.log` before the engine finalizes.
    pub fn fin() -> Self {
        TaskResult {
            result_type: "Fin".to_string(),
            message: String::from("finishing capture: starting exit now."),
            log_msg_to_file: "Main".to_string(),
            next_task: None,
            handoff: None,
        }
    }

    /// Returns the result's type. An empty type means there's nothing to report.
    pub fn result_type(&self) -> &str {
        &self.result_type
    }

    /// Returns the name of the log file the message is written to.
    pub fn log_msg_to_file(&self) -> &str {
        &self.log_msg_to_file
    }
}

impl TaskResult {
    /// The result an [`AdvTask`](crate::packets::task::AdvTask)'s job returns: it carries the
    /// handoff and nothing to log.
    ///
    /// Its type is `AdvTask`, so the shutdown's summary of lost work groups these results under
    /// that name. The shutdown also uses it to put a handoff the engine kept back into a result,
    /// so that it's counted like the results that arrived late.
    ///
    /// For Thryve's own crates. Not part of the public API.
    #[doc(hidden)]
    pub fn new_handoff(handoff: SealedHandoff) -> Self {
        Self {
            result_type: String::from("AdvTask"),
            message: String::default(),
            log_msg_to_file: String::default(),
            next_task: None,
            handoff: Some(handoff),
        }
    }

    /// Takes the handoff out, leaving `None`. The engine calls it first thing when a result
    /// arrives.
    ///
    /// For Thryve's own crates. Not part of the public API.
    #[doc(hidden)]
    pub fn take_handoff(&mut self) -> Option<SealedHandoff> {
        self.handoff.take()
    }

    /// The handoff's error text, if it carries an error, without taking the handoff out.
    ///
    /// `None` when there's no handoff, and when it carries a value or a forward. For Thryve's own
    /// crates. Not part of the public API.
    #[doc(hidden)]
    pub fn handoff_error_text(&self) -> Option<String> {
        if let Some(handoff) = self.handoff.as_ref() {
            return handoff.error_text();
        }

        None
    }
}

/// What an [`AdvTask`](crate::packets::task::AdvTask) hands over when it finishes: its result,
/// sealed so the engine can carry it without knowing its type.
///
/// The worker that ran the task builds it. The engine moves it around but never opens it: it
/// doesn't know `T` or `E`, and it never runs task code. Only code that knows the types opens it:
/// the job of the task's follow-up, on a worker.
///
/// A forwarding task hands over a [`Forward`](Self::Forward) instead: not a result, but the id of
/// the task whose outcome becomes its own. Nothing in it is sealed; the engine reads the id and
/// links the two tasks, so a follow-up never gets a forward.
///
/// For Thryve's own crates. Not part of the public API.
#[doc(hidden)]
pub enum SealedHandoff {
    /// The task's code returned.
    Finished {
        /// The task's whole `Result<T, E>`, boxed: its value, or its own typed error.
        sealed: Box<dyn Any + Send>,
        /// The error's text, written on the worker while the type was still known, so the engine
        /// can log an error that nobody handles. `None` when the task returned a value.
        error_text: Option<String>,
    },

    /// The task forwarded: its closure returned the handle of another task, whose outcome becomes
    /// this task's. Whoever follows this task gets that task's result.
    Forward {
        /// The id of the task forwarded to.
        to: u64,
    },
}

/// What a follow-up gets: the result of the task it follows, as the task's value or its own error.
///
/// A follow-up is declared with
/// [`AdvTaskHandle::then`](crate::packets::task::AdvTaskHandle::then), and its closure takes a
/// `Handoff<T, E>`, where `T` and `E` are the task's types. The error is the task's own, typed,
/// with its fields and causes. Once a follow-up gets it, the engine doesn't write it to
/// `Error.log`: what happens to it is up to the follow-up.
#[derive(Debug)]
pub enum Handoff<T: Send + 'static, E: std::error::Error + Send + 'static> {
    /// The task returned `Ok`: its value.
    Value(T),
    /// The task returned `Err`: its own error.
    Error(E),
}

impl SealedHandoff {
    /// Whether the task's code returned: a value or an error, sealed.
    pub fn is_finish(&self) -> bool {
        matches!(self, SealedHandoff::Finished { .. })
    }

    /// Whether the task forwarded to another task.
    pub fn is_forward(&self) -> bool {
        matches!(self, SealedHandoff::Forward { .. })
    }

    /// The id of the task forwarded to, or `None` if the task's code returned instead.
    pub fn read_forward_id(&self) -> Option<u64> {
        match self {
            Self::Forward { to } => Some(*to),
            _ => None,
        }
    }

    /// The error's text, if the task returned an error. `None` when it returned a value, or
    /// forwarded.
    pub fn error_text(&self) -> Option<String> {
        match self {
            SealedHandoff::Finished {
                sealed: _,
                error_text: e,
            } => e.clone(),
            _ => None,
        }
    }

    /// Opens the handoff as the [`Handoff`] a follow-up gets. `None` if it doesn't hold a
    /// `Result<T, E>`, which only a bug in Thryve can cause: `then` ties a follow-up's types to the
    /// task's, and the engine follows a forward instead of handing it over.
    pub(crate) fn open<T, E>(self) -> Option<Handoff<T, E>>
    where
        T: Send + 'static,
        E: std::error::Error + Send + 'static,
    {
        match self {
            SealedHandoff::Finished { sealed, .. } => sealed
                .downcast::<Result<T, E>>()
                .ok()
                .map(|result| Handoff::from(*result)),

            _ => None,
        }
    }
}

impl<T, E> From<Result<T, E>> for Handoff<T, E>
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    fn from(value: Result<T, E>) -> Self {
        match value {
            Ok(adv_task) => Handoff::Value(adv_task),
            Err(e) => Handoff::Error(e),
        }
    }
}
