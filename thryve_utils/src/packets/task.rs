//! Task packet implementation for the Thryve engine.
//!
//! This module provides the [`Task`] struct, which represents a unit of
//! work that can be submitted to the engine for execution. Tasks are
//! the primary mechanism for triggering plugin operations and are
//! processed by worker threads according to their priority.
//!
//! # Task Lifecycle
//!
//! 1. A task is created with a name, priority, and execution function
//! 2. The task is sent to the engine via [`PacketHandle`](crate::packets::packet_handle::PacketHandle)
//! 3. The engine queues the task in a priority queue
//! 4. A worker thread pops and executes the task
//! 5. The task returns a [`Result`](crate::res::Result) indicating completion
//!
//! # Unique Identification
//!
//! Each task is assigned a unique ID upon creation using an atomic
//! counter. This ID can be used to track task execution and correlate
//! results with their originating tasks.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::extras::{named_input::NamedInput, jobs::TaskJob};
use crate::graphs::callsite::TaskCallSite;
use crate::traits::packets::Packet;
use crate::res;

/// Atomic counter for generating unique task IDs.
///
/// This global static variable is safe to use across threads.
/// It starts at 1 and increments by 1 for each new task.
static COUNTER: AtomicU64 = AtomicU64::new(1);

/// A unit of work for the Thryve engine.
///
/// The `Task` struct represents a packet that can be sent to the engine
/// for execution. Each task has a unique ID, a name, a priority level,
/// and a function to execute. Tasks are processed by worker threads
/// according to their priority and aging.
///
/// # Graph Visualization
///
/// When graph visualization is enabled, tasks can carry call site
/// information via the [`callsite`](Task::callsite) field. This information
/// is automatically captured by [`PacketHandle::send_task`](crate::packets::packet_handle::PacketHandle::send_task)
/// and includes:
///
/// - The file path where the task was created
/// - The module path in the Rust source
/// - The line number of creation
/// - Fingerprints for graph node identification
///
/// # Fields
///
/// - `id`: Unique identifier assigned upon creation
/// - `callsite`: Optional call site information for graph visualization
/// - `task_name`: Human-readable name for debugging and logging
/// - `base_priority`: Priority level (higher = more urgent)
/// - `job`: The execution function (private)
///
/// # Example
///
/// ```no_run
/// use thryve_utils::packets::task::Task;
/// use thryve_utils::res::Result;
///
/// let task = Task::new("MyTask", 2, Box::new(|| {
///     // Task execution logic
///     Result::new("Success", "Done".to_string(), "Main")
/// }));
///
/// println!("Task ID: {}", task.id);
/// ```
pub struct Task {
    /// Unique identifier assigned upon creation.
    pub id: u64,
    /// Optional call site information for graph visualization.
    ///
    /// This field is populated when graph visualization is enabled
    /// and contains information about where this task was created
    /// in the source code.
    pub callsite: Option<TaskCallSite>,
    /// Human-readable name for debugging and logging.
    pub task_name: NamedInput,
    /// Priority level (higher = more urgent).
    pub base_priority: u8,
    /// The execution function (private).
    job: TaskJob,
}

impl Packet for Task {
    /// Executes the task and returns the result.
    ///
    /// This method implements the [`Packet`] trait for tasks, providing
    /// the execution logic that worker threads invoke. The task's job
    /// function is called and wrapped in a result tuple containing the
    /// task ID for tracking.
    ///
    /// # Returns
    ///
    /// A tuple of `(task_id, result)` on success, or `None` if execution
    /// fails (which should not happen under normal circumstances).
    fn process(self) -> Option<(u64, res::Result)> {
        let id = self.id;
        Some((id, self.job.call_once()))
    }

    /// Sets the call site information for this task.
    ///
    /// This method is called by [`PacketHandle::send_task`](crate::packets::packet_handle::PacketHandle::send_task)
    /// when graph visualization is enabled. It attaches call site
    /// information (file path, module path, line number, fingerprints)
    /// to the task for topology graph construction.
    ///
    /// # Arguments
    ///
    /// * `callsite` - The call site information to attach to this task
    fn set_callsite(&mut self , callsite: TaskCallSite){
        self.callsite = Some(callsite);
    }

    /// Returns the call site information for this task.
    ///
    /// This method provides access to the call site information that
    /// was attached by [`PacketHandle::send_task`](crate::packets::packet_handle::PacketHandle::send_task)
    /// when graph visualization is enabled.
    ///
    /// # Returns
    ///
    /// The call site information if available, or a default empty
    /// `TaskCallSite` if no call site was captured.
    fn get_callsite(&self) -> TaskCallSite {
        if let Some(callsite) = self.callsite.clone(){
            callsite
        }

        else {
            TaskCallSite::default()
        }
    }
}

impl Task {
    /// Creates a new task with the specified configuration.
    ///
    /// Initializes a new task with a unique ID, the given name, priority,
    /// and execution function. The task is ready to be submitted to the
    /// engine via a [`PacketHandle`](crate::packets::packet_handle::PacketHandle).
    ///
    /// # Arguments
    ///
    /// * `task_name` - Human-readable name for the task
    /// * `task_priority` - Priority level (higher = more urgent)
    /// * `function_to_exec` - The function to execute when the task runs
    ///
    /// # Returns
    ///
    /// A new `Task` instance with a unique ID.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::task::Task;
    /// use thryve_utils::res::Result;
    ///
    /// let task = Task::new("CalculateSum", 1, Box::new(|| {
    ///     let result = 2 + 2;
    ///     Result::new("Success", format!("Result: {}", result), "Main")
    /// }));
    /// ```
    pub fn new(task_name: &str, task_priority: u8, function_to_exec: TaskJob) -> Self {
        if task_name.is_empty() {
            panic!("The field `task_name` in a Task cannot be empty");
        }

        Task {
            id: next_id(),
            callsite: None,
            task_name: NamedInput::new(task_name),
            base_priority: task_priority,
            job: function_to_exec,
        }
    }

    /// Returns the size of this task in bytes.
    ///
    /// This method returns the memory footprint of the task struct
    /// itself, as determined by `std::mem::size_of_val`. This is
    /// useful for metrics tracking and memory profiling.
    ///
    /// # Returns
    ///
    /// The size of the task in bytes.
    ///
    /// # Note
    ///
    /// This returns the size of the struct itself, not the heap
    /// memory allocated by its fields (such as the `NamedInput` or
    /// the closure in `TaskJob`).
    pub fn size_in_bytes(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

impl Task {
    /// Ensures the task's priority does not exceed the maximum allowed.
    ///
    /// If the task's priority is higher than the configured maximum,
    /// it is clamped down to the maximum. This prevents invalid priority
    /// levels from causing issues in the priority queue.
    ///
    /// # Arguments
    ///
    /// * `max_task_priority` - The maximum allowed priority level
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::task::Task;
    ///
    /// let mut task = Task::new("HighPriority", 10, Box::new(|| {
    ///     // ...
    /// }));
    ///
    /// task.clamp_task_priority(4); // Priority clamped to 4
    /// assert_eq!(task.base_priority, 4);
    /// ```
    pub fn clamp_task_priority(&mut self, max_task_priority: u8) {
        if self.base_priority > max_task_priority {
            self.base_priority = max_task_priority;
        }
    }
}

impl Default for Task {
    fn default() -> Self {
        Task::new("DefTask" , 0 , TaskJob::default())
    }
}

/// Generates a unique ID for a new task.
///
/// This function atomically increments the global counter and returns
/// the previous value. It is thread-safe and requires no locks due to
/// the atomic nature of `fetch_add`.
///
/// # Returns
///
/// A unique ID that has not been used by any previous task.
fn next_id() -> u64 {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}