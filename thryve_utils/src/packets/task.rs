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
/// # Fields
///
/// - `id`: Unique identifier assigned upon creation
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
        Some((id, (self.job)()))
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
        Task {
            id: next_id(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::packets::Packet;

    #[test]
    fn test_task_new_creates_task_with_unique_id() {
        // Arrange
        let task_name = "TestTask";
        let priority = 5;
        
        // Act
        let task1 = Task::new(task_name, priority, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        let task2 = Task::new(task_name, priority, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        // Assert
        assert_ne!(task1.id, task2.id);
        assert_eq!(task1.task_name.inner(), "TestTask");
        assert_eq!(task1.base_priority, 5);
    }

    #[test]
    fn test_task_new_with_zero_priority() {
        // Arrange & Act
        let task = Task::new("LowPriority", 0, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        // Assert
        assert_eq!(task.base_priority, 0);
    }

    #[test]
    fn test_task_new_with_max_u8_priority() {
        // Arrange & Act
        let task = Task::new("MaxPriority", u8::MAX, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        // Assert
        assert_eq!(task.base_priority, u8::MAX);
    }

    #[test]
    fn test_task_process_executes_job() {
        // Arrange
        let task = Task::new("ProcessTest", 1, Box::new(|| {
            res::Result::new("Success", "Processed".to_string(), "Main")
        }));
        let task_id = task.id;
        
        // Act
        let result = task.process();
        
        // Assert
        assert!(result.is_some());
        let (id, res_result) = result.unwrap();
        assert_eq!(id, task_id);
        assert_eq!(res_result.result_type.inner(), "Success");
        assert_eq!(res_result.message, "Processed");
    }

    #[test]
    fn test_task_clamp_task_priority_clamps_high_priority() {
        // Arrange
        let mut task = Task::new("HighPriority", 10, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let max_priority = 4u8;
        
        // Act
        task.clamp_task_priority(max_priority);
        
        // Assert
        assert_eq!(task.base_priority, 4);
    }

    #[test]
    fn test_task_clamp_task_priority_keeps_valid_priority() {
        // Arrange
        let mut task = Task::new("ValidPriority", 3, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let max_priority = 4u8;
        
        // Act
        task.clamp_task_priority(max_priority);
        
        // Assert
        assert_eq!(task.base_priority, 3);
    }

    #[test]
    fn test_task_clamp_task_priority_at_boundary() {
        // Arrange
        let mut task = Task::new("BoundaryPriority", 4, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let max_priority = 4u8;
        
        // Act
        task.clamp_task_priority(max_priority);
        
        // Assert
        assert_eq!(task.base_priority, 4);
    }

    #[test]
    fn test_task_clamp_task_priority_with_zero_max() {
        // Arrange
        let mut task = Task::new("AnyPriority", 100, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let max_priority = 0u8;
        
        // Act
        task.clamp_task_priority(max_priority);
        
        // Assert
        assert_eq!(task.base_priority, 0);
    }

    #[test]
    fn test_task_unique_ids_are_sequential() {
        // Arrange & Act
        let task1 = Task::new("T1", 1, Box::new(|| res::Result::new("Ok", "".to_string(), "Main")));
        let task2 = Task::new("T2", 1, Box::new(|| res::Result::new("Ok", "".to_string(), "Main")));
        let task3 = Task::new("T3", 1, Box::new(|| res::Result::new("Ok", "".to_string(), "Main")));
        
        // Assert
        assert_eq!(task2.id, task1.id + 1);
        assert_eq!(task3.id, task2.id + 1);
    }

    #[test]
    #[should_panic(expected = "NamedInput cannot be empty")]
    fn test_task_with_empty_name() {
        // Arrange & Act & Assert - empty name should panic
        let _task = Task::new("", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
    }

    #[test]
    fn test_task_with_numbers_in_name() {
        // Arrange & Act
        let task = Task::new("Task123", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        // Assert
        assert_eq!(task.task_name.inner(), "Task123");
    }

    #[test]
    #[should_panic(expected = "NamedInput must be alphanumeric")]
    fn test_task_with_invalid_name_panics() {
        // Arrange & Act
        let _ = Task::new("Invalid-Name", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
    }

    #[test]
    fn test_task_process_returns_correct_id() {
        // Arrange
        let task = Task::new("IdTest", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let expected_id = task.id;
        
        // Act
        let result = task.process();
        
        // Assert
        let (returned_id, _) = result.unwrap();
        assert_eq!(returned_id, expected_id);
    }

    #[test]
    fn test_size_in_bytes_returns_struct_size() {
        // Arrange
        let task = Task::new("SizeTest", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));

        // Act
        let size = task.size_in_bytes();

        // Assert - size should match std::mem::size_of_val
        assert_eq!(size, std::mem::size_of_val(&task));
        assert!(size > 0);
    }

    #[test]
    fn test_size_in_bytes_consistent_across_instances() {
        // Arrange
        let task1 = Task::new("Task1", 1, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let task2 = Task::new("Task2", 2, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));

        // Act
        let size1 = task1.size_in_bytes();
        let size2 = task2.size_in_bytes();

        // Assert - struct size should be the same regardless of field values
        assert_eq!(size1, size2);
    }

    #[test]
    fn test_size_in_bytes_matches_mem_size_of() {
        // Arrange
        let task = Task::new("MemSizeTest", 5, Box::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));

        // Act
        let size = task.size_in_bytes();

        // Assert - should equal std::mem::size_of::<Task>()
        assert_eq!(size, std::mem::size_of::<Task>());
    }
}
