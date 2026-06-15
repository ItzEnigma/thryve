//! Metrics tracking for the Thryve engine.
//!
//! This module provides the [`Metrics`] struct, which tracks runtime
//! statistics about tasks, results, and notifications processed by
//! the engine. Metrics are collected in batches and periodically
//! calculated to provide average size information.
//!
//! # Batch Processing
//!
//! Metrics are collected in batches of up to 1024 items. When a batch
//! is full, the metrics are calculated and a debug notification is
//! sent with the formatted results. This batched approach reduces
//! the overhead of continuous metric calculation.
//!
//! # Fields
//!
//! - `task_count`, `res_count`, `notif_count`: Total counts (private)
//! - `avg_*_size`: Running average sizes in bytes (public)
//! - `*_size_batch`: Batch buffers for size tracking (public)
//! - `cur_batch_count_*`: Current batch indices (public)

#[derive(Debug, Clone)]
/// Tracks runtime metrics for tasks, results, and notifications.
///
/// The `Metrics` struct collects size information for packets processed
/// by the engine. It maintains running averages and batches data for
/// efficient periodic calculation.
///
/// # Batch Behavior
///
/// Each packet type (tasks, results, notifications) has a batch buffer
/// of 1024 entries. When a batch fills, metrics are calculated and
/// reported via a debug notification.
///
/// # Fields
///
/// - `task_count`, `res_count`, `notif_count`: Total packet counts
/// - `avg_task_size`, `avg_res_size`, `avg_notif_size`: Running average sizes
/// - `task_size_batch`, `res_size_batch`, `notif_size_batch`: Batch buffers
/// - `cur_batch_count_task`, `cur_batch_count_res`, `cur_batch_count_notif`: Batch indices
pub struct Metrics {
    /// Total number of tasks processed.
    pub task_count: usize,
    /// Total number of results processed.
    pub res_count: usize,
    /// Total number of notifications processed.
    pub notif_count: usize,

    /// Running average size of tasks in bytes.
    pub avg_task_size: f64,
    /// Running average size of results in bytes.
    pub avg_res_size: f64,
    /// Running average size of notifications in bytes.
    pub avg_notif_size: f64,

    /// Batch buffer for task sizes (capacity: 1024).
    pub task_size_batch: [usize; 1024],
    /// Batch buffer for result sizes (capacity: 1024).
    pub res_size_batch: [usize; 1024],
    /// Batch buffer for notification sizes (capacity: 1024).
    pub notif_size_batch: [usize; 1024],

    /// Current index in the task size batch.
    pub cur_batch_count_task: usize,
    /// Current index in the result size batch.
    pub cur_batch_count_res: usize,
    /// Current index in the notification size batch.
    pub cur_batch_count_notif: usize,
}

impl Default for Metrics {
    /// Creates a new `Metrics` instance with all counters and averages set to zero.
    ///
    /// All batch buffers are initialized to zero-filled arrays, and all
    /// batch indices are set to zero, ready to collect metrics data.
    fn default() -> Self {
        Self {
            task_count: 0,
            res_count: 0,
            notif_count: 0,
            avg_task_size: 0.0,
            avg_res_size: 0.0,
            avg_notif_size: 0.0,
            task_size_batch: [0; 1024],
            res_size_batch: [0; 1024],
            notif_size_batch: [0; 1024],
            cur_batch_count_task: 0,
            cur_batch_count_res: 0,
            cur_batch_count_notif: 0,
        }
    }
}

impl Metrics {
    /// Calculates metrics for all packet types and returns a formatted string.
    ///
    /// This method triggers metric calculation for tasks, results, and
    /// notifications, then formats the results into a human-readable
    /// string suitable for logging or display.
    ///
    /// # Returns
    ///
    /// A formatted string containing total counts and average sizes
    /// for all three packet types.
    pub fn calculate_metrics(&mut self) -> String {
        self.calculate_metrics_tasks();
        self.calculate_metrics_results();
        self.calculate_metrics_notifs();

        self.metrics_readable()
    }

    /// Calculates the running average size for tasks from the current batch.
    ///
    /// If the task batch is not empty, this method updates the running
    /// average task size, increments the total task count, and resets
    /// the batch buffer and index.
    pub fn calculate_metrics_tasks(&mut self) {
        let mut sum: usize = 0;
        let old_avg = self.avg_task_size;
        let count = self.task_count;
        let current_batch_count = self.cur_batch_count_task;

        for task_size in self.task_size_batch {
            sum += task_size;
        }

        if current_batch_count != 0 {
            self.avg_task_size = old_avg + ((sum as f64 - old_avg) / (count + current_batch_count) as f64);
            self.task_count += current_batch_count;
            self.task_size_batch = [0; 1024];
            self.cur_batch_count_task = 0;
        }
    }

    /// Calculates the running average size for results from the current batch.
    ///
    /// If the result batch is not empty, this method updates the running
    /// average result size, increments the total result count, and resets
    /// the batch buffer and index.
    pub fn calculate_metrics_results(&mut self) {
        let mut sum: usize = 0;
        let old_avg = self.avg_res_size;
        let count = self.res_count;
        let current_batch_count = self.cur_batch_count_res;

        for res_size in self.res_size_batch {
            sum += res_size;
        }

        if current_batch_count != 0 {
            self.avg_res_size = old_avg + ((sum as f64 - old_avg) / (count + current_batch_count) as f64);
            self.res_count += current_batch_count;
            self.res_size_batch = [0; 1024];
            self.cur_batch_count_res = 0;
        }
    }

    /// Calculates the running average size for notifications from the current batch.
    ///
    /// If the notification batch is not empty, this method updates the
    /// running average notification size, increments the total notification
    /// count, and resets the batch buffer and index.
    pub fn calculate_metrics_notifs(&mut self) {
        let mut sum: usize = 0;
        let old_avg = self.avg_notif_size;
        let count = self.notif_count;
        let current_batch_count = self.cur_batch_count_notif;

        for notif_size in self.notif_size_batch {
            sum += notif_size;
        }

        if current_batch_count != 0 {
            self.avg_notif_size = old_avg + ((sum as f64 - old_avg) / (count + current_batch_count) as f64);
            self.notif_count += current_batch_count;
            self.notif_size_batch = [0; 1024];
            self.cur_batch_count_notif = 0;
        }
    }

    /// Formats the collected metrics into a human-readable string.
    ///
    /// Creates a formatted string showing total counts and average sizes
    /// for tasks, results, and notifications. Sizes are automatically
    /// scaled to appropriate units (B, KB, or MB).
    ///
    /// # Returns
    ///
    /// A formatted string with metrics for all packet types.
    pub fn metrics_readable(&self) -> String {
        let mut final_string = "\n".to_string();

        final_string = format_metrics(self.task_count, self.avg_task_size, "Tasks", final_string);
        final_string = format_metrics(self.res_count, self.avg_res_size, "Results", final_string);
        final_string = format_metrics(self.notif_count, self.avg_notif_size, "Notifications", final_string);

        final_string += "___________________________________________________________\n";
        final_string
    }
}

/// Formats metrics for a single packet type into a readable string.
///
/// This helper function appends the total count and average size for
/// a given packet type to the provided string. The average size is
/// automatically scaled to appropriate units:
/// - Bytes (B) for values less than 1 KB
/// - Kilobytes (KB) for values less than 1 MB
/// - Megabytes (MB) for larger values
///
/// # Arguments
///
/// * `count` - Total number of packets of this type
/// * `avg` - Average size in bytes
/// * `str_to_display` - Human-readable name for the packet type
/// * `str_append` - Accumulator string to append to
///
/// # Returns
///
/// The updated string with metrics appended.
pub fn format_metrics(count: usize, avg: f64, str_to_display: &str, mut str_append: String) -> String {

    str_append += format!("Total {str_to_display}: {count}\n").as_str();
    if avg < 1024.0 {
        str_append += format!("average {} size: {:.3} B\n", str_to_display, avg).as_str();
    } else if avg < 1024.0 * 1024.0 {
        str_append += format!("average {} size: {:.3} KB\n", str_to_display, avg as f64 / 1024.0).as_str();
    } else {
        str_append += format!("average {} size: {:.3} MB\n", str_to_display, avg as f64 / (1024.0 * 1024.0)).as_str();
    }

    str_append
}