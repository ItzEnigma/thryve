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
    task_count: usize,
    /// Total number of results processed.
    res_count: usize,
    /// Total number of notifications processed.
    notif_count: usize,

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
    fn calculate_metrics_tasks(&mut self) {
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
    fn calculate_metrics_results(&mut self) {
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
    fn calculate_metrics_notifs(&mut self) {
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
    fn metrics_readable(&self) -> String {
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
fn format_metrics(count: usize, avg: f64, str_to_display: &str, mut str_append: String) -> String {

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_default_initializes_all_fields_to_zero() {
        // Arrange & Act
        let metrics = Metrics::default();

        // Assert
        assert_eq!(metrics.avg_task_size, 0.0);
        assert_eq!(metrics.avg_res_size, 0.0);
        assert_eq!(metrics.avg_notif_size, 0.0);
        assert_eq!(metrics.cur_batch_count_task, 0);
        assert_eq!(metrics.cur_batch_count_res, 0);
        assert_eq!(metrics.cur_batch_count_notif, 0);
        assert_eq!(metrics.task_size_batch.iter().all(|&x| x == 0), true);
        assert_eq!(metrics.res_size_batch.iter().all(|&x| x == 0), true);
        assert_eq!(metrics.notif_size_batch.iter().all(|&x| x == 0), true);
    }

    #[test]
    fn test_metrics_clone_preserves_all_fields() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.avg_task_size = 100.0;
        metrics.avg_res_size = 200.0;
        metrics.avg_notif_size = 300.0;
        metrics.cur_batch_count_task = 10;
        metrics.cur_batch_count_res = 20;
        metrics.cur_batch_count_notif = 30;

        // Act
        let cloned = metrics.clone();

        // Assert
        assert_eq!(cloned.avg_task_size, 100.0);
        assert_eq!(cloned.avg_res_size, 200.0);
        assert_eq!(cloned.avg_notif_size, 300.0);
        assert_eq!(cloned.cur_batch_count_task, 10);
        assert_eq!(cloned.cur_batch_count_res, 20);
        assert_eq!(cloned.cur_batch_count_notif, 30);
    }

    #[test]
    fn test_calculate_metrics_tasks_with_empty_batch() {
        // Arrange
        let mut metrics = Metrics::default();

        // Act
        metrics.calculate_metrics_tasks();

        // Assert - nothing should change with empty batch
        assert_eq!(metrics.avg_task_size, 0.0);
        assert_eq!(metrics.cur_batch_count_task, 0);
    }

    #[test]
    fn test_calculate_metrics_tasks_with_single_value() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = 100;
        metrics.cur_batch_count_task = 1;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert
        assert_eq!(metrics.avg_task_size, 100.0);
        assert_eq!(metrics.cur_batch_count_task, 0);
        assert_eq!(metrics.task_size_batch[0], 0); // Batch reset
    }

    #[test]
    fn test_calculate_metrics_tasks_with_multiple_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = 100;
        metrics.task_size_batch[1] = 200;
        metrics.task_size_batch[2] = 300;
        metrics.cur_batch_count_task = 3;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert - average of 100, 200, 300 = 200
        assert_eq!(metrics.avg_task_size, 200.0);
        assert_eq!(metrics.cur_batch_count_task, 0);
    }

    #[test]
    fn test_calculate_metrics_tasks_running_average() {
        // Arrange
        let mut metrics = Metrics::default();
        
        // First batch: single value of 100
        metrics.task_size_batch[0] = 100;
        metrics.cur_batch_count_task = 1;
        metrics.calculate_metrics_tasks();
        
        // Second batch: single value of 200
        metrics.task_size_batch[0] = 200;
        metrics.cur_batch_count_task = 1;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert - running average of 100 and 200 = 150
        assert_eq!(metrics.avg_task_size, 150.0);
    }

    #[test]
    fn test_calculate_metrics_results_with_single_value() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.res_size_batch[0] = 250;
        metrics.cur_batch_count_res = 1;

        // Act
        metrics.calculate_metrics_results();

        // Assert
        assert_eq!(metrics.avg_res_size, 250.0);
        assert_eq!(metrics.cur_batch_count_res, 0);
    }

    #[test]
    fn test_calculate_metrics_results_with_multiple_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.res_size_batch[0] = 100;
        metrics.res_size_batch[1] = 300;
        metrics.cur_batch_count_res = 2;

        // Act
        metrics.calculate_metrics_results();

        // Assert - average of 100, 300 = 200
        assert_eq!(metrics.avg_res_size, 200.0);
    }

    #[test]
    fn test_calculate_metrics_notifs_with_single_value() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.notif_size_batch[0] = 50;
        metrics.cur_batch_count_notif = 1;

        // Act
        metrics.calculate_metrics_notifs();

        // Assert
        assert_eq!(metrics.avg_notif_size, 50.0);
        assert_eq!(metrics.cur_batch_count_notif, 0);
    }

    #[test]
    fn test_calculate_metrics_notifs_with_multiple_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.notif_size_batch[0] = 25;
        metrics.notif_size_batch[1] = 75;
        metrics.notif_size_batch[2] = 100;
        metrics.cur_batch_count_notif = 3;

        // Act
        metrics.calculate_metrics_notifs();

        // Assert - average of 25, 75, 100 = 66.666...
        assert!((metrics.avg_notif_size - 66.666666).abs() < 0.0001);
    }

    #[test]
    fn test_calculate_metrics_calculates_all_types() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = 100;
        metrics.cur_batch_count_task = 1;
        metrics.res_size_batch[0] = 200;
        metrics.cur_batch_count_res = 1;
        metrics.notif_size_batch[0] = 50;
        metrics.cur_batch_count_notif = 1;

        // Act
        let output = metrics.calculate_metrics();

        // Assert
        assert_eq!(metrics.avg_task_size, 100.0);
        assert_eq!(metrics.avg_res_size, 200.0);
        assert_eq!(metrics.avg_notif_size, 50.0);
        assert!(output.contains("Tasks"));
        assert!(output.contains("Results"));
        assert!(output.contains("Notifications"));
    }

    #[test]
    fn test_metrics_readable_contains_all_sections() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_count = 10;
        metrics.avg_task_size = 100.0;
        metrics.res_count = 20;
        metrics.avg_res_size = 200.0;
        metrics.notif_count = 30;
        metrics.avg_notif_size = 50.0;

        // Act
        let output = metrics.metrics_readable();

        // Assert
        assert!(output.contains("Total Tasks: 10"));
        assert!(output.contains("Total Results: 20"));
        assert!(output.contains("Total Notifications: 30"));
        assert!(output.contains("___________________________________________________________"));
    }

    #[test]
    fn test_format_metrics_displays_bytes_for_small_values() {
        // Arrange
        let input = "".to_string();

        // Act
        let output = format_metrics(5, 100.0, "Items", input);

        // Assert
        assert!(output.contains("Total Items: 5"));
        assert!(output.contains("average Items size: 100.000 B"));
    }

    #[test]
    fn test_format_metrics_displays_kb_for_medium_values() {
        // Arrange
        let input = "".to_string();
        let avg_bytes = 2048.0; // 2 KB

        // Act
        let output = format_metrics(10, avg_bytes, "Items", input);

        // Assert
        assert!(output.contains("Total Items: 10"));
        assert!(output.contains("average Items size: 2.000 KB"));
    }

    #[test]
    fn test_format_metrics_displays_mb_for_large_values() {
        // Arrange
        let input = "".to_string();
        let avg_bytes = 2_097_152.0; // 2 MB

        // Act
        let output = format_metrics(100, avg_bytes, "Items", input);

        // Assert
        assert!(output.contains("Total Items: 100"));
        assert!(output.contains("average Items size: 2.000 MB"));
    }

    #[test]
    fn test_format_metrics_boundary_at_exactly_1_kb() {
        // Arrange
        let input = "".to_string();
        let avg_bytes = 1024.0; // Exactly 1 KB - should display as KB

        // Act
        let output = format_metrics(1, avg_bytes, "Items", input);

        // Assert
        assert!(output.contains("average Items size: 1.000 KB"));
    }

    #[test]
    fn test_format_metrics_boundary_at_exactly_1_mb() {
        // Arrange
        let input = "".to_string();
        let avg_bytes = 1024.0 * 1024.0; // Exactly 1 MB

        // Act
        let output = format_metrics(1, avg_bytes, "Items", input);

        // Assert
        assert!(output.contains("average Items size: 1.000 MB"));
    }

    #[test]
    fn test_format_metrics_with_zero_count() {
        // Arrange
        let input = "".to_string();

        // Act
        let output = format_metrics(0, 0.0, "Items", input);

        // Assert
        assert!(output.contains("Total Items: 0"));
        assert!(output.contains("average Items size: 0.000 B"));
    }

    #[test]
    fn test_format_metrics_preserves_input_string() {
        // Arrange
        let input = "prefix\n".to_string();

        // Act
        let output = format_metrics(1, 100.0, "Items", input);

        // Assert
        assert!(output.starts_with("prefix"));
    }

    #[test]
    fn test_calculate_metrics_with_zero_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = 0;
        metrics.cur_batch_count_task = 1;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert
        assert_eq!(metrics.avg_task_size, 0.0);
    }

    #[test]
    fn test_calculate_metrics_with_large_values() {
        // Arrange
        let mut metrics = Metrics::default();
        metrics.task_size_batch[0] = usize::MAX / 2;
        metrics.task_size_batch[1] = usize::MAX / 2;
        metrics.cur_batch_count_task = 2;

        // Act
        metrics.calculate_metrics_tasks();

        // Assert - should handle large values without panic
        assert!(metrics.avg_task_size > 0.0);
    }
}
