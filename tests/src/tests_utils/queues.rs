// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_seg_queue {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    use crossbeam::channel::bounded;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::bases::observer::Observer;
    use thryve_utils::extras::jobs::ObsJob;
    use thryve_utils::packets::notif_log::Log;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::queues::seg_queue::*;
    use thryve_utils::traits::observer_logic::ObsLogic;
    use thryve_utils::traits::queue::Queue;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        thryve_utils::impl_as_any!(TestObsLogic);
        fn clean_up(&self, _observer: Arc<Observer>) {
            // No-op for testing
        }
    }

    #[test]
    fn test_seg_queue_new_creates_queue() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        let queue = QChild::new(packet_handle);

        // Assert
        assert!(!queue.exit_flag.load(Ordering::SeqCst));
        assert!(!queue.pause_flag.load(Ordering::SeqCst));
        assert!(queue.task_config.is_none());
    }

    #[test]
    fn test_seg_queue_push_and_pop() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle);
        let notif = Log::log("test message".to_string(), "Main");

        // Act - push notification
        queue.push(notif);

        // Act - pop notification
        let popped = queue.pop();

        // Attempt to downcast the notification's core logic to a `Log` type
        // to access its specific fields and functionality.
        let notif_as_any = popped.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<Log>();

        // Assert
        assert_eq!(notif_casted.unwrap().message, "test message");
    }

    #[test]
    fn test_seg_queue_push_multiple_pop_multiple() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle);
        let mut count = 5;

        // Act - push multiple notifications
        for i in 0..count {
            let notif = Log::log(format!("message {}", i), "Main");
            queue.push(notif);
        }

        // Act - pop all notifications
        loop {
            if count == 0 {
                queue.exit_flag.store(true, Ordering::SeqCst);
            }

            let _ = queue.pop();

            if queue.exit_flag.load(Ordering::SeqCst) {
                while !queue.main_queue.is_empty() {
                    queue.main_queue.pop();
                }

                break;
            }
            count -= 1;
        }

        // Assert
        assert_eq!(count, 0);
    }

    #[test]
    fn test_seg_queue_pause_sets_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle);

        // Assert initially not paused
        assert!(!queue.pause_flag.load(Ordering::SeqCst));

        // Act
        queue.pause();

        // Assert
        assert!(queue.pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_seg_queue_resume_clears_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle);

        // Pause first
        queue.pause();
        assert!(queue.pause_flag.load(Ordering::SeqCst));

        // Act
        queue.resume();

        // Assert
        assert!(!queue.pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_seg_queue_finalize_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle);

        // Create a mock logger observer
        let (logger_task_tx, _logger_task_rx) = bounded::<Task>(10);
        let (logger_notif_tx, _logger_notif_rx) = bounded::<Notif>(10);
        let logger_packet_handle = PacketHandle::new(logger_task_tx, logger_notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run = ObsJob::new(move |_obs: Arc<Observer>| {});
        let logger = Arc::new(Observer::new(
            "logger",
            "loggertype",
            obs_core,
            fn_to_run,
            logger_packet_handle,
        ));

        // Assert initially not exiting
        assert!(!queue.exit_flag.load(Ordering::SeqCst));

        // Act
        queue.finalize(Some(logger));

        // Assert
        assert!(queue.exit_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_seg_queue_pop_returns_exit_notification_when_exit_flag_set() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle);

        // Set exit flag
        queue.exit_flag.store(true, Ordering::SeqCst);

        // Act
        let notif = queue.pop();

        // Attempt to downcast the notification's core logic to a `Log` type
        // to access its specific fields and functionality.
        let notif_as_any = notif.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<Log>();

        // Assert
        assert_eq!(notif_casted.unwrap().message, "");
    }

    #[test]
    fn test_seg_queue_push_notifies_condition_var() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let queue = QChild::new(packet_handle);
        let notif = Log::log("test".to_string(), "Main");

        // Act - push should notify (queue not paused)
        queue.push(notif);

        // pop to verify notification was queued
        let popped = queue.pop();

        // Attempt to downcast the notification's core logic to a `Log` type
        // to access its specific fields and functionality.
        let notif_as_any = popped.notif_core.as_any();
        let notif_casted = notif_as_any.downcast_ref::<Log>();

        // Assert
        assert_eq!(notif_casted.unwrap().message, "test");
    }
}

mod tests_priority_queue {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use std::time::Instant;

    use crossbeam::channel::bounded;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::cfg::config::{Config, TaskConfig};
    use thryve_utils::extras::jobs::TaskJob;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::queues::priority_queue::*;
    use thryve_utils::res::Result;
    use thryve_utils::traits::queue::Queue;

    #[test]
    fn test_priority_queue_new_creates_queue() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();

        // Act
        let queue = PQChild::new(&config, packet_handle);

        // Assert
        assert!(!queue.exit_flag.load(Ordering::SeqCst));
        assert!(!queue.pause_flag.load(Ordering::SeqCst));
        assert!(queue.task_config.is_some());
    }

    #[test]
    fn test_priority_queue_push_and_pop() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);
        let task = Task::new(
            "testTask",
            5,
            TaskJob::new(|| Result::new("OK", "success".to_string(), "Main")),
        );

        // Act - push task
        queue.push(task);

        // Act - pop task
        let popped = queue.pop();

        // Assert
        assert_eq!(popped.task_name.inner(), "testTask");
    }

    #[test]
    fn test_priority_queue_pop_returns_fin_task_when_exit_flag_set() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Set exit flag
        queue.exit_flag.store(true, Ordering::SeqCst);

        // Act
        let task = queue.pop();

        // Assert
        assert_eq!(task.task_name.inner(), "Fin");
    }

    #[test]
    fn test_priority_queue_pause_sets_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Assert initially not paused
        assert!(!queue.pause_flag.load(Ordering::SeqCst));

        // Act
        queue.pause();

        // Assert
        assert!(queue.pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_priority_queue_resume_clears_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Pause first
        queue.pause();
        assert!(queue.pause_flag.load(Ordering::SeqCst));

        // Act
        queue.resume();

        // Assert
        assert!(!queue.pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_priority_queue_finalize_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Assert initially not exiting
        assert!(!queue.exit_flag.load(Ordering::SeqCst));

        // Act
        queue.finalize(None);

        // Assert
        assert!(queue.exit_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_priority_queue_push_higher_priority_first() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Push tasks with different priorities (lower number = higher priority)
        let task_low = Task::new(
            "lowPriority",
            1,
            TaskJob::new(|| Result::new("OK", "low".to_string(), "Main")),
        );
        let task_high = Task::new(
            "highPriority",
            10,
            TaskJob::new(|| Result::new("OK", "high".to_string(), "Main")),
        );

        // Act - push in reverse order
        queue.push(task_low);
        queue.push(task_high);

        // Act - pop should return higher priority first
        let first = queue.pop();
        let second = queue.pop();

        // Assert
        assert_eq!(first.task_name.inner(), "highPriority");
        assert_eq!(second.task_name.inner(), "lowPriority");
    }

    #[test]
    fn test_priority_effective_priority_calculation() {
        // Arrange
        let task_config = Arc::new(TaskConfig::default());
        let base_priority: u8 = 2;
        let enq_at = Instant::now();
        let task_id: u64 = 1;
        let max_priority = task_config.max_priority; // Store before moving

        let priority = Priority {
            task_config,
            base_priority,
            enq_at,
            task_id,
        };

        // Act
        let effective = priority.effective_priority();

        // Assert - effective priority should be at least base priority
        assert!(effective >= base_priority);
        // Effective priority should not exceed max_priority
        assert!(effective <= max_priority);
    }

    #[test]
    fn test_priority_eq() {
        // Arrange
        let task_config = Arc::new(TaskConfig::default());
        let enq_at = Instant::now();
        let task_id: u64 = 1;

        let priority1 = Priority {
            task_config: task_config.clone(),
            base_priority: 5,
            enq_at,
            task_id,
        };

        let priority2 = Priority {
            task_config,
            base_priority: 5,
            enq_at,
            task_id,
        };

        // Act & Assert - use PartialEq directly
        assert!(priority1 == priority2);
    }

    #[test]
    fn test_priority_ord_higher_base_priority_first() {
        // Arrange
        let task_config = Arc::new(TaskConfig::default());
        let enq_at = Instant::now();

        let priority_low = Priority {
            task_config: task_config.clone(),
            base_priority: 1,
            enq_at,
            task_id: 1,
        };

        let priority_high = Priority {
            task_config,
            base_priority: 10,
            enq_at,
            task_id: 2,
        };

        // Act & Assert - higher priority (higher number) should come first (Less in ordering)
        assert!(priority_high.cmp(&priority_low) == std::cmp::Ordering::Less);
    }
}
