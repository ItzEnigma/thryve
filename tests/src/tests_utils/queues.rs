// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_seg_queue {
    use std::sync::atomic::Ordering;

    use crossbeam::channel::bounded;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::packets::notif_log::Log;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::queues::seg_queue::*;
    use thryve_utils::traits::queue::Queue;

    #[test]
    fn test_seg_queue_new_creates_queue() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);

        // Act
        let queue = QChild::new(1, packet_handle);

        // Assert
        assert!(!queue.exit_flag.load(Ordering::SeqCst));
        assert!(!queue.pause_flag.load(Ordering::SeqCst));
        // The observer queue has no task config: its config type is `()`.
    }

    #[test]
    fn test_seg_queue_push_and_pop() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = QChild::new(1, packet_handle);
        let notif = Log::log("test message".to_string(), "Main");

        // Act - push notification
        queue.push(notif);

        // Act - pop notification
        let popped = queue.pop().expect("the pushed notification");

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
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = QChild::new(1, packet_handle);
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
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = QChild::new(1, packet_handle);

        // Assert initially not paused
        assert!(!queue.pause_flag.load(Ordering::SeqCst));

        // Act
        queue.pause(false);

        // Assert
        assert!(queue.pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_seg_queue_resume_clears_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = QChild::new(1, packet_handle);

        // Pause first
        queue.pause(false);
        assert!(queue.pause_flag.load(Ordering::SeqCst));

        // Act
        queue.resume(false);

        // Assert
        assert!(!queue.pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_seg_queue_finalize_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = QChild::new(1, packet_handle);

        // Assert initially not exiting
        assert!(!queue.exit_flag.load(Ordering::SeqCst));

        // Act
        queue.finalize(&mut Vec::new(), &mut Vec::new());

        // Assert
        assert!(queue.exit_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_seg_queue_finalize_hands_back_what_is_left() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = QChild::new(1, packet_handle);
        queue.push(Log::log("first".to_string(), "Main"));
        queue.push(Log::log("second".to_string(), "Main"));

        // Act
        let mut notifs = Vec::new();
        let mut status_reps = Vec::new();
        let left_over = queue.finalize(&mut notifs, &mut status_reps);

        // Assert - both are handed back in order and counted, plus one status line
        assert_eq!(left_over, 2);
        let messages: Vec<String> = notifs
            .iter()
            .map(|notif| {
                let log = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
                log.message.clone()
            })
            .collect();
        assert_eq!(messages, ["first", "second"]);
        assert_eq!(status_reps.len(), 1);
        assert!(queue.main_queue.is_empty());
    }

    #[test]
    fn test_seg_queue_pop_returns_nothing_when_exit_flag_set() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = QChild::new(1, packet_handle);

        // Set exit flag
        queue.exit_flag.store(true, Ordering::SeqCst);

        // Act
        let notif = queue.pop();

        // Assert - nothing, rather than a made-up notification
        assert!(notif.is_none());
    }

    #[test]
    fn test_seg_queue_pop_returns_nothing_at_exit_even_with_items_left() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = QChild::new(1, packet_handle);
        queue.push(Log::log("left over".to_string(), "Main"));

        // Act
        queue.exit_flag.store(true, Ordering::SeqCst);
        let notif = queue.pop();

        // Assert - pop stops at the exit; what's left stays for the caller's exit routine
        assert!(notif.is_none());
        assert_eq!(queue.main_queue.len(), 1);
    }

    #[test]
    fn test_seg_queue_push_notifies_condition_var() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = QChild::new(1, packet_handle);
        let notif = Log::log("test".to_string(), "Main");

        // Act - push should notify (queue not paused)
        queue.push(notif);

        // pop to verify notification was queued
        let popped = queue.pop().expect("the pushed notification");

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
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::queues::priority_queue::*;
    use thryve_utils::res::TaskResult;
    use thryve_utils::traits::queue::Queue;

    #[test]
    fn test_priority_queue_new_creates_queue() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let config = Config::default();

        // Act
        let queue = PQChild::new(&config, packet_handle);

        // Assert
        assert!(!queue.exit_flag.load(Ordering::SeqCst));
        assert!(!queue.pause_flag.load(Ordering::SeqCst));
        assert_eq!(queue.task_config.max_priority, config.tasks.max_priority);
        assert_eq!(
            queue.task_config.aging_interval,
            config.tasks.aging_interval
        );
    }

    #[test]
    fn test_priority_queue_push_and_pop() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);
        let task = Task::new("testTask", 5, || {
            TaskResult::new("OK", "success".to_string(), "Main")
        })
        .unwrap();

        // Act - push task
        queue.push(task);

        // Act - pop task
        let popped = queue.pop().expect("the pushed task");

        // Assert
        assert_eq!(popped.task_name(), "testTask");
    }

    #[test]
    fn test_priority_queue_pop_returns_nothing_when_exit_flag_set() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Set exit flag
        queue.exit_flag.store(true, Ordering::SeqCst);

        // Act
        let task = queue.pop();

        // Assert - nothing, rather than a made-up Fin task
        assert!(task.is_none());
    }

    #[test]
    fn test_priority_queue_finalize_hands_back_what_is_left() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);
        for name in ["first", "second"] {
            let job = || TaskResult::new("OK", String::new(), "Main");
            queue.push(Task::new(name, 5, job).unwrap());
        }

        // Act
        let mut tasks = Vec::new();
        let mut status_reps = Vec::new();
        let left_over = queue.finalize(&mut tasks, &mut status_reps);

        // Assert - both are handed back and counted, plus one status line, and pop now stops
        assert_eq!(left_over, 2);
        assert_eq!(tasks.len(), 2);
        assert_eq!(status_reps.len(), 1);
        assert!(queue.exit_flag.load(Ordering::SeqCst));
        assert!(queue.pop().is_none());
    }

    #[test]
    fn test_priority_queue_pause_sets_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Assert initially not paused
        assert!(!queue.pause_flag.load(Ordering::SeqCst));

        // Act
        queue.pause(false);

        // Assert
        assert!(queue.pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_priority_queue_resume_clears_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Pause first
        queue.pause(false);
        assert!(queue.pause_flag.load(Ordering::SeqCst));

        // Act
        queue.resume(false);

        // Assert
        assert!(!queue.pause_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_priority_queue_finalize_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Assert initially not exiting
        assert!(!queue.exit_flag.load(Ordering::SeqCst));

        // Act
        queue.finalize(&mut Vec::new(), &mut Vec::new());

        // Assert
        assert!(queue.exit_flag.load(Ordering::SeqCst));
    }

    #[test]
    fn test_priority_queue_push_higher_priority_first() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let config = Config::default();
        let queue = PQChild::new(&config, packet_handle);

        // Push tasks with different priorities (lower number = higher priority)
        let task_low = Task::new("lowPriority", 1, || {
            TaskResult::new("OK", "low".to_string(), "Main")
        })
        .unwrap();
        let task_high = Task::new("highPriority", 10, || {
            TaskResult::new("OK", "high".to_string(), "Main")
        })
        .unwrap();

        // Act - push in reverse order
        queue.push(task_low);
        queue.push(task_high);

        // Act - pop should return higher priority first
        let first = queue.pop().expect("the pushed task");
        let second = queue.pop().expect("the pushed task");

        // Assert
        assert_eq!(first.task_name(), "highPriority");
        assert_eq!(second.task_name(), "lowPriority");
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
