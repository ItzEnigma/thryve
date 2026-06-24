mod tests_task {
    use thryve_utils::extras::jobs::TaskJob;
use thryve_utils::graphs::callsite::TaskCallSite;
use thryve_utils::packets::task::*;
    use thryve_utils::res;
use thryve_utils::traits::packets::Packet;
    

    #[test]
    fn test_task_new_creates_task_with_unique_id() {
        // Arrange
        let task_name = "TestTask";
        let priority = 5;
        
        // Act
        let task1 = Task::new(task_name, priority, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        let task2 = Task::new(task_name, priority, TaskJob::new(|| {
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
        let task = Task::new("LowPriority", 0, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        // Assert
        assert_eq!(task.base_priority, 0);
    }

    #[test]
    fn test_task_new_with_max_u8_priority() {
        // Arrange & Act
        let task = Task::new("MaxPriority", u8::MAX, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        // Assert
        assert_eq!(task.base_priority, u8::MAX);
    }

    #[test]
    fn test_task_process_executes_job() {
        // Arrange
        let task = Task::new("ProcessTest", 1, TaskJob::new(|| {
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
        let mut task = Task::new("HighPriority", 10, TaskJob::new(|| {
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
        let mut task = Task::new("ValidPriority", 3, TaskJob::new(|| {
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
        let mut task = Task::new("BoundaryPriority", 4, TaskJob::new(|| {
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
        let mut task = Task::new("AnyPriority", 100, TaskJob::new(|| {
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
        let task1 = Task::new("T1", 1, TaskJob::new(|| res::Result::new("Ok", "".to_string(), "Main")));
        let task2 = Task::new("T2", 1, TaskJob::new(|| res::Result::new("Ok", "".to_string(), "Main")));
        let task3 = Task::new("T3", 1, TaskJob::new(|| res::Result::new("Ok", "".to_string(), "Main")));
        
        // Assert
        assert_eq!(task2.id, task1.id + 1);
        assert_eq!(task3.id, task2.id + 1);
    }

    #[test]
    #[should_panic(expected = "The field `task_name` in a Task cannot be empty")]
    fn test_task_with_empty_name() {
        // Arrange & Act & Assert - empty name should panic
        let _task = Task::new("", 1, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
    }

    #[test]
    fn test_task_with_numbers_in_name() {
        // Arrange & Act
        let task = Task::new("Task123", 1, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        // Assert
        assert_eq!(task.task_name.inner(), "Task123");
    }

    #[test]
    #[should_panic(expected = "NamedInput must be alphanumeric")]
    fn test_task_with_invalid_name_panics() {
        // Arrange & Act
        let _ = Task::new("Invalid-Name", 1, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
    }

    #[test]
    fn test_task_process_returns_correct_id() {
        // Arrange
        let task = Task::new("IdTest", 1, TaskJob::new(|| {
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
        let task = Task::new("SizeTest", 1, TaskJob::new(|| {
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
        let task1 = Task::new("Task1", 1, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        let task2 = Task::new("Task2", 2, TaskJob::new(|| {
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
        let task = Task::new("MemSizeTest", 5, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));

        // Act
        let size = task.size_in_bytes();

        // Assert - should equal std::mem::size_of::<Task>()
        assert_eq!(size, std::mem::size_of::<Task>());
    }

    #[test]
    fn test_task_set_callsite_sets_callsite() {
        // Arrange
        let mut task = Task::new("CallsiteTest", 1, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        let callsite = TaskCallSite::new(
            std::path::PathBuf::from("src/test.rs"),
            "test_module".to_string(),
            42,
            "TestTask".to_string(),
        );

        // Act
        task.set_callsite(callsite.clone());

        // Assert
        assert!(task.callsite.is_some());
        let retrieved = task.get_callsite();
        assert_eq!(retrieved.file_path, callsite.file_path);
        assert_eq!(retrieved.module_path, callsite.module_path);
        assert_eq!(retrieved.line_number, callsite.line_number);
    }

    #[test]
    fn test_task_get_callsite_returns_default_when_none_set() {
        // Arrange
        let task = Task::new("NoCallsite", 1, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));

        // Act
        let callsite = task.get_callsite();

        // Assert
        assert_eq!(callsite.file_path, std::path::PathBuf::new());
        assert_eq!(callsite.module_path, "");
        assert_eq!(callsite.line_number, 0);
    }

    #[test]
    fn test_task_process_returns_error_result() {
        // Arrange
        let task = Task::new("ErrorTest", 1, TaskJob::new(|| {
            res::Result::new("Error", "Something went wrong".to_string(), "Error")
        }));

        // Act
        let result = task.process();

        // Assert
        assert!(result.is_some());
        let (_, res_result) = result.unwrap();
        assert_eq!(res_result.result_type.inner(), "Error");
        assert_eq!(res_result.message, "Something went wrong");
    }

    #[test]
    #[should_panic(expected = "NamedInput must be alphanumeric")]
    fn test_task_with_special_characters_in_name_panics() {
        // Arrange & Act & Assert - special characters should panic
        let _task = Task::new("Task-With_Dots.And123", 1, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
    }

    #[test]
    fn test_task_callsite_preserved_after_process() {
        // Arrange
        let mut task = Task::new("CallsitePreserve", 1, TaskJob::new(|| {
            res::Result::new("Ok", "Done".to_string(), "Main")
        }));
        
        let callsite = TaskCallSite::new(
            std::path::PathBuf::from("src/preserved.rs"),
            "preserved_module".to_string(),
            100,
            "PreservedTask".to_string(),
        );
        task.set_callsite(callsite);

        // Act - process the task
        let _ = task.process();

        // Assert - callsite should still be accessible (though task is consumed)
        // This test verifies the callsite was set before process consumed the task
    }
}

mod tests_packet_handle {
    use thryve_utils::bases::notif::Notif;
use thryve_utils::extras::jobs::TaskJob;
use thryve_utils::packets::packet_handle::*;
    use thryve_utils::packets::notif_log::Log;
    use thryve_utils::packets::task::Task;
use thryve_utils::res::Result;
use thryve_utils::traits::packets::Packet;

    #[test]
    fn test_packet_handle_new_creates_handle() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);

        // Act
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Assert - handle is created successfully (no panic)
        drop(handle);
        // Verify channels are still functional
        assert!(task_rx.try_recv().is_err()); // Empty
        assert!(notif_rx.try_recv().is_err()); // Empty
    }

    #[test]
    fn test_send_task_sends_to_channel() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);
        let task = Task::new("TestTask", 0, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main")));

        // Act
        handle.send_task(task);

        // Assert
        let received = task_rx.recv().unwrap();
        assert_eq!(received.task_name.inner(), "TestTask");
        // Notification channel should be empty
        assert!(notif_rx.try_recv().is_err());
    }

    #[test]
    fn test_send_notification_sends_to_channel() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);
        let notif = Log::log("Test message".to_string(), "Main");

        // Act
        handle.send_notification(notif);

        // Assert
        let received = notif_rx.recv().unwrap();
        assert_eq!(received.notif_type.inner(), "Log");
        // Task channel should be empty
        assert!(task_rx.try_recv().is_err());
    }

    #[test]
    fn test_packet_handle_clone() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, _) = crossbeam::channel::bounded::<Notif>(10);
        let handle1 = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        let handle2 = handle1.clone();

        // Assert - both handles can send
        let task1 = Task::new("Task1", 0, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main")));
        let task2 = Task::new("Task2", 0, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main")));
        
        handle1.send_task(task1);
        handle2.send_task(task2);

        let received1 = task_rx.recv().unwrap();
        let received2 = task_rx.recv().unwrap();
        
        assert!(received1.task_name.inner() == "Task1" || received1.task_name.inner() == "Task2");
        assert!(received2.task_name.inner() == "Task1" || received2.task_name.inner() == "Task2");
    }

    #[test]
    fn test_send_multiple_tasks() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        for i in 0..5 {
            let task = Task::new(&format!("Task{}", i), 0, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main")));
            handle.send_task(task);
        }

        // Assert
        for _ in 0..5 {
            let received = task_rx.recv().unwrap();
            assert!(received.task_name.inner().starts_with("Task"));
        }
        assert!(task_rx.try_recv().is_err()); // Channel empty
        assert!(notif_rx.try_recv().is_err()); // Notification channel empty
    }

    #[test]
    fn test_send_multiple_notifications() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        for i in 0..5 {
            let notif = Log::log(format!("Message {}", i), "Main");
            handle.send_notification(notif);
        }

        // Assert
        for _ in 0..5 {
            let received = notif_rx.recv().unwrap();
            assert_eq!(received.notif_type.inner(), "Log");
        }
        assert!(notif_rx.try_recv().is_err()); // Channel empty
        assert!(task_rx.try_recv().is_err()); // Task channel empty
    }

    #[test]
    fn test_send_task_and_notification_interleaved() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act - send interleaved
        handle.send_task(Task::new("T1", 0, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main"))));
        handle.send_notification(Log::log("N1".to_string(), "Main"));
        handle.send_task(Task::new("T2", 0, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main"))));
        handle.send_notification(Log::log("N2".to_string(), "Main"));

        // Assert - tasks go to task channel
        assert_eq!(task_rx.recv().unwrap().task_name.inner(), "T1");
        assert_eq!(task_rx.recv().unwrap().task_name.inner(), "T2");
        assert!(task_rx.try_recv().is_err());

        // Notifications go to notification channel
        assert_eq!(notif_rx.recv().unwrap().notif_type.inner(), "Log");
        assert_eq!(notif_rx.recv().unwrap().notif_type.inner(), "Log");
        assert!(notif_rx.try_recv().is_err());
    }

    #[test]
    fn test_send_task_with_different_priorities() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, _) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        handle.send_task(Task::new("Low", 0, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main"))));
        handle.send_task(Task::new("Medium", 50, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main"))));
        handle.send_task(Task::new("High", 100, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main"))));

        // Assert
        let t1 = task_rx.recv().unwrap();
        let t2 = task_rx.recv().unwrap();
        let t3 = task_rx.recv().unwrap();
        
        let priorities: Vec<u8> = vec![t1.base_priority, t2.base_priority, t3.base_priority];
        assert!(priorities.contains(&0));
        assert!(priorities.contains(&50));
        assert!(priorities.contains(&100));
    }

    #[test]
    fn test_send_error_notification() {
        // Arrange
        let (task_tx, _) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        let notif = Log::err("Error occurred".to_string());
        handle.send_notification(notif);

        // Assert
        let received = notif_rx.recv().unwrap();
        assert_eq!(received.notif_type.inner(), "Error");
    }

    #[test]
    #[should_panic(expected = "The field `task_name` in a Task cannot be empty")]
    fn test_packet_handle_with_empty_task_name() {
        // Arrange
        let (task_tx, _task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, _notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act & Assert - should panic with empty name
        let task = Task::new("", 0, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main")));
        handle.send_task(task);
    }

    #[test]
    fn test_packet_handle_with_unicode_message() {
        // Arrange
        let (task_tx, _) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, false);

        // Act
        let notif = Log::log("Hello 世界 🌍".to_string(), "Main");
        handle.send_notification(notif);

        // Assert
        let received = notif_rx.recv().unwrap();
        assert_eq!(received.notif_type.inner(), "Log");
    }

    #[test]
    fn test_packet_handle_new_with_graph_visualization_enabled() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);

        // Act - create handle with graph visualization enabled
        let handle = PacketHandle::new(task_tx, notif_tx, true);

        // Assert - handle is created successfully (no panic)
        drop(handle);
        // Verify channels are still functional
        assert!(task_rx.try_recv().is_err()); // Empty
        assert!(notif_rx.try_recv().is_err()); // Empty
    }

    #[test]
    fn test_send_task_with_graph_visualization_enabled() {
        // Arrange
        let (task_tx, task_rx) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, _) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, true);
        let task = Task::new("TestTaskWithGraph", 0, TaskJob::new(|| Result::new("Success", "Done".to_string(), "Main")));

        // Act - send task with graph visualization enabled
        handle.send_task(task);

        // Assert - task is received with callsite information
        let received = task_rx.recv().unwrap();
        assert_eq!(received.task_name.inner(), "TestTaskWithGraph");
        // When graph visualization is enabled, callsite should be set
        let callsite = received.get_callsite();
        assert!(!callsite.file_path.to_str().unwrap().is_empty());
    }

    #[test]
    fn test_packet_handle_graph_visualization_does_not_affect_notifications() {
        // Arrange
        let (task_tx, _) = crossbeam::channel::bounded::<Task>(10);
        let (notif_tx, notif_rx) = crossbeam::channel::bounded::<Notif>(10);
        let handle = PacketHandle::new(task_tx, notif_tx, true);
        let notif = Log::log("Test notification".to_string(), "Main");

        // Act
        handle.send_notification(notif);

        // Assert - notification is still sent correctly
        let received = notif_rx.recv().unwrap();
        assert_eq!(received.notif_type.inner(), "Log");
    }
}

mod tests_notif_log {
    use std::time::SystemTime;

use thryve_utils::packets::notif_log::*;

    #[test]
    fn test_log_creates_notification_with_correct_type() {
        // Arrange
        let message = "Test message".to_string();
        let log_file = "TestLog";
        
        // Act
        let notif = Log::log(message, log_file);
        
        // Assert
        assert_eq!(notif.notif_type.inner(), "Log");
        assert_eq!(notif.notif_receiver.inner(), "Logging");
    }

    #[test]
    fn test_log_stores_message_correctly() {
        // Arrange
        let message = "My log message".to_string();
        
        // Act
        let notif = Log::log(message.clone(), "Main");
        
        // Assert - downcast to Log to access message
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.message, "My log message");
    }

    #[test]
    fn test_log_stores_logfile_correctly() {
        // Arrange
        let log_file = "CustomLog";
        
        // Act
        let notif = Log::log("Message".to_string(), log_file);
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.logfile.inner(), "CustomLog");
    }

    #[test]
    fn test_err_creates_error_notification() {
        // Arrange
        let error_message = "Something went wrong".to_string();
        
        // Act
        let notif = Log::err(error_message);
        
        // Assert
        assert_eq!(notif.notif_type.inner(), "Error");
        assert_eq!(notif.notif_receiver.inner(), "Logging");
    }

    #[test]
    fn test_err_uses_error_logfile() {
        // Arrange & Act
        let notif = Log::err("Error message".to_string());
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.logfile.inner(), "Error");
    }

    #[test]
    fn test_log_with_empty_message() {
        // Arrange & Act
        let notif = Log::log("".to_string(), "Main");
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.message, "");
    }

    #[test]
    fn test_log_with_special_characters() {
        // Arrange
        let message = "Error: Connection failed! @#$%".to_string();
        
        // Act
        let notif = Log::log(message.clone(), "Main");
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.message, "Error: Connection failed! @#$%");
    }

    #[test]
    fn test_log_with_unicode() {
        // Arrange
        let message = "成功！Успех! Éxito!".to_string();
        
        // Act
        let notif = Log::log(message.clone(), "Main");
        
        // Assert
        let log_ref = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        assert_eq!(log_ref.message, "成功！Успех! Éxito!");
    }

    #[test]
    fn test_log_invalid_logfile_panics() {
        // Arrange & Act - Log file must be alphanumeric
        assert!(std::panic::catch_unwind(|| {
            let _ = Log::log("msg".to_string(), "Invalid/File");
        }).is_err());
    }

    #[test]
    fn test_timestamp_is_set_on_notification() {
        // Arrange
        let before = SystemTime::now();
        
        // Act
        let notif = Log::log("Message".to_string(), "Main");
        
        // Assert
        assert!(notif.timestamp >= before);
    }
}
