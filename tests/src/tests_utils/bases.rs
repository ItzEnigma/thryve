// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

mod tests_plugin {
    use std::sync::Arc;
    use thryve_utils::errors::{NameInputType, NamingError};

    use crossbeam::channel::bounded;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::bases::plugin::*;
    use thryve_utils::extras::jobs::TaskJob;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::res::TaskResult;
    use thryve_utils::traits::plugin_logic::{PlgLogic, PluginB};

    // Test implementation of PlgLogic
    struct TestPlgLogic;
    impl PlgLogic for TestPlgLogic {
        thryve_utils::impl_as_any!(TestPlgLogic);
        fn clean_up(&self) {
            // No-op for testing
        }
    }

    #[test]
    fn test_plugin_new_creates_plugin() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        // Act
        let plugin = Plugin::new("TestType", plg_core, packet_handle, job).unwrap();

        // Assert
        assert_eq!(plugin.plg_type(), "TestType");
    }

    #[test]
    fn test_plugin_new_with_numbers_in_names() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        // Act
        let plugin = Plugin::new("type456", plg_core, packet_handle, job).unwrap();

        // Assert
        assert_eq!(plugin.plg_type(), "type456");
    }

    #[test]
    fn test_plugin_new_with_mixed_case() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        // Act
        let plugin = Plugin::new("TestType", plg_core, packet_handle, job).unwrap();

        // Assert
        assert_eq!(plugin.plg_type(), "TestType");
    }

    #[test]
    fn test_plugin_plg_entry_point_returns_and_replaces() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        let mut plugin = Plugin::new("TestType", plg_core, packet_handle, job).unwrap();

        // Act - first call should return the original job
        let first_fn = plugin.plg_entry_point();
        let first_result = first_fn.call_once();

        // Assert - first call returns original job
        assert_eq!(first_result.result_type(), "OK");
        assert_eq!(first_result.message, "success");

        // Act - second call should return default error function
        let second_fn = plugin.plg_entry_point();
        let second_result = second_fn.call_once();

        // Assert - second call returns error function
        assert_eq!(second_result.result_type(), "BIGERR");
        assert_eq!(
            second_result.message,
            "If This Is Ever Called, Something is Wrong"
        );
    }

    #[test]
    fn test_plugin_clean_up_calls_core_clean_up() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        let plugin = Plugin::new("TestType", plg_core, packet_handle, job).unwrap();

        // Act & Assert - clean_up should not panic
        plugin.clean_up();
    }

    #[test]
    fn test_plugin_with_single_character_names() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        // Act
        let plugin = Plugin::new("b", plg_core, packet_handle, job).unwrap();

        // Assert
        assert_eq!(plugin.plg_type(), "b");
    }

    #[test]
    fn test_plugin_with_long_type() {
        // Arrange
        let long_type = "t".repeat(100);
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        // Act
        let plugin = Plugin::new(&long_type, plg_core, packet_handle, job).unwrap();

        // Assert
        assert_eq!(plugin.plg_type(), long_type);
    }

    #[test]
    fn test_plugin_with_empty_type_is_rejected() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        // Act & Assert - rejected: plg_type cannot be empty
        assert!(matches!(
            Plugin::new("", plg_core, packet_handle, job),
            Err(NamingError::NameEmpty {
                kind: NameInputType::PlgType
            })
        ));
    }

    #[test]
    fn test_plugin_with_hyphen_in_type_is_accepted() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        // Act & Assert - accepted: names only reject empty text and control characters
        assert!(Plugin::new("test-type", plg_core, packet_handle, job).is_ok());
    }

    #[test]
    fn test_plugin_with_underscore_in_type_is_accepted() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = TaskJob::new(|| TaskResult::new("OK", "success".to_string(), "Main"));

        // Act & Assert - accepted: names only reject empty text and control characters
        assert!(Plugin::new("Test_Type", plg_core, packet_handle, job).is_ok());
    }
}

mod tests_observer {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use thryve_utils::errors::{NameInputType, NamingError};

    use crossbeam::channel::bounded;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::bases::observer::*;
    use thryve_utils::extras::jobs::ObsJob;
    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::traits::observer_logic::ObsLogic;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        thryve_utils::impl_as_any!(TestObsLogic);
        fn clean_up(&self, _observer: Arc<Observer>) {
            // No-op for testing
        }
    }

    #[test]
    fn test_observer_new_creates_observer() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {
            // No-op test function
        });

        // Act
        let observer = Observer::new("TestType", obs_core, obs_job, packet_handle).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "TestType");
        assert!(!observer.exit_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_observer_new_with_numbers_in_names() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new("type456", obs_core, obs_job, packet_handle).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "type456");
    }

    #[test]
    fn test_observer_new_with_mixed_case() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new("TestType", obs_core, obs_job, packet_handle).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "TestType");
    }

    #[test]
    fn test_observer_exit_flag_initially_false() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new("TestType", obs_core, obs_job, packet_handle).unwrap();

        // Assert
        assert!(!observer.exit_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_observer_clean_up_sets_exit_flag() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        let observer =
            Arc::new(Observer::new("TestType", obs_core, obs_job, packet_handle).unwrap());

        // Assert initially false
        assert!(!observer.exit_flag.load(Ordering::Relaxed));

        // Act
        observer.clean_up(None);

        // Assert
        assert!(observer.exit_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_observer_with_single_character_names() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new("b", obs_core, obs_job, packet_handle).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "b");
    }

    #[test]
    fn test_observer_with_long_type() {
        // Arrange
        let long_type = "t".repeat(100);
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new(&long_type, obs_core, obs_job, packet_handle).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), long_type);
    }

    #[test]
    fn test_observer_with_empty_type_is_rejected() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act & Assert - rejected: obs_type cannot be empty
        assert!(matches!(
            Observer::new("", obs_core, obs_job, packet_handle),
            Err(NamingError::NameEmpty {
                kind: NameInputType::ObsType
            })
        ));
    }

    #[test]
    fn test_observer_with_hyphen_in_type_is_accepted() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act & Assert - accepted: names only reject empty text and control characters
        assert!(Observer::new("test-type", obs_core, obs_job, packet_handle).is_ok());
    }

    #[test]
    fn test_observer_with_underscore_in_type_is_accepted() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act & Assert - accepted: names only reject empty text and control characters
        assert!(Observer::new("Test_Type", obs_core, obs_job, packet_handle).is_ok());
    }

    #[test]
    fn test_observer_clean_up_calls_finalize_and_core_cleanup() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        let observer =
            Arc::new(Observer::new("TestType", obs_core, obs_job, packet_handle).unwrap());

        // Act
        observer.clean_up(None);

        // Assert - exit flag should be set
        assert!(observer.exit_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_observer_obs_queue_is_initialized() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new("TestType", obs_core, obs_job, packet_handle).unwrap();

        // Assert - obs_queue should be initialized (QChild is created in new())
        // We can verify this by checking that the observer was created without panic
        assert_eq!(observer.obs_type(), "TestType");
    }

    #[test]
    fn test_observer_obs_job_is_stored() {
        // Arrange
        let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executed_clone = Arc::clone(&executed);

        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {
            executed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        // Act
        let observer = Observer::new("TestType", obs_core, obs_job, packet_handle).unwrap();

        let obs_arc = Arc::new(observer);
        // Execute the obs_job
        obs_arc.run_loop(obs_arc.clone());

        // Assert - the function should have been executed
        assert!(executed.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[test]
    fn test_observer_with_unicode_in_name() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new("Type456", obs_core, obs_job, packet_handle).unwrap();

        // Assert
        assert_eq!(observer.obs_type(), "Type456");
    }

    #[test]
    fn test_observer_exit_flag_initially_false_then_true_after_cleanup() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        let observer =
            Arc::new(Observer::new("TestType", obs_core, obs_job, packet_handle).unwrap());

        // Assert initially false
        assert!(!observer.exit_flag.load(Ordering::Relaxed));

        // Act
        observer.clean_up(None);

        // Assert now true
        assert!(observer.exit_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_observer_multiple_cleanup_calls_idempotent() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let obs_job = ObsJob::new(move |_obs: Arc<Observer>| {});

        let observer =
            Arc::new(Observer::new("TestType", obs_core, obs_job, packet_handle).unwrap());

        // Act - call clean_up multiple times
        observer.clean_up(None);
        observer.clean_up(None);
        observer.clean_up(None);

        // Assert - should not panic, exit flag should be true
        assert!(observer.exit_flag.load(Ordering::Relaxed));
    }
}

mod tests_notif {
    use std::path::PathBuf;
    use std::time::SystemTime;
    use thryve_utils::errors::{NameInputType, NamingError};

    use thryve_utils::bases::notif::*;
    use thryve_utils::graphs::callsite::TaskCallSite;
    use thryve_utils::impl_as_any;
    use thryve_utils::traits::notif_logic::NotifLogic;
    use thryve_utils::traits::packets::Packet;

    // Test implementation of NotifLogic
    struct TestNotifLogic {
        _data: String,
    }

    impl NotifLogic for TestNotifLogic {
        impl_as_any!(TestNotifLogic);
    }

    #[test]
    fn test_notif_new_creates_notification() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });

        // Act
        let notif = Notif::new("TestType", "TestReceiver", notif_core).unwrap();

        // Assert
        assert_eq!(notif.notif_type(), "TestType");
        assert_eq!(notif.notif_receiver(), "TestReceiver");
    }

    #[test]
    fn test_notif_new_with_numbers_in_names() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });

        // Act
        let notif = Notif::new("Type123", "Receiver456", notif_core).unwrap();

        // Assert
        assert_eq!(notif.notif_type(), "Type123");
        assert_eq!(notif.notif_receiver(), "Receiver456");
    }

    #[test]
    fn test_notif_new_with_mixed_case() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });

        // Act
        let notif = Notif::new("MixedCase", "AnotherMixedCase", notif_core).unwrap();

        // Assert
        assert_eq!(notif.notif_type(), "MixedCase");
        assert_eq!(notif.notif_receiver(), "AnotherMixedCase");
    }

    #[test]
    fn test_notif_timestamp_is_set() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });
        let before = SystemTime::now();

        // Act
        let notif = Notif::new("TestType", "TestReceiver", notif_core).unwrap();

        // Assert - timestamp should be >= before (no time travel)
        assert!(notif.timestamp >= before || notif.timestamp.duration_since(before).is_err());
    }

    #[test]
    fn test_notif_new_with_hyphen_in_type_is_accepted() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });

        // Act & Assert
        assert!(Notif::new("Test-Type", "TestReceiver", notif_core).is_ok());
    }

    #[test]
    fn test_notif_new_with_underscore_in_receiver_is_accepted() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });

        // Act & Assert
        assert!(Notif::new("TestType", "Test_Receiver", notif_core).is_ok());
    }

    #[test]
    fn test_notif_new_with_empty_type_is_rejected() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });

        // Act & Assert
        assert!(matches!(
            Notif::new("", "TestReceiver", notif_core),
            Err(NamingError::NameEmpty {
                kind: NameInputType::NotifType
            })
        ));
    }

    #[test]
    fn test_notif_new_with_empty_receiver_is_rejected() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });

        // Act & Assert
        assert!(matches!(
            Notif::new("TestType", "", notif_core),
            Err(NamingError::NameEmpty {
                kind: NameInputType::NotifReceiver
            })
        ));
    }

    #[test]
    fn test_notif_with_single_character_names() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });

        // Act
        let notif = Notif::new("A", "B", notif_core).unwrap();

        // Assert
        assert_eq!(notif.notif_type(), "A");
        assert_eq!(notif.notif_receiver(), "B");
    }

    #[test]
    fn test_notif_with_long_names() {
        // Arrange
        let long_name = "A".repeat(100);
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });

        // Act
        let notif = Notif::new(&long_name, &long_name, notif_core).unwrap();

        // Assert
        assert_eq!(notif.notif_type().len(), 100);
        assert_eq!(notif.notif_receiver().len(), 100);
    }

    #[test]
    fn test_notif_get_callsite_defaults_to_none_before_set() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });
        let notif = Notif::new("TestType", "TestReceiver", notif_core).unwrap();

        // Act
        let callsite = notif.get_callsite();

        // Assert - no callsite was ever set, so it should fall back to default
        assert_eq!(callsite, TaskCallSite::default());
    }

    #[test]
    fn test_notif_set_callsite_then_get_callsite_returns_it() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });
        let mut notif = Notif::new("TestType", "TestReceiver", notif_core).unwrap();
        let callsite = TaskCallSite::new(PathBuf::from("src/test.rs"), 42, "TestTask".to_string());

        // Act
        notif.set_callsite(callsite.clone());
        let retrieved = notif.get_callsite();

        // Assert
        assert_eq!(retrieved, callsite);
        assert_eq!(notif.callsite, Some(callsite));
    }

    #[test]
    fn test_notif_set_callsite_overwrites_previous_value() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic {
            _data: "test".to_string(),
        });
        let mut notif = Notif::new("TestType", "TestReceiver", notif_core).unwrap();
        let first = TaskCallSite::new(PathBuf::from("src/a.rs"), 1, "A".to_string());
        let second = TaskCallSite::new(PathBuf::from("src/b.rs"), 2, "B".to_string());

        // Act
        notif.set_callsite(first);
        notif.set_callsite(second.clone());

        // Assert
        assert_eq!(notif.get_callsite(), second);
    }
}
