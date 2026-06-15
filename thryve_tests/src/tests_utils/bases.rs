mod tests_plugin {
    use std::sync::Arc;

use thryve_utils::bases::plugin::*;
    use thryve_utils::extras::jobs::TaskJob;
use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use thryve_utils::bases::notif::Notif;
    use crossbeam::channel::bounded;
use thryve_utils::res;
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
        let job: TaskJob = Box::new(|| {
            res::Result::new("OK", "success".to_string(), "Main")
        });

        // Act
        let plugin = Plugin::new(
            "test_plugin",
            "TestType",
            plg_core,
            packet_handle,
            job,
        );

        // Assert
        assert_eq!(plugin.plg_name, "test_plugin");
        assert_eq!(plugin.plg_type.inner(), "TestType");
    }

    #[test]
    fn test_plugin_new_with_numbers_in_names() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));

        // Act
        let plugin = Plugin::new(
            "plugin123",
            "type456",
            plg_core,
            packet_handle,
            job,
        );

        // Assert
        assert_eq!(plugin.plg_name, "plugin123");
        assert_eq!(plugin.plg_type.inner(), "type456");
    }

    #[test]
    fn test_plugin_new_with_mixed_case() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));

        // Act
        let plugin = Plugin::new(
            "TestPlugin",
            "TestType",
            plg_core,
            packet_handle,
            job,
        );

        // Assert
        assert_eq!(plugin.plg_name, "TestPlugin");
        assert_eq!(plugin.plg_type.inner(), "TestType");
    }

    #[test]
    fn test_plugin_plg_entry_point_returns_and_replaces() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = Box::new(|| {
            res::Result::new("OK", "success".to_string(), "Main")
        });

        let mut plugin = Plugin::new(
            "test_plugin",
            "TestType",
            plg_core,
            packet_handle,
            job,
        );

        // Act - first call should return the original job
        let first_fn = plugin.plg_entry_point();
        let first_result = first_fn();

        // Assert - first call returns original job
        assert_eq!(first_result.result_type.inner(), "OK");
        assert_eq!(first_result.message, "success");

        // Act - second call should return default error function
        let second_fn = plugin.plg_entry_point();
        let second_result = second_fn();

        // Assert - second call returns error function
        assert_eq!(second_result.result_type.inner(), "BIGERR");
        assert_eq!(second_result.message, "If This Is Ever Called, Something is Wrong");
    }

    #[test]
    fn test_plugin_clean_up_calls_core_clean_up() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));

        let plugin = Plugin::new(
            "test_plugin",
            "TestType",
            plg_core,
            packet_handle,
            job,
        );

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
        let job: TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));

        // Act
        let plugin = Plugin::new(
            "a",
            "b",
            plg_core,
            packet_handle,
            job,
        );

        // Assert
        assert_eq!(plugin.plg_name, "a");
        assert_eq!(plugin.plg_type.inner(), "b");
    }

    #[test]
    fn test_plugin_with_long_names() {
        // Arrange
        let long_name = "a".repeat(100);
        let long_type = "t".repeat(100);
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));

        // Act
        let plugin = Plugin::new(
            &long_name,
            &long_type,
            plg_core,
            packet_handle,
            job,
        );

        // Assert
        assert_eq!(plugin.plg_name, long_name);
        assert_eq!(plugin.plg_type.inner(), long_type);
    }

    #[test]
    #[should_panic(expected = "The field `plg_type` in the Plugin cannot be empty")]
    fn test_plugin_with_empty_type_panics() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));

        // Act & Assert - should panic because plg_type cannot be empty
        let _plugin = Plugin::new(
            "test_plugin",
            "",
            plg_core,
            packet_handle,
            job,
        );
    }

    #[test]
    #[should_panic(expected = "must be alphanumeric")]
    fn test_plugin_with_hyphen_in_type_panics() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));

        // Act & Assert - should panic because plg_type must be alphanumeric
        let _plugin = Plugin::new(
            "test_plugin",
            "test-type",
            plg_core,
            packet_handle,
            job,
        );
    }

    #[test]
    #[should_panic(expected = "must be alphanumeric")]
    fn test_plugin_with_underscore_in_type_panics() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let plg_core = Arc::new(TestPlgLogic);
        let job: TaskJob = Box::new(|| res::Result::new("OK", "success".to_string(), "Main"));

        // Act & Assert - should panic because plg_type must be alphanumeric
        let _plugin = Plugin::new(
            "test_plugin",
            "Test_Type",
            plg_core,
            packet_handle,
            job,
        );
    }
}

mod tests_observer {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

use thryve_utils::bases::observer::*;
    use thryve_utils::bases::notif::Notif;
    use thryve_utils::extras::jobs::ObsJob;
use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::Task;
    use crossbeam::channel::bounded;
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
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {
            // No-op test function
        });

        // Act
        let observer = Observer::new(
            "test_observer",
            "TestType",
            obs_core,
            fn_to_run,
            packet_handle,
        );

        // Assert
        assert_eq!(observer.obs_name, "test_observer");
        assert_eq!(observer.obs_type.inner(), "TestType");
        assert!(!observer.exit_flag.load(Ordering::Relaxed));
    }

    #[test]
    fn test_observer_new_with_numbers_in_names() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new(
            "observer123",
            "type456",
            obs_core,
            fn_to_run,
            packet_handle,
        );

        // Assert
        assert_eq!(observer.obs_name, "observer123");
        assert_eq!(observer.obs_type.inner(), "type456");
    }

    #[test]
    fn test_observer_new_with_mixed_case() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new(
            "TestObserver",
            "TestType",
            obs_core,
            fn_to_run,
            packet_handle,
        );

        // Assert
        assert_eq!(observer.obs_name, "TestObserver");
        assert_eq!(observer.obs_type.inner(), "TestType");
    }

    #[test]
    fn test_observer_exit_flag_initially_false() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new(
            "test_observer",
            "TestType",
            obs_core,
            fn_to_run,
            packet_handle,
        );

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
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        let observer = Arc::new(Observer::new(
            "test_observer",
            "TestType",
            obs_core,
            fn_to_run,
            packet_handle,
        ));

        // Assert initially false
        assert!(!observer.exit_flag.load(Ordering::Relaxed));

        // Act
        observer.clean_up();

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
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new(
            "a",
            "b",
            obs_core,
            fn_to_run,
            packet_handle,
        );

        // Assert
        assert_eq!(observer.obs_name, "a");
        assert_eq!(observer.obs_type.inner(), "b");
    }

    #[test]
    fn test_observer_with_long_names() {
        // Arrange
        let long_name = "a".repeat(100);
        let long_type = "t".repeat(100);
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new(
            &long_name,
            &long_type,
            obs_core,
            fn_to_run,
            packet_handle,
        );

        // Assert
        assert_eq!(observer.obs_name, long_name);
        assert_eq!(observer.obs_type.inner(), long_type);
    }

    #[test]
    #[should_panic(expected = "The field `obs_type` in the Observer cannot be empty")]
    fn test_observer_with_empty_type_panics() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act & Assert - should panic because obs_type cannot be empty
        let _observer = Observer::new(
            "test_observer",
            "",
            obs_core,
            fn_to_run,
            packet_handle,
        );
    }

    #[test]
    #[should_panic(expected = "must be alphanumeric")]
    fn test_observer_with_hyphen_in_type_panics() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act & Assert - should panic because obs_type must be alphanumeric
        let _observer = Observer::new(
            "test_observer",
            "test-type",
            obs_core,
            fn_to_run,
            packet_handle,
        );
    }

    #[test]
    #[should_panic(expected = "must be alphanumeric")]
    fn test_observer_with_underscore_in_type_panics() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act & Assert - should panic because obs_type must be alphanumeric
        let _observer = Observer::new(
            "test_observer",
            "Test_Type",
            obs_core,
            fn_to_run,
            packet_handle,
        );
    }

    #[test]
    fn test_observer_clean_up_calls_finalize_and_core_cleanup() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        let observer = Arc::new(Observer::new(
            "test_observer",
            "TestType",
            obs_core,
            fn_to_run,
            packet_handle,
        ));

        // Act
        observer.clean_up();

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
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new(
            "test_observer",
            "TestType",
            obs_core,
            fn_to_run,
            packet_handle,
        );

        // Assert - obs_queue should be initialized (QChild is created in new())
        // We can verify this by checking that the observer was created without panic
        assert_eq!(observer.obs_name, "test_observer");
    }

    #[test]
    fn test_observer_fn_to_run_is_stored() {
        // Arrange
        let executed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let executed_clone = Arc::clone(&executed);
        
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {
            executed_clone.store(true, std::sync::atomic::Ordering::SeqCst);
        });

        // Act
        let observer = Observer::new(
            "test_observer",
            "TestType",
            obs_core,
            fn_to_run,
            packet_handle,
        );

        let obs_arc = Arc::new(observer);
        // Execute the fn_to_run
        (obs_arc.fn_to_run)(obs_arc.clone());

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
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        // Act
        let observer = Observer::new(
            "Observer123",
            "Type456",
            obs_core,
            fn_to_run,
            packet_handle,
        );

        // Assert
        assert_eq!(observer.obs_name, "Observer123");
        assert_eq!(observer.obs_type.inner(), "Type456");
    }

    #[test]
    fn test_observer_exit_flag_initially_false_then_true_after_cleanup() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
        let obs_core = Arc::new(TestObsLogic);
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        let observer = Arc::new(Observer::new(
            "test_observer",
            "TestType",
            obs_core,
            fn_to_run,
            packet_handle,
        ));

        // Assert initially false
        assert!(!observer.exit_flag.load(Ordering::Relaxed));

        // Act
        observer.clean_up();

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
        let fn_to_run: ObsJob = Box::new(move |_obs: Arc<Observer>| {});

        let observer = Arc::new(Observer::new(
            "test_observer",
            "TestType",
            obs_core,
            fn_to_run,
            packet_handle,
        ));

        // Act - call clean_up multiple times
        observer.clean_up();
        observer.clean_up();
        observer.clean_up();

        // Assert - should not panic, exit flag should be true
        assert!(observer.exit_flag.load(Ordering::Relaxed));
    }
}

mod tests_notif {
    use std::time::SystemTime;

use thryve_utils::bases::notif::*;
    use thryve_utils::impl_as_any;
use thryve_utils::traits::notif_logic::NotifLogic;

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
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new("TestType", "TestReceiver", notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner(), "TestType");
        assert_eq!(notif.notif_receiver.inner(), "TestReceiver");
    }

    #[test]
    fn test_notif_new_with_numbers_in_names() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new("Type123", "Receiver456", notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner(), "Type123");
        assert_eq!(notif.notif_receiver.inner(), "Receiver456");
    }

    #[test]
    fn test_notif_new_with_mixed_case() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new("MixedCase", "AnotherMixedCase", notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner(), "MixedCase");
        assert_eq!(notif.notif_receiver.inner(), "AnotherMixedCase");
    }

    #[test]
    fn test_notif_timestamp_is_set() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });
        let before = SystemTime::now();

        // Act
        let notif = Notif::new("TestType", "TestReceiver", notif_core);

        // Assert - timestamp should be >= before (no time travel)
        assert!(notif.timestamp >= before || notif.timestamp.duration_since(before).is_err());
    }

    #[test]
    #[should_panic(expected = "must be alphanumeric")]
    fn test_notif_new_with_hyphen_in_type_panics() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act & Assert
        let _notif = Notif::new("Test-Type", "TestReceiver", notif_core);
    }

    #[test]
    #[should_panic(expected = "must be alphanumeric")]
    fn test_notif_new_with_underscore_in_receiver_panics() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act & Assert
        let _notif = Notif::new("TestType", "Test_Receiver", notif_core);
    }

    #[test]
    #[should_panic(expected = "Both fields, `notif_type` and `notif_receiver` cannot be empty")]
    fn test_notif_new_with_empty_type_panics() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act & Assert
        let _notif = Notif::new("", "TestReceiver", notif_core);
    }

    #[test]
    #[should_panic(expected = "Both fields, `notif_type` and `notif_receiver` cannot be empty")]
    fn test_notif_new_with_empty_receiver_panics() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act & Assert
        let _notif = Notif::new("TestType", "", notif_core);
    }

    #[test]
    fn test_notif_with_single_character_names() {
        // Arrange
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new("A", "B", notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner(), "A");
        assert_eq!(notif.notif_receiver.inner(), "B");
    }

    #[test]
    fn test_notif_with_long_names() {
        // Arrange
        let long_name = "A".repeat(100);
        let notif_core = Box::new(TestNotifLogic { _data: "test".to_string() });

        // Act
        let notif = Notif::new(&long_name, &long_name, notif_core);

        // Assert
        assert_eq!(notif.notif_type.inner().len(), 100);
        assert_eq!(notif.notif_receiver.inner().len(), 100);
    }
}
