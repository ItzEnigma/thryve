//! Core observer structure for the Thryve observer system.
//!
//! This module provides the [`Observer`] struct, which encapsulates all state and
//! behavior required for an observer to participate in the notification processing
//! pipeline. Observers are long-running components that continuously monitor and
//! respond to notifications from various parts of the system.
//!
//! # Architecture
//!
//! Each observer maintains:
//! - A unique name and type identifier for routing and debugging
//! - A dedicated notification queue for receiving messages
//! - Core logic implementing the [`ObsLogic`] trait for custom behavior
//! - An exit flag for graceful shutdown coordination
//! - A packet handle for communicating with the engine
//!
//! # Lifecycle
//!
//! Observers are initialized with a function closure that typically runs a loop,
//! processing notifications until the exit flag is set. When shutdown is requested,
//! the exit flag signals the loop to terminate and resources are cleaned up via
//! the [`clean_up`](Observer::clean_up) method.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::extras::{named_input::NamedInput, jobs::ObsJob};
use crate::packets::packet_handle::PacketHandle;
use crate::queues::seg_queue::QChild;
use crate::traits::observer_logic::{ObsB, ObsLogic};
use crate::traits::queue::Queue;

/// Core observer structure for the Thryve observer system.
///
/// The `Observer` struct encapsulates all state and behavior required for an
/// observer to participate in the notification processing pipeline. Observers
/// are long-running components that continuously monitor and respond to
/// notifications from various parts of the system.
///
/// # Fields
///
/// - `obs_name`: Unique identifier for the observer
/// - `obs_type`: Type classifier for routing notifications
/// - `obs_queue`: Dedicated queue for receiving notifications
/// - `obs_core`: Core logic implementing custom observer behavior
/// - `exit_flag`: Atomic flag for graceful shutdown coordination
/// - `tx`: Packet handle for communicating with the engine
/// - `fn_to_run`: Initialization closure that starts the observer's main loop
///
/// # Example
///
/// ```no_run
/// // Observers are typically created by implementing ObsLogic and
/// // passing the core logic to Observer::new along with a run function
/// ```
pub struct Observer {
    /// Unique identifier for the observer.
    pub obs_name: String,
    /// Type classifier for routing notifications to the correct observer.
    pub obs_type: NamedInput,
    /// Dedicated queue for receiving notifications.
    pub obs_queue: QChild,
    /// Core logic implementing custom observer behavior.
    pub obs_core: Arc<dyn ObsLogic>,
    /// Atomic flag for graceful shutdown coordination.
    pub exit_flag: AtomicBool,
    /// Packet handle for communicating with the engine.
    pub tx: PacketHandle,
    /// Initialization closure that starts the observer's main loop.
    pub fn_to_run: ObsJob,
}

impl ObsB for Observer {}

impl Observer {
    /// Creates a new observer with the specified configuration.
    ///
    /// Initializes a new observer instance with the given name, type, core logic,
    /// run function, and packet handle. The observer starts with its exit flag
    /// set to `false`, indicating it is ready to process notifications.
    ///
    /// # Arguments
    ///
    /// * `obs_name` - Unique identifier for the observer
    /// * `obs_type` - Type classifier for routing notifications
    /// * `obs_core` - Core logic implementing custom observer behavior
    /// * `func` - Closure that runs the observer's main processing loop
    /// * `tx` - Packet handle for communicating with the engine
    ///
    /// # Returns
    ///
    /// A new `Observer` instance configured with the provided parameters.
    pub fn new(
        obs_name: &str,
        obs_type: &str,
        obs_core: Arc<dyn ObsLogic>,
        func: ObsJob,
        tx: PacketHandle,
    ) -> Self {
        Self {
            obs_name: obs_name.to_string(),
            obs_type: NamedInput::new(obs_type),
            obs_queue: QChild::new(tx.clone()),
            obs_core,
            exit_flag: AtomicBool::new(false),
            tx: tx,
            fn_to_run: func,
        }
    }

    /// Gracefully shuts down the observer.
    ///
    /// Sets the exit flag to signal the observer's main loop to terminate,
    /// invokes the core logic's cleanup method, and finalizes the notification
    /// queue to ensure all pending notifications are processed.
    ///
    /// This method should be called during engine shutdown to ensure proper
    /// resource cleanup and orderly termination of the observer.
    pub fn clean_up(self: &Arc<Self>) {
        self.exit_flag.store(true, Ordering::SeqCst);
        self.obs_queue.finalize(Some(self.clone()));
        self.obs_core.clean_up(self.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bases::notif::Notif;
    use crate::packets::packet_handle::PacketHandle;
    use crate::packets::task::Task;
    use crossbeam::channel::bounded;

    // Test implementation of ObsLogic
    struct TestObsLogic;
    impl ObsLogic for TestObsLogic {
        crate::impl_as_any!(TestObsLogic);
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
    #[should_panic(expected = "NamedInput cannot be empty")]
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
}
