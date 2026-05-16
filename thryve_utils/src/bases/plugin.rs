//! Core plugin structure for the Thryve plugin system.
//!
//! This module provides the [`Plugin`] struct, which encapsulates all state and
//! behavior required for a plugin to extend the functionality of the Thryve system.
//! Plugins are modular components that can be dynamically loaded and executed
//! without modifying the core codebase.
//!
//! # Architecture
//!
//! Each plugin maintains:
//! - A unique name and type identifier for routing and debugging
//! - Core logic implementing the [`PlgLogic`] trait for custom behavior
//! - A packet handle for communicating with the engine
//! - An initialization function that runs when the plugin is loaded
//!
//! # Lifecycle
//!
//! Plugins are initialized with an `plg_entry_point` closure that performs
//! the main task when the plugin is first loaded. This function typically
//! chains calls to other functions in the plugin, enabling a flexible and
//! modular design. Plugins can be gracefully shut down by calling the
//! [`clean_up`](PluginB::clean_up) method.

use std::sync::Arc;

use crate::extras::{named_input::NamedInput, jobs::TaskJob};
use crate::packets::packet_handle::PacketHandle;
use crate::res;
use crate::traits::plugin_logic::{PlgLogic, PluginB};

/// Core plugin structure for the Thryve plugin system.
///
/// The `Plugin` struct encapsulates all state and behavior required for a
/// plugin to extend the functionality of the Thryve system. Plugins are
/// modular components that can be dynamically loaded and executed without
/// modifying the core codebase.
///
/// # Fields
///
/// - `plg_name`: Unique identifier for the plugin
/// - `plg_type`: Type classifier for the plugin
/// - `plg_core`: Core logic implementing custom plugin behavior
/// - `tx`: Packet handle for communicating with the engine
/// - `plg_entry_point`: Initialization function executed when the plugin loads
///
/// # Example
///
/// ```no_run
/// // Plugins are typically created by implementing PlgLogic and
/// // passing the core logic to Plugin::new along with an initialization function
/// ```
pub struct Plugin {
    /// Unique identifier for the plugin.
    pub plg_name: String,
    /// Type classifier for the plugin.
    pub plg_type: NamedInput,
    /// Core logic implementing custom plugin behavior.
    pub plg_core: Arc<dyn PlgLogic>,
    /// Packet handle for communicating with the engine.
    pub tx: PacketHandle,
    /// Initialization function executed when the plugin loads.
    plg_entry_point: TaskJob,
}

impl PluginB for Plugin {
    /// Gracefully shuts down the plugin.
    ///
    /// Delegates cleanup to the plugin's core logic, allowing each plugin
    /// to perform its own resource deallocation and finalization.
    fn clean_up(&self) {
        let x = Arc::clone(&self.plg_core);
        x.clean_up();
    }
}

impl Plugin {
    /// Creates a new plugin with the specified configuration.
    ///
    /// Initializes a new plugin instance with the given name, type, core logic,
    /// packet handle, and initialization function. The initialization function
    /// will be executed when the plugin is first loaded by the engine.
    ///
    /// # Arguments
    ///
    /// * `plg_name` - Unique identifier for the plugin
    /// * `plg_type` - Type classifier for the plugin
    /// * `plg_core` - Core logic implementing custom plugin behavior
    /// * `tx` - Packet handle for communicating with the engine
    /// * `job` - Initialization function executed when the plugin loads
    ///
    /// # Returns
    ///
    /// A new `Plugin` instance configured with the provided parameters.
    pub fn new(
        plg_name: &str,
        plg_type: &str,
        plg_core: Arc<dyn PlgLogic>,
        tx: PacketHandle,
        job: TaskJob,
    ) -> Self {
        Self {
            plg_name: plg_name.to_string(),
            plg_type: NamedInput::new(plg_type),
            plg_core,
            tx,
            plg_entry_point: job,
        }
    }

    /// Extracts and returns the initialization function.
    ///
    /// Returns the `plg_entry_point` closure and replaces it with a default
    /// error function. This ensures the initialization function is only executed
    /// once when the plugin is first loaded.
    ///
    /// # Returns
    ///
    /// The initialization function closure, or a default error function if
    /// called multiple times.
    pub fn plg_entry_point(&mut self) -> TaskJob {
        let temp: TaskJob = self.default_fn();
        std::mem::replace(&mut self.plg_entry_point, temp)
    }

    /// Returns a default error function as a placeholder.
    ///
    /// This function is used as a replacement for `plg_entry_point` after
    /// the original function has been extracted. If this function is ever
    /// executed, it indicates a serious error in the plugin loading process.
    ///
    /// # Returns
    ///
    /// A closure that returns an error result indicating a critical failure.
    fn default_fn(&self) -> TaskJob {
        let job: TaskJob = Box::new(move || {
            res::Result::new(
                "BIGERR",
                "If This Is Ever Called, Something is Wrong".to_string(),
                "Main",
            )
        });
        job
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packets::packet_handle::PacketHandle;
    use crate::packets::task::Task;
    use crate::bases::notif::Notif;
    use crossbeam::channel::bounded;

    // Test implementation of PlgLogic
    struct TestPlgLogic;
    impl PlgLogic for TestPlgLogic {
        crate::impl_as_any!(TestPlgLogic);
        fn clean_up(&self) {
            // No-op for testing
        }
    }

    #[test]
    fn test_plugin_new_creates_plugin() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
    #[should_panic(expected = "NamedInput cannot be empty")]
    fn test_plugin_with_empty_type_panics() {
        // Arrange
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
        let packet_handle = PacketHandle::new(task_tx, notif_tx);
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
