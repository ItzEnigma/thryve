//! This module contains the `Plugin` struct, which represents a plugin in the system.
//! This allows for dynamic functionality to be added to the system without modifying the core codebase.
//! 
//! An `initial_fn_to_run` is stored in the struct, used to run a specific function when the plugin is initialized.
//! This function is typically a closure that performs the main task of the plugin when it is first loaded.
//! This function is usually used as a chain to call other functions in the plugin, allowing for a flexible and modular design.
//! 
//! Any struct that implements the `PlgLogic` trait can be used as the core of a `Plugin`.
//! 
//! Plugins can be gracefully shut down by calling the `clean_up` method, which will call the core logic's `clean_up` method.

use std::sync::Arc;

use crate::extras::{named_input::NamedInput , jobs::TaskJob};
use crate::packets::packet_handle::PacketHandle;
use crate::res;
use crate::traits::plugin_logic::{PlgLogic, PluginB};

/// Holds information about a plugin.
/// Any struct that implements PlgLogic can be used as the core of a `Plugin`.
pub struct Plugin {
    pub plg_name: String,
    pub plg_type: NamedInput,
    pub plg_core: Arc<dyn PlgLogic>,
    pub tx: PacketHandle,

    initial_fn_to_run: TaskJob
}

impl PluginB for Plugin {
    fn clean_up(&self) {
        let x = Arc::clone(&self.plg_core);
        x.clean_up();
    }
}

impl Plugin {
    /// Creates a new `Plugin` struct with the provided `plugin` name, `plugin` type, core logic of the `plugin`, packet handle for sending notifications, and initial function to run when the `plugin` is initialized.
    pub fn new(plg_name: &str , plg_type: &str , plg_core: Arc<dyn PlgLogic> , tx: PacketHandle , job: TaskJob) -> Self {
        Self {
            plg_name: plg_name.to_string(),
            plg_type: NamedInput::new(plg_type),
            plg_core,
            tx,
            initial_fn_to_run: job
        }
    }

    /// Returns the function within `initial_fn_to_run` field to run when the `plugin` is initialized.
    pub fn initial_fn_to_run(&mut self) -> TaskJob {
        let temp: TaskJob = self.default_fn();
        std::mem::replace(&mut self.initial_fn_to_run , temp)
    }

    /// Is Used as a placeholder for the `initial_fn_to_run` field.
    /// This function is only called in the `initial_fn_to_run` method.
    /// It replaces the original function with a default function.
    /// If this function is ever called, it indicates that something went completely wrong.
    fn default_fn(&self) -> TaskJob{
        let job: TaskJob = Box::new(move || {
            res::Result::new("BIGERR", "If This Is Ever Called, Something is Wrong".to_string(), "Main")
        });

        job
    }
}