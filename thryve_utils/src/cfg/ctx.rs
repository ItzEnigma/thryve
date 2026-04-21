//! This module defines a `Ctx` struct that holds the Config and other shared resources for the application.
//! The `Ctx` structure is designed to be passed around to different parts of the application, allowing them to access shared resources as needed.
//! This happens so that everything is bundled in one place and can be easily accessed without the need for every field to be passed separately.
use std::sync::Arc;
use crossbeam::channel::Receiver;

use crate::bases::notif::Notif;
use crate::bases::plugin::Plugin;
use crate::cfg::config::Config;
use crate::bases::observer::Observer;
use crate::packets::{packet_handle::PacketHandle , task::Task};

/// `Ctx` is the main context structure for the application.
/// This is what will be passed around to different parts of the application to access the configuration and shared resources.
pub struct Ctx {
    pub config: Config,
    pub task_rx: Arc<Receiver<Task>>,
    pub notif_rx: Arc<Receiver<Notif>>,
    packet_handle: PacketHandle,
    pub observers: Vec<Observer>,
    pub plugins: Vec<Plugin>,
    pub logging_file_names: Vec<String>,
}

impl Ctx {
    /// Creates a new `Ctx` instance with the provided Config, task receiver, notification receiver, and PacketHandle.
    pub fn new(config: Config , task_rx: Receiver<Task> , notif_rx: Receiver<Notif> , packet_handle: PacketHandle) -> Self{
        Self {
            config: config,
            task_rx: Arc::new(task_rx),
            notif_rx: Arc::new(notif_rx),
            packet_handle: packet_handle,
            observers: vec![],
            plugins: vec![],
            logging_file_names: vec![],
        }
    }

    /// Returns the list of Observers and clears the list in the `Ctx`.
    /// This is typically used during the initialization when the observers are needed to be passed to the engine for execution.
    pub fn observers(&mut self) -> Vec<Observer>{
        std::mem::take(&mut self.observers)
    }

    /// Returns the list of Plugins and clears the list in the `Ctx`.
    /// This is typically used during the initialization when the plugins are needed to be passed to the engine for execution.
    pub fn plugins(&mut self) -> Vec<Plugin>{
        std::mem::take(&mut self.plugins)
    }

    /// Returns a clone of the PacketHandle stored in the `Ctx`.
    /// This allows different parts of the application to access the packet handle without needing to pass it separately.
    /// This method is only a shortcut to avoid having to write `ctx.packet_handle.clone()` every time the packet handle is needed.
    pub fn packet_handle(&self) -> PacketHandle{
        self.packet_handle.clone()
    }
}