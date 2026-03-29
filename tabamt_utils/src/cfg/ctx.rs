use std::sync::Arc;
use crossbeam::channel::Receiver;

use crate::bases::notif::Notif;
use crate::bases::plugin::Plugin;
use crate::cfg::config::Config;
use crate::bases::observer::Observer;
use crate::packets::{packet_handle::PacketHandle , task::Task};

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

    pub fn observers(&mut self) -> Vec<Observer>{
        std::mem::take(&mut self.observers)
    }

    pub fn plugins(&mut self) -> Vec<Plugin>{
        std::mem::take(&mut self.plugins)
    }

    pub fn packet_handle(&self) -> PacketHandle{
        self.packet_handle.clone()
    }
}