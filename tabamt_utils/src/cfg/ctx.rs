use std::sync::Arc;
use crossbeam::channel::Receiver;

use crate::bases::plugin_base::PluginBase;
use crate::cfg::config::Config;
use crate::bases::observer_base::ObsBase;
use crate::traits::notif_ex::NotifEX;
use crate::packets::{packet_handle::PacketHandle , task::Task};

pub struct Ctx {
    pub config: Config,
    pub task_rx: Arc<Receiver<Task>>,
    pub notif_rx: Arc<Receiver<Box<dyn NotifEX>>>,
    packet_handle: PacketHandle,
    pub observers: Vec<ObsBase>,
    pub plugins: Vec<PluginBase>,
    pub logging_file_names: Vec<String>,
}

impl Ctx {
    pub fn new(config: Config , task_rx: Receiver<Task> , notif_rx: Receiver<Box<dyn NotifEX>> , packet_handle: PacketHandle) -> Self{
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

    pub fn observers(&mut self) -> Vec<ObsBase>{
        std::mem::take(&mut self.observers)
    }

    pub fn plugins(&mut self) -> Vec<PluginBase>{
        std::mem::take(&mut self.plugins)
    }

    pub fn packet_handle(&self) -> PacketHandle{
        self.packet_handle.clone()
    }
}