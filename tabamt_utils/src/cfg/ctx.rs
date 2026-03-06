use std::sync::Arc;
use crossbeam::channel::Receiver;

use crate::cfg::config::Config;
use crate::traits::notif_ex::NotifEX;
use crate::packets::{packet_handle::PacketHandle , task::Task};
use crate::traits::observer::Observer;
use crate::traits::plugin::Plugin;

pub struct Ctx {
    pub config: Config,
    pub task_rx: Arc<Receiver<Task>>,
    pub notif_rx: Arc<Receiver<Box<dyn NotifEX>>>,
    packet_handle: PacketHandle,
    pub observers: Vec<Box<dyn Observer>>,
    pub plugins: Vec<Box<dyn Plugin>>,
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

    pub fn observers(&mut self) -> Vec<Box<dyn Observer>>{
        std::mem::take(&mut self.observers)
    }

    pub fn plugins(&mut self) -> Vec<Box<dyn Plugin>>{
        std::mem::take(&mut self.plugins)
    }

    pub fn packet_handle(&self) -> PacketHandle{
        self.packet_handle.clone()
    }
}