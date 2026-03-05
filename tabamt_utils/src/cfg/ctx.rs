use std::sync::Arc;
use crossbeam::channel::Receiver;

use crate::cfg::config::Config;
use crate::traits::notif::Notif;
use crate::packets::{packet_handle::PacketHandle , task::Task};

pub struct Ctx {
    pub config: Config,
    pub task_rx: Arc<Receiver<Task>>,
    pub notif_rx: Arc<Receiver<Box<dyn Notif>>>,
    packet_handle: PacketHandle,
    pub obs_init_tasks: Vec<Task>,
    pub plugin_init_tasks: Vec<Task>,
    pub logging_file_names: Vec<String>,
}

impl Ctx {
    pub fn new(config: Config , task_rx: Receiver<Task> , notif_rx: Receiver<Box<dyn Notif>> , packet_handle: PacketHandle) -> Self{
        Self {
            config: config,
            task_rx: Arc::new(task_rx),
            notif_rx: Arc::new(notif_rx),
            packet_handle: packet_handle,
            obs_init_tasks: vec![],
            plugin_init_tasks: vec![],
            logging_file_names: vec![],
        }
    }

    pub fn obs_init_tasks(&mut self) -> Vec<Task>{
        std::mem::take(&mut self.obs_init_tasks)
    }

    pub fn plugin_init_tasks(&mut self) -> Vec<Task>{
        std::mem::take(&mut self.plugin_init_tasks)
    }

    pub fn packet_handle(&self) -> PacketHandle{
        self.packet_handle.clone()
    }
}