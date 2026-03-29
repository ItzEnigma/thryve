// Implementation for the observer pattern.
// Observers can be loggers, audits, email sendings, etc.... Each needs to be implemented alone.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::extras::{named_input::NamedInput , jobs::ObsJob};
use crate::packets::packet_handle::PacketHandle;
use crate::queues::seg_queue::QChild;
use crate::traits::observer_logic::{ObsB , ObsLogic};
use crate::traits::queue::Queue;

pub struct Observer {
    pub obs_name: String,
    pub obs_type: NamedInput,
    pub obs_queue: QChild,
    pub obs_core: Arc<dyn ObsLogic>,
    pub exit_flag: AtomicBool,
    pub tx: PacketHandle,
    pub fn_to_run: ObsJob
}

impl ObsB for Observer{}

impl Observer {
    pub fn new(obs_name: &str , obs_type: &str , obs_core: Arc<dyn ObsLogic>, func: ObsJob , tx: PacketHandle) -> Self{
        Self {
            obs_name: obs_name.to_string(),
            obs_type: NamedInput::new(obs_type),
            obs_queue: QChild::new(tx.clone()),
            obs_core,
            exit_flag: AtomicBool::new(false),
            tx: tx,
            fn_to_run: func
        }
    }

    pub fn clean_up(self: &Arc<Self>) {
        self.exit_flag.store(true, Ordering::Relaxed);
        self.obs_core.clean_up(self.clone());
        self.obs_queue.finalize(Some(self.clone()));
    }
}