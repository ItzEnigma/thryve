//! This module contains the `Observer` struct, which is the core of the observer system in Thryve.
//! The `Observer` struct holds information about an observer,
//! including its name, type, queue, core logic, exit flag, and the function to run.
//! 
//! The function that will run is typically a closure that runs in a loop, processing notifications until it exits.
//! 
//! The `Observer` can be gracefully shut down by setting the exit flag.
//! This will signal the loop to exit and clean up resources.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::extras::{named_input::NamedInput , jobs::ObsJob};
use crate::packets::packet_handle::PacketHandle;
use crate::queues::seg_queue::QChild;
use crate::traits::observer_logic::{ObsB , ObsLogic};
use crate::traits::queue::Queue;

/// `Observer` is the observer struct that holds information about an observer.
/// Any struct that implements `ObsLogic` can be used as the core of an `Observer`.
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
    /// Creates a new `Observer` struct with the given `Observer` name, `Observer` type, core logic of the `Observer`, function to run, and PacketHandle for sending notifications.
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

    /// Shuts down the `Observer` gracefully.
    /// It sets the exit flag, calls the core logic's clean_up method, and finalizes the queue.
    pub fn clean_up(self: &Arc<Self>) {
        self.exit_flag.store(true, Ordering::Relaxed);
        self.obs_core.clean_up(self.clone());
        self.obs_queue.finalize(Some(self.clone()));
    }
}