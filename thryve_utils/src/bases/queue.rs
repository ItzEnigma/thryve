//! This module contains the `QueueBase` struct, which is a base struct for all queues in the system.
//! The `QueueBase` struct acts as a parent struct for all queues.
//! This provides common functionality and fields that are shared among all queues.
//! Any queue struct can embed the `QueueBase` struct to inherit its functionality and fields
//! Queues that use this struct implicitly inherit the functionality and fields of the `QueueBase` struct.

use std::{marker::PhantomData, sync::{Arc, Condvar, Mutex, atomic::AtomicBool}};

use crate::cfg::config::TaskConfig;
use crate::packets::packet_handle::PacketHandle;

/// Is a base struct for all queues in the system.
/// It provides common functionality and fields that are shared among all queues.
pub struct QueueBase <Q , P> {
    
    pub task_config: Option<Arc<TaskConfig>>,
    

    pub main_queue: Q,         
    pub gate: Mutex<()>,
    pub condition_var: Condvar,                             
    pub exit_flag: Arc<AtomicBool>,                             
    pub pause_flag: Arc<AtomicBool>,                            
    pub packet_handle: PacketHandle,                        

    pub _marker: PhantomData<P>
}