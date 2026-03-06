use std::{marker::PhantomData, sync::{Arc, Condvar, Mutex, atomic::AtomicBool}};

use crate::cfg::config::TaskConfig;
use crate::packets::packet_handle::PacketHandle;

pub struct QueueBase <Q , P> {
    // ---- CONFIG START
    pub task_config: Arc<TaskConfig>,
    // ---- CONFIG END

    pub main_queue: Q,         // The queue that will hold the objects.                                     | no permissions.
    pub gate: Mutex<()>,
    pub condition_var: Condvar,                             // The condition variable that will notify the threads when a task is available.    | no permissions.
    pub exit_flag: Arc<AtomicBool>,                             // The flag that will indicate when the queue is being finalized.                   | no permissions.
    pub pause_flag: Arc<AtomicBool>,                            // The flag that will indicate when the queue is being paused.                      | no permissions.
    pub packet_handle: PacketHandle,                        // The packet handle that will be used to send notifications to the engine.         | no permissions.

    pub _marker: PhantomData<P>
}