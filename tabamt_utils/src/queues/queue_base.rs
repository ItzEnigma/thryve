use std::{marker::PhantomData, sync::{Arc, Condvar, Mutex, atomic::AtomicBool}};

use crossbeam::queue::SegQueue;

use crate::cfg::config::TaskConfig;
use crate::packets::packet_handle::PacketHandle;
use crate::traits::queue::Queue;

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
pub type QChild<T> = QueueBase<SegQueue<T> , T>;


impl <Q , P: Send + Sync + 'static> Queue<P> for QueueBase<Q , P> 
where
    Q: Queue<P>
{
    
    fn push(&self, value: P) {
        self.main_queue.push(value);
    }

    fn pop(&self) -> P {
        self.main_queue.pop()
    }

    fn finalize(&self) {
        self.main_queue.finalize();
    }

    fn pause(&self) {
        self.main_queue.pause();
    }

    fn resume(&self) {
        self.main_queue.resume();
    }
}