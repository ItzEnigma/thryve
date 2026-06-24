use std::{collections::HashMap, sync::{Arc, atomic::AtomicBool}, thread::JoinHandle};

use crossbeam::channel::Sender;

use crate::{extras::named_input::NamedInput, packets::packet_handle::PacketHandle, queues::seg_queue::QChild, res, traits::packets::Packet};

pub struct WorkerThread {
    /// Thread handle for lifecycle management.
    pub handle: JoinHandle<()>,
    /// Worker's own SkipMap queue.
    local_queue: Arc<QChild>,
    /// All local queues (for stealing).
    local_queues: HashMap<NamedInput , Arc<QChild>>,
    /// Exit flag (shared with ThreadPool).
    exit_flag: Arc<AtomicBool>,
    /// Pause flag (shared with ThreadPool).
    pause_flag: Arc<AtomicBool>,
    /// Randomized order of queue indices to steal from.
    steal_order: Vec<usize>,
    /// Result sender.
    res_tx: Arc<Sender<(u64, res::Result)>>,
    /// Packet handle for notifications.
    packet_handle: PacketHandle,
}