//! This module handles the packet passing between modules and engine.
//! It uses channels to send and receive packets.
//! Packets can be tasks or notifications.
//! It uses crossbeam channels to send and receive packets.


//! This system ensures that the only way for plugins/observers to communicate with the engine is through the 2 channels in the strct below.
//! This will make sure that the engine is isolated from the plugins/observers and that the plugins/observers cannot directly access the engine.
//! This will also make sure that the engine can handle the packets in a multithreaded context.
//! It also allows the engine to not know the plugins/observers it will load at compile time.
//! The engine will only know the receiver part of the channel.
//! what plugins/observers can only do with a `PacketHandle` is use the send functions to send packets to the engine.
use crate::bases::notif::Notif;
use crate::packets::task::Task;

use crossbeam::channel::Sender;

/// Holds the sender part of the channels for Tasks and Notifs.
/// It is used to send Tasks and Notifs to the engine.
#[derive(Clone)]
pub struct PacketHandle {
    task_tx: Sender<Task>,
    notif_tx: Sender<Notif>,
}

impl PacketHandle {
    /// Creates a new `PacketHandle` with the given channels.
    /// The channels are created in the engine and passed to the modules.
    /// Since they are only 2 objects, cloning the struct is very cheap, making it very easy to pass around.
    pub fn new(task_tx: Sender<Task> , notif_tx: Sender<Notif>) -> Self {
        Self {
            task_tx: task_tx,
            notif_tx: notif_tx,
        }
    }

    /// Sends a task to the Engine through the task channel.
    pub fn send_task(&self, task: Task){
        let _ = self.task_tx.send(task);
    }
    
    /// Sends a notification to the Engine through the notification channel.
    pub fn send_notification(&self, notif: Notif){
        let _ = self.notif_tx.send(notif);
    }
}