// This file handles the packet passing between modules and engine.
// It uses channels to send and receive packets.
// Packets can be tasks or notifications.
// It uses crossbeam channels to send and receive packets.


// This system ensures that the only way for plugins to communicate with the engine is through the 2 channels in the strct below.
// This will make sure that the engine is isolated from the plugins and that the plugins cannot directly access the engine.
// This will also make sure that the engine can handle the packets in a multithreaded context.
// It also allows the engine to not know the plugins it will load at compile time.
// The engine will only know the receiver part of the channel.
// It is important to note that any module (plugin or observer) holding the PacketHandle struct will have no read or right access to the senders part.
// All the modules can do is use the send functions to send packets to the engine.

use crate::traits::notif::Notif;
use crate::packets::task::Task;

use crossbeam::channel::Sender;

// The struct that will handle the packets between modules and engine.
#[derive(Clone)]
pub struct PacketHandle {
    task_tx: Sender<Task>,                // Channel to send tasks to the thread pool.        | no permissions.
    notif_tx: Sender<Box<dyn Notif>>,       // Channel to send notifications to the engine.     | no permissions.
}

impl PacketHandle {
    // Create a new PacketHandle with the given channels.
    // The channels are created in the engine and passed to the modules.
    // Since they are only 2 objects, cloning the struct is very cheap, making it very easy to pass around.
    // @param task_tx: The channel to send tasks to the thread pool.
    // @param notif_tx: The channel to send notifications to the engine.
    // @return A new PacketHandle with the given channels. 
    pub fn new(task_tx: Sender<Task> , notif_tx: Sender<Box<dyn Notif>>) -> Self {
        Self {
            task_tx: task_tx,
            notif_tx: notif_tx,
        }
    }

    // Send a task to the thread pool.
    // @param task: The task to send.
    pub fn send_task(&self, task: Task){
        let _ = self.task_tx.send(task);
    }

    // Send a notification to the engine.
    // @param notif: The notification to send.
    pub fn send_notification(&self, notif: Box<dyn Notif>){
        let _ = self.notif_tx.send(notif);
    }
}