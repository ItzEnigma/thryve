use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{ Arc, Condvar, Mutex };
use crossbeam::queue::SegQueue;

use crate::bases::observer::Observer;
use crate::bases::{notif::Notif , queue::QueueBase};
use crate::packets::{notif_log::Log , packet_handle::PacketHandle};
use crate::traits::queue::Queue;

pub type QChild = QueueBase<SegQueue<Notif> , Notif>;

impl Queue<Notif> for QChild{
    fn push(&self, value: Notif) {
        self.main_queue.push(value);

        if self.pause_flag.load(Ordering::SeqCst) == false{
            self.condition_var.notify_one();
        }
    }

    fn pop(&self) -> Notif {
        loop {
            // Check if the queue is being finalized.
            if self.exit_flag.load(Ordering::SeqCst) == true {
                break ;
            }

            // check if the service is paused.
            else if self.pause_flag.load(Ordering::SeqCst) == true {
                let _unused = self.condition_var.wait(self.gate.lock().unwrap()).unwrap();
            }
            
            // If a task is available, return it.
            if let Some(notif) = self.main_queue.pop() {
                return notif;
            }

            // Wait until notified that a task is available or the queue is finalized.
            let _unused = self.condition_var.wait(self.gate.lock().unwrap()).unwrap();
        }

        // If the queue is being finalized, return a special task that will indicate to the thread that it should exit.
        // The task will have the name "Fin" and a priority of 0.
        Log::log(format!("Exit Flag Detected! Obs Thread Exiting..."), "Main")
    }
    
    // Finalize the task queue.
    // This will clear the priority queue, set the exit flag to true and notify all threads waiting on the condition variable.
    fn finalize(&self , logger: Option<Arc<Observer>>) {
        {
            while self.main_queue.len() != 0 {
                let notif = self.main_queue.pop().unwrap();
                logger.as_ref().unwrap().obs_queue.push(notif);
            };
            self.exit_flag.store(true, Ordering::SeqCst);

            self.condition_var.notify_all();

            // Send a notification to the engine that the queue is finalized.
            let notif = Log::log("Obs Queue Finalized".to_string() , "Main");
            logger.unwrap().obs_queue.push(notif);
        }
    }

    // A function to pause the task queue.
    // This will set the pause flag to true and stop notifying threads of new tasks.
    fn pause(&self) {
        // Set the pause flag to true to stop processing tasks.
        self.pause_flag.store(true, Ordering::SeqCst);

        // Send a notification to the engine that the queue is paused.
        let notif = Log::log("Obs Queue Paused".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }

    // A function to continue the task queue.
    // This will set the pause flag to false and notify all threads waiting on the condition variable.
    fn resume(&self) {
        // Set the pause flag to false to continue processing tasks.
        self.pause_flag.store(false, Ordering::SeqCst);
        
        // Send a notification to the engine that the queue is continuing.
        let notif = Log::log("Obs Queue continuing".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }
}

impl QChild {
    // Create a new TaskQueue with the given PacketHandle.
    // In this struct, for now, the PacketHandle is only used to send a notification when the queue is finalized.
    // @param packet_handle: The PacketHandle that will be used to send notifications to the engine.
    // @return A new TaskQueue with the given PacketHandle.
    pub fn new(packet_handle: PacketHandle) -> Self{
        Self {
            task_config: None,
            main_queue: SegQueue::new(),
            gate: Mutex::new(()),
            condition_var: Condvar::new(),
            exit_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
            packet_handle: packet_handle,
            _marker: std::marker::PhantomData,
        }
    }
}