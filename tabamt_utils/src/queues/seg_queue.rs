use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{ Arc, Condvar, Mutex };
use crossbeam::queue::SegQueue;

use crate::cfg::config::Config;
use crate::packets::{notif_log::NotifLog , packet_handle::PacketHandle};
use crate::queues::queue_base::QueueBase;
use crate::traits::notif_ex::NotifEX;
use crate::traits::queue::Queue;

pub type QChild = QueueBase<SegQueue<Box<dyn NotifEX>> , Box<dyn NotifEX>>;

impl Queue<Box<dyn NotifEX>> for QChild{
    fn push(&self, value: Box<dyn NotifEX>) {
        self.main_queue.push(value);

        if self.pause_flag.load(Ordering::SeqCst) == false{
            self.condition_var.notify_one();
        }
    }

    fn pop(&self) -> Box<dyn NotifEX> {
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
        NotifLog::log(format!("Exit Flag Detected! Thread Exiting..."), "Main")
    }
    
    // Finalize the task queue.
    // This will clear the priority queue, set the exit flag to true and notify all threads waiting on the condition variable.
    fn finalize(&self) {
        {
            while self.main_queue.len() != 0 {
                self.main_queue.pop();
            };
            self.exit_flag.store(true, Ordering::SeqCst);

            self.condition_var.notify_all();

            // Send a notification to the engine that the queue is finalized.
            let notif = NotifLog::log("Task Queue Finalized".to_string() , "Main");
            self.packet_handle.send_notification(notif);
        }
    }

    // A function to pause the task queue.
    // This will set the pause flag to true and stop notifying threads of new tasks.
    fn pause(&self) {
        // Set the pause flag to true to stop processing tasks.
        self.pause_flag.store(true, Ordering::SeqCst);

        // Send a notification to the engine that the queue is paused.
        let notif = NotifLog::log("Task Queue Paused".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }

    // A function to continue the task queue.
    // This will set the pause flag to false and notify all threads waiting on the condition variable.
    fn resume(&self) {
        // Set the pause flag to false to continue processing tasks.
        self.pause_flag.store(false, Ordering::SeqCst);
        
        // Send a notification to the engine that the queue is continuing.
        let notif = NotifLog::log("Task Queue continuing".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }
}

impl QChild {
    // Create a new TaskQueue with the given PacketHandle.
    // In this struct, for now, the PacketHandle is only used to send a notification when the queue is finalized.
    // @param packet_handle: The PacketHandle that will be used to send notifications to the engine.
    // @return A new TaskQueue with the given PacketHandle.
    pub fn new(config: &Config, packet_handle: PacketHandle) -> Self{
        Self {
            task_config: Arc::new(config.tasks),
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