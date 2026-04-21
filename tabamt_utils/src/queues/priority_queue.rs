//! This module has the task priority queue that will notify the threadpools of tasks.
//! It is part of the Threadpool system.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{ Arc, Condvar, Mutex };
use std::time::{Instant , Duration};
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;

use crate::bases::observer::Observer;
use crate::cfg::config::{Config, TaskConfig};
use crate::packets::{notif_log::Log , packet_handle::PacketHandle , task::Task};
use crate::bases::queue::QueueBase;
use crate::traits::queue::Queue;
use crate::res::Result;

/// The priority queue for tasks.
/// It uses a `SkipMap` to maintain the order of tasks based on their priority, aging, and Task id.
pub type PQChild<T> = QueueBase<SkipMap<Priority , AtomicCell<Option<EnqueuedTask>>> , T>;

/// A wrapper for a task that includes the time it was enqueued, used for aging.
pub struct EnqueuedTask{
    pub (crate) task: Task,
    enq_at: Instant,
}

/// The priority of a Task, which is determined by its base priority and how long it has been waiting in the queue (aging).
pub struct Priority{
    task_config: Arc<TaskConfig>,
    base_priority: u8,
    enq_at: Instant,
    task_id: u64
}

impl Queue<Task> for PQChild<Task> {
    /// Pushes a given Task into the priority queue, calculating its priority based on its base priority and the time it has been waiting (aging).
    fn push(&self , task: Task) {

        // Wrap the task in an `EnqueuedTask` to track when it was enqueued for aging purposes.
        let mut enq_task = EnqueuedTask{task, enq_at: Instant::now() };
        enq_task.task.check_task_priority(self.task_config.clone().unwrap().max_priority);

        // Create a `Priority` struct for the task, which will be used to determine its position in the queue.
        let priority = Priority{task_config: self.task_config.clone().unwrap() , base_priority: enq_task.task.base_priority , enq_at: enq_task.enq_at , task_id: enq_task.task.id};

        // Insert the task into the `SkipMap` with its calculated priority. The `AtomicCell` allows for safe concurrent access to the task.
        self.main_queue.insert(priority, AtomicCell::new(Some(enq_task)));

        // Notify one waiting thread that a new task has been added to the queue, so it can wake up and process the task.
        if self.pause_flag.load(Ordering::SeqCst) == false{
            self.condition_var.notify_one();
        }
    }

    /// Pops the highest priority task from the queue, waiting if necessary until a task is available or the queue is finalized.
    /// If there are aged tasks, they will be prioritized over newer tasks, even if newer tasks have a higher base priority.
    /// This is to prevent starvation of lower priority tasks.
    fn pop(&self) -> Task {
        // loop until a task is available or the queue is finalized.
        // The loop is necessary because the condition variable can be notified spuriously, meaning that it sometimes can be notified even if no task is available.
        loop {
            // Check if the queue is being finalized.
            if self.exit_flag.load(Ordering::SeqCst) == true {
                break;
            }

            // check if the service is paused.
            else if self.pause_flag.load(Ordering::SeqCst) == true {
                let _unused = self.condition_var.wait(self.gate.lock().unwrap()).unwrap();
            }
            
            // If a task is available, return it.
            if let Some(entry) = self.main_queue.pop_front() {
                let mut wrapper = entry.value().take();
                
                // .take() atomically moves the Task out and leaves None behind.
                // This works behind a shared reference (&) and is lock-free!
                if let Some(moved_task) = wrapper.take() {
                    return moved_task.task; 
                }
            }

            // Wait until notified that a task is available or the queue is finalized.
            let _unused = self.condition_var.wait(self.gate.lock().unwrap()).unwrap();
        }

        // If the queue is being finalized, return a special task that will indicate to the thread that it should exit.
        // The task will have the name "Fin" and a priority of 0.
        Task::new("fin", 0, Box::new(|| {Result::new("fin", "".to_string() , "Main") }))
        
    }

    
    /// Finalizes the queue by clearing all tasks and setting the exit flag.
    fn finalize(&self , _: Option<Arc<Observer>>) {
        {
            self.main_queue.clear();
            self.exit_flag.store(true, Ordering::SeqCst);

            self.condition_var.notify_all();
            
            let notif = Log::log("Task Queue Finalized".to_string() , "Main");
            self.packet_handle.send_notification(notif);
        }
    }

    
    /// Pauses the queue by setting the pause flag and notifying all waiting threads that the queue is paused.
    fn pause(&self) {
        self.pause_flag.store(true, Ordering::SeqCst);

        let notif = Log::log("Task Queue Paused".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }

    
    /// Resumes the queue by clearing the pause flag and notifying all waiting threads that the queue is resuming.
    fn resume(&self) {
        self.pause_flag.store(false, Ordering::SeqCst);
        
        let notif = Log::log("Task Queue continuing".to_string() , "Main");
        self.packet_handle.send_notification(notif);
    }
}

impl PQChild<Task> {
    /// Creates a new priority queue with the given Config and PacketHandle.
    pub fn new(config: &Config, packet_handle: PacketHandle) -> Self{
        Self {
            task_config: Some(Arc::new(config.tasks)),
            main_queue: SkipMap::new(),
            gate: Mutex::new(()),
            condition_var: Condvar::new(),
            exit_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
            packet_handle: packet_handle,
            _marker: std::marker::PhantomData,
        }
    }
}

impl Priority {
    /// Calculates the effective priority of a task by combining its base priority with an aging boost based on how long it has been waiting in the queue.
    pub (crate) fn effective_priority(&self) -> u8 {
        let waited = self.enq_at.elapsed();
        let aging_boost = (waited.as_secs_f64() / Duration::from_secs(self.task_config.aging_interval).as_secs_f64()) as u8;

        (self.base_priority + aging_boost).min(self.task_config.max_priority)
    }
}

impl PartialEq for Priority {
    fn eq(&self, other: &Self) -> bool {
        self.base_priority == other.base_priority
            && self.enq_at == other.enq_at
            && self.task_id == other.task_id
    }
}
impl Eq for Priority {}

impl Ord for Priority {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        if other.base_priority >= (self.task_config.max_priority as f32 * 0.8) as u8{
            return std::cmp::Ordering::Greater
        }
        other.effective_priority()
            .cmp(&self.effective_priority())
            .then_with(|| self.enq_at.cmp(&other.enq_at))
            .then_with(|| self.task_id.cmp(&other.task_id))
    }
    
}

impl PartialOrd for Priority {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}