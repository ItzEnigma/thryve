// This file has the task priority queue that will notify the threadpools of tasks.
// It is part of the threadpool system.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{ Arc, Condvar, Mutex };
use std::time::{Instant , Duration};
use crossbeam::atomic::AtomicCell;
use crossbeam_skiplist::SkipMap;

use crate::any_naming_input::ANNamingInput;
use crate::config::{Config, TaskConfig};
use crate::notif_log::NotifLog;
use crate::packet_handle::PacketHandle;
use crate::res::Result;
use crate::task::Task;

// The task queue struct that will hold the tasks and notify the threads when a task is available.
pub struct TaskQueue{
    // ---- CONFIG START
    task_config: Arc<TaskConfig>,
    // ---- CONFIG END

    priority_queue: SkipMap<Priority , AtomicCell<Option<EnqueuedTask>>>,         // The priority queue that will hold the tasks.                                     | no permissions.
    gate: Mutex<()>,
    condition_var: Condvar,                             // The condition variable that will notify the threads when a task is available.    | no permissions.
    exit_flag: Arc<AtomicBool>,                             // The flag that will indicate when the queue is being finalized.                   | no permissions.
    pause_flag: Arc<AtomicBool>,                            // The flag that will indicate when the queue is being paused.                      | no permissions.
    packet_handle: PacketHandle,                        // The packet handle that will be used to send notifications to the engine.         | no permissions.
}

pub struct EnqueuedTask{
    pub (crate) task: Task,
    enq_at: Instant,
}

pub struct Priority{
    task_config: Arc<TaskConfig>,
    base_priority: u8,
    enq_at: Instant,
    task_id: u64
}

impl TaskQueue {
    // Create a new TaskQueue with the given PacketHandle.
    // In this struct, for now, the PacketHandle is only used to send a notification when the queue is finalized.
    // @param packet_handle: The PacketHandle that will be used to send notifications to the engine.
    // @return A new TaskQueue with the given PacketHandle.
    pub fn new(config: &Config, packet_handle: PacketHandle) -> Self{
        Self {
            task_config: Arc::new(config.tasks),
            priority_queue: SkipMap::new(),
            gate: Mutex::new(()),
            condition_var: Condvar::new(),
            exit_flag: Arc::new(AtomicBool::new(false)),
            pause_flag: Arc::new(AtomicBool::new(false)),
            packet_handle: packet_handle,
        }
    }

    // Push a task to the priority queue and notify one thread that a task is available.
    // @param task: The task to push to the priority queue.
    pub fn push(&self , task: Task) {
        let mut enq_task = EnqueuedTask{task, enq_at: Instant::now() };
        enq_task.task.check_task_priority(self.task_config.max_priority);

        let priority = Priority{task_config: self.task_config.clone() , base_priority: enq_task.task.base_priority , enq_at: enq_task.enq_at , task_id: enq_task.task.id};

        self.priority_queue.insert(priority, AtomicCell::new(Some(enq_task)));

        if self.pause_flag.load(Ordering::SeqCst) == false{
            self.condition_var.notify_one();
        }
    }

    // Pop a task from the priority queue.
    // If the queue is empty, wait until a task is available or the queue is finalized.
    // If the queue is finalized, return a special task that will indicate to the thread that it should exit.
    // @return The task popped from the priority queue.
    pub fn pop(&self) -> Task {
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
            if let Some(entry) = self.priority_queue.pop_front() {
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
        Task::new(ANNamingInput::fin(), 0, Box::new(|| {Result::new(ANNamingInput::fin(), "".to_string() , ANNamingInput::new("Main")) }))
        
    }

    // Finalize the task queue.
    // This will clear the priority queue, set the exit flag to true and notify all threads waiting on the condition variable.
    pub fn finalize(&self) {
        {
            self.priority_queue.clear();
            self.exit_flag.store(true, Ordering::SeqCst);

            self.condition_var.notify_all();

            // Send a notification to the engine that the queue is finalized.
            let notif = NotifLog::log("Task Queue Finalized".to_string() , ANNamingInput::new("Main"));
            self.packet_handle.send_notification(Box::new(notif));
        }
    }

    // A function to pause the task queue.
    // This will set the pause flag to true and stop notifying threads of new tasks.
    pub fn pause(&self) {
        // Set the pause flag to true to stop processing tasks.
        self.pause_flag.store(true, Ordering::SeqCst);

        // Send a notification to the engine that the queue is paused.
        let notif = NotifLog::log("Task Queue Paused".to_string() , ANNamingInput::new("Main"));
        self.packet_handle.send_notification(Box::new(notif));
    }

    // A function to continue the task queue.
    // This will set the pause flag to false and notify all threads waiting on the condition variable.
    pub fn resume(&self) {
        // Set the pause flag to false to continue processing tasks.
        self.pause_flag.store(false, Ordering::SeqCst);
        
        // Send a notification to the engine that the queue is continuing.
        let notif = NotifLog::log("Task Queue continuing".to_string() , ANNamingInput::new("Main"));
        self.packet_handle.send_notification(Box::new(notif));
    }
}

impl Priority {
    // Calculate the effective priority of the task based on its base priority and the time it has been waiting.
    // The effective priority increases by 1 for every AGING_INTERVAL the task has been waiting.
    // @return The effective priority of the task.
    pub (crate) fn effective_priority(&self) -> u8 {
        let waited = self.enq_at.elapsed();
        let aging_boost = (waited.as_secs_f64() / Duration::from_secs(self.task_config.aging_interval).as_secs_f64()) as u8;

        (self.base_priority + aging_boost).min(self.task_config.max_priority)
    }
}

impl PartialEq for Priority {
    // Two tasks are equal if they have the same priority.
    // This is used to prioritize tasks in the thread pool.
    // @param other: The other task to compare to.
    // @return true if the tasks have the same priority, false otherwise.
    fn eq(&self, other: &Self) -> bool {
        self.base_priority == other.base_priority
            && self.enq_at == other.enq_at
            && self.task_id == other.task_id
    }
}
impl Eq for Priority {}

impl Ord for Priority {
    // Imagine task "Self" has priority 4 and task "Other" has priority 2.
    // Then "Self" is greater than "Other" because it has a higher priority.
    // If both tasks have the same priority, then the one that was enqueued earlier is greater.
    // If both tasks have the same priority and were enqueued at the same time, then the one with the lower id is greater.
    // @param other: The other task to compare to.
    // @return An Ordering indicating the relative priority of the tasks.
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
    // This is needed to implement Ord.
    // It simply calls the cmp function.
    // @param other: The other task to compare to.
    // @return An Option containing the Ordering indicating the relative priority of the tasks.
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}