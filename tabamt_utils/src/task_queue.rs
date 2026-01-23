// This file has the task priority queue that will notify the threadpools of tasks.
// It is part of the threadpool system.

use std::collections::BinaryHeap;
use std::sync::{ Arc, Condvar, Mutex };
use std::time::{Instant , Duration};

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

    priority_queue: Mutex<BinaryHeap<EnqueuedTask>>,    // The priority queue that will hold the tasks.                                     | no permissions.
    condition_var: Condvar,                             // The condition variable that will notify the threads when a task is available.    | no permissions.
    exit_flag: Mutex<bool>,                             // The flag that will indicate when the queue is being finalized.                   | no permissions.
    pause_flag: Mutex<bool>,                            // The flag that will indicate when the queue is being paused.                      | no permissions.
    packet_handle: PacketHandle,                        // The packet handle that will be used to send notifications to the engine.         | no permissions.
}

pub struct EnqueuedTask{
    task_config: Arc<TaskConfig>,
    task: Task,
    enqueued_at: Instant,
}

impl TaskQueue {
    // Create a new TaskQueue with the given PacketHandle.
    // In this struct, for now, the PacketHandle is only used to send a notification when the queue is finalized.
    // @param packet_handle: The PacketHandle that will be used to send notifications to the engine.
    // @return A new TaskQueue with the given PacketHandle.
    pub fn new(config: &Config, packet_handle: PacketHandle) -> Self{
        Self {
            task_config: Arc::new(config.tasks),
            priority_queue: Mutex::new(BinaryHeap::new()),
            condition_var: Condvar::new(),
            exit_flag: Mutex::new(false),
            pause_flag: Mutex::new(false),
            packet_handle: packet_handle,
        }
    }

    // Push a task to the priority queue and notify one thread that a task is available.
    // @param task: The task to push to the priority queue.
    pub fn push(&self , task: Task) {
        let mut enqueued_task = EnqueuedTask{ task_config: self.task_config.clone() , task, enqueued_at: Instant::now() };
        enqueued_task.task.check_task_priority(self.task_config.max_priority);

        let mut queue  = self.priority_queue.lock().unwrap();
        queue.push(enqueued_task);

        if *self.pause_flag.lock().unwrap() == false{
            self.condition_var.notify_one();
        }
    }

    // Pop a task from the priority queue.
    // If the queue is empty, wait until a task is available or the queue is finalized.
    // If the queue is finalized, return a special task that will indicate to the thread that it should exit.
    // @return The task popped from the priority queue.
    pub fn pop(&self) -> Task {
        let mut queue = self.priority_queue.lock().unwrap();

        // loop until a task is available or the queue is finalized.
        // The loop is necessary because the condition variable can be notified spuriously, meaning that it sometimes can be notified even if no task is available.
        loop {
            {
                // Check if the queue is being finalized.
                if *self.exit_flag.lock().unwrap() == true {
                    break;
                }

                // check if the service is paused.
                else if *self.pause_flag.lock().unwrap() == true {
                    queue = self.condition_var.wait(queue).unwrap();
                }
            }

            // If a task is available, return it.
            if let Some(enqueued_task) = queue.pop() {
                return enqueued_task.task;
            }

            // Wait until notified that a task is available or the queue is finalized.
            queue = self.condition_var.wait(queue).unwrap();
        }

        // If the queue is being finalized, return a special task that will indicate to the thread that it should exit.
        // The task will have the name "Fin" and a priority of 0.
        Task::new(ANNamingInput::fin(), 0, Box::new(|| {Result::new(ANNamingInput::fin(), "".to_string() , ANNamingInput::new("Main")) }))
    }

    // Finalize the task queue.
    // This will clear the priority queue, set the exit flag to true and notify all threads waiting on the condition variable.
    pub fn finalize(&self) {
        {
            self.priority_queue.lock().unwrap().clear();
            let mut exit = self.exit_flag.lock().unwrap();
            *exit = true;
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
        let mut pause = self.pause_flag.lock().unwrap();
        *pause = true;

        // Send a notification to the engine that the queue is paused.
        let notif = NotifLog::log("Task Queue Paused".to_string() , ANNamingInput::new("Main"));
        self.packet_handle.send_notification(Box::new(notif));
    }

    // A function to continue the task queue.
    // This will set the pause flag to false and notify all threads waiting on the condition variable.
    pub fn resume(&self) {
        // Set the pause flag to false to continue processing tasks.
        let mut pause = self.pause_flag.lock().unwrap();
        *pause = false;

        // Send a notification to the engine that the queue is continuing.
        let notif = NotifLog::log("Task Queue continuing".to_string() , ANNamingInput::new("Main"));
        self.packet_handle.send_notification(Box::new(notif));
    }
}

impl EnqueuedTask {
    // Calculate the effective priority of the task based on its base priority and the time it has been waiting.
    // The effective priority increases by 1 for every AGING_INTERVAL the task has been waiting.
    // @return The effective priority of the task.
    pub (crate) fn effective_priority(&self) -> u8 {
        let waited = self.enqueued_at.elapsed();
        let aging_boost = (waited.as_secs_f64() / Duration::from_secs(self.task_config.aging_interval).as_secs_f64()) as u8;

        (self.task.base_priority() + aging_boost).min(self.task_config.max_priority)
    }
}

impl PartialEq for EnqueuedTask {
    // Two tasks are equal if they have the same priority.
    // This is used to prioritize tasks in the thread pool.
    // @param other: The other task to compare to.
    // @return true if the tasks have the same priority, false otherwise.
    fn eq(&self, other: &Self) -> bool {
        self.task.base_priority() == other.task.base_priority()
    }
}
impl Eq for EnqueuedTask {}

impl Ord for EnqueuedTask {
    // Imagine task "Self" has priority 4 and task "Other" has priority 2.
    // Then "Self" is greater than "Other" because it has a higher priority.
    // If both tasks have the same priority, then the one that was enqueued earlier is greater.
    // If both tasks have the same priority and were enqueued at the same time, then the one with the lower id is greater.
    // @param other: The other task to compare to.
    // @return An Ordering indicating the relative priority of the tasks.
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.effective_priority()
            .cmp(&other.effective_priority())
            .then_with(|| other.enqueued_at.cmp(&self.enqueued_at))
            .then_with(|| other.task.id().cmp(&self.task.id()))
    }
    
}

impl PartialOrd for EnqueuedTask {
    // This is needed to implement Ord.
    // It simply calls the cmp function.
    // @param other: The other task to compare to.
    // @return An Option containing the Ordering indicating the relative priority of the tasks.
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}