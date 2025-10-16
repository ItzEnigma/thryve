// This file has the task priority queue that will notify the threadpools of tasks.
// It is part of the threadpool system.

use std::collections::BinaryHeap;
use std::sync::{ Condvar , Mutex };

use crate::any_naming_input::ANNamingInput;
use crate::notification::Notification;
use crate::packet_handle::PacketHandle;
use crate::res::Result;
use crate::task::{Task, TaskName};

// The task queue struct that will hold the tasks and notify the threads when a task is available.
pub struct TaskQueue{
    priority_queue: Mutex<BinaryHeap<Task>>,    // The priority queue that will hold the tasks.                                     | no permissions.
    condition_var: Condvar,                     // The condition variable that will notify the threads when a task is available.    | no permissions.
    exit_flag: Mutex<bool>,                     // The flag that will indicate when the queue is being finalized.                   | no permissions.
    pause_flag: Mutex<bool>,                    // The flag that will indicate when the queue is being paused.                      | no permissions.
    packet_handle: PacketHandle,                // The packet handle that will be used to send notifications to the engine.         | no permissions.
}

impl TaskQueue {
    // Create a new TaskQueue with the given PacketHandle.
    // In this struct, for now, the PacketHandle is only used to send a notification when the queue is finalized.
    // @param packet_handle: The PacketHandle that will be used to send notifications to the engine.
    // @return A new TaskQueue with the given PacketHandle.
    pub fn new(packet_handle: PacketHandle) -> Self{
        Self { 
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
        let mut queue  = self.priority_queue.lock().unwrap();
        queue.push(task);

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
            if let Some(task) = queue.pop() {
                return task;
            }

            // Wait until notified that a task is available or the queue is finalized.
            queue = self.condition_var.wait(queue).unwrap();
        }

        // If the queue is being finalized, return a special task that will indicate to the thread that it should exit.
        // The task will have the name "Fin" and a priority of 0.
        Task::new(TaskName::Fin, 0, Box::new(|| {Result::new(ANNamingInput::fin(), "".to_string() , ANNamingInput::new("Main")) }))
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
            let notif = Notification::log("Task Queue Finalized".to_string() , ANNamingInput::new("Main"));
            self.packet_handle.send_notification(notif);
        }
    }

    // Pause the task queue.
    pub fn pause(&self) {
        let mut pause = self.pause_flag.lock().unwrap();
        *pause = true;

        // Send a notification to the engine that the queue is paused.
        let notif = Notification::log("Task Queue Paused".to_string() , ANNamingInput::new("Main"));
        self.packet_handle.send_notification(notif);
    }

    // Continue the task queue.
    pub fn cont(&self) {
        let mut pause = self.pause_flag.lock().unwrap();
        *pause = false;

        // Send a notification to the engine that the queue is continuing.
        let notif = Notification::log("Task Queue continuing".to_string() , ANNamingInput::new("Main"));
        self.packet_handle.send_notification(notif);
    }
}