use crate::task::Task;
use std::sync::mpsc;

#[derive(Clone)]
pub struct TaskHandle {
    tx: mpsc::Sender<Task>,
}

impl TaskHandle {
    pub fn new(tx: mpsc::Sender<Task>) -> Self {
        Self { tx }
    }

    pub fn send_task(&self, task: Task){
        let _ = self.tx.send(task);
    }
}