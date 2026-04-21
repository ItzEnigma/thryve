//! This module handles the logic behind results returned by Tasks.
//! Results can be of different types and can contain different data.

use std::fmt::Display;

use crate::bases::plugin::Plugin;

use crate::extras::named_input::NamedInput;
use crate::bases::observer::Observer;
use crate::packets::task::Task;


/// Represents the result of a Task.
/// Any task job that runs must return a `Result`, which can be used to determine the next steps in the execution flow.
/// Think of Tasks as nodes of a graph of operations.
/// `Result`s are the edges that connect these nodes, and they can contain information about which node to execute next.
/// Tasks in the `next_task` field can be from the same plugin or from a different plugin.
/// This gives developers the ability to seperate the logic into multiple plugins and connect them together in a flexible manner.
pub struct Result {
    pub result_type: NamedInput,             
    pub message: String,                        
    pub log_msg_to_file: NamedInput,         
    pub next_task: Option<Task>,                
    pub observer: Option<Observer>,    
    pub plugin: Option<Plugin>,        
}

impl Display for Result {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f , "{}", format!("Message: {}" , self.message))
    }
}

impl Result {
    /// Creates a new `Result` with the given result type, message, and logging file.
    /// This regular constructor is used for results that do not need to return a Task.
    /// This effectively means that the previous Task that ran is a leaf node in the graph of operations. 
    /// The execution flow for a specific Plugin will end after this result is returned.
    /// Note: If Plugins are connected, some tasks may call for the execution of a Task from another Plugin.
    /// In this case, the result of the first Plugin's Task will be a `Result` that contains the next Task to be executed in the second Plugin.
    pub fn new(res_type: &str , msg: String , logging_file: &str) -> Self{
        Result {
            result_type: NamedInput::new(res_type),
            message: msg,
            log_msg_to_file: NamedInput::new(logging_file),
            next_task: None,
            observer: None,
            plugin: None,
        }
    }

    /// Creates a new `Result` with the given parameters, including a next Task.
    /// This constructor is used for results that need to return a Task, allowing for the chaining of operations.
    /// This effectively means that the `Result` is an edge in the graph of operations.
    /// It should connect to the next Task that needs to be executed in the flow of operations.
    pub fn new_tasked_res(res_type: &str , msg: String , logging_file: &str , next_task: Option<Task>) -> Self{
        Result {
            result_type: NamedInput::new(res_type),
            message: msg,
            log_msg_to_file: NamedInput::new(logging_file),
            next_task: next_task,
            observer: None,
            plugin: None,
        }
    }
}