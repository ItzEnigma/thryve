// This file handles the logic behind results returned by tasks.
// Results can be of different types and can contain different data.

use std::fmt::Display;

// This is just a struct that has results returned by executed tasks.
use crate::extras::named_input::NamedInput;
use crate::traits::{observer::Observer, plugin::Plugin};
use crate::packets::task::Task;

// The struct that will hold the results returned by tasks.
// It can hold different types of data depending on the type of result.
pub struct Result {
    pub result_type: NamedInput,             // The type of result.                                                          | Take Ownership.
    pub message: String,                        // A message associated with the result.                                        | Take Ownership.
    pub log_msg_to_file: NamedInput,         // The module type that will be used to name the log file.                      | Take Ownership.
    pub next_task: Option<Task>,                // An optional task that can be executed after the result is handled.           | Take Ownership.
    pub observer: Option<Box<dyn Observer>>,    // An optional observer that can be registered after the result is handled.     | Take Ownership.
    pub plugin: Option<Box<dyn Plugin>>,        // An optional plugin that can be registered after the result is handled.       | Take Ownership.
}

// Implement Display for Result so that it can be printed easily.
impl Display for Result {
    // Implement the fmt function to format the result as a string.
    // This will just print the message associated with the result.
    // @param f: The formatter to use.
    // @return A result indicating whether the formatting was successful.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f , "{}", format!("Message: {}" , self.message))
    }
}

impl Result {
    // Create a new Result with the given parameters.
    // This regular constructor is used for results that do not need to return a task, observer or plugin.
    // @param res_type: The type of result.
    // @param msg: The message associated with the result.
    // @param logging_file: The module type that will be used to name the log file.
    // @return A new Result with the given parameters.
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

    // Create a new Result with the given parameters.
    // This constructor is used for results that need to return a task to be executed after the result is handled.
    // @param res_type: The type of result.
    // @param msg: The message associated with the result.
    // @param logging_file: The module type that will be used to name the log file.
    // @param next_task: An optional task that can be executed after the result is handled
    // @return A new Result with the given parameters.
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

    // Create a new Result with the given parameters.
    // This constructor is used for results that need to return an observer to be registered after the result is handled.
    // It can also return a task to be executed after the result is handled.
    // @param res_type: The type of result.
    // @param msg: The message associated with the result.
    // @param logging_file: The module type that will be used to name the log file.
    // @param next_task: An optional task that can be executed after the result is handled
    // @param obs: An observer that can be registered after the result is handled.
    // @return A new Result with the given parameters.
    pub fn new_obs_result(res_type: &str , msg: String , logging_file: &str , next_task: Option<Task> , obs: Box<dyn Observer>) -> Self{
        Result {
            result_type: NamedInput::new(res_type),
            message: msg,
            log_msg_to_file: NamedInput::new(logging_file),
            next_task: next_task,
            observer: Some(obs),
            plugin: None,
        }
    }

    // Create a new Result with the given parameters.
    // This constructor is used for results that need to return a plugin to be registered after the result is handled.
    // It can also return a task to be executed after the result is handled.
    // @param res_type: The type of result.
    // @param msg: The message associated with the result.
    // @param logging_file: The module type that will be used to name the log file.
    // @param next_task: An optional task that can be executed after the result is handled.
    // @param plugin: A plugin that can be registered after the result is handled.
    // @return A new Result with the given parameters.
    pub fn new_plugin_result(res_type: &str , msg: String , logging_file: &str , next_task: Option<Task> , plugin: Box<dyn Plugin>) -> Self{
        Result {
            result_type: NamedInput::new(res_type),
            message: msg,
            log_msg_to_file: NamedInput::new(logging_file),
            next_task: next_task,
            observer: None,
            plugin: Some(plugin),
        }
    }
}