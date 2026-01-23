// This file handles the logic behind results returned by tasks.
// Results can be of different types and can contain different data.

use std::fmt::Display;

// This is just a struct that has results returned by executed tasks.
use crate::{any_naming_input::ANNamingInput, observer::Observer, plugin::Plugin, task::Task};

// The struct that will hold the results returned by tasks.
// It can hold different types of data depending on the type of result.
pub struct Result {
    result_type: ANNamingInput,             // The type of result.                                                          | Take Ownership.
    message: String,                        // A message associated with the result.                                        | Take Ownership.
    log_msg_to_file: ANNamingInput,         // The module type that will be used to name the log file.                      | Take Ownership.
    next_task: Option<Task>,                // An optional task that can be executed after the result is handled.           | Take Ownership.
    observer: Option<Box<dyn Observer>>,    // An optional observer that can be registered after the result is handled.     | Take Ownership.
    plugin: Option<Box<dyn Plugin>>,        // An optional plugin that can be registered after the result is handled.       | Take Ownership.
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
    pub fn new(res_type: ANNamingInput , msg: String , logging_file: ANNamingInput) -> Self{
        Result {
            result_type: res_type,
            message: msg,
            log_msg_to_file: logging_file,
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
    pub fn new_tasked_res(res_type: ANNamingInput , msg: String , logging_file: ANNamingInput , next_task: Option<Task>) -> Self{
        Result {
            result_type: res_type,
            message: msg,
            log_msg_to_file: logging_file,
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
    pub fn new_obs_result(res_type: ANNamingInput , msg: String , logging_file: ANNamingInput , next_task: Option<Task> , obs: Box<dyn Observer>) -> Self{
        Result {
            result_type: res_type,
            message: msg,
            log_msg_to_file: logging_file,
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
    pub fn new_plugin_result(res_type: ANNamingInput , msg: String , logging_file: ANNamingInput , next_task: Option<Task> , plugin: Box<dyn Plugin>) -> Self{
        Result {
            result_type: res_type,
            message: msg,
            log_msg_to_file: logging_file,
            next_task: next_task,
            observer: None,
            plugin: Some(plugin),
        }
    }
}

impl Result {
    // Take ownership of the message inside the struct and return it.
    // This will ensure that the data is not used after it has been taken.
    // @return The ANNamingInput that will be used to get the type of result.
    pub fn res_type(&mut self) -> ANNamingInput {
        let result_type = std::mem::replace(&mut self.result_type , ANNamingInput::new("")); 
        
        result_type
    }


    // The functions below will take the ownership of the data inside the struct and return it.
    // This is done to ensure that the data is not used after it has been taken.
    // @return The Task if it exists, None otherwise.
    pub fn next_task(&mut self) -> Option<Task>{
        let task = std::mem::replace(&mut self.next_task, None);

        task
    }

    // Take ownership of the observer inside the struct and return it.
    // This will ensure that the data is not used after it has been taken.
    // @return The Observer if it exists, None otherwise.
    pub fn obs(&mut self) -> Option<Box<dyn Observer>>{
        let obs = std::mem::replace(&mut self.observer , None); 
        
        obs
    }
    
    // Take ownership of the plugin inside the struct and return it.
    // This will ensure that the data is not used after it has been taken.
    // @return The Plugin if it exists, None otherwise.
    pub fn plugin(&mut self) -> Option<Box<dyn Plugin>>{
        let plugin = std::mem::replace(&mut self.plugin , None); 
        
        plugin
    }

    // Take ownership of the message inside the struct and return it.
    // This will ensure that the data is not used after it has been taken.
    // @return The message as a String.
    pub fn message(&mut self) -> String {
        let message = std::mem::replace(&mut self.message , "".to_string()); 
        
        message
    }

    // Take ownership of the file name inside the struct and return it.
    // This will ensure that the data is not used after it has been taken.
    // @return The ANNamingInput that will be used to name the log file.
    pub fn logging_file(&mut self) -> ANNamingInput {
        let file_name = std::mem::replace(&mut self.log_msg_to_file , ANNamingInput::new("")); 
        
        file_name
    }
}