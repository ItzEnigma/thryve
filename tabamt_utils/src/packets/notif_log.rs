// This is the notification system for logging.
// It will be used by any module to send log notifications to the logging observer.

use std::{time::SystemTime};
use chrono::{DateTime , Local};
use std::fs::File;
use std::io::Write;

use crate::traits::notif::Notif;
use crate::packets::task::Task;
use crate::any_naming_input::NamedInput;

// This struct will be used to send notifications to any observers created.
pub struct NotifLog {
    event_type: NamedInput,              // The type of event                        | Read Only
    timestamp: SystemTime,                  // The time the event was created           | Read Only
    obs_receiver: NamedInput,                 // The module to send the notification to   | Read Only
    message: String,                        // The log message                          | Read Only
    logfile: NamedInput,                 // The module to log the message to         | Read Only
    task: Option<Task>,                     // The task associated with the log         | Read Only
    notification: Option<Box<NotifLog>>,    // An optional nested notification          | Read Only
}

impl Notif for NotifLog{
    // Functions to get the values of the struct.
    
    // Function to get the struct as Any.
    // @return a reference to dyn Any.
    fn as_any(&self) -> &dyn std::any::Any{
        self
    }

    // Function to get the event type.
    // @return a reference to a NamedInput.
    fn event_type(&self) -> &NamedInput{
        &self.event_type
    }

    // Function to get the timestamp.
    // @return a reference to the system time.
    fn time_stamp(&self) -> &SystemTime{
        &self.timestamp
    }

    // Function to get the module to send the notification to.
    // @return a reference to NamedInput.
    fn obs_receiver(&self) -> &NamedInput{
        &self.obs_receiver
    }
}

impl NotifLog {
    // Functions to get the values of the struct.

    // Function to get the message.
    // @return a reference to the message string.
    pub fn message(&self) -> &String{
        &self.message
    }

    // Function to get the logfile.
    // @return a reference to NamedInput.
    pub fn logfile(&self) -> &NamedInput{
        &self.logfile
    }

    // Function to get the task.
    // @return a reference to an Option<Task>.
    pub fn task(&self) -> &Option<Task>{
        &self.task
    }

    // Function to get the notification.
    // @return a reference to an Option<Box<NotifLog>>.
    pub fn notification(&self) -> &Option<Box<NotifLog>>{
        &self.notification
    }
}

impl NotifLog {
    // Function to create a log notification.
    // This is a shortcut function to create a log notification without needing to specify all parameters from "new".
    // @param message: The log message.
    // @param logfile: The module to log the message to.
    // @return a new NotifLog struct.
    pub fn log(message: String , logfile: &str) -> Box<Self>{
        Box::new(NotifLog {
            event_type: NamedInput::new("Log"),
            timestamp: SystemTime::now(),
            obs_receiver: NamedInput::new("Logging"),
            message: message,
            logfile: NamedInput::new(logfile),
            task: None,
            notification: None,
        })
    }

    // Function to create an error notification.
    // This is a shortcut function to create an error notification without needing to specify all parameters from "new".
    // @param message: The error message.
    // @return a new NotifLog struct.
    pub fn err(message: String) -> Box<Self>{
        Box::new(NotifLog {
            event_type: NamedInput::new("Error"),
            timestamp: SystemTime::now(),
            obs_receiver: NamedInput::new("Logging"),
            message: message,
            logfile: NamedInput::new("Error"),
            task: None,
            notification: None,
        })
    }

    // Function to write the notification to a file.
    // @param file: A mutable reference to a file.
    pub fn display(&self, file: &mut File) {
        let datetime: DateTime<Local> = self.timestamp.clone().into();
        writeln!(file , "Log_Creation: {}, [ LOGGING ], Event_Type: {}, Message: {}" , datetime.format("%Y-%m-%d %H:%M:%S") , self.event_type , self.message).unwrap();
    }
}