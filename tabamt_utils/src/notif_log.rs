// This is the notification system for logging and other events.
// It will be used by all modules to send notifications to any observers created.

use std::{time::SystemTime};
use chrono::{DateTime , Local};
use std::fs::File;
use std::io::Write;

use crate::notif::Notif;
use crate::task::Task;
use crate::any_naming_input::ANNamingInput;

// This struct will be used to send notifications to any observers created.
pub struct NotifLog {
    event_type: ANNamingInput,         // The type of event                        | Read Only
    timestamp: SystemTime,      // The time the event was created           | Read Only
    send_to: ANNamingInput,        // The module to send the notification to   | Read Only
    message: String,
    logfile: ANNamingInput,
    task: Option<Task>,
    notification: Option<Box<NotifLog>>,
}

impl Notif for NotifLog{
    // Functions to get the values of the struct.
    
    fn get_as_any(&self) -> &dyn std::any::Any{
        self
    }

    // Function to get the event type.
    // @return a reference to a string.
    fn get_event_type(&self) -> &ANNamingInput{
        &self.event_type
    }

    // Function to get the timestamp.
    // @return a reference to the system time.
    fn get_time_stamp(&self) -> &SystemTime{
        &self.timestamp
    }

    // Function to get the module to send the notification to.
    // @return a reference to ANNamingInput.
    fn get_send_to(&self) -> &ANNamingInput{
        &self.send_to
    }
}

impl NotifLog {
    pub fn get_message(&self) -> &String{
        &self.message
    }

    pub fn get_logfile(&self) -> &ANNamingInput{
        &self.logfile
    }

    pub fn get_task(&self) -> &Option<Task>{
        &self.task
    }

    pub fn get_notification(&self) -> &Option<Box<NotifLog>>{
        &self.notification
    }
}

impl NotifLog {
    // This function will create a new notification.
    // @param evt_type: The type of event.
    // @param send_notif_to: The module to send the notification to.
    // @param evt: The event itself.
    // @return a new NotifLog struct.
    pub fn new(evt_type: ANNamingInput , send_notif_to:ANNamingInput  , message: String, logfile: ANNamingInput, task: Option<Task>, notification: Option<Box<NotifLog>>) -> Self{
        NotifLog {
            event_type: evt_type,
            timestamp: SystemTime::now(),
            send_to: send_notif_to,
            message: message,
            logfile: logfile,
            task: task,
            notification: notification,
        }
    }

    // Function to create a log notification.
    // This is a shortcut function to create a log notification without needing to specify all parameters from "new".
    // @param message: The log message.
    // @param logfile: The module to log the message to.
    // @return a new NotifLog struct.
    pub fn log(message: String , logfile: ANNamingInput) -> Self{
        NotifLog {
            event_type: ANNamingInput::new("Log"),
            timestamp: SystemTime::now(),
            send_to: ANNamingInput::new("Logging"),
            message: message,
            logfile: logfile,
            task: None,
            notification: None,
        }
    }

    // Function to create an error notification.
    // This is a shortcut function to create an error notification without needing to specify all parameters from "new".
    // @param message: The error message.
    // @return a new NotifLog struct.
    pub fn err(message: String) -> Self{
        NotifLog {
            event_type: ANNamingInput::new("Error"),
            timestamp: SystemTime::now(),
            send_to: ANNamingInput::new("Logging"),
            message: message,
            logfile: ANNamingInput::new("Error"),
            task: None,
            notification: None,
        }
    }

    // Function to display the notification to a file.
    // @param file: A mutable reference to a file.
    pub fn display(&self, file: &mut File) {
        let datetime: DateTime<Local> = self.timestamp.clone().into();
        writeln!(file , "Log_Creation: {}, [ LOGGING ], Event_Type: {}, Message: {}" , datetime.format("%Y-%m-%d %H:%M:%S") , self.event_type , self.message).unwrap();
    }
}