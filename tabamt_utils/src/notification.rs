// This is the notification system for logging and other events.
// It will be used by all modules to send notifications to any observers created.

use std::{time::SystemTime};
use chrono::{DateTime , Local};
use std::fs::File;
use std::io::Write;

use crate::task::Task;
use crate::any_naming_input::ANNamingInput;

//// To be changed from enum in order to support developer freedom.
pub enum NotifEvent{
    Log { message: String , logfile: ANNamingInput, task: Option<Task> , notification: Option<Box<Notification>>}
}

// This struct will be used to send notifications to any observers created.
pub struct Notification {
    event_type: String,         // The type of event                        | Read Only
    timestamp: SystemTime,      // The time the event was created           | Read Only
    send_to: ANNamingInput,        // The module to send the notification to   | Read Only
    event: NotifEvent,          // The event itself                         | Read Only
}

impl Notification{
    // Functions to get the values of the struct.
    
    // Function to get the event type.
    // @return a reference to a string.
    pub fn get_event_type(&self) -> &str{
        &self.event_type
    }

    // Function to get the timestamp.
    // @return a reference to the system time.
    pub fn get_time_stamp(&self) -> &SystemTime{
        &self.timestamp
    }

    // Function to get the module to send the notification to.
    // @return a reference to ANNamingInput.
    pub fn get_send_to(&self) -> &ANNamingInput{
        &self.send_to
    }

    // Function to get the event itself.
    // @return a reference to NotifEvent.
    pub fn get_event(&self) -> &NotifEvent{
        &self.event
    }
}

impl Notification {
    // This function will create a new notification.
    // @param evt_type: The type of event.
    // @param send_notif_to: The module to send the notification to.
    // @param evt: The event itself.
    // @return a new Notification struct.
    pub fn new(evt_type: String , send_notif_to:ANNamingInput  , evt: NotifEvent) -> Self{
        Notification {
            event_type: evt_type,
            timestamp: SystemTime::now(),
            send_to: send_notif_to,
            event: evt,
        }
    }

    // Function to create a log notification.
    // This is a shortcut function to create a log notification without needing to specify all parameters from "new".
    // @param message: The log message.
    // @param logfile: The module to log the message to.
    // @return a new Notification struct.
    pub fn log(message: String , logfile: ANNamingInput) -> Self{
        Notification {
            event_type: "Log".to_string(),
            timestamp: SystemTime::now(),
            send_to: ANNamingInput::new("Logging"),
            event: NotifEvent::Log { message: message, logfile: logfile, task: None, notification: None },
        }
    }

    // Function to create an error notification.
    // This is a shortcut function to create an error notification without needing to specify all parameters from "new".
    // @param message: The error message.
    // @return a new Notification struct.
    pub fn err(message: String) -> Self{
        Notification {
            event_type: "Error".to_string(),
            timestamp: SystemTime::now(),
            send_to: ANNamingInput::new("Logging"),
            event: NotifEvent::Log { message: message, logfile: ANNamingInput::new("Error"), task: None, notification: None },
        }
    }

    // Function to display the notification to a file.
    // @param file: A mutable reference to a file.
    pub fn display(&self, file: &mut File) {
        match &self.event {
            NotifEvent::Log { message, logfile:_ , task:_, notification:_ } => {
                let datetime: DateTime<Local> = self.timestamp.clone().into();
                writeln!(file , "Log_Creation: {}, [ LOGGING ], Event_Type: {}, Message: {}" , datetime.format("%Y-%m-%d %H:%M:%S") , self.event_type , message).unwrap();
            }
        }
    }
}