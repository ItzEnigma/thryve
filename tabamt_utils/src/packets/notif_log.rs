// This is the notification system for logging.
// It will be used by any module to send log notifications to the logging observer.

use std::sync::Arc;
use std::{time::SystemTime};
use chrono::{DateTime , Local};
use std::fs::File;
use std::io::Write;

use crate::bases::notif::Notif;
use crate::impl_as_any;
use crate::traits::notif_logic::NotifLogic;
use crate::extras::named_input::NamedInput;

// This struct will be used to send notifications to any observers created.
pub struct Log {
    message: String,                        // The log message                          | Read Only
    logfile: NamedInput,                 // The module to log the message to         | Read Only
}

impl NotifLogic for Log{
    // Functions to get the values of the struct.
    impl_as_any!(Log);
    // Function to get the struct as Any.
    // @return a reference to dyn Any.
}

impl Log {
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
}

impl Log {
    // Function to create a log notification.
    // This is a shortcut function to create a log notification without needing to specify all parameters from "new".
    // @param message: The log message.
    // @param logfile: The module to log the message to.
    // @return a new Log struct.
    pub fn log(message: String , logfile: &str) -> Notif{
        let temp = Box::new(Log {
            message: message,
            logfile: NamedInput::new(logfile),
        });

        Notif::new("Log", "Logging", temp)
    }

    // Function to create an error notification.
    // This is a shortcut function to create an error notification without needing to specify all parameters from "new".
    // @param message: The error message.
    // @return a new Log struct.
    pub fn err(message: String) -> Notif{
        let temp = Box::new(Log {
            message: message,
            logfile: NamedInput::new("Error"),
        });

        Notif::new("Error", "Logging",temp)
    }

    // Function to write the notification to a file.
    // @param file: A mutable reference to a file.
    pub fn display(&self, timestamp: SystemTime , notif_type: &str , file: Arc<File>) {
        let datetime: DateTime<Local> = timestamp.clone().into();
        writeln!(&*file , "Log_Creation: {}, [ LOGGING ], Event_Type: {}, Message: {}" , datetime.format("%Y-%m-%d %H:%M:%S") , notif_type , self.message).unwrap();
    }
}