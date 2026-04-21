


use std::sync::Arc;
use std::{time::SystemTime};
use chrono::{DateTime , Local};
use std::fs::File;
use std::io::Write;

use crate::bases::notif::Notif;
use crate::impl_as_any;
use crate::traits::notif_logic::NotifLogic;
use crate::extras::named_input::NamedInput;

/// Represents core logic for a notification.
/// It can be used to log messages to a file.
pub struct Log {
    pub message: String,                        
    pub logfile: NamedInput,                 
}

impl NotifLogic for Log{    
    impl_as_any!(Log);
}

impl Log {
    /// Creates a new Log notification with the given `message` and `logfile` name.
    pub fn log(message: String , logfile: &str) -> Notif{
        let temp = Box::new(Log {
            message: message,
            logfile: NamedInput::new(logfile),
        });

        Notif::new("Log", "Logging", temp)
    }

    /// Creates a new Log notification with the given error `message` and a default `logfile` called "Error".
    /// This is typically used to log error messages, allowing for easier identification and separation of error logs from regular logs.
    pub fn err(message: String) -> Notif{
        let temp = Box::new(Log {
            message: message,
            logfile: NamedInput::new("Error"),
        });

        Notif::new("Error", "Logging",temp)
    }
    
    /// Takes a `timestamp`, notification type, and a `file` handle and writes the log message to the file in a formatted manner.
    pub fn display(&self, timestamp: SystemTime , notif_type: &str , file: Arc<File>) {
        let datetime: DateTime<Local> = timestamp.clone().into();
        writeln!(&*file , "Log_Creation: {}, [ LOGGING ], Event_Type: {}, Message: {}" , datetime.format("%Y-%m-%d %H:%M:%S") , notif_type , self.message).unwrap();
    }
}