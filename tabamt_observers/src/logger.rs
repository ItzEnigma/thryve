// This is a simple logger observer that logs to files based on module type.
// It is an observer that can be used by any module to log messages.
// It creates a log directory with a timestamp and creates log files for each module type.

use std::{collections::HashMap, env::current_exe, fs::{create_dir_all, File, OpenOptions}, time::SystemTime};

use chrono::{ DateTime , Local };
use tabamt_utils::{any_naming_input::ANNamingInput, notif_log::NotifLog, notif::Notif , observer::Observer, res};

// The logger observer struct.
// Holds the log files and the logging type.
pub struct LoggerObserver{
    observer_name: String,                  // Name of the observer.                        | read only
    observer_type: ANNamingInput,           // Type of the observer.                        | read only

    _currentdir: String,                    // Current directory of the executable.         | read only

    files: HashMap<ANNamingInput , File>,   // HashMap of log files based on module type.   | no permission
}

// Implement the Observer trait for the LoggerObserver.
impl Observer for LoggerObserver {
    // Consume a notification and log it to the appropriate file.
    // @param notif - The notification to consume.
    fn consume(&mut self , notif: Box<dyn Notif>) {
        self.log(notif);
    }

    // Get the name of the observer.
    // @return - The name of the observer.
    fn observer_name(&self) -> &str {
        &self.observer_name
    }

    // Get the type of the observer.
    // @return - The type of the observer.
    fn observer_type(&self) -> &ANNamingInput {
        &self.observer_type
    }
}

impl LoggerObserver {
    // Create a new logger observer.
    // An instance of this observer should be created once and stored within the observer manager.
    // @param file_names - A vector of module types to create log files for.
    // @return - A result indicating success or failure.
    pub fn new(file_names: Vec<ANNamingInput>) -> res::Result {
        // Get the current executable directory.
        let exe_dir = current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

        // Create a log directory with a timestamp.
        let datetime: DateTime<Local> = SystemTime::now().clone().into();
        let log_dir_path = exe_dir.join("log").join(datetime.format("%Y_%m_%d_%H_%M").to_string());
        create_dir_all(&log_dir_path).unwrap();

        // Create a hashmap to hold the log files.
        let mut temp_hash_map: HashMap<ANNamingInput, File> = HashMap::new();
        // create the main log file and insert it into the hashmap. This file will always be created.
        let log_file_path_main = log_dir_path.join("Main.log");
        temp_hash_map.insert(ANNamingInput::new("Main"), OpenOptions::new().create(true).append(true).open(log_file_path_main).unwrap());

        // create the main log file and insert it into the hashmap. This file will always be created.
        let log_file_path_err = log_dir_path.join("Error.log");
        temp_hash_map.insert(ANNamingInput::new("Error"), OpenOptions::new().create(true).append(true).open(log_file_path_err).unwrap());
        
        // insert the other log files.
        for name in file_names {
            let log_file_path = log_dir_path.join(format!("{}.log" , name));
            // Create log files based on the module types specified by the developer and insert them into the hashmap.
            temp_hash_map.insert(name, OpenOptions::new().create(true).append(true).open(log_file_path).unwrap());
        }

        // Create the logger observer instance.
        let logger = LoggerObserver {
            observer_name: "Logger".to_string(),
            observer_type: ANNamingInput::new("Logging"),
            _currentdir: exe_dir.file_name().unwrap().to_string_lossy().to_string(),
            files: temp_hash_map,       // The ownership of the files hashmap is moved to the struct.
        };

        // Return the logger observer wrapped in a result.
        res::Result::new_obs_result(
            ANNamingInput::new("ObserverInit"),
            "Loaded Observer - Logger".to_string(),
            ANNamingInput::new("Main"),
            None, 
            Box::new(logger),
        )
    }

    // Log a notification to the appropriate file.
    // @param notif - The notification to log.
    fn log(&mut self , notif: Box<dyn Notif>) {
        // Attempt to downcast the notification to a NotifLog.
        let notif_as_any = notif.as_any();
        let notif_casted = notif_as_any.downcast_ref::<NotifLog>();

        if let Some(notif) = notif_casted {
            let logfile = notif.logfile();
            // Get the file from the hashmap based on the module type.
            let file = self.files.get_mut(logfile);

            // If the file does not exist, PANIC because the file chosen should always exist.
            if file.is_none() {
                assert!(false, "Logger Observer Cannot Find Requested Log File. Got {}" , logfile);
            }
            // If the file exists, log the notification to the file.
            notif.display(file.unwrap());
        } else {
            // If the notification cannot be casted, PANIC because the logger only supports NotifLog notifications.
            assert!(false, "Logger Observer Received Unsupported Notification Type.");
        }
    }
}