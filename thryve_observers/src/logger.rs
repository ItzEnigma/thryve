//! This is a simple logger observer that logs to files based on module type.
//! It is an observer that can be used by any module to log messages.
//! It creates a log directory with a timestamp and creates log files for each module type.

use std::{collections::HashMap, env::current_exe, fs::{File, OpenOptions, create_dir_all}, sync::{Arc, atomic::Ordering}, time::SystemTime};

use chrono::{ DateTime , Local };

use thryve_utils::{bases::notif::Notif, impl_as_any};
use thryve_utils::bases::observer::Observer;
use thryve_utils::packets::{notif_log::Log , packet_handle::PacketHandle};
use thryve_utils::extras::{jobs::ObsJob, named_input::NamedInput};
use thryve_utils::traits::{observer_logic::ObsLogic , queue::Queue};

/// The `LoggerObserver` struct is the core logic for the logger observer.
/// It implements the `ObsLogic` trait, which defines the behavior of the observer.
/// It contains a hashmap of log files for each module type and a method to handle incoming notifications and log them to the appropriate files.
pub struct LoggerObserver{
    _currentdir: String,                    
    files: HashMap<NamedInput , Arc<File>>,   
}

impl ObsLogic for LoggerObserver {
    /// The `clean_up` method is responsible for cleaning up the observer when it is being shut down.
    /// It logs a message indicating that the observer is cleaning up and then pops all remaining notifications from the observer's queue.
    fn clean_up(&self , observer: Arc<Observer>) {
        let notif = Log::log("Logger Obs Cleaning Up...".to_string(), "Main");
        self.handle_log(notif);

        while observer.obs_queue.main_queue.len() != 0 {
            let notif = observer.obs_queue.pop();
            self.handle_log(notif);
        };
    }

    impl_as_any!(LoggerObserver);
}

impl LoggerObserver {
    /// The `new` function creates a new instance of the `LoggerObserver`, initializing its components and setting up the log files.
    /// It takes a vector of `NamedInput` representing the module types for which log files should be created and a `PacketHandle` for communication.
    /// It creates a log directory with a timestamp and creates log files for each supplied file name.
    /// It also creates a main log file and an error log file.
    /// It returns an instance of the `Observer` struct that encapsulates the logger observer's logic and functionality.
    // An instance of this observer should be created only once and stored within the observer manager.
    pub fn new(file_names: Vec<NamedInput> , tx: PacketHandle) -> Observer {
        
        // Get the current executable's directory to create the log directory there.
        let exe_dir = current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();

        // Create a log directory with a timestamp to store the log files.
        let datetime: DateTime<Local> = SystemTime::now().clone().into();
        let log_dir_path = exe_dir.join("log").join(datetime.format("%Y_%m_%d_%H_%M").to_string());
        create_dir_all(&log_dir_path).unwrap();

        // Create log files for each module type and store them in a hashmap for easy access.
        let mut temp_hash_map: HashMap<NamedInput, Arc<File>> = HashMap::new();
        
        // Create a main log file and an error log file for general logging and error logging respectively.
        let log_file_path_main = log_dir_path.join("Main.log");
        temp_hash_map.insert(NamedInput::new("Main"), Arc::new(OpenOptions::new().create(true).append(true).open(log_file_path_main).unwrap()));

        
        let log_file_path_err = log_dir_path.join("Error.log");
        temp_hash_map.insert(NamedInput::new("Error"), Arc::new(OpenOptions::new().create(true).append(true).open(log_file_path_err).unwrap()));
        
        // Create log files for each supplied file name and store them in the hashmap.
        for name in file_names {
            let log_file_path = log_dir_path.join(format!("{}.log" , name.inner()));
            
            temp_hash_map.insert(name, Arc::new(OpenOptions::new().create(true).append(true).open(log_file_path).unwrap()));
        }

        // Create an instance of the `LoggerObserver` struct with the initialized log files and return an `Observer` instance that encapsulates its logic.
        let logger = Arc::new(LoggerObserver {
            _currentdir: exe_dir.file_name().unwrap().to_string_lossy().to_string(),
            files: temp_hash_map,       
        });
        
        let logger_core: ObsJob = Box::new(move |logger: Arc<Observer>| {
            let logger_as_any = logger.obs_core.as_any();
            if let Some(logger_core) = logger_as_any.downcast_ref::<LoggerObserver>(){
                logger_core.log(logger.clone());
            }
        });

        // Return a new observer instance with the specified type and name.
        Observer::new("Logger","Logging", logger,logger_core , tx)
    }

    
    // The `log` method is the main loop for the logger observer's thread.
    // It continuously checks for incoming notifications in the observer's queue.
    // It then handles each notification by calling the `handle_log` method to log the notification.
    fn log(&self , observer: Arc<Observer>) {
        loop {
            // Check if the exit flag is set to true, indicating that the engine is shutting down.
            // If so, log a message and break the loop to exit the thread gracefully.
            if observer.exit_flag.load(Ordering::Relaxed) == true {
                let notif = Log::log("Logger Obs Thread Exited...".to_string(), "Main");
                observer.obs_queue.push(notif);
                break;
            }

            // Pop a notification from the observer's queue and handle it by calling the `handle_log` method.
            let notif = observer.obs_queue.pop();
            
            self.handle_log(notif);
        }

    }

    /// The `handle_log` method processes supplied notifications and logs them to the appropriate files based on their type.
    fn handle_log(&self , notif: Notif){
        
            // Attempt to downcast the notification's core logic to a `Log` type to access its specific fields and functionality.
            let notif_as_any = notif.notif_core.as_any();
            let notif_casted = notif_as_any.downcast_ref::<Log>();

            // Extract the notification type and timestamp for logging purposes.
            let notif_type = notif.notif_type.inner();
            let timestamp = notif.timestamp;

            // If the notification is successfully downcasted to a `Log` type,
            //extract the log file name from the notification and log the message to the appropriate file.
            if let Some(notif) = notif_casted {
                let logfile = &notif.logfile;
                
                let file = self.files.get(logfile);

                // If the specified log file does not exist in the hashmap, panic with an error message.
                if file.is_none() {
                    assert!(false, "Logger Observer Cannot Find Requested Log File. Got {}" , logfile);
                }
                
                // Log the message to the appropriate file by calling the `display` method of the `Log` notification.
                notif.display(timestamp , notif_type , file.unwrap().clone());
            } 

            // If the notification cannot be downcasted to a `Log` type, it means that the logger observer received an unsupported notification type.
            else { 
                assert!(false, "Logger Observer Received Unsupported Notification Type.");
            }
    }
}