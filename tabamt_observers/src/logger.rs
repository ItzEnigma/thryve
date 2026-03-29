// This is a simple logger observer that logs to files based on module type.
// It is an observer that can be used by any module to log messages.
// It creates a log directory with a timestamp and creates log files for each module type.

use std::{collections::HashMap, env::current_exe, fs::{File, OpenOptions, create_dir_all}, sync::{Arc, atomic::Ordering}, time::SystemTime};

use chrono::{ DateTime , Local };

use tabamt_utils::{bases::notif::Notif, impl_as_any};
use tabamt_utils::bases::observer::Observer;
use tabamt_utils::packets::{notif_log::Log , packet_handle::PacketHandle};
use tabamt_utils::extras::{jobs::ObsJob, named_input::NamedInput};
use tabamt_utils::traits::{observer_logic::ObsLogic , queue::Queue};

// The logger observer struct.
// Holds the log files and the logging type.
pub struct LoggerObserver{
    _currentdir: String,                    // Current directory of the executable.         | read only
    files: HashMap<NamedInput , Arc<File>>,   // HashMap of log files based on module type.   | no permission
}

impl ObsLogic for LoggerObserver {
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
    // Create a new logger observer.
    // An instance of this observer should be created once and stored within the observer manager.
    // @param file_names - A vector of module types to create log files for.
    // @return - A result indicating success or failure.
    pub fn new(file_names: Vec<NamedInput> , tx: PacketHandle) -> Observer {

        println!("{:?}" , file_names);
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
        let mut temp_hash_map: HashMap<NamedInput, Arc<File>> = HashMap::new();
        // create the main log file and insert it into the hashmap. This file will always be created.
        let log_file_path_main = log_dir_path.join("Main.log");
        temp_hash_map.insert(NamedInput::new("Main"), Arc::new(OpenOptions::new().create(true).append(true).open(log_file_path_main).unwrap()));

        // create the main log file and insert it into the hashmap. This file will always be created.
        let log_file_path_err = log_dir_path.join("Error.log");
        temp_hash_map.insert(NamedInput::new("Error"), Arc::new(OpenOptions::new().create(true).append(true).open(log_file_path_err).unwrap()));
        
        // insert the other log files.
        for name in file_names {
            let log_file_path = log_dir_path.join(format!("{}.log" , name.inner()));
            // Create log files based on the module types specified by the developer and insert them into the hashmap.
            temp_hash_map.insert(name, Arc::new(OpenOptions::new().create(true).append(true).open(log_file_path).unwrap()));
        }

        // Create the logger observer instance.
        let logger = Arc::new(LoggerObserver {
            _currentdir: exe_dir.file_name().unwrap().to_string_lossy().to_string(),
            files: temp_hash_map,       // The ownership of the files hashmap is moved to the struct.
        });
        
        let logger_core: ObsJob = Box::new(move |logger: Arc<Observer>| {
            let logger_as_any = logger.obs_core.as_any();
            if let Some(logger_core) = logger_as_any.downcast_ref::<LoggerObserver>(){
                logger_core.log(logger.clone());
            }
        });

        Observer::new("Logger","Logging", logger,logger_core , tx)
    }

    // Log a notification to the appropriate file.
    // @param notif - The notification to log.
    fn log(&self , observer: Arc<Observer>) {
        loop {
            if observer.exit_flag.load(Ordering::Relaxed) == true {
                let notif = Log::log("Logger Obs Thread Exited...".to_string(), "Main");
                observer.obs_queue.push(notif);
                break;
            }

            let notif = observer.obs_queue.pop();
            
            self.handle_log(notif);
        }

    }

    fn handle_log(&self , notif: Notif){
        // Attempt to downcast the notification to a Log.
            let notif_as_any = notif.notif_core.as_any();
            let notif_casted = notif_as_any.downcast_ref::<Log>();
            let notif_type = notif.notif_type.inner();
            let timestamp = notif.timestamp;

            if let Some(notif) = notif_casted {
                let logfile = notif.logfile();
                // Get the file from the hashmap based on the module type.
                let file = self.files.get(logfile);

                // If the file does not exist, PANIC because the file chosen should always exist.
                if file.is_none() {
                    assert!(false, "Logger Observer Cannot Find Requested Log File. Got {}" , logfile);
                }
                // If the file exists, log the notification to the file.
                notif.display(timestamp , notif_type , file.unwrap().clone());
            } else {
                // If the notification cannot be casted, PANIC because the logger only supports Log notifications.
                assert!(false, "Logger Observer Received Unsupported Notification Type.");
            }
    }
}