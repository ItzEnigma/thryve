//! Observer manager for the Thryve runtime.
//!
//! This module provides the [`ObserverManager`] struct, which is responsible
//! for managing observers and their associated threads in the engine. It
//! provides functionality to load observers, forward notifications to the
//! appropriate observer queues, and finalize the observer manager during
//! engine shutdown.
//!
//! # Architecture
//!
//! The observer manager maintains:
//! - A mapping of observer types to their instances
//! - A thread pool for processing notifications
//! - A sorter thread that routes notifications to correct observers
//!
//! # Thread Pool
//!
//! The thread pool is sized to `ctx.observers.len() + 2`, accounting
//! for the sorter thread and the logging observer thread in addition
//! to one thread per observer.

use thryve_utils::bases::notif::Notif;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::bases::observer::Observer;
use thryve_utils::packets::notif_log::Log;
use thryve_utils::queues::seg_queue::QChild;
use thryve_utils::threads::threadpool::ThreadPool;
use thryve_utils::traits::queue::Queue;

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread;
use std::{collections::HashMap};

/// Manager for observers in the Thryve runtime.
///
/// The `ObserverManager` is responsible for managing observers and their
/// associated threads. It maintains a mapping of observer types to their
/// instances and manages a thread pool for processing notifications.
///
/// # Fields
///
/// - `observers`: Mapping of observer types to instances
/// - `threadpool`: Thread pool for processing notifications
///
/// # Thread Pool Sizing
///
/// The thread pool is sized to `ctx.observers.len() + 2`, accounting
/// for the sorter thread and the logging observer thread in addition
/// to one thread per observer.
///
/// # Example
///
/// ```no_run
/// let manager = ObserverManager::new(&ctx);
/// manager.load_observer(observer);
/// manager.forward_notif(notification);
/// ```
pub struct ObserverManager {
    /// Mapping of observer types to instances.
    observers: HashMap<NamedInput, Arc<Observer>>,
    /// Thread pool for processing notifications.
    pub threadpool: ThreadPool<QChild, Notif>,
}

impl ObserverManager {
    /// Creates a new observer manager.
    ///
    /// Initializes a new observer manager with the given context.
    /// The thread pool is created with `ctx.observers.len() + 2` workers,
    /// accounting for the sorter thread, logging observer thread, and
    /// one thread per observer.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The application context
    ///
    /// # Returns
    ///
    /// A new `ObserverManager` instance.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let manager = ObserverManager::new(&ctx);
    /// ```
    pub fn new(ctx: &Ctx) -> Self {
        let queue = QChild::new(ctx.packet_handle());
        let num_workers = ctx.observers.len() + 2;

        ObserverManager {
            observers: HashMap::new(),
            threadpool: ThreadPool::new(num_workers, queue, ctx.packet_handle(), None),
        }
    }

    /// Initializes the observer sorter thread.
    ///
    /// Spawns a thread responsible for sorting incoming notifications
    /// and forwarding them to the correct observer queues based on
    /// the notification's receiver type. The thread exits gracefully
    /// when the engine signals for shutdown.
    ///
    /// # Example
    ///
    /// ```no_run
    /// manager.init_obs_sorter_thread();
    /// // Sorter thread now routing notifications
    /// ```
    pub fn init_obs_sorter_thread(&mut self) {
        let mut arc_observers: HashMap<NamedInput, Arc<Observer>> = HashMap::new();
        let queue = Arc::clone(&self.threadpool.main_queue);
        let exit_flag = Arc::clone(&self.threadpool.pool_exit_flag());

        for (_, observer) in &self.observers {
            arc_observers.insert(NamedInput::new(observer.obs_type.inner()), Arc::clone(observer));
        }

        let handle = thread::Builder::new()
            .name("ObsSorterThread".to_string())
            .spawn(move || {
                loop {
                    if exit_flag.load(Ordering::SeqCst) == true {
                        // Non-fatal: if logger not found, send to main queue instead
                        if let Some(logger) = arc_observers.get(&NamedInput::new("Logging")) {
                            let notif = Log::log("Obs Sorter Thread Finalized...".to_string(), "Main");
                            logger.obs_queue.push(notif);
                        }
                        
                        while !queue.main_queue.is_empty() {
                            if let Some(obj) = queue.main_queue.pop(){
                                // Non-fatal: if target observer not found, skip the notification
                                if let Some(target_obs) = arc_observers.get(&obj.notif_receiver) {
                                    target_obs.obs_queue.push(obj);
                                } else {
                                    // Log the missing observer notification
                                    let notif = Log::log(
                                        format!("Observer not found for notification: {}", obj.notif_receiver.inner()),
                                        "Error"
                                    );
                                    if let Some(logger) = arc_observers.get(&NamedInput::new("Logging")) {
                                        logger.obs_queue.push(notif);
                                    }
                                }
                            }
                        }

                        break;
                    }

                    let obj = queue.pop();
                    // Non-fatal: if target observer not found, skip the notification
                    if let Some(target_obs) = arc_observers.get(&obj.notif_receiver) {
                        target_obs.obs_queue.push(obj);
                    } else {
                        // Log the missing observer notification
                        let notif = Log::log(
                            format!("Observer not found for notification: {}", obj.notif_receiver.inner()),
                            "Error"
                        );
                        if let Some(logger) = arc_observers.get(&NamedInput::new("Logging")) {
                            logger.obs_queue.push(notif);
                        }
                    }
                }
            }
        )
        .unwrap_or_else(|e| panic!("Failed to spawn observer sorter thread: {}", e));

        self.threadpool.add_worker(handle);
    }

    /// Checks if an observer of the specified type exists.
    ///
    /// # Arguments
    ///
    /// * `obs_type` - The observer type to check
    ///
    /// # Returns
    ///
    /// `true` if an observer of the given type exists.
    fn check_observer(&self, obs_type: &NamedInput) -> bool {
        self.observers.contains_key(obs_type)
    }

    /// Forwards a notification to the main queue.
    ///
    /// Validates that the target observer exists, then pushes the
    /// notification to the main queue for processing by the sorter
    /// thread.
    ///
    /// # Arguments
    ///
    /// * `notif` - The notification to forward
    ///
    /// # Panics
    ///
    /// Panics if the target observer does not exist.
    ///
    /// # Example
    ///
    /// ```no_run
    /// manager.forward_notif(notification);
    /// ```
    pub fn forward_notif(&mut self, notif: Notif) {
        assert!(
            self.check_observer(&notif.notif_receiver),
            "Observer Manager Cannot Find Requested Observer: {}.
            Check that the receiver of the notification is written correctly.",
            &notif.notif_receiver
        );

        self.threadpool.main_queue.push(notif);
    }

    /// Sends an error notification to the logging observer.
    ///
    /// Convenience method for logging error messages directly to
    /// the logging observer's queue.
    ///
    /// # Arguments
    ///
    /// * `message` - The error message
    ///
    /// # Example
    ///
    /// ```no_run
    /// manager.notif_err("Something went wrong");
    /// ```
    pub fn notif_err(&mut self, message: &str) {
        self.threadpool.main_queue.push(Log::err(message.to_string()));
    }

    /// Loads a new observer into the manager.
    ///
    /// Starts the observer's thread and adds it to the observers
    /// hashmap, keyed by its type.
    ///
    /// # Arguments
    ///
    /// * `observer` - The observer to load
    ///
    /// # Example
    ///
    /// ```no_run
    /// manager.load_observer(observer);
    /// ```
    pub fn load_observer(&mut self, observer: Observer) {
        let x = Arc::new(observer);
        self.threadpool.run_thread_obs(Arc::clone(&x));
        self.observers.insert(NamedInput::new(x.obs_type.inner()), x);
    }

    /// Finalizes the observer manager.
    ///
    /// Sets the exit flag, finalizes the thread pool, processes
    /// any remaining notifications, and cleans up all observers.
    /// Returns the logger observer for final logging.
    ///
    /// # Returns
    ///
    /// The logger observer for final logging operations.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let logger = manager.finalize();
    /// logger.obs_queue.push(final_notification);
    /// ```
    /// Finalizes the observer manager.
    ///
    /// # Panics
    ///
    /// Panics if the Logging observer is not found in the registry,
    /// which indicates a fatal configuration error.
    pub fn finalize(&mut self) -> Arc<Observer> {
        let logger = self.observers.remove(&NamedInput::new("Logging"))
            .expect("Failed to find Logging observer during finalize - this is a fatal error");
        logger.exit_flag.store(true, Ordering::SeqCst);
        
        self.threadpool.finalize_obs(logger.clone());

        if !self.observers.is_empty() {
            for (_, observer) in self.observers.drain() {
                observer.clean_up();
            }
        }

        let notif = Log::log("Observer manager Finalized".to_string(), "Main");
        logger.obs_queue.push(notif);

        logger
    }
}

impl ObserverManager {
    /// Returns a vector of all observer names.
    ///
    /// # Returns
    ///
    /// A vector of string slices representing observer names.
    ///
    /// # Example
    ///
    /// ```no_run
    /// let names = manager.observer_names();
    /// for name in names {
    ///     println!("Observer: {}", name);
    /// }
    /// ```
    pub fn observer_names(&self) -> Vec<&str> {
        let mut vector: Vec<&str> = Vec::new();
        for obs in self.observers.keys() {
            vector.push(obs.inner());
        }

        vector
    }
}