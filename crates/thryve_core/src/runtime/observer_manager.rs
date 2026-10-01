// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

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
//! The manager's [`ThreadPool`] never spawns threads by worker count for
//! this pool: the sorter thread ([`init_obs_sorter_thread`](ObserverManager::init_obs_sorter_thread))
//! and each observer's thread ([`load_observer`](ObserverManager::load_observer))
//! are each spawned individually, one at a time, as they're added. Its own
//! main queue has exactly one consumer -- the sorter thread -- so it's sized
//! accordingly.

use thryve_utils::bases::notif::Notif;
use thryve_utils::bases::observer::Observer;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::errors::{NameInputType, StartupError, ThryveError};
use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::packets::notif_log::Log;
use thryve_utils::queues::seg_queue::QChild;
use thryve_utils::threads::threadpool::ThreadPool;
use thryve_utils::traits::queue::Queue;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::{self, JoinHandle};

/// Manager for observers in the Thryve runtime.
///
/// The `ObserverManager` is responsible for managing observers and their
/// associated threads. It maintains a mapping of observer types to their
/// instances and manages a thread pool for processing notifications.
///
/// # Fields
///
/// - `observers`: Mapping of observer types to instances
/// - `st_handle`: The notification sorter's thread, once it's started
/// - `threadpool`: Thread pool for processing notifications
///
/// # Thread Pool Sizing
///
/// Threads are spawned individually as observers are added, not by a worker
/// count: see the [module docs](self) for why `num_workers` is unused for
/// this particular pool.
///
/// # Example
///
/// ```ignore
/// let mut manager = ObserverManager::new(&ctx);
/// manager.load_observer(observer)?;
/// manager.forward_notif(notification);
/// ```
pub struct ObserverManager {
    /// Mapping of observer types to instances.
    observers: HashMap<NamedInput, Arc<Observer>>,

    /// The notification sorter's thread, once it's started. It's kept apart from the thread
    /// pool's observer threads because [`finalize`](Self::finalize) joins the sorter before it
    /// stops the observers, while the observer threads are joined only at the very end.
    st_handle: Option<JoinHandle<()>>,

    /// Thread pool for processing notifications.
    pub threadpool: ThreadPool<QChild, Notif>,
}

impl ObserverManager {
    /// Creates a new observer manager.
    ///
    /// Initializes a new observer manager with the given context. Its main
    /// queue has exactly one consumer -- the sorter thread, started
    /// separately via [`init_obs_sorter_thread`](Self::init_obs_sorter_thread)
    /// -- so it's sized to 1, not to a worker count.
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
    /// ```ignore
    /// let manager = ObserverManager::new(&ctx);
    /// ```
    pub fn new(ctx: &Ctx) -> Self {
        let queue = QChild::new(1, ctx.packet_handle());
        let num_workers = 0;

        ObserverManager {
            observers: HashMap::new(),
            st_handle: None,
            threadpool: ThreadPool::new(num_workers, queue, ctx.packet_handle()),
        }
    }

    /// Initializes the observer sorter thread.
    ///
    /// Spawns a thread responsible for sorting incoming notifications
    /// and forwarding them to the correct observer queues based on
    /// the notification's receiver type. The thread exits gracefully
    /// when the engine signals for shutdown.
    ///
    /// The sorter copies the observer map when it starts, so load every observer first.
    /// A notification for an observer that doesn't exist is logged to `Error.log` (when the
    /// logger exists) and dropped, never a panic.
    ///
    /// # Errors
    ///
    /// Returns [`StartupError::ThreadInit`] if the sorter thread can't be started.
    ///
    /// # Example
    ///
    /// ```ignore
    /// manager.init_obs_sorter_thread()?;
    /// // Sorter thread now routing notifications
    /// ```
    pub fn init_obs_sorter_thread(&mut self) -> Result<(), ThryveError> {
        let mut arc_observers: HashMap<NamedInput, Arc<Observer>> = HashMap::new();
        let queue = Arc::clone(&self.threadpool.main_queue);
        let exit_flag = Arc::clone(&self.threadpool.pool_exit_flag());

        for observer in self.observers.values() {
            arc_observers.insert(observer.obs_type_input().clone(), Arc::clone(observer));
        }

        let thread_name = "ObsSorterThread".to_string();
        let handle = thread::Builder::new()
            .name(thread_name.clone())
            .spawn(move || {
                loop {
                    if exit_flag.load(Ordering::SeqCst) {
                        // Non-fatal: if logger not found, send to main queue instead
                        let obs_type = NamedInput::new_logging();
                        if let Some(logger) = arc_observers.get(&obs_type) {
                            let notif =
                                Log::log("Obs Sorter Thread Finalized...".to_string(), "Main");
                            logger.obs_queue.push(notif);
                        }

                        while !queue.main_queue.is_empty() {
                            if let Some(obj) = queue.main_queue.pop() {
                                // Non-fatal: if target observer not found, skip the notification
                                if let Some(target_obs) = arc_observers.get(obj.notif_receiver()) {
                                    target_obs.obs_queue.push(obj);
                                } else {
                                    // Log the missing observer notification
                                    let notif = Log::log(
                                        format!(
                                            "Observer not found for notification: {}",
                                            obj.notif_receiver()
                                        ),
                                        "Error",
                                    );
                                    let obs_type = NamedInput::new_logging();
                                    if let Some(logger) = arc_observers.get(&obs_type) {
                                        logger.obs_queue.push(notif);
                                    }
                                }
                            }
                        }

                        break;
                    }

                    let obj = queue.pop();

                    // pop() can wake here because the queue was finalized
                    // while this thread was blocked inside it, in which case
                    // it returns a sentinel value rather than a real routed
                    // notification -- discard it instead of forwarding it.
                    if exit_flag.load(Ordering::SeqCst) {
                        continue;
                    }

                    // Non-fatal: if target observer not found, skip the notification
                    if let Some(target_obs) = arc_observers.get(obj.notif_receiver()) {
                        target_obs.obs_queue.push(obj);
                    } else {
                        // Log the missing observer notification
                        let notif = Log::log(
                            format!(
                                "Observer not found for notification: {}",
                                obj.notif_receiver()
                            ),
                            "Error",
                        );
                        let obs_type = NamedInput::new_logging();
                        if let Some(logger) = arc_observers.get(&obs_type) {
                            logger.obs_queue.push(notif);
                        }
                    }
                }
            })
            .map_err(|e| StartupError::ThreadInit {
                thread_name,
                source: e,
            })?;

        self.st_handle = Some(handle);

        Ok(())
    }

    /// Forwards a notification to the main queue.
    ///
    /// Pushes the notification to the main queue for processing by the sorter thread. It
    /// doesn't check the receiver: the sorter does. A notification for an observer that doesn't
    /// exist is logged to `Error.log` (when the logger exists) and dropped. That also covers a
    /// half-built engine being finalized after a failed startup, which may have no logger at all.
    ///
    /// # Arguments
    ///
    /// * `notif` - The notification to forward
    ///
    /// # Example
    ///
    /// ```ignore
    /// manager.forward_notif(notification);
    /// ```
    pub fn forward_notif(&mut self, notif: Notif) {
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
    /// ```ignore
    /// manager.notif_err("Something went wrong");
    /// ```
    pub fn notif_err(&mut self, message: &str) {
        self.threadpool
            .main_queue
            .push(Log::err(message.to_string()));
    }

    /// Loads a new observer into the manager.
    ///
    /// Adds the observer to the observers hashmap, keyed by its type, and
    /// then starts its thread.
    ///
    /// # Arguments
    ///
    /// * `observer` - The observer to load
    ///
    /// # Errors
    ///
    /// Returns [`ThryveError::Startup`] with:
    /// - [`StartupError::RegisterDuplicate`] if an observer of the same type is already loaded.
    ///   Nothing is added or started.
    /// - [`StartupError::ThreadInit`] if the observer's thread can't be started. The observer
    ///   is removed from the hashmap again, so the manager is left as it was.
    ///
    /// # Example
    ///
    /// ```ignore
    /// manager.load_observer(observer)?;
    /// ```
    pub fn load_observer(&mut self, observer: Observer) -> Result<(), ThryveError> {
        let x = Arc::new(observer);
        let obs_type = x.obs_type_input().clone();

        if self.observers.contains_key(&obs_type) {
            return Err(StartupError::RegisterDuplicate {
                name: obs_type.inner().to_string(),
                kind: NameInputType::ObsType,
            }
            .into());
        }

        self.observers.insert(obs_type.clone(), x.clone());

        let thread_spawn_res = self.threadpool.run_thread_obs(x.clone());
        if let Err(e) = thread_spawn_res {
            self.observers.remove(&obs_type);

            return Err(e.into());
        }

        Ok(())
    }

    /// Finalizes the observer manager.
    ///
    /// In order, it:
    ///
    /// 1. takes the logger out of the registry and sets its exit flag;
    /// 2. finalizes the thread pool, which tells the sorter to stop and wakes it;
    /// 3. joins the sorter, so every notification it routes reaches its observer's queue
    ///    before the observers stop;
    /// 4. cleans up every other observer, which tells its thread to stop and sends its queue's
    ///    final line to the logger;
    /// 5. writes a final line to the logger.
    ///
    /// The observer threads aren't joined here: [`Engine::finalize`] joins them once it has
    /// stopped the logger too. On a half-built engine, the parts that never started (the
    /// sorter, the logger) are skipped.
    ///
    /// [`Engine::finalize`]: crate::runtime::engine::Engine::finalize
    ///
    /// # Returns
    ///
    /// The logger observer for final logging operations, or `None` if it was never loaded
    /// (a startup that failed before the logger started).
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(logger) = manager.finalize() {
    ///     logger.obs_queue.push(final_notification);
    /// }
    /// ```
    pub fn finalize(&mut self) -> Option<Arc<Observer>> {
        let obs_type = NamedInput::new_logging();
        let logger = self.observers.remove(&obs_type);

        if let Some(logger) = &logger {
            logger.clone().exit_flag.store(true, Ordering::SeqCst);
        }

        self.threadpool.finalize_obs(logger.clone());

        // A sorter that panicked was already reported by the panic hook. Its join error is
        // dropped, so that the shutdown goes on.
        if let Some(handle) = self.st_handle.take() {
            let _ = handle.join();
        }

        if !self.observers.is_empty() {
            for (_, observer) in self.observers.drain() {
                observer.clean_up(logger.clone());
            }
        }

        if let Some(logger) = &logger {
            let notif = Log::log("Observer manager Finalized".to_string(), "Main");
            logger.obs_queue.push(notif);
        }

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
    /// ```ignore
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
