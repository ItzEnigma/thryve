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
use thryve_utils::threads::threadpool::{TaskSlot, ThreadPool};
use thryve_utils::traits::queue::Queue;

use crossbeam::atomic::AtomicCell;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::thread::{self, JoinHandle};
use std::time::Instant;

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
    /// pool's observer threads because [`finalize`](Self::finalize) joins the sorter before the
    /// observers are stopped; the observer threads are waited for only after their cleanup.
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
    /// the notification's receiver type. Once the pool's exit flag is set, the sorter routes
    /// every notification still in its queue, sends "Obs Sorter Thread Finalized..." to the
    /// logger, and stops.
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
                        break;
                    }

                    let obj_opt = queue.pop();

                    if let Some(obj) = obj_opt {
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
                    } else {
                        break;
                    }
                }

                // The exit flag ends the loop as soon as it's set, even with notifications still
                // queued. The logger hears first, if there is one; this push also wakes its
                // thread, whose exit flag is already set by then.
                let obs_type = NamedInput::new_logging();
                if let Some(logger) = arc_observers.get(&obs_type) {
                    let notif = Log::log("Obs Sorter Thread Finalized...".to_string(), "Main");
                    logger.obs_queue.push(notif);
                }

                // Then everything still queued is routed the same way, so it reaches its
                // observer before the observers' cleanup.
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

    /// Starts finalizing the observer manager: stops the sorter and hands back the logger.
    ///
    /// In order, it:
    ///
    /// 1. takes the logger out of the registry and sets its exit flag, so its thread stops after
    ///    the next line it writes;
    /// 2. sets the sorter's exit flag and wakes it, without emptying its queue;
    /// 3. joins the sorter, which first routes every notification it still holds, so they reach
    ///    their observers' queues before the observers stop;
    /// 4. finalizes the sorter's queue, which is empty by then.
    ///
    /// The other observers are cleaned up next, by
    /// [`finalize_observers`](Self::finalize_observers). No observer thread is joined here:
    /// [`Engine::finalize`] waits for the other observers' threads after their cleanup, and joins
    /// the logger's thread last, once it has handed the logger its final lines. On a half-built
    /// engine, the parts that never started (the sorter, the logger) are skipped.
    ///
    /// [`Engine::finalize`]: crate::runtime::engine::Engine::finalize
    ///
    /// # Arguments
    ///
    /// * `notifs` - Receives the notifications left in the sorter's queue
    /// * `status_reps` - Receives the status lines, which the engine hands to the logger last
    ///
    /// # Returns
    ///
    /// The logger observer for final logging operations, or `None` if it was never loaded
    /// (a startup that failed before the logger started).
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(logger) = manager.finalize(&mut notifs, &mut status_reps) {
    ///     logger.obs_queue.push(final_notification);
    /// }
    /// ```
    pub fn finalize(
        &mut self,
        notifs: &mut Vec<Notif>,
        status_reps: &mut Vec<Notif>,
    ) -> Option<Arc<Observer>> {
        // The logger leaves the registry, so the other observers can be taken out as a group.
        // With its exit flag set, its thread stops after the next line it writes; the engine
        // pushes its final lines later, which makes sure there is one, then joins it.
        let obs_type = NamedInput::new_logging();
        let logger = self.observers.remove(&obs_type);

        if let Some(logger) = &logger {
            logger.clone().exit_flag.store(true, Ordering::SeqCst);
        }

        // Tells the sorter to stop and wakes it if it's waiting. The pool's exit flag is the
        // sorter queue's own, so the sorter's `pop` returns `None` at once, even with
        // notifications left; the sorter routes those after its loop, before it stops.
        self.threadpool.pool_exit_flag.store(true, Ordering::SeqCst);
        self.threadpool.main_queue.worker_registry.notify_all();

        // A sorter that panicked was already reported by the panic hook. Its join error is
        // dropped, so that the shutdown goes on.
        if let Some(handle) = self.st_handle.take() {
            let _ = handle.join();
        }

        // Normally empty by now. Anything still here was never routed (there was no sorter, after
        // a failed startup) and comes back in `notifs`.
        let _ = self.threadpool.finalize_obs(notifs, status_reps);

        logger
    }

    /// Cleans up the given observers on a thread of their own.
    ///
    /// Call it after [`finalize`](Self::finalize), once the sorter has stopped, with the
    /// observers from [`finalize_get_observers`](Self::finalize_get_observers). For each observer,
    /// its own `clean_up` comes first, then its queue is finalized, which stops its thread. Once
    /// the deadline has passed, the remaining observers are only stopped: their `clean_up` doesn't
    /// run, since the engine is about to stop waiting. A line per observer says how it went and
    /// whether notifications were left in its queue (they're dropped), and an "Observer manager
    /// Finalized" line comes last.
    ///
    /// The thread sends these lines as status lines, so the engine has to keep reading its
    /// mailboxes until the thread ends. It must anyway: an observer's `clean_up` may send too,
    /// and wait while a mailbox is full. The thread's slot holds the type of the observer it's
    /// cleaning up.
    ///
    /// The observers' own threads are taken out of the pool (each is named after its observer's
    /// type), so the engine can wait for them too, until the same deadline: a `clean_up` that
    /// never returns never stops its observer's thread, nor those of the observers after it. The
    /// logger's thread stays in the pool.
    ///
    /// If the clean-up thread can't be started, the observers are cleaned up here, on the
    /// calling thread: their threads stop only once their queues are finalized.
    ///
    /// # Arguments
    ///
    /// * `status_reps` - Receives the status lines when there's no clean-up thread: no observer
    ///   to clean up, or a thread that couldn't be started
    /// * `grace_period_deadline` - The observers' deadline, which the engine's wait shares
    /// * `observers` - The observers to clean up, the logger excluded
    ///
    /// # Returns
    ///
    /// The threads for the engine to wait for, each with its slot: the clean-up thread (if it
    /// started) and the observers' own threads. Nothing if there's no observer to clean up.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let logger = manager.finalize(&mut notifs, &mut status_reps);
    /// let observers = manager.finalize_get_observers();
    /// let threads = manager.finalize_observers(&mut status_reps, deadline, observers);
    /// ```
    pub fn finalize_observers(
        &mut self,
        status_reps: &mut Vec<Notif>,
        grace_period_deadline: Instant,
        observers: Vec<Arc<Observer>>,
    ) -> Vec<(JoinHandle<()>, TaskSlot)> {
        // Only the logger was loaded (or nothing): no cleanup to run and nothing to wait for.
        if observers.is_empty() {
            status_reps.push(Log::log("Observer manager Finalized".to_string(), "Main"));
            return Vec::new();
        }

        // Each observer's thread is named after its type. These stop once their queues are
        // finalized, so the engine waits for them along with the clean-up thread; the logger's
        // thread stays in the pool, for the engine to join after the logger's own cleanup.
        let observer_types: Vec<&str> = observers.iter().map(|obs| obs.obs_type()).collect();
        let (observer_threads, logger_thread): (Vec<_>, Vec<_>) =
            std::mem::take(&mut self.threadpool.workers)
                .into_iter()
                .partition(|(handle, _)| {
                    handle
                        .thread()
                        .name()
                        .is_some_and(|name| observer_types.contains(&name))
                });
        self.threadpool.workers = logger_thread;

        // The clean-up thread reports through the engine's status mailbox.
        let tx = self.threadpool.packet_handle.clone();
        let observer_slot: TaskSlot = Arc::new(AtomicCell::new(None));
        let observer_slot_clone = observer_slot.clone();
        // The thread gets its own handles to the observers, so they can still be cleaned up here
        // if it doesn't start.
        let thread_observers = observers.clone();

        let spawn_res = thread::Builder::new()
            .name(String::from("observer cleanup"))
            .spawn(move || {
                let mut lines = Vec::new();
                for observer in thread_observers {
                    // The slot names the observer being cleaned up, so if the engine gives up on
                    // this thread, `Error.log` can say whose cleanup didn't return.
                    observer_slot_clone.swap(Some(Box::new(observer.obs_type().to_string())));
                    Self::clean_up_observer(&observer, &mut lines, grace_period_deadline);
                    let _ = observer_slot_clone.take();

                    // Sent as soon as each observer is done, so a later cleanup that never
                    // returns doesn't hold them back.
                    for line in lines.drain(..) {
                        tx.send_status_log(line);
                    }
                }

                tx.send_status_log(Log::log("Observer manager Finalized".to_string(), "Main"));
            });

        // The clean-up thread (if it started), then the observers' own threads.
        let mut threads = Vec::new();
        match spawn_res {
            Ok(handle) => threads.push((handle, observer_slot)),
            Err(e) => {
                status_reps.push(Log::err(format!(
                    "observer cleanup thread failed to spawn: {e}; the observers are cleaned up \
                     on the engine's thread"
                )));

                for observer in &observers {
                    Self::clean_up_observer(observer, status_reps, grace_period_deadline);
                }
                status_reps.push(Log::log("Observer manager Finalized".to_string(), "Main"));
            }
        }
        threads.extend(observer_threads);

        threads
    }

    /// Cleans up one observer (see [`Observer::clean_up`]), or only stops it once the deadline
    /// has passed (see [`Observer::clean_up_after_grace_period`]). Then adds a line saying how it
    /// went: for `Main.log` if it was cleaned up and nothing was left in its queue, for
    /// `Error.log` if its cleanup was skipped or notifications were left over. Those notifications
    /// are dropped.
    fn clean_up_observer(
        observer: &Arc<Observer>,
        status_reps: &mut Vec<Notif>,
        grace_period_deadline: Instant,
    ) {
        // Past the deadline the engine is about to stop waiting, so the observer's own cleanup
        // isn't started. Its queue is still finalized, though: that's what stops its thread.
        let mut did_grace_period_run_out = false;
        let left_over_obs_count = if grace_period_deadline <= Instant::now() {
            did_grace_period_run_out = true;
            observer.clean_up_after_grace_period(&mut Vec::new(), status_reps)
        } else {
            observer.clean_up(&mut Vec::new(), status_reps)
        };

        let notif = if did_grace_period_run_out {
            Log::err(format!(
                "observer {} was not cleaned up: the shutdown grace period ran out ({} notifications left over)",
                observer.obs_type(),
                left_over_obs_count
            ))
        } else if left_over_obs_count == 0 {
            Log::log(
                format!("observer {} finalized successfully", observer.obs_type()),
                "Main",
            )
        } else {
            Log::err(format!(
                "observer {} finalized with {} notifications left over",
                observer.obs_type(),
                left_over_obs_count
            ))
        };

        status_reps.push(notif);
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

    /// Takes every observer except the logger out of the manager, for
    /// [`finalize_observers`](Self::finalize_observers).
    ///
    /// Call it after [`finalize`](Self::finalize), which has already taken the logger out.
    ///
    /// # Returns
    ///
    /// The observers, in no particular order. The manager is left empty.
    pub fn finalize_get_observers(&mut self) -> Vec<Arc<Observer>> {
        self.observers
            .drain()
            .map(|(_, observer)| observer)
            .collect()
    }
}
