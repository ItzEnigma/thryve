// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Core engine for the Thryve runtime.
//!
//! This module provides the [`Engine`] struct, which is the central
//! component of the Thryve runtime. The engine manages the execution
//! flow of tasks and notifications, coordinating between the plugin
//! manager and observer manager.
//!
//! # Architecture
//!
//! The engine maintains a main loop that:
//! 1. Checks for new tasks from the service
//! 2. Processes notifications from observers
//! 3. Handles results from task executions
//! 4. Coordinates shutdown and pause/resume operations
//!
//! # Communication
//!
//! The engine communicates with other components via crossbeam channels:
//! - Task receiver: Receives tasks from the service
//! - Notification receiver: Receives notifications from observers
//! - Result receiver: Receives results from plugin executions

use crate::runtime::engine_wait_table::{WaitTable, WaitingState};
use crate::runtime::observer_manager::ObserverManager;
use crate::runtime::plugin_manager::PluginManager;

use thryve_utils::bases::notif::Notif;
use thryve_utils::bases::observer::Observer;
use thryve_utils::cfg::config::Config;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::cfg::metrics::Metrics;
use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::graphs::graph::TopologyNode;
use thryve_utils::packets::notif_topology::TopologyModifier;
use thryve_utils::packets::packet_handle::StatusMsg;
use thryve_utils::packets::{notif_log::Log, task::Task};
use thryve_utils::res::{SealedHandoff, TaskResult};
use thryve_utils::threads::threadpool::TaskSlot;
use thryve_utils::traits::queue::Queue;

use crossbeam::channel::{Receiver, Select, bounded};
use std::collections::HashMap;
use std::thread::JoinHandle;
use std::time::Instant;
use std::{sync::Arc, thread, time::Duration};

/// Core engine for the Thryve runtime.
///
/// The `Engine` struct is the central component of the Thryve runtime,
/// managing the execution flow of tasks and notifications. It coordinates
/// between the plugin manager and observer manager, handling the main
/// execution loop and lifecycle management.
///
/// # Fields
///
/// - `config`: The configuration the engine was created with
/// - `metrics`: Size metrics, kept in debug mode with `calculate_metrics` on
/// - `obs_manager`: Manages observers and notifications
/// - `plugin_manager`: Manages plugins and tasks
/// - `task_rx`: Receiver for incoming tasks
/// - `notif_rx`: Receiver for incoming notifications
/// - `res_rx`: Receiver for task execution results
/// - `exit_flag`: Set once the engine is being finalized; the loops stop then
/// - `pause_flag`: Indicates whether the engine is paused
///
/// # Example
///
/// ```ignore
/// // Engine is typically created and managed by the service
/// let engine = Engine::new(&ctx);
/// engine.run_once(); // Single iteration
/// ```
pub struct Engine {
    /// The configuration the engine was created with. The engine reads it as it runs: the idle
    /// wait, the per-turn limits, the grace period and the debugging switches. Settings used only
    /// at creation (the worker count, the mailbox capacities, the task queue's priorities) were
    /// already applied, so changing them here has no effect.
    pub config: Config,
    /// Keeps track of the metrics, in debug mode with `calculate_metrics` on.
    pub metrics: Metrics,
    /// Manages observers and notifications.
    pub obs_manager: ObserverManager,
    /// Manages plugins and tasks.
    pub plugin_manager: PluginManager,
    /// Receiver for incoming tasks.
    task_rx: Arc<Receiver<Task>>,
    /// Receiver for incoming notifications.
    notif_rx: Arc<Receiver<Notif>>,
    /// Receiver for task execution results.
    res_rx: Receiver<(u64, TaskResult)>,
    /// Receiver for Thryve's own messages: status lines, and the releases of dropped task
    /// handles. Unbounded, so their senders never wait.
    status_rx: Arc<Receiver<StatusMsg>>,
    /// Where a typed task's follow-up and its result meet, under the task's id. An entry is a
    /// follow-up waiting for the result, a result kept for a handle that may still call `then`,
    /// a remembered release, a link to the task a task forwarded to, or a join's follower
    /// waiting for one of the join's tasks. Each goes once both sides have arrived (see
    /// [`accept_task`](Self::accept_task), [`handle_result`](Self::handle_result) and
    /// [`check_status`](Self::check_status)), or at the shutdown, which counts what's left.
    waiting_tasks: WaitTable<WaitingState>,
    /// The joins still waiting for some of their tasks, by the join's id. Each missing task's
    /// follower, in `waiting_tasks`, leads here; the join is taken out and queued when the last
    /// one's result arrives.
    waiting_joins: WaitTable<Task>,
    /// Set by [`finalize`](Self::finalize) as it starts; the engine's loops stop once it's set.
    /// Don't set it yourself: `finalize` would then take the engine for finalized already, return
    /// at once, and shut nothing down.
    pub exit_flag: bool,
    /// Indicates whether the engine is paused.
    pub pause_flag: bool,
}

impl Engine {
    /// Creates a new engine instance.
    ///
    /// Initializes a new engine with the given context, setting up
    /// communication channels and creating the observer and plugin
    /// managers.
    ///
    /// The engine starts paused, so that no plugin task runs while it's being set up: the
    /// workers park, and the plugins' init tasks wait in the queue. The engine thread resumes it
    /// once it has the engine (after the debug wait, when there is one). An engine that never
    /// starts, after a failed setup, is finalized without any plugin task having run.
    ///
    /// # Arguments
    ///
    /// * `ctx` - The application context containing configuration
    ///
    /// # Returns
    ///
    /// A new, paused `Engine`; call [`resume`](Self::resume) to start it.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let engine = Engine::new(&ctx);
    /// ```
    pub fn new(ctx: &Ctx) -> Self {
        let (res_tx, res_rx) = bounded(ctx.config.pipes.res_pipe_capacity);

        let mut eng = Self {
            config: ctx.config,
            metrics: ctx.meta_data.metrics.clone(),
            plugin_manager: PluginManager::new(ctx, res_tx),
            obs_manager: ObserverManager::new(ctx),
            task_rx: ctx.task_rx.clone(),
            notif_rx: ctx.notif_rx.clone(),
            res_rx,
            status_rx: ctx.status_rx.clone(),
            waiting_tasks: WaitTable::new(),
            waiting_joins: WaitTable::new(),
            exit_flag: false,
            pause_flag: false,
        };

        eng.pause(true);
        eng
    }
}

impl Engine {
    /// Waits until one of the mailboxes gets something, or the wait's limit runs out.
    ///
    /// Running, it watches all four mailboxes for up to `idle_wait`: a send to any of them wakes
    /// it at once, and something that arrived after the turn read that mailbox makes it return
    /// right away. Paused, it sleeps 500 ms without watching the mailboxes, so a loop that calls
    /// [`run_once`](Self::run_once) while paused doesn't spin; status lines and notifications then
    /// wait up to 500 ms.
    ///
    /// It takes nothing; the next turn does the reading. Now and then it may return for nothing
    /// (crossbeam allows that), and the next turn then finds nothing and waits again.
    fn receive_timeout(&self) {
        let mut receivers = Select::new();

        if self.pause_flag {
            let _ = receivers.ready_timeout(Duration::from_millis(500));
            return;
        }

        receivers.recv(&*self.task_rx);
        receivers.recv(&*self.notif_rx);
        receivers.recv(&self.res_rx);
        receivers.recv(&*self.status_rx);

        let _ = receivers.ready_timeout(Duration::from_millis(self.config.engine.idle_wait));
    }

    /// Runs one turn of the engine's main loop.
    ///
    /// A turn reads the four mailboxes in order: status lines, tasks, notifications and results.
    /// It takes at most a set number from each (500 status lines, and the `pipes.*_per_turn`
    /// settings for the others), so a busy mailbox can't keep the others waiting. Status lines
    /// and notifications go to the observers, tasks are queued for the workers, and results are
    /// handled.
    ///
    /// If the turn found nothing, it waits until any mailbox gets something, or until
    /// [`idle_wait`](thryve_utils::cfg::config::EngineConfig::idle_wait) runs out. So an idle
    /// engine sleeps, and wakes the moment there's work.
    ///
    /// Paused, a turn first waits 500 ms, then reads only status lines and notifications. Once
    /// the engine is finalized, a turn reads those two without waiting.
    ///
    /// # Example
    ///
    /// ```ignore
    /// while !engine.exit_flag {
    ///     engine.run_once();
    /// }
    /// ```
    pub fn run_once(&mut self) {
        if self.pause_flag {
            self.receive_timeout();
        }

        if self.exit_flag || self.pause_flag {
            self.check_status();
            self.check_notifs();
            return;
        }

        let received_status = self.check_status();
        let received_tasks = self.check_tasks();
        let received_notifs = self.check_notifs();
        let received_results = self.check_results();

        if !received_status && !received_tasks && !received_notifs && !received_results {
            self.receive_timeout();
        }
    }

    /// Checks for Thryve's own messages: status lines and releases.
    ///
    /// Takes up to 500 messages from the status channel, stopping early once it's empty. A
    /// status line goes to the observer manager, which routes it to the logger. A release means
    /// a typed task's handle was dropped without a follow-up: if the task's result was kept,
    /// nobody will take it now, so its error is written to `Error.log`; if the result hasn't
    /// arrived yet, the release is remembered until it does. If the task forwarded, the release
    /// follows the links and applies to the task at their end. Messages past the 500 wait for the
    /// next turn.
    ///
    /// # Returns
    ///
    /// `true` if any messages were received.
    pub fn check_status(&mut self) -> bool {
        let mut received_status = false;
        let number_to_process_per_turn = 500;

        for _ in 0..number_to_process_per_turn {
            if let Ok(status) = self.status_rx.try_recv() {
                received_status = true;

                match status {
                    StatusMsg::LogLine(notif) => {
                        self.obs_manager.forward_notif(notif);
                    }
                    StatusMsg::HandleRelease(wait_id) => {
                        self.release(wait_id);
                    }
                }
            } else {
                break;
            }
        }

        received_status
    }

    /// Checks for and processes new tasks.
    ///
    /// Takes up to `pipes.tasks_processed_per_turn` tasks from the task channel, stopping early
    /// once it's empty, and hands each to [`accept_task`](Self::accept_task). The rest wait for
    /// the next turn.
    ///
    /// # Returns
    ///
    /// `true` if any tasks were received and processed.
    pub fn check_tasks(&mut self) -> bool {
        let mut received_tasks = false;

        for _ in 0..self.config.pipes.tasks_processed_per_turn {
            if let Ok(task) = self.task_rx.try_recv() {
                received_tasks = true;
                self.accept_task(task);
            } else {
                break;
            }
        }

        received_tasks
    }

    /// Takes in one task: in debug mode it records the task's metrics and its node in the
    /// topology graph, then it queues the task for the workers.
    ///
    /// A follow-up declared with `then` is queued only once it has the result of the task it
    /// follows. If that result was kept (the task finished first), it gets it now and is queued
    /// at once. Otherwise it waits in `waiting_tasks`, and
    /// [`handle_result`](Self::handle_result) queues it when the result arrives. If the task
    /// forwarded, the follow-up follows the links and waits for the task at their end instead.
    ///
    /// A join declared with `join` is queued only once it has the results of all its tasks: it
    /// takes the kept ones now, leaves a follower for each of the others, and waits in
    /// `waiting_joins` until they arrive (see `add_join_to_waiting`).
    ///
    /// Every task the engine starts goes through here: the ones sent on the task channel, the
    /// follow-up tasks chained to results, and the plugins' init tasks, which startup hands over
    /// directly.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to start
    pub fn accept_task(&mut self, task: Task) {
        if self.config.debugging.is_debugging {
            if self.config.debugging.calculate_metrics {
                self.manage_metrics_tasks(&task);
            }

            if self.config.debugging.visual_graph {
                self.handle_graph_visualization(&task);
            }
        }

        if let Some(id) = task.waiting_on_id() {
            self.attach(task, id);
            return;
        } else if task.waiting_on_join().is_some() {
            self.add_join_to_waiting(task);
            return;
        }

        self.plugin_manager.threadpool.submit_task(task);
    }

    /// Checks for and processes new notifications.
    ///
    /// Takes up to `pipes.notifs_processed_per_turn` notifications from the notification
    /// channel, stopping early once it's empty, and forwards each to the observer manager. The
    /// rest wait for the next turn.
    ///
    /// # Returns
    ///
    /// `true` if any notifications were received and processed.
    pub fn check_notifs(&mut self) -> bool {
        let mut received_notifs = false;

        for _ in 0..self.config.pipes.notifs_processed_per_turn {
            if let Ok(notif) = self.notif_rx.try_recv() {
                received_notifs = true;

                if self.config.debugging.is_debugging && self.config.debugging.calculate_metrics {
                    self.manage_metrics_notifs(&notif);
                }

                self.obs_manager.forward_notif(notif);
            } else {
                break;
            }
        }

        received_notifs
    }

    /// Checks for and processes new results.
    ///
    /// Takes up to `pipes.res_processed_per_turn` results from the result channel, stopping
    /// early once it's empty, and handles each, which may queue a follow-up task or log a
    /// message. The rest wait for the next turn. A `Fin` or `FinErr` finalizes the engine and
    /// ends the turn.
    ///
    /// # Returns
    ///
    /// `true` if any results were received and processed.
    pub fn check_results(&mut self) -> bool {
        let mut received_results = false;

        for _ in 0..self.config.pipes.res_processed_per_turn {
            if let Ok(task_result) = self.res_rx.try_recv() {
                received_results = true;

                if self.config.debugging.is_debugging && self.config.debugging.calculate_metrics {
                    self.manage_metrics_results(&task_result.1);
                }

                self.handle_result(task_result);

                if self.exit_flag {
                    break; // finalize() ran inside handle_result() above -- stop
                    // draining more results into a torn-down observer manager
                }
            } else {
                break;
            }
        }

        received_results
    }

    /// Collects size metrics for an incoming task.
    ///
    /// Stores the task size in the metrics batch buffer. When the batch
    /// is full (1024 entries), triggers metric calculation and sends
    /// a debug notification with the formatted results.
    ///
    /// # Arguments
    ///
    /// * `task` - Reference to the task being processed
    pub fn manage_metrics_tasks(&mut self, task: &Task) {
        let size = task.size_in_bytes();
        let cur_batch_size = self.metrics.cur_batch_count_task;

        self.metrics.task_size_batch[cur_batch_size] = size;
        self.metrics.cur_batch_count_task += 1;

        if self.metrics.cur_batch_count_task == self.metrics.task_size_batch.len() {
            let metrics_readable = self.metrics.calculate_metrics();

            let notif = Log::log(metrics_readable, "Debug");
            self.obs_manager.forward_notif(notif);
        }
    }

    /// Collects size metrics for a task execution result.
    ///
    /// Stores the result size in the metrics batch buffer. When the
    /// batch is full (1024 entries), triggers metric calculation and
    /// sends a debug notification with the formatted results.
    ///
    /// # Arguments
    ///
    /// * `task_result` - Reference to the result being processed
    pub fn manage_metrics_results(&mut self, task_result: &TaskResult) {
        let size = task_result.size_in_bytes();
        let cur_batch_size = self.metrics.cur_batch_count_res;

        self.metrics.res_size_batch[cur_batch_size] = size;
        self.metrics.cur_batch_count_res += 1;

        if self.metrics.cur_batch_count_res == self.metrics.res_size_batch.len() {
            let metrics_readable = self.metrics.calculate_metrics();

            let notif = Log::log(metrics_readable, "Debug");
            self.obs_manager.forward_notif(notif);
        }
    }

    /// Collects size metrics for an incoming notification.
    ///
    /// Stores the notification size in the metrics batch buffer. When
    /// the batch is full (1024 entries), triggers metric calculation
    /// and sends a debug notification with the formatted results.
    ///
    /// # Arguments
    ///
    /// * `notif` - Reference to the notification being processed
    pub fn manage_metrics_notifs(&mut self, notif: &Notif) {
        let size = notif.size_in_bytes();
        let cur_batch_size = self.metrics.cur_batch_count_notif;

        self.metrics.notif_size_batch[cur_batch_size] = size;
        self.metrics.cur_batch_count_notif += 1;

        if self.metrics.cur_batch_count_notif == self.metrics.notif_size_batch.len() {
            let metrics_readable = self.metrics.calculate_metrics();

            let notif_metric = Log::log(metrics_readable, "Debug");
            self.obs_manager.forward_notif(notif_metric);
        }
    }

    /// Finalizes the engine and shuts down all components.
    ///
    /// In order, it:
    ///
    /// 1. sets the exit flag, and the deadline: now plus the grace period
    ///    ([`shutdown_grace_period`](thryve_utils::cfg::config::EngineConfig::shutdown_grace_period)),
    ///    which steps 3 and 4 share. Step 5 gets one more second;
    /// 2. stops the plugin workers: the tasks still queued are taken out and never run, and a
    ///    worker that's running a task finishes it;
    /// 3. waits for every worker to finish while it keeps emptying its mailboxes, because a
    ///    worker can be blocked on a full one. Notifications are forwarded to the observers,
    ///    which still run; tasks, results and status lines are collected. Then it joins the
    ///    workers. A worker still running at the deadline is left behind, and `Error.log` names
    ///    it and its task. Then the wait table is emptied, since results are only collected now:
    ///    a follow-up still waiting never gets its input and never runs, nor does a join still
    ///    waiting for some of its tasks, and a result kept for a handle that still exists is
    ///    counted with the late results;
    /// 4. cleans up the plugins on a thread of its own, emptying the mailboxes the same way while
    ///    it runs, until the same deadline. A failed cleanup goes to `Error.log`. The cleanups are
    ///    skipped, and `Error.log` says so, if a worker was left behind (its plugin may still be
    ///    in use), if the deadline has already passed, or if the thread can't be started;
    /// 5. finalizes the observer manager, which stops the notification sorter once it has routed
    ///    what it holds. Then the other observers are cleaned up on a thread of their own, and
    ///    the engine waits for that thread and for the observers' own threads, emptying the
    ///    mailboxes the same way, since an observer's `clean_up` may send to a full one. This wait
    ///    lasts until the deadline plus one second, or one second from now if the deadline has
    ///    already passed (`Error.log` then says so), because only an observer's cleanup stops its
    ///    thread. An observer whose turn comes after the deadline isn't cleaned up, only stopped,
    ///    and a thread still running at the end is left behind and named in `Error.log`;
    /// 6. sums up the work the shutdown lost, now that nothing more can arrive: the tasks that
    ///    never ran, the results that carried something, the notifications never delivered, and
    ///    the threads left running. If anything was lost, one line counts each kind, in
    ///    `Main.log` after the service's Stop and in `Error.log` otherwise; with debugging on,
    ///    `Debug.log` gets the details;
    /// 7. hands the collected status lines, "Engine Finalized" and the metrics (when enabled) to
    ///    the logger. It pushes them into the logger's queue, which wakes the logger's thread if
    ///    it's waiting, then joins that thread (it writes at most one line, sees its exit flag and
    ///    stops), then cleans up the logger, which writes everything still queued. The cleanup is
    ///    the only writer left, so "Engine Finalized" is the last line;
    /// 8. joins whatever is still in the observer pool, which is normally nothing: a safety net.
    ///
    /// It also works on a half-built engine, which is how a failed startup stops what it
    /// started. Parts that never started are skipped, and without a logger the status lines are
    /// skipped too. A `Fin` or `FinErr` task result calls this from inside
    /// [`run_once`](Self::run_once).
    ///
    /// It runs only once: it sets the exit flag first, and a later call that finds the flag set
    /// returns at once. Two calls can't overlap, since each needs the engine to itself
    /// (`&mut self`).
    ///
    /// A task, a `clean_up` or an observer's loop that never returns holds up the shutdown only
    /// until its deadline, so the whole shutdown takes at most the grace period plus about one
    /// second. Such a thread keeps running on its own: Rust can't stop a thread, so it ends with
    /// the process (in FFI, it keeps running in the host process).
    ///
    /// # What the order rests on
    ///
    /// - The engine is the only reader of its mailboxes, and a running thread may be blocked on
    ///   a full one, waiting for the engine. So the engine never just joins a thread that may
    ///   still send: it waits while it keeps reading (`finalize_handle_logic`), and reads once
    ///   more after the joins, which makes sure it sees everything the threads sent.
    /// - Each read takes only what was in the mailboxes when it started, so threads that keep
    ///   sending can't keep the engine from checking its deadline.
    /// - Notifications go through the sorter while it runs. Once it has stopped, log lines go
    ///   straight to the logger, the only observer that keeps running, and any other
    ///   notification is kept as undelivered.
    /// - An observer's thread stops only once its queue is finalized, which its cleanup does. So
    ///   the observers always get time, even when the plugins used up the grace period.
    /// - The logger stops last, and at the very end its cleanup is the only writer.
    ///
    /// # Arguments
    ///
    /// * `is_service_normal_shutdown` - `true` when the service's Stop shuts the engine down.
    ///   Someone asked for that stop, so work cut short is expected, and the summary of lost work
    ///   goes to `Main.log`. Every other shutdown (a `Fin` or `FinErr` result, a failed startup)
    ///   passes `false`, and the summary goes to `Error.log`: there, lost work is something a
    ///   developer needs to look at.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // A shutdown that isn't the service's Stop.
    /// engine.finalize(false);
    /// // All components shut down gracefully
    /// ```
    pub fn finalize(&mut self, is_service_normal_shutdown: bool) {
        if self.exit_flag {
            return;
        }
        self.exit_flag = true;

        // What the shutdown collects on its way: the tasks that never ran, the results that
        // arrived too late, the notifications that were never delivered, and Thryve's own status
        // lines, which the logger writes at the very end, in order.
        let mut fc = FinalizeCollection::new();
        // The plugin side's deadline. The observers get one more second, below.
        let mut grace_period_deadline =
            Instant::now() + Duration::from_secs(self.config.engine.shutdown_grace_period as u64);

        // No new task starts from here on: the workers are told to stop, and the tasks still
        // queued are taken out. A worker that's running a task finishes it first.
        self.plugin_manager
            .finalize(&mut fc.tasks, &mut fc.status_reps);

        // The handles leave the pool so that the engine can keep emptying its mailboxes while it
        // waits for them. No logger is passed: the sorter still runs, so notifications go
        // through it as usual.
        let workers = std::mem::take(&mut self.plugin_manager.threadpool.workers);
        let did_handle_logic_succeed =
            self.finalize_handle_logic(&mut fc, workers, Some(grace_period_deadline), None);

        // Results are only collected from here on, so a follow-up still waiting never gets its
        // input and never runs, and nobody will take a result kept for a live handle any more.
        // That result joins the late ones: an error counts as lost work, and a value doesn't.
        // A remembered release has nothing left to wait for, and a link has nothing to report:
        // the task at its end reports its own result. Nor has a join's follower: its task
        // reports itself.
        for (wait_id, wait_state) in self.waiting_tasks.table.drain() {
            match wait_state {
                WaitingState::FollowUP(task) => {
                    fc.tasks.push(task);
                }
                WaitingState::HandoffKept(handoff) => {
                    fc.results.push((wait_id, TaskResult::new_handoff(handoff)));
                }
                _ => (),
            }
        }

        // A join still waiting never runs either, so it counts with the tasks that never ran.
        // None of its tasks failed, or it would have run, so its slots hold only values: nothing
        // in them is lost work.
        for (_, join_task) in self.waiting_joins.table.drain() {
            fc.tasks.push(join_task);
        }

        // The plugins' cleanups run on a thread of their own, for the same reason: a `clean_up`
        // may send to a full mailbox. They're skipped, with a line saying why, if a worker was
        // left behind or the deadline has passed; then there's no thread to wait for.
        let plugins_cleanup = self.plugin_manager.finalize_plugins(
            &mut fc.status_reps,
            grace_period_deadline,
            did_handle_logic_succeed,
        );

        if !plugins_cleanup.is_empty() {
            self.finalize_handle_logic(&mut fc, plugins_cleanup, Some(grace_period_deadline), None);
        }

        // The observers' turn. The sorter routes everything it still holds, then stops. The
        // logger comes back with its exit flag set; it's stopped last, below.
        let logger = self
            .obs_manager
            .finalize(&mut fc.notifs, &mut fc.status_reps);

        // An observer's thread stops only once its queue is finalized, which its cleanup does,
        // so the observers get one more second than the plugins, counted from now if the
        // deadline has already passed (a stuck task used it up).
        grace_period_deadline = if grace_period_deadline <= Instant::now() {
            fc.status_reps.push(Log::err(format!("shutdown allotted time of '{}' seconds expired before the observers started cleanup; giving the observers 1 more second to shut down", self.config.engine.shutdown_grace_period)));

            Instant::now() + Duration::from_secs(1)
        } else {
            grace_period_deadline + Duration::from_secs(1)
        };

        // The other observers are cleaned up on a thread of their own, while the engine keeps
        // reading its mailboxes: an observer's `clean_up` may send to a full one. The engine also
        // waits for the observers' own threads here, so a `clean_up` that never returns (and
        // never stops its observer's thread) holds up the shutdown only until the deadline. With
        // no thread to wait for, this is a single pass over the mailboxes.
        let observers = self.obs_manager.finalize_get_observers();
        let observer_cleanup = self.obs_manager.finalize_observers(
            &mut fc.status_reps,
            grace_period_deadline,
            observers,
        );

        // The sorter is gone, so the logger is passed: log lines that arrive now go straight into
        // its queue, and other notifications are kept as undelivered.
        self.finalize_handle_logic(
            &mut fc,
            observer_cleanup,
            Some(grace_period_deadline),
            logger.clone(),
        );

        fc.summarize(self, is_service_normal_shutdown);

        // Every other thread has stopped or been left behind, so these are the last lines.
        let notif = Log::log("Engine Finalized".to_string(), "Main");
        fc.status_reps.push(notif);

        if self.config.debugging.is_debugging && self.config.debugging.calculate_metrics {
            let metrics_readable = self.metrics.calculate_metrics();

            let notif = Log::log(metrics_readable, "Debug");
            fc.status_reps.push(notif);
        }

        // `pop` takes from the back, so reversing first pushes the lines oldest first.
        fc.status_reps.reverse();
        if let Some(logger) = &logger {
            while !fc.status_reps.is_empty() {
                if let Some(status_rep) = fc.status_reps.pop() {
                    logger.obs_queue.push(status_rep);
                }
            }

            // The pushes woke the logger's thread if it was waiting; after a failed startup
            // (no sorter), nothing else would have. Its exit flag is set, so it writes at most
            // one line and stops. Joining it before the logger's cleanup leaves the cleanup as
            // the only writer, so the lines come out in order and "Engine Finalized" is last.
            // Its thread is named after its type.
            let mut handle_vec: Vec<(JoinHandle<()>, TaskSlot)> = self
                .obs_manager
                .threadpool
                .workers
                .extract_if(.., |(handle, _)| {
                    handle.thread().name().unwrap_or("non named thread")
                        == NamedInput::new_logging().inner()
                })
                .collect();

            // A logger thread that panicked was already reported by the panic hook.
            if let Some((handle, _)) = handle_vec.pop() {
                let _ = handle.join();
            }

            // Writes everything still queued, then finalizes the logger's queue.
            logger.clean_up(&mut fc.notifs, &mut fc.status_reps);
        }

        // A safety net: the other observers' threads were waited for above and the logger's was
        // joined, so normally nothing is left in the pool.
        while !self.obs_manager.threadpool.workers.is_empty() {
            if let Some((handle, _)) = self.obs_manager.threadpool.workers.pop()
                && !handle.is_finished()
            {
                let res = handle.join();

                if res.is_err() {
                    continue;
                }
            }
        }
    }

    /// Pauses the engine.
    ///
    /// Sets the pause flag and pauses the plugin manager's thread
    /// pool, causing task processing to halt until resumed.
    ///
    /// # Arguments
    ///
    /// * `is_start` - `true` only for the startup pause ([`new`](Self::new) pauses every engine
    ///   until the engine thread starts it). That pause isn't logged; a user's pause logs
    ///   "Engine Paused".
    ///
    /// # Example
    ///
    /// ```ignore
    /// engine.pause(false);
    /// // Task processing halted
    /// engine.resume(false);
    /// // Task processing resumed
    /// ```
    pub fn pause(&mut self, is_start: bool) {
        self.plugin_manager.threadpool.pause(is_start);

        self.pause_flag = true;

        if !is_start {
            let notif = Log::log("Engine Paused".to_string(), "Main");
            self.obs_manager.forward_notif(notif);
        }
    }

    /// Resumes the engine after a pause.
    ///
    /// Clears the pause flag and resumes the plugin manager's
    /// thread pool, allowing task processing to continue.
    ///
    /// # Arguments
    ///
    /// * `is_start` - `true` only when the engine thread ends the startup pause, which isn't
    ///   logged; resuming a user's pause logs "Engine continued".
    ///
    /// # Example
    ///
    /// ```ignore
    /// engine.resume(false);
    /// // Task processing resumed
    /// ```
    pub fn resume(&mut self, is_start: bool) {
        self.plugin_manager.threadpool.resume(is_start);

        self.pause_flag = false;

        if !is_start {
            let notif = Log::log("Engine continued".to_string(), "Main");
            self.obs_manager.forward_notif(notif);
        }
    }
}

impl Engine {
    /// Handles a task execution result.
    ///
    /// Processes the result of a task execution, which may include:
    /// - Handing an `AdvTask`'s handoff to the follow-up waiting for it, which is then queued.
    ///   A follow-up that inherited its priority gets it capped below realtime here, since only
    ///   the engine knows `max_priority`. With no follow-up waiting, the handoff is kept while
    ///   the task's handle exists, for a `then` that may still come. If the handle is already
    ///   gone, an error is written to `Error.log` and a value is dropped. The handoff is taken
    ///   out first, before anything else, and the engine never opens it
    /// - Following a forward: the task's outcome becomes the forwarded-to task's. A follow-up
    ///   already waiting moves on to that task, and a remembered release applies to it; with
    ///   neither, a link is stored for the follow-up or release still to come
    /// - Handing a handoff to the join whose follower waits for it: it goes into the join's slot
    ///   at the follower's position, and the join is queued once none of its slots is empty, or
    ///   at once if the handoff is an error, since a join fails fast. If the join has already
    ///   run (another of its tasks failed first), nobody will take the handoff: an error is
    ///   written to `Error.log`, and a value is dropped. A forward never goes into a slot: the
    ///   follower moves on to the task forwarded to, even when the join has already run, so that
    ///   task's result finds it (see `attach_follower`)
    /// - Submitting the task chained to the result
    /// - Logging completion messages
    /// - Triggering shutdown on "Fin" or "FinErr" results
    ///
    /// # Arguments
    ///
    /// * `task_result` - Tuple of task ID and result
    pub fn handle_result(&mut self, task_result: (u64, TaskResult)) {
        let (id, mut result) = task_result;

        if let Some(sealed_handoff) = result.take_handoff() {
            let wait_state = self.waiting_tasks.get_as_ref(id);

            if wait_state.is_none() {
                if sealed_handoff.is_finish() {
                    self.waiting_tasks.insert_handoff(id, sealed_handoff);
                } else {
                    if let Some(next_id) = sealed_handoff.read_forward_id() {
                        self.waiting_tasks.insert_link(id, next_id);
                    }
                }
            } else if wait_state.is_released() {
                if sealed_handoff.is_finish() {
                    // The handle is already gone, so nobody can follow up on it.
                    drop(self.waiting_tasks.remove(id));
                    self.drop_unclaimed_handoff(sealed_handoff);
                } else {
                    self.waiting_tasks.remove(id);

                    if let Some(current_id) = sealed_handoff.read_forward_id() {
                        self.release(current_id);
                    }
                }
            } else if wait_state.is_join_part_placeholder() {
                if let WaitingState::JoinPartPlaceHolder { join_id, position } =
                    self.waiting_tasks.remove(id)
                {
                    if let Some(next_id) = sealed_handoff.read_forward_id() {
                        self.attach_follower(next_id, join_id, position);
                    } else if self.waiting_joins.get_mut(join_id).is_some() {
                        self.fill_join_slot(sealed_handoff, join_id, position);
                    } else {
                        self.drop_unclaimed_handoff(sealed_handoff);
                    }
                }
            } else if wait_state.is_follow_up() {
                if let Some(next_id) = sealed_handoff.read_forward_id()
                    && let Some(task) = self.waiting_tasks.take_task(id)
                {
                    self.attach(task, next_id);
                } else if let Some(task) = self.waiting_tasks.take_task(id) {
                    let task = task.with_input(sealed_handoff, self.config.tasks.max_priority);
                    self.plugin_manager.threadpool.submit_task(task);
                }
            }
        }

        if let Some(next_task) = result.next_task.take() {
            self.accept_task(next_task);
        }

        if self.config.debugging.is_debugging && self.config.debugging.log_tasks {
            let notif = Log::log(format!("Finished Task ID: {}.", task_result.0), "Main");

            if self.config.debugging.calculate_metrics {
                self.manage_metrics_notifs(&notif);
            }

            self.obs_manager.forward_notif(notif);
        }

        let result_message = std::mem::take(&mut result.message);
        let result_type = result.result_type();
        let log_file = result.log_msg_to_file();

        if result_type.is_empty() {
            if !result_message.is_empty() {
                let notif = Log::err(String::from(
                    "result type cannot be empty if the result message is not empty",
                ));
                self.obs_manager.forward_notif(notif);
            } else if !log_file.is_empty() {
                let notif = Log::err(String::from(
                    "result type cannot be empty if the result should log to a file",
                ));
                self.obs_manager.forward_notif(notif);
            }
            return;
        }

        if !result_message.is_empty() {
            let notif = Log::log(result_message, log_file);

            if self.config.debugging.is_debugging && self.config.debugging.calculate_metrics {
                self.manage_metrics_notifs(&notif);
            }

            self.obs_manager.forward_notif(notif);
        }

        if (result_type == "Fin" || result_type == "FinErr") && !self.exit_flag {
            // Not the service's Stop, so the summary of lost work, if any, goes to `Error.log`.
            self.finalize(false);
        }
    }

    /// Lets go of a handoff that nobody will take: no follow-up was declared, and the task's
    /// handle is gone. An error is written to `Error.log`, with the text the worker wrote while
    /// the error's type was still known; a value is just dropped. The engine never opens it.
    ///
    /// Only a finished task's handoff gets here: the engine follows a forward instead.
    ///
    /// # Arguments
    ///
    /// * `handoff` - The handoff nobody will take
    fn drop_unclaimed_handoff(&mut self, handoff: SealedHandoff) {
        if let SealedHandoff::Finished {
            error_text: Some(error_text),
            ..
        } = handoff
        {
            self.obs_manager.forward_notif(Log::err(error_text));
        }
    }

    /// Attaches a follow-up to the task it follows, through that task's forwards: if the task
    /// forwarded, the follow-up goes to the task forwarded to, and so on to the end of the links.
    /// Each link passed is removed, since only one follow-up or release ever comes through it.
    ///
    /// At the end, if that task's result was kept, the follow-up gets it and is queued. If
    /// nothing is there yet, the follow-up waits there for the result.
    ///
    /// # Arguments
    ///
    /// * `task` - The follow-up
    /// * `list_starts_from_id` - The id of the task it follows
    fn attach(&mut self, task: Task, list_starts_from_id: u64) {
        let (last_id, wait_state) = self.waiting_tasks.remove_while_is_link(list_starts_from_id);

        if let WaitingState::None = wait_state {
            self.waiting_tasks.insert_follow_up(last_id, task);
        } else if let WaitingState::HandoffKept(sealed_handoff) = wait_state {
            let task = task.with_input(sealed_handoff, self.config.tasks.max_priority);
            self.plugin_manager.threadpool.submit_task(task);
        }
    }

    /// Moves a join's follower on to the task its task forwarded to, through that task's own
    /// forwards like [`attach`](Self::attach): the join now waits for the result of the task at
    /// the end of the links. Each link passed is removed.
    ///
    /// At the end, if that task's result was kept (it can arrive before the forward, since the
    /// two travel separately), the join gets it now through
    /// [`fill_join_slot`](Self::fill_join_slot), or it's let go if the join has already run. If
    /// nothing is there yet, the follower waits there for the result.
    ///
    /// Nothing else can be there, since the handle of the task forwarded to went into the
    /// forward. If something is (only a bug in Thryve could cause it), it's dropped with a line in
    /// `Error.log`, and the join won't get that input.
    ///
    /// # Arguments
    ///
    /// * `list_starts_from_id` - The id of the task forwarded to
    /// * `join_task_id` - The join's id, its key in `waiting_joins`
    /// * `position` - The follower's slot in the join
    fn attach_follower(&mut self, list_starts_from_id: u64, join_task_id: u64, position: usize) {
        let (last_id, wait_state) = self.waiting_tasks.remove_while_is_link(list_starts_from_id);

        if let WaitingState::None = wait_state {
            self.waiting_tasks.store(
                last_id,
                WaitingState::JoinPartPlaceHolder {
                    join_id: join_task_id,
                    position,
                },
            );
        } else if let WaitingState::HandoffKept(sealed_handoff) = wait_state {
            if self.waiting_joins.get_mut(join_task_id).is_some() {
                self.fill_join_slot(sealed_handoff, join_task_id, position);
            } else {
                self.drop_unclaimed_handoff(sealed_handoff);
            }
        } else {
            // The walk never ends on a link, and the handle of the task forwarded to went into
            // the forward, so nothing else can wait on its result.
            let found = match wait_state {
                WaitingState::FollowUP(_) => "a follow-up",
                WaitingState::Released => "a release",
                WaitingState::JoinPartPlaceHolder { .. } => "another join",
                _ => "a link",
            };
            self.obs_manager.forward_notif(Log::err(format!(
                "the input at position {position} of join task {join_task_id} was forwarded to \
                 task {last_id}, which already had {found} waiting on its result; it was \
                 dropped, and the join won't get that input"
            )));
        }
    }

    /// Releases a task whose handle is gone, through its forwards like
    /// [`attach`](Self::attach): nobody will follow up on the task at the end of the links.
    ///
    /// If that task's result was kept, it's let go, and an error is written to `Error.log`. If
    /// nothing is there yet, the release is remembered until the result arrives.
    ///
    /// # Arguments
    ///
    /// * `list_starts_from_id` - The id of the task released
    fn release(&mut self, list_starts_from_id: u64) {
        let (last_id, wait_state) = self.waiting_tasks.remove_while_is_link(list_starts_from_id);

        if let WaitingState::HandoffKept(sealed_handoff) = wait_state {
            self.drop_unclaimed_handoff(sealed_handoff);
        } else if wait_state.is_none() {
            self.waiting_tasks.release(last_id);
        }
    }

    /// Declares a join that has just arrived: for each of its tasks, in order, finds where that
    /// task's result arrives, and sets the join up to receive it.
    ///
    /// Each task's links are followed to their end, as in [`attach`](Self::attach), since a
    /// task that forwarded finishes with the outcome of the task at the end. There:
    /// - a kept result goes into the join's slot for that task now. A join fails fast, so the
    ///   first kept error triggers it, and every kept result after that one is late: nobody will
    ///   take it, so an error is written to `Error.log`, and a value is dropped;
    /// - with nothing there yet, the join leaves its follower, with the join's id and the slot's
    ///   position, for [`handle_result`](Self::handle_result) to find when the result arrives.
    ///   That's the same before and after an error: once the join has run, the result the
    ///   follower waits for is late too.
    ///
    /// Nothing else can be there, since the task's handle went into the join. If something is
    /// (only a bug in Thryve could cause it), it's dropped with a line in `Error.log`, and unless
    /// another of its tasks fails, the join waits for that task until the shutdown.
    ///
    /// After the walk, the join is queued if an error triggered it or no slot is left empty;
    /// otherwise it waits in `waiting_joins`.
    ///
    /// # Arguments
    ///
    /// * `join_task` - The join, as declared with `join`
    fn add_join_to_waiting(&mut self, mut join_task: Task) {
        if let Some(input_ids) = join_task.join_get_ids() {
            let mut has_save_been_triggered = false;
            for (i, current_id) in input_ids.into_iter().enumerate() {
                let (id, wait_state) = self.waiting_tasks.remove_while_is_link(current_id);

                match wait_state {
                    WaitingState::None => {
                        self.waiting_tasks.store(
                            id,
                            WaitingState::JoinPartPlaceHolder {
                                join_id: join_task.id,
                                position: i,
                            },
                        );
                    }
                    WaitingState::HandoffKept(sealed_handoff) => {
                        if has_save_been_triggered {
                            self.drop_unclaimed_handoff(sealed_handoff);
                        } else if sealed_handoff.error_text().is_some() {
                            if let Err(sealed_handoff_err) =
                                join_task.join_insert(sealed_handoff, i)
                            {
                                self.drop_unclaimed_handoff(sealed_handoff_err);
                            }

                            has_save_been_triggered = true;
                        } else if let Err(sealed_handoff_err) =
                            join_task.join_insert(sealed_handoff, i)
                        {
                            self.drop_unclaimed_handoff(sealed_handoff_err);
                        }
                    }
                    // The walk never ends on a link, and the task's handle went into the join,
                    // so nothing else can wait on its result.
                    other => {
                        let found = match other {
                            WaitingState::FollowUP(_) => "a follow-up",
                            WaitingState::Released => "a release",
                            WaitingState::JoinPartPlaceHolder { .. } => "another join",
                            _ => "a link",
                        };
                        self.obs_manager.forward_notif(Log::err(format!(
                            "join task '{}' found {found} waiting on its input at position {i} \
                             (task {id}); it was dropped, and unless another input fails, the \
                             join will wait for that input until the shutdown",
                            join_task.task_name()
                        )));
                    }
                }
            }

            if let Some(remaining_inputs) = join_task.waiting_on_join() {
                if remaining_inputs == 0 || has_save_been_triggered {
                    let join_task_ready =
                        join_task.with_join_inputs(self.config.tasks.max_priority);
                    self.plugin_manager.threadpool.submit_task(join_task_ready);
                } else {
                    self.waiting_joins.store(join_task.id, join_task);
                }
            }
        }
    }

    /// Puts one of a join's results into its slot, at `position`, and queues the join once none
    /// of its slots is empty, or at once if the result is an error, since a join fails fast. If
    /// the join's priority was inherited, it's capped below realtime as it becomes ready.
    ///
    /// Only for a join still waiting in `waiting_joins`: both callers check first, and let the
    /// result go if the join has already run. Here, a join that isn't there drops the result
    /// without a word. A slot that's already full hands the result back, and it's let go.
    ///
    /// # Arguments
    ///
    /// * `sealed_handoff` - The result: a finished task's, never a forward
    /// * `join_task_id` - The join's id, its key in `waiting_joins`
    /// * `position` - The slot the result goes into
    fn fill_join_slot(
        &mut self,
        sealed_handoff: SealedHandoff,
        join_task_id: u64,
        position: usize,
    ) {
        let is_sealed_handoff_an_error = sealed_handoff.error_text().is_some();

        if let Some(join_task) = self.waiting_joins.get_mut(join_task_id) {
            let inserted_result = join_task.join_insert(sealed_handoff, position);
            let remaining_inputs_opt = join_task.waiting_on_join();

            if let Err(sealed_handoff_err) = inserted_result {
                self.drop_unclaimed_handoff(sealed_handoff_err);
            } else if let Some(remaining_inputs) = remaining_inputs_opt
                && (remaining_inputs == 0 || is_sealed_handoff_an_error)
                && let Some(join_task) = self.waiting_joins.take(join_task_id)
            {
                let join_task_ready = join_task.with_join_inputs(self.config.tasks.max_priority);
                self.plugin_manager.threadpool.submit_task(join_task_ready);
            }
        }
    }

    /// Handles graph visualization for a task.
    ///
    /// When graph visualization is enabled, this method captures the
    /// call site information from tasks and sends notifications to
    /// the topology handler observer to build the task call graph.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to process for graph visualization
    pub fn handle_graph_visualization(&mut self, task: &Task) {
        if let Some(callsite) = task.callsite.clone() {
            let topo_node =
                TopologyNode::new(callsite.clone(), callsite.this_callsite_fingerprint.clone());
            let notif = TopologyModifier::add_node(topo_node);
            self.obs_manager.forward_notif(notif);

            if callsite.current_executing_task_fingerprint != String::default() {
                let notif = TopologyModifier::add_edge(
                    callsite.current_executing_task_fingerprint,
                    callsite.this_callsite_fingerprint,
                );
                self.obs_manager.forward_notif(notif);
            }
        }
    }

    /// Waits for the given threads to finish, emptying the mailboxes while they run, until the
    /// deadline if there is one.
    ///
    /// The engine is the only reader of its mailboxes, so a thread that's still running may be
    /// blocked on a full one, waiting for the engine. Each round reads what's waiting in all four
    /// (see [`finalize_mailboxes`](Self::finalize_mailboxes)) and sleeps 10 ms if nothing came
    /// in. That's a fixed tick, not `idle_wait`: a thread finishing doesn't arrive in a mailbox,
    /// so the wait has to look again soon.
    ///
    /// Once every thread has finished, it joins them, which returns at once. If the deadline
    /// comes first, it joins the threads that have finished and leaves the others running on
    /// their own; for each of those, `Error.log` gets a line naming the thread and what its
    /// [`TaskSlot`] says it was busy with, and `fc` counts it as still running for the shutdown's
    /// summary. Either way, it empties the mailboxes one last time, which catches what a thread
    /// sent between the last round and finishing; the join makes sure the engine sees it.
    ///
    /// A thread that panicked was already reported by the panic hook. Its join error is dropped,
    /// so that the shutdown goes on.
    ///
    /// # Arguments
    ///
    /// * `fc` - Collects what arrives (tasks, results, undelivered notifications and Thryve's own
    ///   status lines) and counts the threads left running (see [`FinalizeCollection`])
    /// * `handles` - The threads to wait for, each with its slot: the plugin workers, the plugins'
    ///   clean-up thread, or the observers' clean-up thread with the observers' own threads. With
    ///   none (a pool that never started), this is a single pass over the mailboxes.
    /// * `grace_period_deadline` - When to stop waiting, or `None` to wait as long as the threads
    ///   take
    /// * `logger` - Once the sorter has stopped, the logger, which then gets the log lines that
    ///   arrive (see [`finalize_mailboxes`](Self::finalize_mailboxes)); `None` before that
    ///
    /// # Returns
    ///
    /// `true` if every thread finished, `false` if any was left running.
    fn finalize_handle_logic(
        &mut self,
        fc: &mut FinalizeCollection,
        handles: Vec<(JoinHandle<()>, TaskSlot)>,
        grace_period_deadline: Option<Instant>,
        logger: Option<Arc<Observer>>,
    ) -> bool {
        let mut is_later_than_grace_period = false;

        // Each round: stop waiting once the deadline has passed; otherwise read what's waiting in
        // the mailboxes, which unblocks a thread waiting on a full one, and sleep a 10 ms tick
        // only when nothing came in.
        while handles.iter().any(|(handle, _)| !handle.is_finished()) {
            if grace_period_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                is_later_than_grace_period = true;
                break;
            }

            if !self.finalize_mailboxes(
                &mut fc.tasks,
                &mut fc.notifs,
                &mut fc.results,
                &mut fc.status_reps,
                logger.clone(),
            ) {
                thread::sleep(Duration::from_millis(10));
            }
        }

        if is_later_than_grace_period {
            // The threads that finished are joined; the others are left behind, each named in
            // `Error.log` with what it was busy with.
            let mut did_all_finish = true;
            for (handle, worker_slot) in handles {
                if !handle.is_finished() {
                    let thread_name = handle.thread().name().unwrap_or("unknown");
                    // The slot holds a task's name for a plugin worker, and a plugin's or an
                    // observer's type for a clean-up thread. It's empty for an observer's own
                    // thread, and for a thread that wasn't busy.
                    let message = match worker_slot.take() {
                        Some(busy_with) => format!(
                            "thread '{thread_name}' was still running '{busy_with}' when the \
                            shutdown grace period ran out"
                        ),
                        None => format!(
                            "thread '{thread_name}' hadn't stopped when the shutdown grace period \
                            ran out"
                        ),
                    };

                    fc.still_running += 1;
                    fc.status_reps.push(Log::err(message));

                    // Dropping the handle detaches the thread: it keeps running on its own, and
                    // nothing waits for it any more.
                    drop(handle);
                    did_all_finish = false;
                } else {
                    let _ = handle.join();
                }
            }
            // What the finished threads sent between the last round and the end.
            self.finalize_mailboxes(
                &mut fc.tasks,
                &mut fc.notifs,
                &mut fc.results,
                &mut fc.status_reps,
                logger,
            );
            return did_all_finish;
        }

        // Every thread has finished, so these joins return at once. What a join adds is that
        // everything the thread sent is now visible to the engine, which the last pass reads.
        for (handle, _) in handles {
            let _ = handle.join();
        }

        self.finalize_mailboxes(
            &mut fc.tasks,
            &mut fc.notifs,
            &mut fc.results,
            &mut fc.status_reps,
            logger,
        );

        true
    }

    /// Reads the four mailboxes, taking only what was in them when it started.
    ///
    /// Tasks and results are collected rather than handled, since the shutdown has started.
    /// Notifications are forwarded to the observers through the sorter. Once it has stopped
    /// (`logger` is set), a log line goes straight into the logger's queue, and any other
    /// notification goes into `notifs`: its observer is stopped or stopping, and the engine no
    /// longer holds it. Status lines are collected for the logger, which writes them at the end.
    /// Releases are dropped: the wait table they're for is emptied once the workers have stopped.
    ///
    /// What arrives while it reads waits for the next call. Reading until the mailboxes are
    /// empty could go on forever while threads keep sending, and the callers check their
    /// deadline only between calls.
    ///
    /// # Returns
    ///
    /// `true` if anything came in.
    fn finalize_mailboxes(
        &mut self,
        tasks: &mut Vec<Task>,
        notifs: &mut Vec<Notif>,
        results: &mut Vec<(u64, TaskResult)>,
        status_reps: &mut Vec<Notif>,
        logger: Option<Arc<Observer>>,
    ) -> bool {
        // How much each mailbox holds right now. The engine is the only reader, so at least this
        // much is there to take, and whatever arrives meanwhile waits for the next call.
        let task_count = self.task_rx.len();
        let notif_count = self.notif_rx.len();
        let res_count = self.res_rx.len();
        let status_count = self.status_rx.len();

        let mut received = false;

        for task in self.task_rx.try_iter().take(task_count) {
            tasks.push(task);
            received = true;
        }

        // Through the sorter while it runs. Once it has stopped (`logger` is set), nothing else
        // routes them: log lines, which are all addressed to the logger, go straight into its
        // queue, and anything else is kept as undelivered, the way the sorter routes by receiver.
        for notif in self.notif_rx.try_iter().take(notif_count) {
            if let Some(logger_obs) = logger.clone() {
                if notif.notif_receiver() != NamedInput::new_logging().inner() {
                    notifs.push(notif);
                } else {
                    logger_obs.obs_queue.push(notif);
                }
            } else {
                self.obs_manager.forward_notif(notif);
            }
            received = true;
        }

        for result in self.res_rx.try_iter().take(res_count) {
            results.push(result);
            received = true;
        }

        for status in self.status_rx.try_iter().take(status_count) {
            if let StatusMsg::LogLine(notif) = status {
                status_reps.push(notif);
            }

            received = true;
        }

        received
    }

    /// How many entries the wait table holds, plus the joins still waiting: follow-ups waiting,
    /// handoffs kept, releases remembered, forward links, joins' followers, and the joins
    /// themselves. Each entry goes once both sides have spoken (a link once a follow-up or a
    /// release has followed it, a join once its last task's result has arrived or one of its tasks
    /// has failed, a follower once its task's result has arrived, even after its join has run),
    /// so this is zero once every task has finished and every handle has been used or dropped.
    ///
    /// For Thryve's tests. Not part of the public API.
    #[doc(hidden)]
    pub fn wait_table_len(&self) -> usize {
        self.waiting_tasks.table.len() + self.waiting_joins.table.len()
    }
}

/// What [`Engine::finalize`] collects on its way, in one place, so the engine's helpers can take
/// it as a whole.
struct FinalizeCollection {
    /// Tasks that never ran: still queued when the workers were stopped, follow-ups still waiting
    /// for a result, joins still waiting for some of their tasks, or sent during the shutdown.
    tasks: Vec<Task>,
    /// Notifications that were never delivered: left in the sorter's queue, or sent after the
    /// sorter had stopped to an observer other than the logger.
    notifs: Vec<Notif>,
    /// Results that arrived during the shutdown, and those kept for a task handle that still
    /// existed; nothing handles them any more.
    results: Vec<(u64, TaskResult)>,
    /// Thryve's own status lines, which the logger writes at the very end, in order.
    status_reps: Vec<Notif>,
    /// How many threads were left running at a deadline: a worker stuck in a task, a `clean_up`
    /// that never returned, or an observer's thread that never stopped.
    still_running: usize,
}

impl FinalizeCollection {
    /// An empty collection.
    fn new() -> Self {
        Self {
            tasks: Vec::new(),
            notifs: Vec::new(),
            results: Vec::new(),
            status_reps: Vec::new(),
            still_running: 0,
        }
    }

    /// Writes a summary of the work the shutdown lost, if it lost any, as status lines for the
    /// logger.
    ///
    /// Counted as lost: the tasks that never ran, the results that carried something (a message,
    /// a follow-up task, or an `AdvTask`'s error that would have been logged; an empty one isn't
    /// lost work), the notifications that were never delivered, and the threads left running at a
    /// deadline. One line gives the total and each count. It goes to `Main.log` after the
    /// service's Stop, and to `Error.log` after any other shutdown. If nothing was lost, there's no
    /// line at all.
    ///
    /// With debugging on, `Debug.log` also gets the details, one entry per line: for each kind,
    /// its count, then each group (tasks by name, notifications by receiver, results by type)
    /// with its size and its items. An item is a notification's type, receiver and creation
    /// time, a result in full, or, with task logging on, the call site of a task that has one.
    ///
    /// It empties `tasks`, `notifs` and `results`. The engine calls it after the last read of the
    /// mailboxes, so nothing more can arrive, and before the status lines go to the logger, so
    /// the logger writes the summary too, before "Engine Finalized".
    ///
    /// # Arguments
    ///
    /// * `eng` - The engine, for its debugging flags
    /// * `is_service_normal_shutdown` - Whether the service's Stop is shutting the engine down
    fn summarize(&mut self, eng: &Engine, is_service_normal_shutdown: bool) {
        let mut s = String::from("");

        let mut count_tasks = 0;
        let mut count_notifs = 0;
        let mut count_results = 0;

        let mut task_map: HashMap<String, Vec<Task>> = HashMap::new();
        let mut notif_map: HashMap<String, Vec<Notif>> = HashMap::new();
        let mut result_map: HashMap<String, Vec<TaskResult>> = HashMap::new();

        while !self.tasks.is_empty() || !self.notifs.is_empty() || !self.results.is_empty() {
            if let Some(task) = self.tasks.pop() {
                task_map
                    .entry(task.task_name().to_string())
                    .or_default()
                    .push(task);

                count_tasks += 1;
            }
            if let Some(notif) = self.notifs.pop() {
                notif_map
                    .entry(notif.notif_receiver().to_string())
                    .or_default()
                    .push(notif);

                count_notifs += 1;
            }
            if let Some((_, result)) = self.results.pop()
                && (result.next_task.is_some()
                    || !result.message.is_empty()
                    || result.handoff_error_text().is_some())
            {
                result_map
                    .entry(result.result_type().to_string())
                    .or_default()
                    .push(result);

                count_results += 1
            }
        }

        let total = count_tasks + count_notifs + count_results + self.still_running;

        if total == 0 {
            return;
        }

        s += &format!(
            "thryve exited leaving {} things not done: {} lost tasks, {} lost notifications, {} lost task results, {} still running",
            total, count_tasks, count_notifs, count_results, self.still_running
        );

        if is_service_normal_shutdown {
            self.status_reps.push(Log::log(s, "Main"));
        } else {
            self.status_reps.push(Log::err(s));
        }

        if eng.config.debugging.is_debugging {
            self.status_reps.push(Log::log(
                format!("  tasks: {count_tasks} not done"),
                "Debug",
            ));
            for (task_name, mut tasks) in task_map.drain() {
                self.status_reps.push(Log::log(
                    format!(" {}: {}", task_name, tasks.len()),
                    "Debug",
                ));

                if eng.config.debugging.log_tasks {
                    for task in tasks.drain(..) {
                        if let Some(callsite) = task.callsite {
                            self.status_reps
                                .push(Log::log(format!("    {}", callsite), "Debug"));
                        }
                    }
                }
            }

            self.status_reps.push(Log::log(
                format!("  notifs: {count_notifs} not done"),
                "Debug",
            ));
            for (notif_receiver, mut notifs) in notif_map.drain() {
                self.status_reps.push(Log::log(
                    format!(" {}: {}", notif_receiver, notifs.len()),
                    "Debug",
                ));

                for notif in notifs.drain(..) {
                    self.status_reps
                        .push(Log::log(format!("    {}", notif), "Debug"));
                }
            }

            self.status_reps.push(Log::log(
                format!("  task results: {count_results} not done"),
                "Debug",
            ));
            for (result_type, mut results) in result_map.drain() {
                self.status_reps.push(Log::log(
                    format!(" {}: {}", result_type, results.len()),
                    "Debug",
                ));

                for result in results.drain(..) {
                    self.status_reps
                        .push(Log::log(format!("    {}", result), "Debug"));
                }
            }
        }
    }
}
