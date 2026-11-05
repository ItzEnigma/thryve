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
//! This module provides the [`Engine`] struct, the central component of the Thryve runtime. It
//! runs on a thread of its own, drives the plugin manager (the plugins and their worker pool) and
//! the observer manager (the observers and the routing of notifications to them), and is the only
//! reader of four mailboxes.
//!
//! # The mailboxes
//!
//! - Status: Thryve's own messages, sent through a `PacketHandle`: status lines for the logger,
//!   the releases of dropped task handles, cancels, and the data of fan-out groups.
//! - Tasks: what plugins (and the host) send to run, queued for the plugin workers.
//! - Notifications: what plugins send for the observers, such as log lines.
//! - Results: what a worker reports for each task it ran, or skipped because it was cancelled.
//!
//! # The main loop
//!
//! Each call to [`Engine::run_once`] is one turn. It reads the four mailboxes in the order above,
//! taking a bounded number from each, and if they were all empty, it sleeps until one of them gets
//! something. Pausing stops the workers from starting tasks; [`Engine::finalize`] shuts
//! everything down and sums up the work the shutdown lost.
//!
//! # Typed tasks
//!
//! Most of the engine's own logic is about typed tasks (`AdvTask`), whose parts reach it
//! separately and in any order: a follow-up declared with `then` and the result it waits for, a
//! join and the results of its tasks, a fan-out group and the result its followers share, a
//! forward and the task it leads to, and a dropped handle or a cancel and the task it's about.
//! Whichever part arrives first waits in one of the engine's two tables (see
//! [`engine_wait_table`](crate::runtime::engine_wait_table)) until the other side arrives.

use crate::runtime::engine_task_states::TrackedState;
use crate::runtime::engine_task_states::WaitingState;
use crate::runtime::engine_wait_table::WaitTable;
use crate::runtime::observer_manager::ObserverManager;
use crate::runtime::plugin_manager::PluginManager;

use crossbeam_skiplist::SkipSet;
use thryve_utils::bases::notif::Notif;
use thryve_utils::bases::observer::Observer;
use thryve_utils::cfg::config::Config;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::cfg::metrics::Metrics;
use thryve_utils::errors::get_err_oneline;
use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::graphs::graph::TopologyNode;
use thryve_utils::packets::notif_topology::TopologyModifier;
use thryve_utils::packets::packet_handle::StatusMsg;
use thryve_utils::packets::{notif_log::Log, task::Task};
use thryve_utils::res::{SealedHandoff, TaskResult};
use thryve_utils::threads::threadpool::TaskSlot;
use thryve_utils::traits::queue::Queue;

use crossbeam::channel::{Receiver, Select, bounded};
use std::any::Any;
use std::collections::HashMap;
use std::collections::HashSet;
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
/// - `task_rx`, `notif_rx`, `res_rx`, `status_rx`: The four mailboxes (tasks, notifications,
///   results, and Thryve's own status messages)
/// - `waiting_tasks`: The wait table, where typed tasks' follow-ups, results, joins' followers
///   and fan-out groups meet, under the id of the task waited on
/// - `tracked_tasks`: What the engine keeps about a task under the task's own id: joins still
///   waiting, held fan-out followers, where each follow-up waits, and error texts kept for the
///   tasks queued with them, with their pointers
/// - `count_of_ptrs_to_errors`: How many of those pointers there are, so a report can skip the
///   lookup when there are none
/// - `cancelled_tasks`: The ids of the tasks cancelled before they started, shared with the
///   workers
/// - `exit_flag`: Set once the engine is being finalized; the loops stop then
/// - `pause_flag`: Indicates whether the engine is paused
///
/// # Example
///
/// ```ignore
/// // The engine is normally created and run by `thryve_run`, on a thread of its own.
/// let mut engine = Engine::new(&ctx);
/// engine.resume(true);
/// engine.run_once(); // One turn
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
    /// Receiver for Thryve's own messages: status lines, the releases of dropped task handles,
    /// cancels, and fan-out groups' data. Unbounded, so their senders never wait.
    status_rx: Arc<Receiver<StatusMsg>>,
    /// Where a typed task's follow-up and its result meet, under the task's id. An entry is a
    /// follow-up waiting for the result, a result kept for a handle that may still call `then`,
    /// a remembered release, a link to the task a task forwarded to, a join's follower waiting
    /// for one of the join's tasks, a fan-out group waiting for the result its followers share,
    /// or the note at the end of a task's forwards that leads that result back to the group. Each
    /// goes once both sides have arrived (see
    /// [`accept_task`](Self::accept_task), [`handle_result`](Self::handle_result) and
    /// [`check_status`](Self::check_status)), or at the shutdown, which counts what's left.
    waiting_tasks: WaitTable<WaitingState>,
    /// What the engine keeps about a task under the task's own id, where `waiting_tasks` keys by
    /// the id of the task waited on. An entry is one of:
    /// - a join still waiting for some of its tasks: each missing task's follower, in
    ///   `waiting_tasks`, leads here, and the join is taken out and queued when the last one's
    ///   result arrives;
    /// - a fan-out group's follower that arrived before the group's share was there, held until
    ///   the converter runs (see [`run_group_converter`](Self::run_group_converter));
    /// - where a follow-up waits: the id of the task in whose slot it waits, so that a cancel of
    ///   the follow-up can find that task (see [`cancel_task`](Self::cancel_task));
    /// - an error's text, under the id of the task that produced it, kept for the tasks queued
    ///   with the error as their input: a follow-up given the error of the task it followed, a
    ///   join queued early by one of its tasks' errors, or every follower of a fan-out group. A
    ///   worker that skips a cancelled task can't open its input, so the text waits for those
    ///   tasks' reports, with a count of the ones still to come. Any report that isn't a cancel
    ///   means a task took the error, and the text goes; each cancel takes one off, and once
    ///   they've all been cancelled, nobody took the error, and the text is written to
    ///   `Error.log`;
    /// - a pointer to such a text, under the id of a task queued with the error. Its report
    ///   takes the pointer and follows it (see [`store_error`](Self::store_error) and
    ///   [`take_error`](Self::take_error)).
    tracked_tasks: WaitTable<TrackedState>,
    /// How many pointers to error texts `tracked_tasks` holds. Errors are rare, so it's usually
    /// zero, and a report then skips the lookup. Only `store_error_carrier_ptr` adds to it, and
    /// only `take_error` takes from it. The shutdown's summary counts the texts themselves, not
    /// the pointers.
    count_of_ptrs_to_errors: usize,
    /// The ids of the tasks cancelled before they started. Each plugin worker has a clone, and
    /// looks up every task it pops: a task whose id is there isn't run, and the worker reports it
    /// as cancelled instead. An id leaves when any report for its task arrives: that one, or the
    /// task's own result if it was already running when it was cancelled (see
    /// [`handle_result`](Self::handle_result)).
    ///
    /// Only the engine writes to it. Don't add an id yourself: the engine wouldn't know the task
    /// was cancelled, and whoever waits for it could be left without an outcome.
    pub cancelled_tasks: Arc<SkipSet<u64>>,
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
            config: ctx.config.clone(),
            metrics: ctx.meta_data.metrics.clone(),
            plugin_manager: PluginManager::new(ctx, res_tx),
            obs_manager: ObserverManager::new(ctx),
            task_rx: ctx.task_rx.clone(),
            notif_rx: ctx.notif_rx.clone(),
            res_rx,
            status_rx: ctx.status_rx.clone(),
            waiting_tasks: WaitTable::new(),
            tracked_tasks: WaitTable::new(),
            count_of_ptrs_to_errors: 0,
            cancelled_tasks: Arc::new(SkipSet::new()),
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
    /// A turn reads the four mailboxes in order: status messages, tasks, notifications and
    /// results. It takes at most a set number from each (500 status messages, and the
    /// `pipes.*_per_turn` settings for the others), so a busy mailbox can't keep the others
    /// waiting. Status lines and notifications go to the observers, the other status messages
    /// (releases, cancels, fan-out groups' data) are handled, tasks are queued for the workers,
    /// and results are handled.
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
        // Paused: the 500 ms wait comes first, so a loop of paused turns doesn't spin.
        if self.pause_flag {
            self.receive_timeout();
        }

        // Paused or finalized: only status lines and notifications are read.
        if self.exit_flag || self.pause_flag {
            self.check_status();
            self.check_notifs();
            return;
        }

        // A full turn, in this order: status messages, tasks, notifications, results.
        let received_status = self.check_status();
        let received_tasks = self.check_tasks();
        let received_notifs = self.check_notifs();
        let received_results = self.check_results();

        // Nothing came in: sleep until a mailbox gets something, or `idle_wait` runs out.
        if !received_status && !received_tasks && !received_notifs && !received_results {
            self.receive_timeout();
        }
    }

    /// Checks for Thryve's own messages: status lines, releases, cancels and fan-out groups'
    /// data.
    ///
    /// Takes up to 500 messages from the status channel, stopping early once it's empty. A
    /// status line goes to the observer manager, which routes it to the logger. A release means
    /// a typed task's handle was dropped without a follow-up: if the task's result was kept,
    /// nobody will take it now, so its error is written to `Error.log`; if the result hasn't
    /// arrived yet, the release is remembered until it does. If the task forwarded, the release
    /// follows the links and applies to the task at their end. A cancel is a release that also
    /// cancels the task at the end of the links, unless that task has already reported (its result
    /// was kept): then there's nothing left to cancel. The cancel goes through `cancel_task`: a
    /// join still waiting for its tasks is taken out and cancelled at once, with the tasks it
    /// waits for, and any other task's id goes into
    /// [`cancelled_tasks`](Self::cancelled_tasks), where the worker that pops the task finds it,
    /// or the engine does when a follow-up's input arrives. Then the task it waits for is
    /// cancelled too, if nobody else needs it, and so on up.
    ///
    /// A fan-out group's data (`StatusMsg::GroupUsageGuide`) makes the group's entry in the slot
    /// of the task its followers share, with the converter, the followers' ids, and the ones still
    /// live: those not cancelled yet. What the slot held decides what happens next:
    /// - nothing: the task hasn't reported, and its result will find the group. If no follower
    ///   is live, nobody needs the task, and it's cancelled at once (see `cancel_task`);
    /// - its kept result: the converter runs on it at once (see `run_group_converter`);
    /// - a link: the task forwarded, so the forward is followed as if it had just reached the
    ///   group (see `follow_forward_fanout_group`).
    ///
    /// Messages past the 500 wait for the next turn.
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
                    // A cancel releases the handle first. Only a task that hasn't reported yet
                    // (the release was remembered) has anything left to cancel.
                    StatusMsg::Cancel(task_id) => {
                        if let Some(last_id) = self.release(task_id) {
                            self.cancel_task(last_id);
                        }
                    }
                    StatusMsg::GroupUsageGuide {
                        converter,
                        pre_fanout_task_id,
                        fanout_tasks_ids,
                    } => {
                        // What the slot held before the group: nothing, a kept result, or a link.
                        // It comes out so that the group can take the slot.
                        let waiting_state = self.waiting_tasks.remove(pre_fanout_task_id);

                        // The live followers: those not cancelled before the group's data
                        // arrived. With an empty set, that's all of them.
                        let mut live_fanout_tasks_ids: HashSet<u64> =
                            HashSet::with_capacity(fanout_tasks_ids.len());

                        if !self.cancelled_tasks.is_empty() {
                            for fanout_task_id in fanout_tasks_ids.iter() {
                                if self.cancelled_tasks.contains(fanout_task_id) {
                                    continue;
                                }

                                live_fanout_tasks_ids.insert(*fanout_task_id);
                            }
                        } else {
                            for fanout_task_id in fanout_tasks_ids.iter() {
                                live_fanout_tasks_ids.insert(*fanout_task_id);
                            }
                        }

                        // The group, in the slot of the task its followers share. It waits for
                        // that task until a forward says otherwise.
                        self.waiting_tasks.store(
                            pre_fanout_task_id,
                            WaitingState::FanoutGroup {
                                waits_for_id: pre_fanout_task_id,
                                tasks_left_to_queue: fanout_tasks_ids.len(),
                                fanout_tasks_ids,
                                live_fanout_tasks_ids,
                                converter: Some(converter),
                                pre_fanout_task_result_share: None,
                            },
                        );

                        if waiting_state.is_handoff()
                            && let WaitingState::HandoffKept(sealed_handoff) = waiting_state
                        {
                            // The task has already finished: its kept result is shared now.
                            self.run_group_converter(pre_fanout_task_id, sealed_handoff);
                        } else if waiting_state.is_link()
                            && let WaitingState::Link { next_id } = waiting_state
                        {
                            // The task forwarded: the group waits for the task at the end of the
                            // forwards instead.
                            self.follow_forward_fanout_group(pre_fanout_task_id, next_id);
                        } else {
                            // The task hasn't reported yet. If every follower was already
                            // cancelled, nobody needs it, so it's cancelled now.
                            if let Some(task_id) = self
                                .waiting_tasks
                                .pre_fanout_task_to_cancel(pre_fanout_task_id)
                            {
                                self.cancel_task(task_id);
                            }
                        }
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
    /// `tracked_tasks` until they arrive (see `add_join_to_waiting`).
    ///
    /// A fan-out group's follower is queued at once if its group's share is there, and the
    /// group counts it. If the share is an error, the follower also gets a pointer to the error's
    /// record, so its report can find it (see `take_error`). If the group will never give a share,
    /// because the task it follows was cancelled, a follower that was cancelled too is skipped
    /// at once (see `skip_fanned_out_task`). Otherwise it's held in `tracked_tasks` under its own
    /// id until the converter runs (see `run_group_converter`), and if it arrives cancelled, it's
    /// then withdrawn from the group (see `cancel_task`).
    ///
    /// Every task the engine starts goes through here: the ones sent on the task channel, the
    /// follow-up tasks chained to results, and the plugins' init tasks, which startup hands over
    /// directly.
    ///
    /// # Arguments
    ///
    /// * `task` - The task to start
    pub fn accept_task(&mut self, task: Task) {
        // Debug mode: the task's size for the metrics, and its node in the topology graph.
        if self.config.debugging.is_debugging {
            if self.config.debugging.calculate_metrics {
                self.manage_metrics_tasks(&task);
            }

            if self.config.debugging.visual_graph {
                self.handle_graph_visualization(&task);
            }
        }

        // A task that waits for the result of the task `id`.
        if let Some(id) = task.waiting_on_id() {
            if task.is_job_task_wait() {
                // A follow-up declared with `then`: it meets the result in the wait table.
                self.attach(task, id);
                return;
            } else if task.is_job_task_fanout() {
                // A fan-out group's follower.
                if let Some(waiting_state) = self.waiting_tasks.get(id)
                    && waiting_state.is_fanout_group()
                    && let Some(group_share) = self.waiting_tasks.get_group_share(id)
                {
                    // The group's share is there: the follower is queued with it at once, and
                    // the group counts it. If the share is an error, the follower also gets a
                    // pointer to the error's record.
                    let task = task.with_group_share(group_share, self.config.tasks.max_priority);
                    let task_id = task.id;

                    self.plugin_manager.threadpool.submit_task(task);
                    self.waiting_tasks.fanout_group_count_down(id);

                    if let Some(tracked_state) = self.tracked_tasks.get(id)
                        && tracked_state.is_handed_error()
                    {
                        self.store_error_carrier_ptr(id, task_id);
                    }

                    return;
                } else if !self.waiting_tasks.can_fanout_group_get_share(id)
                    && !self.cancelled_tasks.is_empty()
                    && self.cancelled_tasks.contains(&task.id)
                {
                    // The group will never give a share (the task it follows was cancelled),
                    // and this follower was cancelled too: it's skipped at once.
                    self.skip_fanned_out_task(id, task.id);
                    return;
                } else {
                    // No share yet: the follower is held under its own id until the converter
                    // runs. If its cancel was read before it, it's withdrawn from the group now.
                    let task_id = task.id;
                    self.tracked_tasks
                        .store(task_id, TrackedState::HeldFannedOut(task));

                    if !self.cancelled_tasks.is_empty() && self.cancelled_tasks.contains(&task_id) {
                        self.cancel_task(task_id);
                    }

                    return;
                }
            }
        } else if task.waiting_on_join().is_some() {
            // A join: it waits for the results of its tasks.
            self.add_join_to_waiting(task);
            return;
        }

        // Any other task is queued as it is.
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
    ///    which still run; tasks, results and status lines are collected, and releases, cancels
    ///    and fan-out groups' data are dropped. Then it joins the workers. A worker still running
    ///    at the deadline is left behind, and `Error.log` names it and its task. Then the wait
    ///    table is emptied, since results are only collected now: a follow-up still waiting never
    ///    gets its input and never runs, nor does a join still waiting for some of its tasks or a
    ///    fan-out follower still held, and a result kept for a handle that still exists is
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
    ///    never ran (except those cancelled before the shutdown, which weren't meant to run), the
    ///    results that carried something (an error handed to a task that never took it counts as
    ///    one), the notifications never delivered, and the threads left running. If anything was
    ///    lost, one line counts each kind, in `Main.log` after the service's Stop and in
    ///    `Error.log` otherwise; with debugging on, `Debug.log` gets the details;
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
        // in them is lost work. So does a fan-out follower still held: its share never came.
        let mut held_tasks: HashMap<u64, TrackedState> = self
            .tracked_tasks
            .table
            .extract_if(|_, tracked_state| {
                tracked_state.is_held_join() || tracked_state.is_held_fannedout()
            })
            .collect();

        for (_, tracked_state) in held_tasks.drain() {
            if let Some(task) = tracked_state.take_held_join_or_fannedout() {
                fc.tasks.push(task);
            }
        }

        // The cancelled set, and the error texts kept for queued tasks with their pointers, stay
        // as they are: the summary leaves out the cancelled tasks, and counts a text as a lost
        // result unless its task ran and took it, which only the results arriving until the end
        // can tell.

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
    /// - Taking the task's id out of [`cancelled_tasks`](Self::cancelled_tasks) if it's there.
    ///   Any report for a cancelled task takes it out, whether a worker skipped the task or the
    ///   task was already running when it was cancelled. Either way, the report then goes on like
    ///   any other
    /// - Settling the error text kept for the task, if it was queued with an error as its input.
    ///   Any report that isn't a cancel means the task took the error, so the text goes. A
    ///   cancelled report takes one off the count of the tasks the error went to: a follow-up's
    ///   or a join's error went to that one task, and a fan-out group's to each of its
    ///   followers. Once they've all been cancelled, nobody took the error, and its text is
    ///   written to `Error.log` (see `take_error`). This comes before the task's own result is
    ///   handed on, so if that's an error too, its text finds the task's id free
    /// - Handing an `AdvTask`'s handoff to the follow-up waiting for it, which is then queued.
    ///   A follow-up that inherited its priority gets it capped below realtime here, since only
    ///   the engine knows `max_priority`. With no follow-up waiting, the handoff is kept while
    ///   the task's handle exists, for a `then` that may still come. If the handle is already
    ///   gone, an error is written to `Error.log` and a value is dropped. The handoff is taken
    ///   out first, before anything else, and the engine never opens it. A follow-up that was
    ///   cancelled while it waited isn't queued: the handoff is let go, and the engine reports
    ///   the follow-up as cancelled itself, through this same function
    /// - Following a forward: the task's outcome becomes the forwarded-to task's. A follow-up
    ///   already waiting moves on to that task, and a remembered release applies to it; with
    ///   neither, a link is stored for the follow-up or release still to come
    /// - Handing a handoff to the join whose follower waits for it: it goes into the join's slot
    ///   at the follower's position, and the join is queued once none of its slots is empty, or
    ///   at once if the handoff is an error, since a join fails fast; the tasks it still waits
    ///   for are then cancelled. If the join has already run (another of its tasks failed
    ///   first) or was cancelled, nobody will take the handoff: an error is
    ///   written to `Error.log`, and a value is dropped. A forward never goes into a slot: the
    ///   follower moves on to the task forwarded to, even when the join has already run, so that
    ///   task's result finds it (see `attach_follower`)
    /// - Handing a result to the fan-out group waiting for it, in the task's own slot or through
    ///   the note at the end of its forwards: the group's converter runs on it, once, and the
    ///   followers held so far are queued with their shares (see `run_group_converter`). A
    ///   forward that reaches the group isn't a result: the note goes to the end of the new links
    ///   instead, or the converter runs on the result already kept there (see
    ///   `follow_forward_fanout_group`)
    /// - Submitting the task chained to the result
    /// - Logging completion messages
    /// - Triggering shutdown on "Fin" or "FinErr" results
    /// - Letting a cancelled task's report go: it carries nothing, so wherever it lands (a
    ///   release remembered for the task, or a follower whose join has already run) it goes
    ///   without a line, and with task logging on, no "Finished Task ID" line is written for it
    ///
    /// # Arguments
    ///
    /// * `task_result` - Tuple of task ID and result
    pub fn handle_result(&mut self, task_result: (u64, TaskResult)) {
        let (id, mut result) = task_result;
        let mut is_cancelled = false;

        // Any report takes the task's id out of the cancelled set, if it's there.
        let _ = !self.cancelled_tasks.is_empty() && self.cancelled_tasks.remove(&id).is_some();

        // A typed task's handoff: its result, a forward, or a cancel.
        if let Some(sealed_handoff) = result.take_handoff() {
            if sealed_handoff.is_cancelled() {
                is_cancelled = true;
            }

            // If the task was queued with an error as its input, that error is settled first.
            if !(self.count_of_ptrs_to_errors == 0) {
                self.take_error(id, is_cancelled);
            }

            // What waits in the task's slot decides where the handoff goes.
            let wait_state = self.waiting_tasks.get_as_ref(id);

            if wait_state.is_none() {
                // Nothing yet: a result is kept for a `then` that may still come, and a forward
                // leaves a link for it. A cancel has nobody to tell.
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
                    // A forward passes the release on to the task forwarded to; a cancel just
                    // ends it.
                    self.waiting_tasks.remove(id);

                    if let Some(current_id) = sealed_handoff.read_forward_id() {
                        self.release(current_id);
                    }
                }
            } else if wait_state.is_join_part_placeholder() {
                // A join's follower: a forward moves it on to the task forwarded to. A result
                // fills the join's slot if the join still waits, and is let go if it has
                // already run or was cancelled.
                if let WaitingState::JoinPartPlaceHolder { join_id, position } =
                    self.waiting_tasks.remove(id)
                {
                    if let Some(next_id) = sealed_handoff.read_forward_id() {
                        self.attach_follower(next_id, join_id, position);
                    } else if self.tracked_tasks.get_join_as_mut(join_id).is_some() {
                        self.fill_join_slot(sealed_handoff, id, join_id, position);
                    } else {
                        self.drop_unclaimed_handoff(sealed_handoff);
                    }
                }
            } else if wait_state.is_follow_up() {
                if let Some(next_id) = sealed_handoff.read_forward_id()
                    && let Some(task) = self.take_follow_up(id)
                {
                    // A forward: the follow-up moves on to the task forwarded to.
                    self.attach(task, next_id);
                } else if let Some(task) = self.take_follow_up(id) {
                    if !self.cancelled_tasks.is_empty()
                        && self.cancelled_tasks.remove(&task.id).is_some()
                    {
                        // The follow-up was cancelled while it waited: it's reported as
                        // cancelled, and the handoff is let go.
                        self.handle_result((
                            task.id,
                            TaskResult::new_handoff(SealedHandoff::Cancelled),
                        ));
                        self.drop_unclaimed_handoff(sealed_handoff);
                    } else {
                        // The follow-up gets the result as its input and is queued. An error's
                        // text is kept until the follow-up reports.
                        if let Some(sealed_error_text) = sealed_handoff.error_text() {
                            self.store_error(id, task.id, sealed_error_text);
                        }

                        let task = task.with_input(sealed_handoff, self.config.tasks.max_priority);
                        self.plugin_manager.threadpool.submit_task(task);
                    }
                }
            } else if wait_state.is_fanout_group() {
                // A fan-out group waits in the task's own slot.
                self.result_reached_fanout_group(id, sealed_handoff);
            } else if wait_state.is_backwards_link()
                && let Some(pre_fanout_task_id) = self.waiting_tasks.take_backwards_link_id(id)
            {
                // The note at the end of a task's forwards: the handoff goes back to the group,
                // in the slot of the task the group follows.
                self.result_reached_fanout_group(pre_fanout_task_id, sealed_handoff);
            }
        }

        // The task chained to the result, if there is one, is taken in like any new task.
        if let Some(next_task) = result.next_task.take() {
            self.accept_task(next_task);
        }

        // With task logging on, a line for every report but a cancel.
        if self.config.debugging.is_debugging && self.config.debugging.log_tasks && !is_cancelled {
            let notif = Log::log(format!("Finished Task ID: {}.", task_result.0), "Main");

            if self.config.debugging.calculate_metrics {
                self.manage_metrics_notifs(&notif);
            }

            self.obs_manager.forward_notif(notif);
        }

        // The result's message goes to the log file it names. A message or a file without a
        // result type is a mistake in the plugin, written to `Error.log` instead.
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

        // `Fin` and `FinErr` shut the engine down.
        if (result_type == "Fin" || result_type == "FinErr") && !self.exit_flag {
            // Not the service's Stop, so the summary of lost work, if any, goes to `Error.log`.
            self.finalize(false);
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
    /// Releases, cancels and fan-out groups' data are dropped. The wait table they're for is
    /// emptied once the workers have stopped, and no task starts any more, so a cancel has nothing
    /// left to stop. Its task, if it never ran, still counts as lost work in the summary: only a
    /// task cancelled before the shutdown doesn't.
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

        // Tasks and results are only collected: the shutdown neither runs nor handles them.
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

        // Status lines are kept for the logger; releases, cancels and fan-out groups' data are
        // dropped.
        for status in self.status_rx.try_iter().take(status_count) {
            if let StatusMsg::LogLine(notif) = status {
                status_reps.push(notif);
            }

            received = true;
        }

        received
    }

    /// How many entries the wait table holds, plus what `tracked_tasks` holds: follow-ups
    /// waiting, handoffs kept, releases remembered, forward links, joins' followers, the joins
    /// themselves, fan-out groups and the notes that lead to them, followers held until their
    /// group's share is there, where each waiting follow-up waits, and the error texts kept for
    /// queued tasks with their pointers. A waiting follow-up counts twice: its slot, and where it
    /// waits. Each entry goes once both sides have spoken (a link once a follow-up or a release has
    /// followed it, a join once its last task's result has arrived, one of its tasks has failed
    /// or it was cancelled, a follower once its task's result has arrived, even after its join
    /// has run, a group once each of its followers has been queued or skipped, a note once the
    /// result reaches it, a held follower once the converter runs, a pointer once its task has
    /// reported, and a text once a task has taken the error or every task it went to was
    /// cancelled), so this is zero once every task has finished and every handle has been used
    /// or dropped.
    ///
    /// For Thryve's tests. Not part of the public API.
    #[doc(hidden)]
    pub fn wait_table_len(&self) -> usize {
        self.waiting_tasks.table.len() + self.tracked_tasks.table.len()
    }
}

impl Engine {
    /// Keeps an error's text for the one task queued with the error as its input: the text under
    /// the id of the task that produced it, with a count of 1 (see
    /// [`store_handed_error`](Self::store_handed_error)), and a pointer to it under the id of the
    /// task queued with it, which that task's report takes (see
    /// [`store_error_carrier_ptr`](Self::store_error_carrier_ptr) and
    /// [`take_error`](Self::take_error)). It's a fan-out group of one.
    ///
    /// The table keeps an entry that's already there, so both ids must be free, and they are:
    /// the producer has reported, and its report took out the pointer it had if it carried an
    /// error itself. The task queued with it is new.
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task whose result is the error
    /// * `fanout_task_id` - The id of the task queued with it: a follow-up or a join
    /// * `sealed_error_text` - The error's text, as the worker wrote it
    fn store_error(
        &mut self,
        pre_fanout_task_id: u64,
        fanout_task_id: u64,
        sealed_error_text: String,
    ) {
        self.store_handed_error(pre_fanout_task_id, sealed_error_text, 1);
        self.store_error_carrier_ptr(pre_fanout_task_id, fanout_task_id);
    }

    /// Keeps an error's text under the id of the task that produced it, as a record of the error
    /// with a count of the tasks it goes to that haven't reported yet. Each of them gets its
    /// pointer separately, when it's queued (see
    /// [`store_error_carrier_ptr`](Self::store_error_carrier_ptr)).
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task whose result is the error
    /// * `sealed_error_text` - The error's text, as the worker wrote it
    /// * `number_to_pend` - How many tasks the error goes to: 1 for a follow-up or a join, and
    ///   the number of followers in a fan-out group's data, whether they've arrived or not
    fn store_handed_error(
        &mut self,
        pre_fanout_task_id: u64,
        sealed_error_text: String,
        number_to_pend: usize,
    ) {
        self.tracked_tasks.store(
            pre_fanout_task_id,
            TrackedState::HandedError {
                text: sealed_error_text,
                pending: number_to_pend,
            },
        );
    }

    /// Gives a task queued with an error a pointer to the error's record, under the task's own
    /// id, and counts it. A worker's report carries only the task's id, so the pointer is how
    /// the report finds the record (see [`take_error`](Self::take_error)).
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task whose result is the error, where the record is
    /// * `fanout_task_id` - The id of the task queued with it
    fn store_error_carrier_ptr(&mut self, pre_fanout_task_id: u64, fanout_task_id: u64) {
        self.tracked_tasks.store(
            fanout_task_id,
            TrackedState::CarriesErrorOf(pre_fanout_task_id),
        );

        self.count_of_ptrs_to_errors += 1;
    }

    /// Settles the error kept for the task `fanout_task_id`, if it was queued with one, on the
    /// task's report. The pointer under its id comes out first, and is counted off even if the
    /// record it leads to is already gone. Then the record:
    /// - a report that isn't a cancel means the task took the error: the record is taken out and
    ///   dropped, with no line, and later reports find nothing;
    /// - a cancelled report counts the record down, and the last one writes its text (see
    ///   [`count_down_handed_error`](Self::count_down_handed_error)).
    ///
    /// An entry of another kind under its id stays where it is. Called on the task's report,
    /// unless no pointer is kept at all, and at the shutdown for the reports drained then.
    ///
    /// # Arguments
    ///
    /// * `fanout_task_id` - The id of the task that reported
    /// * `is_cancelled` - Whether the report is a cancel
    fn take_error(&mut self, fanout_task_id: u64, is_cancelled: bool) {
        if let Some(pre_fanout_task_id) =
            self.tracked_tasks.take_pre_fanout_error_id(fanout_task_id)
        {
            self.count_of_ptrs_to_errors -= 1;

            if is_cancelled {
                self.count_down_handed_error(pre_fanout_task_id);
            } else {
                drop(self.tracked_tasks.take_handed_error(pre_fanout_task_id));
            }
        }
    }

    /// Takes one off the count of the error record under `pre_fanout_task_id`, for a task the
    /// error went to that was cancelled. At zero, every task it went to was cancelled, so nobody
    /// took the error: the record is taken out, and its text written to `Error.log`, once. Above
    /// zero, the record stays for the tasks still to report; with no record, because a task
    /// already took the error, nothing happens.
    ///
    /// It's the one place that line is written: for a cancelled report, which found the record
    /// through its pointer (see [`take_error`](Self::take_error)), and for a fan-out follower
    /// skipped before it was queued, which has no pointer (see
    /// [`queue_fanned_out_tasks_inner`](Self::queue_fanned_out_tasks_inner)).
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task whose result is the error, where the record is
    fn count_down_handed_error(&mut self, pre_fanout_task_id: u64) {
        let num_of_error_ptrs_opt = self
            .tracked_tasks
            .sub_one_from_handed_error(pre_fanout_task_id);

        if let Some(reports_left) = num_of_error_ptrs_opt
            && reports_left == 0
            && let Some(sealed_error_text) =
                self.tracked_tasks.take_handed_error(pre_fanout_task_id)
        {
            self.obs_manager.forward_notif(Log::err(sealed_error_text));
        }
    }

    /// Lets go of a handoff that nobody will take: no follow-up was declared, and the task's
    /// handle is gone. An error is written to `Error.log`, with the text the worker wrote while
    /// the error's type was still known; a value is just dropped. The engine never opens it.
    ///
    /// A cancelled task's handoff carries nothing, so it goes without a line. A forward never gets
    /// here: the engine follows it instead.
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

    /// Takes out the follow-up waiting for the task `id`, with the record of where it waits,
    /// since it's leaving that slot. The entry is removed even when it holds something else, and
    /// that gives `None`, like [`take_task`](WaitTable::take_task).
    ///
    /// # Arguments
    ///
    /// * `id` - The id of the task the follow-up waits for
    fn take_follow_up(&mut self, id: u64) -> Option<Task> {
        if let Some(task) = self.waiting_tasks.take_task(id) {
            self.tracked_tasks.take_waiting_on(task.id);
            return Some(task);
        }

        None
    }

    /// Attaches a follow-up to the task it follows, through that task's forwards: if the task
    /// forwarded, the follow-up goes to the task forwarded to, and so on to the end of the links.
    /// Each link passed is removed, since only one follow-up or release ever comes through it.
    ///
    /// At the end, if that task's result was kept, the follow-up gets it and is queued, and if
    /// the result is an error, its text is kept until the follow-up reports (see
    /// [`store_error`](Self::store_error)). If nothing is there yet, the follow-up waits there
    /// for the result, with a record of where it waits under its own id.
    ///
    /// A follow-up can be cancelled before it gets here: its cancel is read first when it comes
    /// right after `then`, since each turn reads the status mailbox before the task mailbox, and
    /// after a forward, the result it moves on to may already be kept. Such a follow-up isn't
    /// queued: the kept result is let go, and the follow-up is reported as cancelled. If nothing
    /// is kept yet, it waits like any other, and is then withdrawn: the task it waits for, which
    /// only it needed, is cancelled too (see [`cancel_task`](Self::cancel_task)).
    ///
    /// # Arguments
    ///
    /// * `task` - The follow-up
    /// * `list_starts_from_id` - The id of the task it follows
    fn attach(&mut self, task: Task, list_starts_from_id: u64) {
        // The task whose result the follow-up gets: the end of the forwards.
        let (last_id, wait_state) = self.waiting_tasks.remove_while_is_link(list_starts_from_id);

        if let WaitingState::None = wait_state {
            // Nothing there yet: the follow-up waits in that task's slot, with a record of where
            // it waits. If its cancel was read before it, it's withdrawn at once.
            let task_id = task.id;
            self.tracked_tasks.insert_waiting_on(task_id, last_id);
            self.waiting_tasks.insert_follow_up(last_id, task);

            if self.cancelled_tasks.contains(&task_id) {
                self.cancel_task(task_id);
            }
        } else if let WaitingState::HandoffKept(sealed_handoff) = wait_state {
            // The result was kept: the follow-up gets it and is queued, and an error's text is
            // kept until it reports.
            if self.cancelled_tasks.is_empty() || self.cancelled_tasks.remove(&task.id).is_none() {
                if let Some(sealed_error_text) = sealed_handoff.error_text() {
                    self.store_error(last_id, task.id, sealed_error_text);
                }

                let task = task.with_input(sealed_handoff, self.config.tasks.max_priority);
                self.plugin_manager.threadpool.submit_task(task);
            } else {
                // The follow-up was cancelled: it's reported as cancelled, and the result is
                // let go.
                self.handle_result((task.id, TaskResult::new_handoff(SealedHandoff::Cancelled)));
                self.drop_unclaimed_handoff(sealed_handoff);
            }
        }
    }

    /// Moves a join's follower on to the task its task forwarded to, through that task's own
    /// forwards like [`attach`](Self::attach): the join now waits for the result of the task at
    /// the end of the links. Each link passed is removed.
    ///
    /// At the end, if that task's result was kept (it can arrive before the forward, since the
    /// two travel separately), the join gets it now through
    /// [`fill_join_slot`](Self::fill_join_slot), or it's let go if the join has already run. If
    /// nothing is there yet, the follower waits there for the result, and if the join is still
    /// waiting, its slot takes that task's id, so that a cancel of the join finds the task it
    /// still waits for.
    ///
    /// Nothing else can be there, since the handle of the task forwarded to went into the
    /// forward. If something is (only a bug in Thryve could cause it), it's dropped with a line in
    /// `Error.log`, and the join won't get that input.
    ///
    /// # Arguments
    ///
    /// * `list_starts_from_id` - The id of the task forwarded to
    /// * `join_task_id` - The join's id, its key in `tracked_tasks`
    /// * `position` - The follower's slot in the join
    fn attach_follower(&mut self, list_starts_from_id: u64, join_task_id: u64, position: usize) {
        // The task whose result the join now waits for: the end of the forwards.
        let (last_id, wait_state) = self.waiting_tasks.remove_while_is_link(list_starts_from_id);

        if let WaitingState::None = wait_state {
            // Nothing there yet: the follower waits there, and the join's slot learns that
            // task's id, if the join still waits.
            self.waiting_tasks.store(
                last_id,
                WaitingState::JoinPartPlaceHolder {
                    join_id: join_task_id,
                    position,
                },
            );

            if let Some(join_task) = self.tracked_tasks.get_join_as_mut(join_task_id) {
                join_task.join_new_input_id(last_id, position)
            }
        } else if let WaitingState::HandoffKept(sealed_handoff) = wait_state {
            // The result was kept: it fills the join's slot, or it's let go if the join has
            // already run.
            if self.tracked_tasks.get_join_as_mut(join_task_id).is_some() {
                self.fill_join_slot(sealed_handoff, last_id, join_task_id, position);
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
    ///
    /// # Returns
    ///
    /// The id of the task at the end of the links if the release was remembered: that task hasn't
    /// reported yet, so a cancel can still stop it. `None` if its result was let go, or if
    /// something else was found there.
    fn release(&mut self, list_starts_from_id: u64) -> Option<u64> {
        let (last_id, wait_state) = self.waiting_tasks.remove_while_is_link(list_starts_from_id);

        if let WaitingState::HandoffKept(sealed_handoff) = wait_state {
            // The result was kept, and nobody will take it now.
            self.drop_unclaimed_handoff(sealed_handoff);
        } else if wait_state.is_none() {
            // Nothing there yet: the release waits for the result.
            self.waiting_tasks.release(last_id);
            return Some(last_id);
        }

        None
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
    ///   position, for [`handle_result`](Self::handle_result) to find when the result arrives,
    ///   and the slot takes the id at the end of the links, so that a cancel finds the task the
    ///   join still waits for. That's the same before and after an error: once the join has run,
    ///   the result the follower waits for is late too.
    ///
    /// Nothing else can be there, since the task's handle went into the join. If something is
    /// (only a bug in Thryve could cause it), it's dropped with a line in `Error.log`, and unless
    /// another of its tasks fails, the join waits for that task until the shutdown.
    ///
    /// After the walk, the join is queued if an error triggered it or no slot is left empty;
    /// otherwise it waits in `tracked_tasks`. When an error triggers it, the tasks it still waits
    /// for are cancelled first (see [`set_empty_inputs_to_cancel`](Self::set_empty_inputs_to_cancel)),
    /// and the error's text is kept until the join reports (see [`store_error`](Self::store_error)).
    ///
    /// A join whose cancel was read before it (its id is already in
    /// [`cancelled_tasks`](Self::cancelled_tasks)) is never held or queued. Its walk still leaves
    /// the followers, so that the results of its tasks find it gone and are let go, but a kept
    /// result is let go at once instead of filling a slot, and after the walk,
    /// [`cancel_join_task`](Self::cancel_join_task) cancels the tasks it waits for and reports it.
    ///
    /// # Arguments
    ///
    /// * `join_task` - The join, as declared with `join`
    fn add_join_to_waiting(&mut self, mut join_task: Task) {
        // A join whose cancel was read before it.
        let is_cancelled =
            !self.cancelled_tasks.is_empty() && self.cancelled_tasks.contains(&join_task.id);

        if let Some(input_ids) = join_task.join_get_ids() {
            let mut has_save_been_error_triggered = false;
            for (i, current_id) in input_ids.into_iter().enumerate() {
                // Each task's result arrives at the end of its forwards.
                let (id, wait_state) = self.waiting_tasks.remove_while_is_link(current_id);

                match wait_state {
                    WaitingState::None => {
                        // Not finished yet: the join leaves its follower there, and its slot
                        // takes that task's id.
                        self.waiting_tasks.store(
                            id,
                            WaitingState::JoinPartPlaceHolder {
                                join_id: join_task.id,
                                position: i,
                            },
                        );

                        join_task.join_new_input_id(id, i);
                    }
                    // Already finished: its kept result is taken now.
                    WaitingState::HandoffKept(sealed_handoff) => {
                        if has_save_been_error_triggered || is_cancelled {
                            // After the first error, or for a cancelled join, nobody will take
                            // it.
                            self.drop_unclaimed_handoff(sealed_handoff);
                        } else if let Some(sealed_error_text) = sealed_handoff.error_text() {
                            // The first error: it goes into its slot and triggers the join. Its
                            // text is kept until the join reports.
                            self.store_error(id, join_task.id, sealed_error_text);

                            if let Err(sealed_handoff_err) =
                                join_task.join_insert(sealed_handoff, i)
                            {
                                self.drop_unclaimed_handoff(sealed_handoff_err);
                            }

                            has_save_been_error_triggered = true;
                        } else if let Err(sealed_handoff_err) =
                            join_task.join_insert(sealed_handoff, i)
                        {
                            // A value goes into its slot; a slot already full hands it back,
                            // and it's let go.
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

            // After the walk, with the number of slots still empty.
            if let Some(remaining_inputs) = join_task.waiting_on_join() {
                // An error runs the join early, so the tasks it still waits for are cancelled.
                if has_save_been_error_triggered {
                    self.set_empty_inputs_to_cancel(&join_task);
                }

                if remaining_inputs == 0 || has_save_been_error_triggered {
                    // Every slot is full, or an error triggered it: queued now.
                    let join_task_ready =
                        join_task.with_join_inputs(self.config.tasks.max_priority);
                    self.plugin_manager.threadpool.submit_task(join_task_ready);
                } else if is_cancelled {
                    // Cancelled before it arrived: cancelled with the tasks it waits for.
                    self.cancel_join_task(join_task);
                } else {
                    // Held until the rest of its results arrive.
                    self.tracked_tasks.insert_join(join_task);
                }
            }
        }
    }

    /// Puts one of a join's results into its slot, at `position`, and queues the join once none
    /// of its slots is empty, or at once if the result is an error, since a join fails fast. If
    /// the join's priority was inherited, it's capped below realtime as it becomes ready.
    ///
    /// When an error queues the join early, the tasks it still waits for are cancelled first (see
    /// [`set_empty_inputs_to_cancel`](Self::set_empty_inputs_to_cancel)), once the join has left
    /// `tracked_tasks`: a join among them is reported at once, and its report has to find this one
    /// gone. The error's text is kept until the join reports (see
    /// [`store_error`](Self::store_error)).
    ///
    /// Only for a join still waiting in `tracked_tasks`: both callers check first, and let the
    /// result go if the join has already run. Here, a join that isn't there drops the result
    /// without a word. A slot that's already full hands the result back, and it's let go.
    ///
    /// # Arguments
    ///
    /// * `sealed_handoff` - The result: a finished task's, never a forward
    /// * `pre_fanout_task_id` - The id of the task whose result it is, under which an error's text is
    ///   kept
    /// * `join_task_id` - The join's id, its key in `tracked_tasks`
    /// * `position` - The slot the result goes into
    fn fill_join_slot(
        &mut self,
        sealed_handoff: SealedHandoff,
        pre_fanout_task_id: u64,
        join_task_id: u64,
        position: usize,
    ) {
        // Read before the result moves into the slot.
        let sealed_error_text_opt: Option<String> = sealed_handoff.error_text();
        let is_sealed_handoff_an_error = sealed_error_text_opt.is_some();

        if let Some(join_task) = self.tracked_tasks.get_join_as_mut(join_task_id) {
            let inserted_result = join_task.join_insert(sealed_handoff, position);
            let remaining_inputs_opt = join_task.waiting_on_join();

            if let Err(sealed_handoff_err) = inserted_result {
                // The slot was already full: the result is let go.
                self.drop_unclaimed_handoff(sealed_handoff_err);
            } else if let Some(remaining_inputs) = remaining_inputs_opt
                && (remaining_inputs == 0 || is_sealed_handoff_an_error)
                && let Some(join_task) = self.tracked_tasks.take_join(join_task_id)
            {
                // The last empty slot was filled, or an error triggered the join: it's taken
                // out and queued. An error first cancels the tasks it still waits for, and its
                // text is kept until the join reports.
                if is_sealed_handoff_an_error {
                    self.set_empty_inputs_to_cancel(&join_task);
                }

                if let Some(sealed_error_text) = sealed_error_text_opt {
                    self.store_error(pre_fanout_task_id, join_task_id, sealed_error_text);
                }

                let join_task_ready = join_task.with_join_inputs(self.config.tasks.max_priority);
                self.plugin_manager.threadpool.submit_task(join_task_ready);
            }
        }
    }

    /// Cancels a join that will never run: one a cancel took out of `tracked_tasks`, or one whose
    /// cancel was read before the join itself. The tasks it still waits for are cancelled (see
    /// [`set_empty_inputs_to_cancel`](Self::set_empty_inputs_to_cancel)), and then the engine
    /// reports the join as cancelled itself, through [`handle_result`](Self::handle_result), which
    /// takes its id out of [`cancelled_tasks`](Self::cancelled_tasks) and lets go of whatever
    /// waits on it. The values already in its slots go with it, without a line: a join that's
    /// still waiting holds no error, or it would have run.
    ///
    /// The join must already be out of `tracked_tasks`: a join among its tasks is reported at
    /// once, and that report has to find this one gone, or it would land in its slot.
    ///
    /// # Arguments
    ///
    /// * `join_task` - The join, out of `tracked_tasks`
    fn cancel_join_task(&mut self, join_task: Task) {
        self.set_empty_inputs_to_cancel(&join_task);

        self.handle_result((
            join_task.id,
            TaskResult::new_handoff(SealedHandoff::Cancelled),
        ));
    }

    /// Cancels the tasks a join still waits for: those whose slots are empty. A slot is empty while
    /// the join's follower waits under its id, and slots learn the id at the end of a forward's
    /// links, so that's the task still to report. Each goes through
    /// [`cancel_task`](Self::cancel_task): a join among them that's still waiting is taken out and
    /// cancelled the same way, all the way in, and any other task's id goes into
    /// [`cancelled_tasks`](Self::cancelled_tasks), along with whatever only it needed. The
    /// followers stay, so that the results of tasks already running find the join gone and are let
    /// go.
    ///
    /// Used when a join is cancelled, and when an error runs it early, since it won't wait for the
    /// rest.
    ///
    /// # Arguments
    ///
    /// * `join_task` - The join whose empty slots are cancelled
    fn set_empty_inputs_to_cancel(&mut self, join_task: &Task) {
        if let Some(mut input_ids) = join_task.join_get_ids() {
            for id in input_ids.drain(..) {
                let task = self.waiting_tasks.get_as_ref(id);

                // Only a slot whose follower still waits: that task hasn't reported.
                if task.is_join_part_placeholder() {
                    self.cancel_task(id);
                }
            }
        }
    }

    /// Withdraws the cancelled task `consumer_id` from the task it waits for, and returns that
    /// task if nobody else needs it now: it's the next to cancel. What `tracked_tasks` holds under
    /// the cancelled task's id says where it waits:
    /// - a follow-up, waiting in a slot: the task in that slot, which only it needed. The record
    ///   is taken out, so a second withdraw can't cancel that task again;
    /// - a fan-out follower, held: the task its group waits for, but only if this follower was
    ///   the last live one (see `fanned_out_task_abort`);
    /// - anything else: no task. A join still waiting is cancelled through its tasks instead (see
    ///   [`cancel_join_task`](Self::cancel_join_task)), and an error's record or pointer belongs
    ///   to a task that already has its input, so the task it waited for has reported.
    ///
    /// # Arguments
    ///
    /// * `consumer_id` - The id of the cancelled task
    fn get_producer_id(&mut self, consumer_id: u64) -> Option<u64> {
        let mut copied_id: Option<u64> = None;
        let mut is_waiting_on = false;
        if let Some(tracked_state) = self.tracked_tasks.get(consumer_id) {
            match tracked_state {
                // A follow-up: the id is copied here, and the record taken out below, once the
                // borrow of the table has ended.
                TrackedState::WaitingOn(producer_id) => {
                    copied_id = Some(*producer_id);
                    is_waiting_on = true;
                }
                // A held follower: it comes off its group's live list.
                TrackedState::HeldFannedOut(task) => {
                    if let Some(pre_fanout_task_id) = task.waiting_on_id() {
                        return self
                            .waiting_tasks
                            .fanned_out_task_abort(pre_fanout_task_id, consumer_id);
                    }
                }
                // Nothing above these to withdraw from.
                TrackedState::CarriesErrorOf(_) => (),
                TrackedState::HandedError { .. } => (),
                TrackedState::Heldjoin(_) => (),
            }

            if is_waiting_on {
                self.tracked_tasks.take_waiting_on(consumer_id);
                return copied_id;
            }
        }

        None
    }

    /// Cancels the task `task_id`, then each task above it that nobody needs any more. Every
    /// cancel the engine makes comes through here.
    ///
    /// A join still waiting for its tasks is taken out and cancelled with them (see
    /// [`cancel_join_task`](Self::cancel_join_task)), which ends the walk. Any other task's id
    /// goes into [`cancelled_tasks`](Self::cancelled_tasks), and the walk goes on with the task
    /// it waits for, if this one was the only one that needed it (see
    /// [`get_producer_id`](Self::get_producer_id)). Say Save follows Resize, which follows
    /// LoadImg: cancelling Save cancels Resize, then LoadImg. A task that's already running
    /// isn't stopped: its report takes its id out of the set.
    ///
    /// A loop, not recursion, so a long chain can't run out of stack.
    ///
    /// # Arguments
    ///
    /// * `task_id` - The id of the task to cancel
    fn cancel_task(&mut self, task_id: u64) {
        let mut current_task_id = task_id;

        loop {
            if let Some(join_task) = self.tracked_tasks.take_join(current_task_id) {
                // A join still waiting: cancelled with the tasks it waits for, which ends the
                // walk.
                self.cancel_join_task(join_task);
                break;
            } else if let Some(producer_id) = self.get_producer_id(current_task_id) {
                // Nobody else needs the task this one waits for: it's the next to cancel.
                self.cancelled_tasks.insert(current_task_id);
                current_task_id = producer_id;
            } else {
                // Nothing above it to cancel.
                self.cancelled_tasks.insert(current_task_id);
                break;
            }
        }
    }

    /// Hands what the task `pre_fanout_task_id` produced to the fan-out group waiting in its slot,
    /// whether it arrived there directly or through the note at the end of its forwards. A forward
    /// isn't the result the followers share: it's followed instead (see
    /// [`follow_forward_fanout_group`](Self::follow_forward_fanout_group)). A cancelled report
    /// isn't one either: the task was cancelled because no follower needed it any more, so the
    /// group is cancelled (see [`cancel_fanout_group`](Self::cancel_fanout_group)). Anything else
    /// is, and the converter runs on it (see [`run_group_converter`](Self::run_group_converter)).
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task the group follows, in whose slot it waits
    /// * `sealed_handoff` - What reached the group
    fn result_reached_fanout_group(
        &mut self,
        pre_fanout_task_id: u64,
        sealed_handoff: SealedHandoff,
    ) {
        if let Some(forward_id) = sealed_handoff.read_forward_id() {
            self.follow_forward_fanout_group(pre_fanout_task_id, forward_id);
        } else if sealed_handoff.is_cancelled() {
            self.cancel_fanout_group(pre_fanout_task_id);
        } else {
            self.run_group_converter(pre_fanout_task_id, sealed_handoff);
        }
    }

    /// Follows a forward that reached the fan-out group of the task `pre_fanout_task_id`, so the
    /// result at the end of the links reaches the group. Say LoadImg forwarded to Decode: if
    /// Decode is still running, a note waits in its slot for its result; if Decode already
    /// finished, the converter runs on its kept result now. Used both when the forward reaches
    /// the group, and when the group's data finds the forward's link in LoadImg's slot.
    ///
    /// While the note waits, the group waits for Decode. If no follower is live any more (they
    /// were all cancelled before the group's data arrived, or before LoadImg forwarded), nobody
    /// needs Decode, and it's cancelled at once (see [`cancel_task`](Self::cancel_task)).
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task the group follows, in whose slot it waits
    /// * `forwarded_id` - The id of the task forwarded to
    fn follow_forward_fanout_group(&mut self, pre_fanout_task_id: u64, forwarded_id: u64) {
        // The group now waits for the task at the end of the forwards.
        let waiting_state = self
            .waiting_tasks
            .fanout_group_remove_while_is_link(pre_fanout_task_id, forwarded_id);

        if let WaitingState::HandoffKept(sealed_handoff) = waiting_state {
            // That task has already finished: its kept result is shared now.
            self.run_group_converter(pre_fanout_task_id, sealed_handoff);
        } else {
            // It hasn't: if no follower is live, nobody needs it, and it's cancelled.
            if let Some(task_id) = self
                .waiting_tasks
                .pre_fanout_task_to_cancel(pre_fanout_task_id)
            {
                self.cancel_task(task_id);
            }
        }
    }

    /// Runs the converter of the fan-out group waiting on the task `pre_fanout_task_id` on that
    /// task's result, the one place it runs, then queues the followers held so far (see
    /// [`queue_fanned_out_tasks`](Self::queue_fanned_out_tasks)).
    ///
    /// If the result is an error, its text is kept first, as a record under the task's id with
    /// a count of every follower in the group's data, whether it has arrived or not: each
    /// follower queued with the error gets a pointer to it, and the text is written to
    /// `Error.log` only if every one of them is cancelled (see [`take_error`](Self::take_error)).
    /// The converter's own failure is written to `Error.log`, and the followers stay held.
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task the group follows, in whose slot it waits
    /// * `sealed_handoff` - The task's result, or the result at the end of its forwards
    fn run_group_converter(&mut self, pre_fanout_task_id: u64, sealed_handoff: SealedHandoff) {
        // Read before the converter takes the result.
        let sealed_error_text_opt = sealed_handoff.error_text();

        if let Err(err) = self
            .waiting_tasks
            .put_group_share(pre_fanout_task_id, sealed_handoff)
        {
            // The converter failed: there's no share, and the followers stay held.
            self.obs_manager
                .forward_notif(Log::err(get_err_oneline(&err)))
        } else if let Some(sealed_error_text) = sealed_error_text_opt
            && let Some(number_of_fanout_tasks) =
                self.waiting_tasks.get_fanout_tasks_len(pre_fanout_task_id)
        {
            // The result is an error: its record counts every follower in the group's data.
            self.store_handed_error(
                pre_fanout_task_id,
                sealed_error_text,
                number_of_fanout_tasks,
            );
        }

        self.queue_fanned_out_tasks(pre_fanout_task_id);
    }

    /// Goes through the group's ids, once its share is there. A follower held under an id is
    /// taken out and queued with its share, and gets a pointer to the error's record if the
    /// share is an error. One that was cancelled while it waited is skipped instead (see
    /// [`skip_fanned_out_task`](Self::skip_fanned_out_task)). Either way, the group counts it. An
    /// id with nothing held under it is left alone: that follower is queued when it arrives (see
    /// [`accept_task`](Self::accept_task)).
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task the group follows, in whose slot it waits
    /// * `fanout_tasks_ids` - The followers' ids, taken out of the group
    /// * `share` - The group's share, cloned once per follower queued
    fn queue_fanned_out_tasks_inner(
        &mut self,
        pre_fanout_task_id: u64,
        fanout_tasks_ids: Vec<u64>,
        share: Arc<dyn Any + Send + Sync>,
    ) {
        for task_id in fanout_tasks_ids {
            // Only the followers held so far. The others are queued as they arrive.
            if let Some(fanned_out_task) = self.tracked_tasks.take_fanout_task(task_id) {
                // Cancelled while it was held: skipped.
                if !self.cancelled_tasks.is_empty()
                    && self.cancelled_tasks.remove(&task_id).is_some()
                {
                    self.skip_fanned_out_task(pre_fanout_task_id, task_id);

                    continue;
                }

                // Queued with a clone of the share, and counted. If the share is an error, the
                // follower also gets a pointer to the error's record.
                let fanned_out_task =
                    fanned_out_task.with_group_share(share.clone(), self.config.tasks.max_priority);

                self.plugin_manager.threadpool.submit_task(fanned_out_task);
                self.waiting_tasks
                    .fanout_group_count_down(pre_fanout_task_id);

                if let Some(tracked_state) = self.tracked_tasks.get(pre_fanout_task_id)
                    && tracked_state.is_handed_error()
                {
                    self.store_error_carrier_ptr(pre_fanout_task_id, task_id);
                }
            }
        }
    }

    /// Queues the followers held for the fan-out group waiting on the task `pre_fanout_task_id`,
    /// once the converter has run (see
    /// [`queue_fanned_out_tasks_inner`](Self::queue_fanned_out_tasks_inner)). With no share,
    /// because the converter failed, nothing happens.
    fn queue_fanned_out_tasks(&mut self, pre_fanout_task_id: u64) {
        if let Some(share) = self.waiting_tasks.get_group_share(pre_fanout_task_id)
            && let Some(fanout_tasks_ids) =
                self.waiting_tasks.take_fanout_tasks_ids(pre_fanout_task_id)
        {
            self.queue_fanned_out_tasks_inner(pre_fanout_task_id, fanout_tasks_ids, share);
        }
    }

    /// Skips the fan-out follower `fanout_task_id` instead of queueing it: the engine reports it
    /// as cancelled itself, the group counts it, and if the share is an error, the error's record
    /// is counted down directly (see [`count_down_handed_error`](Self::count_down_handed_error)),
    /// since the follower was never queued to get a pointer.
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task the group follows, in whose slot it waits
    /// * `fanout_task_id` - The follower's id
    fn skip_fanned_out_task(&mut self, pre_fanout_task_id: u64, fanout_task_id: u64) {
        self.handle_result((
            fanout_task_id,
            TaskResult::new_handoff(SealedHandoff::Cancelled),
        ));

        self.waiting_tasks
            .fanout_group_count_down(pre_fanout_task_id);

        self.count_down_handed_error(pre_fanout_task_id);
    }

    /// Cancels the fan-out group waiting on the task `pre_fanout_task_id`, whose cancelled report
    /// reached it: the task was cancelled because every follower had been. The converter is
    /// dropped, so the group will never give a share, and each follower held so far is skipped,
    /// whether or not it's in the set, since it couldn't run on a cancelled input. A follower that
    /// arrives later is skipped at once (see [`accept_task`](Self::accept_task)), and the group
    /// goes with the last one.
    ///
    /// # Arguments
    ///
    /// * `pre_fanout_task_id` - The id of the task the group follows, in whose slot it waits
    fn cancel_fanout_group(&mut self, pre_fanout_task_id: u64) {
        // With neither a converter nor a share, the group will never give a share.
        self.waiting_tasks
            .fanout_group_drop_converter(pre_fanout_task_id);

        // The followers held so far are skipped now; the others, as they arrive.
        if let Some(mut fanout_tasks_ids) =
            self.waiting_tasks.take_fanout_tasks_ids(pre_fanout_task_id)
        {
            for fanout_task_id in fanout_tasks_ids.drain(..) {
                if self
                    .tracked_tasks
                    .take_fanout_task(fanout_task_id)
                    .is_some()
                {
                    self.skip_fanned_out_task(pre_fanout_task_id, fanout_task_id);
                }
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
        // The task's node, and an edge from the task that sent it, if a running task did.
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
}

/// What [`Engine::finalize`] collects on its way, in one place, so the engine's helpers can take
/// it as a whole.
struct FinalizeCollection {
    /// Tasks that never ran: still queued when the workers were stopped, follow-ups still waiting
    /// for a result, joins still waiting for some of their tasks, fan-out followers still held,
    /// or sent during the shutdown. The cancelled ones among them aren't lost work, and the
    /// summary leaves them out.
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
    /// deadline. An error never taken (below) counts as one lost task result. One line gives the
    /// total and each count. It goes to `Main.log` after the service's Stop, and to `Error.log`
    /// after any other shutdown. If nothing was lost, there's no line at all.
    ///
    /// A cancelled task (its id is in the cancelled set) isn't lost work, wherever it was: it
    /// wasn't meant to run, and it didn't. It's neither counted nor listed.
    ///
    /// An error never taken is a text the engine kept for the tasks queued with an error as their
    /// input, which nobody took while the engine ran. It's handled like a kept error: each text
    /// counts as one lost task result, however many tasks carry a pointer to it (a fan-out
    /// group's followers share one), even none (the followers never arrived), and it never gets a
    /// line of its own in `Error.log`. One exception: if a task's own result is among the late
    /// ones, the task ran and took its error, so the text is dropped, and the result goes through
    /// the usual check. A Cancelled result is the opposite case: the task was skipped, so its text
    /// counts. Which way it goes depends on timing, so two runs can count differently.
    ///
    /// With debugging on, `Debug.log` also gets the details, one entry per line: for each kind,
    /// its count, then each group (tasks by name, notifications by receiver, results by type)
    /// with its size and its items, indented one, two and four spaces. An item is a
    /// notification's type, receiver and creation time, a result in full, or, with task logging
    /// on, the call site of a task that has one. The errors never taken, if there are any, come
    /// last among the results, as one group with a text per item.
    ///
    /// It empties `tasks`, `notifs` and `results`. The engine calls it after the last read of the
    /// mailboxes, so nothing more can arrive, and before the status lines go to the logger, so
    /// the logger writes the summary too, before "Engine Finalized".
    ///
    /// # Arguments
    ///
    /// * `eng` - The engine, for its debugging flags, its cancelled set and the error texts it
    ///   kept
    /// * `is_service_normal_shutdown` - Whether the service's Stop is shutting the engine down
    fn summarize(&mut self, eng: &mut Engine, is_service_normal_shutdown: bool) {
        let mut s = String::from("");

        let mut count_tasks = 0;
        let mut count_notifs = 0;
        let mut count_results = 0;

        let mut task_map: HashMap<String, Vec<Task>> = HashMap::new();
        let mut notif_map: HashMap<String, Vec<Notif>> = HashMap::new();
        let mut result_map: HashMap<String, Vec<TaskResult>> = HashMap::new();

        // Counts and groups what was collected: tasks by name, notifications by receiver, and
        // results by type.
        while !self.tasks.is_empty() || !self.notifs.is_empty() || !self.results.is_empty() {
            if let Some(task) = self.tasks.pop() {
                // A cancelled task wasn't meant to run, so it isn't lost work.
                if !eng.cancelled_tasks.is_empty() && eng.cancelled_tasks.contains(&task.id) {
                    continue;
                }

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
            if let Some((id, result)) = self.results.pop() {
                // A late result that isn't a cancel means its task ran and took its error, so
                // the error's record goes.
                if !(eng.count_of_ptrs_to_errors == 0) && !result.is_handoff_cancelled() {
                    eng.take_error(id, false);
                }

                // Lost only if it carried something.
                if result.next_task.is_some()
                    || !result.message.is_empty()
                    || result.handoff_error_text().is_some()
                {
                    result_map
                        .entry(result.result_type().to_string())
                        .or_default()
                        .push(result);

                    count_results += 1
                }
            }
        }

        // The errors never taken: each record left is one lost result.
        let mut tracked_errors: HashMap<u64, TrackedState> = eng
            .tracked_tasks
            .table
            .extract_if(|_, tracked_state| tracked_state.is_handed_error())
            .collect();

        let num_tracked_errors = tracked_errors.len();
        count_results += num_tracked_errors;

        let total = count_tasks + count_notifs + count_results + self.still_running;

        // Nothing was lost: no line at all.
        if total == 0 {
            return;
        }

        // The one line: `Main.log` after the service's Stop, `Error.log` otherwise.
        s += &format!(
            "thryve exited leaving {} things not done: {} lost tasks, {} lost notifications, {} lost task results, {} still running",
            total, count_tasks, count_notifs, count_results, self.still_running
        );

        if is_service_normal_shutdown {
            self.status_reps.push(Log::log(s, "Main"));
        } else {
            self.status_reps.push(Log::err(s));
        }

        // With debugging on, the details go to `Debug.log`, kind by kind.
        if eng.config.debugging.is_debugging {
            // Tasks by name, with each one's call site when task logging is on.
            self.status_reps
                .push(Log::log(format!(" tasks: {count_tasks} not done"), "Debug"));
            for (task_name, mut tasks) in task_map.drain() {
                self.status_reps.push(Log::log(
                    format!("  {}: {}", task_name, tasks.len()),
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

            // Notifications by receiver.
            self.status_reps.push(Log::log(
                format!(" notifs: {count_notifs} not done"),
                "Debug",
            ));
            for (notif_receiver, mut notifs) in notif_map.drain() {
                self.status_reps.push(Log::log(
                    format!("  {}: {}", notif_receiver, notifs.len()),
                    "Debug",
                ));

                for notif in notifs.drain(..) {
                    self.status_reps
                        .push(Log::log(format!("    {}", notif), "Debug"));
                }
            }

            // Results by type, then the errors never taken.
            self.status_reps.push(Log::log(
                format!(" task results: {count_results} not done"),
                "Debug",
            ));
            for (result_type, mut results) in result_map.drain() {
                self.status_reps.push(Log::log(
                    format!("  {}: {}", result_type, results.len()),
                    "Debug",
                ));

                for result in results.drain(..) {
                    self.status_reps
                        .push(Log::log(format!("    {}", result), "Debug"));
                }
            }

            if !(num_tracked_errors == 0) {
                self.status_reps.push(Log::log(
                    format!("  errors never taken: {}", num_tracked_errors),
                    "Debug",
                ));

                for (_, tracked_state) in tracked_errors.drain() {
                    if let Some(error_text) = tracked_state.take_handed_error() {
                        self.status_reps
                            .push(Log::log(format!("    {error_text}"), "Debug"));
                    }
                }
            }
        }
    }
}
