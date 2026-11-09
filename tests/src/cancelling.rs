// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Cancelling.
//!
//! The set and the skip: the engine keeps the ids of the tasks cancelled before they started,
//! and every plugin worker looks each task it pops up in that set. A task whose id is there never
//! runs: the worker reports it as cancelled instead, and the engine takes the id out when that
//! report, or any other report for the task, arrives. These tests put the id in the set by hand,
//! after the engine has remembered the release that a cancel leaves behind, so they check the
//! skip on its own, without `cancel()`.
//!
//! - a worker skips a task whose id is in the set and reports it as cancelled, runs the others,
//!   and leaves the id for the engine to take out;
//! - a cancelled report lets the remembered release go, takes only its own id out, and writes
//!   nothing to `Error.log`;
//! - a task that was already running when it was cancelled still has its error written to
//!   `Error.log`, and its result takes its id out too;
//! - end to end, a task cancelled before a worker takes it never runs, and nothing is left in the
//!   wait table or the set.
//!
//! `cancel()` and `cancel_all()`: a handle's `cancel` uses it up and sends one message.
//! The engine releases the task the way it does for a dropped handle, following its forwards, and
//! if the task at their end hasn't reported yet, puts that task's id in the set. These tests drive
//! the engine's mailboxes one at a time, so each cancel lands at a known moment.
//!
//! - `cancel` sends one cancel, and dropping the used handle sends nothing more;
//! - a task cancelled before the engine takes it in, or while it waits in the queue, never runs;
//! - a task cancelled while it runs finishes, and its error is still written to `Error.log`;
//! - a cancel after the task finished lets the kept result go and adds no id;
//! - a cancel after a forward cancels the task forwarded to;
//! - `cancel_all` cancels every piece of split work.
//!
//! Each of these ends with an empty set and an empty wait table, and nothing about a cancel in
//! `Error.log`.
//!
//! The tasks the engine holds: a follow-up waiting for its input, and a join waiting for
//! its tasks. No worker ever sees them, so the engine catches them itself and reports them as
//! cancelled.
//!
//! - a follow-up cancelled while it waits, or before the engine takes it in, is never queued, and
//!   the error it would have got is written to `Error.log`;
//! - a follow-up queued with an error and cancelled there still has that error written, once; one
//!   that runs takes the error, and nothing is kept for it afterwards;
//! - cancelling a join that's still waiting cancels the tasks it waits for, and the joins among
//!   them, all the way in;
//! - a join cancelled before the engine takes it in lets its kept error go and cancels the rest;
//! - when one of its tasks fails, a join runs with the error, and the tasks it still waited for
//!   are cancelled; one queued that way and then cancelled still has the error written, once;
//! - after a task forwarded, a join's slot holds the id of the task forwarded to, so cancelling
//!   the join cancels that task, whether the forward came before the join was declared or after.
//!
//! These end with an empty set and an empty wait table, which also counts the error texts kept
//! for queued tasks, and nothing about a cancel in `Error.log`.
//!
//! The shutdown: a cancelled task isn't lost work, and an error that nobody took is.
//!
//! - a cancelled task the shutdown finds is neither counted nor listed, while one nobody cancelled
//!   is a lost task;
//! - an error given to a task that never ran, cancelled or not, counts as a lost task result, and
//!   with debugging on, its text goes to `Debug.log`, never to `Error.log`. It's listed under
//!   "errors never taken", a group written only when there are such errors;
//! - so does one given to a task that was skipped just before the shutdown;
//! - an error taken by a task whose result only arrives during the shutdown isn't counted.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender, bounded, unbounded};
use crossbeam_skiplist::SkipSet;

use thryve_core::runtime::engine::Engine;
use thryve_utils::bases::notif::Notif;
use thryve_utils::cfg::config::Config;
use thryve_utils::packets::packet_handle::{PacketHandle, StatusMsg};
use thryve_utils::packets::task::{AdvTask, AdvTaskHandle, Task};
use thryve_utils::queues::priority_queue::PQChild;
use thryve_utils::res::{Handoff, JoinHandoff, SealedHandoff, TaskResult};
use thryve_utils::threads::threadpool::ThreadPool;
use thryve_utils::traits::join::Joinable;

use crate::shutdown_and_startup::{
    finalize_within, fresh_dir, read_log, started_engine, summary_line,
};
use crate::typed_tasks::{
    forwarding, handle_and_mailbox, handle_mailbox_and_status, run_like_a_worker,
    run_until_received, run_until_the_wait_table_is_empty,
};

/// How long a test waits for a report, or for a shutdown.
pub(crate) const WAIT: Duration = Duration::from_secs(10);

/// Calls `check` on the engine until it says something came in, for up to 5 seconds. With
/// `Engine::check_status`, `check_tasks` or `check_results`, it reads that one mailbox only.
pub(crate) fn until(engine: &mut Engine, check: impl Fn(&mut Engine) -> bool, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !check(engine) {
        assert!(Instant::now() < deadline, "{what} never came");
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// A row that tells `ran` it ran, then returns its number.
fn telling_row(name: &str, number: u32, ran: &Sender<u32>) -> AdvTask<u32, std::io::Error> {
    let ran = ran.clone();
    AdvTask::new(name, 2, move || {
        let _ = ran.send(number);
        Ok(number)
    })
    .unwrap()
}

/// A row that tells `started` it started, then waits until `go` lets it go on. It fails with
/// `error` if there's one, and returns 0 otherwise.
fn gated_row(
    name: &str,
    started: Sender<()>,
    go: Receiver<()>,
    error: Option<&'static str>,
) -> AdvTask<u32, std::io::Error> {
    AdvTask::new(name, 2, move || {
        let _ = started.send(());
        let _ = go.recv();
        match error {
            Some(error) => Err(std::io::Error::other(error)),
            None => Ok(0),
        }
    })
    .unwrap()
}

/// Whether `Error.log` in `dir` mentions a cancel at all: a cancel is never an error.
pub(crate) fn error_log_mentions_a_cancel(dir: &std::path::Path) -> bool {
    read_log(dir, "Error.log").to_lowercase().contains("cancel")
}

/// How many times `text` was written to `Error.log` in `dir`.
pub(crate) fn times_logged(dir: &Path, text: &str) -> usize {
    read_log(dir, "Error.log").matches(text).count()
}

/// What a join's closure gets in these tests.
type Joined = JoinHandoff<Vec<u32>, std::io::Error>;

/// A task that fails at once with `error`.
pub(crate) fn failing_row(name: &str, error: &'static str) -> AdvTask<u32, std::io::Error> {
    AdvTask::new(name, 2, move || Err(std::io::Error::other(error))).unwrap()
}

/// A task that waits until `go` lets it go on, then sends `to` and forwards to it.
fn forwarding_on_go(
    name: &str,
    sender: &PacketHandle,
    to: AdvTask<u32, std::io::Error>,
    go: Receiver<()>,
) -> AdvTask<u32, std::io::Error> {
    let sender = sender.clone();
    AdvTask::forward(name, 2, move || {
        let _ = go.recv();
        Ok(sender.send_adv_task(to).unwrap())
    })
    .unwrap()
}

/// A follow-up's closure that tells `ran` it ran, by `name`.
pub(crate) fn telling_follow_up(
    name: &'static str,
    ran: &Sender<&'static str>,
) -> impl FnOnce(Handoff<u32, std::io::Error>) -> Result<u32, std::io::Error> + Send + 'static {
    let ran = ran.clone();
    move |_| {
        let _ = ran.send(name);
        Ok(0)
    }
}

/// A join's closure that tells `ran` it ran, by `name`.
fn telling_join(
    name: &'static str,
    ran: &Sender<&'static str>,
) -> impl FnOnce(Joined) -> Result<u32, std::io::Error> + Send + 'static {
    let ran = ran.clone();
    move |_| {
        let _ = ran.send(name);
        Ok(0)
    }
}

/// A join's closure that sends `got` the position and the text of the error it got.
fn reporting_error(
    got: Sender<String>,
) -> impl FnOnce(Joined) -> Result<u32, std::io::Error> + Send + 'static {
    move |joined| {
        let _ = got.send(match joined {
            JoinHandoff::Error { position, error } => format!("{position}: {error}"),
            JoinHandoff::Filled { values } => format!("no error, only {values:?}"),
        });
        Ok(0)
    }
}

/// Sends Row 1, which holds the engine's only worker until the test lets it go on, and waits
/// until it has started. Its handle is dropped, so its release waits in the status mailbox, and
/// its value is let go. Returns what lets it go on.
pub(crate) fn holding_the_worker(engine: &mut Engine, packet_handle: &PacketHandle) -> Sender<()> {
    let (started_tx, started_rx) = unbounded();
    let (go_tx, go_rx) = unbounded();
    let row_1 = gated_row("Row1", started_tx, go_rx, None);
    drop(packet_handle.send_adv_task(row_1).unwrap());
    until(engine, Engine::check_tasks, "Row 1");
    started_rx.recv_timeout(WAIT).expect("Row 1 never started");
    go_tx
}

mod skipping {
    use super::*;

    #[test]
    fn only_a_task_whose_id_is_in_the_set_is_skipped_and_reported_as_cancelled() {
        let config = Config::default();
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle = PacketHandle::new(task_tx, notif_tx, unbounded().0, false);
        let queue = PQChild::new(&config, packet_handle.clone());
        // One worker, so the rows are popped one after the other, in the order sent.
        let mut pool: ThreadPool<PQChild<Task>, Task> = ThreadPool::new(1, queue, packet_handle);
        let (res_tx, res_rx) = bounded::<(u64, TaskResult)>(10);
        let cancelled_tasks = Arc::new(SkipSet::new());

        let (ran_tx, ran_rx) = unbounded::<&'static str>();
        let row = |name: &'static str| {
            let ran_tx = ran_tx.clone();
            let job = move || {
                let _ = ran_tx.send(name);
                TaskResult::new("", String::new(), "")
            };
            Task::new(name, 0, job).unwrap()
        };
        let (row_417, row_418) = (row("Row417"), row("Row418"));
        let (id_417, id_418) = (row_417.id, row_418.id);
        cancelled_tasks.insert(id_417);

        pool.run_threads_plugins(Arc::new(res_tx), cancelled_tasks.clone())
            .unwrap();
        pool.submit_task(row_417);
        pool.submit_task(row_418);

        let mut reports = HashMap::new();
        for _ in 0..2 {
            let (id, result) = res_rx.recv_timeout(WAIT).expect("a report never came");
            reports.insert(id, result);
        }
        pool.finalize_plg(&mut Vec::new(), &mut Vec::new());
        pool.stop_workers(&mut Vec::new());

        // Row 417 never ran; its report is a cancelled handoff.
        let mut skipped = reports.remove(&id_417).expect("no report for Row 417");
        let handoff = skipped
            .take_handoff()
            .expect("the report carries a handoff");
        assert!(handoff.is_cancelled());

        // Row 418 ran as usual, and its result is its own.
        assert_eq!(ran_rx.try_iter().collect::<Vec<_>>(), vec!["Row418"]);
        let mut ran = reports.remove(&id_418).expect("no report for Row 418");
        assert!(ran.take_handoff().is_none());

        // The worker only reads the set: Row 417's id is still there, for the engine to take out.
        assert!(cancelled_tasks.contains(&id_417));
        assert_eq!(cancelled_tasks.len(), 1);
    }
}

mod reports {
    use super::*;

    #[test]
    fn a_cancelled_report_lets_the_release_go_and_takes_only_its_own_id_out() {
        let dir = fresh_dir("cancelling_a_cancelled_report_lets_the_release_go");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (row_600, row_601) = (u64::MAX - 600, u64::MAX - 601);

        // What a cancel leaves behind for Row 600: its handle's release, remembered since nothing
        // is kept for it yet, and its id in the set. Row 601 is cancelled too.
        packet_handle.send_status_release(row_600);
        assert!(engine.check_status(), "the release never came");
        assert_eq!(engine.wait_table_len(), 1);
        engine.cancelled_tasks.insert(row_600);
        engine.cancelled_tasks.insert(row_601);

        engine.handle_result((row_600, TaskResult::new_handoff(SealedHandoff::Cancelled)));

        assert_eq!(engine.wait_table_len(), 0);
        assert!(!engine.cancelled_tasks.contains(&row_600));
        assert!(engine.cancelled_tasks.contains(&row_601));

        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn a_task_cancelled_too_late_still_has_its_error_logged_and_takes_its_id_out() {
        let dir = fresh_dir("cancelling_a_task_cancelled_too_late_still_has_its_error_logged");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);

        // Row 601 was already running when it was cancelled, and it fails anyway.
        let row_601 = AdvTask::new("Row601", 2, || {
            Err::<u32, std::io::Error>(std::io::Error::other("row 601 is corrupt"))
        })
        .unwrap();
        let (_, id, result) = run_like_a_worker(row_601);
        packet_handle.send_status_release(id);
        assert!(engine.check_status(), "the release never came");
        engine.cancelled_tasks.insert(id);

        engine.handle_result((id, result));

        assert!(engine.cancelled_tasks.is_empty());
        assert_eq!(engine.wait_table_len(), 0);

        assert!(finalize_within(engine, WAIT).is_some());
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains("row 601 is corrupt"), "{errors}");
        assert!(!error_log_mentions_a_cancel(&dir), "{errors}");
    }
}

mod end_to_end {
    use super::*;

    #[test]
    fn a_task_cancelled_before_a_worker_takes_it_never_runs_and_leaves_nothing_behind() {
        let dir = fresh_dir("cancelling_a_task_cancelled_before_a_worker_takes_it");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);

        // Row 600 is sent from somewhere else, so the test holds it before the engine does.
        let (sender, mailbox) = handle_and_mailbox(false);
        let (ran_tx, ran_rx) = unbounded();
        let row_600 = AdvTask::new("Row600", 2, move || {
            let _ = ran_tx.send(());
            Ok::<u32, std::io::Error>(600)
        })
        .unwrap();
        drop(sender.send_adv_task(row_600).unwrap());
        let row_600 = mailbox.try_recv().unwrap();
        let id = row_600.id;

        // What a cancel does: the engine remembers the release, and the id goes into the set.
        packet_handle.send_status_release(id);
        assert!(engine.check_status(), "the release never came");
        engine.cancelled_tasks.insert(id);

        // The engine queues Row 600, and the worker that pops it reports it as cancelled.
        engine.accept_task(row_600);
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(ran_rx.try_recv().is_err(), "Row 600 ran");
        assert!(engine.cancelled_tasks.is_empty());

        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }
}

mod handles {
    use super::*;

    #[test]
    fn cancel_sends_one_cancel_and_the_used_handle_sends_nothing_more() {
        let (sender, mailbox, status) = handle_mailbox_and_status();
        let thumbnail = AdvTask::new("Thumbnail", 2, || Ok::<u64, std::io::Error>(64)).unwrap();
        let thumbnail = sender.send_adv_task(thumbnail).unwrap();
        let id = mailbox.try_recv().unwrap().id;

        thumbnail.cancel();

        let sent: Vec<String> = status
            .try_iter()
            .map(|message| match message {
                StatusMsg::Cancel(id) => format!("cancel {id}"),
                StatusMsg::HandleRelease(id) => format!("release {id}"),
                StatusMsg::LogLine(_) => String::from("a status line"),
                StatusMsg::GroupUsageGuide { .. } => String::from("a fan-out group"),
            })
            .collect();
        assert_eq!(sent, vec![format!("cancel {id}")]);
    }

    #[test]
    fn a_task_cancelled_before_the_engine_takes_it_in_never_runs() {
        let dir = fresh_dir("cancelling_cancelled_before_the_engine_takes_it_in");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();

        let row_600 = telling_row("Row600", 600, &ran_tx);
        let row_600 = packet_handle.send_adv_task(row_600).unwrap();
        row_600.cancel();

        // The cancel is read first, so the id is in the set before Row 600 is even queued.
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 1);
        until(&mut engine, Engine::check_tasks, "Row 600");
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(ran_rx.try_recv().is_err(), "Row 600 ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn a_task_cancelled_while_it_waits_in_the_queue_never_runs() {
        let dir = fresh_dir("cancelling_cancelled_while_it_waits_in_the_queue");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();

        // The only worker is busy with Row 1 until the test lets it go on.
        let (started_tx, started_rx) = unbounded();
        let (go_tx, go_rx) = unbounded();
        let row_1 = gated_row("Row1", started_tx, go_rx, None);
        drop(packet_handle.send_adv_task(row_1).unwrap());
        until(&mut engine, Engine::check_tasks, "Row 1");
        started_rx.recv_timeout(WAIT).expect("Row 1 never started");

        // Row 600 waits in the queue behind it, and is cancelled there.
        let row_600 = telling_row("Row600", 600, &ran_tx);
        let row_600 = packet_handle.send_adv_task(row_600).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 600");
        row_600.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 1);

        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(ran_rx.try_recv().is_err(), "Row 600 ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn a_task_cancelled_while_it_runs_finishes_and_its_error_is_still_logged() {
        let dir = fresh_dir("cancelling_cancelled_while_it_runs");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);

        let (started_tx, started_rx) = unbounded();
        let (go_tx, go_rx) = unbounded();
        let row_601 = gated_row("Row601", started_tx, go_rx, Some("row 601 is corrupt"));
        let row_601 = packet_handle.send_adv_task(row_601).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 601");
        started_rx
            .recv_timeout(WAIT)
            .expect("Row 601 never started");

        // Too late: Row 601 is already running. Its id still goes in, until its result arrives.
        row_601.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 1);

        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains("row 601 is corrupt"), "{errors}");
        assert!(!error_log_mentions_a_cancel(&dir), "{errors}");
    }

    #[test]
    fn a_cancel_after_the_task_finished_lets_the_kept_result_go_and_adds_no_id() {
        let dir = fresh_dir("cancelling_cancel_after_the_task_finished");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);

        let hash = AdvTask::new("Hash", 2, || {
            Err::<u32, std::io::Error>(std::io::Error::other("photo.png is corrupt"))
        })
        .unwrap();
        let hash = packet_handle.send_adv_task(hash).unwrap();
        until(&mut engine, Engine::check_tasks, "Hash");
        until(&mut engine, Engine::check_results, "Hash's result");
        assert_eq!(
            engine.wait_table_len(),
            1,
            "Hash's result is kept for its handle"
        );

        hash.cancel();
        until(&mut engine, Engine::check_status, "the cancel");

        // Nothing was left to cancel, so no id went in: none would ever have come out again.
        assert!(engine.cancelled_tasks.is_empty());
        assert_eq!(engine.wait_table_len(), 0);
        assert!(finalize_within(engine, WAIT).is_some());
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains("photo.png is corrupt"), "{errors}");
        assert!(!error_log_mentions_a_cancel(&dir), "{errors}");
    }

    #[test]
    fn a_cancel_after_a_forward_cancels_the_task_forwarded_to() {
        let dir = fresh_dir("cancelling_cancel_after_a_forward");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();

        // Hash forwards to Fix, which then waits in the engine's task mailbox. Hash sends Fix only
        // once `go` lets it, after the read that took Hash in: a read takes in every task already
        // sent, so it could take in Fix too.
        let (go_tx, go_rx) = unbounded();
        let fix = telling_row("Fix", 2, &ran_tx);
        let hash = forwarding_on_go("Hash", &packet_handle, fix, go_rx);
        let hash = packet_handle.send_adv_task(hash).unwrap();
        until(&mut engine, Engine::check_tasks, "Hash");
        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "Hash's forward");

        // Hash will never run again, so the id that goes in is Fix's, at the end of the link.
        hash.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 1);
        until(&mut engine, Engine::check_tasks, "Fix");
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(ran_rx.try_recv().is_err(), "Fix ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn cancel_all_cancels_every_piece_of_split_work() {
        let dir = fresh_dir("cancelling_cancel_all_cancels_every_piece");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded::<usize>();

        let rows: Vec<u32> = (0..100).collect();
        let pieces = packet_handle
            .split_into(rows, "Row", 4, 2, move |chunk, start| {
                let _ = ran_tx.send(start);
                Ok::<usize, std::io::Error>(chunk.len())
            })
            .unwrap();
        assert_eq!(pieces.len(), 4);
        pieces.cancel_all();

        until(&mut engine, Engine::check_status, "the cancels");
        assert_eq!(engine.cancelled_tasks.len(), 4);
        until(&mut engine, Engine::check_tasks, "the pieces");
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(ran_rx.try_recv().is_err(), "a piece ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }
}

mod follow_ups {
    use super::*;

    #[test]
    fn a_follow_up_cancelled_while_it_waits_never_runs_and_the_error_it_waited_for_is_logged() {
        let dir = fresh_dir("cancelling_follow_up_cancelled_while_it_waits");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();

        // Hash runs, and fails once the test lets it go on. LoadImg waits for it in the engine.
        let (started_tx, started_rx) = unbounded();
        let (go_tx, go_rx) = unbounded();
        let hash = gated_row("Hash", started_tx, go_rx, Some("photo.png is corrupt"));
        let load = packet_handle
            .send_adv_task(hash)
            .unwrap()
            .then("LoadImg", telling_follow_up("LoadImg", &ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "Hash and LoadImg");
        started_rx.recv_timeout(WAIT).expect("Hash never started");

        load.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        // LoadImg, and Hash, which nobody needs any more. Hash is already running, so it still
        // fails, and its report takes its id out of the set.
        assert_eq!(engine.cancelled_tasks.len(), 2);

        // Hash's error arrives: LoadImg isn't queued, and nobody takes the error.
        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(ran_rx.try_recv().is_err(), "LoadImg ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert_eq!(times_logged(&dir, "photo.png is corrupt"), 1);
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn a_follow_up_cancelled_before_the_engine_takes_it_in_lets_the_kept_error_go() {
        let dir = fresh_dir("cancelling_follow_up_cancelled_before_the_engine_takes_it_in");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();

        // Hash has failed already, and its error is kept for its handle.
        let hash = failing_row("Hash", "photo.png is corrupt");
        let hash = packet_handle.send_adv_task(hash).unwrap();
        until(&mut engine, Engine::check_tasks, "Hash");
        until(&mut engine, Engine::check_results, "Hash's error");
        assert_eq!(engine.wait_table_len(), 1);

        // LoadImg is declared and cancelled at once, so the engine reads the cancel first.
        let load = hash
            .then("LoadImg", telling_follow_up("LoadImg", &ran_tx))
            .unwrap();
        load.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 1);

        // Taken in, LoadImg finds Hash's error kept: it isn't queued, and the error is let go.
        until(&mut engine, Engine::check_tasks, "LoadImg");
        assert_eq!(engine.wait_table_len(), 0);
        assert!(engine.cancelled_tasks.is_empty());

        assert!(finalize_within(engine, WAIT).is_some());
        assert!(ran_rx.try_recv().is_err(), "LoadImg ran");
        assert_eq!(times_logged(&dir, "photo.png is corrupt"), 1);
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn a_follow_up_queued_with_an_error_and_then_cancelled_still_has_the_error_logged_once() {
        let dir = fresh_dir("cancelling_follow_up_queued_with_an_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();

        // Hash has failed already, and its error is kept for its handle.
        let hash = failing_row("Hash", "photo.png is corrupt");
        let hash = packet_handle.send_adv_task(hash).unwrap();
        until(&mut engine, Engine::check_tasks, "Hash");
        until(&mut engine, Engine::check_results, "Hash's error");

        // LoadImg gets Hash's error at once, and waits in the queue behind Row 1. The engine
        // keeps the error's text, since the worker can't open what LoadImg carries: under Hash's
        // id, with a pointer to it under LoadImg's.
        let go_tx = holding_the_worker(&mut engine, &packet_handle);
        let load = hash
            .then("LoadImg", telling_follow_up("LoadImg", &ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");
        assert_eq!(
            engine.wait_table_len(),
            2,
            "the error's text and its pointer are kept"
        );

        load.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 1);

        // The worker skips LoadImg, and its report has the engine write the error it never took.
        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(ran_rx.try_recv().is_err(), "LoadImg ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert_eq!(times_logged(&dir, "photo.png is corrupt"), 1);
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn a_follow_up_that_runs_with_an_error_takes_it_and_leaves_no_text_behind() {
        let dir = fresh_dir("cancelling_follow_up_that_runs_with_an_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (got_tx, got_rx) = unbounded();

        packet_handle
            .send_adv_task(failing_row("Hash", "photo.png is corrupt"))
            .unwrap()
            .then("LoadImg", move |handoff| {
                let _ = got_tx.send(matches!(handoff, Handoff::Error(_)));
                Ok::<u32, std::io::Error>(0)
            })
            .unwrap();

        assert!(
            run_until_received(&mut engine, &got_rx),
            "LoadImg didn't get Hash's error"
        );
        // LoadImg's own report takes the error's text out, so nothing is left once it has run.
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(finalize_within(engine, WAIT).is_some());
        assert_eq!(times_logged(&dir, "photo.png is corrupt"), 0);
    }
}

mod joins {
    use super::*;

    #[test]
    fn cancelling_a_held_join_cancels_the_rows_it_still_waits_for() {
        let dir = fresh_dir("cancelling_cancelling_a_held_join");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (rows_ran_tx, rows_ran_rx) = unbounded();
        let (joins_ran_tx, joins_ran_rx) = unbounded();

        // Row 416 has finished already, and its value is kept for its handle.
        let row_416 = telling_row("Row416", 416, &rows_ran_tx);
        let row_416 = packet_handle.send_adv_task(row_416).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 416");
        until(&mut engine, Engine::check_results, "Row 416's value");
        assert_eq!(rows_ran_rx.try_recv(), Ok(416));

        // Row 417 holds the only worker, and Row 418 waits in the queue behind it.
        let (started_tx, started_rx) = unbounded();
        let (go_tx, go_rx) = unbounded();
        let row_417 = gated_row("Row417", started_tx, go_rx, None);
        let row_417 = packet_handle.send_adv_task(row_417).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 417");
        started_rx
            .recv_timeout(WAIT)
            .expect("Row 417 never started");
        let row_418 = telling_row("Row418", 418, &rows_ran_tx);
        let row_418 = packet_handle.send_adv_task(row_418).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 418");

        // Save takes Row 416's value and waits for the other two.
        let save = vec![row_416, row_417, row_418]
            .join("Save", telling_join("Save", &joins_ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "Save");
        assert_eq!(engine.wait_table_len(), 3, "Save and its two followers");

        // Save is gone at once, and the two rows it still waited for are cancelled. Their
        // followers stay, for the rows' reports to find Save gone.
        save.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 2);
        assert_eq!(engine.wait_table_len(), 2);

        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(rows_ran_rx.try_recv().is_err(), "Row 418 ran");
        assert!(joins_ran_rx.try_recv().is_err(), "Save ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn cancelling_a_join_cancels_a_held_join_inside_it_all_the_way_in() {
        let dir = fresh_dir("cancelling_cancelling_a_join_of_joins");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (rows_ran_tx, rows_ran_rx) = unbounded();
        let (joins_ran_tx, joins_ran_rx) = unbounded();

        // Row 1 holds the only worker; Rows 2 and 9 wait in the queue behind it.
        let (started_tx, started_rx) = unbounded();
        let (go_tx, go_rx) = unbounded();
        let row_1 = gated_row("Row1", started_tx, go_rx, None);
        let row_1 = packet_handle.send_adv_task(row_1).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 1");
        started_rx.recv_timeout(WAIT).expect("Row 1 never started");
        let row_2 = packet_handle
            .send_adv_task(telling_row("Row2", 2, &rows_ran_tx))
            .unwrap();
        let row_9 = packet_handle
            .send_adv_task(telling_row("Row9", 9, &rows_ran_tx))
            .unwrap();

        // Inner joins Rows 1 and 2, and Outer joins Inner and Row 9. Both wait.
        let inner = vec![row_1, row_2]
            .join("Inner", telling_join("Inner", &joins_ran_tx))
            .unwrap();
        let outer = vec![inner, row_9]
            .join("Outer", telling_join("Outer", &joins_ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "the rows and the joins");
        assert_eq!(
            engine.wait_table_len(),
            6,
            "two joins and their four followers"
        );

        // Both joins are gone, and Rows 1, 2 and 9 are cancelled. Inner's report found Outer
        // gone, so only the rows' followers are left.
        outer.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 3);
        assert_eq!(engine.wait_table_len(), 3);

        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(rows_ran_rx.try_recv().is_err(), "Row 2 or Row 9 ran");
        assert!(joins_ran_rx.try_recv().is_err(), "a join ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn a_join_cancelled_before_the_engine_takes_it_in_lets_its_kept_error_go_and_cancels_the_rest()
    {
        let dir = fresh_dir("cancelling_join_cancelled_before_the_engine_takes_it_in");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (rows_ran_tx, rows_ran_rx) = unbounded();
        let (joins_ran_tx, joins_ran_rx) = unbounded();

        // Row 416 has failed already, and its error is kept for its handle.
        let row_416 = failing_row("Row416", "row 416 is corrupt");
        let row_416 = packet_handle.send_adv_task(row_416).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 416");
        until(&mut engine, Engine::check_results, "Row 416's error");

        // Row 417 holds the only worker, and Row 418 waits in the queue behind it.
        let (started_tx, started_rx) = unbounded();
        let (go_tx, go_rx) = unbounded();
        let row_417 = gated_row("Row417", started_tx, go_rx, None);
        let row_417 = packet_handle.send_adv_task(row_417).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 417");
        started_rx
            .recv_timeout(WAIT)
            .expect("Row 417 never started");
        let row_418 = telling_row("Row418", 418, &rows_ran_tx);
        let row_418 = packet_handle.send_adv_task(row_418).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 418");

        // Save is declared and cancelled at once, so the engine reads the cancel first.
        let save = vec![row_416, row_417, row_418]
            .join("Save", telling_join("Save", &joins_ran_tx))
            .unwrap();
        save.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        until(&mut engine, Engine::check_tasks, "Save");

        // Save was never held: Row 416's error was let go, and Rows 417 and 418 are cancelled.
        // Only their followers are left.
        assert_eq!(engine.cancelled_tasks.len(), 2);
        assert_eq!(engine.wait_table_len(), 2);

        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(rows_ran_rx.try_recv().is_err(), "Row 418 ran");
        assert!(joins_ran_rx.try_recv().is_err(), "Save ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert_eq!(times_logged(&dir, "row 416 is corrupt"), 1);
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn when_a_row_fails_the_join_runs_with_its_error_and_the_rows_it_still_waited_for_are_cancelled()
     {
        let dir = fresh_dir("cancelling_fail_fast_cancels_the_rest");
        // Two workers: one for Load, which Row 418 follows, and one for Row 417.
        let (mut engine, packet_handle) = started_engine(&dir, 10, 2, 1);
        let (rows_ran_tx, rows_ran_rx) = unbounded();
        let (got_tx, got_rx) = unbounded();

        let (started_tx, started_rx) = unbounded();
        let (load_go_tx, load_go_rx) = unbounded();
        let (row_go_tx, row_go_rx) = unbounded();
        let load = gated_row("Load", started_tx.clone(), load_go_rx, None);
        let load = packet_handle.send_adv_task(load).unwrap();
        let row_417 = gated_row("Row417", started_tx, row_go_rx, Some("row 417 is corrupt"));
        let row_417 = packet_handle.send_adv_task(row_417).unwrap();

        // Row 418 follows Load, so it waits in the engine, where no worker can take it early.
        let ran = rows_ran_tx.clone();
        let row_418 = load
            .then("Row418", move |_| {
                let _ = ran.send(418);
                Ok::<u32, std::io::Error>(418)
            })
            .unwrap();
        drop(
            vec![row_417, row_418]
                .join("Save", reporting_error(got_tx))
                .unwrap(),
        );
        until(&mut engine, Engine::check_tasks, "the rows and Save");
        for _ in 0..2 {
            started_rx.recv_timeout(WAIT).expect("a row never started");
        }

        // Row 417's error queues Save at once. Row 418, which Save no longer waits for, is
        // cancelled, and so is Load, which only fed Row 418. Load is already running, so it
        // still finishes, and its report takes its id out of the set.
        row_go_tx.send(()).unwrap();
        assert_eq!(
            run_until_received(&mut engine, &got_rx),
            "0: row 417 is corrupt"
        );
        assert_eq!(engine.cancelled_tasks.len(), 2);

        // Load finishes, and Row 418 is caught before it's queued.
        load_go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(rows_ran_rx.try_recv().is_err(), "Row 418 ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        // Save took the error, so nobody wrote it.
        assert_eq!(times_logged(&dir, "row 417 is corrupt"), 0);
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn a_join_queued_by_a_kept_error_and_then_cancelled_still_has_the_error_logged_once() {
        let dir = fresh_dir("cancelling_join_queued_by_a_kept_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (rows_ran_tx, rows_ran_rx) = unbounded();
        let (joins_ran_tx, joins_ran_rx) = unbounded();

        // Row 417 has failed already, and its error is kept for its handle.
        let row_417 = failing_row("Row417", "row 417 is corrupt");
        let row_417 = packet_handle.send_adv_task(row_417).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 417");
        until(&mut engine, Engine::check_results, "Row 417's error");

        // Row 418 waits in the queue behind Row 1.
        let go_tx = holding_the_worker(&mut engine, &packet_handle);
        let row_418 = telling_row("Row418", 418, &rows_ran_tx);
        let row_418 = packet_handle.send_adv_task(row_418).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 418");

        // Row 417's kept error queues Save at once, and Row 418 is cancelled. The engine keeps
        // the error's text under Row 417's id until Save reports, with a pointer to it under
        // Save's.
        let save = vec![row_417, row_418]
            .join("Save", telling_join("Save", &joins_ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "Save");
        assert_eq!(engine.cancelled_tasks.len(), 1);
        assert_eq!(
            engine.wait_table_len(),
            3,
            "Row 418's follower, and the error's text and its pointer"
        );

        // Save is cancelled in the queue: the worker skips it, so nobody takes Row 417's error.
        save.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 2);

        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(rows_ran_rx.try_recv().is_err(), "Row 418 ran");
        assert!(joins_ran_rx.try_recv().is_err(), "Save ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert_eq!(times_logged(&dir, "row 417 is corrupt"), 1);
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn cancelling_a_join_after_a_row_forwarded_cancels_the_task_it_forwarded_to() {
        let dir = fresh_dir("cancelling_join_cancelled_after_a_forward");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (rows_ran_tx, rows_ran_rx) = unbounded();
        let (joins_ran_tx, joins_ran_rx) = unbounded();

        // Row 417 runs, and forwards to Retry 417 once the test lets it go on, after Save waits
        // for it.
        let (go_tx, go_rx) = unbounded();
        let retry_417 = telling_row("Retry417", 417, &rows_ran_tx);
        let row_417 = forwarding_on_go("Row417", &packet_handle, retry_417, go_rx);
        let row_417 = packet_handle.send_adv_task(row_417).unwrap();
        let save = vec![row_417]
            .join("Save", telling_join("Save", &joins_ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "Row 417 and Save");

        // The forward moves Save's follower on to Retry 417, and Save's slot takes its id.
        // Retry 417 itself waits in the engine's task mailbox.
        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "Row 417's forward");

        // The id that goes in is Retry 417's: Row 417 has reported already.
        save.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 1);

        // Retry 417 is taken in after the cancel, and the worker that pops it skips it.
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(rows_ran_rx.try_recv().is_err(), "Retry 417 ran");
        assert!(joins_ran_rx.try_recv().is_err(), "Save ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn cancelling_a_join_declared_after_a_row_forwarded_cancels_the_task_it_forwarded_to() {
        let dir = fresh_dir("cancelling_join_declared_after_a_forward");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (joins_ran_tx, joins_ran_rx) = unbounded();

        // Row 417 forwards to Retry 417 before Save is declared.
        let (started_tx, started_rx) = unbounded();
        let (go_tx, go_rx) = unbounded();
        let retry_417 = gated_row("Retry417", started_tx, go_rx, None);
        let row_417 = forwarding("Row417", &packet_handle, retry_417);
        let row_417 = packet_handle.send_adv_task(row_417).unwrap();
        until(&mut engine, Engine::check_tasks, "Row 417");
        until(&mut engine, Engine::check_results, "Row 417's forward");

        // Save follows the link to Retry 417, and its slot takes Retry 417's id. Retry 417 then
        // holds the only worker.
        let save = vec![row_417]
            .join("Save", telling_join("Save", &joins_ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "Save");
        started_rx
            .recv_timeout(WAIT)
            .expect("Retry 417 never started");

        // Too late to stop Retry 417, but the id that goes in is its own: Row 417 has reported
        // already.
        save.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 1);

        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(joins_ran_rx.try_recv().is_err(), "Save ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert!(!error_log_mentions_a_cancel(&dir));
    }
}

mod shutdown {
    use super::*;

    /// Sends `task`, has the engine take it in, and waits until its result is kept for its handle,
    /// which is returned.
    fn finished_and_kept(
        engine: &mut Engine,
        packet_handle: &PacketHandle,
        task: AdvTask<u32, std::io::Error>,
    ) -> AdvTaskHandle<u32, std::io::Error> {
        let handle = packet_handle.send_adv_task(task).unwrap();
        until(engine, Engine::check_tasks, "the task");
        until(engine, Engine::check_results, "its result");
        handle
    }

    #[test]
    fn a_cancelled_task_is_not_lost_work_but_one_nobody_cancelled_is() {
        let dir = fresh_dir("cancelling_shutdown_a_cancelled_task_is_not_lost");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        engine.config.debugging.is_debugging = true;
        let (ran_tx, ran_rx) = unbounded();

        // Hash, and LoadImg after it, wait in the engine's task mailbox, and LoadImg is cancelled.
        // The shutdown starts before the engine takes either of them in.
        let hash = failing_row("Hash", "photo.png is corrupt");
        let load = packet_handle
            .send_adv_task(hash)
            .unwrap()
            .then("LoadImg", telling_follow_up("LoadImg", &ran_tx))
            .unwrap();
        load.cancel();
        until(&mut engine, Engine::check_status, "the cancel");

        assert!(finalize_within(engine, WAIT).is_some());
        assert!(ran_rx.try_recv().is_err(), "LoadImg ran");

        // Nobody cancelled Hash, which never ran: lost work. LoadImg was cancelled: not lost.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains(&summary_line(1, 0, 0, 0)), "{errors}");
        let debug = read_log(&dir, "Debug.log");
        assert!(debug.contains("Hash: 1"), "{debug}");
        assert!(!debug.contains("LoadImg"), "{debug}");
        // No error was handed to a task, so there's no group for the errors never taken.
        assert!(!debug.contains("errors never taken"), "{debug}");
    }

    #[test]
    fn errors_given_to_tasks_that_never_ran_are_lost_results_listed_in_debug_log() {
        let dir = fresh_dir("cancelling_shutdown_errors_given_to_tasks_that_never_ran");
        // Row 1 holds the only worker through the shutdown, which gives up on it after a second.
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        engine.config.debugging.is_debugging = true;
        let (ran_tx, ran_rx) = unbounded();

        // Hash and Fix have failed already, and their errors are kept for their handles.
        let hash = failing_row("Hash", "photo.png is corrupt");
        let hash = finished_and_kept(&mut engine, &packet_handle, hash);
        let fix = failing_row("Fix", "fix.png is corrupt");
        let fix = finished_and_kept(&mut engine, &packet_handle, fix);

        // Thumbnail gets Hash's error, and Retry gets Fix's. Both wait in the queue behind Row 1,
        // and Retry is cancelled there.
        let go_tx = holding_the_worker(&mut engine, &packet_handle);
        hash.then("Thumbnail", telling_follow_up("Thumbnail", &ran_tx))
            .unwrap();
        let retry = fix
            .then("Retry", telling_follow_up("Retry", &ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "Thumbnail and Retry");
        assert_eq!(
            engine.wait_table_len(),
            4,
            "both errors' texts and their pointers are kept"
        );
        retry.cancel();
        until(&mut engine, Engine::check_status, "the cancel");

        // The shutdown takes both out of the queue, and Row 1 is still running at its deadline.
        assert!(finalize_within(engine, WAIT).is_some());
        drop(go_tx);
        assert!(ran_rx.try_recv().is_err(), "a follow-up ran");

        // Thumbnail is lost work, and Retry, cancelled, isn't. Both errors are lost results:
        // counted, and written to Debug.log only.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains(&summary_line(1, 0, 2, 1)), "{errors}");
        assert!(!errors.contains("photo.png is corrupt"), "{errors}");
        assert!(!errors.contains("fix.png is corrupt"), "{errors}");
        let debug = read_log(&dir, "Debug.log");
        assert!(debug.contains("errors never taken: 2"), "{debug}");
        assert!(
            debug.contains("task 'Hash' returned an error: photo.png is corrupt"),
            "{debug}"
        );
        assert!(
            debug.contains("task 'Fix' returned an error: fix.png is corrupt"),
            "{debug}"
        );
    }

    #[test]
    fn an_error_given_to_a_task_skipped_just_before_the_shutdown_is_a_lost_result() {
        let dir = fresh_dir("cancelling_shutdown_error_given_to_a_skipped_task");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();

        let hash = failing_row("Hash", "photo.png is corrupt");
        let hash = finished_and_kept(&mut engine, &packet_handle, hash);

        // Retry gets Hash's error, waits in the queue behind Row 1, and is cancelled there.
        let go_tx = holding_the_worker(&mut engine, &packet_handle);
        let retry = hash
            .then("Retry", telling_follow_up("Retry", &ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "Retry");
        retry.cancel();
        until(&mut engine, Engine::check_status, "the cancel");

        // Marker, queued after Retry at a lower priority, tells when the worker has gone past it.
        let (marker_tx, marker_rx) = unbounded();
        let marker = AdvTask::new("Marker", 1, move || {
            let _ = marker_tx.send(());
            Ok::<u32, std::io::Error>(0)
        })
        .unwrap();
        drop(packet_handle.send_adv_task(marker).unwrap());
        until(&mut engine, Engine::check_tasks, "Marker");

        // The worker skips Retry, and its report is still unread when the shutdown starts.
        go_tx.send(()).unwrap();
        marker_rx.recv_timeout(WAIT).expect("Marker never ran");

        assert!(finalize_within(engine, WAIT).is_some());
        assert!(ran_rx.try_recv().is_err(), "Retry ran");

        // Retry never ran, so nobody took Hash's error.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains(&summary_line(0, 0, 1, 0)), "{errors}");
        assert!(!errors.contains("photo.png is corrupt"), "{errors}");
    }

    #[test]
    fn an_error_taken_by_a_task_whose_result_arrives_during_the_shutdown_is_not_counted() {
        let dir = fresh_dir("cancelling_shutdown_error_taken_by_a_task_that_ran");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (got_tx, got_rx) = unbounded();

        let hash = failing_row("Hash", "photo.png is corrupt");
        let hash = finished_and_kept(&mut engine, &packet_handle, hash);

        // Thumbnail gets Hash's error and runs, but the engine reads its result only in the
        // shutdown, with the error's text still kept.
        hash.then("Thumbnail", move |handoff| {
            let _ = got_tx.send(matches!(handoff, Handoff::Error(_)));
            Ok::<u32, std::io::Error>(0)
        })
        .unwrap();
        until(&mut engine, Engine::check_tasks, "Thumbnail");
        assert!(
            got_rx.recv_timeout(WAIT).expect("Thumbnail never ran"),
            "Thumbnail didn't get Hash's error"
        );
        assert_eq!(
            engine.wait_table_len(),
            2,
            "the error's text and its pointer are still kept"
        );

        assert!(finalize_within(engine, WAIT).is_some());

        // Thumbnail took the error, so nothing was lost.
        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains("things not done"), "{errors}");
        assert!(!errors.contains("photo.png is corrupt"), "{errors}");
    }
}
