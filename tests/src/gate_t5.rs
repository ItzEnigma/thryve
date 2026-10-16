// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! The gate of milestone T5, fan-in ("join all").
//!
//! Step 1, the join: `join`, called on a `Vec` of handles through the `Joinable` trait, declares
//! one follow-up for several tasks of one type, here Save over a few rows. Save waits with one slot per row, in the order of the handles. Once every slot is
//! full, or as soon as a row fails, its closure gets a `JoinHandoff`: every value in order, or the
//! failed row's position and its own error. These tests fill the slots by hand, with results from
//! rows run the way a worker runs them.
//!
//! - declaring refuses an empty list, and a bad name (releasing the rows); otherwise it uses up
//!   the rows' handles and returns Save's own handle, which works like any other: followed with
//!   `then`, joined again, or released when dropped;
//! - declaring fails cleanly once the engine is gone, and graph mode records the line that did it;
//! - Save waits on the rows' ids, in the order given;
//! - each slot takes one result: a second one, a position past the last slot, or a task that isn't
//!   a join hands the result back, and the count doesn't change;
//! - Save gets every value in the rows' order, whatever order its slots were filled in;
//! - Save gets the failed row's position and error, even when an earlier row is still missing;
//! - a result of the wrong type, a row still missing when none failed, and Save run before it's
//!   ready are reported as errors instead of panicking.
//!
//! Step 2, the engine: when Save arrives, the engine follows each row's links to the end, takes
//! the results already kept, leaves Save's follower where the others will arrive, and holds Save
//! until none is missing. Each arriving result fills its slot, and Save is queued once the last
//! one is in.
//!
//! - rows that finish after Save arrives fill it through their followers, and nothing is left
//!   waiting afterwards;
//! - rows that finished before Save arrived are taken at once, so Save runs without waiting;
//! - a row that forwarded before Save arrived is followed to the task at the end of its links;
//! - a failed row's error reaches Save and isn't written to `Error.log`;
//! - joins nest and chain through the engine: a join of joins, followed with `then`;
//! - 200 joins of 10 rows each get their values in order and leave nothing waiting.
//!
//! Step 3, failing fast: the first error to reach Save makes it run at once, with its slots as
//! they are. A result that reaches Save after that is late, so nobody takes it: an error is
//! written to `Error.log`, and a value is dropped. The rows still running keep their followers,
//! which go once their results arrive.
//!
//! - Save runs as soon as a row fails, while the other rows still run, and their values are
//!   dropped when they arrive;
//! - Save gets the first error to reach it, whatever the rows' order, and an error that arrives
//!   after Save ran is written to `Error.log`;
//! - when Save arrives, the first kept error triggers it, the kept results after that one are let
//!   go, and the rows still running get followers, before the error and after it;
//! - a Save still waiting at the shutdown is one lost task, and the values in its slots aren't
//!   lost work.
//!
//! Step 4, forwards and priority: a row that forwards after Save arrived never puts the forward in
//! its slot; its follower moves on to the task forwarded to, as a follow-up does. Save inherits
//! the highest of its rows' priorities, capped just below realtime, unless it's given one.
//!
//! - rows that forward after Save arrived, once or twice, fill their slots with the last task's
//!   value;
//! - when the task forwarded to finished first, its kept result reaches Save once the forward
//!   arrives;
//! - a forward that arrives after Save ran still moves the follower, so the late result is let go
//!   instead of kept until the shutdown;
//! - Save's priority: the highest of its rows', capped below realtime when it's made ready; one
//!   given with `join_with_priority` is kept, even realtime; graph mode records the line that
//!   called `join_with_priority`.
//!
//! Step 7, splitting work: `split_into` on the packet handle cuts a list into balanced pieces, in
//! order, and sends one typed task per piece; `send_split_tasks` sends one per chunk the caller
//! already cut. Both hand back the pieces' handles, ready to be joined.
//!
//! - the cut is balanced and in order, each piece knowing where its chunk starts, with the name
//!   and priority given; more pieces than rows gives one piece per row; an empty list gives none;
//! - zero pieces and a bad name are refused before anything is sent;
//! - `send_split_tasks` sends one piece per chunk, of any type, each knowing its index;
//! - graph mode records the plugin's line for every piece;
//! - split work joins back through the engine, and a failing piece reaches the join at its index.
//!
//! The mailbox, worker and engine helpers are `gate_t4`'s and `gate_t2`'s. CI runs these tests on
//! one thread (`--test-threads=1`), like the rest of the crate.

use std::fmt;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender, bounded, unbounded};

use thryve_core::runtime::engine::Engine;
use thryve_utils::errors::{
    EngineNotRunningError, JoinError, NamingError, SplitError, ThryveError, get_err,
};
use thryve_utils::packets::packet_handle::{PacketHandle, StatusMsg};
use thryve_utils::packets::task::{AdvTask, AdvTaskHandle, Task};
use thryve_utils::res::{Handoff, JoinHandoff, SealedHandoff};
use thryve_utils::traits::join::Joinable;
use thryve_utils::traits::packets::Packet;

use crate::gate_t2::{finalize_within, fresh_dir, read_log, started_engine, summary_line};
use crate::gate_t4::{
    forwarding, handle_and_mailbox, handle_mailbox_and_status, open, run_like_a_worker,
    run_until_received, run_until_the_wait_table_is_empty,
};

/// A row task's own error.
#[derive(Debug, PartialEq)]
struct RowError {
    /// The row that failed.
    row: u32,
}

impl fmt::Display for RowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "row {} is corrupt", self.row)
    }
}

impl std::error::Error for RowError {}

/// What Save's closure gets.
type Joined = JoinHandoff<Vec<u32>, RowError>;

/// The default `tasks.max_priority`, which the engine passes when it makes Save ready.
const MAX_PRIORITY: u8 = 4;

/// A row that returns `value`.
fn row(value: u32) -> AdvTask<u32, RowError> {
    AdvTask::new("Row", 1, move || Ok(value)).unwrap()
}

/// Row number `row`, which fails.
fn failing_row(row: u32) -> AdvTask<u32, RowError> {
    AdvTask::new("Row", 1, move || Err(RowError { row })).unwrap()
}

/// Save's closure: it hands back what it got, so a test can look at it in Save's result.
fn save(joined: Joined) -> Result<Joined, RowError> {
    Ok(joined)
}

/// Sends `rows`, declares Save over their handles, and runs each row the way a worker does. No
/// engine is involved.
///
/// Returns Save as the engine receives it, then the rows' ids and their sealed results, in the
/// rows' order.
fn declared(rows: Vec<AdvTask<u32, RowError>>) -> (Task, Vec<u64>, Vec<SealedHandoff>) {
    let (handle, mailbox) = handle_and_mailbox(false);
    let handles: Vec<_> = rows
        .into_iter()
        .map(|task| handle.send_adv_task(task).unwrap())
        .collect();
    let count = handles.len();
    handles.join("Save", save).unwrap();

    let mut ids = Vec::new();
    let mut results = Vec::new();
    for _ in 0..count {
        let row = mailbox.try_recv().unwrap();
        ids.push(row.id);
        let (_, mut result) = row.process().unwrap();
        results.push(result.take_handoff().unwrap());
    }

    (mailbox.try_recv().unwrap(), ids, results)
}

/// Makes Save ready, runs it the way a worker does, and opens what its closure handed back.
fn run_save(save: Task) -> Joined {
    let (_, result) = save.with_join_inputs(MAX_PRIORITY).process().unwrap();
    let (joined, error_text) = open::<Joined, RowError>(result);
    assert_eq!(error_text, None);
    joined.unwrap()
}

/// A row that returns `value` once `go` lets it, so a test can hold its result back.
fn gated_row(value: u32, go: Receiver<()>) -> AdvTask<u32, RowError> {
    AdvTask::new("Row", 1, move || {
        let _ = go.recv();
        Ok(value)
    })
    .unwrap()
}

/// Row number `row`, which fails once `go` lets it.
fn gated_failing_row(row: u32, go: Receiver<()>) -> AdvTask<u32, RowError> {
    AdvTask::new("Row", 1, move || {
        let _ = go.recv();
        Err(RowError { row })
    })
    .unwrap()
}

/// A row that tells `ran` it has started, then returns `value`.
fn telling_row(value: u32, ran: Sender<()>) -> AdvTask<u32, RowError> {
    AdvTask::new("Row", 1, move || {
        let _ = ran.send(());
        Ok(value)
    })
    .unwrap()
}

/// A row that returns `value`, at `priority`.
fn row_at(value: u32, priority: u8) -> AdvTask<u32, RowError> {
    AdvTask::new("Row", priority, move || Ok(value)).unwrap()
}

/// The task a row forwards to, which returns `result`.
fn decompress(result: Result<u32, RowError>) -> AdvTask<u32, RowError> {
    AdvTask::new("Decompress", 1, move || result).unwrap()
}

/// A row that sends `to` at once, but forwards to it only once `go` lets it, so the result of
/// `to` can reach the engine before the forward does.
fn forwarding_after(
    sender: &PacketHandle,
    to: AdvTask<u32, RowError>,
    go: Receiver<()>,
) -> AdvTask<u32, RowError> {
    let sender = sender.clone();
    AdvTask::forward("Row", 1, move || {
        let handle = sender.send_adv_task(to).unwrap();
        let _ = go.recv();
        Ok(handle)
    })
    .unwrap()
}

/// Save's error, or a panic naming what it got instead.
fn the_error(joined: Joined) -> (usize, RowError) {
    match joined {
        JoinHandoff::Error { position, error } => (position, error),
        JoinHandoff::Filled { values } => panic!("expected an error, got {values:?}"),
    }
}

/// Save's closure for a running engine: it sends what it got to `got`.
fn reporting(got: Sender<Joined>) -> impl FnOnce(Joined) -> Result<(), RowError> + Send + 'static {
    move |joined| {
        let _ = got.send(joined);
        Ok(())
    }
}

/// A join's closure that adds up its rows, or passes on the failed row's error.
fn sum(joined: Joined) -> Result<u32, RowError> {
    match joined {
        JoinHandoff::Filled { values } => Ok(values.iter().sum()),
        JoinHandoff::Error { error, .. } => Err(error),
    }
}

/// Runs the engine's turns until its wait table holds `entries`, for up to 5 seconds.
fn run_until_the_wait_table_holds(engine: &mut Engine, entries: usize) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while engine.wait_table_len() != entries {
        assert!(
            Instant::now() < deadline,
            "the wait table held {} entries, not {entries}",
            engine.wait_table_len()
        );
        engine.run_once();
    }
}

/// Declaring a join: what `join` refuses, what it uses up, and what it sends.
mod declaring {
    use super::*;

    #[test]
    fn an_empty_list_is_refused() {
        let no_rows: Vec<AdvTaskHandle<u32, RowError>> = Vec::new();

        let refused = no_rows
            .join("Save", save)
            .err()
            .expect("an empty list is refused");
        assert!(matches!(
            &refused,
            ThryveError::Join(JoinError::MonitorsZeroInputs { task_name }) if task_name == "Save"
        ));
        assert_eq!(
            get_err(&refused),
            "join failed\n  caused by: join task 'Save' monitors 0 inputs"
        );
    }

    #[test]
    fn a_bad_name_is_refused_and_the_rows_are_released() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let rows = vec![
            handle.send_adv_task(row(10)).unwrap(),
            handle.send_adv_task(row(20)).unwrap(),
        ];

        assert!(matches!(
            rows.join("", save).err(),
            Some(ThryveError::Naming(NamingError::NameEmpty { .. }))
        ));

        // Only the rows were sent, and their handles were released, in order.
        let row_ids: Vec<u64> = mailbox.try_iter().map(|task| task.id).collect();
        assert_eq!(row_ids.len(), 2);
        let released: Vec<u64> = status
            .try_iter()
            .map(|message| match message {
                StatusMsg::HandleRelease(id) => id,
                StatusMsg::LogLine(_) => panic!("expected only releases"),
            })
            .collect();
        assert_eq!(released, row_ids);
    }

    #[test]
    fn the_rows_handles_are_used_up_and_save_s_handle_releases_like_any_other() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let rows = vec![
            handle.send_adv_task(row(10)).unwrap(),
            handle.send_adv_task(row(20)).unwrap(),
        ];

        let save_handle = rows.join("Save", save).unwrap();
        assert!(status.try_recv().is_err(), "a row's handle sent a release");

        let save_id = mailbox.try_iter().last().unwrap().id;
        drop(save_handle);
        assert!(matches!(
            status.try_recv(),
            Ok(StatusMsg::HandleRelease(id)) if id == save_id
        ));
    }

    #[test]
    fn save_can_be_followed_like_any_task() {
        let (handle, mailbox) = handle_and_mailbox(false);
        let rows = vec![handle.send_adv_task(row(10)).unwrap()];

        rows.join("Save", save)
            .unwrap()
            .then("Upload", |_| Ok::<(), RowError>(()))
            .unwrap();

        // Row, Save, Upload.
        let tasks: Vec<Task> = mailbox.try_iter().collect();
        assert_eq!(tasks[2].waiting_on_id(), Some(tasks[1].id));
    }

    #[test]
    fn a_join_can_join_other_joins() {
        let (handle, mailbox) = handle_and_mailbox(false);
        let first = vec![handle.send_adv_task(row(10)).unwrap()]
            .join("Save", save)
            .unwrap();
        let second = vec![handle.send_adv_task(row(20)).unwrap()]
            .join("Save", save)
            .unwrap();

        vec![first, second]
            .join("Archive", |_| Ok::<(), RowError>(()))
            .unwrap();

        // Row, Save, Row, Save, Archive.
        let tasks: Vec<Task> = mailbox.try_iter().collect();
        assert_eq!(
            tasks[4].join_get_ids(),
            Some(vec![tasks[1].id, tasks[3].id])
        );
    }

    #[test]
    fn save_waits_on_the_rows_ids_in_the_order_given() {
        let (save, ids, _) = declared(vec![row(10), row(20), row(30)]);

        assert_eq!(save.join_get_ids(), Some(ids));
        assert_eq!(save.waiting_on_join(), Some(3));
        // It isn't the follow-up of a single task.
        assert_eq!(save.waiting_on_id(), None);
    }

    #[test]
    fn graph_mode_records_the_line_that_declared_the_join() {
        let (handle, mailbox) = handle_and_mailbox(true);
        let rows = vec![handle.send_adv_task(row(10)).unwrap()];

        let line = line!() + 1;
        rows.join("Save", save).unwrap();

        let save = mailbox.try_iter().last().unwrap();
        let callsite = save.callsite.expect("graph mode records a call site");
        assert_eq!(callsite.line_number, line);
        assert!(
            callsite.file_path.ends_with("gate_t5.rs"),
            "{:?}",
            callsite.file_path
        );
        assert_eq!(callsite.callsite_name, "Save");
    }

    #[test]
    fn declaring_once_the_engine_is_gone_returns_an_error() {
        let (handle, mailbox) = handle_and_mailbox(false);
        let rows = vec![handle.send_adv_task(row(10)).unwrap()];
        drop(mailbox);

        assert!(matches!(
            rows.join("Save", save).err(),
            Some(ThryveError::EngineNotRunning(EngineNotRunningError::Task))
        ));
    }
}

/// Filling Save's slots: each slot takes one result, and a result that can't be placed is handed
/// back.
mod filling {
    use super::*;

    #[test]
    fn each_slot_takes_one_result() {
        let (mut save, _, results) = declared(vec![row(10), row(20), row(30)]);
        let (_, _, mut extra) = run_like_a_worker(row(99));
        let extra = extra.take_handoff().unwrap();

        let mut results = results.into_iter();
        assert!(save.join_insert(results.next().unwrap(), 0).is_ok());
        assert_eq!(save.waiting_on_join(), Some(2));

        // A second result for slot 0, then one past the last slot: both come back, uncounted.
        let extra = save.join_insert(extra, 0).unwrap_err();
        assert!(save.join_insert(extra, 3).is_err());
        assert_eq!(save.waiting_on_join(), Some(2));

        for (position, result) in results.enumerate() {
            assert!(save.join_insert(result, position + 1).is_ok());
        }
        assert_eq!(save.waiting_on_join(), Some(0));

        // Slot 0 kept its first result.
        assert!(matches!(
            run_save(save),
            JoinHandoff::Filled { values } if values == [10, 20, 30]
        ));
    }

    #[test]
    fn a_task_that_is_not_a_join_hands_the_result_back_and_runs_as_itself() {
        let (handle, mailbox) = handle_and_mailbox(false);
        handle.send_adv_task(row(10)).unwrap();
        let mut task = mailbox.try_recv().unwrap();
        let (_, _, mut other) = run_like_a_worker(row(20));

        assert!(task.join_insert(other.take_handoff().unwrap(), 0).is_err());
        assert_eq!(task.join_get_ids(), None);
        assert_eq!(task.waiting_on_join(), None);

        let (_, result) = task.with_join_inputs(MAX_PRIORITY).process().unwrap();
        assert!(matches!(open::<u32, RowError>(result).0, Ok(10)));
    }
}

/// Running Save: what its closure gets, and what's reported instead when its slots don't add up.
mod running {
    use super::*;

    #[test]
    fn save_gets_every_value_in_the_rows_order() {
        let (mut save, _, results) = declared(vec![row(10), row(20), row(30)]);

        // Filled back to front: the order the rows finish in doesn't matter.
        for (position, result) in results.into_iter().enumerate().rev() {
            assert!(save.join_insert(result, position).is_ok());
        }

        match run_save(save) {
            JoinHandoff::Filled { values } => assert_eq!(values, [10, 20, 30]),
            JoinHandoff::Error { position, error } => {
                panic!("expected every value, got position {position}'s error: {error}")
            }
        }
    }

    #[test]
    fn save_gets_the_failed_row_s_position_and_error_even_with_an_earlier_row_missing() {
        // The first row is still running when the second fails, so Save is made ready with slot 0
        // empty.
        let (mut save, _, results) = declared(vec![row(10), failing_row(2), row(30)]);
        let mut results: Vec<Option<SealedHandoff>> = results.into_iter().map(Some).collect();
        assert!(save.join_insert(results[2].take().unwrap(), 2).is_ok());
        assert!(save.join_insert(results[1].take().unwrap(), 1).is_ok());

        match run_save(save) {
            JoinHandoff::Error { position, error } => {
                assert_eq!(position, 1);
                assert_eq!(error, RowError { row: 2 });
            }
            JoinHandoff::Filled { values } => panic!("expected row 2's error, got {values:?}"),
        }
    }

    #[test]
    fn a_result_of_the_wrong_type_is_reported() {
        // Save waits for `Result<u32, RowError>`s; this one holds a `Result<String, RowError>`.
        let (mut save, _, results) = declared(vec![row(10), row(20)]);
        let name = AdvTask::new("Name", 1, || Ok::<String, RowError>("a.png".to_string()));
        let (_, _, mut wrong) = run_like_a_worker(name.unwrap());

        let mut results = results.into_iter();
        assert!(save.join_insert(results.next().unwrap(), 0).is_ok());
        assert!(save.join_insert(wrong.take_handoff().unwrap(), 1).is_ok());

        let (_, result) = save.with_join_inputs(MAX_PRIORITY).process().unwrap();
        assert_eq!(result.result_type(), "JoinError");
        assert_eq!(result.log_msg_to_file(), "Error");
        assert_eq!(
            result.message,
            "join task 'Save' received a wrong type for position '1'"
        );
    }

    #[test]
    fn a_row_still_missing_when_none_failed_is_reported_at_the_first_empty_slot() {
        let (mut save, _, results) = declared(vec![row(10), row(20), row(30)]);
        let mut results = results.into_iter();
        assert!(save.join_insert(results.next().unwrap(), 0).is_ok());

        let (_, result) = save.with_join_inputs(MAX_PRIORITY).process().unwrap();
        assert_eq!(result.result_type(), "JoinError");
        assert_eq!(result.log_msg_to_file(), "Error");
        assert_eq!(
            result.message,
            "join task 'Save' failed to get input in position '1'"
        );
    }

    #[test]
    fn save_run_before_it_is_ready_reports_it() {
        let (save, _, _) = declared(vec![row(10), row(20)]);
        let save_id = save.id;

        let (id, result) = save.process().unwrap();
        assert_eq!(id, save_id);
        assert_eq!(result.result_type(), "TaskError");
        assert_eq!(result.log_msg_to_file(), "Error");
        assert_eq!(result.message, "task 'Save' waited with no input");
    }
}

/// Step 2: Save through a running engine. It waits until every row has reported, whether a row
/// finished before Save arrived or after, and whether it forwarded first.
mod engine {
    use super::*;

    #[test]
    fn save_waits_with_a_follower_per_row_and_gets_every_value_in_order() {
        let dir = fresh_dir("t5_followers");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // The rows wait for `go`, so Save arrives while all three are still running.
        let (go_tx, go_rx) = unbounded();
        let rows = Vec::from([10, 20, 30].map(|value| {
            packet_handle
                .send_adv_task(gated_row(value, go_rx.clone()))
                .unwrap()
        }));
        let (got_tx, got_rx) = bounded(1);
        rows.join("Save", reporting(got_tx)).unwrap();

        // One follower per row, and Save itself.
        assert!(engine.check_tasks(), "the engine didn't get the tasks");
        assert_eq!(engine.wait_table_len(), 4);

        for _ in 0..3 {
            go_tx.send(()).unwrap();
        }
        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            JoinHandoff::Filled { values } if values == [10, 20, 30]
        ));
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn rows_that_finished_before_save_arrived_are_taken_at_once() {
        let dir = fresh_dir("t5_kept");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // The handles are held, so the engine keeps the three results.
        let rows =
            Vec::from([10, 20, 30].map(|value| packet_handle.send_adv_task(row(value)).unwrap()));
        run_until_the_wait_table_holds(&mut engine, 3);

        let (got_tx, got_rx) = bounded(1);
        rows.join("Save", reporting(got_tx)).unwrap();

        // Save took the kept results and was queued: no follower, and it isn't held.
        assert!(engine.check_tasks(), "the engine didn't get Save");
        assert_eq!(engine.wait_table_len(), 0);
        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            JoinHandoff::Filled { values } if values == [10, 20, 30]
        ));
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_row_that_forwarded_before_save_arrived_is_followed_to_its_end() {
        let dir = fresh_dir("t5_forwarded");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // The middle row forwards to a task that waits for `go`; the others just finish.
        let (go_tx, go_rx) = unbounded();
        let rows = vec![
            packet_handle.send_adv_task(row(10)).unwrap(),
            packet_handle
                .send_adv_task(forwarding("Row", &packet_handle, gated_row(20, go_rx)))
                .unwrap(),
            packet_handle.send_adv_task(row(30)).unwrap(),
        ];
        // Two kept results and the middle row's link.
        run_until_the_wait_table_holds(&mut engine, 3);

        let (got_tx, got_rx) = bounded(1);
        rows.join("Save", reporting(got_tx)).unwrap();

        // Save took the two kept results, and left its follower at the end of the link.
        assert!(engine.check_tasks(), "the engine didn't get Save");
        assert_eq!(engine.wait_table_len(), 2);

        go_tx.send(()).unwrap();
        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            JoinHandoff::Filled { values } if values == [10, 20, 30]
        ));
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_failed_row_s_error_reaches_save_and_is_not_logged() {
        let dir = fresh_dir("t5_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        let rows = vec![
            packet_handle.send_adv_task(row(10)).unwrap(),
            packet_handle.send_adv_task(failing_row(2)).unwrap(),
            packet_handle.send_adv_task(row(30)).unwrap(),
        ];
        let (got_tx, got_rx) = bounded(1);
        rows.join("Save", reporting(got_tx)).unwrap();

        match run_until_received(&mut engine, &got_rx) {
            JoinHandoff::Error { position, error } => {
                assert_eq!(position, 1);
                assert_eq!(error, RowError { row: 2 });
            }
            JoinHandoff::Filled { values } => panic!("expected row 2's error, got {values:?}"),
        }
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Save took the error, so the engine didn't write it, and nothing was lost.
        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains("row 2 is corrupt"), "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn joins_nest_and_chain_through_the_engine() {
        let dir = fresh_dir("t5_nested");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        let send = |value| packet_handle.send_adv_task(row(value)).unwrap();

        // Two sums, an archive of both, and an upload of the archive.
        let first = vec![send(10), send(20)].join("Save", sum).unwrap();
        let second = vec![send(30), send(40)].join("Save", sum).unwrap();
        let (got_tx, got_rx) = bounded(1);
        vec![first, second]
            .join(
                "Archive",
                |sums: JoinHandoff<Vec<u32>, RowError>| match sums {
                    JoinHandoff::Filled { values } => Ok(values),
                    JoinHandoff::Error { error, .. } => Err(error),
                },
            )
            .unwrap()
            .then("Upload", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), RowError>(())
            })
            .unwrap();

        match run_until_received(&mut engine, &got_rx) {
            Handoff::Value(sums) => assert_eq!(sums, [30, 70]),
            Handoff::Error(error) => panic!("expected both sums, got {error}"),
        }
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn two_hundred_joins_get_their_values_in_order_and_leave_nothing_waiting() {
        const JOINS: usize = 200;
        const ROWS: u32 = 10;
        // Each round fits in the task mailbox: its joins and their rows.
        const ROUND: usize = 50;

        let dir = fresh_dir("t5_no_growth");
        let (mut engine, packet_handle) = started_engine(&dir, 1_000, 2, 10);
        let (got_tx, got_rx) = unbounded();
        let deadline = Instant::now() + Duration::from_secs(30);

        for round in 0..JOINS / ROUND {
            for save in round * ROUND..(round + 1) * ROUND {
                let first = save as u32 * ROWS;
                let rows: Vec<_> = (first..first + ROWS)
                    .map(|value| packet_handle.send_adv_task(row(value)).unwrap())
                    .collect();
                let got = got_tx.clone();
                rows.join("Save", move |joined: Joined| {
                    let _ = got.send((first, joined));
                    Ok::<(), RowError>(())
                })
                .unwrap();
            }

            // Every Save of the round has run, so the task mailbox is empty again.
            for _ in 0..ROUND {
                let (first, joined) = loop {
                    if let Ok(got) = got_rx.try_recv() {
                        break got;
                    }
                    assert!(Instant::now() < deadline, "the joins didn't all run");
                    engine.run_once();
                };
                let expected: Vec<u32> = (first..first + ROWS).collect();
                assert!(
                    matches!(&joined, JoinHandoff::Filled { values } if *values == expected),
                    "the Save of rows {first} and up got {joined:?}"
                );
            }
        }

        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }
}

/// Step 3: a join fails fast. The first error to reach Save makes it run at once; a result that
/// reaches it afterwards is late, and nobody takes it. A Save still waiting at the shutdown is a
/// lost task.
mod failing {
    use super::*;

    #[test]
    fn save_runs_as_soon_as_a_row_fails_and_the_other_rows_values_are_dropped() {
        let dir = fresh_dir("t5_fail_fast");
        // One worker for each row, so Save can run on Row 2's while the two others wait.
        let (mut engine, packet_handle) = started_engine(&dir, 10, 3, 10);

        // Rows 1 and 3 wait for `go`; Row 2 fails at once.
        let (go_tx, go_rx) = unbounded();
        let rows = vec![
            packet_handle
                .send_adv_task(gated_row(10, go_rx.clone()))
                .unwrap(),
            packet_handle.send_adv_task(failing_row(2)).unwrap(),
            packet_handle.send_adv_task(gated_row(30, go_rx)).unwrap(),
        ];
        let (got_tx, got_rx) = bounded(1);
        rows.join("Save", reporting(got_tx)).unwrap();

        // `go` hasn't been sent, so Save ran with Rows 1 and 3 still running.
        let (position, error) = the_error(run_until_received(&mut engine, &got_rx));
        assert_eq!(position, 1);
        assert_eq!(error, RowError { row: 2 });

        // Their values are late: dropped as they arrive, with their followers.
        for _ in 0..2 {
            go_tx.send(()).unwrap();
        }
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Save took the error, and a late value isn't lost work.
        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains("row 2 is corrupt"), "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn an_error_that_arrives_after_save_ran_is_written_to_the_error_log() {
        let dir = fresh_dir("t5_late_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 2, 10);

        // Row 1 fails only once `go` lets it, after Row 2 has failed: Save gets Row 2's error,
        // the first to reach it, although Row 1 comes first among the handles.
        let (go_tx, go_rx) = unbounded();
        let rows = vec![
            packet_handle
                .send_adv_task(gated_failing_row(1, go_rx))
                .unwrap(),
            packet_handle.send_adv_task(failing_row(2)).unwrap(),
        ];
        let (got_tx, got_rx) = bounded(1);
        rows.join("Save", reporting(got_tx)).unwrap();

        let (position, error) = the_error(run_until_received(&mut engine, &got_rx));
        assert_eq!(position, 1);
        assert_eq!(error, RowError { row: 2 });

        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Row 1's error came too late for Save, so it was written when it arrived, not counted
        // as lost work at the shutdown.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains("row 1 is corrupt"), "{errors}");
        assert!(!errors.contains("row 2 is corrupt"), "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn the_first_kept_error_triggers_save_and_the_kept_results_after_it_are_let_go() {
        let dir = fresh_dir("t5_kept_error");
        // Rows 1 and 5 hold a worker each, which leaves one for the others and Save.
        let (mut engine, packet_handle) = started_engine(&dir, 10, 3, 10);

        // Rows 2 to 4 finish before Save arrives, and the engine keeps their results, since the
        // handles are held. Rows 1 and 5 wait for `go`.
        let (go_tx, go_rx) = unbounded();
        let rows = vec![
            packet_handle
                .send_adv_task(gated_row(10, go_rx.clone()))
                .unwrap(),
            packet_handle.send_adv_task(failing_row(2)).unwrap(),
            packet_handle.send_adv_task(failing_row(3)).unwrap(),
            packet_handle.send_adv_task(row(40)).unwrap(),
            packet_handle.send_adv_task(gated_row(50, go_rx)).unwrap(),
        ];
        run_until_the_wait_table_holds(&mut engine, 3);

        let (got_tx, got_rx) = bounded(1);
        rows.join("Save", reporting(got_tx)).unwrap();

        // Row 2's error triggered Save, which was queued, not held. The kept results of Rows 3
        // and 4 came after it, so they were let go; Rows 1 and 5 got a follower each, before the
        // error and after it alike.
        assert!(engine.check_tasks(), "the engine didn't get Save");
        assert_eq!(engine.wait_table_len(), 2);

        let (position, error) = the_error(run_until_received(&mut engine, &got_rx));
        assert_eq!(position, 1);
        assert_eq!(error, RowError { row: 2 });

        for _ in 0..2 {
            go_tx.send(()).unwrap();
        }
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Row 3's error was let go, so it was written; Save took Row 2's.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains("row 3 is corrupt"), "{errors}");
        assert!(!errors.contains("row 2 is corrupt"), "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn a_save_still_waiting_at_the_shutdown_is_one_lost_task() {
        let dir = fresh_dir("t5_shutdown");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        engine.config.debugging.is_debugging = true;

        // Row 1's value is kept, and goes into Save's slot when Save arrives.
        let first = packet_handle.send_adv_task(row(10)).unwrap();
        run_until_the_wait_table_holds(&mut engine, 1);

        // Row 2 runs, but nothing reads its result before the shutdown starts, so Save is still
        // held, with its follower waiting for Row 2.
        let (ran_tx, ran_rx) = bounded(1);
        let second = packet_handle
            .send_adv_task(telling_row(20, ran_tx))
            .unwrap();
        vec![first, second].join("Save", sum).unwrap();
        assert!(engine.check_tasks(), "the engine didn't get the tasks");
        assert_eq!(engine.wait_table_len(), 2);
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("row 2 never ran");

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Save is the one lost task. The value in its slot and Row 2's value aren't lost work,
        // and the follower reports nothing.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains(&summary_line(1, 0, 0, 0)), "{errors}");
        let debug = read_log(&dir, "Debug.log");
        assert!(debug.contains("Save: 1"), "{debug}");
    }
}

/// Step 4, part 1: a row that forwards after Save was declared. The forward never goes into a
/// slot: the row's follower moves on to the task forwarded to, whose result Save then gets.
mod forwarding_after_declaration {
    use super::*;

    #[test]
    fn rows_that_forward_after_save_arrived_fill_their_slots_with_the_last_task_s_value() {
        let dir = fresh_dir("t5_forward_after");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 2, 10);

        // Row 1 forwards to Decompress; Row 2 forwards to a Decompress that forwards again.
        let rows = vec![
            packet_handle
                .send_adv_task(forwarding("Row", &packet_handle, decompress(Ok(10))))
                .unwrap(),
            packet_handle
                .send_adv_task(forwarding(
                    "Row",
                    &packet_handle,
                    forwarding("Decompress", &packet_handle, row(20)),
                ))
                .unwrap(),
            packet_handle.send_adv_task(row(30)).unwrap(),
        ];
        let (got_tx, got_rx) = bounded(1);
        rows.join("Save", reporting(got_tx)).unwrap();

        // Save is declared before the engine has handled any result, so every forward arrives
        // after it, at a row's follower.
        assert!(engine.check_tasks(), "the engine didn't get the tasks");
        assert_eq!(engine.wait_table_len(), 4);

        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            JoinHandoff::Filled { values } if values == [10, 20, 30]
        ));
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains("wrong type"), "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn a_forward_whose_task_finished_first_hands_save_the_kept_result() {
        let dir = fresh_dir("t5_forward_race");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 2, 10);

        // Row 1's value is kept before Save arrives. Row 2 sends Decompress at once, but holds
        // its forward back until `go`, so Decompress's value reaches the engine first and is
        // kept under Decompress's id.
        let (go_tx, go_rx) = unbounded();
        let first = packet_handle.send_adv_task(row(10)).unwrap();
        let second = packet_handle
            .send_adv_task(forwarding_after(&packet_handle, decompress(Ok(20)), go_rx))
            .unwrap();
        run_until_the_wait_table_holds(&mut engine, 2);

        let (got_tx, got_rx) = bounded(1);
        vec![first, second].join("Save", reporting(got_tx)).unwrap();

        // Row 1's value went into its slot. Decompress's value is still kept, Row 2 has a
        // follower, and Save waits.
        assert!(engine.check_tasks(), "the engine didn't get Save");
        assert_eq!(engine.wait_table_len(), 3);

        // The forward moves the follower to Decompress, where it finds the kept value.
        go_tx.send(()).unwrap();
        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            JoinHandoff::Filled { values } if values == [10, 20]
        ));
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_forward_that_arrives_after_save_ran_still_moves_the_follower() {
        let dir = fresh_dir("t5_forward_late");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 2, 10);

        // Row 1 fails at once, which makes Save run. Row 2 sends Decompress, which fails too,
        // and forwards to it only once `go` lets it, after Save has run.
        let (go_tx, go_rx) = unbounded();
        let failing_decompress = decompress(Err(RowError { row: 2 }));
        let rows = vec![
            packet_handle.send_adv_task(failing_row(1)).unwrap(),
            packet_handle
                .send_adv_task(forwarding_after(&packet_handle, failing_decompress, go_rx))
                .unwrap(),
        ];
        let (got_tx, got_rx) = bounded(1);
        rows.join("Save", reporting(got_tx)).unwrap();

        let (position, error) = the_error(run_until_received(&mut engine, &got_rx));
        assert_eq!(position, 0);
        assert_eq!(error, RowError { row: 1 });

        // The follower still moves to Decompress, so Decompress's error is late rather than
        // kept until the shutdown: it's written to `Error.log`, and nothing is left waiting.
        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains("row 2 is corrupt"), "{errors}");
        assert!(!errors.contains("row 1 is corrupt"), "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }
}

/// Step 4, part 2: Save's priority. Declared with `join`, it inherits the highest priority among
/// its rows, capped just below realtime when the engine makes it ready, through
/// `with_join_inputs`. Declared with `join_with_priority`, it keeps the priority it was given.
mod priority {
    use super::*;

    /// Save over rows at `priorities`, as the engine receives it: declared with `join`, or with
    /// `join_with_priority` when `given` is set.
    fn save_over(priorities: &[u8], given: Option<u8>) -> Task {
        let (handle, mailbox) = handle_and_mailbox(false);
        let rows: Vec<_> = priorities
            .iter()
            .map(|&priority| handle.send_adv_task(row_at(0, priority)).unwrap())
            .collect();
        match given {
            Some(priority) => rows.join_with_priority("Save", priority, save).unwrap(),
            None => rows.join("Save", save).unwrap(),
        };

        mailbox.try_iter().last().unwrap()
    }

    #[test]
    fn save_inherits_the_highest_priority_among_its_rows() {
        assert_eq!(save_over(&[1, 2, 0], None).base_priority, 2);
    }

    #[test]
    fn an_inherited_realtime_priority_is_capped_just_below_realtime() {
        // With a `max_priority` of 4, realtime starts at 3, so the cap is 2.
        for realtime in [3, 4] {
            let save = save_over(&[1, realtime], None);
            assert_eq!(
                save.base_priority, realtime,
                "it's sent with the inherited priority"
            );

            let ready = save.with_join_inputs(MAX_PRIORITY);
            assert_eq!(ready.base_priority, 2);
            assert_eq!(ready.waiting_on_join(), None, "made ready, it can run");
        }
    }

    #[test]
    fn an_inherited_priority_below_realtime_is_kept() {
        let save = save_over(&[0, 1], None);
        assert_eq!(save.with_join_inputs(MAX_PRIORITY).base_priority, 1);
    }

    #[test]
    fn a_given_priority_is_kept_whatever_the_rows_priorities() {
        // Realtime, above every row's.
        let save = save_over(&[1, 1], Some(4));
        assert_eq!(save.base_priority, 4);
        assert_eq!(save.with_join_inputs(MAX_PRIORITY).base_priority, 4);

        // Below the highest row's.
        let save = save_over(&[3], Some(1));
        assert_eq!(save.with_join_inputs(MAX_PRIORITY).base_priority, 1);
    }

    #[test]
    fn when_every_level_is_realtime_an_inherited_priority_is_kept() {
        // With a `max_priority` of 1, realtime starts at 0: there's no level below it to cap to.
        let save = save_over(&[1], None);
        assert_eq!(save.with_join_inputs(1).base_priority, 1);
    }

    #[test]
    fn graph_mode_records_the_line_that_declared_a_join_with_priority() {
        let (handle, mailbox) = handle_and_mailbox(true);
        let rows = vec![handle.send_adv_task(row(10)).unwrap()];

        let line = line!() + 1;
        rows.join_with_priority("Save", 2, save).unwrap();

        let save = mailbox.try_iter().last().unwrap();
        let callsite = save.callsite.expect("graph mode records a call site");
        assert_eq!(callsite.line_number, line);
        assert!(
            callsite.file_path.ends_with("gate_t5.rs"),
            "{:?}",
            callsite.file_path
        );
    }
}

/// Step 7: splitting work. `split_into` cuts a list into balanced pieces, in order, and sends one
/// typed task per piece; `send_split_tasks` sends one per chunk the caller already cut. Both return
/// the pieces' handles, ready to be joined.
mod splitting {
    use super::*;

    /// A piece's closure that hands back where its chunk starts, and the chunk.
    fn report(chunk: Vec<u32>, start: usize) -> Result<(usize, Vec<u32>), RowError> {
        Ok((start, chunk))
    }

    /// A piece's closure that adds up its rows.
    fn add_up(chunk: Vec<u32>, _start: usize) -> Result<u32, RowError> {
        Ok(chunk.iter().sum())
    }

    /// Runs the pieces the way a worker does, and opens what each handed back.
    fn ran<T: 'static>(pieces: Vec<Task>) -> Vec<T> {
        pieces
            .into_iter()
            .map(|piece| {
                let (_, result) = piece.process().unwrap();
                let (value, error_text) = open::<T, RowError>(result);
                assert_eq!(error_text, None);
                value.unwrap()
            })
            .collect()
    }

    #[test]
    fn split_into_cuts_a_list_into_balanced_pieces_in_order() {
        let (handle, mailbox) = handle_and_mailbox(false);
        let rows: Vec<u32> = (0..10).collect();

        let pieces = handle.split_into(rows, "Row", 4, 3, report).unwrap();
        assert_eq!(pieces.len(), 4);

        let sent: Vec<Task> = mailbox.try_iter().collect();
        for piece in &sent {
            assert_eq!(piece.task_name(), "Row");
            assert_eq!(piece.base_priority, 3);
        }
        // Sizes differ by at most one, the larger pieces spread out, and each chunk knows where
        // it starts.
        assert_eq!(
            ran::<(usize, Vec<u32>)>(sent),
            [
                (0, vec![0, 1]),
                (2, vec![2, 3, 4]),
                (5, vec![5, 6]),
                (7, vec![7, 8, 9])
            ]
        );
    }

    #[test]
    fn more_pieces_than_rows_gives_one_piece_per_row() {
        let (handle, mailbox) = handle_and_mailbox(false);

        let pieces = handle
            .split_into(vec![10, 20, 30], "Row", 5, 1, report)
            .unwrap();
        assert_eq!(pieces.len(), 3);
        assert_eq!(
            ran::<(usize, Vec<u32>)>(mailbox.try_iter().collect()),
            [(0, vec![10]), (1, vec![20]), (2, vec![30])]
        );
    }

    #[test]
    fn an_empty_list_gives_no_pieces() {
        let (handle, mailbox) = handle_and_mailbox(false);

        let pieces = handle.split_into(Vec::new(), "Row", 4, 1, report).unwrap();
        assert!(pieces.is_empty());
        assert!(mailbox.try_recv().is_err(), "nothing was sent");
    }

    #[test]
    fn zero_pieces_is_refused_before_anything_is_sent() {
        let (handle, mailbox) = handle_and_mailbox(false);

        let refused = handle
            .split_into(vec![10, 20], "Row", 0, 1, report)
            .err()
            .expect("zero pieces is refused");
        assert!(matches!(
            &refused,
            ThryveError::Split(SplitError::ZeroPieces { split_name }) if split_name == "Row"
        ));
        assert_eq!(
            get_err(&refused),
            "split failed\n  caused by: cannot split 'Row' into 0 pieces"
        );
        assert!(mailbox.try_recv().is_err(), "nothing was sent");
    }

    #[test]
    fn a_bad_name_is_refused_before_anything_is_sent() {
        let (handle, mailbox) = handle_and_mailbox(false);

        assert!(matches!(
            handle.split_into(vec![10, 20], "", 2, 1, report).err(),
            Some(ThryveError::Naming(NamingError::NameEmpty { .. }))
        ));
        assert!(mailbox.try_recv().is_err(), "nothing was sent");
    }

    #[test]
    fn send_split_tasks_sends_one_piece_per_chunk_with_its_index() {
        let (handle, mailbox) = handle_and_mailbox(false);

        // A chunk can be of any type: here, a file name each.
        let files = vec!["a.png", "b.png", "c.png"];
        let pieces = handle
            .send_split_tasks(files, "FileSize", 2, |file, index| {
                Ok::<(usize, String), RowError>((index, file.to_string()))
            })
            .unwrap();
        assert_eq!(pieces.len(), 3);
        assert_eq!(
            ran::<(usize, String)>(mailbox.try_iter().collect()),
            [
                (0, "a.png".to_string()),
                (1, "b.png".to_string()),
                (2, "c.png".to_string())
            ]
        );
    }

    #[test]
    fn graph_mode_records_the_line_that_split_the_work() {
        let (handle, mailbox) = handle_and_mailbox(true);

        // Each call stays on one line: for a method call, the line recorded is the method name's.
        let (rows, chunks) = (vec![10, 20, 30], vec![vec![40]]);
        let line = line!() + 1;
        let split = handle.split_into(rows, "Row", 2, 1, report);
        let other_line = line!() + 1;
        let sent = handle.send_split_tasks(chunks, "Row", 1, report);
        assert!(split.is_ok() && sent.is_ok());

        let lines: Vec<u32> = mailbox
            .try_iter()
            .map(|piece| {
                let callsite = piece.callsite.expect("graph mode records a call site");
                assert!(
                    callsite.file_path.ends_with("gate_t5.rs"),
                    "{:?}",
                    callsite.file_path
                );
                callsite.line_number
            })
            .collect();
        assert_eq!(lines, [line, line, other_line]);
    }

    #[test]
    fn split_work_joins_back_through_the_engine() {
        let dir = fresh_dir("t5_split_join");
        let (mut engine, packet_handle) = started_engine(&dir, 100, 2, 10);

        // 1,000 rows in 7 pieces, each adding up its own; Save gets the 7 sums, in order.
        let (got_tx, got_rx) = bounded(1);
        packet_handle
            .split_into((0..1_000).collect(), "Row", 7, 1, add_up)
            .unwrap()
            .join("Save", reporting(got_tx))
            .unwrap();

        match run_until_received(&mut engine, &got_rx) {
            JoinHandoff::Filled { values } => {
                assert_eq!(values.len(), 7);
                // The first piece holds rows 0 to 141: the pieces start at 0, 142, 285, …
                assert_eq!(values[0], (0..142).sum::<u32>());
                assert_eq!(values.iter().sum::<u32>(), (0..1_000).sum::<u32>());
            }
            JoinHandoff::Error { error, .. } => panic!("expected the sums, got {error}"),
        }
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_failing_piece_reaches_the_join_at_its_index() {
        let dir = fresh_dir("t5_split_fail");
        let (mut engine, packet_handle) = started_engine(&dir, 100, 2, 10);

        // 10 rows in 5 pieces of 2: the piece holding row 7 is the fourth, at index 3.
        let (got_tx, got_rx) = bounded(1);
        packet_handle
            .split_into(
                (0..10).collect(),
                "Row",
                5,
                1,
                |chunk: Vec<u32>, _start| match chunk.iter().find(|&&row| row == 7) {
                    Some(&row) => Err(RowError { row }),
                    None => Ok(chunk.iter().sum::<u32>()),
                },
            )
            .unwrap()
            .join("Save", reporting(got_tx))
            .unwrap();

        let (position, error) = the_error(run_until_received(&mut engine, &got_rx));
        assert_eq!(position, 3);
        assert_eq!(error, RowError { row: 7 });
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }
}
