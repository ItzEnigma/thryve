// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Typed tasks, `then` and forwarding.
//!
//! Typed tasks: an `AdvTask<T, E>`'s closure returns `Result<T, E>`. When it runs, the
//! result reaches the engine sealed, as a handoff, under the task's id; the engine carries it
//! without knowing `T` or `E`. An error's text is written on the worker, where the type is still
//! known, and the engine writes an error nobody handles to `Error.log`.
//!
//! - a value and an error arrive sealed under the task's id, and open as exactly what the task
//!   returned: the error as the task's own type;
//! - an error's text names the task and lists the error's causes;
//! - output and error types that aren't `Sync` work;
//! - the engine writes an unhandled error to `Error.log`, and drops a value without a line;
//! - an error that arrives during the shutdown counts as lost work, and a value doesn't;
//! - `TaskResult`'s `Display` shows the handoff;
//! - sending checks the name, records the sender's line in graph mode, and fails cleanly once the
//!   engine is gone.
//!
//! `then`: sending returns a handle, and `then` on it declares the task's follow-up. The
//! follow-up waits in the engine until the task finishes, then gets the task's value or its own
//! error, opened as a `Handoff`.
//!
//! - the follow-up gets the exact value, or the task's own error, which then isn't logged;
//! - a chain A → B → C hands each value on, even when its type changes;
//! - follow-ups still waiting at the shutdown count as lost tasks;
//! - a follow-up given no input, or an input of the wrong type, reports a `TaskError` instead of
//!   panicking;
//! - a follow-up inherits its task's priority, capped just below realtime, unless it's given one.
//!
//! Late `then` and releasing: the engine keeps a task's result while its handle exists.
//! Dropping the handle without `then` releases the result.
//!
//! - a follow-up declared after its task finished gets the value, or the error (which then isn't
//!   logged);
//! - an error whose handle is dropped is written to `Error.log` once, whether the release comes
//!   before or after the result;
//! - at the shutdown, an error kept for a handle that still exists is lost work, and a value isn't;
//! - 100,000 tasks, half of them followed up, leave nothing in the wait table.
//!
//! Forwarding: a task whose closure returns another task's handle (built with
//! `AdvTask::forward`, or declared with `then_forward`) finishes with that task's outcome.
//!
//! - a forward reaches the engine as the id of the task forwarded to, and the handle it used up
//!   sends no release; a forwarding task's own error is sealed like any task's;
//! - a follow-up gets the value of the task forwarded to, whether it was declared before or after
//!   the forward, and through stacked forwards;
//! - `then_forward` hands on the value of the task it forwards to, or its own error, and inherits
//!   the priority like `then`; graph mode records the line that declared any follow-up;
//! - a release, before or after the forward, lets go of the error it leads to, written to
//!   `Error.log` once;
//! - at the shutdown, a link isn't counted, and the error it leads to is lost work;
//! - 10,000 forwards, half of them followed up, leave nothing in the wait table.
//!
//! The img example, the case typed tasks were made for: "hash the file, then load the image with the
//! digest", written with `then`. A hash plugin (a stand-in) and an image plugin never name each
//! other; the glue wires them.
//!
//! - the loader gets the digest of the file it loads;
//! - a failed hash reaches the glue as the hasher's own error, and isn't logged.
//!
//! That a follow-up of the wrong type doesn't compile is checked by a `compile_fail` example on
//! `AdvTaskHandle::then`.
//!
//! The engine helpers are `shutdown_and_startup`'s. CI runs these tests on one thread
//! (`--test-threads=1`), like the rest of the crate.

use std::cell::Cell;
use std::fmt;
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, bounded, unbounded};

use thryve_core::runtime::engine::Engine;
use thryve_utils::errors::{EngineNotRunningError, NamingError, ThryveError};
use thryve_utils::packets::packet_handle::{PacketHandle, StatusMsg};
use thryve_utils::packets::task::{AdvTask, AdvTaskHandle, Task};
use thryve_utils::res::{Handoff, SealedHandoff, TaskResult};
use thryve_utils::traits::packets::Packet;

use crate::shutdown_and_startup::{
    finalize_within, fresh_dir, read_log, started_engine, summary_line,
};

/// A hash plugin's own error, with a cause, the way a real plugin's error would look.
#[derive(Debug)]
enum HashError {
    /// The file couldn't be read.
    Read {
        path: String,
        source: std::io::Error,
    },
}

impl HashError {
    fn read(path: &str, cause: &str) -> Self {
        HashError::Read {
            path: path.to_string(),
            source: std::io::Error::other(cause.to_string()),
        }
    }
}

impl fmt::Display for HashError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HashError::Read { path, .. } => write!(f, "could not read {path}"),
        }
    }
}

impl std::error::Error for HashError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            HashError::Read { source, .. } => Some(source),
        }
    }
}

/// What a failing hash task's error text reads: the task's name, the error, then its cause.
const HASH_ERROR_TEXT: &str =
    "task 'HashFile' returned an error: could not read a.png: access denied";

/// An error that can move between threads but can't be shared between them: it holds a `Cell`.
#[derive(Debug)]
struct CellError(Cell<u32>);

impl fmt::Display for CellError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "counter stuck at {}", self.0.get())
    }
}

impl std::error::Error for CellError {}

/// A hash task that fails to read `a.png`.
fn failing_hash() -> AdvTask<[u8; 32], HashError> {
    AdvTask::new("HashFile", 1, || {
        Err(HashError::read("a.png", "access denied"))
    })
    .unwrap()
}

/// A task that returns 42.
fn answer() -> AdvTask<u32, HashError> {
    AdvTask::new("Answer", 1, || Ok(42)).unwrap()
}

/// A packet handle whose tasks land in the returned mailbox instead of an engine's.
pub(crate) fn handle_and_mailbox(is_graph_mode_on: bool) -> (PacketHandle, Receiver<Task>) {
    let (task_tx, task_rx) = bounded(10);
    let handle = PacketHandle::new(task_tx, bounded(10).0, unbounded().0, is_graph_mode_on);
    (handle, task_rx)
}

/// Like [`handle_and_mailbox`], but also returns the status mailbox, where releases land.
pub(crate) fn handle_mailbox_and_status() -> (PacketHandle, Receiver<Task>, Receiver<StatusMsg>) {
    let (task_tx, task_rx) = bounded(10);
    let (status_tx, status_rx) = unbounded();
    let handle = PacketHandle::new(task_tx, bounded(10).0, status_tx, false);
    (handle, task_rx, status_rx)
}

/// A task that forwards: when it runs, it sends `to` through `sender` and returns its handle.
pub(crate) fn forwarding<T, E>(
    name: &str,
    sender: &PacketHandle,
    to: AdvTask<T, E>,
) -> AdvTask<T, E>
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    let sender = sender.clone();
    AdvTask::forward(name, 1, move || Ok(sender.send_adv_task(to).unwrap())).unwrap()
}

/// What a forwarding closure returns when it fails before forwarding: an error of its own.
fn no_forward() -> Result<AdvTaskHandle<u32, HashError>, HashError> {
    Err(HashError::read("a.png", "access denied"))
}

/// A task that returns 42 once `go` lets it, so a test can hold its result back.
fn gated_answer(go: Receiver<()>) -> AdvTask<u32, HashError> {
    AdvTask::new("Answer", 1, move || {
        let _ = go.recv();
        Ok(42)
    })
    .unwrap()
}

/// Sends `task`, takes it out of the mailbox and runs it the way a worker does. Returns the id it
/// was sent with, the id its result came back with, and the result.
pub(crate) fn run_like_a_worker<T, E>(task: AdvTask<T, E>) -> (u64, u64, TaskResult)
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    let (handle, mailbox) = handle_and_mailbox(false);
    handle.send_adv_task(task).unwrap();
    let task = mailbox.try_recv().unwrap();
    let sent_id = task.id;
    let (result_id, result) = task.process().unwrap();

    (sent_id, result_id, result)
}

/// Opens a result's handoff as the `Result<T, E>` it sealed, and returns it with the error text.
pub(crate) fn open<T: 'static, E: 'static>(
    mut result: TaskResult,
) -> (Result<T, E>, Option<String>) {
    match result
        .take_handoff()
        .expect("an AdvTask's result carries a handoff")
    {
        SealedHandoff::Finished { sealed, error_text } => {
            let sealed = sealed
                .downcast::<Result<T, E>>()
                .expect("the handoff holds the task's Result<T, E>");
            (*sealed, error_text)
        }
        SealedHandoff::Forward { to } => {
            panic!("expected a finished handoff, got a forward to task {to}")
        }
        SealedHandoff::Cancelled => panic!("expected a finished handoff, got a cancelled one"),
        SealedHandoff::FanoutShare { .. } => {
            panic!("expected a finished handoff, got a follower's share")
        }
    }
}

/// Sends `task` to `engine`, dropping its handle at once, and drives the engine until it has
/// handled the task's result and the handle's release.
fn handle_one<T, E>(engine: &mut Engine, packet_handle: &PacketHandle, task: AdvTask<T, E>)
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    packet_handle.send_adv_task(task).unwrap();
    take_in_and_handle_a_result(engine);
    // The release was sent when the handle was dropped, before the engine's first read.
    assert!(engine.check_status(), "the handle's release never came");
}

/// Drives the engine until it has taken in the tasks sent to it and handled a result. It never
/// reads the status mailbox, so no release is handled.
fn take_in_and_handle_a_result(engine: &mut Engine) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !engine.check_tasks() {
        assert!(Instant::now() < deadline, "the engine never got the task");
        std::thread::sleep(Duration::from_millis(1));
    }

    handle_a_result(engine);
}

/// Drives the engine until it has handled a result, reading only the result mailbox.
fn handle_a_result(engine: &mut Engine) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !engine.check_results() {
        assert!(
            Instant::now() < deadline,
            "the task's result never came back"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// Runs the engine's turns until `got` receives what a follow-up sent it, for up to 5 seconds, and
/// returns it.
pub(crate) fn run_until_received<X>(engine: &mut Engine, got: &Receiver<X>) -> X {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Ok(received) = got.try_recv() {
            return received;
        }
        assert!(Instant::now() < deadline, "the follow-up never ran");
        engine.run_once();
    }
}

/// Runs the engine's turns until its wait table is empty, for up to 5 seconds.
pub(crate) fn run_until_the_wait_table_is_empty(engine: &mut Engine) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while engine.wait_table_len() > 0 {
        assert!(
            Instant::now() < deadline,
            "{} entries stayed in the wait table",
            engine.wait_table_len()
        );
        engine.run_once();
    }
}

/// The follow-up of a task sent at `priority`, as the engine receives it, and the handoff it
/// waits for. No engine is involved.
///
/// The task returns 7. Its follow-up, `Next`, is declared with `then`, or with
/// `then_with_priority` at `given` when that's `Some`. The task is run the way a worker runs it.
fn follow_up_of(priority: u8, given: Option<u8>) -> (Task, SealedHandoff) {
    let (handle, mailbox) = handle_and_mailbox(false);
    let first = AdvTask::new("First", priority, || Ok::<u32, HashError>(7)).unwrap();
    let first_handle = handle.send_adv_task(first).unwrap();
    match given {
        Some(given) => first_handle.then_with_priority("Next", given, |_| Ok::<u32, HashError>(0)),
        None => first_handle.then("Next", |_| Ok::<u32, HashError>(0)),
    }
    .unwrap();

    let first = mailbox.try_recv().unwrap();
    let follow_up = mailbox.try_recv().unwrap();
    assert_eq!(follow_up.waiting_on_id(), Some(first.id));

    let (_, mut result) = first.process().unwrap();
    (follow_up, result.take_handoff().unwrap())
}

mod sealed_results {
    use super::*;

    #[test]
    fn a_value_arrives_sealed_under_the_task_s_id() {
        let (sent_id, result_id, result) = run_like_a_worker(answer());

        assert_eq!(result_id, sent_id);
        // Nothing for the engine to log or run next: just the handoff.
        assert_eq!(result.result_type(), "AdvTask");
        assert!(result.message.is_empty());
        assert!(result.next_task.is_none());

        let (opened, error_text) = open::<u32, HashError>(result);
        assert!(matches!(opened, Ok(42)), "{opened:?}");
        assert_eq!(error_text, None);
    }

    #[test]
    fn an_error_arrives_as_the_task_s_own_type_with_its_text() {
        let (sent_id, result_id, result) = run_like_a_worker(failing_hash());

        assert_eq!(result_id, sent_id);
        let (opened, error_text) = open::<[u8; 32], HashError>(result);
        // The follow-up gets the error itself, not just its words.
        match opened {
            Err(HashError::Read { path, source }) => {
                assert_eq!(path, "a.png");
                assert_eq!(source.to_string(), "access denied");
            }
            other => panic!("expected the task's own error, got {other:?}"),
        }
        assert_eq!(error_text.as_deref(), Some(HASH_ERROR_TEXT));
    }

    #[test]
    fn output_and_error_types_need_not_be_sync() {
        // A `Cell` can move to another thread but can't be shared between threads.
        let counter =
            AdvTask::new("Counter", 1, || Ok::<Cell<u32>, CellError>(Cell::new(3))).unwrap();
        let (_, _, result) = run_like_a_worker(counter);
        let (opened, _) = open::<Cell<u32>, CellError>(result);
        assert_eq!(opened.unwrap().get(), 3);

        let stuck = AdvTask::new("Counter", 1, || {
            Err::<Cell<u32>, _>(CellError(Cell::new(9)))
        })
        .unwrap();
        let (_, _, result) = run_like_a_worker(stuck);
        let (opened, error_text) = open::<Cell<u32>, CellError>(result);
        assert_eq!(opened.unwrap_err().0.get(), 9);
        assert_eq!(
            error_text.as_deref(),
            Some("task 'Counter' returned an error: counter stuck at 9")
        );
    }
}

mod engine {
    use super::*;

    #[test]
    fn an_error_nobody_handles_is_written_to_error_log() {
        let dir = fresh_dir("typed_tasks_unhandled_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        handle_one(&mut engine, &packet_handle, failing_hash());
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains(HASH_ERROR_TEXT), "{errors}");
        // It was handled while the engine ran, so it isn't lost work.
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn a_value_nobody_waits_for_is_dropped_without_a_line() {
        let dir = fresh_dir("typed_tasks_unclaimed_value");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        handle_one(&mut engine, &packet_handle, answer());
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        for file in ["Main.log", "Error.log"] {
            let log = read_log(&dir, file);
            assert!(!log.contains("Answer"), "{file}:\n{log}");
            assert!(!log.contains("things not done"), "{file}:\n{log}");
        }
    }

    #[test]
    fn during_the_shutdown_an_error_counts_as_lost_work_and_a_value_does_not() {
        let dir = fresh_dir("typed_tasks_lost_handoffs");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        engine.config.debugging.is_debugging = true;

        // Both tasks run, but nothing reads their results before the shutdown starts.
        let (ran_tx, ran_rx) = bounded(2);
        let ran = ran_tx.clone();
        let failing = AdvTask::new("HashFile", 1, move || {
            let _ = ran.send(());
            Err::<u32, _>(HashError::read("a.png", "access denied"))
        })
        .unwrap();
        let succeeding = AdvTask::new("Answer", 1, move || {
            let _ = ran_tx.send(());
            Ok::<u32, HashError>(42)
        })
        .unwrap();
        packet_handle.send_adv_task(failing).unwrap();
        packet_handle.send_adv_task(succeeding).unwrap();
        assert!(engine.check_tasks(), "the engine didn't get the tasks");
        for _ in 0..2 {
            ran_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("a task never ran");
        }

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Only the error is lost work: it would have been written to `Error.log`.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains(&summary_line(0, 0, 1, 0)), "{errors}");
        // Counted, not handled: its own line was never written.
        assert!(!errors.contains(HASH_ERROR_TEXT), "{errors}");

        let debug = read_log(&dir, "Debug.log");
        assert!(debug.contains("AdvTask: 1"), "{debug}");
        assert!(
            debug.contains(&format!("handoff: {HASH_ERROR_TEXT}")),
            "{debug}"
        );
    }
}

mod display {
    use super::*;

    #[test]
    fn a_result_shows_its_handoff() {
        let (_, _, value) = run_like_a_worker(answer());
        assert_eq!(
            format!("{value}"),
            "result type: AdvTask, message: , next task: None, handoff: a value"
        );

        let (_, _, error) = run_like_a_worker(failing_hash());
        assert_eq!(
            format!("{error}"),
            format!("result type: AdvTask, message: , next task: None, handoff: {HASH_ERROR_TEXT}")
        );

        let plain = TaskResult::new("Report", "row 7 done".to_string(), "Main");
        assert_eq!(
            format!("{plain}"),
            "result type: Report, message: row 7 done, next task: None, handoff: None"
        );
    }
}

mod sending {
    use super::*;

    #[test]
    fn a_bad_name_is_refused() {
        let empty = AdvTask::new("", 1, || Ok::<u32, HashError>(1));
        assert!(matches!(empty, Err(NamingError::NameEmpty { .. })));

        let control_char = AdvTask::new("Hash\nFile", 1, || Ok::<u32, HashError>(1));
        assert!(matches!(
            control_char,
            Err(NamingError::NameContainsControlChar { .. })
        ));
    }

    #[test]
    fn graph_mode_records_the_line_that_sent_it() {
        let (handle, mailbox) = handle_and_mailbox(true);

        let line = line!() + 1;
        handle.send_adv_task(answer()).unwrap();

        let task = mailbox.try_recv().unwrap();
        let callsite = task.callsite.expect("graph mode records a call site");
        assert_eq!(callsite.line_number, line);
        assert!(
            callsite.file_path.ends_with("typed_tasks.rs"),
            "{:?}",
            callsite.file_path
        );
        assert_eq!(callsite.callsite_name, "Answer");
    }

    #[test]
    fn graph_mode_records_the_line_that_declared_a_follow_up() {
        // A named function keeps each call short enough for one line, since graph mode records
        // the line of the method's name.
        fn zero(_: Handoff<u32, HashError>) -> Result<u32, HashError> {
            Ok(0)
        }

        let (handle, mailbox) = handle_and_mailbox(true);
        let mut lines = Vec::new();

        let first = handle.send_adv_task(answer()).unwrap();
        lines.push(line!() + 1);
        first.then("Next", zero).unwrap();

        let first = handle.send_adv_task(answer()).unwrap();
        lines.push(line!() + 1);
        first.then_with_priority("Next", 2, zero).unwrap();

        let first = handle.send_adv_task(answer()).unwrap();
        lines.push(line!() + 1);
        first.then_forward("Next", |_| no_forward()).unwrap();

        for line in lines {
            let _first = mailbox.try_recv().unwrap();
            let follow_up = mailbox.try_recv().unwrap();
            let callsite = follow_up.callsite.expect("graph mode records a call site");
            assert_eq!(callsite.line_number, line);
            assert!(
                callsite.file_path.ends_with("typed_tasks.rs"),
                "{:?}",
                callsite.file_path
            );
        }
    }

    #[test]
    fn sending_once_the_engine_is_gone_returns_an_error() {
        let (handle, mailbox) = handle_and_mailbox(false);
        drop(mailbox);

        assert!(matches!(
            handle.send_adv_task(answer()),
            Err(ThryveError::EngineNotRunning(EngineNotRunningError::Task))
        ));
    }
}

/// `then`, through a running engine: the follow-up gets the task's exact value or error,
/// a chain hands each value on, and the follow-ups still waiting at the shutdown are lost work.
///
/// Each test sends the task and declares its follow-ups before the engine's first turn, so the
/// engine has every follow-up before the task finishes. (A follow-up declared after that is
/// in `late_then_and_release`.)
mod then {
    use super::*;

    #[test]
    fn the_follow_up_gets_the_exact_value() {
        let dir = fresh_dir("typed_tasks_then_value");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        let (got_tx, got_rx) = bounded(1);
        packet_handle
            .send_adv_task(answer())
            .unwrap()
            .then("Report", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();

        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            Handoff::Value(42)
        ));
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn the_follow_up_gets_the_exact_error_and_it_is_not_logged() {
        let dir = fresh_dir("typed_tasks_then_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        let (got_tx, got_rx) = bounded(1);
        packet_handle
            .send_adv_task(failing_hash())
            .unwrap()
            .then("Recover", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();

        // The task's own error, with its fields and its cause, not just its words.
        match run_until_received(&mut engine, &got_rx) {
            Handoff::Error(HashError::Read { path, source }) => {
                assert_eq!(path, "a.png");
                assert_eq!(source.to_string(), "access denied");
            }
            Handoff::Value(_) => panic!("expected the task's own error, got a value"),
        }
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // The follow-up handled it, so the engine didn't write it, and nothing was lost.
        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains(HASH_ERROR_TEXT), "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn a_chain_hands_each_value_on() {
        let dir = fresh_dir("typed_tasks_then_chain");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // A returns 42; B halves it and returns text, a new type; C passes on what it got.
        let (got_tx, got_rx) = bounded(1);
        packet_handle
            .send_adv_task(answer())
            .unwrap()
            .then("Halve", |handoff| match handoff {
                Handoff::Value(value) => Ok((value / 2).to_string()),
                Handoff::Error(error) => Err(error),
            })
            .unwrap()
            .then("Report", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();

        match run_until_received(&mut engine, &got_rx) {
            Handoff::Value(half) => assert_eq!(half, "21"),
            Handoff::Error(error) => panic!("expected B's value, got {error:?}"),
        }
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn follow_ups_still_waiting_at_the_shutdown_are_lost_tasks() {
        let dir = fresh_dir("typed_tasks_then_shutdown");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        engine.config.debugging.is_debugging = true;

        // A runs, but nothing reads its result before the shutdown starts, so B, and C after it,
        // never get their input.
        let (ran_tx, ran_rx) = bounded(1);
        let first = AdvTask::new("Answer", 1, move || {
            let _ = ran_tx.send(());
            Ok::<u32, HashError>(42)
        })
        .unwrap();
        packet_handle
            .send_adv_task(first)
            .unwrap()
            .then("Halve", |_| Ok::<u32, HashError>(21))
            .unwrap()
            .then("Report", |_| Ok::<(), HashError>(()))
            .unwrap();
        assert!(engine.check_tasks(), "the engine didn't get the tasks");
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the first task never ran");

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Both follow-ups are lost; A's value isn't lost work.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains(&summary_line(2, 0, 0, 0)), "{errors}");

        let debug = read_log(&dir, "Debug.log");
        for lost in ["Halve: 1", "Report: 1"] {
            assert!(debug.contains(lost), "{debug}");
        }
    }
}

/// What a follow-up's job does when it's given no input, or an input of the wrong type. Only a
/// bug in Thryve can cause either, and the job reports it as its result, a `TaskError` for
/// `Error.log`, instead of panicking.
mod task_errors {
    use super::*;

    #[test]
    fn a_follow_up_that_runs_without_its_input_reports_it() {
        let (follow_up, _) = follow_up_of(1, None);
        let follow_up_id = follow_up.id;

        let (id, result) = follow_up.process().unwrap();
        assert_eq!(id, follow_up_id);
        assert_eq!(result.result_type(), "TaskError");
        assert_eq!(result.log_msg_to_file(), "Error");
        assert_eq!(result.message, "task 'Next' waited with no input");
    }

    #[test]
    fn a_follow_up_given_an_input_of_the_wrong_type_reports_it() {
        // `Next` waits for a `Result<u32, HashError>`; this handoff holds a `Result<String, _>`.
        let (follow_up, _) = follow_up_of(1, None);
        let name = AdvTask::new("Name", 1, || Ok::<String, HashError>("a.png".to_string()));
        let (_, _, mut result) = run_like_a_worker(name.unwrap());
        let wrong_input = result.take_handoff().unwrap();

        let (_, result) = follow_up.with_input(wrong_input, 4).process().unwrap();
        assert_eq!(result.result_type(), "TaskError");
        assert_eq!(result.log_msg_to_file(), "Error");
        assert_eq!(
            result.message,
            "handoff to task 'Next' failed to downcast properly"
        );
    }
}

/// Late `then`: the engine keeps a task's handoff while the task's handle exists, so `then` works
/// after the task finished. A dropped handle releases it, in whichever order the release and the
/// result arrive, and an error nobody takes is written to `Error.log` once. Nothing is left in the
/// wait table afterwards.
mod late_then_and_release {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn a_follow_up_declared_after_its_task_finished_gets_the_value() {
        let dir = fresh_dir("typed_tasks_late_then_value");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // The engine handles the result while the handle is still held, so it keeps it.
        let handle = packet_handle.send_adv_task(answer()).unwrap();
        take_in_and_handle_a_result(&mut engine);

        let (got_tx, got_rx) = bounded(1);
        handle
            .then("Report", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();

        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            Handoff::Value(42)
        ));
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_follow_up_declared_after_its_task_finished_gets_the_error_and_it_is_not_logged() {
        let dir = fresh_dir("typed_tasks_late_then_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        let handle = packet_handle.send_adv_task(failing_hash()).unwrap();
        take_in_and_handle_a_result(&mut engine);

        let (got_tx, got_rx) = bounded(1);
        handle
            .then("Recover", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();

        match run_until_received(&mut engine, &got_rx) {
            Handoff::Error(HashError::Read { path, source }) => {
                assert_eq!(path, "a.png");
                assert_eq!(source.to_string(), "access denied");
            }
            Handoff::Value(_) => panic!("expected the task's own error, got a value"),
        }
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Kept for the follow-up, not logged when it arrived, and not lost either.
        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains(HASH_ERROR_TEXT), "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn an_error_whose_handle_was_dropped_before_the_task_finished_is_logged() {
        let dir = fresh_dir("typed_tasks_release_before_result");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // The task waits for `go`, so the release is handled before its result exists.
        let (go_tx, go_rx) = bounded::<()>(1);
        let task = AdvTask::new("HashFile", 1, move || {
            let _ = go_rx.recv();
            Err::<[u8; 32], _>(HashError::read("a.png", "access denied"))
        })
        .unwrap();
        drop(packet_handle.send_adv_task(task).unwrap());
        assert!(engine.check_tasks(), "the engine didn't get the task");
        assert!(engine.check_status(), "the handle's release never came");

        go_tx.send(()).unwrap();
        handle_a_result(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        let errors = read_log(&dir, "Error.log");
        assert_eq!(errors.matches(HASH_ERROR_TEXT).count(), 1, "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn an_error_whose_handle_was_dropped_after_the_task_finished_is_logged() {
        let dir = fresh_dir("typed_tasks_release_after_result");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // Kept while the handle exists, then let go when it's dropped.
        let handle = packet_handle.send_adv_task(failing_hash()).unwrap();
        take_in_and_handle_a_result(&mut engine);
        drop(handle);
        assert!(engine.check_status(), "the handle's release never came");

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        let errors = read_log(&dir, "Error.log");
        assert_eq!(errors.matches(HASH_ERROR_TEXT).count(), 1, "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn at_the_shutdown_an_error_kept_for_a_live_handle_is_lost_work_and_a_value_is_not() {
        let dir = fresh_dir("typed_tasks_kept_at_shutdown");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        engine.config.debugging.is_debugging = true;

        // Both results are kept: their handles are still held when the shutdown starts.
        let failing = packet_handle.send_adv_task(failing_hash()).unwrap();
        take_in_and_handle_a_result(&mut engine);
        let succeeding = packet_handle.send_adv_task(answer()).unwrap();
        take_in_and_handle_a_result(&mut engine);

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
        drop((failing, succeeding));

        // Counted, not handled: the error's own line was never written.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains(&summary_line(0, 0, 1, 0)), "{errors}");
        assert!(!errors.contains(HASH_ERROR_TEXT), "{errors}");

        let debug = read_log(&dir, "Debug.log");
        assert!(debug.contains("AdvTask: 1"), "{debug}");
        assert!(
            debug.contains(&format!("handoff: {HASH_ERROR_TEXT}")),
            "{debug}"
        );
    }

    #[test]
    fn a_hundred_thousand_tasks_leave_nothing_in_the_wait_table() {
        const TASKS: usize = 100_000;
        // Each round fits in the task mailbox: its tasks plus the follow-ups of half of them.
        const ROUND: usize = 500;

        let dir = fresh_dir("typed_tasks_no_growth");
        let (mut engine, packet_handle) = started_engine(&dir, 1_000, 2, 10);
        let ran = Arc::new(AtomicUsize::new(0));
        let deadline = Instant::now() + Duration::from_secs(60);

        for round in 0..TASKS / ROUND {
            for i in round * ROUND..(round + 1) * ROUND {
                let counter = ran.clone();
                let task = AdvTask::new("Count", 1, move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                    Ok::<usize, HashError>(i)
                })
                .unwrap();
                let handle = packet_handle.send_adv_task(task).unwrap();

                // Half get a follow-up, whose own handle is dropped at once. The other half's
                // handles are dropped here, before or after their task finishes.
                if i % 2 == 0 {
                    let counter = ran.clone();
                    handle
                        .then("Recount", move |_| {
                            counter.fetch_add(1, Ordering::Relaxed);
                            Ok::<(), HashError>(())
                        })
                        .unwrap();
                }
            }

            // Every task and follow-up so far has run, so the task mailbox is empty again.
            let sent = (round + 1) * ROUND;
            while ran.load(Ordering::Relaxed) < sent + sent / 2 {
                assert!(Instant::now() < deadline, "the tasks didn't all run");
                engine.run_once();
            }
        }

        // The last results and releases may still be in the mailboxes.
        let settle = Instant::now() + Duration::from_secs(5);
        while engine.wait_table_len() > 0 {
            assert!(
                Instant::now() < settle,
                "{} entries stayed in the wait table",
                engine.wait_table_len()
            );
            engine.run_once();
        }

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
        for file in ["Main.log", "Error.log"] {
            let log = read_log(&dir, file);
            assert!(!log.contains("things not done"), "{file}:\n{log}");
        }
    }
}

/// Forwarding: a task whose closure returns another task's handle finishes with that task's outcome.
/// Its follow-up gets that task's value or error, whether it was declared before or after the
/// forward, and before or after that task finished; a release reaches that task too. Forwards
/// stack, and nothing is left in the wait table afterwards.
mod forwarding {
    use super::*;

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn a_forward_arrives_as_the_id_of_the_task_forwarded_to_and_sends_no_release() {
        let (sender, mailbox, status) = handle_mailbox_and_status();
        let (sent_id, result_id, mut result) =
            run_like_a_worker(forwarding("Process", &sender, answer()));
        assert_eq!(result_id, sent_id);

        // `Process` sent `Answer` while it ran, and forwards to it.
        let answer = mailbox.try_recv().unwrap();
        assert_eq!(answer.task_name(), "Answer");
        assert_eq!(
            format!("{result}"),
            format!(
                "result type: AdvTask, message: , next task: None, handoff: forwarded to task {}",
                answer.id
            )
        );
        let handoff = result.take_handoff().unwrap();
        assert_eq!(handoff.read_forward_id(), Some(answer.id));

        // The forward used the handle up, so dropping it released nothing.
        assert!(
            status.try_recv().is_err(),
            "the forwarded handle sent a release"
        );
    }

    #[test]
    fn a_forwarding_task_s_own_error_is_sealed_like_any_task_s() {
        let process = AdvTask::forward("Process", 1, no_forward).unwrap();
        let (_, _, result) = run_like_a_worker(process);

        let (opened, error_text) = open::<u32, HashError>(result);
        match opened {
            Err(HashError::Read { path, .. }) => assert_eq!(path, "a.png"),
            other => panic!("expected the task's own error, got {other:?}"),
        }
        assert_eq!(
            error_text.as_deref(),
            Some("task 'Process' returned an error: could not read a.png: access denied")
        );
    }

    #[test]
    fn a_follow_up_declared_before_the_forward_moves_on_to_the_task_forwarded_to() {
        let dir = fresh_dir("typed_tasks_forward_then_before");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        let (go_tx, go_rx) = bounded(1);
        let (got_tx, got_rx) = bounded(1);
        packet_handle
            .send_adv_task(forwarding("Process", &packet_handle, gated_answer(go_rx)))
            .unwrap()
            .then("Upload", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();

        // The engine takes in `Process` and `Upload`, which waits for it. `Process` forwards to
        // `Answer`, held back, so `Upload` moves on to wait for `Answer`, and the `WaitingOn`
        // under its own id now names `Answer`.
        take_in_and_handle_a_result(&mut engine);
        assert_eq!(
            engine.wait_table_len(),
            2,
            "only Upload, waiting for Answer, and its `WaitingOn`"
        );

        go_tx.send(()).unwrap();
        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            Handoff::Value(42)
        ));
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_follow_up_declared_after_the_forward_waits_for_the_task_forwarded_to() {
        let dir = fresh_dir("typed_tasks_forward_then_after");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // `Process` forwards to `Answer`, held back, before `Upload` is declared: a link.
        let (go_tx, go_rx) = bounded(1);
        let handle = packet_handle
            .send_adv_task(forwarding("Process", &packet_handle, gated_answer(go_rx)))
            .unwrap();
        take_in_and_handle_a_result(&mut engine);
        assert_eq!(engine.wait_table_len(), 1, "only the link to Answer");

        let (got_tx, got_rx) = bounded(1);
        handle
            .then("Upload", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();
        go_tx.send(()).unwrap();

        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            Handoff::Value(42)
        ));
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn stacked_forwards_hand_the_last_value_to_a_follow_up_declared_afterwards() {
        let dir = fresh_dir("typed_tasks_forward_stacked");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // `Process` forwards to `Save`, which forwards to `Answer`. Each sends the next before it
        // finishes, so the engine takes them in one at a time.
        let save = forwarding("Save", &packet_handle, answer());
        let handle = packet_handle
            .send_adv_task(forwarding("Process", &packet_handle, save))
            .unwrap();
        for _ in 0..3 {
            take_in_and_handle_a_result(&mut engine);
        }
        assert_eq!(
            engine.wait_table_len(),
            3,
            "two links, and Answer's value kept"
        );

        let (got_tx, got_rx) = bounded(1);
        handle
            .then("Upload", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();

        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            Handoff::Value(42)
        ));
        // `Upload` removed both links on its way.
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_release_after_the_forward_lets_go_of_the_error_it_leads_to() {
        let dir = fresh_dir("typed_tasks_forward_release_after");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // `Process` forwards to `HashFile`, whose error is kept while Process's handle exists.
        let handle = packet_handle
            .send_adv_task(forwarding("Process", &packet_handle, failing_hash()))
            .unwrap();
        take_in_and_handle_a_result(&mut engine);
        take_in_and_handle_a_result(&mut engine);
        assert_eq!(
            engine.wait_table_len(),
            2,
            "the link, and HashFile's error kept"
        );

        drop(handle);
        assert!(engine.check_status(), "the handle's release never came");
        assert_eq!(engine.wait_table_len(), 0);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        let errors = read_log(&dir, "Error.log");
        assert_eq!(errors.matches(HASH_ERROR_TEXT).count(), 1, "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn a_release_before_the_forward_reaches_the_task_forwarded_to() {
        let dir = fresh_dir("typed_tasks_forward_release_before");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // `Process` waits for `go`, so its release is handled before it forwards.
        let (go_tx, go_rx) = bounded::<()>(1);
        let sender = packet_handle.clone();
        let process = AdvTask::forward("Process", 1, move || {
            let _ = go_rx.recv();
            Ok(sender.send_adv_task(failing_hash()).unwrap())
        })
        .unwrap();
        drop(packet_handle.send_adv_task(process).unwrap());
        assert!(engine.check_tasks(), "the engine didn't get the task");
        assert!(engine.check_status(), "the handle's release never came");

        // The forward passes the release on to `HashFile`, which hasn't run yet.
        go_tx.send(()).unwrap();
        handle_a_result(&mut engine);
        assert_eq!(engine.wait_table_len(), 1, "HashFile's release, remembered");

        take_in_and_handle_a_result(&mut engine);
        assert_eq!(engine.wait_table_len(), 0);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        let errors = read_log(&dir, "Error.log");
        assert_eq!(errors.matches(HASH_ERROR_TEXT).count(), 1, "{errors}");
        assert!(!errors.contains("things not done"), "{errors}");
    }

    #[test]
    fn then_forward_hands_on_the_value_of_the_task_it_forwards_to() {
        let dir = fresh_dir("typed_tasks_then_forward_value");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // `Process` gets Answer's 42, then sends `Save`, which returns 43, and forwards to it.
        let sender = packet_handle.clone();
        let (got_tx, got_rx) = bounded(1);
        packet_handle
            .send_adv_task(answer())
            .unwrap()
            .then_forward("Process", move |handoff| match handoff {
                Handoff::Value(value) => {
                    let save = AdvTask::new("Save", 1, move || Ok(value + 1)).unwrap();
                    Ok(sender.send_adv_task(save).unwrap())
                }
                Handoff::Error(error) => Err(error),
            })
            .unwrap()
            .then("Upload", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();

        assert!(matches!(
            run_until_received(&mut engine, &got_rx),
            Handoff::Value(43)
        ));
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_forwarding_follow_up_s_own_error_goes_to_whoever_follows_it() {
        let dir = fresh_dir("typed_tasks_then_forward_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // `Process` fails before it forwards.
        let (got_tx, got_rx) = bounded(1);
        packet_handle
            .send_adv_task(answer())
            .unwrap()
            .then_forward("Process", |_| no_forward())
            .unwrap()
            .then("Upload", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashError>(())
            })
            .unwrap();

        match run_until_received(&mut engine, &got_rx) {
            Handoff::Error(HashError::Read { path, .. }) => assert_eq!(path, "a.png"),
            Handoff::Value(_) => panic!("expected Process's own error, got a value"),
        }
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // `Upload` handled it, so the engine didn't write it.
        let errors = read_log(&dir, "Error.log");
        assert!(
            !errors.contains("task 'Process' returned an error"),
            "{errors}"
        );
    }

    #[test]
    fn at_the_shutdown_a_link_is_not_counted_and_the_error_it_leads_to_is_lost_work() {
        let dir = fresh_dir("typed_tasks_forward_at_shutdown");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);

        // The link and HashFile's error are both kept: Process's handle is still held.
        let handle = packet_handle
            .send_adv_task(forwarding("Process", &packet_handle, failing_hash()))
            .unwrap();
        take_in_and_handle_a_result(&mut engine);
        take_in_and_handle_a_result(&mut engine);

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
        drop(handle);

        // One piece of lost work, the error, and nothing for the link.
        let errors = read_log(&dir, "Error.log");
        assert!(errors.contains(&summary_line(0, 0, 1, 0)), "{errors}");
        assert!(!errors.contains(HASH_ERROR_TEXT), "{errors}");
    }

    #[test]
    fn ten_thousand_forwards_leave_nothing_in_the_wait_table() {
        const FORWARDS: usize = 10_000;
        // Each round fits in the task mailbox: its forwarding tasks, the tasks they forward to,
        // and the follow-ups of half of them.
        const ROUND: usize = 250;

        let dir = fresh_dir("typed_tasks_forward_no_growth");
        let (mut engine, packet_handle) = started_engine(&dir, 1_000, 2, 10);
        let ran = Arc::new(AtomicUsize::new(0));
        let deadline = Instant::now() + Duration::from_secs(60);

        for round in 0..FORWARDS / ROUND {
            for i in round * ROUND..(round + 1) * ROUND {
                let counter = ran.clone();
                let save = AdvTask::new("Save", 1, move || {
                    counter.fetch_add(1, Ordering::Relaxed);
                    Ok::<usize, HashError>(i)
                })
                .unwrap();
                let handle = packet_handle
                    .send_adv_task(forwarding("Process", &packet_handle, save))
                    .unwrap();

                // Half get a follow-up, which counts only the value of the task forwarded to.
                // The other half's handles are dropped here, before or after the forward.
                if i % 2 == 0 {
                    let counter = ran.clone();
                    handle
                        .then("Upload", move |handoff| {
                            if matches!(handoff, Handoff::Value(value) if value == i) {
                                counter.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok::<(), HashError>(())
                        })
                        .unwrap();
                }
            }

            // Every task so far has run, and every follow-up got its value.
            let sent = (round + 1) * ROUND;
            while ran.load(Ordering::Relaxed) < sent + sent / 2 {
                assert!(Instant::now() < deadline, "the tasks didn't all run");
                engine.run_once();
            }
        }

        // The last results and releases may still be in the mailboxes.
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
        for file in ["Main.log", "Error.log"] {
            let log = read_log(&dir, file);
            assert!(!log.contains("things not done"), "{file}:\n{log}");
            assert!(!log.contains("downcast"), "{file}:\n{log}");
        }
    }
}

/// Priorities: a follow-up inherits its dependency's priority, capped just below
/// realtime, unless it was given one with `then_with_priority`. The engine applies the cap when it
/// hands the follow-up its input, through `with_input`.
mod priority {
    use super::*;

    #[test]
    fn an_inherited_realtime_priority_is_capped_just_below_realtime() {
        // With a `max_priority` of 4, realtime starts at 3, so the cap is 2.
        for realtime in [3, 4] {
            let (follow_up, sealed) = follow_up_of(realtime, None);
            assert_eq!(
                follow_up.base_priority, realtime,
                "it's sent with the inherited priority"
            );

            let ready = follow_up.with_input(sealed, 4);
            assert_eq!(ready.base_priority, 2);
            assert_eq!(ready.waiting_on_id(), None, "with its input, it can run");
        }
    }

    #[test]
    fn an_inherited_priority_below_realtime_is_kept() {
        let (follow_up, sealed) = follow_up_of(1, None);
        assert_eq!(follow_up.with_input(sealed, 4).base_priority, 1);
    }

    #[test]
    fn a_given_priority_is_kept_even_when_realtime() {
        let (follow_up, sealed) = follow_up_of(1, Some(4));
        assert_eq!(follow_up.base_priority, 4);
        assert_eq!(follow_up.with_input(sealed, 4).base_priority, 4);
    }

    #[test]
    fn a_forwarding_follow_up_inherits_like_then() {
        let (handle, mailbox) = handle_and_mailbox(false);
        let first = AdvTask::new("First", 4, || Ok::<u32, HashError>(7)).unwrap();
        handle
            .send_adv_task(first)
            .unwrap()
            .then_forward("Next", |_| no_forward())
            .unwrap();

        let first = mailbox.try_recv().unwrap();
        let follow_up = mailbox.try_recv().unwrap();
        assert_eq!(
            follow_up.base_priority, 4,
            "it's sent with the inherited priority"
        );

        let (_, mut result) = first.process().unwrap();
        let ready = follow_up.with_input(result.take_handoff().unwrap(), 4);
        assert_eq!(ready.base_priority, 2, "capped just below realtime");
    }

    #[test]
    fn when_every_level_is_realtime_an_inherited_priority_is_kept() {
        // With a `max_priority` of 1, realtime starts at 0: there's no level below it to cap to.
        let (follow_up, sealed) = follow_up_of(1, None);
        assert_eq!(follow_up.with_input(sealed, 1).base_priority, 1);
    }
}

/// The case typed tasks were made for: ImgCompression wants "hash the file, then load the image with the
/// digest". The two plugins below never name each other: each exports typed task makers, and the
/// glue, written by whoever uses both, wires them with `then`. The hash is a stand-in (FNV-1a);
/// the real BLAKE3 plugin lives in TestLib.
mod img_example {
    use super::*;

    use std::path::{Path, PathBuf};

    /// A hash plugin: it knows nothing about images.
    mod hasher {
        use super::*;

        /// A file's digest, standing in for BLAKE3's 32 bytes.
        pub type Digest = u64;

        /// Sends a task that hashes the file at `path`, and returns its handle.
        pub fn hash(
            sender: &PacketHandle,
            path: PathBuf,
        ) -> Result<AdvTaskHandle<Digest, HashError>, ThryveError> {
            let task = AdvTask::new("Hash", 2, move || {
                let bytes = std::fs::read(&path).map_err(|source| HashError::Read {
                    path: path.display().to_string(),
                    source,
                })?;
                Ok(fnv1a(&bytes))
            })?;
            sender.send_adv_task(task)
        }

        /// 64-bit FNV-1a, the stand-in for BLAKE3.
        pub fn fnv1a(bytes: &[u8]) -> Digest {
            bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, &byte| {
                (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
            })
        }
    }

    /// An image plugin: it takes a digest from anywhere, and knows nothing about the hasher.
    mod img_compression {
        use super::*;

        /// A loaded image, with the digest of the file it came from.
        #[derive(Debug)]
        pub struct Img {
            pub pixels: Vec<u8>,
            pub digest: u64,
        }

        /// The image plugin's own error.
        #[derive(Debug)]
        pub enum ImgError {
            /// The image file couldn't be read.
            Read(std::io::Error),
        }

        impl fmt::Display for ImgError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                match self {
                    ImgError::Read(_) => write!(f, "could not load the image"),
                }
            }
        }

        impl std::error::Error for ImgError {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                match self {
                    ImgError::Read(source) => Some(source),
                }
            }
        }

        /// Loads the image at `path`, tagged with `digest`. A real plugin decodes the file; here
        /// the pixels are its bytes.
        pub fn load(path: &Path, digest: u64) -> Result<Img, ImgError> {
            let pixels = std::fs::read(path).map_err(ImgError::Read)?;
            Ok(Img { pixels, digest })
        }
    }

    /// The glue's own error: whichever plugin's error stopped it, still typed.
    #[derive(Debug)]
    enum HashThenLoadError {
        Hash(HashError),
        Load(img_compression::ImgError),
    }

    impl fmt::Display for HashThenLoadError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                HashThenLoadError::Hash(_) => write!(f, "could not hash the image"),
                HashThenLoadError::Load(_) => write!(f, "could not load the image"),
            }
        }
    }

    impl std::error::Error for HashThenLoadError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            match self {
                HashThenLoadError::Hash(source) => Some(source),
                HashThenLoadError::Load(source) => Some(source),
            }
        }
    }

    /// The glue: hash the file, then load it with the digest. Returns the loading step's handle.
    fn hash_then_load(
        sender: &PacketHandle,
        path: PathBuf,
    ) -> Result<AdvTaskHandle<img_compression::Img, HashThenLoadError>, ThryveError> {
        hasher::hash(sender, path.clone())?.then("LoadImg", move |handoff| match handoff {
            Handoff::Value(digest) => {
                img_compression::load(&path, digest).map_err(HashThenLoadError::Load)
            }
            Handoff::Error(error) => Err(HashThenLoadError::Hash(error)),
        })
    }

    #[test]
    fn hash_then_load_hands_the_digest_to_the_loader() {
        let dir = fresh_dir("typed_tasks_img_hash_then_load");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        let photo = dir.join("photo.png");
        std::fs::write(&photo, b"not really a png").unwrap();

        let (got_tx, got_rx) = bounded(1);
        hash_then_load(&packet_handle, photo)
            .unwrap()
            .then("Show", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashThenLoadError>(())
            })
            .unwrap();

        match run_until_received(&mut engine, &got_rx) {
            Handoff::Value(img) => {
                assert_eq!(img.pixels, b"not really a png");
                assert_eq!(img.digest, hasher::fnv1a(b"not really a png"));
            }
            Handoff::Error(error) => panic!("expected the image, got {error:?}"),
        }
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_failed_hash_reaches_the_glue_as_the_hasher_s_own_error() {
        let dir = fresh_dir("typed_tasks_img_hash_fails");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        let missing = dir.join("missing.png");

        let (got_tx, got_rx) = bounded(1);
        hash_then_load(&packet_handle, missing.clone())
            .unwrap()
            .then("Show", move |handoff| {
                let _ = got_tx.send(handoff);
                Ok::<(), HashThenLoadError>(())
            })
            .unwrap();

        match run_until_received(&mut engine, &got_rx) {
            Handoff::Error(HashThenLoadError::Hash(HashError::Read { path, source })) => {
                assert_eq!(path, missing.display().to_string());
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound);
            }
            other => panic!("expected the hasher's error, got {other:?}"),
        }
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // It was handled on the way, so the engine didn't write it.
        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains("returned an error"), "{errors}");
    }
}
