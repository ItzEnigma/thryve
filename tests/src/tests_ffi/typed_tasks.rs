// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! The C side of typed tasks, through a running engine: values and errors handed to follow-ups,
//! forwarding, joins, splits, cancelling and fan-out, and who frees what. Also the last error, C
//! observers, and the generated header.
//!
//! Every job here is an `extern "C" fn`, as a C plugin's would be. A job tells the test what it
//! saw through its user data, a [`Probe`], and the test counts what Thryve frees.

use std::ffi::{CStr, c_void};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crossbeam::channel::{Receiver, Sender, unbounded};

use thryve_core::runtime::engine::Engine;
use thryve_ffi::{
    THRYVE_ERROR_NO_OUTCOME, THRYVE_ERROR_WRONG_OUTCOME, ThryveAdvHandle, ThryveHandoff,
    ThryveObserver, ThryveOutcome, ThryvePacketHandle, ThryveStatus, ThryveTaskResult,
    thryve_adv_handle_cancel, thryve_adv_handle_fan_out, thryve_adv_handle_free,
    thryve_adv_handle_then, thryve_adv_handle_then_forward, thryve_adv_handles_cancel_all,
    thryve_adv_handles_join, thryve_adv_task_create, thryve_adv_task_create_forward,
    thryve_config_default, thryve_config_free, thryve_fanout_send, thryve_fanout_then,
    thryve_handoff_error_code, thryve_handoff_error_message, thryve_handoff_error_position,
    thryve_handoff_is_error, thryve_handoff_take_value, thryve_handoff_value,
    thryve_handoff_value_count, thryve_last_error, thryve_notification_core,
    thryve_notification_create, thryve_notification_free, thryve_observer_core,
    thryve_observer_create, thryve_observer_free, thryve_observer_pop, thryve_outcome_error,
    thryve_outcome_forward, thryve_outcome_value, thryve_packet_handle_send_adv_task,
    thryve_packet_handle_send_notification, thryve_packet_handle_send_split_tasks,
    thryve_packet_handle_split_into, thryve_task_create,
};
use thryve_utils::bases::observer::Observer;
use thryve_utils::packets::packet_handle::PacketHandle;

use crate::cancelling::{WAIT, holding_the_worker, times_logged, until};
use crate::shutdown_and_startup::{
    finalize_within, fresh_dir, started_engine, started_engine_with,
};
use crate::typed_tasks::{
    handle_and_mailbox, run_until_received, run_until_the_wait_table_is_empty,
};

// ============================================================================
// Probes, values and jobs
// ============================================================================

/// What a test counts: user data and values that Thryve freed.
#[derive(Clone, Default)]
struct Counts {
    user_data_freed: Arc<AtomicUsize>,
    values_freed: Arc<AtomicUsize>,
}

impl Counts {
    fn user_data_freed(&self) -> usize {
        self.user_data_freed.load(Ordering::SeqCst)
    }

    fn values_freed(&self) -> usize {
        self.values_freed.load(Ordering::SeqCst)
    }
}

/// A job's user data: where it tells what it saw, the number it returns, and the counts.
struct Probe {
    seen: Sender<String>,
    number: u64,
    counts: Counts,
    /// The engine's packet handle, for a job that sends a task of its own.
    tx: Option<PacketHandle>,
}

/// A probe, as user data.
fn probe(seen: &Sender<String>, number: u64, counts: &Counts) -> *mut c_void {
    let probe = Probe {
        seen: seen.clone(),
        number,
        counts: counts.clone(),
        tx: None,
    };
    Box::into_raw(Box::new(probe)) as *mut c_void
}

/// A probe for a job that sends a task of its own through `tx`.
fn probe_with_tx(
    seen: &Sender<String>,
    number: u64,
    counts: &Counts,
    tx: &PacketHandle,
) -> *mut c_void {
    let probe = Probe {
        seen: seen.clone(),
        number,
        counts: counts.clone(),
        tx: Some(tx.clone()),
    };
    Box::into_raw(Box::new(probe)) as *mut c_void
}

extern "C" fn free_probe(user_data: *mut c_void) {
    let probe = unsafe { Box::from_raw(user_data as *mut Probe) };
    probe.counts.user_data_freed.fetch_add(1, Ordering::SeqCst);
}

fn probe_of<'a>(user_data: *mut c_void) -> &'a Probe {
    unsafe { &*(user_data as *const Probe) }
}

/// A value: a number, counted when it's freed.
struct Number {
    number: u64,
    freed: Arc<AtomicUsize>,
}

fn number(number: u64, counts: &Counts) -> *mut c_void {
    let value = Number {
        number,
        freed: counts.values_freed.clone(),
    };
    Box::into_raw(Box::new(value)) as *mut c_void
}

extern "C" fn free_number(value: *mut c_void) {
    let value = unsafe { Box::from_raw(value as *mut Number) };
    value.freed.fetch_add(1, Ordering::SeqCst);
}

fn number_at(handoff: ThryveHandoff, index: usize) -> u64 {
    unsafe { (*(thryve_handoff_value(handoff, index) as *const Number)).number }
}

/// What a handoff holds, as text: `values 7, 8`, or `error 2 at 1: photo.png is corrupt`.
fn describe(handoff: ThryveHandoff) -> String {
    if thryve_handoff_is_error(handoff) {
        let message = unsafe { CStr::from_ptr(thryve_handoff_error_message(handoff)) };
        return format!(
            "error {} at {}: {}",
            thryve_handoff_error_code(handoff),
            thryve_handoff_error_position(handoff),
            message.to_string_lossy()
        );
    }

    let values: Vec<String> = (0..thryve_handoff_value_count(handoff))
        .map(|index| number_at(handoff, index).to_string())
        .collect();
    format!("values {}", values.join(", "))
}

fn last_error() -> String {
    let error = thryve_last_error();
    if error.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(error) }
        .to_string_lossy()
        .into_owned()
}

/// The engine's packet handle, as C holds it.
fn c_handle(tx: &PacketHandle) -> ThryvePacketHandle {
    tx as *const PacketHandle as ThryvePacketHandle
}

/// Hash, Archive, Shrink and the like: returns its probe's number as its value.
extern "C" fn returning(user_data: *mut c_void) -> ThryveOutcome {
    let probe = probe_of(user_data);
    let _ = probe.seen.send(format!("returned {}", probe.number));
    thryve_outcome_value(number(probe.number, &probe.counts), Some(free_number))
}

/// A Hash that fails: photo.png is corrupt, with its probe's number as the code.
extern "C" fn corrupt(user_data: *mut c_void) -> ThryveOutcome {
    let code = probe_of(user_data).number as i32;
    unsafe { thryve_outcome_error(code, c"photo.png is corrupt".as_ptr()) }
}

/// A job that returns no outcome at all.
extern "C" fn no_outcome(_user_data: *mut c_void) -> ThryveOutcome {
    std::ptr::null_mut()
}

/// LoadImg, Report and the like: tells what it was handed, and returns no data.
extern "C" fn telling(handoff: ThryveHandoff, user_data: *mut c_void) -> ThryveOutcome {
    let _ = probe_of(user_data).seen.send(describe(handoff));
    thryve_outcome_value(std::ptr::null_mut(), None)
}

/// Save: takes the value it was handed, and returns it as its own.
extern "C" fn keeping(handoff: ThryveHandoff, user_data: *mut c_void) -> ThryveOutcome {
    let kept = number_at(handoff, 0);
    let value = thryve_handoff_take_value(handoff, 0);
    let _ = probe_of(user_data).seen.send(format!("took {kept}"));
    thryve_outcome_value(value, Some(free_number))
}

/// Sends Shrink, which returns `number / 4`, and forwards to it.
fn forward_to_shrink(probe: &Probe, number: u64) -> ThryveOutcome {
    let tx = probe.tx.as_ref().expect("a probe with the packet handle");
    let shrink = unsafe {
        thryve_adv_task_create(
            c"Shrink".as_ptr(),
            2,
            Some(returning),
            self::probe(&probe.seen, number / 4, &probe.counts),
            Some(free_probe),
        )
    };
    thryve_outcome_forward(thryve_packet_handle_send_adv_task(c_handle(tx), shrink))
}

/// Prepare, a forwarding task: forwards to a Shrink of its probe's number.
extern "C" fn preparing(user_data: *mut c_void) -> ThryveOutcome {
    let probe = probe_of(user_data);
    forward_to_shrink(probe, probe.number)
}

/// Prepare, a forwarding follow-up: forwards to a Shrink of the value it was handed.
extern "C" fn preparing_what_it_got(
    handoff: ThryveHandoff,
    user_data: *mut c_void,
) -> ThryveOutcome {
    forward_to_shrink(probe_of(user_data), number_at(handoff, 0))
}

/// SumRows: tells which rows it got, and returns how many.
extern "C" fn summing_rows(start: usize, len: usize, user_data: *mut c_void) -> ThryveOutcome {
    let probe = probe_of(user_data);
    let _ = probe.seen.send(format!("rows {start}..{}", start + len));
    thryve_outcome_value(number(len as u64, &probe.counts), Some(free_number))
}

/// FileSize: tells which chunk it got, and returns ten times its number.
extern "C" fn sizing(chunk: *mut c_void, index: usize, user_data: *mut c_void) -> ThryveOutcome {
    let probe = probe_of(user_data);
    let chunk = unsafe { &*(chunk as *const Number) }.number;
    let _ = probe.seen.send(format!("chunk {chunk} at {index}"));
    thryve_outcome_value(number(chunk * 10, &probe.counts), Some(free_number))
}

/// Thumbnail and Metadata: tell which value they read and where it is, and try to take it.
extern "C" fn reading_the_share(handoff: ThryveHandoff, user_data: *mut c_void) -> ThryveOutcome {
    let address = thryve_handoff_value(handoff, 0);
    let refused = thryve_handoff_take_value(handoff, 0).is_null() && !last_error().is_empty();
    let _ = probe_of(user_data).seen.send(format!(
        "read {} at {address:p}, taking it refused: {refused}",
        number_at(handoff, 0)
    ));
    thryve_outcome_value(std::ptr::null_mut(), None)
}

/// Creates and sends a typed task running `job`, with `user_data`.
fn send(
    tx: &PacketHandle,
    name: &CStr,
    job: extern "C" fn(*mut c_void) -> ThryveOutcome,
    user_data: *mut c_void,
) -> ThryveAdvHandle {
    let task =
        unsafe { thryve_adv_task_create(name.as_ptr(), 2, Some(job), user_data, Some(free_probe)) };
    assert!(!task.is_null(), "{}", last_error());
    let handle = thryve_packet_handle_send_adv_task(c_handle(tx), task);
    assert!(!handle.is_null(), "{}", last_error());
    handle
}

/// Declares `job` as the follow-up of `handle`, with `user_data`.
fn then(
    handle: ThryveAdvHandle,
    name: &CStr,
    job: extern "C" fn(ThryveHandoff, *mut c_void) -> ThryveOutcome,
    user_data: *mut c_void,
) -> ThryveAdvHandle {
    let follow_up = unsafe {
        thryve_adv_handle_then(
            handle,
            name.as_ptr(),
            Some(job),
            user_data,
            Some(free_probe),
        )
    };
    assert!(!follow_up.is_null(), "{}", last_error());
    follow_up
}

/// Joins `handles` into `job`, with `user_data`.
fn join(
    handles: &mut [ThryveAdvHandle],
    name: &CStr,
    job: extern "C" fn(ThryveHandoff, *mut c_void) -> ThryveOutcome,
    user_data: *mut c_void,
) -> ThryveAdvHandle {
    let joined = unsafe {
        thryve_adv_handles_join(
            handles.as_mut_ptr(),
            handles.len(),
            name.as_ptr(),
            Some(job),
            user_data,
            Some(free_probe),
        )
    };
    assert!(!joined.is_null(), "{}", last_error());
    joined
}

/// Runs the engine's turns until `done`, for up to 5 seconds.
fn run_until(engine: &mut Engine, what: &str, done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done() {
        assert!(Instant::now() < deadline, "{what} never happened");
        engine.run_once();
    }
}

/// Runs the engine's turns until a job tells something that starts with `start`, and returns it.
/// What else the jobs tell meanwhile is skipped.
fn run_until_told(engine: &mut Engine, seen: &Receiver<String>, start: &str) -> String {
    loop {
        let told = run_until_received(engine, seen);
        if told.starts_with(start) {
            return told;
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

mod values_and_errors {
    use super::*;

    #[test]
    fn a_follow_up_reads_the_value_which_is_freed_once_its_job_returns() {
        let dir = fresh_dir("ffi_a_follow_up_reads_the_value");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let digest = send(&tx, c"Hash", returning, probe(&seen_tx, 7, &counts));
        let pixels = then(digest, c"LoadImg", telling, probe(&seen_tx, 0, &counts));
        thryve_adv_handle_free(pixels);

        assert_eq!(run_until_received(&mut engine, &seen), "returned 7");
        assert_eq!(run_until_received(&mut engine, &seen), "values 7");
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(counts.values_freed(), 1, "Hash's value, after LoadImg");
        assert_eq!(counts.user_data_freed(), 2);
    }

    #[test]
    fn a_follow_up_that_takes_the_value_owns_it_so_it_is_freed_once() {
        let dir = fresh_dir("ffi_a_follow_up_that_takes_the_value");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        // Save hands Hash's value on as its own, and nobody follows Save.
        let digest = send(&tx, c"Hash", returning, probe(&seen_tx, 7, &counts));
        let saved = then(digest, c"Save", keeping, probe(&seen_tx, 0, &counts));
        thryve_adv_handle_free(saved);

        assert_eq!(run_until_received(&mut engine, &seen), "returned 7");
        assert_eq!(run_until_received(&mut engine, &seen), "took 7");
        run_until(&mut engine, "the value's release", || {
            counts.values_freed() == 1
        });
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(counts.values_freed(), 1);
        assert_eq!(counts.user_data_freed(), 2);
    }

    #[test]
    fn an_error_reaches_the_follow_up_with_its_code_and_message_and_is_not_logged() {
        let dir = fresh_dir("ffi_an_error_reaches_the_follow_up");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let digest = send(&tx, c"Hash", corrupt, probe(&seen_tx, 2, &counts));
        let pixels = then(digest, c"LoadImg", telling, probe(&seen_tx, 0, &counts));
        thryve_adv_handle_free(pixels);

        assert_eq!(
            run_until_received(&mut engine, &seen),
            "error 2 at 0: photo.png is corrupt"
        );
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(times_logged(&dir, "photo.png is corrupt"), 0);
        assert_eq!(counts.user_data_freed(), 2);
    }

    #[test]
    fn an_error_nobody_follows_is_logged_with_the_task_s_name_and_the_code() {
        let dir = fresh_dir("ffi_an_error_nobody_follows");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, _seen) = unbounded();
        let counts = Counts::default();

        let digest = send(&tx, c"Hash", corrupt, probe(&seen_tx, 2, &counts));
        thryve_adv_handle_free(digest);

        until(&mut engine, Engine::check_tasks, "Hash");
        until(&mut engine, Engine::check_results, "Hash's error");
        until(&mut engine, Engine::check_status, "the release");
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(
            times_logged(
                &dir,
                "task 'Hash' returned an error: photo.png is corrupt (code 2)"
            ),
            1
        );
    }

    #[test]
    fn a_job_that_returns_no_outcome_fails_with_thryve_s_own_code() {
        let dir = fresh_dir("ffi_a_job_that_returns_no_outcome");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let digest = send(&tx, c"Hash", no_outcome, probe(&seen_tx, 0, &counts));
        let pixels = then(digest, c"LoadImg", telling, probe(&seen_tx, 0, &counts));
        thryve_adv_handle_free(pixels);

        assert_eq!(
            run_until_received(&mut engine, &seen),
            format!("error {THRYVE_ERROR_NO_OUTCOME} at 0: the job returned no outcome")
        );
        assert!(finalize_within(engine, WAIT).is_some());
    }
}

mod forwarding {
    use super::*;

    #[test]
    fn whoever_follows_a_forwarding_task_gets_the_value_of_the_task_it_forwarded_to() {
        let dir = fresh_dir("ffi_a_forwarding_task");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let prepare = unsafe {
            thryve_adv_task_create_forward(
                c"Prepare".as_ptr(),
                2,
                Some(preparing),
                probe_with_tx(&seen_tx, 28, &counts, &tx),
                Some(free_probe),
            )
        };
        let prepared = thryve_packet_handle_send_adv_task(c_handle(&tx), prepare);
        let report = then(prepared, c"Report", telling, probe(&seen_tx, 0, &counts));
        thryve_adv_handle_free(report);

        assert_eq!(run_until_received(&mut engine, &seen), "returned 7");
        assert_eq!(run_until_received(&mut engine, &seen), "values 7");
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(counts.values_freed(), 1, "Shrink's value, after Report");
        assert_eq!(
            counts.user_data_freed(),
            3,
            "Prepare's, Shrink's and Report's"
        );
    }

    #[test]
    fn a_follow_up_can_forward_too() {
        let dir = fresh_dir("ffi_a_follow_up_can_forward");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let digest = send(&tx, c"Hash", returning, probe(&seen_tx, 28, &counts));
        let prepared = unsafe {
            thryve_adv_handle_then_forward(
                digest,
                c"Prepare".as_ptr(),
                Some(preparing_what_it_got),
                probe_with_tx(&seen_tx, 0, &counts, &tx),
                Some(free_probe),
            )
        };
        let report = then(prepared, c"Report", telling, probe(&seen_tx, 0, &counts));
        thryve_adv_handle_free(report);

        assert_eq!(run_until_received(&mut engine, &seen), "returned 28");
        assert_eq!(run_until_received(&mut engine, &seen), "returned 7");
        assert_eq!(run_until_received(&mut engine, &seen), "values 7");
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(counts.values_freed(), 2, "Hash's and Shrink's");
    }

    #[test]
    fn a_forwarding_task_that_returns_a_value_fails_and_its_value_is_freed() {
        let dir = fresh_dir("ffi_a_forwarding_task_that_returns_a_value");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let prepare = unsafe {
            thryve_adv_task_create_forward(
                c"Prepare".as_ptr(),
                2,
                Some(returning),
                probe(&seen_tx, 7, &counts),
                Some(free_probe),
            )
        };
        let prepared = thryve_packet_handle_send_adv_task(c_handle(&tx), prepare);
        let report = then(prepared, c"Report", telling, probe(&seen_tx, 0, &counts));
        thryve_adv_handle_free(report);

        assert_eq!(run_until_received(&mut engine, &seen), "returned 7");
        assert_eq!(
            run_until_received(&mut engine, &seen),
            format!(
                "error {THRYVE_ERROR_WRONG_OUTCOME} at 0: the job returned a value, but its task forwards"
            )
        );
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(counts.values_freed(), 1);
    }
}

mod joins_and_splits {
    use super::*;

    #[test]
    fn a_join_gets_every_value_in_the_array_s_order_and_sets_the_entries_to_null() {
        let dir = fresh_dir("ffi_a_join_gets_every_value");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let mut parts = [
            send(&tx, c"Archive", returning, probe(&seen_tx, 1, &counts)),
            send(&tx, c"Thumbnail", returning, probe(&seen_tx, 2, &counts)),
            send(&tx, c"Metadata", returning, probe(&seen_tx, 3, &counts)),
        ];
        let report = join(&mut parts, c"Report", telling, probe(&seen_tx, 0, &counts));
        thryve_adv_handle_free(report);

        assert!(parts.iter().all(|part| part.is_null()));
        assert_eq!(
            run_until_told(&mut engine, &seen, "values"),
            "values 1, 2, 3"
        );
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(counts.values_freed(), 3);
        assert_eq!(counts.user_data_freed(), 4);
    }

    #[test]
    fn a_join_gets_the_first_error_and_where_it_came_from() {
        let dir = fresh_dir("ffi_a_join_gets_the_first_error");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let mut parts = [
            send(&tx, c"Archive", returning, probe(&seen_tx, 1, &counts)),
            send(&tx, c"Thumbnail", corrupt, probe(&seen_tx, 2, &counts)),
            send(&tx, c"Metadata", returning, probe(&seen_tx, 3, &counts)),
        ];
        let report = join(&mut parts, c"Report", telling, probe(&seen_tx, 0, &counts));
        thryve_adv_handle_free(report);

        assert_eq!(
            run_until_told(&mut engine, &seen, "error"),
            "error 2 at 1: photo.png is corrupt"
        );
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(counts.values_freed(), 2, "Archive's and Metadata's");
        assert_eq!(times_logged(&dir, "photo.png is corrupt"), 0);
    }

    #[test]
    fn a_join_refuses_a_null_entry_or_a_handle_given_twice_and_frees_its_user_data() {
        let (tx, _mailbox) = handle_and_mailbox(false);
        let (seen_tx, _seen) = unbounded();
        let counts = Counts::default();

        let archive = send(&tx, c"Archive", returning, probe(&seen_tx, 1, &counts));
        let mut parts = [archive, std::ptr::null_mut()];
        let report = unsafe {
            thryve_adv_handles_join(
                parts.as_mut_ptr(),
                2,
                c"Report".as_ptr(),
                Some(telling),
                probe(&seen_tx, 0, &counts),
                Some(free_probe),
            )
        };
        assert!(report.is_null());
        assert_eq!(last_error(), "handles[1] is NULL");
        assert!(parts.iter().all(|part| part.is_null()));
        assert_eq!(counts.user_data_freed(), 1, "Report's");

        let thumbnail = send(&tx, c"Thumbnail", returning, probe(&seen_tx, 2, &counts));
        let mut parts = [thumbnail, thumbnail];
        let report = unsafe {
            thryve_adv_handles_join(
                parts.as_mut_ptr(),
                2,
                c"Report".as_ptr(),
                Some(telling),
                std::ptr::null_mut(),
                None,
            )
        };
        assert!(report.is_null());
        assert_eq!(last_error(), "handles[1] appears twice");
    }

    #[test]
    fn split_into_cuts_balanced_pieces_that_cover_every_item_once() {
        let dir = fresh_dir("ffi_split_into_cuts_balanced_pieces");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        // Ten rows in four pieces; the rows' probe is shared by every piece.
        let mut pieces = [std::ptr::null_mut(); 4];
        let mut piece_count = 0;
        let status = unsafe {
            thryve_packet_handle_split_into(
                c_handle(&tx),
                10,
                c"SumRows".as_ptr(),
                4,
                2,
                Some(summing_rows),
                probe(&seen_tx, 0, &counts),
                Some(free_probe),
                pieces.as_mut_ptr(),
                &mut piece_count,
            )
        };
        assert_eq!(status, ThryveStatus::Ok, "{}", last_error());
        assert_eq!(piece_count, 4);

        let total = join(
            &mut pieces,
            c"TotalRows",
            telling,
            probe(&seen_tx, 0, &counts),
        );
        thryve_adv_handle_free(total);

        let told: Vec<String> = (0..5)
            .map(|_| run_until_received(&mut engine, &seen))
            .collect();
        assert_eq!(
            told,
            [
                "rows 0..2",
                "rows 2..5",
                "rows 5..7",
                "rows 7..10",
                "values 2, 3, 2, 3"
            ]
        );
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(
            counts.user_data_freed(),
            2,
            "the rows' probe once, and TotalRows'"
        );
        assert_eq!(counts.values_freed(), 4);
    }

    #[test]
    fn send_split_tasks_hands_each_piece_its_own_chunk_and_frees_it() {
        let dir = fresh_dir("ffi_send_split_tasks");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();
        let chunk_counts = Counts::default();

        let chunks = [
            number(1, &chunk_counts),
            number(2, &chunk_counts),
            number(3, &chunk_counts),
        ];
        let mut pieces = [std::ptr::null_mut(); 3];
        let status = unsafe {
            thryve_packet_handle_send_split_tasks(
                c_handle(&tx),
                chunks.as_ptr(),
                3,
                Some(free_number),
                c"FileSize".as_ptr(),
                2,
                Some(sizing),
                probe(&seen_tx, 0, &counts),
                Some(free_probe),
                pieces.as_mut_ptr(),
            )
        };
        assert_eq!(status, ThryveStatus::Ok, "{}", last_error());

        let sizes = join(
            &mut pieces,
            c"TotalSize",
            telling,
            probe(&seen_tx, 0, &counts),
        );
        thryve_adv_handle_free(sizes);

        let told: Vec<String> = (0..4)
            .map(|_| run_until_received(&mut engine, &seen))
            .collect();
        assert_eq!(
            told,
            [
                "chunk 1 at 0",
                "chunk 2 at 1",
                "chunk 3 at 2",
                "values 10, 20, 30"
            ]
        );
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(
            chunk_counts.values_freed(),
            3,
            "each chunk, after its piece"
        );
        assert_eq!(
            counts.values_freed(),
            3,
            "each piece's value, after TotalSize"
        );
    }

    #[test]
    fn a_split_into_zero_pieces_is_refused_and_its_user_data_is_freed() {
        let (tx, _mailbox) = handle_and_mailbox(false);
        let (seen_tx, _seen) = unbounded();
        let counts = Counts::default();

        let mut pieces = [std::ptr::null_mut(); 1];
        let mut piece_count = 99;
        let status = unsafe {
            thryve_packet_handle_split_into(
                c_handle(&tx),
                10,
                c"SumRows".as_ptr(),
                0,
                2,
                Some(summing_rows),
                probe(&seen_tx, 0, &counts),
                Some(free_probe),
                pieces.as_mut_ptr(),
                &mut piece_count,
            )
        };

        assert_eq!(status, ThryveStatus::OperationFail);
        assert_eq!(
            last_error(),
            "split failed: cannot split 'SumRows' into 0 pieces"
        );
        assert_eq!(piece_count, 99, "nothing is written");
        assert_eq!(counts.user_data_freed(), 1);
    }
}

mod cancelling {
    use super::*;

    #[test]
    fn cancelling_a_follow_up_cancels_the_task_it_follows_and_frees_both_user_data() {
        let dir = fresh_dir("ffi_cancelling_a_follow_up");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        // The only worker is busy, so Hash waits in the queue, and LoadImg waits for Hash.
        let go = holding_the_worker(&mut engine, &tx);
        let digest = send(&tx, c"Hash", returning, probe(&seen_tx, 7, &counts));
        until(&mut engine, Engine::check_tasks, "Hash");
        let pixels = then(digest, c"LoadImg", telling, probe(&seen_tx, 0, &counts));
        until(&mut engine, Engine::check_tasks, "LoadImg");

        thryve_adv_handle_cancel(pixels);
        until(&mut engine, Engine::check_status, "the cancel");
        go.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(finalize_within(engine, WAIT).is_some());

        assert!(seen.try_recv().is_err(), "Hash or LoadImg ran");
        assert_eq!(counts.user_data_freed(), 2);
        assert_eq!(counts.values_freed(), 0);
    }

    #[test]
    fn cancel_all_cancels_every_piece_and_sets_the_entries_to_null() {
        let dir = fresh_dir("ffi_cancel_all");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let go = holding_the_worker(&mut engine, &tx);
        let mut pieces = [std::ptr::null_mut(); 3];
        let mut piece_count = 0;
        let status = unsafe {
            thryve_packet_handle_split_into(
                c_handle(&tx),
                3,
                c"SumRows".as_ptr(),
                3,
                2,
                Some(summing_rows),
                probe(&seen_tx, 0, &counts),
                Some(free_probe),
                pieces.as_mut_ptr(),
                &mut piece_count,
            )
        };
        assert_eq!(status, ThryveStatus::Ok, "{}", last_error());
        until(&mut engine, Engine::check_tasks, "the pieces");

        unsafe { thryve_adv_handles_cancel_all(pieces.as_mut_ptr(), piece_count) };
        assert!(pieces.iter().all(|piece| piece.is_null()));
        until(&mut engine, Engine::check_status, "the cancels");
        go.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(finalize_within(engine, WAIT).is_some());

        assert!(seen.try_recv().is_err(), "a piece ran");
        assert_eq!(counts.user_data_freed(), 1, "the pieces share one probe");
    }
}

mod fan_out {
    use super::*;

    #[test]
    fn every_follower_reads_the_one_value_in_place_and_it_is_freed_once_after_the_last() {
        let dir = fresh_dir("ffi_every_follower_reads_the_one_value");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let pixels = send(&tx, c"LoadImg", returning, probe(&seen_tx, 7, &counts));
        let group = thryve_adv_handle_fan_out(pixels);
        let followers = [c"Thumbnail", c"Metadata"].map(|name| {
            let follower = unsafe {
                thryve_fanout_then(
                    group,
                    name.as_ptr(),
                    Some(reading_the_share),
                    probe(&seen_tx, 0, &counts),
                    Some(free_probe),
                )
            };
            assert!(!follower.is_null(), "{}", last_error());
            follower
        });
        assert_eq!(thryve_fanout_send(group), ThryveStatus::Ok);
        for follower in followers {
            thryve_adv_handle_free(follower);
        }

        assert_eq!(run_until_received(&mut engine, &seen), "returned 7");
        let thumbnail = run_until_received(&mut engine, &seen);
        let metadata = run_until_received(&mut engine, &seen);
        assert!(thumbnail.starts_with("read 7 at "), "{thumbnail}");
        assert!(
            thumbnail.ends_with("taking it refused: true"),
            "{thumbnail}"
        );
        assert_eq!(thumbnail, metadata, "both read the same value, in place");
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(
            counts.values_freed(),
            1,
            "LoadImg's, after the last follower"
        );
        assert_eq!(counts.user_data_freed(), 3);
    }

    #[test]
    fn a_group_with_no_follower_is_refused_and_its_task_s_value_is_freed() {
        let dir = fresh_dir("ffi_a_group_with_no_follower");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let counts = Counts::default();

        let pixels = send(&tx, c"LoadImg", returning, probe(&seen_tx, 7, &counts));
        let group = thryve_adv_handle_fan_out(pixels);
        assert_eq!(thryve_fanout_send(group), ThryveStatus::InvalidState);
        assert!(!last_error().is_empty());

        assert_eq!(run_until_received(&mut engine, &seen), "returned 7");
        run_until(&mut engine, "LoadImg's value freed", || {
            counts.values_freed() == 1
        });
        assert!(finalize_within(engine, WAIT).is_some());
    }
}

mod last_error {
    use super::*;

    extern "C" fn empty_job(_result: *mut ThryveTaskResult, _user_data: *mut std::ffi::c_void) {}

    #[test]
    fn a_failed_call_tells_why_and_the_next_call_clears_it() {
        let task = unsafe {
            thryve_task_create(c"".as_ptr(), 2, Some(empty_job), std::ptr::null_mut(), None)
        };
        assert!(task.is_null());
        assert!(
            last_error().contains("must not be empty"),
            "{}",
            last_error()
        );

        let config = thryve_config_default();
        assert!(thryve_last_error().is_null());
        thryve_config_free(config);
    }

    #[test]
    fn a_null_argument_is_named_and_the_user_data_is_freed_anyway() {
        let (seen_tx, _seen) = unbounded();
        let counts = Counts::default();

        let follow_up = unsafe {
            thryve_adv_handle_then(
                std::ptr::null_mut(),
                c"LoadImg".as_ptr(),
                Some(telling),
                probe(&seen_tx, 0, &counts),
                Some(free_probe),
            )
        };

        assert!(follow_up.is_null());
        assert_eq!(last_error(), "handle is NULL");
        assert_eq!(counts.user_data_freed(), 1);
    }

    #[test]
    fn a_send_once_the_engine_is_gone_says_so_and_frees_the_user_data() {
        let (tx, mailbox) = handle_and_mailbox(false);
        drop(mailbox);
        let (seen_tx, _seen) = unbounded();
        let counts = Counts::default();

        let task = unsafe {
            thryve_adv_task_create(
                c"Hash".as_ptr(),
                2,
                Some(returning),
                probe(&seen_tx, 7, &counts),
                Some(free_probe),
            )
        };
        let digest = thryve_packet_handle_send_adv_task(c_handle(&tx), task);

        assert!(digest.is_null());
        assert_eq!(
            last_error(),
            "engine is not running: failed to send task to the engine"
        );
        assert_eq!(counts.user_data_freed(), 1);
    }
}

mod observers {
    use super::*;

    /// The observer's job: reads each reading the notifications carry, until the shutdown. Then it
    /// tries to free the observer, as older docs said to, which must do nothing.
    extern "C" fn reading_job(observer: ThryveObserver) {
        // Its own copy of the sender: the clean-up frees the observer's data before the last pop
        // returns.
        let seen = unsafe { &*(thryve_observer_core(observer) as *const Sender<String>) }.clone();

        loop {
            let notif = thryve_observer_pop(observer);
            if notif.is_null() {
                break;
            }
            let reading = unsafe { Box::from_raw(thryve_notification_core(notif) as *mut u64) };
            let _ = seen.send(format!("got {reading}"));
            thryve_notification_free(notif);
        }

        thryve_observer_free(observer);
        let _ = seen.send(format!("stopped; {}", last_error()));
    }

    extern "C" fn reading_clean_up(core: *const std::ffi::c_void) {
        let seen = unsafe { Box::from_raw(core as *mut Sender<String>) };
        let _ = seen.send(String::from("cleaned up"));
    }

    #[test]
    fn a_c_observer_gets_its_notifications_until_the_shutdown_and_can_t_free_itself() {
        let dir = fresh_dir("ffi_a_c_observer");
        let (seen_tx, seen) = unbounded::<String>();
        let core = Box::into_raw(Box::new(seen_tx)) as *mut std::ffi::c_void;

        let (mut engine, tx) = started_engine_with(&dir, 10, 1, 1, |ctx, engine| {
            let packet_handle = ctx.packet_handle();
            let observer = unsafe {
                thryve_observer_create(
                    c"Readings".as_ptr(),
                    core,
                    c_handle(&packet_handle),
                    Some(reading_clean_up),
                    Some(reading_job),
                )
            };
            assert!(!observer.is_null(), "{}", last_error());
            let observer = *unsafe { Box::from_raw(observer as *mut Observer) };
            engine.obs_manager.load_observer(observer).unwrap();
        });

        for reading in [3_u64, 4] {
            let notif = unsafe {
                thryve_notification_create(
                    c"Reading".as_ptr(),
                    c"Readings".as_ptr(),
                    Box::into_raw(Box::new(reading)) as *mut std::ffi::c_void,
                )
            };
            assert_eq!(
                thryve_packet_handle_send_notification(c_handle(&tx), notif),
                ThryveStatus::Ok
            );
        }

        assert_eq!(run_until_received(&mut engine, &seen), "got 3");
        assert_eq!(run_until_received(&mut engine, &seen), "got 4");
        assert!(finalize_within(engine, WAIT).is_some());

        let mut at_the_shutdown: Vec<String> =
            (0..2).map(|_| seen.recv_timeout(WAIT).unwrap()).collect();
        at_the_shutdown.sort();
        assert_eq!(
            at_the_shutdown,
            [
                "cleaned up",
                "stopped; the observer belongs to the engine while its job runs"
            ]
        );
    }
}

mod header {
    use super::*;

    /// Every `extern "C"` function the FFI crate's sources define.
    fn exported_functions(crate_dir: &Path) -> Vec<String> {
        let mut functions = Vec::new();

        for file in ["src/lib.rs", "src/typed_tasks.rs"] {
            let source = std::fs::read_to_string(crate_dir.join(file)).unwrap();
            for line in source.lines() {
                let declaration = line
                    .strip_prefix("pub extern \"C\" fn ")
                    .or_else(|| line.strip_prefix("pub unsafe extern \"C\" fn "));
                if let Some(declaration) = declaration {
                    let name = declaration.split('(').next().unwrap();
                    functions.push(name.to_string());
                }
            }
        }

        functions
    }

    #[test]
    fn the_generated_header_declares_every_function_the_library_exports() {
        let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../crates/thryve_ffi");
        let header = std::fs::read_to_string(crate_dir.join("out/thryve.h")).unwrap();
        let functions = exported_functions(&crate_dir);

        let missing: Vec<&String> = functions
            .iter()
            .filter(|name| {
                !header.contains(&format!(" {name}(")) && !header.contains(&format!("*{name}("))
            })
            .collect();
        assert!(functions.len() > 50, "{functions:?}");
        assert!(missing.is_empty(), "missing from the header: {missing:?}");
    }

    #[test]
    fn the_committed_header_is_the_generated_one() {
        let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../crates/thryve_ffi");
        let read = |file: &str| {
            std::fs::read_to_string(crate_dir.join(file))
                .unwrap()
                .replace("\r\n", "\n")
        };

        assert!(
            read("thryve.h") == read("out/thryve.h"),
            "crates/thryve_ffi/thryve.h is out of date: copy out/thryve.h over it"
        );
    }
}
