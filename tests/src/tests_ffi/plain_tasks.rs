// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Plain tasks, plugins and observers from C: a task's job and a plugin's entry point fill in the
//! result they're given, which starts empty and reports nothing if left alone; a plain task's
//! user data is freed once; the jobs are required, and the clean-ups aren't.
//!
//! Every callback here is an `extern "C" fn`, as a C plugin's would be.

use std::ffi::{CStr, c_void};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crossbeam::channel::{Sender, unbounded};

use thryve_core::runtime::engine::Engine;
use thryve_ffi::{
    ThryveObserver, ThryvePacketHandle, ThryveStatus, ThryveTaskResult, thryve_last_error,
    thryve_notification_free, thryve_observer_create, thryve_observer_pop,
    thryve_packet_handle_send_task, thryve_plugin_create, thryve_task_create, thryve_task_free,
};
use thryve_utils::bases::observer::Observer;
use thryve_utils::bases::plugin::Plugin;
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::packets::task::Task;

use crate::cancelling::{WAIT, times_logged, until};
use crate::shutdown_and_startup::{
    finalize_within, fresh_dir, read_log, started_engine, started_engine_with,
};
use crate::typed_tasks::{handle_and_mailbox, run_until_received};

/// What a job is given: where to tell the test what it did, a name, and a count of frees.
struct Note {
    seen: Sender<String>,
    about: &'static str,
    freed: Arc<AtomicUsize>,
}

fn note(seen: &Sender<String>, about: &'static str, freed: &Arc<AtomicUsize>) -> *mut c_void {
    let note = Note {
        seen: seen.clone(),
        about,
        freed: freed.clone(),
    };
    Box::into_raw(Box::new(note)) as *mut c_void
}

extern "C" fn free_note(user_data: *mut c_void) {
    let note = unsafe { Box::from_raw(user_data as *mut Note) };
    note.freed.fetch_add(1, Ordering::SeqCst);
}

fn note_of<'a>(user_data: *mut c_void) -> &'a Note {
    unsafe { &*(user_data as *const Note) }
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

/// Creates and sends a plain task running `job`, with `user_data`.
fn send(
    tx: &PacketHandle,
    name: &CStr,
    job: extern "C" fn(*mut ThryveTaskResult, *mut c_void),
    user_data: *mut c_void,
) {
    let task =
        unsafe { thryve_task_create(name.as_ptr(), 2, Some(job), user_data, Some(free_note)) };
    assert!(!task.is_null(), "{}", last_error());
    assert_eq!(
        thryve_packet_handle_send_task(c_handle(tx), task),
        ThryveStatus::Ok
    );
}

/// Hash and Upload: tell what they ran with, and leave the result alone: nothing to report.
extern "C" fn quiet(_result: *mut ThryveTaskResult, user_data: *mut c_void) {
    let note = note_of(user_data);
    let _ = note.seen.send(format!("ran with {}", note.about));
}

/// Save: reports that the report was saved, in Main.log.
extern "C" fn saving(result: *mut ThryveTaskResult, user_data: *mut c_void) {
    unsafe {
        (*result).result_type = c"Saved".as_ptr();
        (*result).message = c"the report was saved".as_ptr();
        (*result).log_file = c"Main".as_ptr();
    }
    let _ = note_of(user_data).seen.send(String::from("saved"));
}

/// Save, followed by Upload, which it chains through its result.
extern "C" fn saving_then_uploading(result: *mut ThryveTaskResult, user_data: *mut c_void) {
    let note = note_of(user_data);
    let upload = unsafe {
        thryve_task_create(
            c"Upload".as_ptr(),
            2,
            Some(quiet),
            self::note(&note.seen, "the report", &note.freed),
            Some(free_note),
        )
    };
    unsafe {
        (*result).has_chain = true;
        (*result).chain_task = upload;
    }
    let _ = note.seen.send(String::from("saved"));
}

mod results {
    use super::*;

    #[test]
    fn a_job_that_leaves_its_result_alone_logs_nothing_and_its_user_data_is_freed_once() {
        let dir = fresh_dir("ffi_a_job_that_leaves_its_result_alone");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let freed = Arc::new(AtomicUsize::new(0));

        send(&tx, c"Hash", quiet, note(&seen_tx, "photo.png", &freed));

        assert_eq!(run_until_received(&mut engine, &seen), "ran with photo.png");
        until(&mut engine, Engine::check_results, "Hash's result");
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(freed.load(Ordering::SeqCst), 1);
        let main = read_log(&dir, "Main.log");
        assert!(!main.contains("No message"), "{main}");
        assert_eq!(times_logged(&dir, "result type cannot be empty"), 0);
    }

    #[test]
    fn a_job_reports_through_the_result_it_fills_in() {
        let dir = fresh_dir("ffi_a_job_reports_through_its_result");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let freed = Arc::new(AtomicUsize::new(0));

        send(&tx, c"Save", saving, note(&seen_tx, "report.txt", &freed));

        assert_eq!(run_until_received(&mut engine, &seen), "saved");
        until(&mut engine, Engine::check_results, "Save's result");
        assert!(finalize_within(engine, WAIT).is_some());

        let main = read_log(&dir, "Main.log");
        assert_eq!(main.matches("the report was saved").count(), 1, "{main}");
        assert_eq!(freed.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn a_job_chains_the_next_task_through_its_result() {
        let dir = fresh_dir("ffi_a_job_chains_the_next_task");
        let (mut engine, tx) = started_engine(&dir, 10, 1, 1);
        let (seen_tx, seen) = unbounded();
        let freed = Arc::new(AtomicUsize::new(0));

        send(
            &tx,
            c"Save",
            saving_then_uploading,
            note(&seen_tx, "report.txt", &freed),
        );

        assert_eq!(run_until_received(&mut engine, &seen), "saved");
        assert_eq!(
            run_until_received(&mut engine, &seen),
            "ran with the report"
        );
        assert!(finalize_within(engine, WAIT).is_some());

        assert_eq!(freed.load(Ordering::SeqCst), 2, "Save's note and Upload's");
    }
}

mod user_data {
    use super::*;

    #[test]
    fn a_task_freed_unsent_or_refused_frees_its_user_data_once() {
        let (seen_tx, _seen) = unbounded();
        let freed = Arc::new(AtomicUsize::new(0));

        // Freed without ever being sent.
        let task = unsafe {
            thryve_task_create(
                c"Save".as_ptr(),
                2,
                Some(quiet),
                note(&seen_tx, "report.txt", &freed),
                Some(free_note),
            )
        };
        assert!(!task.is_null(), "{}", last_error());
        thryve_task_free(task);
        assert_eq!(freed.load(Ordering::SeqCst), 1);

        // Refused: its name is empty.
        let task = unsafe {
            thryve_task_create(
                c"".as_ptr(),
                2,
                Some(quiet),
                note(&seen_tx, "report.txt", &freed),
                Some(free_note),
            )
        };
        assert!(task.is_null());
        assert_eq!(freed.load(Ordering::SeqCst), 2);
    }
}

mod callbacks {
    use super::*;

    extern "C" fn waiting(observer: ThryveObserver) {
        loop {
            let notif = thryve_observer_pop(observer);
            if notif.is_null() {
                break;
            }
            thryve_notification_free(notif);
        }
    }

    /// The Reader plugin's entry point: tells the test through its core, and reports that it
    /// started.
    extern "C" fn starting(result: *mut ThryveTaskResult, core_ptr: *mut c_void) {
        let seen = unsafe { &*(core_ptr as *const Sender<String>) };
        let _ = seen.send(String::from("started"));
        unsafe {
            (*result).result_type = c"Started".as_ptr();
            (*result).message = c"the reader started".as_ptr();
            (*result).log_file = c"Main".as_ptr();
        }
    }

    #[test]
    fn every_create_refuses_a_null_job_names_it_and_frees_the_user_data() {
        let (tx, _mailbox) = handle_and_mailbox(false);
        let (seen_tx, _seen) = unbounded();
        let freed = Arc::new(AtomicUsize::new(0));

        let task = unsafe {
            thryve_task_create(
                c"Save".as_ptr(),
                2,
                None,
                note(&seen_tx, "report.txt", &freed),
                Some(free_note),
            )
        };
        assert!(task.is_null());
        assert_eq!(last_error(), "job is NULL");
        assert_eq!(freed.load(Ordering::SeqCst), 1);

        let plugin = unsafe {
            thryve_plugin_create(
                c"Reader".as_ptr(),
                std::ptr::null_mut(),
                c_handle(&tx),
                None,
                None,
            )
        };
        assert!(plugin.is_null());
        assert_eq!(last_error(), "entry_point is NULL");

        let observer = unsafe {
            thryve_observer_create(
                c"Readings".as_ptr(),
                std::ptr::null_mut(),
                c_handle(&tx),
                None,
                None,
            )
        };
        assert!(observer.is_null());
        assert_eq!(last_error(), "obs_job_fn is NULL");
    }

    #[test]
    fn a_plugin_and_an_observer_without_a_clean_up_start_and_shut_down_cleanly() {
        let dir = fresh_dir("ffi_without_a_clean_up");
        let (seen_tx, seen) = unbounded::<String>();
        let core = Box::into_raw(Box::new(seen_tx)) as *mut c_void;
        let mut reader_start: Option<Task> = None;

        // The Reader plugin and the Readings observer, neither with a clean-up. The plugin's start
        // task is sent once the workers run.
        let (mut engine, tx) = started_engine_with(&dir, 10, 1, 1, |ctx, engine| {
            let packet_handle = ctx.packet_handle();
            let plugin = unsafe {
                thryve_plugin_create(
                    c"Reader".as_ptr(),
                    core,
                    c_handle(&packet_handle),
                    None,
                    Some(starting),
                )
            };
            assert!(!plugin.is_null(), "{}", last_error());
            let plugin = *unsafe { Box::from_raw(plugin as *mut Plugin) };
            reader_start = Some(
                engine
                    .plugin_manager
                    .load_plugin(&ctx.config, plugin, false)
                    .unwrap(),
            );

            let observer = unsafe {
                thryve_observer_create(
                    c"Readings".as_ptr(),
                    std::ptr::null_mut(),
                    c_handle(&packet_handle),
                    None,
                    Some(waiting),
                )
            };
            assert!(!observer.is_null(), "{}", last_error());
            let observer = *unsafe { Box::from_raw(observer as *mut Observer) };
            engine.obs_manager.load_observer(observer).unwrap();
        });
        tx.send_task(reader_start.take().unwrap()).unwrap();

        assert_eq!(run_until_received(&mut engine, &seen), "started");
        until(&mut engine, Engine::check_results, "the Reader's start");
        assert!(finalize_within(engine, WAIT).is_some());

        let main = read_log(&dir, "Main.log");
        assert_eq!(main.matches("the reader started").count(), 1, "{main}");
        assert_eq!(times_logged(&dir, "failed to clean up"), 0);

        // The core pointer stays the caller's: Thryve never freed it.
        drop(unsafe { Box::from_raw(core as *mut Sender<String>) });
    }
}
