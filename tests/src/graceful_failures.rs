// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Graceful failures: what Thryve does with a developer's mistake or a failing environment.
//!
//! It doesn't panic on them: it returns an error, or degrades and says so in the logs. Each
//! section below covers one group of such cases and proves that the process keeps running and
//! that the error is returned or logged:
//!
//! - names: empty names, control characters, log-file names that are not ASCII, reserved, one of
//!   Thryve's own, or registered twice ignoring case;
//! - control characters in messages, which every log line escapes;
//! - configuration: several invalid values reported at once;
//! - startup: a failed startup returns its error and leaves no Thryve thread running;
//! - sending after the engine is gone, from Rust and through the FFI;
//! - the engine: an empty result type, a notification for an observer that doesn't exist,
//!   duplicate plugin and observer types;
//! - the logger and the topology handler: their degrade paths;
//! - shutdown: a worker that panicked doesn't stop the others from being joined;
//! - protections: aging can't overflow.
//!
//! Some of these cases can't be reached from a unit test: the Windows service receiving
//! `Start` twice and its poisoned locks (both need the service manager; the admin-only
//! end-to-end test in `integration::windows_service_e2e` starts a real service), and a viewer
//! that fails to spawn. Their code logs instead of panicking.
//!
//! A call site can't go missing: the compiler passes it to `send_task` and `chain`
//! (`#[track_caller]`), so there's no degrade path to test here. The tests of the exact lines are
//! with the packet-handle and result tests.
//!
//! Tests that replace the panic hook restore the previous one, and CI runs these tests on one
//! thread (`--test-threads=1`), like the rest of the crate.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Local};
use crossbeam::channel::bounded;
use crossbeam_skiplist::SkipSet;

use thryve_utils::bases::notif::Notif;
use thryve_utils::bases::observer::Observer;
use thryve_utils::cfg::config::Config;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::cfg::dir_data::DirData;
use thryve_utils::cfg::meta_data::MetaData;
use thryve_utils::extras::dir_paths::DirName;
use thryve_utils::extras::jobs::ObsJob;
use thryve_utils::extras::named_input::LogFileNamedInput;
use thryve_utils::impl_as_any;
use thryve_utils::packets::notif_log::Log;
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::packets::task::Task;
use thryve_utils::traits::notif_logic::NotifLogic;
use thryve_utils::traits::observer_logic::ObsLogic;

/// An observer core that does nothing.
struct QuietObserver;

impl ObsLogic for QuietObserver {
    impl_as_any!(QuietObserver);
    fn clean_up(&self, _observer: Arc<Observer>) {}
}

/// A notification core that no built-in observer understands.
struct ForeignCore;

impl NotifLogic for ForeignCore {
    impl_as_any!(ForeignCore);
}

/// A fresh, empty folder for one test's files.
fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("thryve_graceful_failures")
        .join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A context with its own `Main.log` and `Error.log` in `dir`, and a topology folder
/// `dir/topology`. `graph_mode` sets the packet handles' graph-mode switch.
fn ctx_with_files(dir: &Path, graph_mode: bool) -> Ctx {
    let (task_tx, task_rx) = bounded::<Task>(10);
    let (notif_tx, notif_rx) = bounded::<Notif>(10);
    let (status_tx, status_rx) = crossbeam::channel::unbounded();
    let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, graph_mode);

    let mut log_files: HashMap<LogFileNamedInput, Arc<File>> = HashMap::new();
    log_files.insert(
        LogFileNamedInput::new_main(),
        Arc::new(File::create(dir.join("Main.log")).unwrap()),
    );
    log_files.insert(
        LogFileNamedInput::new_err(),
        Arc::new(File::create(dir.join("Error.log")).unwrap()),
    );

    let topology_dir = dir.join("topology");
    std::fs::create_dir_all(&topology_dir).unwrap();

    let mut dir_data = DirData::new(dir.to_path_buf(), log_files);
    dir_data.dir_paths.insert(DirName::Topology, topology_dir);
    let datetime: DateTime<Local> = SystemTime::now().into();

    let mut ctx = Ctx::new(
        Config::default(),
        task_rx,
        notif_rx,
        status_rx,
        packet_handle,
    );
    ctx.meta_data = MetaData::new(datetime, dir_data);
    ctx
}

/// A context with no log files and no folders.
fn bare_ctx() -> Ctx {
    let (task_tx, task_rx) = bounded::<Task>(10);
    let (notif_tx, notif_rx) = bounded::<Notif>(10);
    let (status_tx, status_rx) = crossbeam::channel::unbounded();
    let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
    let dir_data = DirData::new(std::env::temp_dir(), HashMap::new());
    let datetime: DateTime<Local> = SystemTime::now().into();

    let mut ctx = Ctx::new(
        Config::default(),
        task_rx,
        notif_rx,
        status_rx,
        packet_handle,
    );
    ctx.meta_data = MetaData::new(datetime, dir_data);
    ctx
}

/// The message of a log notification, or `None` for any other notification.
fn log_message(notif: &Notif) -> Option<String> {
    notif
        .notif_core
        .as_any()
        .downcast_ref::<Log>()
        .map(|log| log.message.clone())
}

/// Runs `f` with a panic hook that prints nothing, then puts the previous hook back.
fn with_silent_panics<T>(f: impl FnOnce() -> T) -> T {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(|_| {}));
    let result = f();
    std::panic::set_hook(previous);
    result
}

/// Waits up to five seconds for `done` to become true.
fn wait_until(mut done: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if done() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    done()
}

mod names {
    use super::*;
    use thryve_run::init::init::init_directories;
    use thryve_run::run::run_interactive::summon_requirements;
    use thryve_utils::bases::plugin::Plugin;
    use thryve_utils::errors::{NameInputType, NamingError, StartupError, ThryveError};
    use thryve_utils::res::TaskResult;
    use thryve_utils::traits::plugin_logic::PlgLogic;

    struct QuietPlugin;
    impl PlgLogic for QuietPlugin {
        impl_as_any!(QuietPlugin);
        fn clean_up(&self) -> Result<(), thryve_utils::errors::ThryveError> {
            Ok(())
        }
    }

    fn job() -> impl FnOnce() -> TaskResult + Send + Sync + 'static {
        || TaskResult::new("", String::new(), "")
    }

    fn is_control_char_error(error: &NamingError, expected: NameInputType, bad: &str) -> bool {
        matches!(
            error,
            NamingError::NameContainsControlChar { name, kind }
                if name == bad && std::mem::discriminant(kind) == std::mem::discriminant(&expected)
        )
    }

    #[test]
    fn every_kind_of_name_rejects_control_characters() {
        let ctx = bare_ctx();

        let task = Task::new("bad\u{7}task", 0, job()).err().unwrap();
        assert!(is_control_char_error(
            &task,
            NameInputType::TaskName,
            "bad\u{7}task"
        ));

        let plugin = Plugin::new(
            "bad\nplugin",
            Arc::new(QuietPlugin),
            ctx.packet_handle(),
            job(),
        )
        .err()
        .unwrap();
        assert!(is_control_char_error(
            &plugin,
            NameInputType::PlgType,
            "bad\nplugin"
        ));

        let observer = Observer::new(
            "bad\tobserver",
            Arc::new(QuietObserver),
            ObsJob::new(|_: Arc<Observer>| {}),
            ctx.packet_handle(),
        )
        .err()
        .unwrap();
        assert!(is_control_char_error(
            &observer,
            NameInputType::ObsType,
            "bad\tobserver"
        ));

        let notif_type = Notif::new("bad\u{0}type", "Main", Box::new(ForeignCore))
            .err()
            .unwrap();
        assert!(is_control_char_error(
            &notif_type,
            NameInputType::NotifType,
            "bad\u{0}type"
        ));

        let receiver = Notif::new("Log", "bad\rreceiver", Box::new(ForeignCore))
            .err()
            .unwrap();
        assert!(is_control_char_error(
            &receiver,
            NameInputType::NotifReceiver,
            "bad\rreceiver"
        ));
    }

    #[test]
    fn general_names_accept_any_other_text() {
        // Spaces, punctuation and other alphabets are fine; only control characters aren't.
        assert!(Task::new("Größe ändern 1-2 🚀", 0, job()).is_ok());
    }

    #[test]
    fn empty_names_are_rejected() {
        assert!(matches!(
            Task::new("", 0, job()),
            Err(NamingError::NameEmpty {
                kind: NameInputType::TaskName
            })
        ));
    }

    #[test]
    fn log_file_names_must_be_ascii_letters_and_digits() {
        for bad in ["Bad Name", "Payments.log", "Zahlungen\u{e4}"] {
            assert!(matches!(
                LogFileNamedInput::new_register(bad),
                Err(NamingError::LogFileNameASCII { name }) if name == bad
            ));
        }
    }

    #[test]
    fn windows_device_names_are_reserved_ignoring_case() {
        for bad in ["CON", "con", "Com1", "LPT9", "nul", "aux", "Prn"] {
            assert!(
                matches!(
                    LogFileNamedInput::new_register(bad),
                    Err(NamingError::LogFileNameReserved { name }) if name == bad
                ),
                "{bad} should be reserved"
            );
        }
        // Only the exact device names are reserved.
        assert!(LogFileNamedInput::new_register("Console").is_ok());
        assert!(LogFileNamedInput::new_register("COM").is_ok());
    }

    #[test]
    fn thryves_own_log_files_are_reserved_ignoring_case() {
        for bad in ["Main", "ERROR", "debug"] {
            assert!(matches!(
                LogFileNamedInput::new_register(bad),
                Err(NamingError::LogFileNameReservedThryve { name }) if name == bad
            ));
        }
    }

    #[test]
    fn log_file_names_that_differ_only_in_case_are_a_duplicate() {
        // Payments.log and PAYMENTS.log are the same file on Windows.
        let mut ctx = summon_requirements(Config::default());
        ctx.logging_file_names = vec!["Payments".to_string(), "PAYMENTS".to_string()];

        let result = init_directories(&mut ctx);

        assert!(matches!(
            result,
            Err(ThryveError::Startup(StartupError::RegisterDuplicate {
                name,
                kind: NameInputType::LogFileName,
            })) if name == "PAYMENTS"
        ));
    }

    #[test]
    fn a_reserved_log_file_name_stops_startup_with_its_error() {
        let mut ctx = summon_requirements(Config::default());
        ctx.logging_file_names = vec!["nul".to_string()];

        assert!(matches!(
            init_directories(&mut ctx),
            Err(ThryveError::Naming(NamingError::LogFileNameReserved { .. }))
        ));
    }
}

mod messages {
    use super::*;
    use thryve_observers::logger::LoggerObserver;
    use thryve_utils::extras::log_format::log_line;

    #[test]
    fn log_lines_escape_control_characters() {
        let line = log_line(
            SystemTime::now(),
            "LOGGING",
            "Log",
            "first\nsecond\r\tthird\u{1b}[31m",
        );

        assert_eq!(line.lines().count(), 1);
        assert!(line.ends_with("Message: first\\nsecond\\r\\tthird\\u{1b}[31m\n"));
    }

    #[test]
    fn the_logger_writes_a_message_with_control_characters_as_one_line() {
        let dir = fresh_dir("logger_control_chars");
        let ctx = ctx_with_files(&dir, false);
        let observer = LoggerObserver::new(&ctx).unwrap();
        let logger = observer
            .obs_core
            .as_any()
            .downcast_ref::<LoggerObserver>()
            .unwrap();

        logger.handle_log(Log::log("first\nsecond\u{1b}[2J".to_string(), "Main"));

        let written = std::fs::read_to_string(dir.join("Main.log")).unwrap();
        assert_eq!(written.lines().count(), 1);
        assert!(written.contains("Message: first\\nsecond\\u{1b}[2J"));
    }
}

mod config {
    use super::*;
    use thryve_utils::errors::{ConfigError, RuleError, ThryveError, get_err};

    #[test]
    fn a_config_file_reports_every_invalid_value_at_once() {
        let dir = fresh_dir("config_violations");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            "[service]\ncan_stop = true\ncan_pause = true\n\n\
             [engine]\nidle_wait = 100\nshutdown_grace_period = 10\n\n\
             [threads]\nworker_count = 0\n\n\
             [pipes]\ntask_pipe_capacity = 0\nnotif_pipe_capacity = 1024\nres_pipe_capacity = 1024\n\
             tasks_processed_per_turn = 500\nnotifs_processed_per_turn = 500\n\
             res_processed_per_turn = 500\n\n\
             [tasks]\nmax_priority = 15\naging_interval = 3\n\n\
             [debugging]\nis_debugging = false\nwait_before_execution = 10\nvisual_graph = false\n\
             calculate_metrics = false\nlog_tasks = false\n",
        )
        .unwrap();

        let error = Config::load_config_from_file(&path).err().unwrap();

        let ThryveError::Config(ConfigError::ConfigViolation { violation, .. }) = &error else {
            panic!("expected a ConfigViolation, got: {}", get_err(&error));
        };
        assert_eq!(violation.types.len(), 3);
        assert!(matches!(violation.types[0], RuleError::WorkerCount(0, _)));
        assert!(matches!(violation.types[1], RuleError::TaskPipeCap(0, _)));
        assert!(matches!(violation.types[2], RuleError::MaxPriority(15, _)));

        let message = get_err(&error);
        assert!(message.contains("threads.worker_count is 0"));
        assert!(message.contains("pipes.task_pipe_capacity is 0"));
        assert!(message.contains("tasks.max_priority is 15"));
    }
}

#[cfg(windows)]
mod startup {
    use super::*;
    use std::collections::HashSet;
    use thryve_core::runtime::engine::Engine;
    use thryve_run::init::init::EngineInitialization;
    use thryve_run::run::run_interactive::summon_requirements;
    use thryve_utils::errors::{NameInputType, StartupError, ThryveError};

    /// Reads the names of this process's threads from Windows.
    mod os_threads {
        use std::ffi::c_void;

        #[repr(C)]
        struct ThreadEntry32 {
            dw_size: u32,
            cnt_usage: u32,
            th32_thread_id: u32,
            th32_owner_process_id: u32,
            tp_base_pri: i32,
            tp_delta_pri: i32,
            dw_flags: u32,
        }

        const TH32CS_SNAPTHREAD: u32 = 0x0000_0004;
        const THREAD_QUERY_LIMITED_INFORMATION: u32 = 0x0800;

        #[link(name = "kernel32", kind = "raw-dylib")]
        unsafe extern "system" {
            fn CreateToolhelp32Snapshot(flags: u32, process_id: u32) -> *mut c_void;
            fn Thread32First(snapshot: *mut c_void, entry: *mut ThreadEntry32) -> i32;
            fn Thread32Next(snapshot: *mut c_void, entry: *mut ThreadEntry32) -> i32;
            fn OpenThread(desired_access: u32, inherit_handle: i32, thread_id: u32) -> *mut c_void;
            fn GetThreadDescription(thread: *mut c_void, description: *mut *mut u16) -> i32;
            fn LocalFree(memory: *mut c_void) -> *mut c_void;
            fn CloseHandle(object: *mut c_void) -> i32;
            fn GetCurrentProcessId() -> u32;
        }

        /// The id and name of every named thread of this process.
        pub fn named_threads() -> Vec<(u32, String)> {
            let mut threads = Vec::new();

            // SAFETY: plain Win32 calls; every handle and buffer they return is released here.
            unsafe {
                let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
                if snapshot as isize == -1 {
                    return threads;
                }

                let process_id = GetCurrentProcessId();
                let mut entry = ThreadEntry32 {
                    dw_size: size_of::<ThreadEntry32>() as u32,
                    cnt_usage: 0,
                    th32_thread_id: 0,
                    th32_owner_process_id: 0,
                    tp_base_pri: 0,
                    tp_delta_pri: 0,
                    dw_flags: 0,
                };

                let mut more = Thread32First(snapshot, &mut entry);
                while more != 0 {
                    if entry.th32_owner_process_id == process_id {
                        let thread =
                            OpenThread(THREAD_QUERY_LIMITED_INFORMATION, 0, entry.th32_thread_id);
                        if !thread.is_null() {
                            let mut description: *mut u16 = std::ptr::null_mut();
                            if GetThreadDescription(thread, &mut description) >= 0
                                && !description.is_null()
                            {
                                let mut len = 0;
                                while *description.add(len) != 0 {
                                    len += 1;
                                }
                                let name = String::from_utf16_lossy(std::slice::from_raw_parts(
                                    description,
                                    len,
                                ));
                                LocalFree(description.cast());
                                if !name.is_empty() {
                                    threads.push((entry.th32_thread_id, name));
                                }
                            }
                            CloseHandle(thread);
                        }
                    }
                    more = Thread32Next(snapshot, &mut entry);
                }

                CloseHandle(snapshot);
            }

            threads
        }
    }

    /// Whether `name` is one of the names Thryve gives its threads.
    fn is_thryve_thread(name: &str) -> bool {
        name == "Engine Thread"
            || name.starts_with("plugin-worker-")
            || name == "ObsSorterThread"
            || name == "Logging"
            || name == "TopologyHandler"
    }

    #[test]
    fn a_failed_startup_returns_its_error_and_leaves_no_thryve_thread_running() {
        // Check first that thread names can be read, so the final check can't pass by accident.
        let (ready_tx, ready_rx) = bounded::<()>(1);
        let (release_tx, release_rx) = bounded::<()>(1);
        let probe = std::thread::Builder::new()
            .name("graceful-failures-probe".to_string())
            .spawn(move || {
                ready_tx.send(()).unwrap();
                let _ = release_rx.recv();
            })
            .unwrap();
        ready_rx.recv().unwrap();
        let can_read_names = os_threads::named_threads()
            .iter()
            .any(|(_, name)| name == "graceful-failures-probe");
        release_tx.send(()).unwrap();
        probe.join().unwrap();
        assert!(can_read_names, "this system doesn't report thread names");

        let before: HashSet<u32> = os_threads::named_threads()
            .into_iter()
            .filter(|(_, name)| is_thryve_thread(name))
            .map(|(id, _)| id)
            .collect();

        // Arrange: a user observer typed "Logging" collides with the logger. The startup finds
        // it only after the engine thread, the workers and the logger's thread are running.
        let mut ctx = summon_requirements(Config::default());
        let duplicate = Observer::new(
            "Logging",
            Arc::new(QuietObserver),
            ObsJob::new(|_: Arc<Observer>| {}),
            ctx.packet_handle(),
        )
        .unwrap();
        ctx.observers.push(duplicate);
        let packet_handle = ctx.packet_handle();

        // Act (the startup installs Thryve's panic hook; the previous one is put back)
        let previous_hook = std::panic::take_hook();
        let result = EngineInitialization::prepare(ctx, |mut eng: Engine| {
            while !eng.exit_flag {
                eng.run_once();
            }
        });
        std::panic::set_hook(previous_hook);

        // Assert: the error comes back...
        assert!(matches!(
            result,
            Err(ThryveError::Startup(StartupError::RegisterDuplicate {
                name,
                kind: NameInputType::ObsType,
            })) if name == "Logging"
        ));

        // ...the engine is gone...
        assert!(matches!(
            packet_handle.send_notification(Log::log("after".to_string(), "Main")),
            Err(ThryveError::EngineNotRunning(_))
        ));

        // ...and no thread the startup started is still running.
        let left_over = || -> Vec<(u32, String)> {
            os_threads::named_threads()
                .into_iter()
                .filter(|(id, name)| is_thryve_thread(name) && !before.contains(id))
                .collect()
        };
        wait_until(|| left_over().is_empty());
        assert!(
            left_over().is_empty(),
            "Thryve threads still running: {:?}",
            left_over()
        );
    }
}

mod sending {
    use super::*;
    use thryve_utils::errors::{EngineNotRunningError, ThryveError, get_err};
    use thryve_utils::res::TaskResult;

    fn task() -> Task {
        Task::new("Late", 0, || TaskResult::new("", String::new(), "")).unwrap()
    }

    #[test]
    fn sending_after_the_engine_is_gone_returns_engine_not_running() {
        let (task_tx, task_rx) = bounded::<Task>(1);
        let (notif_tx, notif_rx) = bounded::<Notif>(1);
        let handle = PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        drop(task_rx);
        drop(notif_rx);

        let task_error = handle.send_task(task()).err().unwrap();
        assert!(matches!(
            task_error,
            ThryveError::EngineNotRunning(EngineNotRunningError::Task)
        ));
        assert_eq!(
            get_err(&task_error),
            "engine is not running\n  caused by: failed to send task to the engine"
        );

        assert!(matches!(
            handle.send_notification(Log::log("late".to_string(), "Main")),
            Err(ThryveError::EngineNotRunning(EngineNotRunningError::Notif))
        ));

        // Thryve's own status lines are dropped without an error or a panic.
        handle.send_status_log(Log::log("ThreadPool Finalized".to_string(), "Main"));
    }

    mod ffi {
        use std::ffi::CString;
        use thryve_ffi::{
            ThryveStatus, ThryveTaskResult, thryve_config_default, thryve_config_free,
            thryve_ctx_create, thryve_ctx_free, thryve_ctx_get_packet_handle,
            thryve_notification_create, thryve_packet_handle_free,
            thryve_packet_handle_send_notification, thryve_packet_handle_send_task,
            thryve_task_create, thryve_task_result_new,
        };

        extern "C" fn job(result: *mut ThryveTaskResult, _user_data: *mut std::ffi::c_void) {
            unsafe {
                *result =
                    thryve_task_result_new(c"Success".as_ptr(), c"Done".as_ptr(), c"Main".as_ptr())
            };
        }

        #[test]
        fn ffi_sends_after_the_engine_is_gone_return_invalid_state() {
            // Freeing the context drops the receiving end; the packet handle outlives it.
            let config = thryve_config_default();
            let ctx = thryve_ctx_create(config);
            let pkt = thryve_ctx_get_packet_handle(ctx);
            thryve_ctx_free(ctx);

            let name = CString::new("Late").unwrap();
            let task = unsafe {
                thryve_task_create(name.as_ptr(), 0, Some(job), std::ptr::null_mut(), None)
            };
            let task_status = thryve_packet_handle_send_task(pkt, task);

            let notif_type = CString::new("Late").unwrap();
            let receiver = CString::new("Nobody").unwrap();
            let notif = unsafe {
                thryve_notification_create(
                    notif_type.as_ptr(),
                    receiver.as_ptr(),
                    std::ptr::null_mut(),
                )
            };
            let notif_status = thryve_packet_handle_send_notification(pkt, notif);

            // The task and the notification were consumed, so only the handle is freed.
            thryve_packet_handle_free(pkt);
            thryve_config_free(config);

            assert_eq!(task_status, ThryveStatus::InvalidState);
            assert_eq!(notif_status, ThryveStatus::InvalidState);
        }
    }
}

mod engine {
    use super::*;
    use thryve_core::runtime::engine::Engine;
    use thryve_core::runtime::observer_manager::ObserverManager;
    use thryve_core::runtime::plugin_manager::PluginManager;
    use thryve_run::run::run_interactive::summon_requirements;
    use thryve_utils::bases::plugin::Plugin;
    use thryve_utils::errors::{NameInputType, StartupError, ThryveError};
    use thryve_utils::res::TaskResult;
    use thryve_utils::traits::plugin_logic::PlgLogic;

    struct QuietPlugin;
    impl PlgLogic for QuietPlugin {
        impl_as_any!(QuietPlugin);
        fn clean_up(&self) -> Result<(), thryve_utils::errors::ThryveError> {
            Ok(())
        }
    }

    /// Everything the engine has forwarded to the sorter so far.
    fn forwarded(engine: &Engine) -> Vec<Notif> {
        let mut notifs = Vec::new();
        while let Some(notif) = engine.obs_manager.threadpool.main_queue.main_queue.pop() {
            notifs.push(notif);
        }
        notifs
    }

    #[test]
    fn an_empty_result_type_is_reported_not_fatal() {
        let ctx = summon_requirements(Config::default());
        let mut engine = Engine::new(&ctx);

        engine.handle_result((1, TaskResult::new("", "orphan message".to_string(), "Main")));
        engine.handle_result((2, TaskResult::new("", String::new(), "Main")));

        let messages: Vec<String> = forwarded(&engine).iter().filter_map(log_message).collect();
        assert!(messages.contains(
            &"result type cannot be empty if the result message is not empty".to_string()
        ));
        assert!(messages.contains(
            &"result type cannot be empty if the result should log to a file".to_string()
        ));
    }

    #[test]
    fn a_notification_for_an_unknown_observer_is_logged_not_fatal() {
        let ctx = summon_requirements(Config::default());
        let mut manager = ObserverManager::new(&ctx);
        let logger = Observer::new(
            "Logging",
            Arc::new(QuietObserver),
            ObsJob::new(|_: Arc<Observer>| {}),
            ctx.packet_handle(),
        )
        .unwrap();
        manager.load_observer(logger).unwrap();
        manager.init_obs_sorter_thread().unwrap();

        manager.forward_notif(Notif::new("Ping", "Nobody", Box::new(ForeignCore)).unwrap());
        let logger = manager
            .finalize(&mut Vec::new(), &mut Vec::new())
            .expect("the logger was loaded");

        let mut messages = Vec::new();
        while let Some(notif) = logger.obs_queue.main_queue.pop() {
            messages.extend(log_message(&notif));
        }
        assert!(messages.contains(&"Observer not found for notification: Nobody".to_string()));
    }

    #[test]
    fn duplicate_plugin_and_observer_types_are_startup_errors() {
        let ctx = summon_requirements(Config::default());

        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);
        let mut plugins = PluginManager::new(&ctx, res_tx);
        let plugin = || {
            let job = || TaskResult::new("", String::new(), "");
            Plugin::new("Resizer", Arc::new(QuietPlugin), ctx.packet_handle(), job).unwrap()
        };
        plugins.load_plugin(&ctx.config, plugin(), false).unwrap();
        assert!(matches!(
            plugins.load_plugin(&ctx.config, plugin(), false),
            Err(ThryveError::Startup(StartupError::RegisterDuplicate {
                name,
                kind: NameInputType::PlgType,
            })) if name == "Resizer"
        ));

        let mut observers = ObserverManager::new(&ctx);
        let observer = || {
            Observer::new(
                "Audit",
                Arc::new(QuietObserver),
                ObsJob::new(|_: Arc<Observer>| {}),
                ctx.packet_handle(),
            )
            .unwrap()
        };
        observers.load_observer(observer()).unwrap();
        assert!(matches!(
            observers.load_observer(observer()),
            Err(ThryveError::Startup(StartupError::RegisterDuplicate {
                name,
                kind: NameInputType::ObsType,
            })) if name == "Audit"
        ));
        let _ = observers.finalize(&mut Vec::new(), &mut Vec::new());
    }
}

mod logger {
    use super::*;
    use thryve_observers::logger::LoggerObserver;
    use thryve_utils::errors::StartupError;
    use thryve_utils::extras::log_format::write_line;

    #[test]
    fn the_logger_needs_error_log_and_says_so() {
        assert!(matches!(
            LoggerObserver::new(&bare_ctx()),
            Err(StartupError::LogFileFetch)
        ));
    }

    #[test]
    fn a_log_line_that_cant_be_written_goes_to_stderr_not_a_panic() {
        let dir = fresh_dir("read_only_log");
        let path = dir.join("ReadOnly.log");
        std::fs::write(&path, "").unwrap();
        let read_only = File::open(&path).unwrap();

        // Directly, and through the logger's own write.
        write_line(&read_only, "direct line\n");

        let notif = Log::log("logger line".to_string(), "Main");
        let log = notif.notif_core.as_any().downcast_ref::<Log>().unwrap();
        log.display(SystemTime::now(), "Log", None, Arc::new(read_only));

        // Nothing reached the file, and the process is still running.
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "");
    }
}

mod topology {
    use super::*;
    use std::sync::PoisonError;
    use thryve_observers::topology_handler::TopologyHandlerObserver;
    use thryve_utils::errors::StartupError;
    use thryve_utils::extras::dir_paths::get_workspace_root;
    use thryve_utils::graphs::callsite::TaskCallSite;
    use thryve_utils::graphs::graph::{TaskTopology, TopologyNode};
    use thryve_utils::packets::notif_topology::TopologyModifier;

    fn node(fingerprint: &str) -> TopologyNode {
        TopologyNode::new(TaskCallSite::default(), fingerprint.to_string())
    }

    fn handler(observer: &Observer) -> &TopologyHandlerObserver {
        observer
            .obs_core
            .as_any()
            .downcast_ref::<TopologyHandlerObserver>()
            .unwrap()
    }

    fn error_log(dir: &Path) -> String {
        std::fs::read_to_string(dir.join("Error.log")).unwrap()
    }

    #[test]
    fn the_topology_handler_needs_its_folder_and_says_so() {
        assert!(matches!(
            TopologyHandlerObserver::new(&bare_ctx()),
            Err(StartupError::DirFetch)
        ));
    }

    #[test]
    fn a_foreign_notification_is_dropped_with_a_note() {
        let dir = fresh_dir("foreign_notification");
        let ctx = ctx_with_files(&dir, true);
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();
        let handler = handler(&observer);

        handler.handle_entry(Notif::new("Ping", "TopologyHandler", Box::new(ForeignCore)).unwrap());

        let topology = handler.topology.read().unwrap();
        assert_eq!(topology.graph.node_count(), 0);
        assert!(topology.enabled);
        assert!(ctx.packet_handle().graph_mode_on());
        assert!(error_log(&dir).contains(
            "[ TOPOLOGY ], Event_Type: Error, Message: [dropped a notification of type \"Ping\""
        ));
    }

    #[test]
    fn a_poisoned_topology_lock_turns_graph_mode_off() {
        let dir = fresh_dir("poisoned_lock");
        let ctx = ctx_with_files(&dir, true);
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();

        // A thread panics while holding the lock.
        let core = observer.obs_core.clone();
        with_silent_panics(|| {
            let _ = std::thread::spawn(move || {
                let handler = core
                    .as_any()
                    .downcast_ref::<TopologyHandlerObserver>()
                    .unwrap();
                let _guard = handler.topology.write().unwrap();
                panic!("poison the topology lock");
            })
            .join();
        });
        let handler = handler(&observer);
        assert!(handler.topology.is_poisoned());

        handler.handle_entry(TopologyModifier::add_node(node("after")));

        let topology = handler
            .topology
            .read()
            .unwrap_or_else(PoisonError::into_inner);
        assert_eq!(
            topology
                .get_node(TopologyNode::FAILURE_FINGERPRINT)
                .failure
                .as_deref(),
            Some("the topology lock was poisoned")
        );
        assert!(!topology.contains_node("after"));
        assert!(!ctx.packet_handle().graph_mode_on());
        assert!(error_log(&dir).contains("graph mode turned off: the topology lock was poisoned"));
    }

    #[test]
    fn a_failed_export_is_reported_and_the_shutdown_goes_on() {
        let dir = fresh_dir("failed_export");
        let mut ctx = ctx_with_files(&dir, true);
        // A folder that doesn't exist, so writing the export fails.
        ctx.meta_data
            .dir_data
            .dir_paths
            .insert(DirName::Topology, dir.join("missing").join("topology"));
        let observer = Arc::new(TopologyHandlerObserver::new(&ctx).unwrap());
        let handler = handler(&observer);
        handler.handle_entry(TopologyModifier::add_node(node("n")));

        handler.clean_up(observer.clone());

        assert!(error_log(&dir).contains("could not write the topology graph to"));
    }

    #[test]
    fn without_a_workspace_root_the_graph_keeps_full_paths_and_warns_once() {
        let dir = fresh_dir("no_workspace_root");
        let ctx = ctx_with_files(&dir, true);
        let observer = TopologyHandlerObserver::new(&ctx).unwrap();
        let handler = handler(&observer);

        let full_path = PathBuf::from("E:\\Projects\\Thryve\\src\\main.rs");
        let with_path = |fingerprint: &str| {
            TopologyNode::new(
                TaskCallSite::new(full_path.clone(), 1, "Task".to_string()),
                fingerprint.to_string(),
            )
        };

        // Point cargo somewhere that doesn't exist, as in a deployed program.
        let saved = std::env::var("CARGO").ok();
        // SAFETY: the tests run on one thread, so nothing reads the environment meanwhile.
        unsafe { std::env::set_var("CARGO", "thryve-tests-no-such-cargo.exe") };
        let root = get_workspace_root();
        handler.handle_entry(TopologyModifier::add_node(with_path("one")));
        handler.handle_entry(TopologyModifier::add_node(with_path("two")));
        match saved {
            // SAFETY: as above.
            Some(value) => unsafe { std::env::set_var("CARGO", value) },
            None => unsafe { std::env::remove_var("CARGO") },
        }

        assert!(root.is_none());
        let topology = handler.topology.read().unwrap();
        assert_eq!(topology.get_node("one").callsite.file_path, full_path);
        assert_eq!(topology.get_node("two").callsite.file_path, full_path);
        assert_eq!(
            error_log(&dir)
                .matches("could not find the workspace root")
                .count(),
            1
        );
    }

    #[test]
    fn a_stale_index_reads_as_a_missing_node() {
        let mut topology = TaskTopology::default();
        let index = topology.graph.add_node(node("gone"));
        topology
            .graph_node_indices
            .insert("gone".to_string(), index);
        topology.graph.remove_node(index);

        assert!(topology.node_index("gone").is_none());
        assert_eq!(topology.get_node("gone"), TopologyNode::default());
    }
}

mod shutdown {
    use super::*;
    use thryve_utils::queues::priority_queue::PQChild;
    use thryve_utils::res::TaskResult;
    use thryve_utils::threads::threadpool::ThreadPool;

    #[test]
    fn a_worker_that_panicked_does_not_stop_the_shutdown() {
        let config = Config::default();
        let (task_tx, _task_rx) = bounded::<Task>(10);
        let (notif_tx, _notif_rx) = bounded::<Notif>(10);
        let packet_handle =
            PacketHandle::new(task_tx, notif_tx, crossbeam::channel::unbounded().0, false);
        let queue = PQChild::new(&config, packet_handle.clone());
        let mut pool: ThreadPool<PQChild<Task>, Task> = ThreadPool::new(2, queue, packet_handle);
        let (res_tx, _res_rx) = bounded::<(u64, TaskResult)>(10);

        let finished = with_silent_panics(|| {
            pool.run_threads_plugins(Arc::new(res_tx), Arc::new(SkipSet::new()))
                .unwrap();
            pool.submit_task(Task::new("Boom", 0, || panic!("the task panics")).unwrap());

            // One worker dies with the task's panic...
            let died = wait_until(|| pool.workers.iter().any(|(worker, _)| worker.is_finished()));

            // ...and stopping the pool still joins both, without panicking on the dead one.
            pool.finalize_plg(&mut Vec::new(), &mut Vec::new());
            pool.stop_workers(&mut Vec::new());
            died
        });

        assert!(finished, "the panicking task never ran");
        assert!(pool.workers.is_empty());
    }
}

mod protections {
    use super::*;
    use thryve_utils::queues::priority_queue::Priority;

    #[test]
    fn aging_stops_at_the_top_instead_of_overflowing() {
        let mut tasks = Config::default().tasks;
        tasks.max_priority = 4;
        tasks.aging_interval = 3;
        let waited_long = Instant::now()
            .checked_sub(Duration::from_secs(800))
            .expect("the machine has been up for more than 800 seconds");

        // 800 s / 3 s saturates the boost at 255, and 1 + 255 used to overflow a u8.
        let priority = Priority {
            task_config: Arc::new(tasks),
            base_priority: 1,
            enq_at: waited_long,
            task_id: 0,
        };

        assert_eq!(priority.effective_priority(), 4);
    }
}
