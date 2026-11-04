// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Shutdown and startup: neither one ever hangs.
//!
//! The engine is the only reader of its mailboxes, so it must never wait on a thread that may be
//! waiting for the engine to read. These tests shut down real engines (a real logger, the
//! notification sorter and plugin workers) in the situations that could hang:
//!
//! - draining shutdown: a `Fin` while another task is in the middle of a burst of `send_task`
//!   through a mailbox that holds one task, and a worker blocked on a full result mailbox;
//! - the plugins' cleanup: it runs on a thread of its own, so a `clean_up` can send more than the
//!   mailbox holds; a failed one is logged and the others still run. The observers' cleanup runs
//!   on a thread of its own too, for the same reason;
//! - the grace period: a task that never returns doesn't hold up the shutdown, the log names it,
//!   and no plugin is cleaned up while one of its tasks may still be running. An observer's
//!   cleanup that never returns doesn't hold it up either, and after a stuck task has used up the
//!   grace period, the observers still get a second to clean up. A grace period below one second
//!   is rejected;
//! - observers last: the topology handler's exit line, sent as its thread stops, reaches
//!   `Main.log`, and "Engine Finalized" is the last line, also when a startup failed after the
//!   observers were loaded. A notification that isn't a log, sent once the sorter has stopped,
//!   isn't handed to the logger;
//! - lost work: one line counts the tasks that never ran, the results that carried something,
//!   the notifications never delivered and the threads left running. It goes to `Error.log`, or
//!   to `Main.log` after the service's Stop, and `Debug.log` gets the details when debugging is
//!   on. A shutdown that lost nothing writes no line;
//! - finalize runs once: a second call returns at once and writes nothing;
//! - the real startup: with every mailbox holding one item, a startup with several plugins, pauses
//!   and resumes, and a `Fin` never hang; no plugin code runs before the engine starts; the
//!   service's loop ends after an in-band `Fin`, and on `Stop`;
//! - the workers: `resume` wakes the workers a pause parked, a task chained after the workers went
//!   idle still runs, and a shutdown during a pause doesn't wait for them (a failed startup is one
//!   too, since the engine stays paused until it starts; `graceful_failures` checks that it leaves no
//!   thread behind);
//! - status lines never wait, even when the notification mailbox is full;
//! - graph mode: a plugin's init task has a node of its own.
//!
//! Each shutdown runs on a thread of its own with a time limit, so a regression fails its test
//! instead of hanging the whole run. CI runs these tests on one thread (`--test-threads=1`), like
//! the rest of the crate.

use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use chrono::{DateTime, Local};
use crossbeam::channel::{bounded, unbounded};

use thryve_core::runtime::engine::Engine;
use thryve_observers::logger::LoggerObserver;
use thryve_observers::topology_handler::TopologyHandlerObserver;
use thryve_run::init::init::EngineInitialization;
use thryve_run::run::run_interactive::summon_requirements;
use thryve_utils::bases::notif::Notif;
use thryve_utils::bases::observer::Observer;
use thryve_utils::bases::plugin::Plugin;
use thryve_utils::cfg::config::Config;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::cfg::dir_data::DirData;
use thryve_utils::cfg::meta_data::MetaData;
use thryve_utils::errors::{FinalizationError, ThryveError, get_err};
use thryve_utils::extras::dir_paths::DirName;
use thryve_utils::extras::jobs::ObsJob;
use thryve_utils::extras::named_input::LogFileNamedInput;
use thryve_utils::impl_as_any;
use thryve_utils::packets::notif_log::Log;
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::packets::task::Task;
use thryve_utils::res::TaskResult;
use thryve_utils::traits::notif_logic::NotifLogic;
use thryve_utils::traits::observer_logic::ObsLogic;
use thryve_utils::traits::plugin_logic::PlgLogic;
use thryve_utils::traits::queue::Queue;

/// A fresh, empty folder for one test's files.
pub(crate) fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("thryve_engine_tests").join(name);
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A started engine, as after startup, with its `Main.log`, `Error.log` and `Debug.log` in `dir`.
///
/// It has a real logger and the notification sorter, `workers` plugin workers, mailboxes (tasks,
/// notifications and results) that each hold `capacity` items, and a grace period of
/// `grace_period` seconds. Also returns a packet handle connected to it.
pub(crate) fn started_engine(
    dir: &Path,
    capacity: usize,
    workers: usize,
    grace_period: usize,
) -> (Engine, PacketHandle) {
    started_engine_with(dir, capacity, workers, grace_period, |_, _| {})
}

/// Like [`started_engine`], but `load` can load more observers before the sorter starts. The
/// context has a topology folder, `dir/topology`, for the topology handler.
fn started_engine_with(
    dir: &Path,
    capacity: usize,
    workers: usize,
    grace_period: usize,
    load: impl FnOnce(&Ctx, &mut Engine),
) -> (Engine, PacketHandle) {
    let (mut engine, packet_handle) = loaded_engine(dir, capacity, workers, grace_period, load);
    engine.obs_manager.init_obs_sorter_thread().unwrap();
    let res_tx = engine.plugin_manager.res_tx.clone();
    let cancelled_tasks = engine.cancelled_tasks.clone();
    engine
        .plugin_manager
        .threadpool
        .run_threads_plugins(res_tx, cancelled_tasks)
        .unwrap();
    engine.resume(true);

    (engine, packet_handle)
}

/// An engine as startup leaves it once the observers are loaded: the real logger and whatever
/// `load` adds, with their threads running, but no sorter and no plugin workers yet. A startup
/// that fails after this point (a plugin that doesn't load, say) finalizes such an engine. The
/// context has a topology folder, `dir/topology`, for the topology handler.
pub(crate) fn loaded_engine(
    dir: &Path,
    capacity: usize,
    workers: usize,
    grace_period: usize,
    load: impl FnOnce(&Ctx, &mut Engine),
) -> (Engine, PacketHandle) {
    let (task_tx, task_rx) = bounded::<Task>(capacity);
    let (notif_tx, notif_rx) = bounded::<Notif>(capacity);
    let (status_tx, status_rx) = unbounded();
    let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);

    let mut log_files: HashMap<LogFileNamedInput, Arc<File>> = HashMap::new();
    log_files.insert(
        LogFileNamedInput::new_main(),
        Arc::new(File::create(dir.join("Main.log")).unwrap()),
    );
    log_files.insert(
        LogFileNamedInput::new_err(),
        Arc::new(File::create(dir.join("Error.log")).unwrap()),
    );
    log_files.insert(
        LogFileNamedInput::new_debug(),
        Arc::new(File::create(dir.join("Debug.log")).unwrap()),
    );

    let mut config = Config::default();
    config.threads.worker_count = workers;
    config.pipes.res_pipe_capacity = capacity;
    config.engine.shutdown_grace_period = grace_period;

    let mut ctx = Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle.clone());
    let datetime: DateTime<Local> = SystemTime::now().into();
    ctx.meta_data = MetaData::new(datetime, DirData::new(dir.to_path_buf(), log_files));
    let topology_dir = dir.join("topology");
    std::fs::create_dir_all(&topology_dir).unwrap();
    ctx.meta_data
        .dir_data
        .dir_paths
        .insert(DirName::Topology, topology_dir);

    let mut engine = Engine::new(&ctx);
    engine
        .obs_manager
        .load_observer(LoggerObserver::new(&ctx).unwrap())
        .unwrap();
    load(&ctx, &mut engine);

    (engine, packet_handle)
}

/// Runs `finalize` on a thread of its own, as a `Fin` or a failed startup does (not the service's
/// Stop), and waits up to `limit` for it.
///
/// Returns how long the shutdown took, or `None` if it didn't finish in time; its thread is then
/// left behind, so the test fails instead of hanging the run.
pub(crate) fn finalize_within(engine: Engine, limit: Duration) -> Option<Duration> {
    finalize_within_as(engine, limit, false)
}

/// Like [`finalize_within`], but `is_service_normal_shutdown` says whether the service's Stop is
/// shutting the engine down.
fn finalize_within_as(
    mut engine: Engine,
    limit: Duration,
    is_service_normal_shutdown: bool,
) -> Option<Duration> {
    let (done_tx, done_rx) = bounded(1);
    let start = Instant::now();
    std::thread::spawn(move || {
        engine.finalize(is_service_normal_shutdown);
        let _ = done_tx.send(());
    });
    done_rx.recv_timeout(limit).ok().map(|_| start.elapsed())
}

/// Runs the engine's loop on a thread of its own until a task's `Fin` shuts it down, and waits up
/// to `limit` for that. Returns whether it shut down in time.
fn run_until_shut_down(mut engine: Engine, limit: Duration) -> bool {
    let (done_tx, done_rx) = bounded(1);
    std::thread::spawn(move || {
        while !engine.exit_flag {
            engine.run_once();
        }
        let _ = done_tx.send(());
    });
    done_rx.recv_timeout(limit).is_ok()
}

/// Starts an engine through the real startup, the way interactive mode does (`prepare`, then
/// `complete_and_run`), with `run` as the run mode's loop. All of it runs on a thread of its own,
/// and this waits up to `limit` for the engine thread to end. Returns why it failed, if it did.
///
/// Its logs go where a real run's do, next to the test executable. The startup installs Thryve's
/// panic hook, which ends the process on any panic; the previous hook is put back as soon as the
/// setup is done, so that a failing assertion still fails only its own test.
pub(crate) fn run_real_engine(
    ctx: Ctx,
    run: impl FnOnce(Engine) + Send + 'static,
    limit: Duration,
) -> Result<(), String> {
    let (done_tx, done_rx) = bounded(1);
    std::thread::spawn(move || {
        let previous_hook = std::panic::take_hook();
        let prepared = EngineInitialization::prepare(ctx, run);
        std::panic::set_hook(previous_hook);

        let outcome = match prepared.and_then(|prepared| prepared.complete_and_run()) {
            Ok(handle) => handle
                .join()
                .map_err(|_| "the engine thread panicked".to_string()),
            Err(error) => Err(format!("the startup failed: {}", get_err(&error))),
        };
        let _ = done_tx.send(outcome);
    });

    done_rx
        .recv_timeout(limit)
        .unwrap_or_else(|_| Err(format!("it didn't end within {limit:?}")))
}

/// The text of one of the engine's log files.
pub(crate) fn read_log(dir: &Path, file: &str) -> String {
    std::fs::read_to_string(dir.join(file)).unwrap()
}

/// The summary line a shutdown writes for these counts of lost work.
pub(crate) fn summary_line(
    tasks: usize,
    notifs: usize,
    results: usize,
    still_running: usize,
) -> String {
    let total = tasks + notifs + results + still_running;
    format!(
        "thryve exited leaving {total} things not done: {tasks} lost tasks, {notifs} lost \
         notifications, {results} lost task results, {still_running} still running"
    )
}

/// A task with nothing to do.
pub(crate) fn idle_task(name: &str) -> Task {
    Task::new(name, 0, || TaskResult::new("", String::new(), "")).unwrap()
}

/// A task that tells `ran` it has run, then returns a result of type `res_type` with `message`
/// (both empty: a result with nothing to report).
fn reporting_task(
    name: &str,
    res_type: &str,
    message: &str,
    ran: crossbeam::channel::Sender<()>,
) -> Task {
    let (res_type, message) = (res_type.to_string(), message.to_string());
    let job = move || {
        let _ = ran.send(());
        TaskResult::new(&res_type, message, "Main")
    };
    Task::new(name, 0, job).unwrap()
}

/// A plugin whose `clean_up` records that it ran, and fails if told to.
struct RecordingPlugin {
    plugin_type: &'static str,
    cleaned_up: Arc<AtomicBool>,
    fails: bool,
}

impl PlgLogic for RecordingPlugin {
    impl_as_any!(RecordingPlugin);

    fn clean_up(&self) -> Result<(), ThryveError> {
        self.cleaned_up.store(true, Ordering::SeqCst);

        if self.fails {
            return Err(FinalizationError::PluginCleanup {
                plugin_type: self.plugin_type.to_string(),
            }
            .into());
        }

        Ok(())
    }
}

/// A plugin with nothing to clean up.
pub(crate) struct QuietPlugin;

impl PlgLogic for QuietPlugin {
    impl_as_any!(QuietPlugin);

    fn clean_up(&self) -> Result<(), ThryveError> {
        Ok(())
    }
}

/// A plugin whose `clean_up` sends three notifications.
struct ChattyPlugin {
    plugin_type: &'static str,
    packet_handle: PacketHandle,
}

impl PlgLogic for ChattyPlugin {
    impl_as_any!(ChattyPlugin);

    fn clean_up(&self) -> Result<(), ThryveError> {
        for i in 0..3 {
            let goodbye = format!("{} says goodbye {i}", self.plugin_type);
            self.packet_handle
                .send_notification(Log::log(goodbye, "Main"))?;
        }

        Ok(())
    }
}

/// An observer whose `clean_up` sends three notifications.
struct ChattyObserver;

impl ObsLogic for ChattyObserver {
    impl_as_any!(ChattyObserver);

    fn clean_up(&self, observer: Arc<Observer>) {
        for i in 0..3 {
            let goodbye = format!("{} says goodbye {i}", observer.obs_type());
            let _ = observer.tx.send_notification(Log::log(goodbye, "Main"));
        }
    }
}

/// A notification core that no built-in observer understands.
struct ForeignCore;

impl NotifLogic for ForeignCore {
    impl_as_any!(ForeignCore);
}

/// An observer whose `clean_up` sends a notification that isn't a log, to an observer named
/// "Metrics".
struct MetricsSender;

impl ObsLogic for MetricsSender {
    impl_as_any!(MetricsSender);

    fn clean_up(&self, observer: Arc<Observer>) {
        let notif = Notif::new("Measurement", "Metrics", Box::new(ForeignCore)).unwrap();
        let _ = observer.tx.send_notification(notif);
    }
}

/// An observer whose `clean_up` doesn't return until the test lets it go.
struct StuckObserver {
    release: crossbeam::channel::Receiver<()>,
}

impl ObsLogic for StuckObserver {
    impl_as_any!(StuckObserver);

    fn clean_up(&self, _observer: Arc<Observer>) {
        let _ = self.release.recv_timeout(Duration::from_secs(30));
    }
}

/// An observer whose `clean_up` records that it ran.
struct RecordingObserver {
    cleaned_up: Arc<AtomicBool>,
}

impl ObsLogic for RecordingObserver {
    impl_as_any!(RecordingObserver);

    fn clean_up(&self, _observer: Arc<Observer>) {
        self.cleaned_up.store(true, Ordering::SeqCst);
    }
}

/// A task that runs until the test lets it go. Returns the task, a receiver that hears when it
/// has started, and the sender that lets it go.
fn stuck_task(
    name: &str,
) -> (
    Task,
    crossbeam::channel::Receiver<()>,
    crossbeam::channel::Sender<()>,
) {
    let (started_tx, started_rx) = bounded(1);
    let (release_tx, release_rx) = bounded::<()>(1);
    let job = move || {
        let _ = started_tx.send(());
        let _ = release_rx.recv_timeout(Duration::from_secs(30));
        TaskResult::new("", String::new(), "")
    };
    (Task::new(name, 0, job).unwrap(), started_rx, release_tx)
}

/// An observer with `core`, whose thread handles nothing and stops once its queue is finalized.
fn observer(obs_type: &str, core: Arc<dyn ObsLogic>, packet_handle: &PacketHandle) -> Observer {
    let job = ObsJob::new(|observer: Arc<Observer>| while observer.obs_queue.pop().is_some() {});
    Observer::new(obs_type, core, job, packet_handle.clone()).unwrap()
}

/// Loads a plugin with `core` into `engine`. Its init task isn't needed, so it never runs.
fn load_plugin(
    engine: &mut Engine,
    packet_handle: &PacketHandle,
    plugin_type: &str,
    core: Arc<dyn PlgLogic>,
) {
    let job = || TaskResult::new("", String::new(), "");
    let plugin = Plugin::new(plugin_type, core, packet_handle.clone(), job).unwrap();
    let config = Config::default();
    let _init_task = engine
        .plugin_manager
        .load_plugin(&config, plugin, false)
        .unwrap();
}

/// A loaded plugin whose `clean_up` records that it ran. Returns the record.
fn recording_plugin(
    engine: &mut Engine,
    packet_handle: &PacketHandle,
    plugin_type: &'static str,
    fails: bool,
) -> Arc<AtomicBool> {
    let cleaned_up = Arc::new(AtomicBool::new(false));
    let core = Arc::new(RecordingPlugin {
        plugin_type,
        cleaned_up: cleaned_up.clone(),
        fails,
    });
    load_plugin(engine, packet_handle, plugin_type, core);
    cleaned_up
}

mod draining {
    use super::*;

    #[test]
    fn a_fin_during_a_send_task_burst_through_a_one_task_mailbox_shuts_down() {
        let dir = fresh_dir("burst");
        let (mut engine, packet_handle) = started_engine(&dir, 1, 2, 10);

        // "Burst" sends 200 tasks through a mailbox that holds one, so each send waits for the
        // engine to read. "Finisher" returns `Fin` once the burst has started.
        let (burst_started_tx, burst_started_rx) = bounded(1);
        let burst_job = move || {
            for i in 0..200 {
                if packet_handle.send_task(idle_task("Child")).is_err() {
                    break;
                }
                if i == 0 {
                    let _ = burst_started_tx.send(());
                }
            }
            TaskResult::new("", String::new(), "")
        };
        let finisher_job = move || {
            let _ = burst_started_rx.recv_timeout(Duration::from_secs(5));
            TaskResult::fin()
        };
        engine.accept_task(Task::new("Burst", 0, burst_job).unwrap());
        engine.accept_task(Task::new("Finisher", 0, finisher_job).unwrap());

        assert!(
            run_until_shut_down(engine, Duration::from_secs(10)),
            "the shutdown hung"
        );

        // Each worker's last status line was read and written, after the drain.
        let main = read_log(&dir, "Main.log");
        assert_eq!(
            main.matches("Plugin Thread Exiting...").count(),
            2,
            "{main}"
        );
        assert!(main.contains("Engine Finalized"), "{main}");
    }

    #[test]
    fn a_worker_blocked_on_a_full_result_mailbox_shuts_down() {
        let dir = fresh_dir("full_results");
        let (mut engine, _packet_handle) = started_engine(&dir, 1, 2, 10);

        // Nothing reads the results before the shutdown, and the mailbox holds one: the second
        // worker's result has to wait for the engine.
        let (ran_tx, ran_rx) = bounded(2);
        for name in ["First", "Second"] {
            let ran_tx = ran_tx.clone();
            let job = move || {
                let _ = ran_tx.send(());
                TaskResult::new("Ok", format!("{name} done"), "Main")
            };
            engine.accept_task(Task::new(name, 0, job).unwrap());
        }
        for _ in 0..2 {
            ran_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("a task never ran");
        }

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }
}

mod cleanup {
    use super::*;

    #[test]
    fn clean_ups_can_send_more_than_the_mailbox_holds() {
        let dir = fresh_dir("chatty_clean_ups");
        let (mut engine, packet_handle) = started_engine(&dir, 1, 1, 10);
        for plugin_type in ["ChattyA", "ChattyB"] {
            let core = Arc::new(ChattyPlugin {
                plugin_type,
                packet_handle: packet_handle.clone(),
            });
            load_plugin(&mut engine, &packet_handle, plugin_type, core);
        }

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Every notification reached the logger: the engine kept reading while the cleanups ran,
        // and the sorter delivered them before it stopped.
        let main = read_log(&dir, "Main.log");
        for plugin_type in ["ChattyA", "ChattyB"] {
            for i in 0..3 {
                let goodbye = format!("{plugin_type} says goodbye {i}");
                assert!(main.contains(&goodbye), "missing {goodbye:?} in:\n{main}");
            }
        }
    }

    #[test]
    fn an_observer_clean_up_can_send_more_than_the_mailbox_holds() {
        let dir = fresh_dir("chatty_observer");
        let (mut engine, packet_handle) = started_engine(&dir, 1, 1, 10);
        // Loaded after the sorter started, so nothing is routed to it: only its cleanup matters.
        engine
            .obs_manager
            .load_observer(observer(
                "ChattyObserver",
                Arc::new(ChattyObserver),
                &packet_handle,
            ))
            .unwrap();

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // The engine kept reading while the cleanup ran, and with the sorter stopped, it handed
        // the notifications straight to the logger.
        let main = read_log(&dir, "Main.log");
        for i in 0..3 {
            let goodbye = format!("ChattyObserver says goodbye {i}");
            assert!(main.contains(&goodbye), "missing {goodbye:?} in:\n{main}");
        }
        assert!(
            main.contains("observer ChattyObserver finalized successfully"),
            "{main}"
        );
    }

    #[test]
    fn a_failed_clean_up_is_logged_and_the_other_plugins_are_still_cleaned_up() {
        let dir = fresh_dir("failed_clean_up");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        let failing = recording_plugin(&mut engine, &packet_handle, "Failing", true);
        let working = recording_plugin(&mut engine, &packet_handle, "Working", false);

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        assert!(failing.load(Ordering::SeqCst));
        assert!(working.load(Ordering::SeqCst));
        let errors = read_log(&dir, "Error.log");
        assert!(
            errors.contains("plugin Failing failed to clean up successfully"),
            "{errors}"
        );
    }
}

mod grace_period {
    use super::*;
    use thryve_utils::errors::{ConfigError, RuleError, get_err};

    #[test]
    fn a_task_that_outlives_the_grace_period_is_named_and_does_not_hold_up_the_shutdown() {
        let dir = fresh_dir("grace_period");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let cleaned_up = recording_plugin(&mut engine, &packet_handle, "Watched", false);

        // "Sleeper" is stuck until the test lets it go, long after the 1 s grace period.
        let (task, started_rx, release_tx) = stuck_task("Sleeper");
        engine.accept_task(task);
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the task never started");

        let took = finalize_within(engine, Duration::from_secs(10));
        // The abandoned worker can finish now.
        let _ = release_tx.send(());

        let took = took.expect("the shutdown waited for the stuck task");
        assert!(
            took >= Duration::from_millis(900),
            "it gave up early: {took:?}"
        );
        assert!(
            took < Duration::from_secs(5),
            "it waited too long: {took:?}"
        );

        let errors = read_log(&dir, "Error.log");
        assert!(
            errors.contains("thread 'plugin-worker-0' was still running 'Sleeper'"),
            "{errors}"
        );

        // Its plugin may still be in use, so no plugin was cleaned up, and the log says so.
        assert!(!cleaned_up.load(Ordering::SeqCst));
        assert!(errors.contains("cannot cleanup plugins"), "{errors}");

        // The summary counts the task as still running.
        assert!(errors.contains(&summary_line(0, 0, 0, 1)), "{errors}");
    }

    #[test]
    fn an_observer_clean_up_that_never_returns_does_not_hold_up_the_shutdown() {
        let dir = fresh_dir("stuck_observer");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (release_tx, release_rx) = bounded::<()>(1);
        engine
            .obs_manager
            .load_observer(observer(
                "StuckObserver",
                Arc::new(StuckObserver {
                    release: release_rx,
                }),
                &packet_handle,
            ))
            .unwrap();

        let took = finalize_within(engine, Duration::from_secs(10));
        // The abandoned cleanup can finish now, which also stops the observer's thread.
        let _ = release_tx.send(());

        // The observers' deadline is the 1 s grace period plus one second.
        let took = took.expect("the shutdown waited for the stuck cleanup");
        assert!(
            took >= Duration::from_millis(1900),
            "it gave up early: {took:?}"
        );
        assert!(
            took < Duration::from_secs(5),
            "it waited too long: {took:?}"
        );

        // Neither the clean-up thread nor the observer's own thread, which only the cleanup
        // would have stopped, held it up; the log names both.
        let errors = read_log(&dir, "Error.log");
        assert!(
            errors.contains("thread 'observer cleanup' was still running 'StuckObserver'"),
            "{errors}"
        );
        assert!(
            errors.contains("thread 'StuckObserver' hadn't stopped"),
            "{errors}"
        );

        // The summary counts both threads as still running.
        assert!(errors.contains(&summary_line(0, 0, 0, 2)), "{errors}");
    }

    #[test]
    fn the_observers_are_still_cleaned_up_after_a_stuck_task_used_up_the_grace_period() {
        let dir = fresh_dir("observers_after_stuck_task");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let cleaned_up = Arc::new(AtomicBool::new(false));
        engine
            .obs_manager
            .load_observer(observer(
                "Recorder",
                Arc::new(RecordingObserver {
                    cleaned_up: cleaned_up.clone(),
                }),
                &packet_handle,
            ))
            .unwrap();

        let (task, started_rx, release_tx) = stuck_task("Sleeper");
        engine.accept_task(task);
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the task never started");

        let took = finalize_within(engine, Duration::from_secs(10));
        let _ = release_tx.send(());

        // The task used up the whole grace period, but the observers got one more second.
        let took = took.expect("the shutdown waited for the stuck task");
        assert!(
            took < Duration::from_secs(5),
            "it waited too long: {took:?}"
        );
        assert!(cleaned_up.load(Ordering::SeqCst));
    }

    #[test]
    fn a_grace_period_below_one_second_is_rejected() {
        let dir = fresh_dir("config_grace_period");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            "[service]\ncan_stop = true\ncan_pause = true\n\n\
             [engine]\nidle_wait = 100\nshutdown_grace_period = 0\n\n\
             [threads]\nworker_count = 4\n\n\
             [pipes]\ntask_pipe_capacity = 1024\nnotif_pipe_capacity = 1024\n\
             res_pipe_capacity = 1024\ntasks_processed_per_turn = 500\n\
             notifs_processed_per_turn = 500\nres_processed_per_turn = 500\n\n\
             [tasks]\nmax_priority = 4\naging_interval = 3\n\n\
             [debugging]\nis_debugging = false\nwait_before_execution = 10\nvisual_graph = false\n\
             calculate_metrics = false\nlog_tasks = false\n",
        )
        .unwrap();

        let error = Config::load_config_from_file(&path).err().unwrap();

        let ThryveError::Config(ConfigError::ConfigViolation { violation, .. }) = &error else {
            panic!("expected a ConfigViolation, got: {}", get_err(&error));
        };
        assert!(
            matches!(violation.types[..], [RuleError::ShutdownGracePeriod(0, 1)]),
            "{}",
            get_err(&error)
        );
        assert!(get_err(&error).contains("engine.shutdown_grace_period is 0, must be at least 1"));
    }
}

mod observers_last {
    use super::*;

    #[test]
    fn the_topology_handler_s_exit_line_reaches_main_log() {
        let dir = fresh_dir("topology_exit_line");
        let (engine, _packet_handle) = started_engine_with(&dir, 10, 1, 10, |ctx, engine| {
            engine
                .obs_manager
                .load_observer(TopologyHandlerObserver::new(ctx).unwrap())
                .unwrap();
        });

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Its thread sends the line as it stops. The engine waited for that thread, then read
        // its mailboxes once more.
        let main = read_log(&dir, "Main.log");
        assert!(
            main.contains("Topology Handler Obs Thread Exited..."),
            "{main}"
        );
    }

    #[test]
    fn a_notification_that_is_not_a_log_is_not_handed_to_the_logger_once_the_sorter_stopped() {
        let dir = fresh_dir("late_notification");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        engine
            .obs_manager
            .load_observer(observer(
                "MetricsSender",
                Arc::new(MetricsSender),
                &packet_handle,
            ))
            .unwrap();

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // It's sent from a cleanup, after the sorter has stopped. Its observer can't get it any
        // more, and the logger can't show it, so it's kept as undelivered instead of being handed
        // to the logger, which would note that it dropped it. The summary counts it.
        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains("dropped a notification"), "{errors}");
        assert!(errors.contains(&summary_line(0, 1, 0, 0)), "{errors}");
    }

    #[test]
    fn engine_finalized_is_the_last_line() {
        // A normal shutdown, and a startup that failed after the observers were loaded (no
        // sorter), which used to end on a line of the logger's own thread about once in twenty
        // runs.
        for run in 0..20 {
            for aborted in [false, true] {
                let dir = fresh_dir(&format!("last_line_{run}_{aborted}"));
                let engine = if aborted {
                    loaded_engine(&dir, 10, 1, 10, |_, _| {}).0
                } else {
                    started_engine(&dir, 10, 1, 10).0
                };

                assert!(
                    finalize_within(engine, Duration::from_secs(10)).is_some(),
                    "the shutdown hung (run {run}, aborted: {aborted})"
                );

                let main = read_log(&dir, "Main.log");
                let last = main.lines().rfind(|line| !line.trim().is_empty());
                assert!(
                    last.is_some_and(|line| line.contains("Engine Finalized")),
                    "run {run} (aborted: {aborted}) ended on {last:?}:\n{main}"
                );
            }
        }
    }
}

mod lost_work {
    use super::*;

    #[test]
    fn one_line_in_error_log_counts_each_kind_of_lost_work() {
        let dir = fresh_dir("lost_work_summary");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        // Its cleanup sends a notification that can't be delivered any more.
        engine
            .obs_manager
            .load_observer(observer(
                "MetricsSender",
                Arc::new(MetricsSender),
                &packet_handle,
            ))
            .unwrap();

        // Two results wait in the mailbox, unread: one with a message, which is lost work, and
        // an empty one, which isn't.
        let (ran_tx, ran_rx) = bounded(2);
        engine.accept_task(reporting_task(
            "Reporter",
            "Report",
            "row 7 done",
            ran_tx.clone(),
        ));
        engine.accept_task(reporting_task("Quiet", "", "", ran_tx));
        for _ in 0..2 {
            ran_rx
                .recv_timeout(Duration::from_secs(5))
                .expect("a task never ran");
        }

        // The only worker gets stuck, so the next two tasks stay queued and never run.
        let (task, started_rx, release_tx) = stuck_task("Sleeper");
        engine.accept_task(task);
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the task never started");
        engine.accept_task(idle_task("Waiting"));
        engine.accept_task(idle_task("Waiting"));

        let took = finalize_within(engine, Duration::from_secs(10));
        // The abandoned worker can finish now.
        let _ = release_tx.send(());
        assert!(took.is_some(), "the shutdown hung");

        let errors = read_log(&dir, "Error.log");
        assert!(
            errors.contains(
                "thryve exited leaving 5 things not done: 2 lost tasks, 1 lost notifications, \
                 1 lost task results, 1 still running"
            ),
            "{errors}"
        );
        // Written once, and only there; with debugging off, there are no details.
        assert_eq!(errors.matches("things not done").count(), 1, "{errors}");
        let main = read_log(&dir, "Main.log");
        assert!(!main.contains("things not done"), "{main}");
        let debug = read_log(&dir, "Debug.log");
        assert!(!debug.contains("not done"), "{debug}");
    }

    #[test]
    fn after_the_service_s_stop_the_summary_goes_to_main_log() {
        let dir = fresh_dir("lost_work_service_stop");
        let (mut engine, _packet_handle) = started_engine(&dir, 10, 1, 10);
        let (ran_tx, ran_rx) = bounded(1);
        engine.accept_task(reporting_task("Reporter", "Report", "row 7 done", ran_tx));
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the task never ran");

        assert!(
            finalize_within_as(engine, Duration::from_secs(10), true).is_some(),
            "the shutdown hung"
        );

        // Someone asked for this stop, so work cut short is expected: it's not an error.
        let main = read_log(&dir, "Main.log");
        assert!(main.contains(&summary_line(0, 0, 1, 0)), "{main}");
        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains("things not done"), "{errors}");

        // "Engine Finalized" still comes after it.
        let last = main.lines().rfind(|line| !line.trim().is_empty());
        assert!(
            last.is_some_and(|line| line.contains("Engine Finalized")),
            "ended on {last:?}:\n{main}"
        );
    }

    #[test]
    fn a_shutdown_that_lost_nothing_writes_no_summary() {
        let dir = fresh_dir("lost_work_none");
        let (mut engine, _packet_handle) = started_engine(&dir, 10, 1, 10);
        // Its result arrives during the shutdown, but it's empty, so nothing is lost.
        let (ran_tx, ran_rx) = bounded(1);
        engine.accept_task(reporting_task("Quiet", "", "", ran_tx));
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the task never ran");

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        for file in ["Main.log", "Error.log"] {
            let log = read_log(&dir, file);
            assert!(!log.contains("things not done"), "{file}:\n{log}");
        }
    }

    #[test]
    fn with_debugging_on_debug_log_lists_what_was_lost() {
        let dir = fresh_dir("lost_work_debug");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 10);
        engine.config.debugging.is_debugging = true;
        engine
            .obs_manager
            .load_observer(observer(
                "MetricsSender",
                Arc::new(MetricsSender),
                &packet_handle,
            ))
            .unwrap();
        let (ran_tx, ran_rx) = bounded(1);
        engine.accept_task(reporting_task("Reporter", "Report", "row 7 done", ran_tx));
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the task never ran");

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // Each kind's count, each group's size, and each item.
        let debug = read_log(&dir, "Debug.log");
        for expected in [
            "tasks: 0 not done",
            "notifs: 1 not done",
            "Metrics: 1",
            "notification type: Measurement, notification receiver: Metrics",
            "task results: 1 not done",
            "Report: 1",
            "result type: Report, message: row 7 done, next task: None, handoff: None",
        ] {
            assert!(
                debug.contains(expected),
                "missing {expected:?} in:\n{debug}"
            );
        }
        // One entry per line: each kind's count is a line of its own.
        let counts = debug.lines().filter(|line| line.contains("not done"));
        assert_eq!(counts.count(), 3, "{debug}");
    }
}

mod finalize_once {
    use super::*;

    #[test]
    fn a_second_finalize_returns_at_once_and_writes_nothing() {
        let dir = fresh_dir("finalize_twice");
        let (mut engine, _packet_handle) = started_engine(&dir, 10, 1, 10);

        // Both calls run on one thread, with a time limit, like the other shutdowns here.
        let (done_tx, done_rx) = bounded(1);
        std::thread::spawn(move || {
            engine.finalize(false);
            let start = Instant::now();
            engine.finalize(false);
            let _ = done_tx.send(start.elapsed());
        });
        let second = done_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the shutdown hung");
        assert!(
            second < Duration::from_millis(100),
            "the second call took {second:?}"
        );

        // "Engine Finalized" is there once, and still the last line.
        let main = read_log(&dir, "Main.log");
        assert_eq!(main.matches("Engine Finalized").count(), 1, "{main}");
        let last = main.lines().rfind(|line| !line.trim().is_empty());
        assert!(
            last.is_some_and(|line| line.contains("Engine Finalized")),
            "ended on {last:?}:\n{main}"
        );
    }
}

mod real_startup {
    use super::*;
    use thryve_run::service::service::engine_loop;

    /// Interactive mode's loop.
    fn interactive_loop(mut eng: Engine) {
        while !eng.exit_flag {
            eng.run_once();
        }
    }

    /// A loaded plugin whose init task runs `job`.
    fn plugin(
        ctx: &Ctx,
        plugin_type: &str,
        job: impl FnOnce() -> TaskResult + Send + Sync + 'static,
    ) -> Plugin {
        Plugin::new(plugin_type, Arc::new(QuietPlugin), ctx.packet_handle(), job).unwrap()
    }

    #[test]
    fn with_every_mailbox_holding_one_startup_pauses_and_a_fin_never_hang() {
        let mut config = Config::default();
        config.pipes.task_pipe_capacity = 1;
        config.pipes.notif_pipe_capacity = 1;
        config.pipes.res_pipe_capacity = 1;
        let mut ctx = summon_requirements(config);

        // Five plugins whose init tasks each send three log lines and two tasks through the
        // mailboxes; each of those tasks returns a result with a message.
        let (child_ran_tx, child_ran_rx) = bounded::<()>(10);
        for i in 0..5 {
            let packet_handle = ctx.packet_handle();
            let child_ran_tx = child_ran_tx.clone();
            let job = move || {
                for line in 0..3 {
                    let text = format!("Busy{i} says {line}");
                    let _ = packet_handle.send_notification(Log::log(text, "Main"));
                }
                for _ in 0..2 {
                    let child_ran_tx = child_ran_tx.clone();
                    let child = move || {
                        let _ = child_ran_tx.send(());
                        TaskResult::new("ChildDone", "child done".to_string(), "Main")
                    };
                    let _ = packet_handle.send_task(Task::new("Child", 0, child).unwrap());
                }
                TaskResult::new("", String::new(), "")
            };
            ctx.plugins.push(plugin(&ctx, &format!("Busy{i}"), job));
        }

        // A sixth one ends the run once all ten tasks have run.
        let stopper = move || {
            for _ in 0..10 {
                if child_ran_rx.recv_timeout(Duration::from_secs(5)).is_err() {
                    break;
                }
            }
            TaskResult::fin()
        };
        ctx.plugins.push(plugin(&ctx, "Stopper", stopper));

        // The loop pauses and resumes the engine a few times before it runs. Each of those used
        // to send status lines into a mailbox that holds one, from the engine's own thread.
        let outcome = run_real_engine(
            ctx,
            |mut eng: Engine| {
                for _ in 0..3 {
                    eng.pause(false);
                    eng.resume(false);
                }
                interactive_loop(eng);
            },
            Duration::from_secs(20),
        );
        assert_eq!(outcome, Ok(()));
    }

    #[test]
    fn no_plugin_code_runs_before_the_engine_starts() {
        let mut ctx = summon_requirements(Config::default());
        let (ran_tx, ran_rx) = bounded::<()>(1);
        let job = move || {
            let _ = ran_tx.send(());
            TaskResult::fin()
        };
        ctx.plugins.push(plugin(&ctx, "Early", job));

        let previous_hook = std::panic::take_hook();
        let prepared = EngineInitialization::prepare(ctx, interactive_loop);
        std::panic::set_hook(previous_hook);
        let prepared = prepared.expect("the startup failed");

        // The workers are running and the init task is queued, but the engine stays paused until
        // its thread starts it.
        assert!(
            ran_rx.recv_timeout(Duration::from_millis(300)).is_err(),
            "the plugin's init task ran during the startup"
        );

        let handle = prepared
            .complete_and_run()
            .expect("the engine didn't start");
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the init task never ran");

        // Its `Fin` ends the run.
        let (done_tx, done_rx) = bounded(1);
        std::thread::spawn(move || {
            let _ = handle.join();
            let _ = done_tx.send(());
        });
        assert!(
            done_rx.recv_timeout(Duration::from_secs(20)).is_ok(),
            "the shutdown hung"
        );
    }

    #[test]
    fn the_service_loop_ends_after_a_fin() {
        let mut ctx = summon_requirements(Config::default());
        ctx.plugins.push(plugin(&ctx, "Stopper", TaskResult::fin));

        // `Stop` never comes, so only the `Fin` can end the loop. The engine is finalized once,
        // by the `Fin`; the loop's own finalize, meant for `Stop`, doesn't run again.
        let is_running = Arc::new(AtomicBool::new(true));
        let pause_flag = Arc::new(AtomicBool::new(false));
        let outcome = run_real_engine(
            ctx,
            move |eng| engine_loop(eng, is_running, pause_flag),
            Duration::from_secs(20),
        );
        assert_eq!(outcome, Ok(()));
    }

    #[test]
    fn the_service_loop_ends_on_stop() {
        let mut ctx = summon_requirements(Config::default());
        let is_running = Arc::new(AtomicBool::new(true));
        let pause_flag = Arc::new(AtomicBool::new(false));

        // The init task plays the service manager, sending `Stop` while the engine runs.
        let stop = is_running.clone();
        let job = move || {
            stop.store(false, Ordering::SeqCst);
            TaskResult::new("", String::new(), "")
        };
        ctx.plugins.push(plugin(&ctx, "Stopper", job));

        let outcome = run_real_engine(
            ctx,
            move |eng| engine_loop(eng, is_running, pause_flag),
            Duration::from_secs(20),
        );
        assert_eq!(outcome, Ok(()));
    }
}

mod workers_wake_up {
    use super::*;

    #[test]
    fn resume_wakes_the_workers_a_pause_parked() {
        let dir = fresh_dir("resume_wakes");
        let (mut engine, _packet_handle) = started_engine(&dir, 10, 2, 10);
        engine.pause(false);
        // Long enough for both idle workers to see the pause and park.
        std::thread::sleep(Duration::from_millis(200));

        let (ran_tx, ran_rx) = bounded(1);
        engine.accept_task(reporting_task("AfterPause", "", "", ran_tx));
        assert!(
            ran_rx.recv_timeout(Duration::from_millis(200)).is_err(),
            "a task ran during the pause"
        );

        engine.resume(false);
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("resume didn't wake a worker");

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }

    #[test]
    fn a_task_chained_after_the_workers_went_idle_still_runs() {
        let dir = fresh_dir("chained_after_idle");
        let (mut engine, _packet_handle) = started_engine(&dir, 10, 2, 10);

        // "First" chains "Second", which returns `Fin`.
        let (ran_tx, ran_rx) = bounded(1);
        let first = move || {
            let _ = ran_tx.send(());
            let second = Task::new("Second", 0, TaskResult::fin).unwrap();
            TaskResult::new("", String::new(), "").chain(second)
        };
        engine.accept_task(Task::new("First", 0, first).unwrap());
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the first task never ran");
        // Nothing reads the result yet, so both workers find the queue empty and park.
        std::thread::sleep(Duration::from_millis(200));

        // The loop reads the result and queues "Second", which has to wake a parked worker.
        assert!(
            run_until_shut_down(engine, Duration::from_secs(10)),
            "the chained task never ran"
        );
    }

    #[test]
    fn a_shutdown_during_a_pause_does_not_wait_for_the_parked_workers() {
        let dir = fresh_dir("shutdown_while_paused");
        let (mut engine, _packet_handle) = started_engine(&dir, 10, 2, 10);
        engine.pause(false);
        std::thread::sleep(Duration::from_millis(200));

        // With a 10 s grace period, workers that never woke would hold it up for 10 s.
        let took = finalize_within(engine, Duration::from_secs(20)).expect("the shutdown hung");
        assert!(took < Duration::from_secs(5), "it took {took:?}");
        let errors = read_log(&dir, "Error.log");
        assert!(!errors.contains("hadn't stopped"), "{errors}");
    }
}

mod status_lines {
    use super::*;

    #[test]
    fn status_lines_never_wait_even_when_the_notification_mailbox_is_full() {
        let dir = fresh_dir("status_lines");
        let (engine, packet_handle) = started_engine(&dir, 1, 1, 10);
        // Nothing reads the mailboxes yet, and the notification mailbox holds one.
        packet_handle
            .send_notification(Log::log("filler".to_string(), "Main"))
            .unwrap();

        // Status lines have a mailbox of their own, with no limit, so these return at once.
        let (done_tx, done_rx) = bounded(1);
        let sender = packet_handle.clone();
        std::thread::spawn(move || {
            for i in 0..100 {
                sender.send_status_log(Log::log(format!("status line {i}"), "Main"));
            }
            let _ = done_tx.send(());
        });
        done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("a status line waited");

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // The shutdown read them, and the logger wrote them all, in order.
        let main = read_log(&dir, "Main.log");
        let first = main
            .find("status line 0")
            .expect("the first line is missing");
        let last = main
            .find("status line 99")
            .expect("the last line is missing");
        assert!(first < last, "{main}");
        assert!(main.contains("filler"), "{main}");
    }
}

mod graph_mode {
    use super::*;

    #[test]
    fn a_plugin_s_init_task_has_a_node_of_its_own() {
        let dir = fresh_dir("init_task_node");
        let (mut engine, packet_handle) = started_engine_with(&dir, 10, 1, 10, |ctx, engine| {
            engine
                .obs_manager
                .load_observer(TopologyHandlerObserver::new(ctx).unwrap())
                .unwrap();
        });
        // Graph mode, as startup sets it: debugging and `visual_graph` both on.
        engine.config.debugging.is_debugging = true;
        engine.config.debugging.visual_graph = true;

        // Loaded and queued the way startup does it.
        let (ran_tx, ran_rx) = bounded(1);
        let job = move || {
            let _ = ran_tx.send(());
            TaskResult::new("", String::new(), "")
        };
        let plugin = Plugin::new("Grapher", Arc::new(QuietPlugin), packet_handle, job).unwrap();
        let init_task = engine
            .plugin_manager
            .load_plugin(&Config::default(), plugin, true)
            .unwrap();
        engine.accept_task(init_task);
        ran_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the init task never ran");

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );

        // The topology handler exports the graph as it's cleaned up.
        let export =
            std::fs::read_to_string(dir.join("topology").join("topology_export.ron")).unwrap();
        assert!(export.contains("callsite_name: \"Grapher\""), "{export}");
    }
}
