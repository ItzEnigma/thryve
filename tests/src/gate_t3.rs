// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! The gate of milestone T3, "instant engine wake-up".
//!
//! The engine used to nap a fixed 4 ms whenever a turn found nothing, so every hop through it
//! could wait out a nap, and an idle engine still woke 250 times a second. Now it waits on its
//! four mailboxes at once and wakes the moment one of them gets something; the wait's limit
//! (`idle_wait`) only runs out when nothing arrives. Each turn also takes at most a set number of
//! items from each mailbox, so a busy mailbox can't keep the others waiting.
//!
//! - a chain of 1,000 tasks, each passing through the engine, finishes far faster than 1,000 naps
//!   of the old 4 ms;
//! - an idle engine wakes only when its wait runs out, so it uses no CPU, and a paused one too;
//! - each turn takes at most its limit from each mailbox;
//! - the new settings must be at least 1, like the others.
//!
//! The helpers are T2's (see `gate_t2`). CI runs these tests on one thread
//! (`--test-threads=1`), like the rest of the crate.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use thryve_core::runtime::engine::Engine;
use thryve_run::run::run_interactive::summon_requirements;
use thryve_utils::bases::plugin::Plugin;
use thryve_utils::cfg::config::Config;
use thryve_utils::errors::{ConfigError, RuleError, ThryveError, get_err};
use thryve_utils::extras::jobs::TaskJob;
use thryve_utils::packets::notif_log::Log;
use thryve_utils::packets::task::Task;
use thryve_utils::res::TaskResult;

use crate::gate_t2::{
    QuietPlugin, finalize_within, fresh_dir, idle_task, loaded_engine, run_real_engine,
    started_engine,
};

/// Interactive mode's loop.
fn interactive_loop(mut eng: Engine) {
    while !eng.exit_flag {
        eng.run_once();
    }
}

/// Calls `run_once` until `length` has passed, and returns how many turns that took.
fn turns_in(engine: &mut Engine, length: Duration) -> usize {
    let start = Instant::now();
    let mut turns = 0;

    while start.elapsed() < length {
        engine.run_once();
        turns += 1;
    }

    turns
}

/// A task that counts itself in `ran` and chains the next link until `remaining` reaches 0; the
/// last one returns `Fin`.
fn link(remaining: usize, ran: Arc<AtomicUsize>) -> Task {
    let job = TaskJob::new(move || {
        ran.fetch_add(1, Ordering::SeqCst);

        if remaining == 0 {
            TaskResult::fin()
        } else {
            TaskResult::new("Next", String::new(), "Main").chain(link(remaining - 1, ran))
        }
    });

    Task::new("Link", 0, job).unwrap()
}

mod wake_up {
    use super::*;

    #[test]
    fn a_chain_of_a_thousand_tasks_does_not_wait_out_naps() {
        // The plugin's init task chains the first of 1,000 links, each chained by the one before,
        // so each one starts only once the engine has handled the previous one's result.
        let mut ctx = summon_requirements(Config::default());
        let ran = Arc::new(AtomicUsize::new(0));
        let chain = ran.clone();
        let first = TaskJob::new(move || {
            TaskResult::new("Next", String::new(), "Main").chain(link(999, chain))
        });
        let plugin = Plugin::new("Chain", Arc::new(QuietPlugin), ctx.packet_handle(), first);
        ctx.plugins.push(plugin.unwrap());

        let start = Instant::now();
        let outcome = run_real_engine(ctx, interactive_loop, Duration::from_secs(20));
        let took = start.elapsed();

        // Startup and shutdown included. A 4 ms nap per hop would take about 4 s, and a wait that
        // didn't wake on arrival would take 100 ms (`idle_wait`) per hop.
        assert_eq!(outcome, Ok(()));
        assert_eq!(ran.load(Ordering::SeqCst), 1_000);
        assert!(took < Duration::from_secs(1), "it took {took:?}");
    }

    #[test]
    fn an_idle_engine_wakes_only_when_its_wait_runs_out() {
        let dir = fresh_dir("t3_idle");
        let (mut engine, _packet_handle) = started_engine(&dir, 10, 1, 10);
        assert_eq!(engine.config.engine.idle_wait, 100);

        // The start-up's own lines go through first.
        turns_in(&mut engine, Duration::from_millis(200));

        // Nothing arrives, so each turn waits the full 100 ms: about 5 in half a second. The
        // old 4 ms nap made 125, and a turn that didn't wait at all would make millions.
        let turns = turns_in(&mut engine, Duration::from_millis(500));
        assert!(turns <= 10, "{turns} turns in half a second");

        // Paused, a turn waits 500 ms before it reads anything: about 2 in a second.
        engine.pause(false);
        let turns = turns_in(&mut engine, Duration::from_secs(1));
        assert!(turns <= 4, "{turns} turns in a second while paused");

        engine.resume(false);
        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }
}

mod bounded_turns {
    use super::*;

    #[test]
    fn each_turn_takes_at_most_its_limit_from_each_mailbox() {
        // No workers and no sorter: the tasks stay queued, and nothing else fills the mailboxes.
        let dir = fresh_dir("t3_bounded");
        let (mut engine, packet_handle) = loaded_engine(&dir, 10, 1, 10, |_, _| {});
        engine.config.pipes.tasks_processed_per_turn = 2;
        engine.config.pipes.notifs_processed_per_turn = 2;
        engine.config.pipes.res_processed_per_turn = 2;
        let res_tx = engine.plugin_manager.res_tx.clone();

        // Three of each, so a turn leaves one behind for the next.
        for i in 0..3 {
            packet_handle.send_task(idle_task("Waiting")).unwrap();
            let line = Log::log(format!("notification {i}"), "Main");
            packet_handle.send_notification(line).unwrap();
            let result = TaskResult::new("Done", String::new(), "Main");
            res_tx.send((i, result)).unwrap();
        }
        // Status lines have a fixed limit of 500.
        for i in 0..501 {
            packet_handle.send_status(Log::log(format!("status {i}"), "Main"));
        }

        assert!(engine.check_tasks(), "the first turn found no task");
        assert!(engine.check_tasks(), "the first turn took all three tasks");
        assert!(!engine.check_tasks(), "a fourth task appeared");

        assert!(
            engine.check_notifs(),
            "the first turn found no notification"
        );
        assert!(
            engine.check_notifs(),
            "the first turn took all three notifications"
        );
        assert!(!engine.check_notifs(), "a fourth notification appeared");

        assert!(engine.check_results(), "the first turn found no result");
        assert!(
            engine.check_results(),
            "the first turn took all three results"
        );
        assert!(!engine.check_results(), "a fourth result appeared");

        assert!(engine.check_status(), "the first turn found no status line");
        assert!(
            engine.check_status(),
            "the first turn took all 501 status lines"
        );
        assert!(!engine.check_status(), "a 502nd status line appeared");

        assert!(
            finalize_within(engine, Duration::from_secs(10)).is_some(),
            "the shutdown hung"
        );
    }
}

mod settings {
    use super::*;

    #[test]
    fn the_new_settings_must_be_at_least_one() {
        let dir = fresh_dir("t3_config");
        let path = dir.join("config.toml");
        std::fs::write(
            &path,
            "[service]\ncan_stop = true\ncan_pause = true\n\n\
             [engine]\nidle_wait = 0\nshutdown_grace_period = 10\n\n\
             [threads]\nworker_count = 4\n\n\
             [pipes]\ntask_pipe_capacity = 1024\nnotif_pipe_capacity = 1024\n\
             res_pipe_capacity = 1024\ntasks_processed_per_turn = 0\n\
             notifs_processed_per_turn = 0\nres_processed_per_turn = 0\n\n\
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
            matches!(
                violation.types[..],
                [
                    RuleError::IdleWait(0, 1),
                    RuleError::TasksProcessedPerTurn(0, 1),
                    RuleError::NotifsProcessedPerTurn(0, 1),
                    RuleError::ResultsProcessedPerTurn(0, 1),
                ]
            ),
            "{}",
            get_err(&error)
        );

        let message = get_err(&error);
        for expected in [
            "engine.idle_wait is 0, must be at least 1",
            "pipes.tasks_processed_per_turn is 0, must be at least 1",
            "pipes.notifs_processed_per_turn is 0, must be at least 1",
            "pipes.res_processed_per_turn is 0, must be at least 1",
        ] {
            assert!(
                message.contains(expected),
                "missing {expected:?} in: {message}"
            );
        }
    }
}
