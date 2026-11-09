// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Fan-out: several followers sharing one task's result.
//!
//! An error's record and its pointer (`records_and_pointers`): when the engine queues a task with
//! another task's error as its input, it keeps the error's text under the id of the task that
//! produced it, and a pointer to it under the id of the task that carries it. The carrier's report
//! takes the pointer and follows it. For a follow-up or a join, each record has one pointer, and
//! the cancelling tests (`cancelling`) cover that, counting both entries in the wait table. One id can
//! also hold both, one after the other:
//!
//! - a task that took an error and failed hands its own error on. Its report takes the pointer
//!   it carried before its own record goes under its id, so the error it took is never written,
//!   and its own error, if nobody takes it, is written to `Error.log` once.
//!
//! The typed side (`the_typed_side`): everything the dev writes, up to the moment a group reaches
//! the engine. `fan_out()` on LoadImg's handle returns a group, `then` on it declares Thumbnail,
//! Compress and Metadata, and `send_fanout()` sends them. No engine is involved: the tests read the
//! two mailboxes, and run tasks the way a worker does.
//!
//! - the followers wait in the group until it's sent, then go to the task mailbox one by one, in
//!   the order they were declared, as fan-out followers of LoadImg. The group's data follows on the
//!   status mailbox, and LoadImg's handle, used up, sends no release;
//! - a group sent with no follower is refused, and LoadImg is released;
//! - a group dropped unsent is sent anyway, and one with no follower only releases LoadImg;
//! - every follower reads the one result, through the converter, without a copy. When LoadImg
//!   fails, each follower can pass LoadImg's one error on as its own;
//! - the converter refuses a result of another type, and a follower handed the sealed box instead
//!   of a share reports it;
//! - a follower inherits LoadImg's priority, capped below realtime, unless it's given one;
//! - graph mode records the line of each follower's `then`;
//! - a task's closure can make a group and send it while it runs. That no closure can capture one
//!   is checked by a `compile_fail` example on `FanoutGroup`.
//!
//! The engine's group (`the_engine_s_group`): whatever reaches the engine first, the group's data,
//! its followers or LoadImg's result, the three meet, and each follower gets a share.
//!
//! - followers that arrive before the share are held under their own ids, and queued once the
//!   converter runs; the ones that arrive after it are queued at once, and the group goes once
//!   all three are queued;
//! - after LoadImg forwards, before or after the group's data, a note at the end of the links
//!   leads the result back to the group, moves on with each further forward, and isn't left
//!   behind when the result is already kept there;
//! - all three read the one value, and the last one done with it frees it, once;
//! - a held follower cancelled before the result is skipped, and the others still run;
//! - LoadImg's error gets one record, counting every follower, whether it has arrived or not.
//!   Nothing is written once any follower takes the error, and the error is written once if
//!   they're all cancelled, while held or in the queue;
//! - a follower's handle is an ordinary handle, and a group dropped unsent still runs its
//!   followers.
//!
//! Cancels (`the_engine_s_group::cancels`): a cancelled task stops the work that only it needed.
//! The cancel goes up from the task cancelled to the task it waits for, as long as nobody else
//! needs that task's result, and so on up. A task that's already running isn't stopped, and its
//! report takes its id out of the cancelled set.
//!
//! - cancelling the end of a chain stops every task above it that hasn't started, even when the
//!   cancel is read before the follow-up itself;
//! - after a forward, a cancelled follow-up stops the task forwarded to, not the one that
//!   forwarded, which has already reported;
//! - LoadImg stops once every follower is cancelled: before the group's data, after it but
//!   before they arrive, or while they're held, and so does the task LoadImg waits for, if only
//!   LoadImg needed it. A forward LoadImg makes after that stops the task forwarded to;
//! - LoadImg's cancelled report skips the held followers, and a follower that arrives after it
//!   is skipped at once;
//! - a cancelled follow-up of one follower, or a cancelled join over two, withdraws only those
//!   followers, and the others still run;
//! - an error that the group's data finds with no follower live is still written once.
//!
//! The shutdown (`the_engine_s_group::the_shutdown`): a follower still held is a task that never
//! ran, unless it was cancelled, and an error's record is one lost result, however many followers
//! carry a pointer to it, even none.

use crossbeam::channel::unbounded;

use thryve_core::runtime::engine::Engine;

use crate::cancelling::{
    WAIT, error_log_mentions_a_cancel, failing_row, holding_the_worker, telling_follow_up,
    times_logged, until,
};
use crate::shutdown_and_startup::{finalize_within, fresh_dir, started_engine};
use crate::typed_tasks::run_until_the_wait_table_is_empty;

mod records_and_pointers {
    use super::*;

    #[test]
    fn a_task_that_took_an_error_and_failed_hands_its_own_on_and_it_is_logged_once_if_nobody_takes_it()
     {
        let dir = fresh_dir("fan_out_a_task_that_took_an_error_and_failed");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (thumbnail_ran_tx, thumbnail_ran_rx) = unbounded();

        // Hash has failed already, and its error is kept for its handle.
        let hash = failing_row("Hash", "photo.png is corrupt");
        let hash = packet_handle.send_adv_task(hash).unwrap();
        until(&mut engine, Engine::check_tasks, "Hash");
        until(&mut engine, Engine::check_results, "Hash's error");

        // Thumbnail is queued with Hash's error, and the only worker runs it at once: it takes
        // the error and fails with its own. The engine doesn't read its result yet.
        let thumbnail = hash
            .then("Thumbnail", move |_| {
                let _ = thumbnail_ran_tx.send(());
                Err::<u32, std::io::Error>(std::io::Error::other("no thumbnail for photo.png"))
            })
            .unwrap();
        until(&mut engine, Engine::check_tasks, "Thumbnail");
        thumbnail_ran_rx
            .recv_timeout(WAIT)
            .expect("Thumbnail never ran");

        // Save waits for Thumbnail's result, and Row 1 holds the worker, now that Thumbnail is done.
        let save = thumbnail
            .then("Save", telling_follow_up("Save", &ran_tx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "Save");
        let go_tx = holding_the_worker(&mut engine, &packet_handle);

        // Thumbnail's report takes the pointer under its id, and drops Hash's error, which it
        // took. Only then does its own error go under its id, with a pointer to it under Save's,
        // and Save waits in the queue behind Row 1.
        until(&mut engine, Engine::check_results, "Thumbnail's error");
        assert_eq!(
            engine.wait_table_len(),
            2,
            "Thumbnail's error and its pointer, and nothing of Hash's"
        );

        save.cancel();
        until(&mut engine, Engine::check_status, "the cancel");
        assert_eq!(engine.cancelled_tasks.len(), 1);

        // The worker skips Save, and its report has the engine write the error it never took.
        go_tx.send(()).unwrap();
        run_until_the_wait_table_is_empty(&mut engine);

        assert!(ran_rx.try_recv().is_err(), "Save ran");
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert_eq!(times_logged(&dir, "no thumbnail for photo.png"), 1);
        assert_eq!(times_logged(&dir, "photo.png is corrupt"), 0);
        assert!(!error_log_mentions_a_cancel(&dir));
    }
}

/// The typed side: the group, from `fan_out()` to the two mailboxes, with no engine.
mod the_typed_side {
    use std::any::Any;
    use std::sync::Arc;

    use crossbeam::channel::Receiver;

    use thryve_utils::errors::{FanoutError, ThryveError};
    use thryve_utils::packets::packet_handle::StatusMsg;
    use thryve_utils::packets::task::{AdvTask, Task};
    use thryve_utils::res::{Handoff, SealedHandoff, TaskResult};
    use thryve_utils::traits::packets::Packet;

    use crate::typed_tasks::{
        handle_and_mailbox, handle_mailbox_and_status, open, run_like_a_worker,
    };

    /// The converter, as the group's data carries it.
    type Converter =
        Box<dyn FnOnce(SealedHandoff) -> Result<Arc<dyn Any + Send + Sync>, ThryveError> + Send>;

    /// What a follower of LoadImg gets: LoadImg's pixels or its error, shared.
    type Share = Arc<Handoff<Vec<u8>, Arc<std::io::Error>>>;

    /// LoadImg, which decodes photo.png: here, into 16 bytes of 7.
    fn load_img(priority: u8) -> AdvTask<Vec<u8>, std::io::Error> {
        AdvTask::new("LoadImg", priority, || Ok(vec![7; 16])).unwrap()
    }

    /// A follower whose result doesn't matter.
    fn ignoring(_: Share) -> Result<u32, std::io::Error> {
        Ok(0)
    }

    /// A follower that returns where the pixels it reads are: two followers reading the same
    /// pixels return the same address.
    fn pixels_address(share: Share) -> Result<usize, std::io::Error> {
        match &*share {
            Handoff::Value(pixels) => Ok(pixels.as_ptr() as usize),
            Handoff::Error(error) => Err(std::io::Error::other(error.to_string())),
        }
    }

    /// A follower that passes LoadImg's error on as its own, so its error type is
    /// `Arc<std::io::Error>`.
    fn passing_the_error_on(share: Share) -> Result<u32, Arc<std::io::Error>> {
        match &*share {
            Handoff::Value(_) => Ok(0),
            Handoff::Error(error) => Err(Arc::clone(error)),
        }
    }

    /// Everything on the status mailbox, in order: "release {id}", "cancel {id}",
    /// "group {id}: {followers' ids}", or "a status line".
    fn statuses(status: &Receiver<StatusMsg>) -> Vec<String> {
        status
            .try_iter()
            .map(|message| match message {
                StatusMsg::HandleRelease(id) => format!("release {id}"),
                StatusMsg::Cancel(id) => format!("cancel {id}"),
                StatusMsg::GroupUsageGuide {
                    pre_fanout_task_id,
                    fanout_tasks_ids,
                    ..
                } => format!("group {pre_fanout_task_id}: {fanout_tasks_ids:?}"),
                StatusMsg::LogLine(_) => String::from("a status line"),
            })
            .collect()
    }

    /// The converter, taken from the group's data, which must be next on the status mailbox.
    fn converter_from(status: &Receiver<StatusMsg>) -> Converter {
        match status.try_recv() {
            Ok(StatusMsg::GroupUsageGuide { converter, .. }) => converter,
            _ => panic!("expected the group's data on the status mailbox"),
        }
    }

    /// Runs a follower the way a worker does, with its share as its input.
    fn run_with_share(follower: Task, share: &Arc<dyn Any + Send + Sync>) -> (u64, TaskResult) {
        let input = SealedHandoff::FanoutShare {
            share: Arc::clone(share),
        };
        follower.with_input(input, 4).process().unwrap()
    }

    #[test]
    fn the_followers_wait_in_the_group_then_go_one_by_one_with_the_group_s_data_after_them() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let load = handle.send_adv_task(load_img(2)).unwrap();
        let load_id = mailbox.try_recv().unwrap().id;

        let mut group = load.fan_out();
        let _thumbnail = group.then("Thumbnail", ignoring).unwrap();
        let _compress = group.then("Compress", ignoring).unwrap();
        let _metadata = group.then("Metadata", ignoring).unwrap();
        assert!(
            mailbox.try_recv().is_err(),
            "a follower left before the group was sent"
        );

        group.send_fanout().unwrap();

        let followers: Vec<Task> = mailbox.try_iter().collect();
        let names: Vec<&str> = followers.iter().map(Task::task_name).collect();
        assert_eq!(names, ["Thumbnail", "Compress", "Metadata"]);
        for follower in &followers {
            assert!(follower.is_job_task_fanout(), "{}", follower.task_name());
            assert!(!follower.is_job_task_wait(), "{}", follower.task_name());
            assert_eq!(follower.waiting_on_id(), Some(load_id));
        }

        // The group's data comes after them. LoadImg's handle, used up, sends no release.
        let ids: Vec<u64> = followers.iter().map(|follower| follower.id).collect();
        assert_eq!(statuses(&status), [format!("group {load_id}: {ids:?}")]);
    }

    #[test]
    fn a_group_sent_with_no_follower_is_refused_and_load_img_is_released() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let load = handle.send_adv_task(load_img(2)).unwrap();
        let load_id = mailbox.try_recv().unwrap().id;

        let refused = load.fan_out().send_fanout();

        assert!(matches!(
            refused,
            Err(ThryveError::Fanout(FanoutError::MonitorsZeroInputs { pre_fanout_task_id }))
                if pre_fanout_task_id == load_id
        ));
        assert!(mailbox.try_recv().is_err());
        assert_eq!(statuses(&status), [format!("release {load_id}")]);
    }

    #[test]
    fn a_group_dropped_unsent_is_sent_anyway() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let load = handle.send_adv_task(load_img(2)).unwrap();
        let load_id = mailbox.try_recv().unwrap().id;

        let mut group = load.fan_out();
        let _thumbnail = group.then("Thumbnail", ignoring).unwrap();
        let _compress = group.then("Compress", ignoring).unwrap();
        drop(group);

        let ids: Vec<u64> = mailbox.try_iter().map(|follower| follower.id).collect();
        assert_eq!(ids.len(), 2, "a follower wasn't sent");
        assert_eq!(statuses(&status), [format!("group {load_id}: {ids:?}")]);
    }

    #[test]
    fn a_group_dropped_with_no_follower_only_releases_load_img() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let load = handle.send_adv_task(load_img(2)).unwrap();
        let load_id = mailbox.try_recv().unwrap().id;

        drop(load.fan_out());

        assert!(mailbox.try_recv().is_err());
        assert_eq!(statuses(&status), [format!("release {load_id}")]);
    }

    #[test]
    fn every_follower_reads_the_one_result_load_img_returned() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let load = handle.send_adv_task(load_img(2)).unwrap();
        let load_task = mailbox.try_recv().unwrap();

        let mut group = load.fan_out();
        let _thumbnail = group.then("Thumbnail", pixels_address).unwrap();
        let _compress = group.then("Compress", pixels_address).unwrap();
        group.send_fanout().unwrap();
        let followers: Vec<Task> = mailbox.try_iter().collect();

        // LoadImg runs on a worker, and the engine runs the converter once on its result.
        let (_, mut result) = load_task.process().unwrap();
        let share = converter_from(&status)(result.take_handoff().unwrap()).unwrap();

        let addresses: Vec<usize> = followers
            .into_iter()
            .map(|follower| {
                let (_, result) = run_with_share(follower, &share);
                open::<usize, std::io::Error>(result).0.unwrap()
            })
            .collect();
        assert_eq!(addresses.len(), 2);
        assert_eq!(
            addresses[0], addresses[1],
            "the followers read different pixels"
        );
    }

    #[test]
    fn when_load_img_fails_each_follower_can_pass_its_one_error_on_as_its_own() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let load = AdvTask::new("LoadImg", 2, || {
            Err::<Vec<u8>, _>(std::io::Error::other("photo.png is corrupt"))
        })
        .unwrap();
        let load = handle.send_adv_task(load).unwrap();
        let load_task = mailbox.try_recv().unwrap();

        let mut group = load.fan_out();
        let _thumbnail = group.then("Thumbnail", passing_the_error_on).unwrap();
        let _compress = group.then("Compress", passing_the_error_on).unwrap();
        group.send_fanout().unwrap();
        let followers: Vec<Task> = mailbox.try_iter().collect();

        let (_, mut result) = load_task.process().unwrap();
        let share = converter_from(&status)(result.take_handoff().unwrap()).unwrap();

        let errors: Vec<(Arc<std::io::Error>, Option<String>)> = followers
            .into_iter()
            .map(|follower| {
                let (_, result) = run_with_share(follower, &share);
                let (outcome, text) = open::<u32, Arc<std::io::Error>>(result);
                (outcome.unwrap_err(), text)
            })
            .collect();

        // Both hold LoadImg's one error, not copies of it, and each names itself in its text.
        assert!(Arc::ptr_eq(&errors[0].0, &errors[1].0));
        assert_eq!(errors[0].0.to_string(), "photo.png is corrupt");
        assert_eq!(
            errors[0].1.as_deref(),
            Some("task 'Thumbnail' returned an error: photo.png is corrupt")
        );
        assert_eq!(
            errors[1].1.as_deref(),
            Some("task 'Compress' returned an error: photo.png is corrupt")
        );
    }

    #[test]
    fn the_converter_refuses_a_result_of_another_type() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let load = handle.send_adv_task(load_img(2)).unwrap();
        let load_id = mailbox.try_recv().unwrap().id;
        let mut group = load.fan_out();
        let _thumbnail = group.then("Thumbnail", ignoring).unwrap();
        group.send_fanout().unwrap();

        // Name's result is a `Result<String, _>`, not LoadImg's `Result<Vec<u8>, _>`.
        let name = AdvTask::new("Name", 1, || {
            Ok::<String, std::io::Error>("photo.png".into())
        });
        let (_, _, mut result) = run_like_a_worker(name.unwrap());

        assert!(matches!(
            converter_from(&status)(result.take_handoff().unwrap()),
            Err(ThryveError::Fanout(FanoutError::SealedHandoffFailedDowncast { pre_fanout_task_id }))
                if pre_fanout_task_id == load_id
        ));
    }

    #[test]
    fn a_follower_handed_the_sealed_box_instead_of_a_share_reports_it() {
        let (handle, mailbox, _status) = handle_mailbox_and_status();
        let load = handle.send_adv_task(load_img(2)).unwrap();
        let load_task = mailbox.try_recv().unwrap();
        let mut group = load.fan_out();
        let _thumbnail = group.then("Thumbnail", ignoring).unwrap();
        group.send_fanout().unwrap();
        let thumbnail = mailbox.try_recv().unwrap();

        // The sealed box is the converter's to open; a follower can't.
        let (_, mut result) = load_task.process().unwrap();
        let (_, result) = thumbnail
            .with_input(result.take_handoff().unwrap(), 4)
            .process()
            .unwrap();

        assert_eq!(result.result_type(), "TaskError");
        assert_eq!(
            result.message,
            "handoff to task 'Thumbnail' failed to downcast properly"
        );
    }

    #[test]
    fn a_follower_inherits_load_img_s_priority_capped_below_realtime_unless_given_one() {
        // With a `max_priority` of 4, realtime starts at 3, so an inherited 4 is capped at 2.
        let (handle, mailbox, _status) = handle_mailbox_and_status();
        let load = handle.send_adv_task(load_img(4)).unwrap();
        let _load = mailbox.try_recv().unwrap();
        let mut group = load.fan_out();
        let _thumbnail = group.then("Thumbnail", ignoring).unwrap();
        let _compress = group.then_with_priority("Compress", 4, ignoring).unwrap();
        group.send_fanout().unwrap();

        let thumbnail = mailbox.try_recv().unwrap();
        let compress = mailbox.try_recv().unwrap();
        assert_eq!(
            thumbnail.base_priority, 4,
            "it's sent with LoadImg's priority"
        );
        assert_eq!(compress.base_priority, 4);

        // Any input will do: only the priority is checked, and neither runs.
        let thumbnail = thumbnail.with_input(SealedHandoff::Cancelled, 4);
        let compress = compress.with_input(SealedHandoff::Cancelled, 4);
        assert_eq!(thumbnail.base_priority, 2, "capped just below realtime");
        assert_eq!(
            compress.base_priority, 4,
            "a given priority is kept, even realtime"
        );
    }

    #[test]
    fn graph_mode_records_the_line_of_each_follower_s_then_not_the_line_that_sends_the_group() {
        let (handle, mailbox) = handle_and_mailbox(true);
        let load = handle.send_adv_task(load_img(2)).unwrap();
        let _load = mailbox.try_recv().unwrap();
        let mut group = load.fan_out();
        let mut lines = Vec::new();

        lines.push(line!() + 1);
        let _thumbnail = group.then("Thumbnail", ignoring).unwrap();
        lines.push(line!() + 1);
        let _compress = group.then_with_priority("Compress", 2, ignoring).unwrap();
        group.send_fanout().unwrap();

        for line in lines {
            let follower = mailbox.try_recv().unwrap();
            let name = follower.task_name().to_string();
            let callsite = follower.callsite.expect("graph mode records a call site");
            assert_eq!(callsite.line_number, line, "{name}");
            assert!(
                callsite.file_path.ends_with("fan_out.rs"),
                "{:?}",
                callsite.file_path
            );
        }
    }

    #[test]
    fn a_task_s_closure_can_make_a_group_and_send_it_while_it_runs() {
        let (handle, mailbox, status) = handle_mailbox_and_status();
        let load = handle.send_adv_task(load_img(2)).unwrap();
        let load_id = mailbox.try_recv().unwrap().id;

        // Publish captures LoadImg's handle, which can move to a worker. It couldn't capture a
        // group (see `FanoutGroup`'s `compile_fail` example), but it can make one as it runs.
        let publish = AdvTask::new("Publish", 2, move || {
            let mut group = load.fan_out();
            group.then("Thumbnail", ignoring)?;
            group.send_fanout()
        })
        .unwrap();
        let _publish = handle.send_adv_task(publish).unwrap();
        let (_, result) = mailbox.try_recv().unwrap().process().unwrap();

        assert!(open::<(), ThryveError>(result).0.is_ok());
        let thumbnail = mailbox.try_recv().unwrap();
        assert_eq!(thumbnail.task_name(), "Thumbnail");
        assert_eq!(thumbnail.waiting_on_id(), Some(load_id));
        let group = format!("group {load_id}: [{}]", thumbnail.id);
        assert!(
            statuses(&status).contains(&group),
            "the group's data wasn't sent"
        );
    }
}

/// The engine's side: the engine puts the group together and shares LoadImg's result, and an
/// error it shares gets one record.
mod the_engine_s_group {
    use std::sync::Arc;
    use std::time::Duration;

    use crossbeam::channel::{Receiver, Sender, bounded};

    use thryve_utils::packets::packet_handle::PacketHandle;
    use thryve_utils::packets::task::{AdvTask, AdvTaskHandle, FanoutGroup};
    use thryve_utils::res::Handoff;

    use crate::typed_tasks::forwarding;

    use super::*;

    /// What a follower of LoadImg gets: LoadImg's pixels or its error, shared.
    type Share = Arc<Handoff<Vec<u8>, Arc<std::io::Error>>>;

    /// A follower's handle. A follower returns how many pixels it read: none, for an error.
    type Follower = AdvTaskHandle<usize, std::io::Error>;

    /// The followers, in the order they're declared.
    const FOLLOWERS: [&str; 3] = ["Thumbnail", "Compress", "Metadata"];

    /// What LoadImg's error says.
    const MISSING: &str = "photo.png is missing";

    /// How long a test waits to be sure that something didn't happen.
    const A_MOMENT: Duration = Duration::from_millis(300);

    /// A task named `name` that decodes photo.png into 16 bytes of 7.
    fn decoding(name: &str) -> AdvTask<Vec<u8>, std::io::Error> {
        AdvTask::new(name, 2, || Ok(vec![7; 16])).unwrap()
    }

    /// Like [`decoding`], but only once `go` lets it, so a test can hold its result back.
    fn held_back(name: &str, go: Receiver<()>) -> AdvTask<Vec<u8>, std::io::Error> {
        AdvTask::new(name, 2, move || {
            let _ = go.recv();
            Ok(vec![7; 16])
        })
        .unwrap()
    }

    /// LoadImg, which fails to find photo.png once `go` lets it.
    fn failing_load_img(go: Receiver<()>) -> AdvTask<Vec<u8>, std::io::Error> {
        AdvTask::new("LoadImg", 2, move || {
            let _ = go.recv();
            Err(std::io::Error::other(MISSING))
        })
        .unwrap()
    }

    /// Like [`failing_load_img`], but it tells `started` once a worker runs it, so a test can be
    /// sure it's running before cancelling everything that needs it: a running task isn't
    /// stopped, and its result still comes.
    fn failing_load_img_once_started(
        started: Sender<()>,
        go: Receiver<()>,
    ) -> AdvTask<Vec<u8>, std::io::Error> {
        AdvTask::new("LoadImg", 2, move || {
            let _ = started.send(());
            let _ = go.recv();
            Err(std::io::Error::other(MISSING))
        })
        .unwrap()
    }

    /// A task named `name` that forwards to `to` once `go` lets it, so a test can hold the
    /// forward back.
    fn forwarding_when_let(
        name: &str,
        sender: &PacketHandle,
        to: AdvTask<Vec<u8>, std::io::Error>,
        go: Receiver<()>,
    ) -> AdvTask<Vec<u8>, std::io::Error> {
        let sender = sender.clone();
        AdvTask::forward(name, 1, move || {
            let _ = go.recv();
            Ok(sender.send_adv_task(to).unwrap())
        })
        .unwrap()
    }

    /// Declares Thumbnail, Compress and Metadata on `group`. Each tells `ran` its name and how
    /// many pixels it read.
    fn declare_three_followers(
        group: &mut FanoutGroup<Vec<u8>, std::io::Error>,
        ran: &Sender<String>,
    ) -> Vec<Follower> {
        FOLLOWERS
            .iter()
            .map(|name| {
                let ran = ran.clone();
                let told = name.to_string();
                group
                    .then(name, move |share: Share| {
                        let pixels = match &*share {
                            Handoff::Value(pixels) => pixels.len(),
                            Handoff::Error(_) => 0,
                        };
                        let _ = ran.send(format!("{told} {pixels}"));
                        Ok(pixels)
                    })
                    .unwrap()
            })
            .collect()
    }

    /// Fans LoadImg out to Thumbnail, Compress and Metadata, and sends the group: the followers
    /// go on the task mailbox, and the group's data on the status mailbox.
    fn send_three_followers(
        load: AdvTaskHandle<Vec<u8>, std::io::Error>,
        ran: &Sender<String>,
    ) -> Vec<Follower> {
        let mut group = load.fan_out();
        let followers = declare_three_followers(&mut group, ran);
        group.send_fanout().unwrap();
        followers
    }

    /// Takes in the followers: they're all on the task mailbox already, so one read takes them.
    fn the_followers_arrive(engine: &mut Engine) {
        until(engine, Engine::check_tasks, "the followers");
    }

    /// Reads tasks and results, never the status mailbox, until the wait table holds `entries`.
    /// A task that forwards sends the next task while it runs, so one read of the task mailbox
    /// can take in both, or only the first: how many reads it takes depends on the worker.
    fn until_the_wait_table_holds(engine: &mut Engine, entries: usize, what: &str) {
        until(
            engine,
            |engine| {
                engine.check_tasks();
                engine.check_results();
                engine.wait_table_len() == entries
            },
            what,
        );
    }

    /// What `count` followers told, sorted.
    fn told(ran: &Receiver<String>, count: usize) -> Vec<String> {
        let mut told: Vec<String> = (0..count)
            .map(|_| ran.recv_timeout(WAIT).expect("a follower never ran"))
            .collect();
        told.sort();
        told
    }

    /// What the followers `names` tell when each read `pixels` pixels, sorted.
    fn each_read(names: &[&str], pixels: usize) -> Vec<String> {
        let mut told: Vec<String> = names
            .iter()
            .map(|name| format!("{name} {pixels}"))
            .collect();
        told.sort();
        told
    }

    /// Drops the followers' handles, runs the engine until both tables are empty, and
    /// finalizes it.
    fn finish(mut engine: Engine, followers: Vec<Follower>) {
        drop(followers);
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(finalize_within(engine, WAIT).is_some());
    }

    /// LoadImg fails at once, and its error is kept. Row 1 holds the only worker, and the group
    /// is sent: its data runs the converter on the error, which keeps the error's record, and
    /// the three followers are queued behind Row 1, each with a pointer to the record. Returns
    /// what lets Row 1 go on, and the followers' handles.
    fn three_queued_with_load_img_s_error(
        engine: &mut Engine,
        packet_handle: &PacketHandle,
        ran: &Sender<String>,
    ) -> (Sender<()>, Vec<Follower>) {
        let (go_tx, go_rx) = bounded(1);
        go_tx.send(()).unwrap();
        let load = packet_handle
            .send_adv_task(failing_load_img(go_rx))
            .unwrap();
        until(engine, Engine::check_tasks, "LoadImg");
        until(engine, Engine::check_results, "LoadImg's error");

        let row_1 = holding_the_worker(engine, packet_handle);
        let followers = send_three_followers(load, ran);
        until(engine, Engine::check_status, "the group's data");
        the_followers_arrive(engine);
        (row_1, followers)
    }

    // Whichever arrives first, the group's data, the followers and the result meet.

    #[test]
    fn followers_held_before_the_group_s_data_run_once_it_finds_load_img_s_kept_result() {
        let dir = fresh_dir("fan_out_followers_held_then_the_group_s_data");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let load = packet_handle.send_adv_task(decoding("LoadImg")).unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");
        until(&mut engine, Engine::check_results, "LoadImg's result");

        // The followers arrive before the group's data, so there's no group yet: each is held
        // under its own id.
        let followers = send_three_followers(load, &ran_tx);
        the_followers_arrive(&mut engine);
        assert_eq!(
            engine.wait_table_len(),
            4,
            "LoadImg's result, and the three followers held"
        );

        // The group's data finds LoadImg's result: the converter runs, and the three are queued.
        until(&mut engine, Engine::check_status, "the group's data");

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        finish(engine, followers);
    }

    #[test]
    fn followers_that_arrive_after_the_share_are_queued_at_once() {
        let dir = fresh_dir("fan_out_the_group_s_data_then_the_followers");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let load = packet_handle.send_adv_task(decoding("LoadImg")).unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");
        until(&mut engine, Engine::check_results, "LoadImg's result");

        // The group's data finds LoadImg's result first: the converter runs, and the group waits
        // for its followers, with the share.
        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        assert_eq!(engine.wait_table_len(), 1, "only the group, with its share");

        // Each follower finds the share and is queued at once. Once all three are, the group
        // goes.
        the_followers_arrive(&mut engine);
        assert_eq!(
            engine.wait_table_len(),
            0,
            "the group, after its three followers"
        );

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        finish(engine, followers);
    }

    #[test]
    fn followers_held_while_the_group_waits_run_when_load_img_s_result_arrives() {
        let dir = fresh_dir("fan_out_the_group_waits_for_the_result");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (go_tx, go_rx) = bounded(1);
        let load = packet_handle
            .send_adv_task(held_back("LoadImg", go_rx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");

        // The group waits in LoadImg's slot with its converter, and the followers under their
        // own ids, since there's no share yet.
        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        the_followers_arrive(&mut engine);
        assert_eq!(
            engine.wait_table_len(),
            4,
            "the group, and the three followers held"
        );

        // LoadImg's result finds the group: the converter runs, and the three are queued.
        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "LoadImg's result");

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        finish(engine, followers);
    }

    // LoadImg forwards: the result at the end of the links reaches the group.

    #[test]
    fn a_forward_made_before_the_group_s_data_leaves_the_note_at_the_task_forwarded_to() {
        let dir = fresh_dir("fan_out_a_forward_then_the_group_s_data");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (go_tx, go_rx) = bounded(1);
        let decode = held_back("Decode", go_rx);
        let load = packet_handle
            .send_adv_task(forwarding("LoadImg", &packet_handle, decode))
            .unwrap();
        // Decode is taken in with LoadImg or after it, at the latest with the followers.
        until_the_wait_table_holds(&mut engine, 1, "LoadImg's link");

        // The group takes the link's place in LoadImg's slot, and a note waits in Decode's.
        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        assert_eq!(
            engine.wait_table_len(),
            2,
            "the group, and the note at Decode"
        );
        the_followers_arrive(&mut engine);

        // Decode's result reaches the group through the note.
        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "Decode's result");

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        finish(engine, followers);
    }

    #[test]
    fn a_result_already_kept_at_the_end_of_a_forward_runs_the_converter_at_once() {
        let dir = fresh_dir("fan_out_a_kept_result_at_the_end_of_a_forward");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let load = packet_handle
            .send_adv_task(forwarding("LoadImg", &packet_handle, decoding("Decode")))
            .unwrap();
        until_the_wait_table_holds(&mut engine, 2, "LoadImg's link, and Decode's result kept,");

        // The group's data follows the link to Decode's kept result: the converter runs on it,
        // and no note is left behind.
        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        assert_eq!(engine.wait_table_len(), 1, "only the group, with its share");
        the_followers_arrive(&mut engine);

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        finish(engine, followers);
    }

    #[test]
    fn a_forward_made_after_the_group_s_data_leaves_the_note_at_the_task_forwarded_to() {
        let dir = fresh_dir("fan_out_the_group_s_data_then_a_forward");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (go_tx, go_rx) = bounded(1);
        let load_img = forwarding_when_let("LoadImg", &packet_handle, decoding("Decode"), go_rx);
        let load = packet_handle.send_adv_task(load_img).unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");

        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        the_followers_arrive(&mut engine);

        // LoadImg's forward reaches the group. It isn't the result, so a note goes to Decode's
        // slot instead.
        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "LoadImg's forward");
        assert_eq!(
            engine.wait_table_len(),
            5,
            "the group, the note at Decode, and the three followers held"
        );

        // Decode's result reaches the group through the note.
        until(&mut engine, Engine::check_tasks, "Decode");
        until(&mut engine, Engine::check_results, "Decode's result");

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        finish(engine, followers);
    }

    #[test]
    fn two_forwards_made_before_the_group_s_data_leave_one_note_at_the_last_task() {
        let dir = fresh_dir("fan_out_two_forwards_then_the_group_s_data");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (go_tx, go_rx) = bounded(1);
        let decode = forwarding("Decode", &packet_handle, held_back("Upload", go_rx));
        let load = packet_handle
            .send_adv_task(forwarding("LoadImg", &packet_handle, decode))
            .unwrap();
        // Upload is taken in on the way, or at the latest with the followers.
        until_the_wait_table_holds(&mut engine, 2, "the two links");

        // The group's data removes both links as it passes them, and leaves the note at Upload.
        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        assert_eq!(
            engine.wait_table_len(),
            2,
            "the group, and the note at Upload"
        );
        the_followers_arrive(&mut engine);

        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "Upload's result");

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        finish(engine, followers);
    }

    #[test]
    fn a_forward_that_reaches_the_note_moves_it_on() {
        let dir = fresh_dir("fan_out_the_note_moves_on");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (go_tx, go_rx) = bounded(1);
        let decode = forwarding_when_let("Decode", &packet_handle, decoding("Upload"), go_rx);
        let load = packet_handle
            .send_adv_task(forwarding("LoadImg", &packet_handle, decode))
            .unwrap();
        // Decode is taken in with LoadImg or after it, at the latest with the followers.
        until_the_wait_table_holds(&mut engine, 1, "LoadImg's link");

        // The note waits at Decode.
        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        the_followers_arrive(&mut engine);

        // Decode forwards to Upload: the note moves on to Upload's slot, and Decode's is left
        // empty.
        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "Decode's forward");
        assert_eq!(
            engine.wait_table_len(),
            5,
            "the group, the note at Upload, and the three followers held"
        );

        until(&mut engine, Engine::check_tasks, "Upload");
        until(&mut engine, Engine::check_results, "Upload's result");

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        finish(engine, followers);
    }

    // One value, shared and freed once.

    /// Pixels that tell `freed` when they're dropped.
    struct Pixels {
        bytes: Vec<u8>,
        freed: Sender<()>,
    }

    impl Drop for Pixels {
        fn drop(&mut self) {
            let _ = self.freed.send(());
        }
    }

    #[test]
    fn the_followers_share_one_value_and_the_last_one_done_with_it_frees_it() {
        let dir = fresh_dir("fan_out_one_value_shared");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (address_tx, address_rx) = unbounded();
        let (freed_tx, freed_rx) = unbounded();
        let (started_tx, started_rx) = unbounded();
        let (go_tx, go_rx) = bounded::<()>(1);
        let load_img = AdvTask::new("LoadImg", 2, move || {
            Ok::<_, std::io::Error>(Pixels {
                bytes: vec![7; 16],
                freed: freed_tx,
            })
        })
        .unwrap();
        let load = packet_handle.send_adv_task(load_img).unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");
        until(&mut engine, Engine::check_results, "LoadImg's result");

        // Each follower tells where the pixels it reads are. Metadata, queued last, then holds
        // on to its share until the test lets it go on.
        let mut group = load.fan_out();
        let mut followers = Vec::new();
        for name in FOLLOWERS {
            let address_tx = address_tx.clone();
            let holding = (name == "Metadata").then(|| (started_tx.clone(), go_rx.clone()));
            let follower = group
                .then(
                    name,
                    move |share: Arc<Handoff<Pixels, Arc<std::io::Error>>>| {
                        let Handoff::Value(pixels) = &*share else {
                            unreachable!("LoadImg didn't fail");
                        };
                        let _ = address_tx.send(pixels.bytes.as_ptr() as usize);
                        if let Some((started, go)) = holding {
                            let _ = started.send(());
                            let _ = go.recv();
                        }
                        Ok::<_, std::io::Error>(0_u32)
                    },
                )
                .unwrap();
            followers.push(follower);
        }
        group.send_fanout().unwrap();
        until(&mut engine, Engine::check_status, "the group's data");
        the_followers_arrive(&mut engine);

        // All three read the same pixels: they were never copied.
        let addresses: Vec<usize> = (0..3)
            .map(|_| address_rx.recv_timeout(WAIT).expect("a follower never ran"))
            .collect();
        assert!(addresses.iter().all(|address| *address == addresses[0]));

        // Thumbnail and Compress are done, and the group went with the engine's share once all
        // three were queued. Metadata's share still holds the pixels.
        started_rx
            .recv_timeout(WAIT)
            .expect("Metadata never started");
        assert!(
            freed_rx.recv_timeout(A_MOMENT).is_err(),
            "the pixels were freed while Metadata still read them"
        );

        // Metadata is done: the last share goes, and the pixels with it, once.
        go_tx.send(()).unwrap();
        freed_rx
            .recv_timeout(WAIT)
            .expect("the pixels were never freed");
        drop(followers);
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(finalize_within(engine, WAIT).is_some());
        assert!(freed_rx.try_recv().is_err(), "the pixels were freed twice");
    }

    // A cancelled follower: it's skipped instead of queued, and the others still run.

    #[test]
    fn a_held_follower_cancelled_before_the_result_is_skipped_and_the_others_still_run() {
        let dir = fresh_dir("fan_out_a_held_follower_cancelled");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (go_tx, go_rx) = bounded(1);
        let load = packet_handle
            .send_adv_task(held_back("LoadImg", go_rx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");

        let mut followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        the_followers_arrive(&mut engine);
        followers.remove(0).cancel(); // Thumbnail
        until(&mut engine, Engine::check_status, "the cancel");

        // The converter runs: Thumbnail is skipped and reported as cancelled, and still counts
        // for the group, which goes once Compress and Metadata are queued.
        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "LoadImg's result");

        assert_eq!(told(&ran_rx, 2), each_read(&["Compress", "Metadata"], 16));
        assert!(ran_rx.recv_timeout(A_MOMENT).is_err(), "Thumbnail ran");
        finish(engine, followers);
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    // LoadImg's error, shared: written once, and only if no follower took it.

    #[test]
    fn load_img_s_error_reaches_every_held_follower_and_nothing_is_written() {
        let dir = fresh_dir("fan_out_an_error_reaches_every_follower");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (go_tx, go_rx) = bounded(1);
        let load = packet_handle
            .send_adv_task(failing_load_img(go_rx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");

        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        the_followers_arrive(&mut engine);

        // The converter runs on the error: its record goes under LoadImg's id, counting three,
        // and each follower queued gets a pointer to it. Each takes the error, and the first
        // report drops the record.
        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "LoadImg's error");

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 0));
        finish(engine, followers);
        assert_eq!(times_logged(&dir, MISSING), 0);
    }

    #[test]
    fn load_img_s_kept_error_reaches_followers_that_arrive_later_and_nothing_is_written() {
        let dir = fresh_dir("fan_out_a_kept_error_reaches_later_followers");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (go_tx, go_rx) = bounded(1);
        go_tx.send(()).unwrap();
        let load = packet_handle
            .send_adv_task(failing_load_img(go_rx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");
        until(&mut engine, Engine::check_results, "LoadImg's error");

        // The group's data runs the converter on the kept error. The record counts all three
        // followers, though none has arrived yet.
        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        assert_eq!(
            engine.wait_table_len(),
            2,
            "the group, and the error's record"
        );

        // Each follower is queued as it arrives, with a pointer to the record.
        the_followers_arrive(&mut engine);
        assert_eq!(
            engine.wait_table_len(),
            4,
            "the error's record, and its three pointers"
        );

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 0));
        finish(engine, followers);
        assert_eq!(times_logged(&dir, MISSING), 0);
    }

    #[test]
    fn held_followers_all_cancelled_have_load_img_s_error_written_once() {
        let dir = fresh_dir("fan_out_held_followers_all_cancelled");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (started_tx, started_rx) = bounded(1);
        let (go_tx, go_rx) = bounded(1);
        let load = packet_handle
            .send_adv_task(failing_load_img_once_started(started_tx, go_rx))
            .unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");
        started_rx
            .recv_timeout(WAIT)
            .expect("LoadImg never started");

        let followers = send_three_followers(load, &ran_tx);
        until(&mut engine, Engine::check_status, "the group's data");
        the_followers_arrive(&mut engine);
        for follower in followers {
            follower.cancel();
        }
        until(&mut engine, Engine::check_status, "the cancels");
        // The three, and LoadImg, which nobody needs any more. It's already running,
        // so it still fails, and its report takes its id out of the set.
        assert_eq!(engine.cancelled_tasks.len(), 4);

        // The converter runs on the error, and all three are skipped. None was queued to get a
        // pointer, so each takes one off the record directly: 3, 2, 1, 0, and the error is
        // written, since nobody took it.
        go_tx.send(()).unwrap();
        until(&mut engine, Engine::check_results, "LoadImg's error");

        assert!(ran_rx.recv_timeout(A_MOMENT).is_err(), "a follower ran");
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert_eq!(times_logged(&dir, MISSING), 1);
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn followers_all_cancelled_in_the_queue_have_load_img_s_error_written_once() {
        let dir = fresh_dir("fan_out_followers_all_cancelled_in_the_queue");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (row_1, followers) =
            three_queued_with_load_img_s_error(&mut engine, &packet_handle, &ran_tx);
        for follower in followers {
            follower.cancel();
        }
        until(&mut engine, Engine::check_status, "the cancels");
        assert_eq!(engine.cancelled_tasks.len(), 3);

        // The worker skips all three. Each cancelled report follows its pointer and takes one
        // off the record: 3, 2, 1, 0, and the error is written, once.
        row_1.send(()).unwrap();

        assert!(ran_rx.recv_timeout(A_MOMENT).is_err(), "a follower ran");
        run_until_the_wait_table_is_empty(&mut engine);
        assert!(engine.cancelled_tasks.is_empty());
        assert!(finalize_within(engine, WAIT).is_some());
        assert_eq!(times_logged(&dir, MISSING), 1);
        assert!(!error_log_mentions_a_cancel(&dir));
    }

    #[test]
    fn a_follower_cancelled_in_the_queue_while_the_others_take_the_error_writes_nothing() {
        let dir = fresh_dir("fan_out_one_cancelled_and_two_take_the_error");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (row_1, mut followers) =
            three_queued_with_load_img_s_error(&mut engine, &packet_handle, &ran_tx);
        followers.remove(0).cancel(); // Thumbnail
        until(&mut engine, Engine::check_status, "the cancel");

        // Thumbnail is skipped first: its report takes one off, 3 to 2, and the record stays,
        // since Compress and Metadata may still take the error. They do, so nothing is written.
        row_1.send(()).unwrap();

        assert_eq!(told(&ran_rx, 2), each_read(&["Compress", "Metadata"], 0));
        assert!(ran_rx.recv_timeout(A_MOMENT).is_err(), "Thumbnail ran");
        finish(engine, followers);
        assert_eq!(times_logged(&dir, MISSING), 0);
    }

    #[test]
    fn once_a_follower_took_the_error_the_cancelled_ones_write_nothing() {
        let dir = fresh_dir("fan_out_one_takes_the_error_and_two_are_cancelled");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (row_1, mut followers) =
            three_queued_with_load_img_s_error(&mut engine, &packet_handle, &ran_tx);
        followers.pop().unwrap().cancel(); // Metadata
        followers.pop().unwrap().cancel(); // Compress
        until(&mut engine, Engine::check_status, "the cancels");

        // Thumbnail runs first and takes the error: its report drops the record. Compress's and
        // Metadata's cancelled reports find nothing, so nothing is written.
        row_1.send(()).unwrap();

        assert_eq!(told(&ran_rx, 1), each_read(&["Thumbnail"], 0));
        assert!(
            ran_rx.recv_timeout(A_MOMENT).is_err(),
            "Compress or Metadata ran"
        );
        finish(engine, followers);
        assert_eq!(times_logged(&dir, MISSING), 0);
    }

    // A follower's handle, and a group dropped unsent.

    #[test]
    fn a_follower_s_handle_is_an_ordinary_handle_and_its_follow_up_gets_its_result() {
        let dir = fresh_dir("fan_out_a_follower_s_follow_up");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let (upload_tx, upload_rx) = unbounded();
        let load = packet_handle.send_adv_task(decoding("LoadImg")).unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");
        until(&mut engine, Engine::check_results, "LoadImg's result");

        // Upload follows Thumbnail. It's sent at once, before the group sends Thumbnail, and
        // waits in Thumbnail's slot, with a `WaitingOn` naming Thumbnail under its own id, while
        // Thumbnail itself is held under its own id.
        let mut group = load.fan_out();
        let mut followers = declare_three_followers(&mut group, &ran_tx);
        let upload = followers
            .remove(0)
            .then(
                "Upload",
                move |thumbnail: Handoff<usize, std::io::Error>| {
                    let _ = upload_tx.send(matches!(thumbnail, Handoff::Value(16)));
                    Ok::<_, std::io::Error>(())
                },
            )
            .unwrap();
        group.send_fanout().unwrap();
        the_followers_arrive(&mut engine);
        assert_eq!(
            engine.wait_table_len(),
            6,
            "LoadImg's result, Upload and its `WaitingOn`, and the three followers held"
        );
        until(&mut engine, Engine::check_status, "the group's data");

        // Thumbnail's result reaches Upload as the engine runs on.
        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        drop(upload);
        finish(engine, followers);
        assert!(
            upload_rx.try_recv().expect("Upload never ran"),
            "Upload didn't get Thumbnail's 16 pixels"
        );
    }

    #[test]
    fn a_group_dropped_without_send_fanout_still_runs_its_followers() {
        let dir = fresh_dir("fan_out_a_group_dropped_unsent");
        let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
        let (ran_tx, ran_rx) = unbounded();
        let load = packet_handle.send_adv_task(decoding("LoadImg")).unwrap();
        until(&mut engine, Engine::check_tasks, "LoadImg");
        until(&mut engine, Engine::check_results, "LoadImg's result");

        // Dropped unsent, the group is sent anyway, as `send_fanout()` would send it.
        let mut group = load.fan_out();
        let followers = declare_three_followers(&mut group, &ran_tx);
        drop(group);
        the_followers_arrive(&mut engine);
        until(&mut engine, Engine::check_status, "the group's data");

        assert_eq!(told(&ran_rx, 3), each_read(&FOLLOWERS, 16));
        finish(engine, followers);
    }

    // Cancels: a cancelled task stops the work that only it needed, and so on up.

    mod cancels {
        use std::path::Path;

        use thryve_utils::traits::join::Joinable;

        use super::*;

        /// A task named `name` that tells `ran` its name when it runs, and returns 0.
        fn telling(name: &'static str, ran: &Sender<&'static str>) -> AdvTask<u32, std::io::Error> {
            let ran = ran.clone();
            AdvTask::new(name, 2, move || {
                let _ = ran.send(name);
                Ok(0)
            })
            .unwrap()
        }

        /// Like [`forwarding_when_let`], but it tells `started` once a worker runs it, so a test
        /// can be sure it's running before cancelling everything that needs it.
        fn forwarding_once_started(
            name: &str,
            sender: &PacketHandle,
            to: AdvTask<Vec<u8>, std::io::Error>,
            started: Sender<()>,
            go: Receiver<()>,
        ) -> AdvTask<Vec<u8>, std::io::Error> {
            let sender = sender.clone();
            AdvTask::forward(name, 1, move || {
                let _ = started.send(());
                let _ = go.recv();
                Ok(sender.send_adv_task(to).unwrap())
            })
            .unwrap()
        }

        /// Runs the engine until both tables are empty, checks that nothing told `ran` it ran and
        /// that every id left the set, and finalizes the engine. `Error.log` mustn't mention a
        /// cancel.
        fn ends_with_nothing_run<X>(mut engine: Engine, dir: &Path, ran: &Receiver<X>) {
            run_until_the_wait_table_is_empty(&mut engine);
            assert!(ran.try_recv().is_err(), "a cancelled task ran");
            assert!(engine.cancelled_tasks.is_empty(), "an id stayed in the set");
            assert!(finalize_within(engine, WAIT).is_some());
            assert!(!error_log_mentions_a_cancel(dir));
        }

        // Plain follow-ups: a cancelled follow-up also stops the task it waits for.

        #[test]
        fn cancelling_the_end_of_a_chain_stops_every_task_above_it_that_has_not_started() {
            let dir = fresh_dir("fan_out_cancels_the_end_of_a_chain");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();

            // Row 1 holds the only worker, so Hash waits in the queue, and LoadImg, Resize and
            // Save wait in the engine, each in the slot of the task before it.
            let go_tx = holding_the_worker(&mut engine, &packet_handle);
            let save = packet_handle
                .send_adv_task(telling("Hash", &ran_tx))
                .unwrap()
                .then("LoadImg", telling_follow_up("LoadImg", &ran_tx))
                .unwrap()
                .then("Resize", telling_follow_up("Resize", &ran_tx))
                .unwrap()
                .then("Save", telling_follow_up("Save", &ran_tx))
                .unwrap();
            until(&mut engine, Engine::check_tasks, "the chain");

            // Each task fed only the one after it, so Save's cancel goes all the way up.
            save.cancel();
            until(&mut engine, Engine::check_status, "the cancel");
            assert_eq!(engine.cancelled_tasks.len(), 4);

            // The worker skips Hash, and each cancelled report skips the next task down.
            go_tx.send(()).unwrap();
            ends_with_nothing_run(engine, &dir, &ran_rx);
        }

        #[test]
        fn a_follow_up_cancelled_before_the_engine_takes_it_in_still_stops_its_producer() {
            let dir = fresh_dir("fan_out_cancels_before_the_follow_up_arrives");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let go_tx = holding_the_worker(&mut engine, &packet_handle);
            let hash = packet_handle
                .send_adv_task(telling("Hash", &ran_tx))
                .unwrap();
            until(&mut engine, Engine::check_tasks, "Hash");

            // Each turn reads the status mailbox first, so LoadImg's cancel comes before LoadImg.
            // Nothing waits under LoadImg's id yet, so only LoadImg goes into the set.
            let load = hash
                .then("LoadImg", telling_follow_up("LoadImg", &ran_tx))
                .unwrap();
            load.cancel();
            until(&mut engine, Engine::check_status, "the cancel");
            assert_eq!(engine.cancelled_tasks.len(), 1);

            // LoadImg arrives cancelled. It waits in Hash's slot like any follow-up, and is then
            // withdrawn, so Hash, which only fed it, goes into the set too.
            until(&mut engine, Engine::check_tasks, "LoadImg");
            assert_eq!(engine.cancelled_tasks.len(), 2);

            go_tx.send(()).unwrap();
            ends_with_nothing_run(engine, &dir, &ran_rx);
        }

        #[test]
        fn after_a_forward_a_cancelled_follow_up_stops_the_task_forwarded_to() {
            let dir = fresh_dir("fan_out_cancels_after_a_forward");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let (go_tx, go_rx) = bounded(1);
            let (answer_go_tx, answer_go_rx) = bounded(1);

            // Upload waits for Process, which forwards to Answer once it's let go. Answer is
            // held back too.
            let upload = packet_handle
                .send_adv_task(forwarding_when_let(
                    "Process",
                    &packet_handle,
                    held_back("Answer", answer_go_rx),
                    go_rx,
                ))
                .unwrap()
                .then("Upload", move |_: Handoff<Vec<u8>, std::io::Error>| {
                    let _ = ran_tx.send("Upload");
                    Ok::<(), std::io::Error>(())
                })
                .unwrap();
            until(&mut engine, Engine::check_tasks, "Process and Upload");

            // The forward moves Upload into Answer's slot, and the `WaitingOn` under Upload's id
            // now names Answer.
            go_tx.send(()).unwrap();
            until(&mut engine, Engine::check_results, "Process's forward");
            until(&mut engine, Engine::check_tasks, "Answer");

            // Upload's cancel reaches Answer, not Process: Process has already reported, so its
            // id would stay in the set for good.
            upload.cancel();
            until(&mut engine, Engine::check_status, "the cancel");
            assert_eq!(engine.cancelled_tasks.len(), 2);

            // Answer finishes if it had started, and is skipped if not. Upload is skipped either
            // way.
            let _ = answer_go_tx.send(());
            ends_with_nothing_run(engine, &dir, &ran_rx);
        }

        // A group's followers: the task the group follows stops once every follower is cancelled.

        #[test]
        fn cancelling_every_held_follower_stops_load_img_while_it_waits_in_the_queue() {
            let dir = fresh_dir("fan_out_cancels_every_held_follower");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();

            // Row 1 holds the only worker, so LoadImg waits in the queue, and its three followers
            // are held under their own ids.
            let go_tx = holding_the_worker(&mut engine, &packet_handle);
            let load = packet_handle.send_adv_task(decoding("LoadImg")).unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");
            let followers = send_three_followers(load, &ran_tx);
            until(&mut engine, Engine::check_status, "the group's data");
            the_followers_arrive(&mut engine);

            // Each cancel takes one follower off the live list. The third empties it, so LoadImg,
            // which nobody needs any more, goes into the set too.
            for follower in followers {
                follower.cancel();
            }
            until(&mut engine, Engine::check_status, "the cancels");
            assert_eq!(engine.cancelled_tasks.len(), 4);

            // The worker skips LoadImg, and its cancelled report reaches the group: the converter
            // is dropped, and the three held followers are skipped.
            go_tx.send(()).unwrap();
            ends_with_nothing_run(engine, &dir, &ran_rx);
        }

        #[test]
        fn cancelling_every_follower_also_stops_the_task_load_img_waits_for() {
            let dir = fresh_dir("fan_out_cancels_up_past_load_img");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let (above_tx, above_rx) = unbounded();

            // Row 1 holds the only worker, so Hash waits in the queue, LoadImg follows Hash and
            // waits in its slot, and LoadImg's three followers are held.
            let go_tx = holding_the_worker(&mut engine, &packet_handle);
            let load = packet_handle
                .send_adv_task(telling("Hash", &above_tx))
                .unwrap()
                .then("LoadImg", move |_: Handoff<u32, std::io::Error>| {
                    let _ = above_tx.send("LoadImg");
                    Ok::<Vec<u8>, std::io::Error>(vec![7; 16])
                })
                .unwrap();
            until(&mut engine, Engine::check_tasks, "Hash and LoadImg");
            let followers = send_three_followers(load, &ran_tx);
            until(&mut engine, Engine::check_status, "the group's data");
            the_followers_arrive(&mut engine);

            // The third cancel empties the live list, so LoadImg is cancelled, and the check goes
            // on up: Hash, which only LoadImg needed, is cancelled too.
            for follower in followers {
                follower.cancel();
            }
            until(&mut engine, Engine::check_status, "the cancels");
            assert_eq!(engine.cancelled_tasks.len(), 5);

            // The worker skips Hash. Its cancelled report skips LoadImg, whose own reaches the
            // group, and the three held followers are skipped.
            go_tx.send(()).unwrap();
            ends_with_nothing_run(engine, &dir, &ran_rx);
            assert!(above_rx.try_recv().is_err(), "Hash or LoadImg ran");
        }

        #[test]
        fn followers_cancelled_before_the_group_s_data_leave_load_img_with_nobody() {
            let dir = fresh_dir("fan_out_cancels_before_the_group_s_data");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let go_tx = holding_the_worker(&mut engine, &packet_handle);
            let load = packet_handle.send_adv_task(decoding("LoadImg")).unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");

            // The three are cancelled before the group is sent, so their cancels come before the
            // group's data. Its live list starts empty, and LoadImg is cancelled at once.
            let mut group = load.fan_out();
            for follower in declare_three_followers(&mut group, &ran_tx) {
                follower.cancel();
            }
            group.send_fanout().unwrap();
            until(
                &mut engine,
                Engine::check_status,
                "the cancels and the group's data",
            );
            assert_eq!(engine.cancelled_tasks.len(), 4);

            // The followers arrive cancelled and are held. They were never on the live list, so
            // withdrawing them changes nothing.
            the_followers_arrive(&mut engine);
            assert_eq!(engine.cancelled_tasks.len(), 4);

            go_tx.send(()).unwrap();
            ends_with_nothing_run(engine, &dir, &ran_rx);
        }

        #[test]
        fn followers_cancelled_after_the_group_s_data_but_before_they_arrive_still_stop_load_img() {
            let dir = fresh_dir("fan_out_cancels_before_the_followers_arrive");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let go_tx = holding_the_worker(&mut engine, &packet_handle);
            let load = packet_handle.send_adv_task(decoding("LoadImg")).unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");

            // The group is sent, and the three are cancelled at once. The turn reads the group's
            // data and the cancels before the followers: nothing is held yet, so nothing is
            // withdrawn, and only the three go into the set.
            for follower in send_three_followers(load, &ran_tx) {
                follower.cancel();
            }
            until(
                &mut engine,
                Engine::check_status,
                "the group's data and the cancels",
            );
            assert_eq!(engine.cancelled_tasks.len(), 3);

            // Each follower is withdrawn once it's held. The third empties the live list, and
            // LoadImg is cancelled.
            the_followers_arrive(&mut engine);
            assert_eq!(engine.cancelled_tasks.len(), 4);

            go_tx.send(()).unwrap();
            ends_with_nothing_run(engine, &dir, &ran_rx);
        }

        #[test]
        fn a_follower_that_arrives_after_its_group_was_cancelled_is_skipped_at_once() {
            let dir = fresh_dir("fan_out_cancels_a_follower_after_its_group");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let go_tx = holding_the_worker(&mut engine, &packet_handle);
            let load = packet_handle.send_adv_task(decoding("LoadImg")).unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");

            // As above, the cancels come before the group's data, which cancels LoadImg.
            let mut group = load.fan_out();
            for follower in declare_three_followers(&mut group, &ran_tx) {
                follower.cancel();
            }
            group.send_fanout().unwrap();
            until(
                &mut engine,
                Engine::check_status,
                "the cancels and the group's data",
            );

            // LoadImg's cancelled report reaches the group before its followers do: the converter
            // is dropped, so no share will ever come.
            go_tx.send(()).unwrap();
            until(
                &mut engine,
                |engine| {
                    engine.check_results();
                    engine.cancelled_tasks.len() == 3
                },
                "LoadImg's cancelled report",
            );

            // Each follower is skipped as it arrives, instead of held, and the group goes with
            // the third.
            the_followers_arrive(&mut engine);
            assert_eq!(engine.wait_table_len(), 0, "a follower was held");

            ends_with_nothing_run(engine, &dir, &ran_rx);
        }

        #[test]
        fn a_forward_made_after_every_follower_was_cancelled_stops_the_task_forwarded_to() {
            let dir = fresh_dir("fan_out_cancels_then_a_forward");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let (started_tx, started_rx) = bounded(1);
            let (go_tx, go_rx) = bounded(1);

            // LoadImg runs, and forwards to Decode once it's let go.
            let load = packet_handle
                .send_adv_task(forwarding_once_started(
                    "LoadImg",
                    &packet_handle,
                    decoding("Decode"),
                    started_tx,
                    go_rx,
                ))
                .unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");
            started_rx
                .recv_timeout(WAIT)
                .expect("LoadImg never started");
            let followers = send_three_followers(load, &ran_tx);
            until(&mut engine, Engine::check_status, "the group's data");
            the_followers_arrive(&mut engine);

            // All three are cancelled. LoadImg goes into the set, but a running task isn't
            // stopped.
            for follower in followers {
                follower.cancel();
            }
            until(&mut engine, Engine::check_status, "the cancels");
            assert_eq!(engine.cancelled_tasks.len(), 4);

            // LoadImg forwards, and its report takes its id out of the set. The group now waits
            // for Decode, which nobody needs either, so Decode goes into the set before the
            // engine even takes it in.
            go_tx.send(()).unwrap();
            until(&mut engine, Engine::check_results, "LoadImg's forward");
            assert_eq!(engine.cancelled_tasks.len(), 4);

            // The worker skips Decode, and its cancelled report reaches the group through the
            // note: the three held followers are skipped.
            ends_with_nothing_run(engine, &dir, &ran_rx);
        }

        #[test]
        fn cancelling_a_held_follower_s_follow_up_withdraws_the_follower_and_the_others_still_run()
        {
            let dir = fresh_dir("fan_out_cancels_a_follower_s_follow_up");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let (upload_ran_tx, upload_ran_rx) = unbounded();
            let (go_tx, go_rx) = bounded(1);
            let load = packet_handle
                .send_adv_task(held_back("LoadImg", go_rx))
                .unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");

            // Upload follows Thumbnail, and only Upload needs Thumbnail's result.
            let mut group = load.fan_out();
            let mut followers = declare_three_followers(&mut group, &ran_tx);
            let upload = followers
                .remove(0)
                .then("Upload", move |_: Handoff<usize, std::io::Error>| {
                    let _ = upload_ran_tx.send(());
                    Ok::<(), std::io::Error>(())
                })
                .unwrap();
            group.send_fanout().unwrap();
            the_followers_arrive(&mut engine);
            until(&mut engine, Engine::check_status, "the group's data");

            // Upload's cancel goes up to Thumbnail, which comes off the live list. Compress and
            // Metadata still need LoadImg, so the cancel stops there.
            upload.cancel();
            until(&mut engine, Engine::check_status, "the cancel");
            assert_eq!(engine.cancelled_tasks.len(), 2);

            // The converter runs: Thumbnail is skipped, and Upload with it.
            go_tx.send(()).unwrap();
            until(&mut engine, Engine::check_results, "LoadImg's result");
            assert_eq!(told(&ran_rx, 2), each_read(&["Compress", "Metadata"], 16));

            drop(followers);
            run_until_the_wait_table_is_empty(&mut engine);
            assert!(ran_rx.try_recv().is_err(), "Thumbnail ran");
            assert!(upload_ran_rx.try_recv().is_err(), "Upload ran");
            assert!(engine.cancelled_tasks.is_empty());
            assert!(finalize_within(engine, WAIT).is_some());
            assert!(!error_log_mentions_a_cancel(&dir));
        }

        #[test]
        fn a_cancelled_join_over_two_followers_withdraws_both_and_load_img_runs_for_the_third() {
            let dir = fresh_dir("fan_out_cancels_a_join_over_two_followers");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let (save_ran_tx, save_ran_rx) = unbounded();
            let (go_tx, go_rx) = bounded(1);
            let load = packet_handle
                .send_adv_task(held_back("LoadImg", go_rx))
                .unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");

            // Save joins Thumbnail and Compress, and Metadata stands alone.
            let mut group = load.fan_out();
            let mut followers = declare_three_followers(&mut group, &ran_tx);
            let thumbnail_and_compress: Vec<Follower> = followers.drain(..2).collect();
            let save = thumbnail_and_compress
                .join("Save", move |_| {
                    let _ = save_ran_tx.send(());
                    Ok::<(), std::io::Error>(())
                })
                .unwrap();
            group.send_fanout().unwrap();
            the_followers_arrive(&mut engine);
            until(&mut engine, Engine::check_status, "the group's data");

            // Save is a join: its cancel cancels the two followers it waits for, and each comes
            // off the live list. Metadata still needs LoadImg.
            save.cancel();
            until(&mut engine, Engine::check_status, "the cancel");
            assert_eq!(engine.cancelled_tasks.len(), 2);

            // The converter runs: Thumbnail and Compress are skipped, and Metadata runs.
            go_tx.send(()).unwrap();
            until(&mut engine, Engine::check_results, "LoadImg's result");
            assert_eq!(told(&ran_rx, 1), each_read(&["Metadata"], 16));

            drop(followers);
            run_until_the_wait_table_is_empty(&mut engine);
            assert!(ran_rx.try_recv().is_err(), "Thumbnail or Compress ran");
            assert!(save_ran_rx.try_recv().is_err(), "Save ran");
            assert!(engine.cancelled_tasks.is_empty());
            assert!(finalize_within(engine, WAIT).is_some());
            assert!(!error_log_mentions_a_cancel(&dir));
        }

        #[test]
        fn load_img_s_error_found_with_no_follower_live_is_still_written_once() {
            let dir = fresh_dir("fan_out_cancels_then_the_group_s_data_finds_an_error");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            let (ran_tx, ran_rx) = unbounded();
            let (go_tx, go_rx) = bounded(1);
            go_tx.send(()).unwrap();
            let load = packet_handle
                .send_adv_task(failing_load_img(go_rx))
                .unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");
            until(&mut engine, Engine::check_results, "LoadImg's error");

            // The three are cancelled before the group is sent. The group's data finds LoadImg's
            // error kept and no follower live: LoadImg has already reported, so there's nothing to
            // stop, and the converter runs as usual, with the error's record.
            let mut group = load.fan_out();
            for follower in declare_three_followers(&mut group, &ran_tx) {
                follower.cancel();
            }
            group.send_fanout().unwrap();
            until(
                &mut engine,
                Engine::check_status,
                "the cancels and the group's data",
            );
            assert_eq!(engine.cancelled_tasks.len(), 3);

            // Each follower is queued with the share and a pointer, and the worker skips it. Each
            // cancelled report takes one off the record: 3, 2, 1, 0, and the error is written
            // once, since nobody took it.
            the_followers_arrive(&mut engine);
            ends_with_nothing_run(engine, &dir, &ran_rx);
            assert_eq!(times_logged(&dir, MISSING), 1);
        }
    }

    // The shutdown: a held follower is a task that never ran, and an error's record is one lost
    // result, however many followers carry it.

    mod the_shutdown {
        use crate::shutdown_and_startup::{read_log, summary_line};

        use super::*;

        #[test]
        fn followers_still_held_at_the_shutdown_are_tasks_that_never_ran_unless_cancelled() {
            let dir = fresh_dir("fan_out_shutdown_held_followers");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            engine.config.debugging.is_debugging = true;
            let (ran_tx, ran_rx) = unbounded();
            let load = packet_handle.send_adv_task(decoding("LoadImg")).unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");
            until(&mut engine, Engine::check_results, "LoadImg's result");

            // Compress is cancelled before the group is sent. The followers arrive and are held,
            // and the shutdown starts before the engine reads the group's data, which it then
            // drops.
            let mut group = load.fan_out();
            let mut followers = declare_three_followers(&mut group, &ran_tx);
            followers.remove(1).cancel(); // Compress
            until(&mut engine, Engine::check_status, "the cancel");
            group.send_fanout().unwrap();
            the_followers_arrive(&mut engine);

            assert!(finalize_within(engine, WAIT).is_some());
            drop(followers);
            assert!(ran_rx.try_recv().is_err(), "a follower ran");

            // Thumbnail and Metadata never ran: lost work. Compress was cancelled: not lost.
            let errors = read_log(&dir, "Error.log");
            assert!(errors.contains(&summary_line(2, 0, 0, 0)), "{errors}");
            let debug = read_log(&dir, "Debug.log");
            assert!(debug.contains("Thumbnail: 1"), "{debug}");
            assert!(debug.contains("Metadata: 1"), "{debug}");
            assert!(!debug.contains("Compress"), "{debug}");
        }

        #[test]
        fn an_error_s_record_is_one_lost_result_however_many_followers_carry_it() {
            let dir = fresh_dir("fan_out_shutdown_one_record_three_pointers");
            // Row 1 holds the only worker through the shutdown, which gives up on it after a
            // second.
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            engine.config.debugging.is_debugging = true;
            let (ran_tx, ran_rx) = unbounded();
            let (row_1, followers) =
                three_queued_with_load_img_s_error(&mut engine, &packet_handle, &ran_tx);

            // The shutdown takes the three out of the queue. Each carries a pointer to the one
            // record, and the error counts once.
            assert!(finalize_within(engine, WAIT).is_some());
            drop(row_1);
            drop(followers);
            assert!(ran_rx.try_recv().is_err(), "a follower ran");

            let errors = read_log(&dir, "Error.log");
            assert!(errors.contains(&summary_line(3, 0, 1, 1)), "{errors}");
            assert!(!errors.contains(MISSING), "{errors}");
            let debug = read_log(&dir, "Debug.log");
            assert!(debug.contains("errors never taken: 1"), "{debug}");
            assert_eq!(debug.matches(MISSING).count(), 1, "{debug}");
        }

        #[test]
        fn an_error_s_record_whose_followers_never_arrived_is_one_lost_result() {
            let dir = fresh_dir("fan_out_shutdown_a_record_with_no_follower");
            let (mut engine, packet_handle) = started_engine(&dir, 10, 1, 1);
            engine.config.debugging.is_debugging = true;
            let (ran_tx, ran_rx) = unbounded();
            let (go_tx, go_rx) = bounded(1);
            go_tx.send(()).unwrap();
            let load = packet_handle
                .send_adv_task(failing_load_img(go_rx))
                .unwrap();
            until(&mut engine, Engine::check_tasks, "LoadImg");
            until(&mut engine, Engine::check_results, "LoadImg's error");

            // The group's data runs the converter on the kept error, and the record counts all
            // three followers. The shutdown starts before any of them arrives.
            let followers = send_three_followers(load, &ran_tx);
            until(&mut engine, Engine::check_status, "the group's data");

            assert!(finalize_within(engine, WAIT).is_some());
            drop(followers);
            assert!(ran_rx.try_recv().is_err(), "a follower ran");

            // The three never ran, and nobody took the error: one lost result, though no follower
            // carries a pointer to it.
            let errors = read_log(&dir, "Error.log");
            assert!(errors.contains(&summary_line(3, 0, 1, 0)), "{errors}");
            let debug = read_log(&dir, "Debug.log");
            assert!(debug.contains("errors never taken: 1"), "{debug}");
            assert_eq!(debug.matches(MISSING).count(), 1, "{debug}");
        }
    }
}
