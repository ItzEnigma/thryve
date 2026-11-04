// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! The engine's wait table: where a typed task's follow-up and its result meet.
//!
//! The follow-up, the result and the release of a dropped handle reach the engine through
//! different mailboxes, so they can arrive in any order. Under the task's id, the table remembers
//! whichever came first until the other side arrives. A task that forwarded leaves a link to the
//! task forwarded to; links can chain into a list, which the follow-up or the release follows to
//! its end. A join waits on several tasks: each of them gets the join's follower, which leads to
//! the join in a second table of the same kind, keyed by the join's own id, and which moves along
//! a forward like a follow-up does. A fan-out group is several followers sharing one task's
//! result: the group waits in that task's slot, and its followers that arrive before the share
//! is there wait in the second table, under their own ids. If the task forwarded, a note at the
//! end of the links leads the result back to the group, and moves along a forward like a join's
//! follower. What the entries of both tables can be is in
//! [`engine_task_states`](crate::runtime::engine_task_states).
//!
//! It's a plain store: the engine decides what each arrival means (see
//! [`Engine::accept_task`](crate::runtime::engine::Engine::accept_task),
//! [`Engine::handle_result`](crate::runtime::engine::Engine::handle_result) and
//! [`Engine::check_status`](crate::runtime::engine::Engine::check_status)).
//!
//! Only the engine thread uses it.

use std::{any::Any, collections::HashMap, sync::Arc};

use thryve_utils::{errors::ThryveError, packets::task::Task, res::SealedHandoff};

use crate::runtime::engine_task_states::{TrackedState, WaitingState};

/// A store keyed by task id, used twice by the engine.
///
/// As `WaitTable<WaitingState>`, the wait table: one entry per typed task whose follow-up or
/// result is waiting for the other side, whose forward hasn't been followed yet, or whose result
/// a join or a fan-out group waits for. As `WaitTable<TrackedState>`, what the engine keeps about
/// a task under the task's own id: the joins still waiting for some of their tasks, the fan-out
/// followers held until their share is there, and the error texts kept for tasks queued with an
/// error as their input, under the id of the task that produced it, with a pointer under the id
/// of each task that carries it until that task reports. An entry goes once both sides have
/// arrived, so no table grows.
pub struct WaitTable<T> {
    /// The entries, by task id. The shutdown empties it directly.
    pub table: HashMap<u64, T>,
}

impl<T> Default for WaitTable<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> WaitTable<T> {
    /// An empty table.
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    /// Stores `state` under `id`, unless the id already has an entry: then the table keeps that
    /// one, and `state` is dropped. The engine checks for an entry first.
    pub fn store(&mut self, id: u64, state: T) {
        self.table.entry(id).or_insert(state);
    }

    /// Takes out the entry for `id`, if there is one.
    pub fn take(&mut self, id: u64) -> Option<T> {
        self.table.remove(&id)
    }

    /// The entry for `id`, left in place to be changed, if there is one.
    pub fn get_mut(&mut self, id: u64) -> Option<&mut T> {
        self.table.get_mut(&id)
    }

    /// The entry for `id`, left in place, if there is one.
    pub fn get(&self, id: u64) -> Option<&T> {
        self.table.get(&id)
    }
}

impl WaitTable<WaitingState> {
    /// Takes out the entry for `wait_id`, or returns [`WaitingState::None`] if there's none.
    pub fn remove(&mut self, wait_id: u64) -> WaitingState {
        if let Some(state) = self.table.remove(&wait_id) {
            state
        } else {
            WaitingState::None
        }
    }

    /// The entry for `wait_id`, left in place, or [`WaitingState::None`] if there's none.
    pub fn get_as_ref(&self, wait_id: u64) -> &WaitingState {
        if let Some(state) = self.table.get(&wait_id) {
            state
        } else {
            &WaitingState::None
        }
    }

    /// Follows the links from the task `current_id` to the end of the list, removing each, and
    /// takes out the entry at the end too. Returns the id at the end and its entry, which is
    /// [`WaitingState::None`] if there's nothing there. With no link at `current_id`, that's
    /// `current_id` and its own entry.
    ///
    /// It always ends, even on a cycle, since every link is removed as it's passed.
    pub fn remove_while_is_link(&mut self, mut current_id: u64) -> (u64, WaitingState) {
        let mut wait_state = self.remove(current_id);

        while wait_state.is_link() {
            if let WaitingState::Link { next_id } = wait_state {
                current_id = next_id;
                wait_state = self.remove(current_id)
            }
        }

        (current_id, wait_state)
    }

    /// Follows a forward that reached the fan-out group of the task `pre_fanout_task_id`: the
    /// plain walk from the task `forwarded_id`, then, if the end of the links holds nothing (the
    /// task there is still running), a note there that leads its result back to the group. Its
    /// own slot is the end if it hasn't forwarded. The group now waits for that task, so its id
    /// goes into the group too: a cancel that leaves no follower live stops that task. Returns what
    /// the end held: a kept result means that task has already finished, and the group's converter
    /// can run on it now.
    pub fn fanout_group_remove_while_is_link(
        &mut self,
        pre_fanout_task_id: u64,
        forwarded_id: u64,
    ) -> WaitingState {
        let (last_id, waiting_state) = self.remove_while_is_link(forwarded_id);

        if waiting_state.is_none() {
            self.store(last_id, WaitingState::BackwardsLink { pre_fanout_task_id });

            if let Some(WaitingState::FanoutGroup { waits_for_id, .. }) =
                self.get_mut(pre_fanout_task_id)
            {
                *waits_for_id = last_id;
            }
        }

        waiting_state
    }

    /// Remembers that the handle of the task `wait_id` was dropped before the task's result
    /// arrived.
    pub fn release(&mut self, wait_id: u64) {
        self.store(wait_id, WaitingState::Released);
    }

    /// Stores a follow-up that waits for the result of the task `wait_id`.
    pub fn insert_follow_up(&mut self, wait_id: u64, task: Task) {
        self.store(wait_id, WaitingState::FollowUP(task));
    }

    /// Keeps the result of the task `wait_id` for a follow-up that may still be declared.
    pub fn insert_handoff(&mut self, wait_id: u64, sealed_handoff: SealedHandoff) {
        self.store(wait_id, WaitingState::HandoffKept(sealed_handoff));
    }

    /// Stores that the task `wait_id` forwarded to the task `next_id`, for a follow-up or a
    /// release still to come.
    pub fn insert_link(&mut self, wait_id: u64, next_id: u64) {
        self.store(wait_id, WaitingState::Link { next_id });
    }

    /// Takes out the kept result of the task `wait_id`. The entry is removed even when it holds
    /// something else, and that gives `None`.
    pub fn take_sealed_handoff(&mut self, wait_id: u64) -> Option<SealedHandoff> {
        match self.remove(wait_id) {
            WaitingState::HandoffKept(sealed_handoff) => Some(sealed_handoff),
            _ => None,
        }
    }

    /// Takes out the follow-up waiting for the task `wait_id`. The entry is removed even when it
    /// holds something else, and that gives `None`.
    pub fn take_task(&mut self, wait_id: u64) -> Option<Task> {
        match self.remove(wait_id) {
            WaitingState::FollowUP(task) => Some(task),
            _ => None,
        }
    }

    /// Runs the converter of the fan-out group waiting on the task `wait_id` on that task's
    /// result, and keeps what it returns as the group's share. The converter is taken out first,
    /// so it runs at most once; with no group, or a converter already used, nothing happens.
    ///
    /// # Errors
    ///
    /// The converter's own error, if the result isn't of the type the group was made for, which
    /// only a bug in Thryve can cause. The share then stays empty.
    pub fn put_group_share(
        &mut self,
        wait_id: u64,
        sealed_handoff: SealedHandoff,
    ) -> Result<(), ThryveError> {
        if let Some(WaitingState::FanoutGroup {
            converter,
            pre_fanout_task_result_share,
            ..
        }) = self.get_mut(wait_id)
            && let Some(converter) = std::mem::take(converter)
        {
            *pre_fanout_task_result_share = Some(converter(sealed_handoff)?);
        }

        Ok(())
    }

    /// A clone of the share of the fan-out group waiting on the task `wait_id`, for one follower.
    /// `None` if there's no group, or if its converter hasn't run yet.
    pub fn get_group_share(&self, wait_id: u64) -> Option<Arc<dyn Any + Send + Sync>> {
        if let WaitingState::FanoutGroup {
            pre_fanout_task_result_share,
            ..
        } = self.get_as_ref(wait_id)
        {
            return pre_fanout_task_result_share.clone();
        }

        None
    }

    /// Takes the followers' ids out of the fan-out group waiting on the task `wait_id`, leaving
    /// the group in place. They're needed once, after the converter has run or once the group is
    /// cancelled, to find the followers held under them.
    pub fn take_fanout_tasks_ids(&mut self, wait_id: u64) -> Option<Vec<u64>> {
        if let Some(wait_state) = self.get_mut(wait_id)
            && let WaitingState::FanoutGroup {
                fanout_tasks_ids, ..
            } = wait_state
        {
            return Some(std::mem::take(fanout_tasks_ids));
        }

        None
    }

    /// How many followers the fan-out group waiting on the task `wait_id` has, from its ids,
    /// which it reads without taking. Before the ids are taken out, that's the number in the
    /// group's data.
    pub fn get_fanout_tasks_len(&self, wait_id: u64) -> Option<usize> {
        if let Some(wait_state) = self.get(wait_id)
            && let WaitingState::FanoutGroup {
                fanout_tasks_ids, ..
            } = wait_state
        {
            return Some(fanout_tasks_ids.len());
        }

        None
    }

    /// Takes the follower `wait_id`, which was cancelled while held, off the live list of the
    /// fan-out group waiting on the task `pre_fanout_task_id`. Returns the task the group waits
    /// for if that emptied the list: nobody wants its result any more, so it's the next to
    /// cancel. Say Thumbnail, Compress and Metadata wait on LoadImg: the third cancel returns
    /// LoadImg, or Decode once LoadImg has forwarded to it.
    ///
    /// Only the call that empties the list returns an id, so the task is cancelled once, and a
    /// follower that isn't on the list (it was cancelled before the group's data arrived) changes
    /// nothing. Once the converter is gone, the task has reported, and nothing changes either.
    pub fn fanned_out_task_abort(&mut self, pre_fanout_task_id: u64, wait_id: u64) -> Option<u64> {
        if let Some(WaitingState::FanoutGroup {
            waits_for_id,
            live_fanout_tasks_ids,
            converter,
            ..
        }) = self.get_mut(pre_fanout_task_id)
            && converter.is_some()
            && live_fanout_tasks_ids.remove(&wait_id)
            && live_fanout_tasks_ids.is_empty()
        {
            return Some(*waits_for_id);
        }

        None
    }

    /// The task the fan-out group waiting on the task `pre_fanout_task_id` waits for, if no
    /// follower is live while the converter is still there: nobody wants that task's result. It
    /// changes nothing. After a forward, that's the task at the end of the forwards, not the task
    /// the group follows.
    ///
    /// For the two moments when the list is already empty and a new task to cancel appears: the
    /// group's data arrives after every follower was cancelled, and the task the group waits for
    /// forwards after that.
    pub fn pre_fanout_task_to_cancel(&mut self, pre_fanout_task_id: u64) -> Option<u64> {
        if let Some(WaitingState::FanoutGroup {
            waits_for_id,
            live_fanout_tasks_ids,
            converter,
            ..
        }) = self.get_mut(pre_fanout_task_id)
            && converter.is_some()
            && live_fanout_tasks_ids.is_empty()
        {
            return Some(*waits_for_id);
        }

        None
    }

    /// Drops the converter of the fan-out group waiting on the task `pre_fanout_task_id`, whose
    /// cancelled report reached the group: there will never be a result to share. The rest of
    /// the group stays, so that followers still to arrive find it and are skipped, and it goes
    /// once all of them have been.
    pub fn fanout_group_drop_converter(&mut self, pre_fanout_task_id: u64) {
        if let Some(WaitingState::FanoutGroup { converter, .. }) = self.get_mut(pre_fanout_task_id)
            && converter.is_some()
        {
            drop(std::mem::take(converter));
        }
    }

    /// Whether the fan-out group waiting on the task `pre_fanout_task_id` can still give a
    /// share. Only a group with neither a converter nor a share can't: its task was cancelled, or
    /// its converter failed. With no group yet, the group's data may still come, so it can.
    pub fn can_fanout_group_get_share(&mut self, pre_fanout_task_id: u64) -> bool {
        if let Some(WaitingState::FanoutGroup {
            converter,
            pre_fanout_task_result_share,
            ..
        }) = self.get_mut(pre_fanout_task_id)
            && converter.is_none()
            && pre_fanout_task_result_share.is_none()
        {
            return false;
        }

        true
    }

    /// Takes one follower off the count of the fan-out group waiting on the task `wait_id`, once
    /// that follower has been queued or skipped. At zero, every follower has had its share or
    /// been skipped, and the group goes, with the engine's share.
    pub fn fanout_group_count_down(&mut self, wait_id: u64) {
        let mut is_count_zero = false;

        if let Some(fanout_group) = self.get_mut(wait_id)
            && let WaitingState::FanoutGroup {
                tasks_left_to_queue,
                ..
            } = fanout_group
        {
            *tasks_left_to_queue = tasks_left_to_queue.saturating_sub(1);

            if *tasks_left_to_queue == 0 {
                is_count_zero = true;
            }
        }

        if is_count_zero {
            self.remove(wait_id);
        }
    }

    /// Takes out the note in the slot of the task `wait_id` and returns the id it leads to: the
    /// task in whose slot the fan-out group waits. The entry is removed even when it holds
    /// something else, and that gives `None`.
    pub fn take_backwards_link_id(&mut self, wait_id: u64) -> Option<u64> {
        if let WaitingState::BackwardsLink { pre_fanout_task_id } = self.remove(wait_id) {
            return Some(pre_fanout_task_id);
        }

        None
    }
}

impl WaitTable<TrackedState> {
    /// Holds a join still waiting for some of its tasks, under the join's own id.
    pub fn insert_join(&mut self, task: Task) {
        self.store(task.id, TrackedState::Heldjoin(Box::new(task)));
    }

    /// Takes out the join held under `id`, if there is one. Unlike `take_task`, an entry that
    /// holds something else stays where it is.
    pub fn take_join(&mut self, id: u64) -> Option<Task> {
        if let Some(state) = self.get(id)
            && state.is_held_join()
            && let Some(TrackedState::Heldjoin(join_task)) = self.take(id)
        {
            return Some(*join_task);
        }

        None
    }

    /// The join held under `id`, left in place to be changed, if there is one.
    pub fn get_join_as_mut(&mut self, id: u64) -> Option<&mut Task> {
        if let Some(TrackedState::Heldjoin(join_task)) = self.get_mut(id) {
            return Some(join_task.as_mut());
        }

        None
    }

    /// Records that the follow-up `id` waits in the slot of the task `wait_id`. Like every store,
    /// it never replaces an entry, so the old record has to be gone first.
    pub fn insert_waiting_on(&mut self, id: u64, wait_id: u64) {
        self.store(id, TrackedState::WaitingOn(wait_id));
    }

    /// Takes out the record of where the follow-up `id` waits, if there is one, and returns the
    /// id of that task. Like `take_join`, it leaves an entry that holds something else where it
    /// is.
    pub fn take_waiting_on(&mut self, id: u64) -> Option<u64> {
        if let Some(state) = self.get(id)
            && state.is_waiting_on()
            && let Some(TrackedState::WaitingOn(wait_id)) = self.take(id)
        {
            return Some(wait_id);
        }

        None
    }

    /// Takes out the pointer under `id`, if there is one, and returns the id it points to: where
    /// the text of the error that the task `id` carries is kept. Like `take_join`, it leaves an
    /// entry that holds something else where it is.
    pub fn take_pre_fanout_error_id(&mut self, id: u64) -> Option<u64> {
        if let Some(state) = self.get(id)
            && state.is_carrying_error()
            && let Some(TrackedState::CarriesErrorOf(pre_fanout_task_id)) = self.take(id)
        {
            return Some(pre_fanout_task_id);
        }

        None
    }

    /// Takes out the fan-out follower held under its own id, `id`, if there is one. Like
    /// `take_join`, it leaves an entry that holds something else where it is.
    pub fn take_fanout_task(&mut self, id: u64) -> Option<Task> {
        if let Some(state) = self.get(id)
            && state.is_held_fannedout()
            && let Some(TrackedState::HeldFannedOut(task)) = self.take(id)
        {
            return Some(task);
        }

        None
    }

    /// Takes one off the count of the error record under `id`, for a task the error went to that
    /// was cancelled, and returns what's left. At zero, nobody took the error. `None` if there's
    /// no record: a task already took the error, or the result wasn't an error.
    pub fn sub_one_from_handed_error(&mut self, id: u64) -> Option<usize> {
        if let Some(TrackedState::HandedError { pending, .. }) = self.get_mut(id) {
            *pending = pending.saturating_sub(1);
            return Some(*pending);
        }

        None
    }

    /// Takes out the error record under `id`, if there is one, and returns its text. Like
    /// `take_join`, it leaves an entry that holds something else where it is.
    pub fn take_handed_error(&mut self, id: u64) -> Option<String> {
        if let Some(state) = self.get(id)
            && state.is_handed_error()
            && let Some(TrackedState::HandedError { text, .. }) = self.take(id)
        {
            return Some(text);
        }

        None
    }
}
