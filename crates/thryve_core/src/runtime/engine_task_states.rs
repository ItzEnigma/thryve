// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! What the engine's two tables can hold for a task id.
//!
//! Both are [`WaitTable`](crate::runtime::engine_wait_table::WaitTable)s, keyed by task id, but
//! by different ids. The wait table holds [`WaitingState`]s under the id of the task waited on:
//! a follow-up and the result it waits for meet there, under the id of the task that produces
//! it, and so do a fan-out group and the result its followers share. The other holds
//! [`TrackedState`]s under a task's own id: what the engine keeps about that task itself, such as
//! a join still waiting for some of its tasks, a group's follower that arrived before its share,
//! the slot a follow-up waits in, or the error the task produced, kept for the tasks queued with
//! it.
//!
//! Only the engine thread uses them.

use std::{any::Any, collections::HashSet, sync::Arc};

use thryve_utils::{
    packets::task::{Converter, Task},
    res::SealedHandoff,
};

/// What the wait table holds under the id of the task waited on.
///
/// For Thryve's own crates. Not part of the public API.
#[doc(hidden)]
pub enum WaitingState {
    /// The follow-up declared with `then`, waiting for the task's result.
    FollowUP(Task),
    /// The task's result, kept because its handle still exists and may still call `then`.
    HandoffKept(SealedHandoff),
    /// The task forwarded to the task `next_id`: whoever follows this task goes on to that one.
    /// Only one follow-up or release ever comes through, so it removes the link as it passes.
    Link {
        /// The id of the task forwarded to.
        next_id: u64,
    },

    /// A note at the end of a task's forwards, for the fan-out group that waits in that task's
    /// slot: the result that arrives here is the one the group's followers share. Say a group
    /// waits on LoadImg, and LoadImg forwarded to Decode: the note waits in Decode's slot, and
    /// Decode's result reaches the group through it, however many forwards there were. If Decode
    /// forwards too, the note moves on to the task forwarded to. It's a join's follower that
    /// leads to a group instead of a join.
    BackwardsLink {
        /// The id of the task the group follows, in whose slot the group waits.
        pre_fanout_task_id: u64,
    },

    /// A join's follower: the join waits for this task's result, which goes into its slot at
    /// `position`. Left when the join is declared, since its task hadn't finished yet. If the
    /// task forwards, the follower moves on to the task forwarded to, whose result the join then
    /// waits for. If the join has already run when the result arrives (another of its tasks
    /// failed first), the result is late, and nobody takes it.
    JoinPartPlaceHolder {
        /// The join's id, under which the engine holds the join (see
        /// [`TrackedState::Heldjoin`]).
        join_id: u64,
        /// The slot the result goes into: the task's place among the join's handles.
        position: usize,
    },

    /// A fan-out group, in the slot of the task whose result its followers share. Only the
    /// group's data makes it. Until the task's result reaches it, it holds the converter and the
    /// followers' ids; once the converter has run, the share. The followers that arrived before
    /// the share wait under their own ids (see [`TrackedState::HeldFannedOut`]), and the ones
    /// that arrive after it are queued at once. It goes once every follower has been queued or
    /// skipped, and the engine's share with it, so the last follower to finish frees the result.
    ///
    /// Until the result reaches it, the group also knows which followers still want the result,
    /// and which task it waits for, so that cancels can stop that task once nobody wants it. If
    /// that task is cancelled, its cancelled report drops the converter instead of running it:
    /// with no converter and no share, the group will never give a share, and every follower is
    /// skipped, the held ones at once and the others as they arrive.
    FanoutGroup {
        /// The task whose report will reach the group: the task the group follows, or, once it
        /// has forwarded, the task at the end of its forwards, where the note waits (see
        /// [`WaitingState::BackwardsLink`]). The one to cancel once no follower is live.
        waits_for_id: u64,
        /// How many followers haven't been queued or skipped yet. It starts at the number of
        /// ids, and goes down once the share is there, or once the group is cancelled; at zero,
        /// the entry goes.
        tasks_left_to_queue: usize,
        /// The followers' ids, from the group's data. Taken out when the converter has run, to
        /// queue the followers held under them, or when the group is cancelled, to skip them.
        fanout_tasks_ids: Vec<u64>,
        /// The followers that still want the result: the ids not yet cancelled when the group's
        /// data arrived. A cancel takes its follower out once the follower is held, and the one
        /// that empties it cancels [`waits_for_id`](WaitingState::FanoutGroup::waits_for_id).
        /// It only counts while the converter is there.
        live_fanout_tasks_ids: HashSet<u64>,
        /// Opens the task's result, once (see
        /// [`Converter`](thryve_utils::packets::task::Converter)). Taken out when it runs, and
        /// dropped when the task's cancelled report reaches the group.
        converter: Option<Box<Converter>>,
        /// What the converter returned: the result, shared. Each follower gets a clone.
        pre_fanout_task_result_share: Option<Arc<dyn Any + Send + Sync>>,
    },

    /// The task's handle was dropped before its result arrived, so nobody will take the result.
    Released,
    /// No entry: what
    /// [`WaitTable::remove`](crate::runtime::engine_wait_table::WaitTable::remove) and
    /// [`WaitTable::get_as_ref`](crate::runtime::engine_wait_table::WaitTable::get_as_ref) return
    /// for an id the table doesn't hold. Never stored.
    None,
}

/// What the engine keeps about a task under the task's own id, where a [`WaitingState`] is kept
/// under the id of the task waited on.
///
/// For Thryve's own crates. Not part of the public API.
#[doc(hidden)]
pub enum TrackedState {
    /// A join still waiting for some of its tasks. Each missing task's follower, a
    /// [`WaitingState::JoinPartPlaceHolder`], leads here; the join is taken out and queued when
    /// the last one's result arrives, or at once when one of them fails.
    Heldjoin(Box<Task>),
    /// The task's error, handed to the tasks queued with it as their input, and kept under the id
    /// of the task that produced it. A worker that skips a cancelled task can't open its input,
    /// so the text waits here until those tasks report, in case it's written to `Error.log`
    /// because nobody took the error. The first report that isn't a cancel means a task took the
    /// error, and the record goes, with no line. Each cancel takes one off the count, and at zero
    /// the text is written, once. Each of those tasks holds a [`TrackedState::CarriesErrorOf`]
    /// that leads here, except a fan-out follower skipped before it was queued: the engine takes
    /// one off for it directly. A record still here at the shutdown is one lost result, however
    /// many pointers lead to it.
    HandedError {
        /// The error's text, as the worker wrote it while the error's type was still known.
        text: String,
        /// How many of the tasks the error goes to haven't reported yet: 1 for a follow-up or a
        /// join, and for a fan-out group, its number of followers, whether they've arrived yet or
        /// not.
        pending: usize,
    },
    /// The task was queued with an error as its input, and the error's text is kept under this
    /// id, the id of the task that produced it (see [`TrackedState::HandedError`]). The task's
    /// report takes it out and follows it there.
    CarriesErrorOf(u64),

    /// The follow-up waits in the slot of the task with this id, for its result. A cancel of the
    /// follow-up reads it to find that task, which only the follow-up needed, and cancels it too.
    /// Written whenever the follow-up is stored in a slot, so a forward that moves it rewrites
    /// it, and taken out whenever it leaves the slot.
    WaitingOn(u64),

    /// A fan-out group's follower that arrived before its group's share was there. When the
    /// converter runs, it's taken out and queued with its share, or, if it was cancelled while it
    /// waited, skipped and reported as cancelled. A cancel takes it off the group's live list
    /// while it waits, and it's skipped at once if the group is cancelled. One still held at the
    /// shutdown is a task that never ran.
    HeldFannedOut(Task),
}

impl WaitingState {
    /// Whether it's a follow-up waiting for the task's result.
    pub fn is_follow_up(&self) -> bool {
        matches!(self, WaitingState::FollowUP(_))
    }

    /// Whether it's a kept result.
    pub fn is_handoff(&self) -> bool {
        matches!(self, WaitingState::HandoffKept(_))
    }

    /// Whether it's a link to the task forwarded to.
    pub fn is_link(&self) -> bool {
        matches!(self, WaitingState::Link { .. })
    }

    /// Whether it's a join's follower.
    pub fn is_join_part_placeholder(&self) -> bool {
        matches!(self, WaitingState::JoinPartPlaceHolder { .. })
    }

    /// Whether it's a fan-out group.
    pub fn is_fanout_group(&self) -> bool {
        matches!(self, WaitingState::FanoutGroup { .. })
    }

    /// Whether it's a note that leads to a fan-out group.
    pub fn is_backwards_link(&self) -> bool {
        matches!(self, WaitingState::BackwardsLink { .. })
    }

    /// Whether it's a remembered release.
    pub fn is_released(&self) -> bool {
        matches!(self, WaitingState::Released)
    }

    /// Whether there's no entry.
    pub fn is_none(&self) -> bool {
        matches!(self, WaitingState::None)
    }
}

impl TrackedState {
    /// Whether it's a join still waiting for some of its tasks.
    pub fn is_held_join(&self) -> bool {
        matches!(self, TrackedState::Heldjoin(_))
    }

    /// Whether it's an error's text, kept for a task queued with the error.
    pub fn is_handed_error(&self) -> bool {
        matches!(
            self,
            TrackedState::HandedError {
                text: _,
                pending: _
            }
        )
    }

    /// Whether it's a pointer to an error's text, kept under another task's id.
    pub fn is_carrying_error(&self) -> bool {
        matches!(self, TrackedState::CarriesErrorOf(_))
    }

    /// Whether it's a fan-out group's follower, held until its share is there.
    pub fn is_held_fannedout(&self) -> bool {
        matches!(self, TrackedState::HeldFannedOut(_))
    }

    /// Whether it's where a follow-up waits.
    pub fn is_waiting_on(&self) -> bool {
        matches!(self, TrackedState::WaitingOn(_))
    }

    /// The task, if it's a join still waiting or a held fan-out follower, and `None` otherwise.
    pub fn take_held_join_or_fannedout(self) -> Option<Task> {
        match self {
            TrackedState::Heldjoin(join_task) => Some(*join_task),
            TrackedState::HeldFannedOut(task) => Some(task),
            _ => None,
        }
    }

    /// The error's text, if it's a kept error, and `None` otherwise.
    pub fn take_handed_error(self) -> Option<String> {
        match self {
            TrackedState::HandedError { text, pending: _ } => Some(text),
            _ => None,
        }
    }
}
