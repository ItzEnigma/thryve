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
//! its end. It's a plain store: the engine decides what
//! each arrival means (see
//! [`Engine::accept_task`](crate::runtime::engine::Engine::accept_task),
//! [`Engine::handle_result`](crate::runtime::engine::Engine::handle_result) and
//! [`Engine::check_status`](crate::runtime::engine::Engine::check_status)).
//!
//! Only the engine thread uses it.

use std::collections::HashMap;

use thryve_utils::{packets::task::Task, res::SealedHandoff};

/// One entry per typed task whose follow-up or result is waiting for the other side, or whose
/// forward hasn't been followed yet. An entry goes once both sides have arrived, so the table
/// doesn't grow.
pub struct WaitTable {
    /// The entries, by task id. The shutdown empties it directly.
    pub table: HashMap<u64, WaitingState>,
}

impl Default for WaitTable {
    fn default() -> Self {
        Self::new()
    }
}

/// What the table holds for a task id.
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

    /// The task's handle was dropped before its result arrived, so nobody will take the result.
    Released,
    /// No entry: what [`WaitTable::remove`] and [`WaitTable::get_as_ref`] return for an id the
    /// table doesn't hold. Never stored.
    None,
}

impl WaitTable {
    /// An empty table.
    pub fn new() -> Self {
        Self {
            table: HashMap::new(),
        }
    }

    /// Stores `state` under `wait_id`, unless the id already has an entry: then the table keeps
    /// that one, and `state` is dropped. The engine checks for an entry first.
    pub fn store(&mut self, wait_id: u64, state: WaitingState) {
        self.table.entry(wait_id).or_insert(state);
    }

    /// Takes out the entry for `wait_id`, or returns [`WaitingState::None`] if there's none.
    pub fn remove(&mut self, wait_id: u64) -> WaitingState {
        if let Some(state) = self.table.remove(&wait_id) {
            state
        } else {
            WaitingState::None
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

    /// The entry for `wait_id`, left in place, or [`WaitingState::None`] if there's none.
    pub fn get_as_ref(&self, wait_id: u64) -> &WaitingState {
        if let Some(state) = self.table.get(&wait_id) {
            state
        } else {
            &WaitingState::None
        }
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
        matches!(self, WaitingState::Link { next_id: _ })
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
