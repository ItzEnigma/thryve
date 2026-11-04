// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Tests for the Thryve workspace.
//!
//! Unit tests are grouped by the crate they cover (`tests_utils`, `tests_core`, `tests_observers`,
//! `tests_run`, `tests_ffi`); `integration` holds end-to-end tests. The other modules each test one
//! feature through a running engine, in one place:
//!
//! - `graceful_failures`: a developer's mistake or a failing environment returns an error, or
//!   degrades and says so, and the process keeps running;
//! - `shutdown_and_startup`: shutdowns and startups that never hang;
//! - `engine_wake_up`: the engine wakes the moment it has work;
//! - `typed_tasks`: typed tasks, `then` and forwarding;
//! - `joins_and_splits`: joins and splitting work;
//! - `cancelling`: cancelling;
//! - `fan_out`: fan-out.
//!
//! CI runs them on one thread (`--test-threads=1`), because some tests share files and global
//! state.

#[cfg(test)]
pub mod tests_utils;

#[cfg(test)]
pub mod tests_core;

#[cfg(test)]
pub mod tests_observers;

#[cfg(test)]
pub mod tests_run;

#[cfg(test)]
pub mod tests_ffi;

#[cfg(test)]
pub mod integration;

#[cfg(test)]
pub mod graceful_failures;

#[cfg(test)]
pub mod shutdown_and_startup;

#[cfg(test)]
pub mod engine_wake_up;

#[cfg(test)]
pub mod typed_tasks;

#[cfg(test)]
pub mod joins_and_splits;

#[cfg(test)]
pub mod cancelling;

#[cfg(test)]
pub mod fan_out;
