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
//! `tests_run`, `tests_ffi`); `integration` holds end-to-end tests. `gate_t1` is the gate of
//! milestone T1: it shows, in one place, that every former panic site now returns an error or
//! degrades. CI runs them on one thread (`--test-threads=1`), because some tests share files and
//! global state.

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
pub mod gate_t1;

#[cfg(test)]
pub mod gate_t2;
