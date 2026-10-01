// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Thryve's built-in observers.
//!
//! - [`logger`] writes log notifications to the log files opened at startup.
//! - [`topology_handler`] records the task graph in graph mode.

pub mod logger;
pub mod topology_handler;
