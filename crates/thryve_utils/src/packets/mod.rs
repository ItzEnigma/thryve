// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! What travels to the engine: tasks ([`task`]), log and topology notifications ([`notif_log`],
//! [`notif_topology`]), and the [`packet_handle`] that sends them.

pub mod notif_log;
pub mod notif_topology;
pub mod packet_handle;
pub mod task;
