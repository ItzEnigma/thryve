// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Shared building blocks for the Thryve framework.
//!
//! Every other Thryve crate builds on this one: the plugin, observer and notification types, tasks
//! and their results, the queues and the thread pool, configuration, checked names and the error
//! types. Applications normally use it through the `thryve` crate, which re-exports the public parts.

pub mod bases;
pub mod cfg;
pub mod errors;
pub mod extras;
pub mod graphs;
pub mod macros;
pub mod packets;
pub mod queues;
pub mod res;
pub mod threads;
pub mod traits;
