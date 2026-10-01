// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! The engine ([`engine`]) and the two managers it drives: [`plugin_manager`], which owns the plugins
//! and the worker pool, and [`observer_manager`], which owns the observers and routes notifications
//! to them.

pub mod engine;
pub mod observer_manager;
pub mod plugin_manager;
