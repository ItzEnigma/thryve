// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! The Thryve engine.
//!
//! [`runtime::engine::Engine`] runs the main loop. It takes tasks, results and notifications from its
//! channels, hands tasks to the plugin manager's worker pool and notifications to the observer
//! manager. Applications don't use this crate directly: `thryve_run` builds and starts the engine.
pub mod runtime;
