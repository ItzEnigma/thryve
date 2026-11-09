// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Startup and the run modes of Thryve: interactive, or as a Windows service.
//!
//! Most programs use the `thryve` crate instead, which re-exports the run modes as
//! `thryve::run`.

pub mod init;
pub mod run;
pub mod service;

// Runs the repository README's examples as doctests. Only doctests read the file: the
// published crate has no README at this path, so a normal build there would fail.
#[cfg(doctest)]
#[doc = include_str!("../../../README.md")]
pub struct ReadmeDoctests;
