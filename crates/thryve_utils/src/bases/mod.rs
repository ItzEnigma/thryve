// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! What an application registers with Thryve: [`plugin::Plugin`], [`observer::Observer`] and
//! [`notif::Notif`], plus [`queue`], the queue base their queues share.

pub mod notif;
pub mod observer;
pub mod plugin;
pub mod queue;
