// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Minimal Windows-service host used only by the Windows Service E2E integration
//! test (`thryve_tests::integration::windows_service_e2e`). Not a product binary.
//!
//! Registers with the real Windows Service Control Manager via thryve_run's
//! `Thryve` service wrapper and idles until SCM sends Start/Pause/Resume/Stop.
//! No plugins/observers are loaded - this exists purely to prove the SCM
//! lifecycle works end-to-end through `thryve_run::service::service::Thryve`.
//! The integration test builds this binary with `cargo build -p thryve_tests
//! --bin service_test_host` and installs the resulting executable as a real
//! Windows service via `sc.exe`.

use thryve_run::run::run_service::{create_service, try_run_as_service};
use thryve_utils::cfg::config::Config;
use thryve_utils::errors::get_err;

fn main() {
    let mut config = Config::default();
    config.service.can_stop = true;
    config.service.can_pause = true;

    let (service, ctx) = create_service(config);

    // The error is also in Error.log once the log folder exists; under the service
    // manager, stderr goes nowhere, but the exit code is recorded.
    if let Err(e) = try_run_as_service(service, ctx) {
        eprintln!("{}", get_err(&e));
        std::process::exit(1);
    }
}
