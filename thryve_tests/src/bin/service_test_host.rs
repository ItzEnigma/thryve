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

fn main() {
    let mut config = Config::default();
    config.service.can_stop = true;
    config.service.can_pause = true;

    let (service, ctx) = create_service(config);
    try_run_as_service(service, ctx);
}
