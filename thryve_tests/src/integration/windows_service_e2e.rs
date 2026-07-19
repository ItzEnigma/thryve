//! Windows Service E2E integration test.
//!
//! Everything else in this crate runs the engine directly in-process. This is
//! the one test that can't work that way: `windows_services::Service::run`
//! (wrapped by `thryve_run::service::service::Thryve`) calls into
//! `StartServiceCtrlDispatcher`, which only succeeds when the calling process
//! was actually launched by the Service Control Manager. There is no way to
//! exercise that path without a real installed service, so this test builds
//! the `service_test_host` binary (`thryve_tests/src/bin/service_test_host.rs`
//! - a minimal host around the same `Thryve::new`/`try_run_as_service` used in
//! production), installs it as a real Windows service via `sc.exe`, and drives
//! it through install -> start -> pause -> resume -> stop -> delete.
//!
//! # Why this is `#[ignore]`d by default
//!
//! - Requires Administrator privileges (`sc.exe create`/`delete` need elevation).
//! - Registers and unregisters a real service in the Windows SCM database -
//!   real, if temporary, system-wide state outside this repo/process.
//! - Windows-only.
//!
//! Run explicitly, from an elevated (Administrator) terminal:
//! `cargo test -p thryve_tests --test-threads=1 -- --ignored windows_service_e2e`

#![cfg(windows)]

mod tests_windows_service_e2e {
    use std::path::PathBuf;
    use std::process::{Command, Output};
    use std::time::{Duration, Instant};

    const SERVICE_NAME: &str = "ThryveE2ETestService";

    fn workspace_root() -> PathBuf {
        // thryve_tests lives directly under the workspace root.
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .expect("thryve_tests should live directly under the workspace root")
            .to_path_buf()
    }

    /// Builds `service_test_host` and returns the path to its executable.
    /// Building here (rather than relying on it already being built) keeps
    /// this test self-sufficient - it works whether or not a prior `cargo
    /// build` happened to include it.
    fn build_service_host() -> PathBuf {
        let root = workspace_root();
        let status = Command::new("cargo")
            .args(["build", "-p", "thryve_tests", "--bin", "service_test_host"])
            .current_dir(&root)
            .status()
            .expect("failed to invoke `cargo build` for service_test_host");
        assert!(status.success(), "building service_test_host failed");

        let exe = root.join("target").join("debug").join("service_test_host.exe");
        assert!(
            exe.exists(),
            "expected built executable at {} - if this was built in release mode, \
             update this path accordingly",
            exe.display()
        );
        exe
    }

    fn sc(args: &[&str]) -> Output {
        Command::new("sc.exe").args(args).output().expect("failed to invoke sc.exe")
    }

    fn query_state(name: &str) -> String {
        let output = sc(&["query", name]);
        String::from_utf8_lossy(&output.stdout).into_owned()
    }

    fn wait_for_state(name: &str, expected: &str, timeout: Duration) -> bool {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if query_state(name).contains(expected) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(300));
        }
        false
    }

    /// Best-effort teardown of any leftover instance from a previous failed
    /// run, so this test can be re-run without manual cleanup.
    fn force_remove_existing() {
        let _ = sc(&["stop", SERVICE_NAME]);
        std::thread::sleep(Duration::from_secs(1));
        let _ = sc(&["delete", SERVICE_NAME]);
        std::thread::sleep(Duration::from_secs(1));
    }

    #[test]
    #[ignore = "requires Administrator privileges and registers a real Windows service via SCM"]
    fn test_windows_service_e2e_install_start_pause_resume_stop_uninstall() {
        let exe = build_service_host();
        force_remove_existing();

        // Install. sc.exe's argv parsing is unusual: `binPath=` and the value
        // must be separate argv entries (no space before '=', one space
        // conceptually after it) - passing them as one Command::args() slice
        // sidesteps cmd.exe's shell-quoting rules entirely.
        let exe_str = exe.display().to_string();
        let create = Command::new("sc.exe")
            .args(["create", SERVICE_NAME, "binPath=", &exe_str, "start=", "demand"])
            .output()
            .expect("failed to invoke sc.exe create");
        assert!(
            create.status.success(),
            "sc.exe create failed: {}",
            String::from_utf8_lossy(&create.stderr)
        );

        // Run the actual lifecycle, but guarantee `sc delete` runs even if an
        // assertion below fails - otherwise a failed run leaves the test
        // service registered on the machine.
        let outcome = std::panic::catch_unwind(|| {
            let start = sc(&["start", SERVICE_NAME]);
            assert!(start.status.success(), "sc.exe start failed: {}", String::from_utf8_lossy(&start.stderr));
            assert!(
                wait_for_state(SERVICE_NAME, "RUNNING", Duration::from_secs(15)),
                "service did not reach RUNNING state; last query:\n{}",
                query_state(SERVICE_NAME)
            );

            let pause = sc(&["pause", SERVICE_NAME]);
            assert!(pause.status.success(), "sc.exe pause failed: {}", String::from_utf8_lossy(&pause.stderr));
            assert!(
                wait_for_state(SERVICE_NAME, "PAUSED", Duration::from_secs(10)),
                "service did not reach PAUSED state; last query:\n{}",
                query_state(SERVICE_NAME)
            );

            let resume = sc(&["continue", SERVICE_NAME]);
            assert!(resume.status.success(), "sc.exe continue failed: {}", String::from_utf8_lossy(&resume.stderr));
            assert!(
                wait_for_state(SERVICE_NAME, "RUNNING", Duration::from_secs(10)),
                "service did not resume to RUNNING state; last query:\n{}",
                query_state(SERVICE_NAME)
            );

            let stop = sc(&["stop", SERVICE_NAME]);
            assert!(stop.status.success(), "sc.exe stop failed: {}", String::from_utf8_lossy(&stop.stderr));
            assert!(
                wait_for_state(SERVICE_NAME, "STOPPED", Duration::from_secs(15)),
                "service did not reach STOPPED state; last query:\n{}",
                query_state(SERVICE_NAME)
            );
        });

        // Always uninstall, whether or not the lifecycle assertions passed.
        let _ = sc(&["delete", SERVICE_NAME]);

        outcome.expect("windows service E2E lifecycle assertions failed");
    }
}
