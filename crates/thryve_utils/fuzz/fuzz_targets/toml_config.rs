#![no_main]

use libfuzzer_sys::fuzz_target;
use thryve_utils::cfg::config::Config;

// Mirrors the exact parsing call in `read_config_file` (thryve_utils/src/cfg/config.rs),
// which is reachable from an untrusted-ish config file via `thryve_config_from_file`.
// We only care whether deserializing arbitrary bytes into `Config` can panic or
// trigger memory unsafety — not whether the result is a valid config.
fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let _ = toml::from_str::<Config>(s);
    }
});
