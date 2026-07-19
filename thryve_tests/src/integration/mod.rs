//! Integration tests: real, end-to-end exercises of the engine, FFI boundary,
//! and Windows Service lifecycle.
//!
//! These differ from the per-crate unit tests (`tests_core`, `tests_ffi`,
//! `tests_observers`, `tests_run`, `tests_utils`) in kind, not just size: unit
//! tests call one function/struct in isolation, often with a hand-built `Ctx`
//! that never runs the engine loop. Everything here spins up the real engine
//! (or a real Windows service, or the real FFI boundary) and drives it to
//! completion through actual threads/channels/OS calls.

pub mod engine_lifecycle;
pub mod plugin_pipeline;
pub mod ffi_lifecycle;
pub mod windows_service_e2e;
