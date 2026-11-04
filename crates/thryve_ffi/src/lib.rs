// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! FFI-safe wrapper functions for the Thryve framework.
//!
//! This module provides C-compatible wrapper functions for the Thryve framework,
//! enabling integration with other programming languages (C, C++, Python, Node.js, etc.).
//!
//! # Design Principles
//!
//! - All complex Rust types are wrapped in opaque pointers (`*mut u8`) for FFI safety
//! - Memory management is explicit: every `create` function has a corresponding `free` function
//! - Functions use `extern "C"` calling convention for cross-language compatibility
//! - Error handling uses nullable pointers and status codes
//! - Callbacks follow a specific ownership model — see the [Callback Types](#callback-types) section
//! - Every wrapper's body runs through `ffi_guard`, so a panic anywhere in the
//!   call chain becomes a null/error return instead of unwinding across the FFI
//!   boundary into the foreign caller (which is undefined behavior). Once an
//!   engine has started, though, Thryve's panic hook logs a panic and ends the
//!   process before the guard sees it
//!
//! # Typical Usage Sequence (Interactive Mode)
//!
//! ```c
//! // 1. Create configuration
//! ThryveConfig config = thryve_config_default();
//!
//! // 2. Create context from configuration
//! ThryveCtx ctx = thryve_ctx_create(config);
//! if (ctx != NULL) {
//!     // 3. Add plugins and observers to the context
//!     thryve_ctx_add_plugin(ctx, plugin_ptr);
//!     thryve_ctx_add_observer(ctx, observer_ptr);
//!
//!     // 4. Start the engine in interactive mode
//!     ThryveRunHandle handle = thryve_run_interactive_start(ctx);
//!     if (handle != NULL) {
//!         // Wait for engine to finish
//!         thryve_run_interactive_free(handle);
//!     }
//!
//!     // 5. Cleanup
//!     thryve_ctx_free(ctx);
//! }
//!
//! // 6. Free configuration
//! thryve_config_free(config);
//! ```
//!
//! # Callback Types
//!
//! All callback types use the `Thryve` prefix and follow C-compatible calling conventions.
//!
//! ## Task and Plugin Callbacks
//!
//! - **`ThryveTaskJob`**: A function pointer that takes no arguments and returns a `ThryveTaskResult`.
//!   Used for plugin entry points and task execution.
//! - **`ThryveTaskResult`**: A pub struct containing:
//!   - `result_type`: A null-terminated string describing the result type (e.g., "Success", "Failed")
//!   - `message`: A null-terminated string with a human-readable result message
//!   - `log_file`: A null-terminated string specifying the log file name for the result
//!   - `has_chain`: A boolean indicating whether a chained task should be executed next
//!   - `chain_task`: A pointer to the chained task (only valid if `has_chain` is true and pointer is not null)
//! - **`ThryvePluginEntryPoint`**: Alias for `ThryveTaskJob` — used when creating plugins.
//! - **`ThryvePluginCleanUp`**: A function pointer that takes a `*const u8` (the plugin's core pointer)
//!   and returns an `int`: `ThryveStatus_Ok` (0) if the cleanup worked, any other value if it failed.
//!   Called once when the engine shuts down. A failure is logged to `Error.log`.
//!
//! ## Observer Callbacks
//!
//! - **`ThryveObsJob`**: **Primary callback** for observer execution. Takes an `ThryveObserver` (raw pointer to `Arc<Observer>`)
//!   and returns nothing. **The FFI implementation must convert the raw pointer back to an Arc and drop it** — see
//!   [Observer Job Memory Safety](#observer-job-memory-safety) for details.
//! - **`ThryveObserverCleanUp`**: A function pointer that takes a `*const u8` (the observer's core pointer).
//!   Called when the observer is being cleaned up.
//! - **`ThryveObserverMainLoop`**: A function pointer that takes a `*const u8` (the observer's core pointer).
//!   Currently **unused** in the FFI layer.
//!
//! ### Observer Job Memory Safety
//!
//! When an observer is executed, the Rust side performs the following:
//!
//! 1. Clones the `Arc<Observer>`: `observer.clone()`
//! 2. Converts to raw pointer: `Arc::into_raw(observer_clone) as ThryveObserver`
//! 3. Calls the `ThryveObsJob` callback with this raw pointer
//!
//! The FFI callback **must**:
//!
//! 1. Convert the raw pointer back to an `Arc`: `Arc::from_raw(obs as *const Observer as *mut Observer)`
//! 2. Use the Arc to access the observer
//! 3. Drop the Arc when done (automatic when it goes out of scope in C++)
//!
//! **C example:**
//!
//! ```c
//! void my_observer_job(ThryveObserver obs) {
//!     // Convert raw pointer back to Arc<Observer> (C++ style)
//!     // In C, you may need to manage this differently
//!     // Best practice: use C++ with RAII
//! }
//! ```
//!
//! **C++ example:**
//!
//! ```cpp
//! extern "C" void my_observer_job(ThryveObserver obs) {
//!     // Convert raw pointer back to Arc<Observer>
//!     auto observer = std::shared_ptr<Observer>(
//!         static_cast<Observer*>(obs),
//!         [](Observer* p) { /* No-op: Arc will be dropped by Rust */ }
//!     );
//!
//!     // Use the observer
//!     while (!observer->exit_flag) {
//!         // Process notifications
//!     }
//!
//!     // Arc is automatically dropped when observer goes out of scope
//! }
//! ```
//!
//! **Critical**: If the callback crashes or leaks the Arc, the Observer will be leaked.
//!
//! ## Notification Callbacks
//!
//! - **`ThryveNotifLogic`**: A function pointer that takes a `*const u8` (the notification's core pointer).
//!   **Currently unused**: no FFI function takes one, so Thryve never calls it.
//!
//! ## Threading Model
//!
//! Callbacks run on different threads depending on the context:
//!
//! - **`ThryveTaskJob` / `ThryvePluginEntryPoint`**: Runs on one of Thryve's plugin worker
//!   threads, never on the thread that sent the task. Several tasks can run at the same time, on
//!   different workers.
//! - **`ThryveObsJob`**: Runs on the observer's own thread, which Thryve starts when the engine
//!   loads the observer. It's called once and is the observer's whole loop: when it returns, the
//!   observer's thread ends. The Rust side clones the `Arc<Observer>`, converts it to a raw
//!   pointer, and calls the callback with it.
//! - **`ThryvePluginCleanUp`**: Runs on Thryve's plugin clean-up thread during shutdown, after every
//!   task has stopped. The plugins are cleaned up one after another on that thread.
//! - **`ThryveObserverCleanUp`**: Runs once during shutdown, on Thryve's observer clean-up thread.
//!   The observers are cleaned up one after another on that thread.
//!
//! **Critical for `ThryveObsJob`**: Because this callback runs on a separate thread, the FFI
//! implementation must ensure any shared data accessed within the callback is protected by
//! synchronization primitives (mutexes, atomics, etc.). The `ThryveObserver` raw pointer itself
//! is only valid during the callback execution — it must not be stored for later use.
//!
//! # Plugin/Observer/Notification Logic
//!
//! The `PlgLogic`, `ObsLogic`, and `NotifLogic` traits are implemented by adapter structs
//! (`PluginLogicAdapter`, `ObserverLogicAdapter`, `NotificationLogicAdapter`) that wrap
//! FFI function pointers. Foreign language developers do **not** implement these traits directly —
//! they provide function pointers to `thryve_plugin_create`, `thryve_observer_create`, or
//! `thryve_notification_create`, and the Rust side creates the adapter.
//!
//! See the [`thryve_plugin_create`](#thryve_plugin_create), [`thryve_observer_create`](#thryve_observer_create),
//! and [`thryve_notification_create`](#thryve_notification_create) functions for details.
//!
//! # Memory Management Rules
//!
//! 1. **Ownership Transfer**: When you pass a pointer to a Rust function, ownership may be transferred.
//!    - `thryve_plugin_create`: Takes ownership of `entry_point` (wraps it in the plugin's init closure)
//!    - `thryve_task_create`: Takes ownership of `job` (wraps it in the task's closure)
//!    - `thryve_ctx_add_plugin`: Takes ownership of `plugin` (converts from raw pointer to Box, then clones into vector)
//!    - `thryve_ctx_add_observer`: Takes ownership of `observer` (converts from raw pointer to Box, then clones into vector)
//! 2. **Never free after transfer**: After calling `thryve_plugin_create` or `thryve_task_create`, do **not** free the
//!    callback pointer — it has been transferred to Rust and will be called from there.
//! 3. **String borrowing**: String arguments (`name`, `type`, etc.) are borrowed via `CStr::from_ptr` and
//!    converted to `&str`. The foreign language must **not** free these pointers after passing them.
//! 4. **PacketHandle is cloned**: The PacketHandle pointer is cloned inside the Rust side, so the foreign
//!    language can continue to use it after passing to plugin/observer creation.

use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::sync::Arc;
use std::thread::JoinHandle;

use thryve_run::service::service::Thryve;
use thryve_utils::bases::notif::Notif;
use thryve_utils::bases::observer::Observer;
use thryve_utils::bases::plugin::Plugin;
use thryve_utils::cfg::config::Config;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::errors::{FinalizationError, StartupError, ThryveError};
use thryve_utils::extras::jobs::ObsJob;
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::packets::task::Task;
use thryve_utils::traits::notif_logic::NotifLogic;
use thryve_utils::traits::observer_logic::ObsLogic;
use thryve_utils::traits::plugin_logic::PlgLogic;

use thryve_run::run::run_interactive::{
    run_interactive as inner_run_interactive, summon_requirements as inner_summon_requirements,
};
use thryve_run::run::run_service::{
    create_service as inner_create_service, try_run_as_service as inner_try_run_as_service,
};
use thryve_utils::res::TaskResult;

// ============================================================================
// Opaque Pointer Types
// ============================================================================
//
// Each handle is a pointer to a locally-defined, never-instantiated marker
// type rather than a shared `*mut u8`. Every site that creates or consumes
// one of these already casts explicitly to/from the real internal type
// (`as *mut Config`, `as ThryveConfig`, ...), so swapping the marker in below
// is the only change needed here. This keeps internal types (Config, Ctx,
// Task, ...) out of the generated C header entirely — cbindgen only ever
// sees e.g. `ConfigHandle`/`TaskHandle` — while still making it a compile
// error in C/C++ to pass, say, an ThryveTask where an ThryveCtx is expected,
// instead of silently corrupting memory.

/// Marker type for [`ThryveConfig`]. Never instantiated.
pub struct ConfigHandle {}
/// Opaque pointer to a Config instance (wraps `*mut Config`).
pub type ThryveConfig = *mut ConfigHandle;

/// Marker type for [`ThryveCtx`]. Never instantiated.
pub struct CtxHandle {}
/// Opaque pointer to a Ctx instance (wraps `*mut Ctx`).
pub type ThryveCtx = *mut CtxHandle;

/// Marker type for [`ThryveRunHandle`]. Never instantiated.
pub struct RunHandleHandle {}
/// Opaque pointer to a JoinHandle instance (wraps `*mut JoinHandle<()>`).
pub type ThryveRunHandle = *mut RunHandleHandle;

/// Marker type for [`ThryveService`]. Never instantiated.
pub struct ServiceHandle {}
/// Opaque pointer to a Thryve service instance (wraps `*mut Thryve`).
pub type ThryveService = *mut ServiceHandle;

/// Marker type for [`ThryvePlugin`]. Never instantiated.
pub struct PluginHandle {}
/// Opaque pointer to a Plugin instance (wraps `*mut Plugin`).
pub type ThryvePlugin = *mut PluginHandle;

/// Marker type for [`ThryveObserver`]. Never instantiated.
pub struct ObserverHandle {}
/// Opaque pointer to an Observer instance (wraps `*mut Observer`).
pub type ThryveObserver = *mut ObserverHandle;

/// Marker type for [`ThryvePacketHandle`]. Never instantiated.
pub struct PacketHandleHandle {}
/// Opaque pointer to a PacketHandle instance (wraps `*mut PacketHandle`).
pub type ThryvePacketHandle = *mut PacketHandleHandle;

/// Marker type for [`ThryveTask`]. Never instantiated.
pub struct TaskHandle {}
/// Opaque pointer to a Task instance (wraps `*mut Task`).
pub type ThryveTask = *mut TaskHandle;

/// Marker type for [`ThryveNotif`]. Never instantiated.
pub struct NotifHandle {}
/// Opaque pointer to a Notification instance (wraps `*mut Notif`).
pub type ThryveNotif = *mut NotifHandle;

// ============================================================================
// Panic Safety
// ============================================================================

/// Runs `f`, catching any panic instead of letting it unwind out of this
/// `extern "C"` function. Unwinding across an FFI boundary into a foreign
/// caller is undefined behavior (e.g. a malformed config file can trigger an
/// internal `assert!`/`unwrap()` several calls deep) — every wrapper in this
/// file routes its body through here so a caught panic becomes `on_panic`
/// instead of crossing into C. Once an engine has started, Thryve's panic hook
/// ends the process first (it calls `exit(1)`), so nothing reaches this catch.
fn ffi_guard<T>(on_panic: T, f: impl FnOnce() -> T) -> T {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or(on_panic)
}

/// FFI-safe status code for operations.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThryveStatus {
    /// Operation completed successfully.
    Ok = 0,
    /// A null argument was passed to the function.
    NullArg = 1,
    /// Memory allocation failed.
    AllocFail = 2,
    /// The operation was attempted in an invalid state.
    InvalidState = 3,
    /// The operation failed.
    OperationFail = 4,
    /// String conversion error (e.g., invalid UTF-8).
    StrConvError = 5,
}

/// FFI-safe result for task execution.
///
/// This pub struct represents the result of a task or plugin execution, allowing the
/// foreign language implementation to communicate the outcome back to the Rust runtime.
///
/// # Fields
///
/// - `result_type`: A null-terminated string describing the result type (e.g., "Success", "Failed")
/// - `message`: A null-terminated string with a human-readable result message
/// - `log_file`: A null-terminated string specifying the log file name for the result
/// - `has_chain`: Indicates whether a chained task should be executed next
/// - `chain_task`: Pointer to the chained task (only valid if `has_chain` is true)
///
/// # Constructing a Task Result
///
/// In C, you create the result with zero-initialized memory:
///
/// ```c
/// ThryveTaskResult result = {0};  // has_chain = false, chain_task = NULL
/// result.result_type = "Success";
/// result.message = "Task completed";
/// result.log_file = "Main";
/// ```
///
/// If you want to return a chained task:
///
/// ```c
/// ThryveTaskResult result = {0};
/// result.result_type = "Chained";
/// result.message = "Chaining to next task";
/// result.log_file = "Main";
/// result.has_chain = 1;
/// result.chain_task = my_chained_task;  // Pointer to a ThryveTask created by thryve_task_create
/// ```
///
/// **Important**: The `chain_task` pointer must point to a valid `ThryveTask` that has been
/// created (e.g., via `thryve_task_create`). The Rust side will take ownership of this task
/// when `has_chain` is true and the pointer is not NULL.
///
/// # Memory Management
///
/// - String fields (`result_type`, `message`, `log_file`) should point to static/const strings
///   or strings that remain valid for the lifetime of the result. Do NOT free these pointers
///   after returning the result.
/// - The `chain_task` pointer, if provided, is owned by the Rust side after the result is returned.
///   Do NOT free the chained task manually.
#[repr(C)]
pub struct ThryveTaskResult {
    /// The result's type (a null-terminated C string). NULL, or text that isn't valid UTF-8,
    /// becomes `"Error"`.
    pub result_type: *const c_char,
    /// The message to log (a null-terminated C string). NULL becomes `"No message"`.
    pub message: *const c_char,
    /// The log file to write the message to (a null-terminated C string). NULL, or text that isn't
    /// valid UTF-8, becomes `"Main"`.
    pub log_file: *const c_char,
    /// Whether `chain_task` holds a follow-up task to run next.
    pub has_chain: bool,
    /// The follow-up task, used only when `has_chain` is true. Rust takes ownership of it.
    pub chain_task: ThryveTask,
}

impl Default for ThryveTaskResult {
    fn default() -> Self {
        Self {
            result_type: std::ptr::null(),
            message: std::ptr::null(),
            log_file: std::ptr::null(),
            has_chain: false,
            chain_task: std::ptr::null_mut(),
        }
    }
}

impl ThryveTaskResult {
    /// Creates a new `ThryveTaskResult` with the specified result type, message, and log file.
    ///
    /// All string parameters must be null-terminated C strings that remain valid for the
    /// lifetime of the result. Use static/const strings to avoid lifetime issues.
    ///
    /// # Arguments
    ///
    /// * `result_type` - The result type (null-terminated C string)
    /// * `message` - The result message (null-terminated C string)
    /// * `log_file` - The log file name (null-terminated C string)
    ///
    /// # Returns
    ///
    /// A new `ThryveTaskResult` with `has_chain` set to `false` and `chain_task` set to NULL.
    ///
    /// # Example (C)
    ///
    /// ```c
    /// ThryveTaskResult result = thryve_task_result_new("Success", "Task completed", "Main");
    /// ```
    ///
    /// # Example (C++)
    ///
    /// ```cpp
    /// ThryveTaskResult result = thryve_task_result_new("Success", "Task completed", "Main");
    /// ```
    pub fn new(
        result_type: *const c_char,
        message: *const c_char,
        log_file: *const c_char,
    ) -> Self {
        Self {
            result_type,
            message,
            log_file,
            has_chain: false,
            chain_task: std::ptr::null_mut(),
        }
    }

    /// Creates a new `ThryveTaskResult` with a chained task.
    ///
    /// # Arguments
    ///
    /// * `result_type` - The result type (null-terminated C string)
    /// * `message` - The result message (null-terminated C string)
    /// * `log_file` - The log file name (null-terminated C string)
    /// * `chain_task` - The task to chain (opaque pointer to a Task instance)
    ///
    /// # Returns
    ///
    /// A new `ThryveTaskResult` with `has_chain` set to `true` and the specified chained task.
    ///
    /// # Example (C)
    ///
    /// ```c
    /// ThryveTaskResult result = thryve_task_result_new_with_chain("Chained", "Chaining to next task", "Main", my_chained_task);
    /// ```
    pub fn new_with_chain(
        result_type: *const c_char,
        message: *const c_char,
        log_file: *const c_char,
        chain_task: ThryveTask,
    ) -> Self {
        Self {
            result_type,
            message,
            log_file,
            has_chain: true,
            chain_task,
        }
    }
}

/// Converts an `ThryveTaskResult` to a `TaskResult`.
///
/// This function takes the FFI result pub struct and converts it into a proper
/// `TaskResult` that the Rust runtime can use. If the result contains a
/// chained task pointer, that task is taken ownership of and chained
/// to the result.
///
/// # Arguments
///
/// * `result` - The FFI task result to convert
///
/// # Returns
///
/// A `TaskResult` with the appropriate fields set based on the FFI result.
/// If `has_chain` is true and `chain_task` is not NULL, the chained task
/// is taken ownership of and attached to the result.
pub fn ffi_result_to_result(result: ThryveTaskResult) -> TaskResult {
    // Extract the string fields from FFI result, falling back to defaults
    let result_type = if result.result_type.is_null() {
        String::from("Error")
    } else {
        // Safety: result_type is only valid if not NULL, and the foreign language
        // guarantees the string remains valid for the lifetime of the result.
        // We also require the string to be valid UTF-8.
        unsafe { CStr::from_ptr(result.result_type) }
            .to_str()
            .unwrap_or("Error")
            .to_string()
    };

    let message = if result.message.is_null() {
        String::from("No message")
    } else {
        // Safety: result.message is only valid if not NULL
        unsafe { CStr::from_ptr(result.message) }
            .to_string_lossy()
            .into_owned()
    };

    let log_file = if result.log_file.is_null() {
        String::from("Main")
    } else {
        // Safety: result.log_file is only valid if not NULL
        unsafe { CStr::from_ptr(result.log_file) }
            .to_str()
            .unwrap_or("Main")
            .to_string()
    };

    let mut res = TaskResult::new(&result_type, message, &log_file);

    // Handle chained task if present
    if result.has_chain && !result.chain_task.is_null() {
        // Take ownership of the chained task from the FFI pointer
        let chained_task = unsafe { *Box::from_raw(result.chain_task as *mut Task) };
        res.next_task = Some(chained_task);
    }

    res
}

/// Creates a new ThryveTaskResult with the specified result type, message, and log file.
///
/// This is a helper function for foreign language code to create task results.
/// All string parameters must be null-terminated C strings that remain valid for
/// the lifetime of the result. Use static/const strings to avoid lifetime issues.
///
/// # Arguments
///
/// * `result_type` - The result type (null-terminated C string)
/// * `message` - The result message (null-terminated C string)
/// * `log_file` - The log file name (null-terminated C string)
///
/// # Returns
///
/// A new `ThryveTaskResult` with `has_chain` set to `false` and `chain_task` set to NULL.
///
/// # Example (C)
///
/// ```c
/// ThryveTaskResult result = thryve_task_result_new("Success", "Task completed", "Main");
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_task_result_new(
    result_type: *const c_char,
    message: *const c_char,
    log_file: *const c_char,
) -> ThryveTaskResult {
    ffi_guard(ThryveTaskResult::default(), || {
        ThryveTaskResult::new(result_type, message, log_file)
    })
}

/// Creates a new ThryveTaskResult with a chained task.
///
/// # Arguments
///
/// * `result_type` - The result type (null-terminated C string)
/// * `message` - The result message (null-terminated C string)
/// * `log_file` - The log file name (null-terminated C string)
/// * `chain_task` - The task to chain (opaque pointer to a Task instance)
///
/// # Returns
///
/// A new `ThryveTaskResult` with `has_chain` set to `true` and the specified chained task.
///
/// # Example (C)
///
/// ```c
/// ThryveTaskResult result = thryve_task_result_new_with_chain("Chained", "Chaining to next task", "Main", my_chained_task);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_task_result_new_with_chain(
    result_type: *const c_char,
    message: *const c_char,
    log_file: *const c_char,
    chain_task: ThryveTask,
) -> ThryveTaskResult {
    ffi_guard(ThryveTaskResult::default(), || {
        ThryveTaskResult::new_with_chain(result_type, message, log_file, chain_task)
    })
}

/// FFI-safe function pointer for task job — takes no arguments and returns a `ThryveTaskResult`.
///
/// Used for plugin entry points and task execution. The foreign language implementation
/// should return a `ThryveTaskResult` with `has_chain` set to `false` if no chained task follows.
pub type ThryveTaskJob = extern "C" fn() -> ThryveTaskResult;

/// FFI-safe function pointer for plugin cleanup — takes a pointer to the plugin's core data and
/// reports whether the cleanup worked.
///
/// Called once when the engine shuts down, on Thryve's plugin clean-up thread, after every task
/// has stopped. The `core_ptr` is the same pointer that was passed to `thryve_plugin_create` as
/// `plg_core_ptr`.
///
/// Return `ThryveStatus_Ok` (0) if the cleanup worked. Any other value means it failed: Thryve
/// logs that the plugin failed to clean up to `Error.log`, then goes on with the other plugins
/// and the shutdown. The return type is a plain `int` rather than `ThryveStatus`, so that any
/// value the callback returns is safe for Thryve to read.
pub type ThryvePluginCleanUp = extern "C" fn(*const u8) -> c_int;

/// FFI-safe function pointer for plugin entry point — returns a task result.
///
/// Used by `thryve_plugin_create` as the `entry_point` parameter. This function is called
/// when the plugin's task is executed.
pub type ThryvePluginEntryPoint = extern "C" fn() -> ThryveTaskResult;

/// FFI-safe function pointer for observer cleanup — takes a pointer to the observer's core data.
///
/// Called once during shutdown, on Thryve's observer clean-up thread. The `core_ptr` is the same
/// pointer that was passed to `thryve_observer_create` as `obs_core_ptr`.
pub type ThryveObserverCleanUp = extern "C" fn(*const u8);

/// FFI-safe function pointer for observer main loop — takes a pointer to the observer's core data.
///
/// **Currently unused** in the FFI layer. The primary observer callback is `ThryveObsJob`.
pub type ThryveObserverMainLoop = extern "C" fn(*const u8);

/// FFI-safe function pointer for observer job — takes an `ThryveObserver` handle and returns nothing.
///
/// This is the **primary callback** for observer execution. When an observer's main loop runs,
/// the Rust side:
/// 1. Clones the `Arc<Observer>` and converts to raw pointer
/// 2. Calls this function with the raw pointer
/// 3. The FFI implementation must convert the raw pointer back to an Arc and drop it when done
///
/// See the module-level documentation for the [Observer Job Memory Safety](#observer-job-memory-safety) details.
pub type ThryveObsJob = extern "C" fn(ThryveObserver);

/// FFI-safe function pointer for notification logic — takes a pointer to the notification's core data.
///
/// **Currently unused**: no FFI function takes one, so Thryve never calls it. A notification's
/// core data is passed to `thryve_notification_create` as `notif_core_ptr`.
pub type ThryveNotifLogic = extern "C" fn(*const u8);

// ============================================================================
// Config Wrappers
// ============================================================================

/// Creates a default configuration instance.
///
/// # Returns
///
/// An opaque pointer to the Config instance, or NULL on failure.
///
/// # Examples
///
/// ```c
/// ThryveConfig config = thryve_config_default();
/// thryve_config_free(config);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_config_default() -> ThryveConfig {
    ffi_guard(std::ptr::null_mut(), || {
        let config = Config::load_def_config();
        let boxed = Box::into_raw(Box::new(config)) as ThryveConfig;
        if boxed.is_null() {
            return std::ptr::null_mut();
        }
        boxed
    })
}

/// Creates a configuration instance from a TOML file.
///
/// # Arguments
///
/// * `path` - The path to the TOML configuration file (null-terminated C string)
///
/// # Returns
///
/// An opaque pointer to the Config instance, or NULL if `path` is NULL or not valid UTF-8, or the
/// file can't be read, doesn't parse, or has invalid values. There is no fallback to the
/// defaults: call [`thryve_config_default`] if you want them instead.
///
/// # Safety
///
/// `path` must be either null or a valid pointer to a null-terminated C
/// string, valid for reads for the duration of this call.
///
/// # Examples
///
/// ```c
/// ThryveConfig config = thryve_config_from_file("config.toml");
/// if (config == NULL) {
///     config = thryve_config_default(); // only if the defaults are acceptable
/// }
/// thryve_config_free(config);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_config_from_file(path: *const c_char) -> ThryveConfig {
    ffi_guard(std::ptr::null_mut(), || {
        if path.is_null() {
            return std::ptr::null_mut();
        }

        // Invalid UTF-8 can't name the file here, so it's rejected like a missing file.
        let Ok(path_str) = unsafe { CStr::from_ptr(path) }.to_str() else {
            return std::ptr::null_mut();
        };

        let Ok(config) = Config::load_config_from_file(path_str) else {
            return std::ptr::null_mut();
        };
        let boxed = Box::into_raw(Box::new(config)) as ThryveConfig;
        if boxed.is_null() {
            return std::ptr::null_mut();
        }
        boxed
    })
}

/// Frees a configuration instance.
///
/// # Arguments
///
/// * `config` - The opaque pointer to the Config instance to free.
///   If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// ThryveConfig config = thryve_config_default();
/// thryve_config_free(config);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_config_free(config: ThryveConfig) {
    ffi_guard((), || {
        if !config.is_null() {
            unsafe {
                let _ = Box::from_raw(config as *mut Config);
            }
        }
    })
}

// ============================================================================
// PacketHandle Wrappers
// ============================================================================

/// Frees a PacketHandle instance.
///
/// # Arguments
///
/// * `handle` - The opaque pointer to the PacketHandle instance to free.
///   If NULL, this function does nothing.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_packet_handle_free(handle: ThryvePacketHandle) {
    ffi_guard((), || {
        if !handle.is_null() {
            unsafe {
                let _ = Box::from_raw(handle as *mut PacketHandle);
            }
        }
    })
}

/// Clones a PacketHandle instance.
///
/// # Arguments
///
/// * `handle` - The opaque pointer to the PacketHandle instance to clone
///
/// # Returns
///
/// A new opaque pointer to the cloned PacketHandle instance, or NULL on failure.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_packet_handle_clone(handle: ThryvePacketHandle) -> ThryvePacketHandle {
    ffi_guard(std::ptr::null_mut(), || {
        if handle.is_null() {
            return std::ptr::null_mut();
        }

        let ph = unsafe { &*(handle as *const PacketHandle) };
        let cloned = ph.clone();
        let boxed = Box::into_raw(Box::new(cloned)) as ThryvePacketHandle;
        if boxed.is_null() {
            return std::ptr::null_mut();
        }
        boxed
    })
}

// ============================================================================
// Ctx Wrappers
// ============================================================================

/// Creates a context from a configuration instance.
///
/// This function sets up the communication channels and creates a Ctx instance
/// ready for use with the Thryve engine.
///
/// **Interactive mode only.** Service-mode callers should not call this — use the
/// `ctx` field returned by [`thryve_service_create`] instead, which is already wired
/// to the service's engine.
///
/// # Arguments
///
/// * `config` - The opaque pointer to the Config instance
///
/// # Returns
///
/// An opaque pointer to the Ctx instance, or NULL on failure.
///
/// # Examples
///
/// ```c
/// ThryveConfig config = thryve_config_default();
/// ThryveCtx ctx = thryve_ctx_create(config);
/// if (ctx != NULL) {
///     thryve_ctx_free(ctx);
/// }
/// thryve_config_free(config);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_create(config: ThryveConfig) -> ThryveCtx {
    ffi_guard(std::ptr::null_mut(), || {
        if config.is_null() {
            return std::ptr::null_mut();
        }

        let config = unsafe { &*(config as *const Config) };
        let ctx = inner_summon_requirements(*config);
        let boxed = Box::into_raw(Box::new(ctx)) as ThryveCtx;
        if boxed.is_null() {
            return std::ptr::null_mut();
        }
        boxed
    })
}

/// Frees a context instance.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance to free.
///   If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// ThryveCtx ctx = thryve_ctx_create(config);
/// thryve_ctx_free(ctx);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_free(ctx: ThryveCtx) {
    ffi_guard((), || {
        if !ctx.is_null() {
            unsafe {
                let _ = Box::from_raw(ctx as *mut Ctx);
            }
        }
    })
}

/// Adds a plugin to the context.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance
/// * `plugin` - The opaque pointer to the Plugin instance
///
/// # Examples
///
/// ```c
/// ThryveCtx ctx = thryve_ctx_create(config);
/// ThryvePlugin plugin = thryve_plugin_create(...);
/// thryve_ctx_add_plugin(ctx, plugin);
/// thryve_plugin_free(plugin);
/// thryve_ctx_free(ctx);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_add_plugin(ctx: ThryveCtx, plugin: ThryvePlugin) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if ctx.is_null() || plugin.is_null() {
            return ThryveStatus::NullArg;
        }

        unsafe {
            let ctx = &mut *(ctx as *mut Ctx);
            let plg = Box::from_raw(plugin as *mut Plugin);
            ctx.plugins.push(*plg);
        }

        ThryveStatus::Ok
    })
}

/// Adds an observer to the context.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance
/// * `observer` - The opaque pointer to the Observer instance
///
/// # Examples
///
/// ```c
/// ThryveCtx ctx = thryve_ctx_create(config);
/// ThryveObserver observer = thryve_observer_create(...);
/// thryve_ctx_add_observer(ctx, observer);
/// thryve_observer_free(observer);
/// thryve_ctx_free(ctx);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_add_observer(
    ctx: ThryveCtx,
    observer: ThryveObserver,
) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if ctx.is_null() || observer.is_null() {
            return ThryveStatus::NullArg;
        }

        unsafe {
            let ctx = &mut *(ctx as *mut Ctx);
            let obs = Box::from_raw(observer as *mut Observer);
            ctx.observers.push(*obs);
        }

        ThryveStatus::Ok
    })
}

/// Sets the logging file names for the context.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance
/// * `file_names` - Array of null-terminated C strings
/// * `count` - Number of file names
///
/// # Safety
///
/// `ctx` must be a valid, non-null `ThryveCtx` obtained from `thryve_ctx_create`
/// and not yet freed. `file_names` must be null or point to an array of at
/// least `count` pointers, each either null or a valid null-terminated C
/// string; the array and its strings must remain valid for reads for the
/// duration of this call.
///
/// # Examples
///
/// ```c
/// ThryveCtx ctx = thryve_ctx_create(config);
/// const char* log_files[] = {"ImgCompression", "Main"};
/// thryve_ctx_set_logging_file_names(ctx, log_files, 2);
/// thryve_ctx_free(ctx);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_ctx_set_logging_file_names(
    ctx: ThryveCtx,
    file_names: *const *const c_char,
    count: i32,
) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if ctx.is_null() || file_names.is_null() || count <= 0 {
            return ThryveStatus::NullArg;
        }

        unsafe {
            let ctx = &mut *(ctx as *mut Ctx);
            for i in 0..count {
                let name_ptr = *file_names.offset(i as isize);
                if !name_ptr.is_null() {
                    let name_str = CStr::from_ptr(name_ptr as *const c_char)
                        .to_str()
                        .unwrap_or("")
                        .to_string();
                    ctx.logging_file_names.push(name_str);
                }
            }
        }

        ThryveStatus::Ok
    })
}

/// Turns the result of sending a task or notification into a status for C.
///
/// A send fails only once the engine is gone (it stopped, or its context was freed without
/// starting it), which is `InvalidState`. The task or notification is dropped either way.
fn send_result_status(result: Result<(), ThryveError>) -> ThryveStatus {
    match result {
        Ok(()) => ThryveStatus::Ok,
        Err(ThryveError::EngineNotRunning(_)) => ThryveStatus::InvalidState,
        Err(_) => ThryveStatus::OperationFail,
    }
}

/// Sends a task to the engine via the context's packet handle.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance
/// * `task` - The opaque pointer to the Task instance. It is consumed, even when the send fails.
///
/// # Returns
///
/// * `ThryveStatus::Ok` if the task was sent successfully
/// * `ThryveStatus::NullArg` if ctx or task is NULL
/// * `ThryveStatus::InvalidState` if the engine isn't running; the task is dropped
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_send_task(ctx: ThryveCtx, task: ThryveTask) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if ctx.is_null() || task.is_null() {
            return ThryveStatus::NullArg;
        }

        unsafe {
            let ctx = &*(ctx as *const Ctx);
            let task = Box::from_raw(task as *mut Task);
            send_result_status(ctx.packet_handle().send_task(*task))
        }
    })
}

/// Returns a clone of the PacketHandle from the context.
///
/// This allows the foreign language to send tasks directly to the engine
/// using the engine's own PacketHandle, rather than using a separate one
/// created from the Config.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance
///
/// # Returns
///
/// A clone of the PacketHandle (an opaque pointer that should be freed with
/// `thryve_packet_handle_free`), or NULL if ctx is NULL.
///
/// # Example
///
/// ```c
/// ThryveCtx ctx = thryve_ctx_create(config);
/// ThryvePacketHandle pkt = thryve_ctx_get_packet_handle(ctx);
/// if (pkt != NULL) {
///     // Use pkt to send tasks directly to the engine
///     thryve_ctx_send_task(ctx, task);
///     // or
///     thryve_packet_handle_send_task(pkt, task);
///     thryve_packet_handle_free(pkt);
/// }
/// thryve_ctx_free(ctx);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_get_packet_handle(ctx: ThryveCtx) -> ThryvePacketHandle {
    ffi_guard(std::ptr::null_mut(), || {
        if ctx.is_null() {
            return std::ptr::null_mut();
        }

        unsafe {
            let ctx = &*(ctx as *const Ctx);
            let cloned = ctx.packet_handle().clone();
            let boxed = Box::into_raw(Box::new(cloned)) as ThryvePacketHandle;
            if boxed.is_null() {
                return std::ptr::null_mut();
            }
            boxed
        }
    })
}

// ============================================================================
// PacketHandle Task/Notification Senders
// ============================================================================

/// Sends a task using a PacketHandle directly.
///
/// # Arguments
///
/// * `handle` - The opaque pointer to the PacketHandle instance
/// * `task` - The opaque pointer to the Task instance. It is consumed, even when the send fails.
///
/// # Returns
///
/// * `ThryveStatus::Ok` if the task was sent successfully
/// * `ThryveStatus::NullArg` if handle or task is NULL
/// * `ThryveStatus::InvalidState` if the engine isn't running; the task is dropped
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_packet_handle_send_task(
    handle: ThryvePacketHandle,
    task: ThryveTask,
) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if handle.is_null() || task.is_null() {
            return ThryveStatus::NullArg;
        }

        unsafe {
            let pkt = &*(handle as *const PacketHandle);
            let task = Box::from_raw(task as *mut Task);
            send_result_status(pkt.send_task(*task))
        }
    })
}

/// Sends a notification using a PacketHandle directly.
///
/// # Arguments
///
/// * `handle` - The opaque pointer to the PacketHandle instance
/// * `notif` - The opaque pointer to the Notification instance. It is consumed, even when the
///   send fails.
///
/// # Returns
///
/// * `ThryveStatus::Ok` if the notification was sent successfully
/// * `ThryveStatus::NullArg` if handle or notif is NULL
/// * `ThryveStatus::InvalidState` if the engine isn't running; the notification is dropped
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_packet_handle_send_notification(
    handle: ThryvePacketHandle,
    notif: ThryveNotif,
) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if handle.is_null() || notif.is_null() {
            return ThryveStatus::NullArg;
        }

        unsafe {
            let pkt = &*(handle as *const PacketHandle);
            let n = Box::from_raw(notif as *mut Notif);
            send_result_status(pkt.send_notification(*n))
        }
    })
}

// ============================================================================
// Task Wrappers
// ============================================================================

/// Creates a new task with the specified parameters.
///
/// # Arguments
///
/// * `name` - The task name (null-terminated C string)
/// * `priority` - The task priority (0-14)
/// * `job` - The task job function pointer
///
/// # Returns
///
/// An opaque pointer to the Task instance, or NULL on failure, including when
/// `name` is empty or contains control characters.
///
/// # Safety
///
/// `name` must be either null or a valid pointer to a null-terminated C
/// string, valid for reads for the duration of this call. `job` must be a
/// valid, non-null function pointer that is safe to call with no arguments.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_task_create(
    name: *const c_char,
    priority: u8,
    job: ThryveTaskJob,
) -> ThryveTask {
    ffi_guard(std::ptr::null_mut(), || {
        if name.is_null() {
            return std::ptr::null_mut();
        }

        unsafe {
            let task_name = CStr::from_ptr(name)
                .to_str()
                .unwrap_or("Unknown")
                .to_string();
            // Wrap the FFI function pointer in a closure that converts its result
            let task_job = move || {
                let ffi_result = (job)();
                // Convert ThryveTaskResult to TaskResult
                ffi_result_to_result(ffi_result)
            };
            let Ok(task) = Task::new(&task_name, priority, task_job) else {
                return std::ptr::null_mut();
            };

            let boxed = Box::into_raw(Box::new(task)) as ThryveTask;
            if boxed.is_null() {
                return std::ptr::null_mut();
            }
            boxed
        }
    })
}

/// Frees a task instance.
///
/// # Arguments
///
/// * `task` - The opaque pointer to the Task instance to free.
///   If NULL, this function does nothing.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_task_free(task: ThryveTask) {
    ffi_guard((), || {
        if !task.is_null() {
            unsafe {
                let _ = Box::from_raw(task as *mut Task);
            }
        }
    })
}

// ============================================================================
// Run Interactive Wrappers
// ============================================================================

/// Starts the Thryve engine in interactive mode.
///
/// This function spawns a thread that runs the engine in interactive mode.
/// The returned handle can be used to track the thread's execution.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance. **Ownership is
///   transferred**: this function consumes `ctx` (via `Box::from_raw`) and
///   hands it to the spawned engine thread. Do not call `thryve_ctx_free` on it
///   afterward - that would be a double-free.
///
/// # Returns
///
/// An opaque pointer to the JoinHandle, or NULL if `ctx` is NULL or the engine couldn't start. A
/// failed start stops every thread it started, and writes its error to `Error.log` once the log
/// folder exists.
///
/// # Examples
///
/// ```c
/// ThryveCtx ctx = thryve_ctx_create(config);
/// ThryvePacketHandle pkt = thryve_ctx_get_packet_handle(ctx);
/// ThryveRunHandle handle = thryve_run_interactive_start(ctx); // ctx is consumed here
/// if (handle != NULL) {
///     thryve_run_interactive_free(handle);
/// }
/// thryve_packet_handle_free(pkt);
/// // Do NOT call thryve_ctx_free(ctx) here - ctx was already consumed above.
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_run_interactive_start(ctx: ThryveCtx) -> ThryveRunHandle {
    ffi_guard(std::ptr::null_mut(), || {
        if ctx.is_null() {
            return std::ptr::null_mut();
        }

        let ctx = unsafe { Box::from_raw(ctx as *mut Ctx) };
        let ctx_val = *ctx;

        // A failed startup has already cleaned up after itself and written its error to
        // Error.log (when the log folder exists); the C caller only sees NULL.
        match inner_run_interactive(ctx_val) {
            Ok(h) => {
                let boxed = Box::into_raw(Box::new(h)) as ThryveRunHandle;
                if boxed.is_null() {
                    return std::ptr::null_mut();
                }
                boxed
            }
            Err(_) => std::ptr::null_mut(),
        }
    })
}

/// Waits for the interactive engine to finish and frees the handle.
///
/// # Arguments
///
/// * `handle` - The opaque pointer to the JoinHandle.
///   If NULL, this function does nothing.
///
/// # Returns
///
/// * `ThryveStatus::Ok` if the handle was joined successfully
/// * `ThryveStatus::NullArg` if handle is NULL
///
/// # Examples
///
/// ```c
/// ThryveRunHandle handle = thryve_run_interactive_start(ctx);
/// if (handle != NULL) {
///     thryve_run_interactive_free(handle);
/// }
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_run_interactive_free(handle: ThryveRunHandle) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if handle.is_null() {
            return ThryveStatus::NullArg;
        }

        unsafe {
            let h = Box::from_raw(handle as *mut JoinHandle<()>);
            let _ = h.join();
        }

        ThryveStatus::Ok
    })
}

// ============================================================================
// Service Wrappers
// ============================================================================

/// The result of [`thryve_service_create`]: the service instance paired with its
/// matching Ctx.
///
/// Add plugins/observers to `ctx` (e.g. via [`thryve_ctx_add_plugin`]/[`thryve_ctx_add_observer`]),
/// then pass both `service` and `ctx` to [`thryve_service_run`]. Do not call [`thryve_ctx_create`]
/// for service mode — `ctx` here is already the one the service will run.
///
/// If `config` was NULL, both fields are NULL.
#[repr(C)]
pub struct ThryveServiceCreateResult {
    /// The service instance, or NULL if `config` was NULL.
    pub service: ThryveService,
    /// The service's context, or NULL if `config` was NULL.
    pub ctx: ThryveCtx,
}

/// Creates a service instance from a configuration, together with its Ctx.
///
/// This function constructs a Thryve service ready for execution as a Windows service,
/// along with the Ctx that must be passed to [`thryve_service_run`] alongside it.
///
/// # Arguments
///
/// * `config` - The opaque pointer to the Config instance
///
/// # Returns
///
/// An [`ThryveServiceCreateResult`] with both `service` and `ctx` set, or both NULL on failure.
///
/// # Examples
///
/// ```c
/// ThryveConfig config = thryve_config_default();
/// ThryveServiceCreateResult created = thryve_service_create(config);
/// if (created.service != NULL && created.ctx != NULL) {
///     thryve_service_run(created.service, created.ctx);
/// }
/// thryve_config_free(config);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_service_create(config: ThryveConfig) -> ThryveServiceCreateResult {
    ffi_guard(
        ThryveServiceCreateResult {
            service: std::ptr::null_mut(),
            ctx: std::ptr::null_mut(),
        },
        || {
            if config.is_null() {
                return ThryveServiceCreateResult {
                    service: std::ptr::null_mut(),
                    ctx: std::ptr::null_mut(),
                };
            }

            let config = unsafe { &*(config as *const Config) };
            let (service, ctx) = inner_create_service(*config);

            let service_boxed = Box::into_raw(Box::new(service)) as ThryveService;
            let ctx_boxed = Box::into_raw(Box::new(ctx)) as ThryveCtx;

            ThryveServiceCreateResult {
                service: service_boxed,
                ctx: ctx_boxed,
            }
        },
    )
}

/// Runs the service with the given context.
///
/// This function starts the service and blocks until the service is stopped. It consumes both
/// `service` and `ctx`, so don't free them afterwards.
///
/// # Arguments
///
/// * `service` - The opaque pointer to the Thryve service instance
/// * `ctx` - The opaque pointer to the Ctx instance
///
/// # Returns
///
/// * `ThryveStatus::Ok` if the service ran and has stopped
/// * `ThryveStatus::NullArg` if either pointer is NULL
/// * `ThryveStatus::InvalidState` if the process wasn't started by the Windows service manager
/// * `ThryveStatus::OperationFail` if the engine couldn't start (for example, a log folder or a
///   thread couldn't be created)
///
/// A failed start stops every thread it started, and writes its error to `Error.log` once the log
/// folder exists.
///
/// # Examples
///
/// ```c
/// ThryveConfig config = thryve_config_default();
/// ThryveServiceCreateResult created = thryve_service_create(config);
/// if (created.service != NULL && created.ctx != NULL) {
///     thryve_service_run(created.service, created.ctx);
/// }
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_service_run(service: ThryveService, ctx: ThryveCtx) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if service.is_null() || ctx.is_null() {
            return ThryveStatus::NullArg;
        }

        // Take ownership of the service and ctx
        let service = unsafe { Box::from_raw(service as *mut Thryve) };
        let ctx = unsafe { Box::from_raw(ctx as *mut Ctx) };

        // Run the service (this is a blocking operation)
        match inner_try_run_as_service(*service, *ctx) {
            Ok(()) => ThryveStatus::Ok,
            // The service manager didn't start this process, so the call was made in the wrong
            // situation.
            Err(ThryveError::Startup(StartupError::Service { .. })) => ThryveStatus::InvalidState,
            Err(_) => ThryveStatus::OperationFail,
        }
    })
}

/// Frees a service instance.
///
/// # Arguments
///
/// * `service` - The opaque pointer to the Thryve service instance to free.
///   If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// ThryveServiceCreateResult created = thryve_service_create(config);
/// thryve_service_free(created.service);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_service_free(service: ThryveService) {
    ffi_guard((), || {
        if !service.is_null() {
            unsafe {
                let _ = Box::from_raw(service as *mut Thryve);
            }
        }
    })
}

// ============================================================================
// Plugin Wrappers
// ============================================================================

/// Creates a new plugin with the specified parameters.
///
/// This function allows foreign languages to create plugins by providing
/// function pointers for the plugin's logic. The foreign language implementation
/// should maintain a pointer to its own plugin core data, which will be
/// passed to the callback functions.
///
/// # Arguments
///
/// * `plg_type` - The plugin type (null-terminated C string), which identifies the plugin and must be
///   unique. **Consumed** — do not free after calling.
/// * `plg_core_ptr` - An opaque pointer to the plugin core data maintained by the foreign language.
///   This pointer is passed back to `clean_up` and is available to the plugin's logic.
/// * `tx` - The opaque pointer to the PacketHandle instance. This is **cloned** inside the plugin,
///   so the PacketHandle remains valid after this call and can be used elsewhere.
/// * `clean_up` - Function pointer called once when the engine shuts down, with `plg_core_ptr`
///   as its argument. It returns `ThryveStatus_Ok` if the cleanup worked, and any other value if
///   it failed; a failure is logged to `Error.log`. See [`ThryvePluginCleanUp`].
/// * `entry_point` - Function pointer that returns the task execution result.
///   **Takes ownership** — do not free this pointer after calling.
///
/// # Returns
///
/// An opaque pointer to the Plugin instance, or NULL on failure, including when
/// `plg_type` is empty or contains control characters.
///
/// # Safety
///
/// `plg_type` must be either null or a valid pointer to a null-terminated
/// C string, valid for reads for the duration of this call. `tx` must be a valid, non-null `ThryvePacketHandle`. `plg_core_ptr`
/// is an opaque pointer whose validity for the plugin's entire lifetime is
/// the caller's responsibility. `clean_up` and `entry_point` must be valid
/// function pointers matching their declared signatures, safe to call from
/// any thread for as long as the returned plugin is alive.
///
/// # PacketHandle Usage
///
/// To use `tx`, get the engine's connected PacketHandle from a Ctx:
///
/// ```c
/// // Create a configuration and context first
/// ThryveConfig config = thryve_config_default();
/// ThryveCtx ctx = thryve_ctx_create(config);
///
/// // Get the engine's connected PacketHandle from ctx
/// ThryvePacketHandle packet_handle = thryve_ctx_get_packet_handle(ctx);
///
/// // Now use it when creating the plugin
/// ThryvePlugin plugin = thryve_plugin_create(
///     "MyType",
///     my_core_data,
///     packet_handle,  // <-- use the PacketHandle
///     my_plugin_clean_up,
///     my_plugin_entry_point
/// );
///
/// // The PacketHandle is still valid and can be used for other plugins/observers
/// thryve_packet_handle_free(packet_handle);
/// thryve_config_free(config);
/// ```
///
/// # Examples
///
/// ```c
/// // In C, you would define your plugin logic functions:
/// int my_plugin_clean_up(const void* core_ptr) {
///     // Cleanup logic here
///     if (!flush_my_cache(core_ptr)) {
///         return ThryveStatus_OperationFail;  // logged to Error.log
///     }
///     return ThryveStatus_Ok;
/// }
///
/// ThryveTaskResult my_plugin_entry_point(void) {
///     ThryveTaskResult result = {0};
///     // Execute plugin logic here
///     return result;
/// }
///
/// // Then create the plugin:
/// ThryvePlugin plugin = thryve_plugin_create(
///     "MyType",
///     my_core_data,
///     packet_handle,
///     my_plugin_clean_up,
///     my_plugin_entry_point
/// );
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_plugin_create(
    plg_type: *const c_char,
    plg_core_ptr: *mut u8,
    tx: ThryvePacketHandle,
    clean_up: ThryvePluginCleanUp,
    entry_point: ThryvePluginEntryPoint,
) -> ThryvePlugin {
    ffi_guard(std::ptr::null_mut(), || {
        if plg_type.is_null() || tx.is_null() {
            return std::ptr::null_mut();
        }

        let plg_type = unsafe { CStr::from_ptr(plg_type) }
            .to_str()
            .unwrap_or("Unknown");

        // Create the packet handle clone
        let packet_handle = unsafe { &*(tx as *const PacketHandle) }.clone();

        // Wrap the FFI function pointer in a closure that converts its result
        let task_job = move || {
            let ffi_result = (entry_point)();
            // Convert ThryveTaskResult to TaskResult
            ffi_result_to_result(ffi_result)
        };

        // Create the PlgLogic adapter - PluginLogicAdapter already implements PlgLogic
        let adapter: Arc<dyn PlgLogic> =
            Arc::new(PluginLogicAdapter::new(plg_type, plg_core_ptr, clean_up));

        let Ok(plugin) = Plugin::new(plg_type, adapter, packet_handle, task_job) else {
            return std::ptr::null_mut();
        };

        let boxed = Box::into_raw(Box::new(plugin)) as ThryvePlugin;

        if boxed.is_null() {
            println!("  boxed is null!");
            return std::ptr::null_mut();
        }
        boxed
    })
}

/// Frees a plugin instance.
///
/// # Arguments
///
/// * `plugin` - The opaque pointer to the Plugin instance to free.
///   If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// ThryvePlugin plugin = thryve_plugin_create(...);
/// thryve_ctx_add_plugin(ctx, plugin);
/// thryve_plugin_free(plugin);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_plugin_free(plugin: ThryvePlugin) {
    ffi_guard((), || {
        if !plugin.is_null() {
            unsafe {
                let _ = Box::from_raw(plugin as *mut Plugin);
            }
        }
    })
}

// ============================================================================
// Observer Wrappers
// ============================================================================

/// Creates a new observer with the specified parameters.
///
/// This function allows foreign languages to create observers by providing
/// function pointers for the observer's logic. The foreign language implementation
/// should maintain a pointer to its own observer core data, which will be
/// passed to the callback functions.
///
/// # Arguments
///
/// * `obs_type` - The observer type (null-terminated C string), which identifies the observer and
///   must be unique. **Consumed** — do not free after calling.
/// * `obs_core_ptr` - An opaque pointer to the observer core data maintained by the foreign language.
///   This pointer is passed back to `clean_up`.
/// * `tx` - The opaque pointer to the PacketHandle instance. This is **cloned** inside the observer,
///   so the PacketHandle remains valid after this call and can be used elsewhere.
/// * `clean_up` - Function pointer called when the observer is cleaned up.
///   The `obs_core_ptr` is passed as an argument.
/// * `obs_job_fn` - Function pointer for the observer job (takes `ThryveObserver`, returns nothing).
///   **This is the primary callback** for observer execution. The `ThryveObserver` handle is a raw pointer
///   to an `Arc<Observer>` — see [Observer Job Memory Safety](#observer-job-memory-safety) for details.
///
/// # Returns
///
/// An opaque pointer to the Observer instance, or NULL on failure, including when
/// `obs_type` is empty or contains control characters.
///
/// # Safety
///
/// `obs_type` must be either null or a valid pointer to a null-terminated
/// C string, valid for reads for the duration of this call. `tx` must be a valid, non-null `ThryvePacketHandle`. `obs_core_ptr`
/// is an opaque pointer whose validity for the observer's entire lifetime
/// is the caller's responsibility. `clean_up` and `obs_job_fn` must be
/// valid function pointers matching their declared signatures, safe to
/// call from any thread for as long as the returned observer is alive.
///
/// # PacketHandle Usage
///
/// To use `tx`, get the engine's connected PacketHandle from a Ctx:
///
/// ```c
/// // Create a configuration and context first
/// ThryveConfig config = thryve_config_default();
/// ThryveCtx ctx = thryve_ctx_create(config);
///
/// // Get the engine's connected PacketHandle from ctx
/// ThryvePacketHandle packet_handle = thryve_ctx_get_packet_handle(ctx);
///
/// // Now use it when creating the observer
/// ThryveObserver observer = thryve_observer_create(
///     "MyType",
///     my_core_data,
///     packet_handle,  // <-- use the PacketHandle
///     my_observer_clean_up,
///     my_observer_job
/// );
///
/// // The PacketHandle is still valid and can be used for other plugins/observers
/// thryve_packet_handle_free(packet_handle);
/// thryve_config_free(config);
/// ```
///
/// # Examples
///
/// ```c
/// // In C++, you would define your observer logic functions:
/// void my_observer_clean_up(const void* core_ptr) {
///     // Cleanup logic here
/// }
///
/// void my_observer_job(ThryveObserver obs) {
///     // Observer job logic here
/// }
///
/// // Then create the observer:
/// ThryveObserver observer = thryve_observer_create(
///     "MyType",
///     my_core_data,
///     packet_handle,
///     my_observer_clean_up,
///     my_observer_job
/// );
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_observer_create(
    obs_type: *const c_char,
    obs_core_ptr: *mut u8,
    tx: ThryvePacketHandle,
    clean_up: ThryveObserverCleanUp,
    obs_job_fn: ThryveObsJob,
) -> ThryveObserver {
    ffi_guard(std::ptr::null_mut(), || {
        if obs_type.is_null() || tx.is_null() {
            return std::ptr::null_mut();
        }

        let obs_type = unsafe { CStr::from_ptr(obs_type) }
            .to_str()
            .unwrap_or("Unknown");

        // Create the packet handle clone
        let packet_handle = unsafe { &*(tx as *const PacketHandle) }.clone();

        // Wrap the raw pointer in ObsCorePtr to make it Send + Sync for the closure
        let obs_core_ptr_wrapper = ObsCorePtr(obs_core_ptr);

        // Create the ObsJob wrapper - capture the FFI function pointer
        let obs_job = ObsJob::new(move |observer: Arc<Observer>| {
            // Clone the Arc before passing the raw pointer to the FFI callback.
            // This ensures the original Arc is dropped when the closure returns,
            // even if the FFI callback crashes (preventing memory leaks).
            // The FFI callback owns the cloned Arc and must drop it when done.
            let ffi_observer = Arc::into_raw(observer.clone()) as ThryveObserver;
            (obs_job_fn)(ffi_observer);
            // The original Arc is dropped here, but the FFI callback still holds
            // its copy, so the Observer stays alive during the callback execution.
        });

        // Create the ObsLogic adapter - ObserverLogicAdapter already implements ObsLogic
        let adapter: Arc<dyn ObsLogic> =
            Arc::new(ObserverLogicAdapter::new(obs_core_ptr_wrapper.0, clean_up));

        // Create the Observer
        let Ok(observer) = Observer::new(obs_type, adapter, obs_job, packet_handle) else {
            return std::ptr::null_mut();
        };

        let boxed = Box::into_raw(Box::new(observer)) as ThryveObserver;
        if boxed.is_null() {
            return std::ptr::null_mut();
        }
        boxed
    })
}

/// Frees an observer instance.
///
/// # Arguments
///
/// * `observer` - The opaque pointer to the Observer instance to free.
///   If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// ThryveObserver observer = thryve_observer_create(...);
/// thryve_ctx_add_observer(ctx, observer);
/// thryve_observer_free(observer);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_observer_free(observer: ThryveObserver) {
    ffi_guard((), || {
        if !observer.is_null() {
            unsafe {
                let _ = Box::from_raw(observer as *mut Observer);
            }
        }
    })
}

// ============================================================================
// Notification Wrappers
// ============================================================================

/// Creates a new notification with the specified parameters.
///
/// This function allows foreign languages to create notifications by providing
/// a function pointer for the notification logic. The foreign language implementation
/// should maintain a pointer to its own notification core data.
///
/// # Arguments
///
/// * `notif_type` - The notification type (null-terminated C string)
/// * `notif_receiver` - The notification receiver type (null-terminated C string)
/// * `notif_core_ptr` - An opaque pointer to the notification core data maintained by the foreign language
///
/// # Returns
///
/// An opaque pointer to the Notif instance, or NULL on failure, including when
/// `notif_type` or `notif_receiver` is empty or contains control characters.
///
/// # Safety
///
/// `notif_type` and `notif_receiver` must be either null or valid pointers
/// to null-terminated C strings, valid for reads for the duration of this
/// call. `notif_core_ptr` is an opaque pointer whose validity for the
/// notification's entire lifetime is the caller's responsibility.
///
/// # Examples
///
/// ```c
/// // In C, you would define your notification logic function:
/// void my_notif_logic(const void* core_ptr) {
///     // Notification processing logic here
/// }
///
/// // Then create the notification:
/// ThryveNotif notif = thryve_notification_create(
///     "MyNotifType",
///     "MyReceiverType",
///     my_core_data
/// );
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_notification_create(
    notif_type: *const c_char,
    notif_receiver: *const c_char,
    notif_core_ptr: *mut u8,
) -> ThryveNotif {
    ffi_guard(std::ptr::null_mut(), || {
        if notif_type.is_null() || notif_receiver.is_null() {
            return std::ptr::null_mut();
        }

        let notif_type = unsafe { CStr::from_ptr(notif_type) }
            .to_str()
            .unwrap_or("Unknown");
        let notif_receiver = unsafe { CStr::from_ptr(notif_receiver) }
            .to_str()
            .unwrap_or("Unknown");

        // Create the NotifLogic adapter
        let notif_core: Box<dyn NotifLogic> =
            Box::new(NotificationLogicAdapter::new(notif_core_ptr));

        // Create the notification
        let Ok(notif) = Notif::new(notif_type, notif_receiver, notif_core) else {
            return std::ptr::null_mut();
        };

        let boxed = Box::into_raw(Box::new(notif)) as ThryveNotif;
        if boxed.is_null() {
            return std::ptr::null_mut();
        }
        boxed
    })
}

/// Frees a notification instance.
///
/// # Arguments
///
/// * `notif` - The opaque pointer to the Notif instance to free.
///   If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// ThryveNotif notif = thryve_notification_create(...);
/// // ... use notif ...
/// thryve_notification_free(notif);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_notification_free(notif: ThryveNotif) {
    ffi_guard((), || {
        if !notif.is_null() {
            unsafe {
                let _ = Box::from_raw(notif as *mut Notif);
            }
        }
    })
}

/// Sends a notification to the engine via the context's packet handle.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance
/// * `notif` - The opaque pointer to the Notif instance. It is consumed, even when the send fails.
///
/// # Returns
///
/// * `ThryveStatus::Ok` if the notification was sent successfully
/// * `ThryveStatus::NullArg` if ctx or notif is NULL
/// * `ThryveStatus::InvalidState` if the engine isn't running; the notification is dropped
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_send_notification(ctx: ThryveCtx, notif: ThryveNotif) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if ctx.is_null() || notif.is_null() {
            return ThryveStatus::NullArg;
        }

        unsafe {
            let ctx = &*(ctx as *const Ctx);
            let notif = Box::from_raw(notif as *mut Notif);
            send_result_status(ctx.packet_handle().send_notification(*notif))
        }
    })
}

// ============================================================================
// FFI Adapter Implementations
// ============================================================================

/// Adapter for PlgLogic trait to support FFI callbacks.
pub struct PluginLogicAdapter {
    /// The plugin's type, which a failed cleanup names in its error.
    pub plg_type: String,
    /// The foreign plugin's own data, passed back to `clean_up`.
    pub core_ptr: *mut u8,
    /// The foreign cleanup callback, called with `core_ptr`.
    pub clean_up: ThryvePluginCleanUp,
}

impl PluginLogicAdapter {
    /// Wraps a foreign plugin's type, data pointer and cleanup callback.
    pub fn new(plg_type: &str, core_ptr: *mut u8, clean_up: ThryvePluginCleanUp) -> Self {
        Self {
            plg_type: plg_type.to_string(),
            core_ptr,
            clean_up,
        }
    }
}

unsafe impl Send for PluginLogicAdapter {}
unsafe impl Sync for PluginLogicAdapter {}

impl PlgLogic for PluginLogicAdapter {
    /// Calls the foreign cleanup callback.
    ///
    /// # Errors
    ///
    /// Returns [`FinalizationError::PluginCleanup`], naming the plugin's type, if the callback
    /// returns anything other than `ThryveStatus_Ok`.
    fn clean_up(&self) -> Result<(), ThryveError> {
        // Function pointers are always valid if defined, no null check needed
        let status = (self.clean_up)(self.core_ptr);

        if status != ThryveStatus::Ok as c_int {
            return Err(FinalizationError::PluginCleanup {
                plugin_type: self.plg_type.clone(),
            }
            .into());
        }

        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Adapter for ObsLogic trait to support FFI callbacks.
pub struct ObserverLogicAdapter {
    /// The foreign observer's own data, passed back to `clean_up`.
    pub core_ptr: *mut u8,
    /// The foreign cleanup callback, called with `core_ptr`.
    pub clean_up: ThryveObserverCleanUp,
}

impl ObserverLogicAdapter {
    /// Wraps a foreign observer's data pointer and cleanup callback.
    pub fn new(core_ptr: *mut u8, clean_up: ThryveObserverCleanUp) -> Self {
        Self { core_ptr, clean_up }
    }
}

unsafe impl Send for ObserverLogicAdapter {}
unsafe impl Sync for ObserverLogicAdapter {}

impl ObsLogic for ObserverLogicAdapter {
    fn clean_up(&self, _observer: Arc<Observer>) {
        // Function pointers are always valid if defined, no null check needed
        (self.clean_up)(self.core_ptr);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Adapter for NotifLogic trait to support FFI callbacks.
pub struct NotificationLogicAdapter {
    /// The foreign notification's own data.
    pub core_ptr: *mut u8,
}

impl NotificationLogicAdapter {
    /// Wraps a foreign notification's data pointer.
    pub fn new(core_ptr: *mut u8) -> Self {
        Self { core_ptr }
    }
}

unsafe impl Send for NotificationLogicAdapter {}
unsafe impl Sync for NotificationLogicAdapter {}

impl thryve_utils::traits::notif_logic::NotifLogic for NotificationLogicAdapter {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Wrapper for *mut u8 that implements Send + Sync so it can be captured by closures
/// used in ObsJob (which requires Send + Sync).
pub struct ObsCorePtr(pub *mut u8);

unsafe impl Send for ObsCorePtr {}
unsafe impl Sync for ObsCorePtr {}
