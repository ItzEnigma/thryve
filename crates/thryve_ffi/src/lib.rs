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
//! enabling integration with other programming languages, such as C, C++ and Python.
//!
//! # Design Principles
//!
//! - All complex Rust types are wrapped in opaque pointers for FFI safety
//! - Memory management is explicit: every `create` function has a corresponding `free` function
//! - Functions use `extern "C"` calling convention for cross-language compatibility
//! - Error handling uses nullable pointers and status codes, and [`thryve_last_error`] tells why
//!   the latest call on the thread failed
//! - Callbacks follow a specific ownership model — see the [Callback Types](#callback-types) section
//! - Every wrapper's body runs through `ffi_guard`, so a panic anywhere in the
//!   call chain becomes a null/error return instead of unwinding across the FFI
//!   boundary into the foreign caller (which is undefined behavior). Once an
//!   engine has started, though, Thryve's panic hook logs a panic and ends the
//!   process before the guard sees it
//!
//! Typed tasks, whose job returns a value or an error, and the follow-ups, joins, splits, cancels
//! and fan-out groups built on them, are in [`typed_tasks`].
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
//! - **`ThryveTaskJob`**: a plain task's job. It gets the task's result to fill in, and the user
//!   data given to `thryve_task_create`.
//! - **`ThryvePluginEntryPoint`**: a plugin's entry point, run once when the engine starts. It gets
//!   a result to fill in, like a task's job, and the plugin's core pointer.
//! - **`ThryveTaskResult`**: what a job reports, filled in through the pointer it gets. It starts
//!   empty, which reports nothing:
//!   - `result_type`: the result's type, such as `"Saved"`; `"Fin"` stops the engine
//!   - `message`: a message to log
//!   - `log_file`: the log file to write the message to, such as `"Main"`
//!   - `has_chain`: whether `chain_task` should run next
//!   - `chain_task`: a task to run next, which Thryve takes over
//! - **`ThryvePluginCleanUp`**: A function pointer that takes a `const void*` (the plugin's core pointer)
//!   and returns an `int`: `ThryveStatus_Ok` (0) if the cleanup worked, any other value if it failed.
//!   Called once when the engine shuts down. A failure is logged to `Error.log`.
//!
//! Jobs and entry points fill in a result instead of returning one, so that languages whose
//! callbacks can't return a struct, such as Python's ctypes, can write them.
//!
//! A task's job, a plugin's entry point and an observer's job are required: a create given NULL
//! for one returns NULL, and [`thryve_last_error`] names it. A clean-up or a free function may be
//! NULL, when there's nothing to clean up or free.
//!
//! ## Typed Task Callbacks
//!
//! - **`ThryveAdvJob`**, **`ThryveThenJob`**, **`ThryveSplitJob`**, **`ThryveChunkJob`**: the jobs
//!   of typed tasks, follow-ups, joins, fan-out followers and split pieces. Each gets its user
//!   data, and returns a `ThryveOutcome`: a value, an error, or a task to forward to.
//! - **`ThryveFreeFn`**: frees user data, a value or a chunk once Thryve is done with it.
//!
//! See [`typed_tasks`] for who owns what.
//!
//! ## Observer Callbacks
//!
//! - **`ThryveObsJob`**: The observer's job: its whole loop, called once on the observer's own
//!   thread. It gets the observer, which it borrows until it returns, and receives the observer's
//!   notifications with [`thryve_observer_pop`] — see [Observer Jobs](#observer-jobs).
//! - **`ThryveObserverCleanUp`**: A function pointer that takes a `const void*` (the observer's core pointer).
//!   Called when the observer is being cleaned up.
//!
//! ### Observer Jobs
//!
//! The engine starts an observer's thread when it loads the observer, and the thread calls the
//! job once. The job loops on [`thryve_observer_pop`], which waits for the next notification sent
//! to the observer, and returns NULL once the engine shuts the observer down; the job then
//! returns, which ends the thread.
//!
//! The `ThryveObserver` the job gets belongs to the engine: it stays valid until the job returns,
//! and the job must not keep it or free it. (Older versions of these docs asked the job to free it
//! with `thryve_observer_free`; on the observer a job was given, that now does nothing.)
//!
//! ```c
//! void my_observer_job(ThryveObserver observer) {
//!     ThryveNotif notif;
//!     while ((notif = thryve_observer_pop(observer)) != NULL) {
//!         Reading* reading = thryve_notification_core(notif);
//!         record(reading);
//!         free(reading);
//!         thryve_notification_free(notif);
//!     }
//! }
//! ```
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
//!   observer's thread ends.
//! - **Typed tasks' jobs** run on the plugin workers, like `ThryveTaskJob`. **`ThryveFreeFn`**
//!   runs wherever Thryve lets the memory go, which can be the engine's own thread, so it must
//!   only free memory: see [`typed_tasks`].
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
//! (`PluginLogicAdapter`, `ObserverLogicAdapter`, `NotificationLogicAdapter`) that hold the
//! foreign side's core pointer and, for plugins and observers, its clean-up callback. Foreign
//! language developers do **not** implement these traits directly — they pass a core pointer and
//! callbacks to `thryve_plugin_create` or `thryve_observer_create`, or a core pointer to
//! `thryve_notification_create`, and the Rust side creates the adapter.
//!
//! See the [`thryve_plugin_create`](#thryve_plugin_create), [`thryve_observer_create`](#thryve_observer_create),
//! and [`thryve_notification_create`](#thryve_notification_create) functions for details.
//!
//! # Memory Management Rules
//!
//! 1. **Adding moves**: `thryve_ctx_add_plugin` and `thryve_ctx_add_observer` move the plugin or
//!    observer into the context, which frees it. A plugin or observer that's never added is freed
//!    with `thryve_plugin_free` or `thryve_observer_free`.
//! 2. **Sending moves**: the send functions take the task or notification, even when the send
//!    fails. Don't free it afterwards.
//! 3. **Strings are copied**: String arguments (`name`, `type`, etc.) are read during the call and
//!    copied. The foreign language keeps its strings, and can free them once the call returns.
//! 4. **PacketHandle is cloned**: The PacketHandle pointer is cloned inside the Rust side, so the foreign
//!    language can continue to use it after passing to plugin/observer creation.
//! 5. **Core pointers stay yours**: the core pointer given to a plugin, observer or notification is
//!    never freed by Thryve: it's handed back to your callbacks, and to [`thryve_observer_core`]
//!    and [`thryve_notification_core`].
//! 6. **User data, values and chunks** given to Thryve, with a plain task or a typed one, are
//!    freed by Thryve, once, with the free function given with them. Typed tasks' handles are used
//!    up by every call that takes them. See [`typed_tasks`].
//! 7. **The last error**: the string [`thryve_last_error`] returns stays valid until the next
//!    Thryve call on the same thread.

use std::cell::{Cell, RefCell};
use std::ffi::{CStr, CString, c_void};
use std::os::raw::{c_char, c_int};
use std::sync::Arc;
use std::thread::JoinHandle;

use thryve_run::service::service::Thryve;
use thryve_utils::bases::notif::Notif;
use thryve_utils::bases::observer::Observer;
use thryve_utils::bases::plugin::Plugin;
use thryve_utils::cfg::config::Config;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::errors::{
    FanoutError, FinalizationError, StartupError, ThryveError, get_err_oneline,
};
use thryve_utils::extras::jobs::ObsJob;
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::packets::task::Task;
use thryve_utils::traits::notif_logic::NotifLogic;
use thryve_utils::traits::observer_logic::ObsLogic;
use thryve_utils::traits::plugin_logic::PlgLogic;
use thryve_utils::traits::queue::Queue;

use thryve_run::run::run_interactive::{
    run_interactive as inner_run_interactive, summon_requirements as inner_summon_requirements,
};
use thryve_run::run::run_service::{
    create_service as inner_create_service, try_run_as_service as inner_try_run_as_service,
};
use thryve_utils::res::TaskResult;

pub mod typed_tasks;
pub use typed_tasks::*;

use typed_tasks::UserData;

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
// Panic Safety and Errors
// ============================================================================

/// Runs `f`, catching any panic instead of letting it unwind out of this
/// `extern "C"` function. Unwinding across an FFI boundary into a foreign
/// caller is undefined behavior (e.g. a malformed config file can trigger an
/// internal `assert!`/`unwrap()` several calls deep) — every wrapper in this
/// crate routes its body through here so a caught panic becomes `on_panic`
/// instead of crossing into C. Once an engine has started, Thryve's panic hook
/// ends the process first (it calls `exit(1)`), so nothing reaches this catch.
///
/// It clears the thread's last error first, so that [`thryve_last_error`] only ever tells why the
/// latest call failed. A caught panic becomes the last error.
pub(crate) fn ffi_guard<T>(on_panic: T, f: impl FnOnce() -> T) -> T {
    let _ = LAST_ERROR.try_with(|last| last.borrow_mut().take());

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(returned) => returned,
        Err(payload) => {
            // A panic's message is a `&str` or a `String`.
            let message = payload
                .downcast_ref::<&str>()
                .map(|message| message.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            fail(on_panic, format!("Thryve panicked: {message}"))
        }
    }
}

thread_local! {
    /// Why the latest Thryve call on this thread failed, for [`thryve_last_error`].
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };

    /// The observer whose job runs on this thread, while it runs: see [`thryve_observer_free`].
    static OBSERVER_IN_JOB: Cell<ThryveObserver> = const { Cell::new(std::ptr::null_mut()) };
}

/// Records why the current call failed, for [`thryve_last_error`].
pub(crate) fn set_last_error(message: impl Into<String>) {
    // A C string can't hold a NUL, so a NUL in the message becomes a space.
    let message = CString::new(message.into().replace('\0', " ")).unwrap_or_default();
    let _ = LAST_ERROR.try_with(|last| *last.borrow_mut() = Some(message));
}

/// Records why the current call failed, and returns `failed`: what the call returns then.
pub(crate) fn fail<T>(failed: T, message: impl Into<String>) -> T {
    set_last_error(message);
    failed
}

/// Reads the C string argument named `what`. Fails, once it has recorded why, with `NullArg` if
/// it's NULL, and with `StrConvError` if it isn't valid UTF-8.
///
/// # Safety
///
/// `arg` must be NULL or a valid null-terminated C string that outlives `'a`.
pub(crate) unsafe fn str_arg<'a>(arg: *const c_char, what: &str) -> Result<&'a str, ThryveStatus> {
    if arg.is_null() {
        return Err(fail(ThryveStatus::NullArg, format!("{what} is NULL")));
    }

    // Safety: the caller guarantees a valid C string.
    unsafe { CStr::from_ptr(arg) }.to_str().map_err(|_| {
        fail(
            ThryveStatus::StrConvError,
            format!("{what} isn't valid UTF-8"),
        )
    })
}

/// The status for an error Thryve returned: `InvalidState` if the engine isn't running, or a
/// fan-out group has nothing to send, and `OperationFail` otherwise.
pub(crate) fn status_of(error: &ThryveError) -> ThryveStatus {
    match error {
        ThryveError::EngineNotRunning(_)
        | ThryveError::Fanout(FanoutError::MonitorsZeroInputs { .. }) => ThryveStatus::InvalidState,
        _ => ThryveStatus::OperationFail,
    }
}

/// Why the latest Thryve call on this thread failed, as a null-terminated C string, or NULL if it
/// didn't fail.
///
/// Every Thryve call clears it first, so read it right after the call that failed. The string
/// stays valid until the next Thryve call on the same thread. For an error Thryve returned, it's
/// the error's text with its causes, as the logs show it.
///
/// # Example
///
/// ```c
/// ThryveTask task = thryve_task_create("", 2, my_job, NULL, NULL);
/// if (task == NULL) {
///     fprintf(stderr, "no task: %s\n", thryve_last_error());
/// }
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_last_error() -> *const c_char {
    LAST_ERROR
        .try_with(|last| {
            last.try_borrow()
                .ok()
                .and_then(|last| last.as_ref().map(|message| message.as_ptr()))
        })
        .ok()
        .flatten()
        .unwrap_or(std::ptr::null())
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

/// What a plain task's job or a plugin's entry point reports, filled in through the pointer the
/// job gets.
///
/// The result starts empty, every field NULL or false, which reports nothing. A NULL string is an
/// empty one, as in Rust's `TaskResult::new("", String::new(), "")`.
///
/// # Fields
///
/// - `result_type`: the result's type, such as `"Saved"`. `"Fin"` stops the engine.
/// - `message`: a message to write to `log_file`. It needs a result type: without one, `Error.log`
///   gets an error instead.
/// - `log_file`: the log file to write the message to, such as `"Main"`. A message for a log file
///   that wasn't registered goes to `Error.log`, with a note.
/// - `has_chain`: whether `chain_task` should run next
/// - `chain_task`: the task to run next, made with `thryve_task_create`
///
/// # Filling in a Result
///
/// A job with nothing to report leaves the result alone. One with something to say fills it in:
///
/// ```c
/// void save(ThryveTaskResult* result, void* report) {
///     if (!save_report(report)) {
///         result->result_type = "SaveFailed";
///         result->message = "the report couldn't be saved";
///         result->log_file = "Main";
///     }
/// }
/// ```
///
/// To run another task next:
///
/// ```c
/// result->has_chain = true;
/// result->chain_task = thryve_task_create("Upload", 2, upload, NULL, NULL);
/// ```
///
/// # Memory Management
///
/// - Thryve copies the strings as soon as the job returns, so they must still be valid then:
///   string literals work, a buffer local to the job doesn't.
/// - The chained task is Thryve's once the job returns: don't free it.
#[repr(C)]
pub struct ThryveTaskResult {
    /// The result's type (a null-terminated C string). NULL is empty; text that isn't valid UTF-8
    /// becomes `"Error"`.
    pub result_type: *const c_char,
    /// The message to log (a null-terminated C string). NULL is empty: nothing is logged.
    pub message: *const c_char,
    /// The log file to write the message to (a null-terminated C string). NULL is empty; text that
    /// isn't valid UTF-8 becomes `"Main"`.
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
    /// *result = thryve_task_result_new("Saved", "Report saved", "Main");
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
    /// *result = thryve_task_result_new_with_chain("Saved", "Report saved", "Main", upload_task);
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

/// Converts the result a C job filled in to a `TaskResult`.
///
/// A NULL string is an empty one, so a result left as it started reports nothing, as Rust's
/// `TaskResult::new("", String::new(), "")` does. A result type that isn't valid UTF-8 becomes
/// `"Error"`, and a log file that isn't becomes `"Main"`.
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
    // A NULL string is an empty one; text that isn't valid UTF-8 falls back to a default.
    let result_type = if result.result_type.is_null() {
        String::new()
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
        String::new()
    } else {
        // Safety: result.message is only valid if not NULL
        unsafe { CStr::from_ptr(result.message) }
            .to_string_lossy()
            .into_owned()
    };

    let log_file = if result.log_file.is_null() {
        String::new()
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

/// Creates a new ThryveTaskResult with the specified result type, message, and log file, for a
/// job to copy into the result it's given.
///
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
/// void finish(ThryveTaskResult* result, void* user_data) {
///     *result = thryve_task_result_new("Fin", "", "Main");  // stops the engine
/// }
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
/// ThryveTask upload = thryve_task_create("Upload", 2, upload_report, NULL, NULL);
/// *result = thryve_task_result_new_with_chain("Saved", "Report saved", "Main", upload);
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

/// A plain task's job: runs once, on a worker. It gets the task's result to fill in, which starts
/// empty and reports nothing if left alone, and the user data given to [`thryve_task_create`].
///
/// It fills in the result instead of returning one, so that languages whose callbacks can't
/// return a struct can write it. See [`ThryveTaskResult`].
pub type ThryveTaskJob =
    Option<extern "C" fn(result: *mut ThryveTaskResult, user_data: *mut c_void)>;

/// Runs a plain task's job or a plugin's entry point: it fills in a result that starts empty,
/// which then becomes the task's `TaskResult`.
fn run_filling_job(
    job: extern "C" fn(*mut ThryveTaskResult, *mut c_void),
    user_data: *mut c_void,
) -> TaskResult {
    let mut result = ThryveTaskResult::default();
    job(&mut result, user_data);
    ffi_result_to_result(result)
}

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
///
/// NULL: the plugin has nothing to clean up.
pub type ThryvePluginCleanUp = Option<extern "C" fn(*const c_void) -> c_int>;

/// A plugin's entry point: runs once, on a worker, when the engine starts.
///
/// Like a task's job, it gets a result to fill in, which starts empty, and it gets the plugin's
/// core pointer, the `plg_core_ptr` given to `thryve_plugin_create`.
pub type ThryvePluginEntryPoint =
    Option<extern "C" fn(result: *mut ThryveTaskResult, plg_core_ptr: *mut c_void)>;

/// FFI-safe function pointer for observer cleanup — takes a pointer to the observer's core data.
///
/// Called once during shutdown, on Thryve's observer clean-up thread. The `core_ptr` is the same
/// pointer that was passed to `thryve_observer_create` as `obs_core_ptr`. NULL: the observer has
/// nothing to clean up.
pub type ThryveObserverCleanUp = Option<extern "C" fn(*const c_void)>;

/// FFI-safe function pointer for an observer's job: its whole loop, called once on the observer's
/// own thread.
///
/// It gets the observer, which it borrows until it returns: it receives the observer's
/// notifications with [`thryve_observer_pop`] until that returns NULL, then returns. It must not
/// keep or free the observer. See [Observer Jobs](#observer-jobs).
pub type ThryveObsJob = Option<extern "C" fn(ThryveObserver)>;

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
        // Invalid UTF-8 can't name the file here, so it's rejected like a missing file.
        let Ok(path_str) = (unsafe { str_arg(path, "path") }) else {
            return std::ptr::null_mut();
        };

        let config = match Config::load_config_from_file(path_str) {
            Ok(config) => config,
            Err(error) => return fail(std::ptr::null_mut(), get_err_oneline(&error)),
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
            return fail(std::ptr::null_mut(), "handle is NULL");
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
            return fail(std::ptr::null_mut(), "config is NULL");
        }

        let config = unsafe { &*(config as *const Config) };
        let ctx = inner_summon_requirements(config.clone());
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
/// * `plugin` - The opaque pointer to the Plugin instance. It's moved into the context: don't use
///   or free it afterwards
///
/// # Examples
///
/// ```c
/// ThryveCtx ctx = thryve_ctx_create(config);
/// ThryvePlugin plugin = thryve_plugin_create(...);
/// thryve_ctx_add_plugin(ctx, plugin); // ctx owns the plugin now
/// thryve_ctx_free(ctx);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_add_plugin(ctx: ThryveCtx, plugin: ThryvePlugin) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if ctx.is_null() || plugin.is_null() {
            return fail(ThryveStatus::NullArg, "ctx or plugin is NULL");
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
/// * `observer` - The opaque pointer to the Observer instance. It's moved into the context: don't
///   use or free it afterwards
///
/// # Examples
///
/// ```c
/// ThryveCtx ctx = thryve_ctx_create(config);
/// ThryveObserver observer = thryve_observer_create(...);
/// thryve_ctx_add_observer(ctx, observer); // ctx owns the observer now
/// thryve_ctx_free(ctx);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_add_observer(
    ctx: ThryveCtx,
    observer: ThryveObserver,
) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if ctx.is_null() || observer.is_null() {
            return fail(ThryveStatus::NullArg, "ctx or observer is NULL");
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
/// * `file_names` - Array of null-terminated C strings; NULL entries are skipped
/// * `count` - Number of file names
///
/// # Returns
///
/// * `ThryveStatus::Ok` once every name is added
/// * `ThryveStatus::NullArg` if `ctx` or `file_names` is NULL, or `count` isn't above 0
/// * `ThryveStatus::StrConvError` if a name isn't valid UTF-8; none of the names is added then
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
            return fail(
                ThryveStatus::NullArg,
                "ctx or file_names is NULL, or count isn't above 0",
            );
        }

        // Every name is read first, so that one that isn't UTF-8 adds none of them.
        let mut names = Vec::with_capacity(count as usize);
        for i in 0..count as usize {
            // Safety: the caller guarantees `count` entries.
            let name_ptr = unsafe { *file_names.add(i) };
            if name_ptr.is_null() {
                continue;
            }

            // Safety: the caller guarantees each entry is NULL or a valid C string.
            match unsafe { str_arg(name_ptr, &format!("file_names[{i}]")) } {
                Ok(name) => names.push(name.to_string()),
                Err(status) => return status,
            }
        }

        // Safety: the caller guarantees a valid `ctx`.
        let ctx = unsafe { &mut *(ctx as *mut Ctx) };
        ctx.logging_file_names.extend(names);

        ThryveStatus::Ok
    })
}

/// Turns the result of sending a task or notification into a status for C.
///
/// A send fails only once the engine is gone (it stopped, or its context was freed without
/// starting it), which is `InvalidState`, and records why. The task or notification is dropped
/// either way.
fn send_result_status(result: Result<(), ThryveError>) -> ThryveStatus {
    match result {
        Ok(()) => ThryveStatus::Ok,
        Err(error) => fail(status_of(&error), get_err_oneline(&error)),
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
/// * `ThryveStatus::NullArg` if ctx or task is NULL; a task is dropped
/// * `ThryveStatus::InvalidState` if the engine isn't running; the task is dropped
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_send_task(ctx: ThryveCtx, task: ThryveTask) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if task.is_null() {
            return fail(ThryveStatus::NullArg, "task is NULL");
        }
        // Taken first, so that it's dropped whatever happens next.
        let task = unsafe { Box::from_raw(task as *mut Task) };
        if ctx.is_null() {
            return fail(ThryveStatus::NullArg, "ctx is NULL");
        }

        let ctx = unsafe { &*(ctx as *const Ctx) };
        send_result_status(ctx.packet_handle().send_task(*task))
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
            return fail(std::ptr::null_mut(), "ctx is NULL");
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
/// * `ThryveStatus::NullArg` if handle or task is NULL; a task is dropped
/// * `ThryveStatus::InvalidState` if the engine isn't running; the task is dropped
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_packet_handle_send_task(
    handle: ThryvePacketHandle,
    task: ThryveTask,
) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if task.is_null() {
            return fail(ThryveStatus::NullArg, "task is NULL");
        }
        // Taken first, so that it's dropped whatever happens next.
        let task = unsafe { Box::from_raw(task as *mut Task) };
        if handle.is_null() {
            return fail(ThryveStatus::NullArg, "handle is NULL");
        }

        let pkt = unsafe { &*(handle as *const PacketHandle) };
        send_result_status(pkt.send_task(*task))
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
/// * `ThryveStatus::NullArg` if handle or notif is NULL; a notification is dropped
/// * `ThryveStatus::InvalidState` if the engine isn't running; the notification is dropped
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_packet_handle_send_notification(
    handle: ThryvePacketHandle,
    notif: ThryveNotif,
) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if notif.is_null() {
            return fail(ThryveStatus::NullArg, "notif is NULL");
        }
        // Taken first, so that it's dropped whatever happens next.
        let notif = unsafe { Box::from_raw(notif as *mut Notif) };
        if handle.is_null() {
            return fail(ThryveStatus::NullArg, "handle is NULL");
        }

        let pkt = unsafe { &*(handle as *const PacketHandle) };
        send_result_status(pkt.send_notification(*notif))
    })
}

// ============================================================================
// Task Wrappers
// ============================================================================

/// Creates a plain task: its job fills in the task's result.
///
/// # Arguments
///
/// * `name` - The task name (null-terminated C string), copied
/// * `priority` - The task priority: higher is more urgent. The engine caps it at the configured
///   `max_priority`, which is 14 at most
/// * `job` - Runs once, on a worker, with the task's result to fill in, which starts empty, and
///   `user_data`. See [`ThryveTaskJob`]
/// * `user_data` - Passed to `job`
/// * `free_user_data` - Frees `user_data` once the job has run, or with the task if it never runs.
///   NULL: never freed
///
/// # Returns
///
/// An opaque pointer to the Task instance, or NULL if `name` or `job` is NULL, or the name is
/// empty, has a control character or isn't valid UTF-8; [`thryve_last_error`] says which.
/// `user_data` is freed either way.
///
/// # Safety
///
/// `name` must be either null or a valid pointer to a null-terminated C
/// string, valid for reads for the duration of this call. `job` and `free_user_data` must be safe
/// to call from any thread.
///
/// # Example
///
/// ```c
/// void save(ThryveTaskResult* result, void* report) {
///     if (!save_report(report)) {
///         result->result_type = "SaveFailed";
///         result->message = "the report couldn't be saved";
///         result->log_file = "Main";
///     }
/// }
///
/// ThryveTask task = thryve_task_create("Save", 2, save, strdup("report.txt"), free);
/// thryve_packet_handle_send_task(packet_handle, task);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_task_create(
    name: *const c_char,
    priority: u8,
    job: ThryveTaskJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveTask {
    ffi_guard(std::ptr::null_mut(), || {
        // Taken first, so that it's freed even when the task isn't made.
        let user_data = UserData::new(user_data, free_user_data);

        let Ok(task_name) = (unsafe { str_arg(name, "name") }) else {
            return std::ptr::null_mut();
        };
        let Some(job) = job else {
            return fail(std::ptr::null_mut(), "job is NULL");
        };

        // The closure owns the user data, which is freed once it has run, or with the task.
        let task_job = move || run_filling_job(job, user_data.get());
        let task = match Task::new(task_name, priority, task_job) {
            Ok(task) => task,
            Err(error) => return fail(std::ptr::null_mut(), get_err_oneline(&error)),
        };

        Box::into_raw(Box::new(task)) as ThryveTask
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
            return fail(std::ptr::null_mut(), "ctx is NULL");
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
            Err(error) => fail(std::ptr::null_mut(), get_err_oneline(&error)),
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
            return fail(ThryveStatus::NullArg, "handle is NULL");
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
                set_last_error("config is NULL");
                return ThryveServiceCreateResult {
                    service: std::ptr::null_mut(),
                    ctx: std::ptr::null_mut(),
                };
            }

            let config = unsafe { &*(config as *const Config) };
            let (service, ctx) = inner_create_service(config.clone());

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
            return fail(ThryveStatus::NullArg, "service or ctx is NULL");
        }

        // Take ownership of the service and ctx
        let service = unsafe { Box::from_raw(service as *mut Thryve) };
        let ctx = unsafe { Box::from_raw(ctx as *mut Ctx) };

        // Run the service (this is a blocking operation)
        match inner_try_run_as_service(*service, *ctx) {
            Ok(()) => ThryveStatus::Ok,
            // The service manager didn't start this process, so the call was made in the wrong
            // situation.
            Err(error @ ThryveError::Startup(StartupError::Service { .. })) => {
                fail(ThryveStatus::InvalidState, get_err_oneline(&error))
            }
            Err(error) => fail(ThryveStatus::OperationFail, get_err_oneline(&error)),
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
///   unique. It's copied.
/// * `plg_core_ptr` - An opaque pointer to the plugin core data maintained by the foreign language.
///   This pointer is passed back to `clean_up` and is available to the plugin's logic.
/// * `tx` - The opaque pointer to the PacketHandle instance. This is **cloned** inside the plugin,
///   so the PacketHandle remains valid after this call and can be used elsewhere.
/// * `clean_up` - Function pointer called once when the engine shuts down, with `plg_core_ptr`
///   as its argument. It returns `ThryveStatus_Ok` if the cleanup worked, and any other value if
///   it failed; a failure is logged to `Error.log`. NULL: nothing to clean up. See
///   [`ThryvePluginCleanUp`].
/// * `entry_point` - Runs once, on a worker, when the engine starts, with a result to fill in and
///   `plg_core_ptr`. See [`ThryvePluginEntryPoint`].
///
/// # Returns
///
/// An opaque pointer to the Plugin instance, or NULL if `plg_type`, `tx` or `entry_point` is NULL,
/// or `plg_type` is empty, has a control character or isn't valid UTF-8; [`thryve_last_error`]
/// says which.
///
/// # Safety
///
/// `plg_type` must be either null or a valid pointer to a null-terminated
/// C string, valid for reads for the duration of this call. `tx` must be a valid, non-null `ThryvePacketHandle`. `plg_core_ptr`
/// is an opaque pointer whose validity for the plugin's entire lifetime is
/// the caller's responsibility. `clean_up` and `entry_point` must be NULL or
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
/// void my_plugin_entry_point(ThryveTaskResult* result, void* core_ptr) {
///     // Start the plugin's work here. Leaving `result` alone reports nothing.
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
    plg_core_ptr: *mut c_void,
    tx: ThryvePacketHandle,
    clean_up: ThryvePluginCleanUp,
    entry_point: ThryvePluginEntryPoint,
) -> ThryvePlugin {
    ffi_guard(std::ptr::null_mut(), || {
        let Ok(plg_type) = (unsafe { str_arg(plg_type, "plg_type") }) else {
            return std::ptr::null_mut();
        };
        if tx.is_null() {
            return fail(std::ptr::null_mut(), "tx is NULL");
        }
        let Some(entry_point) = entry_point else {
            return fail(std::ptr::null_mut(), "entry_point is NULL");
        };

        // Create the packet handle clone
        let packet_handle = unsafe { &*(tx as *const PacketHandle) }.clone();

        // The entry point gets the core pointer, which stays the caller's: no free function.
        let core = UserData::new(plg_core_ptr, None);
        let task_job = move || run_filling_job(entry_point, core.get());

        // Create the PlgLogic adapter - PluginLogicAdapter already implements PlgLogic
        let adapter: Arc<dyn PlgLogic> =
            Arc::new(PluginLogicAdapter::new(plg_type, plg_core_ptr, clean_up));

        let plugin = match Plugin::new(plg_type, adapter, packet_handle, task_job) {
            Ok(plugin) => plugin,
            Err(error) => return fail(std::ptr::null_mut(), get_err_oneline(&error)),
        };

        Box::into_raw(Box::new(plugin)) as ThryvePlugin
    })
}

/// Frees a plugin that was never added to a context: once added, the context owns it.
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
/// if (thryve_ctx_add_plugin(ctx, plugin) != ThryveStatus_Ok) {
///     thryve_plugin_free(plugin); // never added, so it's still yours
/// }
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
///   must be unique. It's copied.
/// * `obs_core_ptr` - An opaque pointer to the observer core data maintained by the foreign language.
///   This pointer is passed back to `clean_up`, and the job reads it with [`thryve_observer_core`].
/// * `tx` - The opaque pointer to the PacketHandle instance. This is **cloned** inside the observer,
///   so the PacketHandle remains valid after this call and can be used elsewhere.
/// * `clean_up` - Function pointer called when the observer is cleaned up.
///   The `obs_core_ptr` is passed as an argument. NULL: nothing to clean up.
/// * `obs_job_fn` - The observer's job, its whole loop: called once on the observer's own thread
///   with the observer, which it borrows until it returns. See [Observer Jobs](#observer-jobs).
///
/// # Returns
///
/// An opaque pointer to the Observer instance, or NULL if `obs_type`, `tx` or `obs_job_fn` is
/// NULL, or `obs_type` is empty, has a control character or isn't valid UTF-8;
/// [`thryve_last_error`] says which.
///
/// # Safety
///
/// `obs_type` must be either null or a valid pointer to a null-terminated
/// C string, valid for reads for the duration of this call. `tx` must be a valid, non-null `ThryvePacketHandle`. `obs_core_ptr`
/// is an opaque pointer whose validity for the observer's entire lifetime
/// is the caller's responsibility. `clean_up` and `obs_job_fn` must be NULL
/// or function pointers matching their declared signatures, safe to
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
/// void my_observer_job(ThryveObserver observer) {
///     ThryveNotif notif;
///     while ((notif = thryve_observer_pop(observer)) != NULL) {
///         // Handle the notification here, then free it.
///         thryve_notification_free(notif);
///     }
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
    obs_core_ptr: *mut c_void,
    tx: ThryvePacketHandle,
    clean_up: ThryveObserverCleanUp,
    obs_job_fn: ThryveObsJob,
) -> ThryveObserver {
    ffi_guard(std::ptr::null_mut(), || {
        let Ok(obs_type) = (unsafe { str_arg(obs_type, "obs_type") }) else {
            return std::ptr::null_mut();
        };
        if tx.is_null() {
            return fail(std::ptr::null_mut(), "tx is NULL");
        }
        let Some(obs_job_fn) = obs_job_fn else {
            return fail(std::ptr::null_mut(), "obs_job_fn is NULL");
        };

        // Create the packet handle clone
        let packet_handle = unsafe { &*(tx as *const PacketHandle) }.clone();

        // Create the ObsJob wrapper - capture the FFI function pointer
        let obs_job = ObsJob::new(move |observer: Arc<Observer>| {
            // The job borrows the observer: `observer` keeps it alive until the job returns. The
            // pointer is remembered on this thread while the job runs, so that freeing it there
            // does nothing (see `thryve_observer_free`).
            let ffi_observer = Arc::as_ptr(&observer) as ThryveObserver;
            OBSERVER_IN_JOB.with(|in_job| in_job.set(ffi_observer));
            (obs_job_fn)(ffi_observer);
            OBSERVER_IN_JOB.with(|in_job| in_job.set(std::ptr::null_mut()));
        });

        // Create the ObsLogic adapter - ObserverLogicAdapter already implements ObsLogic
        let adapter: Arc<dyn ObsLogic> =
            Arc::new(ObserverLogicAdapter::new(obs_core_ptr, clean_up));

        // Create the Observer
        let observer = match Observer::new(obs_type, adapter, obs_job, packet_handle) {
            Ok(observer) => observer,
            Err(error) => return fail(std::ptr::null_mut(), get_err_oneline(&error)),
        };

        Box::into_raw(Box::new(observer)) as ThryveObserver
    })
}

/// Frees an observer that was never added to a context: once added, the context owns it.
///
/// The observer a job was given belongs to the engine: freeing it from the job does nothing,
/// and [`thryve_last_error`] says so.
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
/// if (thryve_ctx_add_observer(ctx, observer) != ThryveStatus_Ok) {
///     thryve_observer_free(observer); // never added, so it's still yours
/// }
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_observer_free(observer: ThryveObserver) {
    ffi_guard((), || {
        if observer.is_null() {
            return;
        }

        // The observer a job was given is the engine's, not the caller's.
        let is_in_job = OBSERVER_IN_JOB
            .try_with(|in_job| in_job.get() == observer)
            .unwrap_or(false);
        if is_in_job {
            set_last_error("the observer belongs to the engine while its job runs");
            return;
        }

        unsafe {
            let _ = Box::from_raw(observer as *mut Observer);
        }
    })
}

/// Waits for the next notification sent to the observer, and returns it.
///
/// It's for the observer's job, which is the observer's whole loop: the job calls it until it
/// returns NULL, which happens once the engine shuts the observer down, even if notifications are
/// still queued. The job should then return, which ends the observer's thread.
///
/// # Arguments
///
/// * `observer` - The observer the job was given
///
/// # Returns
///
/// The notification, for the job to free with [`thryve_notification_free`]: read the data it
/// carries with [`thryve_notification_core`]. NULL once the observer is shut down, or if
/// `observer` is NULL.
///
/// # Example
///
/// ```c
/// void my_observer_job(ThryveObserver observer) {
///     ThryveNotif notif;
///     while ((notif = thryve_observer_pop(observer)) != NULL) {
///         Reading* reading = thryve_notification_core(notif);
///         record(reading);
///         free(reading);
///         thryve_notification_free(notif);
///     }
/// }
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_observer_pop(observer: ThryveObserver) -> ThryveNotif {
    ffi_guard(std::ptr::null_mut(), || {
        if observer.is_null() {
            return fail(std::ptr::null_mut(), "observer is NULL");
        }

        // Safety: a non-NULL observer is the one a job was given, or one from
        // `thryve_observer_create`.
        let observer = unsafe { &*(observer as *const Observer) };
        match observer.obs_queue.pop() {
            Some(notif) => Box::into_raw(Box::new(notif)) as ThryveNotif,
            None => std::ptr::null_mut(),
        }
    })
}

/// The data an observer was created with: the `obs_core_ptr` given to
/// [`thryve_observer_create`], for its job to reach. Thryve never frees it.
///
/// The shutdown calls the observer's `clean_up` before [`thryve_observer_pop`] returns NULL to the
/// job. So if `clean_up` frees this data, the job must not use it once `thryve_observer_pop` has
/// returned NULL.
///
/// # Returns
///
/// The pointer, or NULL if `observer` is NULL.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_observer_core(observer: ThryveObserver) -> *mut c_void {
    ffi_guard(std::ptr::null_mut(), || {
        if observer.is_null() {
            return fail(std::ptr::null_mut(), "observer is NULL");
        }

        // Safety: a non-NULL observer is the one a job was given, or one from
        // `thryve_observer_create`.
        let observer = unsafe { &*(observer as *const Observer) };
        observer
            .obs_core
            .as_any()
            .downcast_ref::<ObserverLogicAdapter>()
            .map_or(std::ptr::null_mut(), |adapter| adapter.core_ptr)
    })
}

// ============================================================================
// Notification Wrappers
// ============================================================================

/// Creates a new notification with the specified parameters.
///
/// The notification carries the foreign language's own data as `notif_core_ptr`: the observer
/// it's sent to reads it with [`thryve_notification_core`]. Thryve never frees it.
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
/// `notif_type` or `notif_receiver` is empty, contains control characters or isn't valid UTF-8.
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
/// // A reading for the "Readings" observer, which frees it once it has read it.
/// Reading* reading = malloc(sizeof *reading);
/// reading->celsius = 21.5;
/// ThryveNotif notif = thryve_notification_create("Reading", "Readings", reading);
/// thryve_packet_handle_send_notification(packet_handle, notif);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_notification_create(
    notif_type: *const c_char,
    notif_receiver: *const c_char,
    notif_core_ptr: *mut c_void,
) -> ThryveNotif {
    ffi_guard(std::ptr::null_mut(), || {
        let Ok(notif_type) = (unsafe { str_arg(notif_type, "notif_type") }) else {
            return std::ptr::null_mut();
        };
        let Ok(notif_receiver) = (unsafe { str_arg(notif_receiver, "notif_receiver") }) else {
            return std::ptr::null_mut();
        };

        // Create the NotifLogic adapter
        let notif_core: Box<dyn NotifLogic> =
            Box::new(NotificationLogicAdapter::new(notif_core_ptr));

        // Create the notification
        let notif = match Notif::new(notif_type, notif_receiver, notif_core) {
            Ok(notif) => notif,
            Err(error) => return fail(std::ptr::null_mut(), get_err_oneline(&error)),
        };

        Box::into_raw(Box::new(notif)) as ThryveNotif
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

/// The data a notification carries: the `notif_core_ptr` it was created with by
/// [`thryve_notification_create`]. Thryve never frees it.
///
/// # Returns
///
/// The pointer, or NULL if `notif` is NULL, or wasn't created from C (Thryve's own log lines,
/// say).
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_notification_core(notif: ThryveNotif) -> *mut c_void {
    ffi_guard(std::ptr::null_mut(), || {
        if notif.is_null() {
            return fail(std::ptr::null_mut(), "notif is NULL");
        }

        // Safety: a non-NULL notification comes from Thryve.
        let notif = unsafe { &*(notif as *const Notif) };
        notif
            .notif_core
            .as_any()
            .downcast_ref::<NotificationLogicAdapter>()
            .map_or(std::ptr::null_mut(), |adapter| adapter.core_ptr)
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
/// * `ThryveStatus::NullArg` if ctx or notif is NULL; a notification is dropped
/// * `ThryveStatus::InvalidState` if the engine isn't running; the notification is dropped
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_ctx_send_notification(ctx: ThryveCtx, notif: ThryveNotif) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if notif.is_null() {
            return fail(ThryveStatus::NullArg, "notif is NULL");
        }
        // Taken first, so that it's dropped whatever happens next.
        let notif = unsafe { Box::from_raw(notif as *mut Notif) };
        if ctx.is_null() {
            return fail(ThryveStatus::NullArg, "ctx is NULL");
        }

        let ctx = unsafe { &*(ctx as *const Ctx) };
        send_result_status(ctx.packet_handle().send_notification(*notif))
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
    pub core_ptr: *mut c_void,
    /// The foreign cleanup callback, called with `core_ptr`; `None` if there's nothing to clean up.
    pub clean_up: ThryvePluginCleanUp,
}

impl PluginLogicAdapter {
    /// Wraps a foreign plugin's type, data pointer and cleanup callback.
    pub fn new(plg_type: &str, core_ptr: *mut c_void, clean_up: ThryvePluginCleanUp) -> Self {
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
        // A plugin created without a clean-up has nothing to clean up.
        let Some(clean_up) = self.clean_up else {
            return Ok(());
        };
        let status = clean_up(self.core_ptr);

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
    pub core_ptr: *mut c_void,
    /// The foreign cleanup callback, called with `core_ptr`; `None` if there's nothing to clean up.
    pub clean_up: ThryveObserverCleanUp,
}

impl ObserverLogicAdapter {
    /// Wraps a foreign observer's data pointer and cleanup callback.
    pub fn new(core_ptr: *mut c_void, clean_up: ThryveObserverCleanUp) -> Self {
        Self { core_ptr, clean_up }
    }
}

unsafe impl Send for ObserverLogicAdapter {}
unsafe impl Sync for ObserverLogicAdapter {}

impl ObsLogic for ObserverLogicAdapter {
    fn clean_up(&self, _observer: Arc<Observer>) {
        // An observer created without a clean-up has nothing to clean up.
        if let Some(clean_up) = self.clean_up {
            clean_up(self.core_ptr);
        }
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Adapter for NotifLogic trait to support FFI callbacks.
pub struct NotificationLogicAdapter {
    /// The foreign notification's own data.
    pub core_ptr: *mut c_void,
}

impl NotificationLogicAdapter {
    /// Wraps a foreign notification's data pointer.
    pub fn new(core_ptr: *mut c_void) -> Self {
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
