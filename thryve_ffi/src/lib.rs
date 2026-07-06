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
//!
//! # Typical Usage Sequence (Interactive Mode)
//!
//! ```c
//! // 1. Create configuration
//! FfiConfig config = ffi_config_default();
//!
//! // 2. Create context from configuration
//! FfiCtx ctx = ffi_ctx_create(config);
//! if (ctx != NULL) {
//!     // 3. Add plugins and observers to the context
//!     ffi_ctx_add_plugin(ctx, plugin_ptr);
//!     ffi_ctx_add_observer(ctx, observer_ptr);
//!
//!     // 4. Start the engine in interactive mode
//!     FfiRunHandle handle = ffi_run_interactive_start(ctx);
//!     if (handle != NULL) {
//!         // Wait for engine to finish
//!         ffi_run_interactive_free(handle);
//!     }
//!
//!     // 5. Cleanup
//!     ffi_ctx_free(ctx);
//! }
//!
//! // 6. Free configuration
//! ffi_config_free(config);
//! ```
//!
//! # Callback Types
//!
//! All callback types use the `Ffi` prefix and follow C-compatible calling conventions.
//!
//! ## Task and Plugin Callbacks
//!
//! - **`FfiTaskJob`**: A function pointer that takes no arguments and returns a `FfiTaskResult`.
//!   Used for plugin entry points and task execution.
//! - **`FfiTaskResult`**: A struct containing:
//!   - `result_type`: A null-terminated string describing the result type (e.g., "Success", "Failed")
//!   - `message`: A null-terminated string with a human-readable result message
//!   - `log_file`: A null-terminated string specifying the log file name for the result
//!   - `has_chain`: A boolean indicating whether a chained task should be executed next
//!   - `chain_task`: A pointer to the chained task (only valid if `has_chain` is true and pointer is not null)
//! - **`FfiPluginEntryPoint`**: Alias for `FfiTaskJob` — used when creating plugins.
//! - **`FfiPluginCleanUp`**: A function pointer that takes a `*const u8` (the plugin's core pointer).
//!   Called when the plugin is being cleaned up.
//!
//! ## Observer Callbacks
//!
//! - **`FfiObsJob`**: **Primary callback** for observer execution. Takes an `FfiObserver` (raw pointer to `Arc<Observer>`)
//!   and returns nothing. **The FFI implementation must convert the raw pointer back to an Arc and drop it** — see
//!   [Observer Job Memory Safety](#observer-job-memory-safety) for details.
//! - **`FfiObserverCleanUp`**: A function pointer that takes a `*const u8` (the observer's core pointer).
//!   Called when the observer is being cleaned up.
//! - **`FfiObserverMainLoop`**: A function pointer that takes a `*const u8` (the observer's core pointer).
//!   Currently **unused** in the FFI layer.
//!
//! ### Observer Job Memory Safety
//!
//! When an observer is executed, the Rust side performs the following:
//!
//! 1. Clones the `Arc<Observer>`: `observer.clone()`
//! 2. Converts to raw pointer: `Arc::into_raw(observer_clone) as FfiObserver`
//! 3. Calls the `FfiObsJob` callback with this raw pointer
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
//! void my_observer_job(FfiObserver obs) {
//!     // Convert raw pointer back to Arc<Observer> (C++ style)
//!     // In C, you may need to manage this differently
//!     // Best practice: use C++ with RAII
//! }
//! ```
//!
//! **C++ example:**
//!
//! ```cpp
//! extern "C" void my_observer_job(FfiObserver obs) {
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
//! - **`FfiNotifLogic`**: A function pointer that takes a `*const u8` (the notification's core pointer).
//!   Called when the notification needs to be processed.
//!
//! ## Threading Model
//!
//! Callbacks run on different threads depending on the context:
//!
//! - **`FfiTaskJob` / `FfiPluginEntryPoint`**: Runs on the thread that schedules the task.
//!   If the task is scheduled from the main thread, the callback runs on the main thread.
//! - **`FfiObsJob`**: Runs on a **separate thread spawned by the observer's main loop**.
//!   The Rust side clones the `Arc<Observer>`, converts to raw pointer, and calls the callback.
//!   The callback must be thread-safe — it cannot assume the calling thread.
//! - **`FfiPluginCleanUp` / `FfiObserverCleanUp`**: Runs on the thread that frees the object.
//! - **`FfiNotifLogic`**: Runs on the thread that processes the notification.
//!
//! **Critical for `FfiObsJob`**: Because this callback runs on a separate thread, the FFI
//! implementation must ensure any shared data accessed within the callback is protected by
//! synchronization primitives (mutexes, atomics, etc.). The `FfiObserver` raw pointer itself
//! is only valid during the callback execution — it must not be stored for later use.
//!
//! # Plugin/Observer/Notification Logic
//!
//! The `PlgLogic`, `ObsLogic`, and `NotifLogic` traits are implemented by adapter structs
//! (`PluginLogicAdapter`, `ObserverLogicAdapter`, `NotificationLogicAdapter`) that wrap
//! FFI function pointers. Foreign language developers do **not** implement these traits directly —
//! they provide function pointers to `ffi_plugin_create`, `ffi_observer_create`, or
//! `ffi_notification_create`, and the Rust side creates the adapter.
//!
//! See the [`ffi_plugin_create`](#ffi_plugin_create), [`ffi_observer_create`](#ffi_observer_create),
//! and [`ffi_notification_create`](#ffi_notification_create) functions for details.
//!
//! # Memory Management Rules
//!
//! 1. **Ownership Transfer**: When you pass a pointer to a Rust function, ownership may be transferred.
//!    - `ffi_plugin_create`: Takes ownership of `entry_point` (transmutes it into `TaskJob`)
//!    - `ffi_task_create`: Takes ownership of `job` (transmutes it into `TaskJob`)
//!    - `ffi_ctx_add_plugin`: Takes ownership of `plugin` (converts from raw pointer to Box, then clones into vector)
//!    - `ffi_ctx_add_observer`: Takes ownership of `observer` (converts from raw pointer to Box, then clones into vector)
//! 2. **Never free after transfer**: After calling `ffi_plugin_create` or `ffi_task_create`, do **not** free the
//!    callback pointer — it has been transferred to Rust and will be called from there.
//! 3. **String borrowing**: String arguments (`name`, `type`, etc.) are borrowed via `CStr::from_ptr` and
//!    converted to `&str`. The foreign language must **not** free these pointers after passing them.
//! 4. **PacketHandle is cloned**: The PacketHandle pointer is cloned inside the Rust side, so the foreign
//!    language can continue to use it after passing to plugin/observer creation.

use std::os::raw::c_char;
use std::ffi::CStr;
use std::sync::Arc;
use std::thread::JoinHandle;


use thryve::utils::jobs::ObsJob;
use thryve::utils::jobs::TaskJob;
use thryve::utils::traits::NotifLogic;
use thryve::utils::traits::ObsLogic;
use thryve::utils::traits::PlgLogic;
use thryve_run::service::service::Thryve;
use thryve_utils::bases::notif::Notif;
use thryve_utils::bases::observer::Observer;
use thryve_utils::bases::plugin::Plugin;
use thryve_utils::cfg::config::Config;
use thryve_utils::cfg::ctx::Ctx;
use thryve_utils::extras::named_input::NamedInput;
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::packets::task::Task;

use thryve_run::run::run_interactive::{summon_requirements as inner_summon_requirements, run_interactive as inner_run_interactive};
use thryve_run::run::run_service::{create_service as inner_create_service, try_run_as_service as inner_try_run_as_service};
use thryve_utils::res;

// ============================================================================
// Opaque Pointer Types
// ============================================================================

/// Opaque pointer to a Config instance (wraps `*mut Config`).
pub type FfiConfig = *mut u8;

/// Opaque pointer to a Ctx instance (wraps `*mut Ctx`).
pub type FfiCtx = *mut u8;

/// Opaque pointer to a JoinHandle instance (wraps `*mut JoinHandle<()>`).
pub type FfiRunHandle = *mut u8;

/// Opaque pointer to a Thryve service instance (wraps `*mut Thryve`).
pub type FfiService = *mut u8;

/// Opaque pointer to a Plugin instance (wraps `*mut Plugin`).
pub type FfiPlugin = *mut u8;

/// Opaque pointer to an Observer instance (wraps `*mut Observer`).
pub type FfiObserver = *mut u8;

/// Opaque pointer to a PacketHandle instance (wraps `*mut PacketHandle`).
pub type FfiPacketHandle = *mut u8;

/// Opaque pointer to a Task instance (wraps `*mut Task`).
pub type FfiTask = *mut u8;

/// Opaque pointer to a Notification instance (wraps `*mut Notif`).
pub type FfiNotif = *mut u8;

/// FFI-safe status code for operations.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FfiStatus {
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
/// This struct represents the result of a task or plugin execution, allowing the
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
/// FfiTaskResult result = {0};  // has_chain = false, chain_task = NULL
/// result.result_type = "Success";
/// result.message = "Task completed";
/// result.log_file = "Main";
/// ```
///
/// If you want to return a chained task:
///
/// ```c
/// FfiTaskResult result = {0};
/// result.result_type = "Chained";
/// result.message = "Chaining to next task";
/// result.log_file = "Main";
/// result.has_chain = 1;
/// result.chain_task = my_chained_task;  // Pointer to a FfiTask created by ffi_task_create
/// ```
///
/// **Important**: The `chain_task` pointer must point to a valid `FfiTask` that has been
/// created (e.g., via `ffi_task_create`). The Rust side will take ownership of this task
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
pub struct FfiTaskResult {
    result_type: *const c_char,
    message: *const c_char,
    log_file: *const c_char,
    has_chain: bool,
    chain_task: FfiTask,
}

impl Default for FfiTaskResult {
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

impl FfiTaskResult {
    /// Creates a new `FfiTaskResult` with the specified result type, message, and log file.
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
    /// A new `FfiTaskResult` with `has_chain` set to `false` and `chain_task` set to NULL.
    ///
    /// # Example (C)
    ///
    /// ```c
    /// FfiTaskResult result = ffi_task_result_new("Success", "Task completed", "Main");
    /// ```
    ///
    /// # Example (C++)
    ///
    /// ```cpp
    /// FfiTaskResult result = ffi_task_result_new("Success", "Task completed", "Main");
    /// ```
    pub fn new(result_type: *const c_char, message: *const c_char, log_file: *const c_char) -> Self {
        Self {
            result_type,
            message,
            log_file,
            has_chain: false,
            chain_task: std::ptr::null_mut(),
        }
    }

    /// Creates a new `FfiTaskResult` with a chained task.
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
    /// A new `FfiTaskResult` with `has_chain` set to `true` and the specified chained task.
    ///
    /// # Example (C)
    ///
    /// ```c
    /// FfiTaskResult result = ffi_task_result_new_with_chain("Chained", "Chaining to next task", "Main", my_chained_task);
    /// ```
    pub fn new_with_chain(result_type: *const c_char, message: *const c_char, log_file: *const c_char, chain_task: FfiTask) -> Self {
        Self {
            result_type,
            message,
            log_file,
            has_chain: true,
            chain_task,
        }
    }
}

/// Converts an `FfiTaskResult` to a `res::Result`.
///
/// This function takes the FFI result struct and converts it into a proper
/// `res::Result` that the Rust runtime can use. If the result contains a
/// chained task pointer, that task is taken ownership of and chained
/// to the result.
///
/// # Arguments
///
/// * `result` - The FFI task result to convert
///
/// # Returns
///
/// A `res::Result` with the appropriate fields set based on the FFI result.
/// If `has_chain` is true and `chain_task` is not NULL, the chained task
/// is taken ownership of and attached to the result.
fn ffi_result_to_result(result: FfiTaskResult) -> res::Result {
    // Extract the string fields from FFI result, falling back to defaults
    let result_type = if result.result_type.is_null() {
        NamedInput::new("Error")
    } else {
        // Safety: result_type is only valid if not NULL, and the foreign language
        // guarantees the string remains valid for the lifetime of the result.
        // We also require the string to be valid UTF-8.
        NamedInput::new(unsafe { CStr::from_ptr(result.result_type) }.to_str().unwrap_or("Error"))
    };

    let message = if result.message.is_null() {
        String::from("No message")
    } else {
        // Safety: result.message is only valid if not NULL
        unsafe { CStr::from_ptr(result.message) }.to_string_lossy().into_owned()
    };

    let log_file = if result.log_file.is_null() {
        NamedInput::new("Main")
    } else {
        // Safety: result.log_file is only valid if not NULL
        NamedInput::new(unsafe { CStr::from_ptr(result.log_file) }.to_str().unwrap_or("Main"))
    };

    let mut res = res::Result {
        result_type,
        message,
        log_msg_to_file: log_file,
        ..Default::default()
    };

    // Handle chained task if present
    if result.has_chain && !result.chain_task.is_null() {
        // Take ownership of the chained task from the FFI pointer
        let chained_task = unsafe { *Box::from_raw(result.chain_task as *mut Task) };
        res.next_task = Some(chained_task);
    }

    res
}

/// Creates a new FfiTaskResult with the specified result type, message, and log file.
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
/// A new `FfiTaskResult` with `has_chain` set to `false` and `chain_task` set to NULL.
///
/// # Example (C)
///
/// ```c
/// FfiTaskResult result = ffi_task_result_new("Success", "Task completed", "Main");
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_task_result_new(
    result_type: *const c_char,
    message: *const c_char,
    log_file: *const c_char,
) -> FfiTaskResult {
    FfiTaskResult::new(result_type, message, log_file)
}

/// Creates a new FfiTaskResult with a chained task.
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
/// A new `FfiTaskResult` with `has_chain` set to `true` and the specified chained task.
///
/// # Example (C)
///
/// ```c
/// FfiTaskResult result = ffi_task_result_new_with_chain("Chained", "Chaining to next task", "Main", my_chained_task);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_task_result_new_with_chain(
    result_type: *const c_char,
    message: *const c_char,
    log_file: *const c_char,
    chain_task: FfiTask,
) -> FfiTaskResult {
    FfiTaskResult::new_with_chain(result_type, message, log_file, chain_task)
}

/// FFI-safe function pointer for task job — takes no arguments and returns a `FfiTaskResult`.
///
/// Used for plugin entry points and task execution. The foreign language implementation
/// should return a `FfiTaskResult` with `has_chain` set to `false` if no chained task follows.
pub type FfiTaskJob = extern "C" fn() -> FfiTaskResult;

/// FFI-safe function pointer for plugin cleanup — takes a pointer to the plugin's core data.
///
/// Called when the plugin is being freed. The `core_ptr` is the same pointer that was passed
/// to `ffi_plugin_create` as `plg_core_ptr`.
pub type FfiPluginCleanUp = extern "C" fn(*const u8);

/// FFI-safe function pointer for plugin entry point — returns a task result.
///
/// Used by `ffi_plugin_create` as the `entry_point` parameter. This function is called
/// when the plugin's task is executed.
pub type FfiPluginEntryPoint = extern "C" fn() -> FfiTaskResult;

/// FFI-safe function pointer for observer cleanup — takes a pointer to the observer's core data.
///
/// Called when the observer is being freed. The `core_ptr` is the same pointer that was passed
/// to `ffi_observer_create` as `obs_core_ptr`.
pub type FfiObserverCleanUp = extern "C" fn(*const u8);

/// FFI-safe function pointer for observer main loop — takes a pointer to the observer's core data.
///
/// **Currently unused** in the FFI layer. The primary observer callback is `FfiObsJob`.
pub type FfiObserverMainLoop = extern "C" fn(*const u8);

/// FFI-safe function pointer for observer job — takes an `FfiObserver` handle and returns nothing.
///
/// This is the **primary callback** for observer execution. When an observer's main loop runs,
/// the Rust side:
/// 1. Clones the `Arc<Observer>` and converts to raw pointer
/// 2. Calls this function with the raw pointer
/// 3. The FFI implementation must convert the raw pointer back to an Arc and drop it when done
///
/// See the module-level documentation for the [Observer Job Memory Safety](#observer-job-memory-safety) details.
pub type FfiObsJob = extern "C" fn(FfiObserver);

/// FFI-safe function pointer for notification logic — takes a pointer to the notification's core data.
///
/// Called when a notification needs to be processed. The `core_ptr` is the same pointer that was
/// passed to `ffi_notification_create` as `notif_core_ptr`.
pub type FfiNotifLogic = extern "C" fn(*const u8);

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
/// FfiConfig config = ffi_config_default();
/// ffi_config_free(config);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_config_default() -> FfiConfig {
    let config = Config::load_def_config();
    let boxed = Box::into_raw(Box::new(config)) as FfiConfig;
    if boxed.is_null() {
        return std::ptr::null_mut();
    }
    boxed
}

/// Creates a configuration instance from a TOML file.
///
/// # Arguments
///
/// * `path` - The path to the TOML configuration file (null-terminated C string)
///
/// # Returns
///
/// An opaque pointer to the Config instance, or NULL on failure.
///
/// # Examples
///
/// ```c
/// FfiConfig config = ffi_config_from_file("config.toml");
/// if (config != NULL) {
///     ffi_config_free(config);
/// }
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_config_from_file(path: *const c_char) -> FfiConfig {
    if path.is_null() {
        return std::ptr::null_mut();
    }

    let path_str = unsafe { CStr::from_ptr(path) }.to_str().unwrap_or("");

    let config = Config::load_config_from_file(path_str);
    let boxed = Box::into_raw(Box::new(config)) as FfiConfig;
    if boxed.is_null() {
        return std::ptr::null_mut();
    }
    boxed
}

/// Frees a configuration instance.
///
/// # Arguments
///
/// * `config` - The opaque pointer to the Config instance to free.
///             If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// FfiConfig config = ffi_config_default();
/// ffi_config_free(config);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_config_free(config: FfiConfig) {
    if !config.is_null() {
        unsafe {
            let _ = Box::from_raw(config as *mut Config);
        }
    }
}

// ============================================================================
// PacketHandle Wrappers
// ============================================================================

/// Creates a PacketHandle from a configuration.
///
/// # Arguments
///
/// * `config` - The opaque pointer to the Config instance
///
/// # Returns
///
/// An opaque pointer to the PacketHandle instance, or NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn ffi_packet_handle_create(config: FfiConfig) -> FfiPacketHandle {
    if config.is_null() {
        return std::ptr::null_mut();
    }

    let config = unsafe { &*(config as *const Config) };
    let (task_tx, _task_rx) = crossbeam::channel::bounded(config.pipes.task_pipe_capacity);
    let (notif_tx, _notif_rx) = crossbeam::channel::bounded(config.pipes.notif_pipe_capacity);
    
    let packet_handle = PacketHandle::new(task_tx, notif_tx, false);
    let boxed = Box::into_raw(Box::new(packet_handle)) as FfiPacketHandle;
    if boxed.is_null() {
        return std::ptr::null_mut();
    }
    boxed
}

/// Frees a PacketHandle instance.
///
/// # Arguments
///
/// * `handle` - The opaque pointer to the PacketHandle instance to free.
///              If NULL, this function does nothing.
#[unsafe(no_mangle)]
pub extern "C" fn ffi_packet_handle_free(handle: FfiPacketHandle) {
    if !handle.is_null() {
        unsafe {
            let _ = Box::from_raw(handle as *mut PacketHandle);
        }
    }
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
#[unsafe(no_mangle)]
pub extern "C" fn ffi_packet_handle_clone(handle: FfiPacketHandle) -> FfiPacketHandle {
    if handle.is_null() {
        return std::ptr::null_mut();
    }

    let ph = unsafe { &*(handle as *const PacketHandle) };
    let cloned = ph.clone();
    let boxed = Box::into_raw(Box::new(cloned)) as FfiPacketHandle;
    if boxed.is_null() {
        return std::ptr::null_mut();
    }
    boxed
}

// ============================================================================
// Ctx Wrappers
// ============================================================================

/// Creates a context from a configuration instance.
///
/// This function sets up the communication channels and creates a Ctx instance
/// ready for use with the Thryve engine.
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
/// FfiConfig config = ffi_config_default();
/// FfiCtx ctx = ffi_ctx_create(config);
/// if (ctx != NULL) {
///     ffi_ctx_free(ctx);
/// }
/// ffi_config_free(config);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_ctx_create(config: FfiConfig) -> FfiCtx {
    if config.is_null() {
        return std::ptr::null_mut();
    }

    let config = unsafe { &*(config as *const Config) };
    let ctx = inner_summon_requirements(*config);
    let boxed = Box::into_raw(Box::new(ctx)) as FfiCtx;
    if boxed.is_null() {
        return std::ptr::null_mut();
    }
    boxed
}

/// Frees a context instance.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance to free.
///           If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// FfiCtx ctx = ffi_ctx_create(config);
/// ffi_ctx_free(ctx);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_ctx_free(ctx: FfiCtx) {
    if !ctx.is_null() {
        unsafe {
            let _ = Box::from_raw(ctx as *mut Ctx);
        }
    }
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
/// FfiCtx ctx = ffi_ctx_create(config);
/// FfiPlugin plugin = ffi_plugin_create(...);
/// ffi_ctx_add_plugin(ctx, plugin);
/// ffi_plugin_free(plugin);
/// ffi_ctx_free(ctx);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_ctx_add_plugin(ctx: FfiCtx, plugin: FfiPlugin) -> FfiStatus {
    if ctx.is_null() || plugin.is_null() {
        return FfiStatus::NullArg;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut Ctx);
        let plg = Box::from_raw(plugin as *mut Plugin);
        ctx.plugins.push(*plg);
    }

    FfiStatus::Ok
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
/// FfiCtx ctx = ffi_ctx_create(config);
/// FfiObserver observer = ffi_observer_create(...);
/// ffi_ctx_add_observer(ctx, observer);
/// ffi_observer_free(observer);
/// ffi_ctx_free(ctx);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_ctx_add_observer(ctx: FfiCtx, observer: FfiObserver) -> FfiStatus {
    if ctx.is_null() || observer.is_null() {
        return FfiStatus::NullArg;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut Ctx);
        let obs = Box::from_raw(observer as *mut Observer);
        ctx.observers.push(*obs);
    }

    FfiStatus::Ok
}

/// Sets the logging file names for the context.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance
/// * `file_names` - Array of null-terminated C strings
/// * `count` - Number of file names
///
/// # Examples
///
/// ```c
/// FfiCtx ctx = ffi_ctx_create(config);
/// const char* log_files[] = {"ImgCompression", "Main"};
/// ffi_ctx_set_logging_file_names(ctx, log_files, 2);
/// ffi_ctx_free(ctx);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_ctx_set_logging_file_names(ctx: FfiCtx, file_names: *const *const c_char, count: i32) -> FfiStatus {
    if ctx.is_null() || file_names.is_null() || count <= 0 {
        return FfiStatus::NullArg;
    }

    unsafe {
        let ctx = &mut *(ctx as *mut Ctx);
        for i in 0..count {
            let name_ptr = *file_names.offset(i as isize);
            if !name_ptr.is_null() {
                let name_str = CStr::from_ptr(name_ptr as *const c_char).to_str().unwrap_or("").to_string();
                ctx.logging_file_names.push(name_str);
            }
        }
    }

    FfiStatus::Ok
}

/// Sends a task to the engine via the context's packet handle.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance
/// * `task` - The opaque pointer to the Task instance
///
/// # Returns
///
/// * `FfiStatus::Ok` if the task was sent successfully
/// * `FfiStatus::NullArg` if ctx or task is NULL
#[unsafe(no_mangle)]
pub extern "C" fn ffi_ctx_send_task(ctx: FfiCtx, task: FfiTask) -> FfiStatus {
    if ctx.is_null() || task.is_null() {
        return FfiStatus::NullArg;
    }

    unsafe {
        let ctx = &*(ctx as *const Ctx);
        let task = Box::from_raw(task as *mut Task);
        ctx.packet_handle().send_task(*task);
        FfiStatus::Ok
    }
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
/// `ffi_packet_handle_free`), or NULL if ctx is NULL.
///
/// # Example
///
/// ```c
/// FfiCtx ctx = ffi_ctx_create(config);
/// FfiPacketHandle pkt = ffi_ctx_get_packet_handle(ctx);
/// if (pkt != NULL) {
///     // Use pkt to send tasks directly to the engine
///     ffi_ctx_send_task(ctx, task);
///     // or
///     ffi_packet_handle_send_task(pkt, task);
///     ffi_packet_handle_free(pkt);
/// }
/// ffi_ctx_free(ctx);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_ctx_get_packet_handle(ctx: FfiCtx) -> FfiPacketHandle {
    if ctx.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let ctx = &*(ctx as *const Ctx);
        let cloned = ctx.packet_handle().clone();
        let boxed = Box::into_raw(Box::new(cloned)) as FfiPacketHandle;
        if boxed.is_null() {
            return std::ptr::null_mut();
        }
        boxed
    }
}

// ============================================================================
// PacketHandle Task/Notification Senders
// ============================================================================

/// Sends a task using a PacketHandle directly.
///
/// # Arguments
///
/// * `handle` - The opaque pointer to the PacketHandle instance
/// * `task` - The opaque pointer to the Task instance
///
/// # Returns
///
/// * `FfiStatus::Ok` if the task was sent successfully
/// * `FfiStatus::NullArg` if handle or task is NULL
#[unsafe(no_mangle)]
pub extern "C" fn ffi_packet_handle_send_task(handle: FfiPacketHandle, task: FfiTask) -> FfiStatus {
    if handle.is_null() || task.is_null() {
        return FfiStatus::NullArg;
    }

    unsafe {
        let pkt = &*(handle as *const PacketHandle);
        let task = Box::from_raw(task as *mut Task);
        pkt.send_task(*task);
        FfiStatus::Ok
    }
}

/// Sends a notification using a PacketHandle directly.
///
/// # Arguments
///
/// * `handle` - The opaque pointer to the PacketHandle instance
/// * `notif` - The opaque pointer to the Notification instance
///
/// # Returns
///
/// * `FfiStatus::Ok` if the notification was sent successfully
/// * `FfiStatus::NullArg` if handle or notif is NULL
#[unsafe(no_mangle)]
pub extern "C" fn ffi_packet_handle_send_notification(handle: FfiPacketHandle, notif: FfiNotif) -> FfiStatus {
    if handle.is_null() || notif.is_null() {
        return FfiStatus::NullArg;
    }

    unsafe {
        let pkt = &*(handle as *const PacketHandle);
        let n = Box::from_raw(notif as *mut Notif);
        pkt.send_notification(*n);
        FfiStatus::Ok
    }
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
/// An opaque pointer to the Task instance, or NULL on failure.
#[unsafe(no_mangle)]
pub extern "C" fn ffi_task_create(name: *const c_char, priority: u8, job: FfiTaskJob) -> FfiTask {
    if name.is_null() {
        return std::ptr::null_mut();
    }

    unsafe {
        let task_name = CStr::from_ptr(name).to_str().unwrap_or("Unknown").to_string();
        // Create the TaskJob wrapper — wrap the FFI function pointer in a TaskJob closure
        let task_job = TaskJob::new(move || {
            let ffi_result = (job)();
            // Convert FfiTaskResult to res::Result
            ffi_result_to_result(ffi_result)
        });
        let task = Task::new(&task_name, priority, task_job);
    
        let boxed = Box::into_raw(Box::new(task)) as FfiTask;
        if boxed.is_null() {
            return std::ptr::null_mut();
        }
        boxed
    }
    
}

/// Frees a task instance.
///
/// # Arguments
///
/// * `task` - The opaque pointer to the Task instance to free.
///           If NULL, this function does nothing.
#[unsafe(no_mangle)]
pub extern "C" fn ffi_task_free(task: FfiTask) {
    if !task.is_null() {
        unsafe {
            let _ = Box::from_raw(task as *mut Task);
        }
    }
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
/// * `ctx` - The opaque pointer to the Ctx instance
///
/// # Returns
///
/// An opaque pointer to the JoinHandle, or NULL on failure.
///
/// # Examples
///
/// ```c
/// FfiCtx ctx = ffi_ctx_create(config);
/// FfiPacketHandle pkt = ffi_ctx_get_packet_handle(ctx);
/// FfiRunHandle handle = ffi_run_interactive_start(ctx);
/// if (handle != NULL) {
///     ffi_run_interactive_free(handle);
/// }
/// ffi_packet_handle_free(pkt);
/// ffi_ctx_free(ctx);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_run_interactive_start(ctx: FfiCtx) -> FfiRunHandle {
    if ctx.is_null() {
        return std::ptr::null_mut();
    }

    let ctx = unsafe { Box::from_raw(ctx as *mut Ctx) };
    let ctx_val = *ctx;

    let handle = inner_run_interactive(ctx_val);
    match handle {
        Some(h) => {
            let boxed = Box::into_raw(Box::new(h)) as FfiRunHandle;
            if boxed.is_null() {
                return std::ptr::null_mut();
            }
            boxed
        }
        None => std::ptr::null_mut(),
    }
}

/// Waits for the interactive engine to finish and frees the handle.
///
/// # Arguments
///
/// * `handle` - The opaque pointer to the JoinHandle.
///              If NULL, this function does nothing.
///
/// # Returns
///
/// * `FfiStatus::Ok` if the handle was joined successfully
/// * `FfiStatus::NullArg` if handle is NULL
///
/// # Examples
///
/// ```c
/// FfiRunHandle handle = ffi_run_interactive_start(ctx);
/// if (handle != NULL) {
///     ffi_run_interactive_free(handle);
/// }
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_run_interactive_free(handle: FfiRunHandle) -> FfiStatus {
    if handle.is_null() {
        return FfiStatus::NullArg;
    }

    unsafe {
        let h = Box::from_raw(handle as *mut JoinHandle<()>);
        let _ = h.join();
    }

    FfiStatus::Ok
}

// ============================================================================
// Service Wrappers
// ============================================================================

/// Creates a service instance from a configuration.
///
/// This function constructs a Thryve service ready for execution as a Windows service.
///
/// # Arguments
///
/// * `config` - The opaque pointer to the Config instance
///
/// # Returns
///
/// An opaque pointer to the Thryve service instance, or NULL on failure.
///
/// # Examples
///
/// ```c
/// FfiConfig config = ffi_config_default();
/// FfiService service = ffi_service_create(config);
/// if (service != NULL) {
///     ffi_service_free(service);
/// }
/// ffi_config_free(config);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_service_create(config: FfiConfig) -> FfiService {
    if config.is_null() {
        return std::ptr::null_mut();
    }

    let config = unsafe { &*(config as *const Config) };
    let (service, _ctx) = inner_create_service(*config);
    
    let boxed = Box::into_raw(Box::new(service)) as FfiService;
    if boxed.is_null() {
        return std::ptr::null_mut();
    }
    boxed
}

/// Runs the service with the given context.
///
/// This function starts the service and blocks until the service is stopped.
///
/// # Arguments
///
/// * `service` - The opaque pointer to the Thryve service instance
/// * `ctx` - The opaque pointer to the Ctx instance
///
/// # Returns
///
/// * `FfiStatus::Ok` if the service ran successfully
/// * `FfiStatus::NullArg` if either pointer is NULL
/// * `FfiStatus::InvalidState` if the service is in an invalid state
///
/// # Examples
///
/// ```c
/// FfiConfig config = ffi_config_default();
/// FfiService service = ffi_service_create(config);
/// FfiCtx ctx = ffi_ctx_create(config);
/// if (service != NULL && ctx != NULL) {
///     ffi_service_run(service, ctx);
/// }
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_service_run(service: FfiService, ctx: FfiCtx) -> FfiStatus {
    if service.is_null() || ctx.is_null() {
        return FfiStatus::NullArg;
    }

    // Take ownership of the service and ctx
    let service = unsafe { Box::from_raw(service as *mut Thryve) };
    let ctx = unsafe { Box::from_raw(ctx as *mut Ctx) };

    // Run the service (this is a blocking operation)
    inner_try_run_as_service(*service, *ctx);

    FfiStatus::Ok
}

/// Frees a service instance.
///
/// # Arguments
///
/// * `service` - The opaque pointer to the Thryve service instance to free.
///              If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// FfiService service = ffi_service_create(config);
/// ffi_service_free(service);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_service_free(service: FfiService) {
    if !service.is_null() {
        unsafe {
            let _ = Box::from_raw(service as *mut Thryve);
        }
    }
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
/// * `plg_name` - The plugin name (null-terminated C string). **Consumed** — do not free after calling.
/// * `plg_type` - The plugin type (null-terminated C string). **Consumed** — do not free after calling.
/// * `plg_core_ptr` - An opaque pointer to the plugin core data maintained by the foreign language.
///   This pointer is passed back to `clean_up` and is available to the plugin's logic.
/// * `tx` - The opaque pointer to the PacketHandle instance. This is **cloned** inside the plugin,
///   so the PacketHandle remains valid after this call and can be used elsewhere.
/// * `clean_up` - Function pointer called when the plugin is cleaned up.
///   The `plg_core_ptr` is passed as an argument.
/// * `entry_point` - Function pointer that returns the task execution result.
///   **Takes ownership** — do not free this pointer after calling.
///
/// # Returns
///
/// An opaque pointer to the Plugin instance, or NULL on failure.
///
/// # PacketHandle Usage
///
/// To use `tx`, you must first create a PacketHandle:
///
/// ```c
/// // Create a configuration first
/// FfiConfig config = ffi_config_default();
///
/// // Create a PacketHandle from the config
/// FfiPacketHandle packet_handle = ffi_packet_handle_create(config);
///
/// // Now use it when creating the plugin
/// FfiPlugin plugin = ffi_plugin_create(
///     "MyPlugin",
///     "MyType",
///     my_core_data,
///     packet_handle,  // <-- use the PacketHandle
///     my_plugin_clean_up,
///     my_plugin_entry_point
/// );
///
/// // The PacketHandle is still valid and can be used for other plugins/observers
/// ffi_packet_handle_free(packet_handle);
/// ffi_config_free(config);
/// ```
///
/// # Examples
///
/// ```c
/// // In C, you would define your plugin logic functions:
/// void my_plugin_clean_up(const void* core_ptr) {
///     // Cleanup logic here
/// }
///
/// FfiTaskResult my_plugin_entry_point(void) {
///     FfiTaskResult result = {0};
///     // Execute plugin logic here
///     return result;
/// }
///
/// // Then create the plugin:
/// FfiPlugin plugin = ffi_plugin_create(
///     "MyPlugin",
///     "MyType",
///     my_core_data,
///     packet_handle,
///     my_plugin_clean_up,
///     my_plugin_entry_point
/// );
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_plugin_create(
    plg_name: *const c_char,
    plg_type: *const c_char,
    plg_core_ptr: *mut u8,
    tx: FfiPacketHandle,
    clean_up: FfiPluginCleanUp,
    entry_point: FfiPluginEntryPoint,
) -> FfiPlugin {
    if plg_name.is_null() || plg_type.is_null() || tx.is_null() {
        return std::ptr::null_mut();
    }

    let name_str = unsafe { CStr::from_ptr(plg_name) }.to_str().unwrap_or("Unknown");
    let type_str = unsafe { CStr::from_ptr(plg_type) }.to_str().unwrap_or("Unknown");
    let plg_name = name_str;
    let plg_type = type_str;

    // Create the packet handle clone
    let packet_handle = unsafe { &*(tx as *const PacketHandle) }.clone();

    // Create the TaskJob wrapper — wrap the FFI function pointer in a TaskJob closure
    let task_job = TaskJob::new(move || {
        let ffi_result = (entry_point)();
        // Convert FfiTaskResult to res::Result
        ffi_result_to_result(ffi_result)
    });

    
    // Create the PlgLogic adapter - PluginLogicAdapter already implements PlgLogic
    let adapter: Arc<dyn PlgLogic> = Arc::new(PluginLogicAdapter::new(
        plg_core_ptr,
        clean_up,
    ));

    let plugin = Plugin::new(
        plg_name,
        plg_type,
        adapter,
        packet_handle,
        task_job,
    );

    let boxed = Box::into_raw(Box::new(plugin)) as FfiPlugin;

    if boxed.is_null() {
        println!("  boxed is null!");
        return std::ptr::null_mut();
    }
    boxed
}

/// Frees a plugin instance.
///
/// # Arguments
///
/// * `plugin` - The opaque pointer to the Plugin instance to free.
///             If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// FfiPlugin plugin = ffi_plugin_create(...);
/// ffi_ctx_add_plugin(ctx, plugin);
/// ffi_plugin_free(plugin);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_plugin_free(plugin: FfiPlugin) {
    if !plugin.is_null() {
        unsafe {
            let _ = Box::from_raw(plugin as *mut Plugin);
        }
    }
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
/// * `obs_name` - The observer name (null-terminated C string). **Consumed** — do not free after calling.
/// * `obs_type` - The observer type (null-terminated C string). **Consumed** — do not free after calling.
/// * `obs_core_ptr` - An opaque pointer to the observer core data maintained by the foreign language.
///   This pointer is passed back to `clean_up`.
/// * `tx` - The opaque pointer to the PacketHandle instance. This is **cloned** inside the observer,
///   so the PacketHandle remains valid after this call and can be used elsewhere.
/// * `clean_up` - Function pointer called when the observer is cleaned up.
///   The `obs_core_ptr` is passed as an argument.
/// * `obs_job_fn` - Function pointer for the observer job (takes `FfiObserver`, returns nothing).
///   **This is the primary callback** for observer execution. The `FfiObserver` handle is a raw pointer
///   to an `Arc<Observer>` — see [Observer Job Memory Safety](#observer-job-memory-safety) for details.
///
/// # Returns
///
/// An opaque pointer to the Observer instance, or NULL on failure.
///
/// # PacketHandle Usage
///
/// To use `tx`, you must first create a PacketHandle:
///
/// ```c
/// // Create a configuration first
/// FfiConfig config = ffi_config_default();
///
/// // Create a PacketHandle from the config
/// FfiPacketHandle packet_handle = ffi_packet_handle_create(config);
///
/// // Now use it when creating the observer
/// FfiObserver observer = ffi_observer_create(
///     "MyObserver",
///     "MyType",
///     my_core_data,
///     packet_handle,  // <-- use the PacketHandle
///     my_observer_clean_up,
///     my_observer_job
/// );
///
/// // The PacketHandle is still valid and can be used for other plugins/observers
/// ffi_packet_handle_free(packet_handle);
/// ffi_config_free(config);
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
/// void my_observer_job(FfiObserver obs) {
///     // Observer job logic here
/// }
///
/// // Then create the observer:
/// FfiObserver observer = ffi_observer_create(
///     "MyObserver",
///     "MyType",
///     my_core_data,
///     packet_handle,
///     my_observer_clean_up,
///     my_observer_job
/// );
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_observer_create(
    obs_name: *const c_char,
    obs_type: *const c_char,
    obs_core_ptr: *mut u8,
    tx: FfiPacketHandle,
    clean_up: FfiObserverCleanUp,
    obs_job_fn: FfiObsJob,
) -> FfiObserver {
    if obs_name.is_null() || obs_type.is_null() || tx.is_null() {
        return std::ptr::null_mut();
    }

    let obs_name = unsafe { CStr::from_ptr(obs_name) }.to_str().unwrap_or("Unknown");
    let obs_type = unsafe { CStr::from_ptr(obs_type) }.to_str().unwrap_or("Unknown");

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
        let ffi_observer = Arc::into_raw(observer.clone()) as FfiObserver;
        (obs_job_fn)(ffi_observer);
        // The original Arc is dropped here, but the FFI callback still holds
        // its copy, so the Observer stays alive during the callback execution.
    });

    // Create the ObsLogic adapter - ObserverLogicAdapter already implements ObsLogic
    let adapter: Arc<dyn ObsLogic> = Arc::new(ObserverLogicAdapter::new(
        obs_core_ptr_wrapper.0,
        clean_up,
    ));

    // Create the Observer
    let observer = Observer::new(
        obs_name,
        obs_type,
        adapter,
        obs_job,
        packet_handle,
    );

    let boxed = Box::into_raw(Box::new(observer)) as FfiObserver;
    if boxed.is_null() {
        return std::ptr::null_mut();
    }
    boxed
}

/// Frees an observer instance.
///
/// # Arguments
///
/// * `observer` - The opaque pointer to the Observer instance to free.
///               If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// FfiObserver observer = ffi_observer_create(...);
/// ffi_ctx_add_observer(ctx, observer);
/// ffi_observer_free(observer);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_observer_free(observer: FfiObserver) {
    if !observer.is_null() {
        unsafe {
            let _ = Box::from_raw(observer as *mut Observer);
        }
    }
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
/// An opaque pointer to the Notif instance, or NULL on failure.
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
/// FfiNotif notif = ffi_notification_create(
///     "MyNotifType",
///     "MyReceiverType",
///     my_core_data
/// );
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_notification_create(
    notif_type: *const c_char,
    notif_receiver: *const c_char,
    notif_core_ptr: *mut u8,
) -> FfiNotif {
    if notif_type.is_null() || notif_receiver.is_null() {
        return std::ptr::null_mut();
    }

    let notif_type = unsafe { CStr::from_ptr(notif_type) }.to_str().unwrap_or("Unknown");
    let notif_receiver = unsafe { CStr::from_ptr(notif_receiver) }.to_str().unwrap_or("Unknown");

    // Create the NotifLogic adapter
    let notif_core: Box<dyn NotifLogic> = 
        Box::new(NotificationLogicAdapter::new(notif_core_ptr));

    // Create the notification
    let notif = Notif::new(
        notif_type,
        notif_receiver,
        notif_core,
    );

    let boxed = Box::into_raw(Box::new(notif)) as FfiNotif;
    if boxed.is_null() {
        return std::ptr::null_mut();
    }
    boxed
}

/// Frees a notification instance.
///
/// # Arguments
///
/// * `notif` - The opaque pointer to the Notif instance to free.
///            If NULL, this function does nothing.
///
/// # Examples
///
/// ```c
/// FfiNotif notif = ffi_notification_create(...);
/// // ... use notif ...
/// ffi_notification_free(notif);
/// ```
#[unsafe(no_mangle)]
pub extern "C" fn ffi_notification_free(notif: FfiNotif) {
    if !notif.is_null() {
        unsafe {
            let _ = Box::from_raw(notif as *mut Notif);
        }
    }
}

/// Sends a notification to the engine via the context's packet handle.
///
/// # Arguments
///
/// * `ctx` - The opaque pointer to the Ctx instance
/// * `notif` - The opaque pointer to the Notif instance
///
/// # Returns
///
/// * `FfiStatus::Ok` if the notification was sent successfully
/// * `FfiStatus::NullArg` if ctx or notif is NULL
#[unsafe(no_mangle)]
pub extern "C" fn ffi_ctx_send_notification(ctx: FfiCtx, notif: FfiNotif) -> FfiStatus {
    if ctx.is_null() || notif.is_null() {
        return FfiStatus::NullArg;
    }

    unsafe {
        let ctx = &*(ctx as *const Ctx);
        let notif = Box::from_raw(notif as *mut Notif);
        ctx.packet_handle().send_notification(*notif);
        FfiStatus::Ok
    }
}

// ============================================================================
// FFI Adapter Implementations
// ============================================================================

/// Adapter for PlgLogic trait to support FFI callbacks.
struct PluginLogicAdapter {
    core_ptr: *mut u8,
    clean_up: FfiPluginCleanUp,
}

impl PluginLogicAdapter {
    fn new(core_ptr: *mut u8, clean_up: FfiPluginCleanUp) -> Self {
        Self { core_ptr, clean_up }
    }
}

unsafe impl Send for PluginLogicAdapter {}
unsafe impl Sync for PluginLogicAdapter {}

impl PlgLogic for PluginLogicAdapter {
    fn clean_up(&self) {
        // Function pointers are always valid if defined, no null check needed
        (self.clean_up)(self.core_ptr);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Adapter for ObsLogic trait to support FFI callbacks.
struct ObserverLogicAdapter {
    core_ptr: *mut u8,
    clean_up: FfiObserverCleanUp,
}

impl ObserverLogicAdapter {
    fn new(core_ptr: *mut u8, clean_up: FfiObserverCleanUp) -> Self {
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
struct NotificationLogicAdapter {
    core_ptr: *mut u8,
}

impl NotificationLogicAdapter {
    fn new(core_ptr: *mut u8) -> Self {
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
struct ObsCorePtr(*mut u8);

unsafe impl Send for ObsCorePtr {}
unsafe impl Sync for ObsCorePtr {}

