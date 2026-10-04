# Thryve FFI — C-Compatible Foreign Function Interface

[![License](https://img.shields.io/badge/license-AGPL--3.0%20%2F%20Commercial-blue.svg)](../../LICENSE)

Thryve FFI is distributed under the same dual-license terms as the rest of the Thryve workspace: **AGPL-3.0-or-later** for open-source use, or a **Commercial License** for closed-source integration. See [`LICENSE`](../../LICENSE) and [`COMMERCIAL.md`](../../COMMERCIAL.md).

C-compatible wrapper functions for the Thryve framework, enabling integration with other programming languages (C, C++, Python, Node.js, etc.).

## Table of Contents

- [Overview](#overview)
- [Design Principles](#design-principles)
- [Quick Start](#quick-start)
- [Types](#types)
- [Callback Types](#callback-types)
- [Memory Management](#memory-management)
- [Build System](#build-system)
- [API Reference](#api-reference)
- [Complete Example](#complete-example)

---

## Overview

The FFI layer exposes the Thryve framework's core types and functions through a C-compatible interface. This allows applications written in any language that supports C FFI to use Thryve without modifying Rust code.

The FFI layer handles:
- **Opaque pointers**: All complex Rust types are wrapped in `*mut u8` pointers
- **Memory safety**: Explicit ownership transfer rules prevent double-frees and leaks
- **Error handling**: Status codes and null pointers for error detection
- **Callback management**: Function pointers for plugin, observer, and notification logic

---

## Design Principles

1. **Opaque pointers**: All Rust types are wrapped in `*mut u8` pointers. Foreign languages never need to know the internal structure of Rust types.

2. **Explicit memory management**: Every `ffi_*_create` function has a corresponding `ffi_*_free` function. The foreign language is responsible for calling the free function when done.

3. **C-compatible calling convention**: All functions use `extern "C"` to ensure cross-language compatibility.

4. **Error handling**: Functions return `ThryveStatus` codes or `NULL` pointers on failure.

5. **Ownership transfer**: When passing pointers to Rust functions, ownership may be transferred. See [Memory Management](#memory-management) for details.

---

## Quick Start

Here's a minimal C example showing how to use the FFI layer. Note that a `PacketHandle` (obtained from `ctx`) is required for plugin creation:

```c
#include "thryve.h"

// Plugin cleanup function — called once at shutdown
int my_plugin_cleanup(const void* core_ptr) {
    // Cleanup logic here
    return ThryveStatus_Ok;  // any other value is logged to Error.log as a failed cleanup
}

// Plugin entry point function
ThryveTaskResult my_plugin_entry_point(void) {
    // Use the helper function to create a result with proper fields
    ThryveTaskResult result = thryve_task_result_new("Success", "Plugin executed", "Main");
    // Or construct manually:
    // ThryveTaskResult result = {0};
    // result.result_type = "Success";
    // result.message = "Plugin executed";
    // result.log_file = "Main";
    return result;
}

// Shutdown task job — a "Fin" result_type is what tells the engine to
// finalize and exit its loop. There is no separate "stop" FFI call.
ThryveTaskResult shutdown_task_job(void) {
    return thryve_task_result_new("Fin", "Shutting down", "Main");
}

int main(void) {
    // 1. Create configuration
    ThryveConfig config = thryve_config_default();
    if (!config) return 1;

    // 2. Create context from configuration
    ThryveCtx ctx = thryve_ctx_create(config);
    if (!ctx) {
        thryve_config_free(config);
        return 1;
    }

    // 3. Get the engine's connected PacketHandle from ctx
    ThryvePacketHandle packet_handle = thryve_ctx_get_packet_handle(ctx);
    if (!packet_handle) {
        thryve_ctx_free(ctx);
        thryve_config_free(config);
        return 1;
    }

    // 4. Create a plugin (PacketHandle is required)
    ThryvePlugin plugin = thryve_plugin_create(
        "MyType",             // type
        NULL,                 // plg_core_ptr (opaque pointer)
        packet_handle,        // tx (packet handle - REQUIRED)
        my_plugin_cleanup,    // clean_up callback
        my_plugin_entry_point // entry_point callback
    );

    // 5. Add plugin to context (takes ownership of the plugin)
    thryve_ctx_add_plugin(ctx, plugin);

    // 6. Start the engine. This consumes ctx — never touch or free ctx again.
    ThryveRunHandle handle = thryve_run_interactive_start(ctx);
    if (handle) {
        // Submit a shutdown task so the engine finalizes and its loop
        // exits — without this, thryve_run_interactive_free would hang forever.
        ThryveTask shutdown_task = thryve_task_create("Shutdown", 0, shutdown_task_job);
        if (shutdown_task) {
            thryve_packet_handle_send_task(packet_handle, shutdown_task);
        }
        thryve_run_interactive_free(handle);
    }

    // 7. Cleanup - plugin is already in ctx, and ctx was already consumed by
    // thryve_run_interactive_start, so don't free either of them again
    thryve_packet_handle_free(packet_handle);
    thryve_config_free(config);

    return 0;
}
```

---

## Types

### Opaque Pointers

All opaque pointers are `*mut u8` — foreign languages should treat them as opaque handles.

| Type | Rust Equivalent | Description |
|------|-----------------|-------------|
| `ThryveConfig` | `*mut Config` | Configuration instance |
| `ThryveCtx` | `*mut Ctx` | Context (engine) instance |
| `ThryveRunHandle` | `*mut JoinHandle<()>` | Thread handle for interactive mode |
| `ThryveService` | `*mut Thryve` | Windows service instance |
| `ThryvePlugin` | `*mut Plugin` | Plugin instance |
| `ThryveObserver` | `*mut Observer` | Observer instance |
| `ThryvePacketHandle` | `*mut PacketHandle` | Packet handle for sending tasks/notifications |
| `ThryveTask` | `*mut Task` | Task instance |
| `ThryveNotif` | `*mut Notif` | Notification instance |

`thryve_service_create` doesn't return a bare opaque pointer — it returns a small `#[repr(C)]` struct pairing a service with its matching context:

```c
typedef struct {
    ThryveService service;
    ThryveCtx ctx;
} ThryveServiceCreateResult;
```

### Status Codes

```c
typedef enum {
    FFI_OK = 0,
    FFI_NULL_ARG = 1,
    FFI_ALLOC_FAIL = 2,
    FFI_INVALID_STATE = 3,
    FFI_OPERATION_FAIL = 4,
    FFI_STR_CONV_ERROR = 5,
} ThryveStatus;
```

### Task Result

```c
typedef struct {
    const char *result_type;  // Result type (e.g., "Success", "Failed")
    const char *message;      // Human-readable result message
    const char *log_file;     // Log file name for the result
    bool has_chain;           // 1 if a chained task follows, 0 otherwise
    void *chain_task;         // Pointer to the chained task (valid only if has_chain is 1)
} ThryveTaskResult;
```

#### Constructing a Task Result

In C, use the helper functions to create results:

```c
// Simple result with proper fields
ThryveTaskResult result = thryve_task_result_new("Success", "Task completed", "Main");
```

If you want to return a chained task, use the helper function:

```c
ThryveTaskResult result = thryve_task_result_new_with_chain("Chained", "Chaining to next task", "Main", my_chained_task);
```

Or construct manually with zero-initialized memory:

```c
ThryveTaskResult result = {0};  // has_chain = 0, chain_task = NULL
result.result_type = "Success";
result.message = "Task completed";
result.log_file = "Main";
```

If you want to return a chained task manually:

```c
ThryveTaskResult result = {0};
result.result_type = "Chained";
result.message = "Chaining to next task";
result.log_file = "Main";
result.has_chain = 1;
result.chain_task = my_chained_task;  // Pointer to a ThryveTask created by thryve_task_create
```

**Important**:
- String fields (`result_type`, `message`, `log_file`) should point to static/const strings
  or strings that remain valid for the lifetime of the result. Do NOT free these pointers
  after returning the result.
- The `chain_task` pointer, if provided, is owned by the Rust side after the result is returned.
  Do NOT free the chained task manually.

---

## Callback Types

### Task Callbacks

```c
// Task job — takes no arguments, returns a task result
typedef ThryveTaskResult (*ThryveTaskJob)(void);

// Plugin entry point — same as ThryveTaskJob, used when creating plugins
typedef ThryveTaskJob ThryvePluginEntryPoint;

// Plugin cleanup — called once when the engine shuts down, after every task has stopped
// Takes a pointer to the plugin's core data (plg_core_ptr)
// Returns ThryveStatus_Ok (0) if the cleanup worked; any other value means it failed,
// and Thryve logs that the plugin failed to clean up to Error.log
typedef int (*ThryvePluginCleanUp)(const void* core_ptr);
```

### Observer Callbacks

```c
// Observer job — primary callback for observer execution
// Takes an ThryveObserver (raw pointer to Arc<Observer>)
// IMPORTANT: See Observer Job Memory Safety below
typedef void (*ThryveObsJob)(ThryveObserver obs);

// Observer cleanup — called once when the engine shuts down, on a thread of its own
// Takes a pointer to the observer's core data (obs_core_ptr)
typedef void (*ThryveObserverCleanUp)(const void* core_ptr);

// Observer main loop — currently unused
typedef void (*ThryveObserverMainLoop)(const void* obs_ptr);
```

#### Observer Job Memory Safety

The `ThryveObsJob` callback receives a raw pointer to an `Arc<Observer>`. The Rust side creates this by:
1. Cloning the Arc: `observer.clone()`
2. Converting to raw pointer: `Arc::into_raw(observer_clone) as ThryveObserver`
3. Calling the FFI callback with this raw pointer

**The FFI callback must** call `thryve_observer_free()` on the received handle before returning — this is what triggers Rust to reconstruct and drop the `Arc<Observer>` internally. There's no separate "convert back to an Arc" step on the C/C++ side: `Observer` is a Rust-only type, never exposed across the FFI boundary, so nothing to `static_cast` to.

**C++ example**:
```cpp
extern "C" void my_observer_job(ThryveObserver obs) {
    // Do your observer work here using the opaque obs handle if needed
    // (e.g., pass it to other ffi_* calls) ...

    // Then drop it — this is the "3. Drop the Arc when done" step. Nothing
    // does this automatically; skipping this call leaks the Observer.
    thryve_observer_free(obs);
}
```

**Critical**: If the callback crashes or skips `thryve_observer_free`, the Observer will be leaked.

### Notification Callbacks

```c
// Notification logic — called when the notification needs to be processed
// Takes a pointer to the notification's core data (notif_core_ptr)
typedef void (*ThryveNotifLogic)(const void* core_ptr);
```

---

## Memory Management

### Ownership Transfer Rules

When passing data to Rust functions, ownership may be transferred. Follow these rules:

1. **Callback pointers**: `entry_point` and `job` parameters are transmuted into Rust closures. The foreign language must **never free** these pointers after passing them.

2. **String pointers**: `name`, `type`, and similar string arguments are consumed via `CString::from_raw`. The foreign language must **not free** these pointers after passing them.

3. **Plugin/Observer creation**: After calling `thryve_plugin_create` or `thryve_observer_create`, the plugin/observer is owned by Rust. Free it with `thryve_plugin_free` or `thryve_observer_free` when done.

4. **Context addition**: After calling `thryve_ctx_add_plugin` or `thryve_ctx_add_observer`, the plugin/observer is moved into the context's vectors. **Do not free it** — the context will clean it up.

5. **PacketHandle**: Get it from `thryve_ctx_get_packet_handle(ctx)` — the engine's real, connected handle. It is **cloned** inside the Rust side, so the foreign language can continue to use its copy after passing to plugin/observer creation.

6. **Sending tasks/notifications**: `thryve_ctx_send_task`, `thryve_ctx_send_notification`, `thryve_packet_handle_send_task`, and `thryve_packet_handle_send_notification` all **take ownership** of the task/notification passed in. **Do not** call `thryve_task_free`/`thryve_notification_free` on it afterward. This holds even when the send fails: once the engine is gone, they return `InvalidState` and drop what was passed in.

7. **`thryve_ctx_create` vs `thryve_service_create`**: `thryve_ctx_create(config)` is for **interactive mode** only. Service mode doesn't use it — `thryve_service_create(config)` already returns its own matching `ctx` bundled together with the service (see [`ThryveServiceCreateResult`](#types)).

8. **`thryve_run_interactive_start`**: consumes `ctx` unconditionally. After calling it, `ctx` must never be freed or otherwise used again.

### Ownership Transfer Diagram

```
Foreign Language                              Rust
──────────────                                ──────
                                              ┌─────────────────────────┐
1. thryve_config_default()                       │ Creates and owns Config   │
   → ThryveConfig                                 └─────────────────────────┘
                                              ┌─────────────────────────┐
2. thryve_ctx_create(config)                     │ Creates and owns Ctx      │
   → ThryveCtx                                    └─────────────────────────┘
                                              ┌─────────────────────────┐
3. thryve_plugin_create(type, core, tx,          │ Takes ownership of:       │
   clean_up, entry_point)                      │ - type (CString)          │
   → ThryvePlugin                                 │ - entry_point (TaskJob)   │
                                              │ Does NOT take:            │
                                              │ - clean_up                │
                                              │ - plg_core_ptr (passed    │
                                              │   through adapter)        │
                                              └─────────────────────────┘
                                              ┌─────────────────────────┐
4. thryve_ctx_add_plugin(ctx, plugin)            │ Clones plugin into ctx    │
   → ThryveStatus                                 │ Does NOT take ownership │
                                              └─────────────────────────┘
                                              ┌─────────────────────────┐
5. thryve_plugin_free(plugin)                    │ Frees the plugin          │
   (after ctx_add_plugin)                      │ (if NOT added to ctx)   │
                                              └─────────────────────────┘
```

### Free Functions

Every `create` function has a corresponding `free` function:

| Create Function | Free Function |
|-----------------|---------------|
| `thryve_config_default()` / `thryve_config_from_file()` | `thryve_config_free()` |
| `thryve_ctx_create()` (interactive mode only) | `thryve_ctx_free()` (unless consumed by `thryve_run_interactive_start`) |
| `thryve_plugin_create()` | `thryve_plugin_free()` |
| `thryve_observer_create()` | `thryve_observer_free()` |
| `thryve_task_create()` | `thryve_task_free()` (unless already sent via a `*_send_task` call) |
| `thryve_service_create()` → `.service` | `thryve_service_free()` (unless consumed by `thryve_service_run`) |
| `thryve_service_create()` → `.ctx` | `thryve_ctx_free()` (unless consumed by `thryve_service_run`) |
| `thryve_ctx_get_packet_handle()` / `thryve_packet_handle_clone()` | `thryve_packet_handle_free()` |
| `thryve_notification_create()` | `thryve_notification_free()` (unless already sent via a `*_send_notification` call) |

There is no `ffi_packet_handle_create()` — a standalone packet handle would not be wired to any engine. `thryve_ctx_get_packet_handle(ctx)` is the only way to obtain one.

---

## Build System

The FFI library uses [cbindgen](https://github.com/mozilla/cbindgen) to automatically generate C/C++ header files during the build process. This ensures the headers are always in sync with the Rust definitions.

### Auto-Generated Headers

When you build the FFI library with Cargo, the `build.rs` script automatically generates two header files in the `OUT_DIR`:

- **`thryve.h`** — The C-compatible header, generated by cbindgen. Include this from C code.
- **`thryve_ffi.h`** — The C++ wrapper header, generated by `build.rs`. It includes `thryve.h` inside `extern "C"` guards. Include this from C++ code.

### Including in C/C++ Code

**C code:**
```c
#include "thryve.h"
```

**C++ code:**
```cpp
#include "thryve_ffi.h"  // Wrapper with extern "C"
```

### Building

Simply run:
```bash
cargo build --release
```

The headers will be generated in the `OUT_DIR`. To find the `OUT_DIR`:
```bash
cargo rustc --package thryve_ffi -- --print out-dir
```

Then add that directory to your include path:
```bash
# Using rustc directly
rustc ... -I $(cargo rustc --package thryve_ffi -- --print out-dir)
```

### Manual Header Generation

If you need to regenerate the headers manually (e.g., for editing or distribution):
```bash
cargo run --bin cbindgen -- -o thryve.h
```

### Build System Files

The `thryve_ffi` directory includes example build files for integrating with C/C++ projects:

- **`CMakeLists.txt`** — CMake build file for integrating with CMake projects
- **`Makefile`** — GNU Make file for simple build workflows

These files demonstrate how to include the generated headers and link against the compiled FFI library.

---

## API Reference

### Config Wrappers

| Function | Description |
|----------|-------------|
| `thryve_config_default()` | Creates a default configuration instance. Returns `ThryveConfig` or NULL. |
| `thryve_config_from_file(path)` | Creates a configuration from a TOML file. `path` is a null-terminated C string. Returns `ThryveConfig`, or NULL if the file can't be read, doesn't parse (every key is required, unknown keys are rejected) or has invalid values. There is no fallback to the defaults; call `thryve_config_default()` if you want them. |
| `thryve_config_free(config)` | Frees a configuration instance. Safe to call with NULL. |

### PacketHandle Wrappers

| Function | Description |
|----------|-------------|
| `thryve_packet_handle_free(handle)` | Frees a PacketHandle instance. Safe to call with NULL. |
| `thryve_packet_handle_clone(handle)` | Clones a PacketHandle. Returns a new `ThryvePacketHandle`. |
| `thryve_packet_handle_send_task(handle, task)` | Sends a task through the handle. **Takes ownership of `task`.** Returns `ThryveStatus`: `InvalidState` if the engine isn't running (the task is dropped). |
| `thryve_packet_handle_send_notification(handle, notif)` | Sends a notification through the handle. **Takes ownership of `notif`.** Returns `ThryveStatus`: `InvalidState` if the engine isn't running (the notification is dropped). |

### Ctx Wrappers

| Function | Description |
|----------|-------------|
| `thryve_ctx_create(config)` | Creates a context from a configuration (interactive mode only — service mode gets its ctx from `thryve_service_create`). Returns `ThryveCtx` or NULL. |
| `thryve_ctx_free(ctx)` | Frees a context instance. Safe to call with NULL. Do not call on a `ctx` already consumed by `thryve_run_interactive_start`/`thryve_service_run`. |
| `thryve_ctx_get_packet_handle(ctx)` | Returns the engine's real, connected `ThryvePacketHandle` for this ctx — the only correct way to obtain one. |
| `thryve_ctx_add_plugin(ctx, plugin)` | Adds a plugin to the context. Returns `ThryveStatus`. |
| `thryve_ctx_add_observer(ctx, observer)` | Adds an observer to the context. Returns `ThryveStatus`. |
| `thryve_ctx_set_logging_file_names(ctx, file_names, count)` | Sets logging file names. `file_names` is an array of null-terminated C strings. |
| `thryve_ctx_send_task(ctx, task)` | Sends a task to the engine. **Takes ownership of `task`.** Returns `ThryveStatus`: `InvalidState` if the engine isn't running (the task is dropped). |
| `thryve_ctx_send_notification(ctx, notif)` | Sends a notification to the engine. **Takes ownership of `notif`.** Returns `ThryveStatus`: `InvalidState` if the engine isn't running (the notification is dropped). |

### Task Wrappers

| Function | Description |
|----------|-------------|
| `thryve_task_create(name, priority, job)` | Creates a new task. `name` is a null-terminated C string. `job` is a `ThryveTaskJob` function pointer. **Takes ownership of `job`**. Returns `ThryveTask` or NULL. |
| `thryve_task_free(task)` | Frees a task instance. Safe to call with NULL. |
| `thryve_task_result_new(result_type, message, log_file)` | Creates a `ThryveTaskResult` with the specified fields. All parameters are null-terminated C strings. Returns `ThryveTaskResult`. |
| `thryve_task_result_new_with_chain(result_type, message, log_file, chain_task)` | Creates a `ThryveTaskResult` with a chained task. Returns `ThryveTaskResult`. |

### Run Interactive Wrappers

| Function | Description |
|----------|-------------|
| `thryve_run_interactive_start(ctx)` | Starts the Thryve engine in interactive mode. Returns `ThryveRunHandle`, or NULL if the engine couldn't start (it stops every thread it started, and its error is in `Error.log` once the log folder exists). |
| `thryve_run_interactive_free(handle)` | Waits for the engine to finish and frees the handle. Returns `ThryveStatus`. |

### Service Wrappers

| Function | Description |
|----------|-------------|
| `thryve_service_create(config)` | Creates a service instance bundled with its matching ctx (for Windows service mode). Returns `ThryveServiceCreateResult { service, ctx }` — both NULL on failure. |
| `thryve_service_run(service, ctx)` | Runs the service (blocking until SCM sends Stop). **Takes ownership of both `service` and `ctx`.** Returns `ThryveStatus`: `Ok` once the service has stopped, `InvalidState` if the process wasn't started by the SCM, `OperationFail` if the engine couldn't start. |
| `thryve_service_free(service)` | Frees a service instance. Safe to call with NULL. Do not call on a `service` already consumed by `thryve_service_run`. |

### Plugin Wrappers

| Function | Description |
|----------|-------------|
| `thryve_plugin_create(type, core_ptr, tx, clean_up, entry_point)` | Creates a new plugin. Returns `ThryvePlugin` or NULL. |
| `thryve_plugin_free(plugin)` | Frees a plugin instance. Safe to call with NULL. |

### Observer Wrappers

| Function | Description |
|----------|-------------|
| `thryve_observer_create(type, core_ptr, tx, clean_up, obs_job_fn)` | Creates a new observer. Returns `ThryveObserver` or NULL. |
| `thryve_observer_free(observer)` | Frees an observer instance. Safe to call with NULL. |

### Notification Wrappers

| Function | Description |
|----------|-------------|
| `thryve_notification_create(notif_type, receiver, core_ptr)` | Creates a new notification. Returns `ThryveNotif` or NULL. |
| `thryve_notification_free(notif)` | Frees a notification instance. Safe to call with NULL. |

---

## FFI Adapter Implementations

The FFI layer uses adapter structs to bridge C function pointers with Rust traits:

### PluginLogicAdapter

Wraps `plg_type: String`, `core_ptr: *mut u8` and `clean_up: ThryvePluginCleanUp`. Implements the `PlgLogic` trait: a callback that returns anything other than `ThryveStatus_Ok` becomes a `FinalizationError::PluginCleanup` naming the plugin's type. Marked `Send + Sync` for use in `Arc`.

### ObserverLogicAdapter

Wraps `core_ptr: *mut u8` and `clean_up: ThryveObserverCleanUp`. Implements the `ObsLogic` trait. Marked `Send + Sync` for use in `Arc`.

### NotificationLogicAdapter

Wraps `core_ptr: *mut u8`. Implements the `NotifLogic` trait. Marked `Send + Sync` for use in `Arc`.

### ObsCorePtr

A wrapper for `*mut u8` that implements `Send + Sync`. Used in the observer creation closure to allow capturing `obs_core_ptr` in the closure.

---

## Complete C Example

A complete C example showing the full lifecycle with PacketHandle:

```c
#include "thryve.h"  // C-compatible header (generated by cbindgen)

// ===== Plugin Functions =====
int my_plugin_cleanup(const void* core_ptr) {
    // Free plugin resources
    return ThryveStatus_Ok;
}

ThryveTaskResult my_plugin_entry(void) {
    ThryveTaskResult result = {0};
    // Execute plugin logic
    return result;
}

// ===== Observer Functions =====
void my_observer_cleanup(const void* core_ptr) {
    // Free observer resources
}

void my_observer_job(ThryveObserver obs) {
    // In C, the FFI callback receives a raw pointer to Arc<Observer>.
    // You must manage the Arc lifecycle manually:
    // 1. Convert the raw pointer back to Arc
    // 2. Use the Arc to access the observer
    // 3. Drop the Arc when done
    
    // Note: This is simplified - in practice, you'd need to
    // use C++ or platform-specific mechanisms to properly
    // manage the Arc lifecycle from C code.
}

// Shutdown task job — a "Fin" result_type is what tells the engine to
// finalize and exit its loop. There is no separate "stop" FFI call.
ThryveTaskResult shutdown_task_job(void) {
    return thryve_task_result_new("Fin", "Shutting down", "Main");
}

// ===== Main =====
int main(void) {
    // 1. Create configuration
    ThryveConfig config = thryve_config_default();
    if (!config) return 1;

    // 2. Create context
    ThryveCtx ctx = thryve_ctx_create(config);
    if (!ctx) {
        thryve_config_free(config);
        return 1;
    }

    // 3. Get the engine's connected PacketHandle from ctx (required for
    // plugin/observer creation)
    ThryvePacketHandle packet_handle = thryve_ctx_get_packet_handle(ctx);
    if (!packet_handle) {
        thryve_ctx_free(ctx);
        thryve_config_free(config);
        return 1;
    }

    // 4. Create plugin (PacketHandle is required)
    ThryvePlugin plugin = thryve_plugin_create(
        "MyType",
        NULL,  // core_ptr
        packet_handle,  // tx (PacketHandle)
        my_plugin_cleanup,
        my_plugin_entry
    );

    // 5. Add plugin to context (takes ownership)
    thryve_ctx_add_plugin(ctx, plugin);

    // 6. Create observer (PacketHandle is required)
    ThryveObserver observer = thryve_observer_create(
        "MyType",
        NULL,  // core_ptr
        packet_handle,  // tx (PacketHandle)
        my_observer_cleanup,
        my_observer_job
    );

    // 7. Add observer to context (takes ownership)
    thryve_ctx_add_observer(ctx, observer);

    // 8. Start engine. This consumes ctx — never touch or free ctx again.
    ThryveRunHandle handle = thryve_run_interactive_start(ctx);
    if (handle) {
        // Submit a shutdown task so the engine finalizes and its loop
        // exits — without this, thryve_run_interactive_free would hang forever.
        ThryveTask shutdown_task = thryve_task_create("Shutdown", 0, shutdown_task_job);
        if (shutdown_task) {
            thryve_packet_handle_send_task(packet_handle, shutdown_task);
        }
        thryve_run_interactive_free(handle);
    }

    // 9. Cleanup
    // Plugin and observer are already in ctx, and ctx was already consumed
    // by thryve_run_interactive_start, so don't free any of them here
    thryve_packet_handle_free(packet_handle);
    thryve_config_free(config);

    return 0;
}
```

## Complete C++ Example

A complete C++ example showing the full lifecycle with PacketHandle:

```c++
#include "thryve_ffi.h"  // C++ wrapper with extern "C" (generated by build.rs)

// ===== Plugin Functions =====
extern "C" int my_plugin_cleanup(const void* core_ptr) {
    // Free plugin resources
    return ThryveStatus_Ok;
}

extern "C" ThryveTaskResult my_plugin_entry(void) {
    ThryveTaskResult result = {0};
    // Execute plugin logic
    return result;
}

// ===== Observer Functions =====
extern "C" void my_observer_cleanup(const void* core_ptr) {
    // Free observer resources
}

extern "C" void my_observer_job(ThryveObserver obs) {
    // Do your observer work here using the opaque obs handle if needed ...

    // Then drop it — nothing does this automatically; skipping this call
    // leaks the Observer.
    thryve_observer_free(obs);
}

// Shutdown task job — a "Fin" result_type is what tells the engine to
// finalize and exit its loop. There is no separate "stop" FFI call.
extern "C" ThryveTaskResult shutdown_task_job(void) {
    return thryve_task_result_new("Fin", "Shutting down", "Main");
}

// ===== Main =====
int main(void) {
    // 1. Create configuration
    ThryveConfig config = thryve_config_default();
    if (!config) return 1;

    // 2. Create context
    ThryveCtx ctx = thryve_ctx_create(config);
    if (!ctx) {
        thryve_config_free(config);
        return 1;
    }

    // 3. Get the engine's connected PacketHandle from ctx (required for
    // plugin/observer creation)
    ThryvePacketHandle packet_handle = thryve_ctx_get_packet_handle(ctx);
    if (!packet_handle) {
        thryve_ctx_free(ctx);
        thryve_config_free(config);
        return 1;
    }

    // 4. Create plugin (PacketHandle is required)
    ThryvePlugin plugin = thryve_plugin_create(
        "MyType",
        nullptr,  // core_ptr
        packet_handle,  // tx (PacketHandle)
        my_plugin_cleanup,
        my_plugin_entry
    );

    // 5. Add plugin to context (takes ownership)
    thryve_ctx_add_plugin(ctx, plugin);

    // 6. Create observer (PacketHandle is required)
    ThryveObserver observer = thryve_observer_create(
        "MyType",
        nullptr,  // core_ptr
        packet_handle,  // tx (PacketHandle)
        my_observer_cleanup,
        my_observer_job
    );

    // 7. Add observer to context (takes ownership)
    thryve_ctx_add_observer(ctx, observer);

    // 8. Start engine. This consumes ctx — never touch or free ctx again.
    ThryveRunHandle handle = thryve_run_interactive_start(ctx);
    if (handle) {
        // Submit a shutdown task so the engine finalizes and its loop
        // exits — without this, thryve_run_interactive_free would hang forever.
        ThryveTask shutdown_task = thryve_task_create("Shutdown", 0, shutdown_task_job);
        if (shutdown_task) {
            thryve_packet_handle_send_task(packet_handle, shutdown_task);
        }
        thryve_run_interactive_free(handle);
    }

    // 9. Cleanup
    // Plugin and observer are already in ctx, and ctx was already consumed
    // by thryve_run_interactive_start, so don't free any of them here
    thryve_packet_handle_free(packet_handle);
    thryve_config_free(config);

    return 0;
}
```

---

## Related Files

- [`src/lib.rs`](src/lib.rs) — Main FFI implementation
- [`Cargo.toml`](Cargo.toml) — Dependencies
- [`thryve_utils/src/traits/plugin_logic.rs`](../thryve_utils/src/traits/plugin_logic.rs) — PlgLogic trait
- [`thryve_utils/src/traits/observer_logic.rs`](../thryve_utils/src/traits/observer_logic.rs) — ObsLogic trait
- [`thryve_utils/src/traits/notif_logic.rs`](../thryve_utils/src/traits/notif_logic.rs) — NotifLogic trait
