# Thryve

A high-performance, extensible task management and execution framework written in Rust. Thryve provides a robust architecture for managing concurrent task execution, event-driven notifications, and modular plugin-based extensions.

This is the main entry point crate for using Thryve from Rust — it re-exports the pieces of the framework you need under a single dependency. If you're integrating from C, C++, or another native language instead, see [`thryve_ffi`](https://crates.io/crates/thryve_ffi).

## Installation

```bash
cargo add thryve
```

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Core Concepts](#core-concepts)
- [Public API](#public-api)
- [Configuration](#configuration)
- [Usage](#usage)
- [License](#license)

## Overview

Thryve is designed around three main pillars:

1. **Task Execution**: Submit tasks with priorities to a thread pool for concurrent execution
2. **Observer Pattern**: Event-driven notification system for decoupled component communication
3. **Plugin System**: Extensible architecture allowing dynamic functionality addition

### Key Features

- **Priority-based scheduling** with aging mechanism to prevent task starvation
- **Graceful shutdown** with pause/resume capabilities
- **Thread-safe** concurrent data structures using `crossbeam`
- **Configurable** via TOML with sensible defaults
- **Extensible** through plugins and observers
- **Production-ready** logging with timestamped, categorized output
- **Graph visualization** for task topology analysis (debug mode)
- **Runs interactively or as a Windows service**

## Architecture

![Architecture](../../md_images/Architecture.png)

### Data Flow

1. **Tasks** enter through the task channel and are queued by priority
2. **Worker threads** execute tasks from the priority queue
3. **Results** are sent back to the engine for processing
4. **Notifications** flow through the observer system for logging and monitoring

## Core Concepts

### Tasks

Tasks are units of work submitted to the engine for execution:

```rust
use thryve::utils::packets::task::Task;
use thryve::utils::jobs::TaskJob;
use thryve::utils::res::TaskResult;

let task = Task::new("MyTask", 2, TaskJob::new(|| {
    // Task execution logic
    TaskResult::new("Success", "Done".to_string(), "Main")
}));
```

**Task Properties:**
- **Unique ID**: Automatically assigned for tracking
- **Name**: Human-readable identifier
- **Priority**: Higher values = more urgent (0-14)
- **Execution Function**: Closure returning a `TaskResult`, wrapped in a `TaskJob`

### Results

Results represent task execution outcomes and enable task chaining:

```rust
use thryve::utils::res::TaskResult;
use thryve::utils::packets::task::Task;
use thryve::utils::jobs::TaskJob;

// Terminal result
let result = TaskResult::new("Success", "Done".to_string(), "Main");

// Chained result — executes the next task once this one completes
let next_task = Task::new("NextTask", 1, TaskJob::new(|| {
    TaskResult::new("Ok", "".to_string(), "Main")
}));
let chained = TaskResult::new("Chained", "Moving on".to_string(), "Main").chain(next_task);
```

### Observers

Observers are long-running components that process notifications, following the same self-constructing pattern as plugins:

```rust
use std::sync::Arc;
use thryve::utils::bases::observer::Observer;
use thryve::utils::traits::{ObsLogic, Queue};
use thryve::utils::jobs::ObsJob;
use thryve::utils::packets::packet_handle::PacketHandle;
use thryve::utils::impl_as_any;

struct MyObserver;

impl MyObserver {
    pub fn new(tx: PacketHandle, obs_name: &str) -> Observer {
        let core = Arc::new(Self);

        let run_fn = ObsJob::new(move |observer: Arc<Observer>| {
            loop {
                if observer.exit_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }

                // `pop` blocks until a notification arrives or the queue is
                // finalized, in which case it wakes with a sentinel value —
                // recheck before treating it as real work.
                let _notif = observer.obs_queue.pop();
                if observer.exit_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    return;
                }

                // Handle the notification.
            }
        });

        Observer::new(obs_name, "MyObserverType", core, run_fn, tx)
    }
}

impl ObsLogic for MyObserver {
    fn clean_up(&self, _observer: Arc<Observer>) {
        // Cleanup logic here
    }

    impl_as_any!(MyObserver);
}
```

### Plugins

Plugins extend functionality without modifying the core. The idiomatic pattern is to give your logic type its own `new` that builds and returns the whole `Plugin`:

```rust
use std::sync::Arc;
use thryve::utils::bases::plugin::Plugin;
use thryve::utils::traits::PlgLogic;
use thryve::utils::jobs::TaskJob;
use thryve::utils::packets::packet_handle::PacketHandle;
use thryve::utils::res::TaskResult;
use thryve::utils::impl_as_any;

struct MyPlugin;

impl MyPlugin {
    pub fn new(tx: PacketHandle, plg_name: &str) -> Plugin {
        let core = Arc::new(Self);
        let core_clone = Arc::clone(&core);
        let tx_clone = tx.clone();

        // The task job calls back into the plugin's own logic, so it has
        // access to whatever state the plugin needs.
        let entry_point = TaskJob::new(move || core_clone.task_a(tx_clone));

        Plugin::new(plg_name, "MyPluginType", core, tx, entry_point)
    }

    fn task_a(&self, _tx: PacketHandle) -> TaskResult {
        TaskResult::new("Success", "Task A completed".to_string(), "Main")
    }
}

impl PlgLogic for MyPlugin {
    fn clean_up(&self) {
        // Cleanup logic here
    }

    impl_as_any!(MyPlugin);
}
```

## Public API

Everything usable from Rust is re-exported under `thryve::`:

| Path | Description |
|------|-------------|
| `thryve::run::run_interactive` | Interactive mode: `summon_requirements`, `run_interactive` |
| `thryve::run::run_service` | Windows service mode: `create_service`, `try_run_as_service` |
| `thryve::utils::cfg::Config` / `thryve::utils::cfg::ctx::Ctx` | Configuration and application context |
| `thryve::utils::packets::task::Task` | Unit of work with priority and unique ID |
| `thryve::utils::packets::packet_handle::PacketHandle` | Handle for sending tasks/notifications to the engine |
| `thryve::utils::res::TaskResult` | Task execution outcome, with chaining via `.chain(task)` |
| `thryve::utils::bases::plugin::Plugin` / `thryve::utils::traits::PlgLogic` | Plugin structure and the trait your plugin logic implements |
| `thryve::utils::bases::observer::Observer` / `thryve::utils::traits::ObsLogic` | Observer structure and the trait your observer logic implements |
| `thryve::utils::bases::notif::Notif` / `thryve::utils::traits::NotifLogic` | Notification structure and its logic trait |
| `thryve::utils::jobs::{TaskJob, ObsJob}` | Closure wrappers expected by `Task::new`, `Plugin::new`, `Observer::new` |
| `thryve::utils::NamedInput` | Alphanumeric-validated string wrapper used for names |
| `thryve::utils::impl_as_any!` | Macro that implements the `as_any` downcasting method required by `NotifLogic`/`ObsLogic` |
The engine loop itself and the built-in logger/topology observers are implementation details of internal crates (`thryve_core`, `thryve_observers`) and aren't part of this public surface.

## Configuration

Configuration is loaded from a TOML file or uses defaults.

### Default Configuration

```toml
[service]
can_stop = true
can_pause = true

[engine]
sleep_tick_rate = 4

[threads]
worker_count = 4

[pipes]
task_pipe_capacity = 1024
notif_pipe_capacity = 1024
res_pipe_capacity = 1024

[tasks]
max_priority = 4
aging_interval = 3

[debugging]
is_debugging = false
wait_before_execution = 10
visual_graph= false
calculate_metrics= false
log_tasks= false
```

### Configuration Options

| Section | Option | Type | Default | Description |
|---------|--------|------|---------|-------------|
| `service` | `can_stop` | bool | true | Accept stop commands |
| `service` | `can_pause` | bool | true | Accept pause/resume commands |
| `engine` | `sleep_tick_rate` | u64 | 4 | Idle sleep duration (ms) |
| `threads` | `worker_count` | usize | 4 | Number of worker threads |
| `pipes` | `task_pipe_capacity` | usize | 1024 | Task channel buffer size |
| `pipes` | `notif_pipe_capacity` | usize | 1024 | Notification channel buffer size |
| `pipes` | `res_pipe_capacity` | usize | 1024 | Result channel buffer size |
| `tasks` | `max_priority` | u8 | 4 | Maximum task priority (0-14) |
| `tasks` | `aging_interval` | u64 | 3 | Seconds before priority boost |
| `debugging` | `is_debugging` | bool | false | Enable debug mode |
| `debugging` | `wait_before_execution` | u64 | 10 | Debug wait time (seconds) |
| `debugging` | `visual_graph` | bool | false | Enable task topology graph visualization |
| `debugging` | `calculate_metrics` | bool | false | Enable metrics calculation |
| `debugging` | `log_tasks` | bool | false | Enable task execution logging |

## Usage

### Creating and Sending a Task

```rust
use thryve::utils::packets::task::Task;
use thryve::utils::packets::packet_handle::PacketHandle;
use thryve::utils::jobs::TaskJob;
use thryve::utils::res::TaskResult;

fn send_calculation(handle: &PacketHandle) {
    let task = Task::new("CalculateSum", 1, TaskJob::new(|| {
        let result = 2 + 2;
        TaskResult::new("Success", format!("Result: {}", result), "Main")
    }));

    handle.send_task(task);
}
```

### Full Sample — Interactive Mode

These pull in the same `MyPlugin`/`MyObserver` shape shown in [Observers](#observers) and [Plugins](#plugins) above, restated here so the sample is fully self-contained. Marked `no_run` since actually executing it starts a real engine thread that runs until told to stop.

```rust,no_run
use std::sync::Arc;
use thryve::run::run_interactive;
use thryve::utils::cfg::Config;
use thryve::utils::bases::plugin::Plugin;
use thryve::utils::bases::observer::Observer;
use thryve::utils::traits::{PlgLogic, ObsLogic, Queue};
use thryve::utils::jobs::{TaskJob, ObsJob};
use thryve::utils::packets::packet_handle::PacketHandle;
use thryve::utils::res::TaskResult;
use thryve::utils::impl_as_any;

struct MyPlugin;
impl MyPlugin {
    pub fn new(tx: PacketHandle, plg_name: &str) -> Plugin {
        let core = Arc::new(Self);
        let core_clone = Arc::clone(&core);
        let tx_clone = tx.clone();
        let entry_point = TaskJob::new(move || core_clone.task_a(tx_clone));
        Plugin::new(plg_name, "MyPluginType", core, tx, entry_point)
    }
    fn task_a(&self, _tx: PacketHandle) -> TaskResult {
        TaskResult::new("Success", "Task A completed".to_string(), "Main")
    }
}
impl PlgLogic for MyPlugin {
    fn clean_up(&self) {}
    impl_as_any!(MyPlugin);
}

struct MyObserver;
impl MyObserver {
    pub fn new(tx: PacketHandle, obs_name: &str) -> Observer {
        let core = Arc::new(Self);
        let run_fn = ObsJob::new(move |observer: Arc<Observer>| {
            while !observer.exit_flag.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::yield_now();
            }
        });
        Observer::new(obs_name, "MyObserverType", core, run_fn, tx)
    }
}
impl ObsLogic for MyObserver {
    fn clean_up(&self, _observer: Arc<Observer>) {}
    impl_as_any!(MyObserver);
}

fn main() {
    let config = Config::load_def_config();
    let mut ctx = run_interactive::summon_requirements(config);

    let plugin = MyPlugin::new(ctx.packet_handle(), "MyPlugin");
    let observer = MyObserver::new(ctx.packet_handle(), "MyObserver");

    ctx.plugins.push(plugin);
    ctx.observers.push(observer);
    ctx.logging_file_names.push("TestFile".to_string());

    if let Some(handle) = run_interactive::run_interactive(ctx) {
        let _ = handle.join();
    }
}
```

### Full Sample — Windows Service Mode

```rust,no_run
use std::sync::Arc;
use thryve::run::run_service;
use thryve::utils::cfg::Config;
use thryve::utils::bases::plugin::Plugin;
use thryve::utils::bases::observer::Observer;
use thryve::utils::traits::{PlgLogic, ObsLogic, Queue};
use thryve::utils::jobs::{TaskJob, ObsJob};
use thryve::utils::packets::packet_handle::PacketHandle;
use thryve::utils::res::TaskResult;
use thryve::utils::impl_as_any;

struct MyPlugin;
impl MyPlugin {
    pub fn new(tx: PacketHandle, plg_name: &str) -> Plugin {
        let core = Arc::new(Self);
        let core_clone = Arc::clone(&core);
        let tx_clone = tx.clone();
        let entry_point = TaskJob::new(move || core_clone.task_a(tx_clone));
        Plugin::new(plg_name, "MyPluginType", core, tx, entry_point)
    }
    fn task_a(&self, _tx: PacketHandle) -> TaskResult {
        TaskResult::new("Success", "Task A completed".to_string(), "Main")
    }
}
impl PlgLogic for MyPlugin {
    fn clean_up(&self) {}
    impl_as_any!(MyPlugin);
}

struct MyObserver;
impl MyObserver {
    pub fn new(tx: PacketHandle, obs_name: &str) -> Observer {
        let core = Arc::new(Self);
        let run_fn = ObsJob::new(move |observer: Arc<Observer>| {
            while !observer.exit_flag.load(std::sync::atomic::Ordering::Relaxed) {
                std::thread::yield_now();
            }
        });
        Observer::new(obs_name, "MyObserverType", core, run_fn, tx)
    }
}
impl ObsLogic for MyObserver {
    fn clean_up(&self, _observer: Arc<Observer>) {}
    impl_as_any!(MyObserver);
}

fn main() {
    let config = Config::load_def_config();
    let (svc, mut ctx) = run_service::create_service(config);

    let plugin = MyPlugin::new(ctx.packet_handle(), "MyPlugin");
    let observer = MyObserver::new(ctx.packet_handle(), "MyObserver");

    ctx.plugins.push(plugin);
    ctx.observers.push(observer);
    ctx.logging_file_names.push("TestFile".to_string());

    run_service::try_run_as_service(svc, ctx);
}
```

## License

Thryve is dual-licensed under two models:

### Open-Source License (AGPL-3.0)

For open-source projects and personal use, Thryve is available under the **GNU Affero General Public License v3.0**. If used over a network (SaaS), you must make your source code available to users.

### Commercial License

For closed-source commercial projects that cannot comply with AGPL-3.0's disclosure requirements, a commercial license is available. Contact [thryve-framework.com](https://thryve-framework.com) for details.

See the full project repository for the [`LICENSE`](../../LICENSE) text, [`COMMERCIAL.md`](../../COMMERCIAL.md), and contribution guidelines.
