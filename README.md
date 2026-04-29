# Thryve Task Manager

A high-performance, extensible task management and execution framework written in Rust. Thryve provides a robust architecture for managing concurrent task execution, event-driven notifications, and modular plugin-based extensions.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Core Concepts](#core-concepts)
- [Modules](#modules)
- [Configuration](#configuration)
- [Usage](#usage)
- [Building](#building)
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

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SERVICE LAYER                                  │
│ ┌────────────────────────────────────────────────────────────────────────┐  │
│ │                         Service / RunInteractive                       │  │
│ │  (Entry point: receives external commands, manages engine lifecycle)   │  │
│ └───────────────────────────────────┬────────────────────────────────────┘  │
│                                     │                                       │
│                                     ▼                                       │
└─────────────────────────────────────┼───────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                            CONTEXT (Ctx)                                     │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │  Configuration │ Task Receiver │ Notif Receiver │ Packet Handle Factory│  │
│  └────────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                        ENGINE (Core)                                         │
│                                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                            Engine Main Loop (run_once)                                 │  │
│  │                                                                                        │  │
│  │    ┌──────────────┐                ┌──────────────┐               ┌──────────────┐     │  │
│  │    │ check_tasks  │                │ check_notifs │               │check_results │     │  │
│  │    └──────┬───────┘                └──────┬───────┘               └──────┬───────┘     │  │
│  │           │                               │                              │             │  │
│  │           ▼                               ▼                              ▼             │  │
│  └───────────┼───────────────────────────────┼──────────────────────────────┼─────────────┘  │
│              │                               │                              │                │
│              ▼                               ▼                              ▼                │
│  ┌─────────────────────┐  ┌─────────────────────────────────────┐ ┌─────────────────────┐    │
│  │   PluginManager     │  │           ObserverManager           | │  handle_result()    │    │
│  │                     │  │                                     | │  (chaining,         │    │
│  │  ┌───────────────┐  │  │  ┌───────────────────────────────┐  | │   logging,          │    │
│  │  │  ThreadPool   │  │  │  │          ThreadPool           |  | │   shutdown)         │    │
│  │  │               │  │  │  │                               |  | └─────────────────────┘    │
│  │  │  ┌─────────┐  │  │  │  │          ┌─────────┐          |  |                            │
│  │  │  │ PQChild │  │  │  │  │          │ QChild  │          |  |                            │
│  │  │  │ (Task   │  │  │  │  │          │ (Notif  │          |  |                            │
│  │  │  │  Queue) │  │  │  │  │          │  Queue) │          |  |                            │
│  │  │  └────┬────┘  │  │  │  │          └────┬────┘          |  |                            │
│  │  │       │       │  │  │  │               │               |  |                            │
│  │  │       ▼       │  │  │  │               ▼               |  |                            │
│  │  │  Worker       │  │  │  │          ┌────────┐           |  |                            │
│  │  │  Threads      │  │  │  │          │Sorter  │           |  |                            │
│  │  │  (execute     │  │  │  │          │Thread  │           |  |                            │
│  │  │   tasks)      │  │  │  │          └───┬────┘           |  |                            │
│  │  │               │  │  │  │              │                |  |                            │
│  │  └───────────────┘  │  │  │              ▼                |  |                            │
│  │                     │  │  │  ┌─────────────────────────┐  |  |                            │
│  │  ┌─────────────────┐│  │  │  │   Observer Threads      │  |  |                            │
│  │  │ Plugins Map     ││  │  │  │  ┌──────────┬──────────┐│  |  |                            │
│  │  │ (type → Plugin) ││  │  │  │  │ Logger   │  Other   ││  |  |                            │
│  │  └─────────────────┘│  │  │  │  │ Observer │ Observers││  |  |                            │
│  │                     │  │  │  │  └──────────┴──────────┘│  |  |                            │
│  │                     │  │  │  └─────────────────────────┘  |  |                            │
│  │                     │  │  │                               |  |                            │
│  └─────────────────────┘  │  └───────────────────────────────┘  |                            │
│                           │                                     |                            │
│                           └─────────────────────────────────────┘                            │
│                                                                                              │
└──────────────────────────────────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **Tasks** enter through the task channel and are queued by priority
2. **Worker threads** execute tasks from the priority queue
3. **Results** are sent back to the engine for processing
4. **Notifications** flow through the observer system for logging and monitoring

## Core Concepts

### Tasks

Tasks are units of work submitted to the engine for execution:

```rust
use thryve_utils::packets::task::Task;
use thryve_utils::res::Result;

let task = Task::new("MyTask", 2, Box::new(|| {
    // Task execution logic
    Result::new("Success", "Task completed".to_string(), "Main")
}));
```

**Task Properties:**
- **Unique ID**: Automatically assigned for tracking
- **Name**: Human-readable identifier
- **Priority**: Higher values = more urgent (0-14)
- **Execution Function**: Closure returning a `Result`

### Results

Results represent task execution outcomes and enable task chaining:

```rust
use thryve_utils::res::Result;
use thryve_utils::packets::task::Task;

// Terminal result
let result = Result::new("Success", "Done".to_string(), "Main");

// Chained result (executes next task)
let next = Some(Task::new("NextTask", 1, Box::new(|| {
    Result::new("Ok", "".to_string(), "Main")
})));
let chained = Result::new_tasked_res("Chained", "Moving on".to_string(), "Main", next);
```

### Observers

Observers are long-running components that process notifications:

```rust
use thryve_observers::logger::LoggerObserver;
use thryve_utils::cfg::ctx::Ctx;

let logger = LoggerObserver::new(&ctx);
```

### Plugins

Plugins extend functionality without modifying the core:

```rust
use thryve_utils::bases::plugin::Plugin;
use thryve_utils::traits::plugin_logic::{PlgLogic, PluginB};

// Implement PlgLogic for your plugin
struct MyPlugin;

impl PlgLogic for MyPlugin {
    fn clean_up(&self) {
        // Cleanup logic
    }
    // impl_as_any macro required
}
```

## Modules

### thryve_core

The central runtime engine coordinating all components.

| Component | Description |
|-----------|-------------|
| `Engine` | Main execution loop, coordinates plugin and observer managers |
| `PluginManager` | Manages plugins, uses priority queue for task scheduling |
| `ObserverManager` | Manages observers, routes notifications via sorter thread |

### thryve_utils

Core utilities, data structures, and traits.

| Component | Description |
|-----------|-------------|
| `Task` | Unit of work with priority and unique ID |
| `Notif` | Notification structure for observer communication |
| `Log` | Logging notification implementation |
| `Result` | Task execution outcome with chaining capability |
| `ThreadPool` | Generic thread pool for tasks and notifications |
| `PQChild` | Priority queue with aging mechanism |
| `QChild` | Segmented queue for notifications |
| `Plugin` | Plugin structure |
| `Observer` | Observer structure |
| `Config` | Configuration structures |
| `Ctx` | Application context |

### thryve_observers

Built-in observer implementations.

| Component | Description |
|-----------|-------------|
| `LoggerObserver` | File-based logging with timestamped output |

### thryve_run

Service initialization and execution.

| Component | Description |
|-----------|-------------|
| `Service` | Windows service wrapper |
| `RunInteractive` | Interactive mode execution |

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

## Usage

### Creating a Simple Task

```rust
use thryve_utils::packets::task::Task;
use thryve_utils::res::Result;
use thryve_utils::packets::packet_handle::PacketHandle;

fn main() {
    // Get packet handle from context
    let handle: PacketHandle = /* ... */;
    
    // Create and send task
    let task = Task::new("CalculateSum", 1, Box::new(|| {
        let result = 2 + 2;
        Result::new("Success", format!("Result: {}", result), "Main")
    }));
    
    handle.send_task(task);
}
```

### Creating a Plugin

```rust
use std::sync::Arc;
use thryve_utils::bases::plugin::Plugin;
use thryve_utils::traits::plugin_logic::{PlgLogic, PluginB};
use thryve_utils::impl_as_any;
use thryve_utils::packets::packet_handle::PacketHandle;

struct MyPlugin {
    handle: PacketHandle,
}

impl PlgLogic for MyPlugin {
    fn clean_up(&self) {
        println!("Cleaning up plugin");
    }
    
    impl_as_any!(MyPlugin);
}

fn create_plugin(handle: PacketHandle) -> Plugin {
    let core = Arc::new(MyPlugin { handle: handle.clone() });
    
    let init_fn = Box::new(move || {
        // Initialization task
        thryve_utils::res::Result::new("Init", "Plugin initialized".to_string(), "Main")
    });
    
    Plugin::new("MyPlugin", "MyPluginType", core, handle, init_fn)
}
```

### Creating an Observer

```rust
use std::sync::Arc;
use thryve_utils::bases::observer::Observer;
use thryve_utils::traits::observer_logic::{ObsLogic, ObsB};
use thryve_utils::impl_as_any;
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::bases::notif::Notif;

struct MyObserver {
    handle: PacketHandle,
}

impl ObsLogic for MyObserver {
    fn clean_up(&self, observer: Arc<Observer>) {
        println!("Cleaning up observer");
    }
    
    impl_as_any!(MyObserver);
}

fn create_observer(handle: PacketHandle) -> Observer {
    let core = Arc::new(MyObserver { handle: handle.clone() });
    
    let run_fn = Box::new(move |observer: Arc<Observer>| {
        // Observer main loop
        loop {
            if observer.exit_flag.load(std::sync::atomic::Ordering::Relaxed) {
                break;
            }
            
            let notif = observer.obs_queue.pop();
            // Process notification
        }
    });
    
    Observer::new("MyObserver", "MyObserverType", core, run_fn, handle)
}
```

## Building

### Prerequisites

- Rust 1.75+ 
- Cargo

### Build Commands

```bash
# Build in release mode
cargo build --release

# Run tests
cargo test

# Build documentation
cargo doc --open
```

### Dependencies

Key dependencies:

- `crossbeam` - Lock-free concurrent data structures
- `chrono` - Date/time formatting
- `serde` - Configuration deserialization
- `toml` - TOML parsing
- `crossbeam-skiplist` - Priority queue implementation

## Priority Queue Details

The priority queue uses an aging mechanism to prevent starvation:

```
effective_priority = min(base_priority + aging_boost, max_priority)
```

Where `aging_boost` increases based on wait time. Tasks inserted with a priority of more than 80% of max priority are treated as "realtime" and prioritized over newer tasks.

## Log Output Format

```
Log_Creation: 2024-01-15 10:30:45, [ LOGGING ], Event_Type: Log, Message: Hello
```

Logs are organized by module:
```
log/
  2024_01_15_10_30/
    Main.log
    Error.log
    <Module>.log
```

## License

[License information]

## Contributing

[Contributing guidelines]
