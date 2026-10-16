// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Task packet implementation for the Thryve engine.
//!
//! This module provides the [`Task`] struct, which represents a unit of
//! work that can be submitted to the engine for execution. Tasks are
//! the primary mechanism for triggering plugin operations and are
//! processed by worker threads according to their priority.
//!
//! # Task Lifecycle
//!
//! 1. A task is created with a name, priority, and execution function
//! 2. The task is sent to the engine via [`PacketHandle`]
//! 3. The engine queues the task in a priority queue
//! 4. A worker thread pops and executes the task
//! 5. The task returns a [`TaskResult`] indicating completion
//!
//! # Typed Tasks
//!
//! An [`AdvTask<T, E>`](AdvTask) is a task whose closure returns `Result<T, E>`: a value, or the
//! task's own error type. Sending it returns an [`AdvTaskHandle`], and
//! [`then`](AdvTaskHandle::then) on it declares a follow-up, a task of its own that gets the
//! result as a [`Handoff`]. A task that creates its last step while it runs can forward to it
//! with [`AdvTask::forward`] or [`then_forward`](AdvTaskHandle::then_forward), and whoever follows
//! it gets that step's result. Several tasks of one type can share one follow-up: the
//! [`Joinable`](crate::traits::join::Joinable) trait lets a `Vec` of their handles
//! [`join`](crate::traits::join::Joinable::join), and the follow-up runs once all of them have
//! finished, with all their values, or as soon as one fails.
//!
//! # Unique Identification
//!
//! Each task is assigned a unique ID upon creation using an atomic
//! counter. This ID can be used to track task execution and correlate
//! results with their originating tasks.

use std::marker::PhantomData;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::errors::{
    JoinError, NameInputType, NamingError, TaskError, ThryveError, get_err_oneline,
};
use crate::extras::jobs::Job;
use crate::extras::{jobs::TaskJob, named_input::NamedInput};
use crate::graphs::callsite::TaskCallSite;
use crate::packets::packet_handle::PacketHandle;
use crate::queues::priority_queue::inherited_priority_cap;
use crate::res::{Handoff, JoinHandoff, SealedHandoff, TaskResult};
use crate::traits::packets::Packet;

/// Atomic counter for generating unique task IDs.
///
/// This global static variable is safe to use across threads.
/// It starts at 1 and increments by 1 for each new task.
static COUNTER: AtomicU64 = AtomicU64::new(1);

/// A unit of work for the Thryve engine.
///
/// The `Task` struct represents a packet that can be sent to the engine
/// for execution. Each task has a unique ID, a name, a priority level,
/// and a function to execute. Tasks are processed by worker threads
/// according to their priority and aging.
///
/// # Graph Visualization
///
/// When graph visualization is enabled, tasks can carry call site
/// information via the [`callsite`](Task::callsite) field. This information
/// is automatically captured by [`PacketHandle::send_task`](crate::packets::packet_handle::PacketHandle::send_task)
/// and includes:
///
/// - The file path where the task was sent
/// - The line number where it was sent
/// - Fingerprints for graph node identification
///
/// # Fields
///
/// - `id`: Unique identifier assigned upon creation
/// - `callsite`: Optional call site information for graph visualization
/// - `task_name`: Human-readable name for debugging and logging
/// - `base_priority`: Priority level (higher = more urgent)
/// - `job`: The execution function (private)
///
/// # Example
///
/// ```no_run
/// use thryve_utils::packets::task::Task;
/// use thryve_utils::res::TaskResult;
///
/// let task = Task::new("MyTask", 2, || {
///     // Task execution logic
///     TaskResult::new("Success", "Done".to_string(), "Main")
/// }).expect("valid task name");
///
/// println!("Task ID: {}", task.id);
/// ```
pub struct Task {
    /// Unique identifier assigned upon creation.
    pub id: u64,

    /// Optional call site information for graph visualization.
    ///
    /// This field is populated when graph visualization is enabled
    /// and contains information about where this task was created
    /// in the source code.
    pub callsite: Option<TaskCallSite>,

    /// Human-readable name for debugging and logging.
    task_name: NamedInput,
    /// Priority level (higher = more urgent).
    pub base_priority: u8,
    /// The execution function (private).
    job: Job,
}

impl Packet for Task {
    fn get_task_name(&self) -> Option<String> {
        Some(self.task_name.inner().to_string())
    }

    /// Executes the task and returns the result.
    ///
    /// This method implements the [`Packet`] trait for tasks, providing
    /// the execution logic that worker threads invoke. The task's job
    /// function is called and wrapped in a result tuple containing the
    /// task ID for tracking.
    ///
    /// # Returns
    ///
    /// Always `Some((task_id, result))`. The `Option` comes from the [`Packet`] trait, whose
    /// default (for notifications) returns `None`.
    fn process(self) -> Option<(u64, TaskResult)> {
        let id = self.id;
        match self.job {
            Job::TaskReady(ready_task) => Some((id, ready_task.call_once())),
            _ => Some((
                id,
                TaskError::WaitWithNoInput {
                    task_name: String::from(self.task_name.inner()),
                }
                .into(),
            )),
        }
    }

    /// Sets the call site information for this task.
    ///
    /// This method is called by [`PacketHandle::send_task`](crate::packets::packet_handle::PacketHandle::send_task)
    /// when graph visualization is enabled. It attaches call site
    /// information (file path, line number, fingerprints)
    /// to the task for topology graph construction.
    ///
    /// # Arguments
    ///
    /// * `callsite` - The call site information to attach to this task
    fn set_callsite(&mut self, callsite: TaskCallSite) {
        self.callsite = Some(callsite);
    }

    /// Returns the call site information for this task.
    ///
    /// This method provides access to the call site information that
    /// was attached by [`PacketHandle::send_task`](crate::packets::packet_handle::PacketHandle::send_task)
    /// when graph visualization is enabled.
    ///
    /// # Returns
    ///
    /// The call site information if available, or a default empty
    /// `TaskCallSite` if no call site was captured.
    fn get_callsite(&self) -> TaskCallSite {
        self.callsite.clone().unwrap_or_default()
    }
}

impl Task {
    /// Creates a new task with the specified configuration.
    ///
    /// Initializes a new task with a unique ID, the given name, priority,
    /// and execution function. The task is ready to be submitted to the
    /// engine via a [`PacketHandle`].
    ///
    /// # Arguments
    ///
    /// * `task_name` - Human-readable name for the task
    /// * `task_priority` - Priority level (higher = more urgent)
    /// * `function_to_exec` - The function to execute when the task runs
    ///
    /// # Returns
    ///
    /// A new `Task` instance with a unique ID.
    ///
    /// # Errors
    ///
    /// Returns [`NamingError::NameEmpty`] if `task_name` is empty, or
    /// [`NamingError::NameContainsControlChar`] if it contains a control character. The
    /// error's kind is `NameInputType::TaskName`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::task::Task;
    /// use thryve_utils::res::TaskResult;
    ///
    /// let task = Task::new("CalculateSum", 1, || {
    ///     let result = 2 + 2;
    ///     TaskResult::new("Success", format!("Result: {}", result), "Main")
    /// }).expect("valid task name");
    /// ```
    pub fn new(
        task_name: &str,
        task_priority: u8,
        fn_to_exec: impl FnOnce() -> TaskResult + Send + 'static,
    ) -> Result<Self, NamingError> {
        Ok(Self {
            id: next_id(),
            callsite: None,
            task_name: NamedInput::new(task_name, NameInputType::TaskName)?,
            base_priority: task_priority,
            job: Job::TaskReady(TaskJob::new(fn_to_exec)),
        })
    }

    /// Creates a follow-up that waits for the task with id `wait_on_id`: the engine holds it until
    /// that task's handoff arrives, then hands the handoff to `fn_to_exec` (see
    /// [`with_input`](Self::with_input)).
    ///
    /// `inherits_priority` says whether `task_priority` was inherited from that task, in which
    /// case it's capped below realtime when the follow-up becomes ready.
    fn new_wait(
        task_name: &str,
        task_priority: u8,
        wait_on_id: u64,
        inherits_priority: bool,
        fn_to_exec: impl FnOnce(SealedHandoff) -> TaskResult + Send + 'static,
    ) -> Result<Self, NamingError> {
        Ok(Self {
            id: next_id(),
            callsite: None,
            task_name: NamedInput::new(task_name, NameInputType::TaskName)?,
            base_priority: task_priority,
            job: Job::TaskWait {
                on: wait_on_id,
                inherits_priority,
                input_job: Box::new(fn_to_exec),
            },
        })
    }

    /// Creates a join that waits for the tasks whose ids are in `join_values`: it starts with one
    /// empty slot per id, in that order, and the engine fills them (see
    /// [`join_insert`](Self::join_insert)) and then makes it ready (see
    /// [`with_join_inputs`](Self::with_join_inputs)), which hands the slots to `fn_to_exec`.
    ///
    /// `inherits_priority` says whether `task_priority` was inherited from those tasks, in which
    /// case it's capped below realtime when the join becomes ready.
    fn new_join_wait(
        task_name: &str,
        task_priority: u8,
        inherits_priority: bool,
        join_values: Vec<u64>,
        fn_to_exec: impl FnOnce(Vec<Option<SealedHandoff>>) -> TaskResult + Send + 'static,
    ) -> Result<Self, NamingError> {
        Ok(Self {
            id: next_id(),
            callsite: None,
            task_name: NamedInput::new(task_name, NameInputType::TaskName)?,
            base_priority: task_priority,
            job: Job::JoinWait {
                slots: join_values.iter().map(|id| (*id, None)).collect(),
                inherits_priority,
                remaining_inputs: join_values.len(),
                input_job: Box::new(fn_to_exec),
            },
        })
    }

    /// Creates a plugin's init task, named after the plugin's type.
    ///
    /// The plugin manager sends it when it loads the plugin. The type was checked when the plugin
    /// was created, so this can't fail.
    pub fn new_init(
        plg_type: NamedInput,
        task_priority: u8,
        fn_to_exec: impl FnOnce() -> TaskResult + Send + 'static,
    ) -> Self {
        Self {
            id: next_id(),
            callsite: None,
            task_name: plg_type,
            base_priority: task_priority,
            job: Job::TaskReady(TaskJob::new(fn_to_exec)),
        }
    }

    /// Creates a placeholder task, named `Empty`, whose job does nothing and reports nothing.
    pub fn new_empty() -> Self {
        Self {
            id: next_id(),
            callsite: None,
            task_name: NamedInput::new_empty(),
            base_priority: 0,
            job: Job::TaskReady(TaskJob::new_empty()),
        }
    }

    /// Creates the default task, named `DefTask`.
    ///
    /// Its job is the placeholder that reports an error if it ever runs.
    pub fn new_default() -> Self {
        Self {
            id: next_id(),
            callsite: None,
            task_name: NamedInput::new_def_task(),
            base_priority: 0,
            job: Job::TaskReady(TaskJob::default()),
        }
    }

    /// Makes a waiting task ready: puts the sealed handoff of the task it waited on into its job.
    ///
    /// If its priority was inherited (it was declared with [`then`](AdvTaskHandle::then)), it's
    /// lowered to just below realtime when it's realtime or higher, so a chain never runs as
    /// realtime by inheritance. A priority given with
    /// [`then_with_priority`](AdvTaskHandle::then_with_priority) is kept as it is. A task that
    /// isn't waiting comes back unchanged.
    ///
    /// For Thryve's engine. Not part of the public API.
    ///
    /// # Arguments
    ///
    /// * `sealed` - The handoff of the task it waited on
    /// * `max_priority` - The configured `tasks.max_priority`, which sets where realtime starts
    #[doc(hidden)]
    pub fn with_input(mut self, sealed: SealedHandoff, max_priority: u8) -> Self {
        if let Job::TaskWait {
            inherits_priority,
            input_job,
            ..
        } = self.job
        {
            if inherits_priority && let Some(cap) = inherited_priority_cap(max_priority) {
                self.base_priority = self.base_priority.min(cap);
            }
            self.job = Job::TaskReady(TaskJob::new(move || input_job(sealed)));
        }
        self
    }

    /// Puts the handoff of one of a join's tasks into its slot, at `position`, and counts it as
    /// arrived.
    ///
    /// The engine knows which task the handoff came from, so it finds the slot by position and
    /// never searches the slots by id.
    ///
    /// For Thryve's engine. Not part of the public API.
    ///
    /// # Errors
    ///
    /// Gives the handoff back, unplaced and uncounted, if this task isn't a join, `position` is
    /// past its last slot, or that slot is already full. Only a bug in Thryve can cause any of
    /// them; the engine then treats the handoff like any result nobody claimed.
    #[doc(hidden)]
    pub fn join_insert(
        &mut self,
        sealed: SealedHandoff,
        position: usize,
    ) -> Result<(), SealedHandoff> {
        if let Job::JoinWait {
            slots,
            remaining_inputs,
            ..
        } = &mut self.job
            && let Some((_, sealed_handoff_opt)) = slots.get_mut(position)
            && sealed_handoff_opt.is_none()
        {
            slots[position].1 = Some(sealed);
            *remaining_inputs -= 1;
            return Ok(());
        }

        Err(sealed)
    }

    /// Makes a join ready: hands its slots' handoffs, in order, to its job, which then runs like any
    /// ready task's.
    ///
    /// The engine calls it once every slot is full, or as soon as one of the join's tasks fails (a
    /// join fails fast), so some slots may still be empty; the job tells the two cases apart.
    ///
    /// If its priority was inherited (it was declared with
    /// [`join`](crate::traits::join::Joinable::join)), it's lowered to just below realtime when it's
    /// realtime or higher, as a follow-up's is in [`with_input`](Self::with_input). A priority given
    /// with [`join_with_priority`](crate::traits::join::Joinable::join_with_priority) is kept as it
    /// is. A task that isn't a join comes back unchanged.
    ///
    /// For Thryve's engine. Not part of the public API.
    ///
    /// # Arguments
    ///
    /// * `max_priority` - The configured `tasks.max_priority`, which sets where realtime starts
    #[doc(hidden)]
    pub fn with_join_inputs(mut self, max_priority: u8) -> Self {
        if let Job::JoinWait {
            mut slots,
            inherits_priority,
            input_job,
            ..
        } = self.job
        {
            let inputs: Vec<Option<SealedHandoff>> = slots
                .drain(..)
                .map(|(_, sealed_handoff_opt)| sealed_handoff_opt)
                .collect();

            if inherits_priority && let Some(cap) = inherited_priority_cap(max_priority) {
                self.base_priority = self.base_priority.min(cap);
            }

            self.job = Job::TaskReady(TaskJob::new(move || input_job(inputs)));
        }

        self
    }

    /// Returns the size of this task in bytes.
    ///
    /// This method returns the memory footprint of the task struct
    /// itself, as determined by `std::mem::size_of_val`. This is
    /// useful for metrics tracking and memory profiling.
    ///
    /// # Returns
    ///
    /// The size of the task in bytes.
    ///
    /// # Note
    ///
    /// This returns the size of the struct itself, not the heap
    /// memory allocated by its fields (such as the `NamedInput` or
    /// the boxed closure the task runs).
    pub fn size_in_bytes(&self) -> usize {
        std::mem::size_of_val(self)
    }

    /// Returns the task's name.
    pub fn task_name(&self) -> &str {
        self.task_name.inner()
    }

    /// The id of the task this one waits on, or `None` for a task that can run now.
    ///
    /// For Thryve's engine, which holds a waiting task instead of queueing it. Not part of the
    /// public API.
    #[doc(hidden)]
    pub fn waiting_on_id(&self) -> Option<u64> {
        match self.job {
            Job::TaskWait { on, .. } => Some(on),
            _ => None,
        }
    }

    /// How many of a join's slots are still empty, or `None` for a task that isn't a join.
    ///
    /// For Thryve's engine, which holds a join until it's ready instead of queueing it. Not part of
    /// the public API.
    #[doc(hidden)]
    pub fn waiting_on_join(&self) -> Option<usize> {
        match self.job {
            Job::JoinWait {
                remaining_inputs, ..
            } => Some(remaining_inputs),
            _ => None,
        }
    }

    /// The ids of the tasks a join waits for, in the order their handles were given, or `None` for
    /// a task that isn't a join.
    ///
    /// It's a copy, so the engine can fill the join's slots while it goes through the ids. For
    /// Thryve's engine. Not part of the public API.
    #[doc(hidden)]
    pub fn join_get_ids(&self) -> Option<Vec<u64>> {
        if let Job::JoinWait { slots, .. } = &self.job {
            return Some(slots.iter().map(|(id, _)| *id).collect());
        }

        None
    }
}

impl Task {
    /// Ensures the task's priority does not exceed the maximum allowed.
    ///
    /// If the task's priority is higher than the configured maximum,
    /// it is clamped down to the maximum. This prevents invalid priority
    /// levels from causing issues in the priority queue.
    ///
    /// # Arguments
    ///
    /// * `max_task_priority` - The maximum allowed priority level
    ///
    /// # Example
    ///
    /// ```no_run
    /// use thryve_utils::packets::task::Task;
    /// use thryve_utils::res::TaskResult;
    ///
    /// let mut task = Task::new("HighPriority", 10, || {
    ///     TaskResult::new("", "".to_string(), "")
    /// }).expect("valid task name");
    ///
    /// task.clamp_task_priority(4); // Priority clamped to 4
    /// assert_eq!(task.base_priority, 4);
    /// ```
    pub fn clamp_task_priority(&mut self, max_task_priority: u8) {
        if self.base_priority > max_task_priority {
            self.base_priority = max_task_priority;
        }
    }
}

impl Default for Task {
    fn default() -> Self {
        Self::new_default()
    }
}

/// A task with a typed result: its closure returns `Result<T, E>`, a value or the task's own error.
///
/// It's sent with
/// [`PacketHandle::send_adv_task`](crate::packets::packet_handle::PacketHandle::send_adv_task)
/// and runs in the same queue, on the same workers and under the same priority and ageing as any
/// other [`Task`].
///
/// When it finishes, its result travels to the engine sealed, as a handoff: the engine carries it
/// without knowing `T` or `E`. Sending the task returns an [`AdvTaskHandle`], and
/// [`then`](AdvTaskHandle::then) on it declares a follow-up, which gets the result as a
/// [`Handoff`]. Once the handle is dropped without a follow-up, an error is written to
/// `Error.log`, with the task's name and the error's causes, and a value is dropped.
///
/// A task that creates its last step while it runs can forward to it instead:
/// [`forward`](AdvTask::forward) builds a task whose closure returns that step's handle, and
/// whoever follows the task gets the step's result.
///
/// # Type Parameters
///
/// - `T`: the value the task produces. It only has to be able to move to another thread
///   (`Send + 'static`); it doesn't have to be `Sync`.
/// - `E`: the task's own error type: any [`std::error::Error`] that is `Send + 'static`, and
///   doesn't have to be `Sync` either. A task that can't fail can use
///   [`Infallible`](std::convert::Infallible).
///
/// # Example
///
/// Here `T` is `u64` and `E` is [`std::io::Error`]:
///
/// ```no_run
/// # use crossbeam::channel::{bounded, unbounded};
/// # use thryve_utils::packets::packet_handle::PacketHandle;
/// use thryve_utils::packets::task::AdvTask;
///
/// # let (task_tx, _task_rx) = bounded(1);
/// # let (notif_tx, _notif_rx) = bounded(1);
/// # let handle = PacketHandle::new(task_tx, notif_tx, unbounded().0, false);
/// let file_size = AdvTask::new("FileSize", 2, || {
///     std::fs::metadata("photo.png").map(|meta| meta.len())
/// })?;
///
/// handle.send_adv_task(file_size)?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
pub struct AdvTask<T: Send + 'static, E: std::error::Error + Send + 'static> {
    /// The task that's queued and run: its job is the wrapper built by [`new`](AdvTask::new) or
    /// [`forward`](AdvTask::forward).
    task: Task,
    /// Ties `T` and `E` to the task. `fn() -> (T, E)` keeps the task `Send` and `Sync` whatever
    /// `T` and `E` are.
    _marker: PhantomData<fn() -> (T, E)>,
}

/// The handle of a sent [`AdvTask`]: [`then`](Self::then) on it declares the task's follow-up.
///
/// [`PacketHandle::send_adv_task`] returns it, and `then` returns one for the follow-up, so steps
/// can be chained. It doesn't hold the task's result: the result goes to the engine, which hands it
/// to the follow-up. `then` uses the handle up, so a task has at most one follow-up. Returning the
/// handle from a forwarding task (see [`AdvTask::forward`]) uses it up too: whoever follows the
/// forwarding task gets this task's result. So does joining it with others
/// ([`join`](crate::traits::join::Joinable::join)), whose follow-up waits for this task together
/// with them.
///
/// The engine keeps the task's result for as long as the handle exists, so `then` works even
/// after the task has finished. Dropping the handle without calling `then` tells the engine that
/// nobody will follow up: the result is let go, and an error is written to `Error.log`. Dropping
/// it never cancels the task, which still runs.
pub struct AdvTaskHandle<T: Send + 'static, E: std::error::Error + Send + 'static> {
    /// The task's id. The engine holds the follow-up, or the task's kept result, under it.
    task_id: u64,
    /// The task's priority, which [`then`](Self::then) passes on to the follow-up.
    base_priority: u8,
    /// Sends the follow-up, or the release, to the engine.
    tx: PacketHandle,
    /// Set once the handle is used: by `then` once the follow-up is on its way, or when a
    /// forwarding task returns the handle. Dropping it afterwards sends no release: a release would
    /// tell the engine nobody follows up, and could reach it before the follow-up or the forward.
    is_used: bool,
    /// Ties `T` and `E` to the handle, like [`AdvTask`]'s marker.
    _marker: PhantomData<fn() -> (T, E)>,
}

impl<T, E> Drop for AdvTaskHandle<T, E>
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    /// Tells the engine that nobody will follow up on the task, unless the handle was used, by
    /// `then` or by a forward.
    ///
    /// The release goes through the status mailbox, which never makes a sender wait, so a handle
    /// can be dropped anywhere, even on the engine thread.
    fn drop(&mut self) {
        if !self.is_used {
            self.tx.send_status_release(self.task_id);
        }
    }
}

impl<T: Send + 'static, E: std::error::Error + Send + 'static> AdvTask<T, E> {
    /// Creates a typed task.
    ///
    /// The closure runs later, once, on a worker. Thryve wraps it so that its result reaches the
    /// engine as a handoff. If it returns an error, the wrapper writes the error's text there and
    /// then, while the error's type is still known: the task's name, the error and its causes
    /// (see [`get_err_oneline`]).
    ///
    /// # Arguments
    ///
    /// * `task_name` - Human-readable name for the task
    /// * `task_priority` - Priority level (higher = more urgent)
    /// * `function_to_exec` - The closure to run: it returns the task's value or its own error
    ///
    /// # Errors
    ///
    /// Same as [`Task::new`]: [`NamingError::NameEmpty`] if `task_name` is empty, or
    /// [`NamingError::NameContainsControlChar`] if it contains a control character.
    pub fn new(
        task_name: &str,
        task_priority: u8,
        fn_to_exec: impl FnOnce() -> Result<T, E> + Send + 'static,
    ) -> Result<Self, NamingError> {
        let name_as_string = String::from(task_name);
        let wrapper = move || seal_result(name_as_string, fn_to_exec());
        let task = Task::new(task_name, task_priority, wrapper)?;

        Ok(Self {
            task,
            _marker: PhantomData,
        })
    }

    /// Creates a typed task that forwards: its closure creates and sends another typed task, and
    /// returns that task's handle. It builds the task; the forward happens when the task finishes.
    /// Its outcome then becomes the other task's, so whoever follows it gets that task's value or
    /// error.
    ///
    /// It's for a last step that only exists once the task runs, such as one picked by what the
    /// task found. If the steps are known up front, chain them with
    /// [`then`](AdvTaskHandle::then) instead.
    ///
    /// - An `Err` from the closure is this task's own error, as with [`new`](Self::new), and
    ///   nothing is forwarded.
    /// - The returned handle is used up: nothing else can follow up on that task, and the handle
    ///   sends no release.
    /// - No worker waits: the task finishes as soon as its closure returns, and the engine makes
    ///   whoever follows it wait for the task forwarded to.
    /// - Forwards can stack: A forwards to B, which forwards to C. A forward that leads back into
    ///   its own chain, which takes passing a task's handle into its own closure, never produces
    ///   an outcome.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Human-readable name for the task
    /// * `task_priority` - Priority level (higher = more urgent)
    /// * `fn_to_exec` - The closure to run: it returns the handle of the task to forward to, or
    ///   this task's own error
    ///
    /// # Errors
    ///
    /// The same as [`new`](Self::new).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::{bounded, unbounded};
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// use thryve_utils::packets::task::AdvTask;
    ///
    /// # let (task_tx, _task_rx) = bounded(2);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let handle = PacketHandle::new(task_tx, notif_tx, unbounded().0, false);
    /// let sender = handle.clone();
    /// // Which step comes next is only known once the photo's size is.
    /// let prepare = AdvTask::forward("Prepare", 2, move || {
    ///     let size = std::fs::metadata("photo.png")?.len();
    ///     let next = if size > 1_000_000 {
    ///         AdvTask::new("Shrink", 2, move || Ok(size / 4))
    ///     } else {
    ///         AdvTask::new("Keep", 2, move || Ok(size))
    ///     }
    ///     .map_err(std::io::Error::other)?;
    ///
    ///     sender.send_adv_task(next).map_err(std::io::Error::other)
    /// })?;
    ///
    /// // Whoever follows `Prepare` gets the value of `Shrink` or `Keep`.
    /// handle.send_adv_task(prepare)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn forward(
        task_name: &str,
        task_priority: u8,
        fn_to_exec: impl FnOnce() -> Result<AdvTaskHandle<T, E>, E> + Send + 'static,
    ) -> Result<Self, NamingError> {
        let name_as_string = String::from(task_name);
        let wrapper = move || seal_forward(name_as_string, fn_to_exec());
        let task = Task::new(task_name, task_priority, wrapper)?;

        Ok(Self {
            task,
            _marker: PhantomData,
        })
    }

    /// Hands over the task inside, to be sent.
    pub(crate) fn into_task(self) -> Task {
        self.task
    }
}

impl<T, E> AdvTaskHandle<T, E>
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    /// The handle of `task`, which is about to be sent. `tx` will send its follow-up.
    pub(crate) fn new(task: &Task, tx: PacketHandle) -> Self {
        Self {
            task_id: task.id,
            base_priority: task.base_priority,
            tx,
            is_used: false,
            _marker: PhantomData,
        }
    }

    /// Declares the task to run after this one: it gets this task's value or error.
    ///
    /// The follow-up is sent to the engine now, but it waits there until this task finishes;
    /// then its closure runs on a worker with this task's result, opened as a [`Handoff`]. It
    /// inherits this task's priority, capped below realtime, so a chain never runs as realtime
    /// by inheritance. To give it a priority of its own, use
    /// [`then_with_priority`](Self::then_with_priority).
    ///
    /// It uses up this handle (one follow-up per task) and returns the follow-up's own handle,
    /// so steps can be chained.
    ///
    /// It can be called at any time while you hold the handle. If this task has already
    /// finished, the engine kept its result for the follow-up, which then runs at once.
    ///
    /// # Errors
    ///
    /// [`ThryveError::Naming`] if `task_name` is empty or contains a control character, and
    /// [`ThryveError::EngineNotRunning`] if the engine is gone.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::{bounded, unbounded};
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// use thryve_utils::packets::task::AdvTask;
    /// use thryve_utils::res::Handoff;
    ///
    /// # let (task_tx, _task_rx) = bounded(2);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let handle = PacketHandle::new(task_tx, notif_tx, unbounded().0, false);
    /// let file_size = AdvTask::new("FileSize", 2, || {
    ///     std::fs::metadata("photo.png").map(|meta| meta.len())
    /// })?;
    ///
    /// handle
    ///     .send_adv_task(file_size)?
    ///     .then("SizeInKb", |handoff| match handoff {
    ///         Handoff::Value(bytes) => Ok(bytes / 1024),
    ///         // `FileSize`'s own error, a `std::io::Error`, passed on to whoever follows.
    ///         Handoff::Error(error) => Err(error),
    ///     })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    ///
    /// The follow-up takes exactly this task's `Handoff<T, E>`, so one that expects another type
    /// doesn't compile:
    ///
    /// ```compile_fail,E0631
    /// # use crossbeam::channel::{bounded, unbounded};
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// use thryve_utils::packets::task::AdvTask;
    /// use thryve_utils::res::Handoff;
    ///
    /// # let (task_tx, _task_rx) = bounded(2);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let handle = PacketHandle::new(task_tx, notif_tx, unbounded().0, false);
    /// let file_size = AdvTask::new("FileSize", 2, || {
    ///     std::fs::metadata("photo.png").map(|meta| meta.len())
    /// })?;
    ///
    /// // `FileSize` produces a `u64`, not text.
    /// handle
    ///     .send_adv_task(file_size)?
    ///     .then("Shout", |handoff: Handoff<String, std::io::Error>| match handoff {
    ///         Handoff::Value(text) => Ok(text.to_uppercase()),
    ///         Handoff::Error(error) => Err(error),
    ///     })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[track_caller]
    pub fn then<U, E2>(
        self,
        task_name: &str,
        fn_to_exec: impl FnOnce(Handoff<T, E>) -> Result<U, E2> + Send + 'static,
    ) -> Result<AdvTaskHandle<U, E2>, ThryveError>
    where
        U: Send + 'static,
        E2: std::error::Error + Send + 'static,
    {
        let task_priority = self.base_priority;
        self.follow_up(task_name, task_priority, true, fn_to_exec, seal_result)
    }

    /// Like [`then`](Self::then), but the follow-up runs at `task_priority` instead of inheriting
    /// this task's priority. It's kept as given, so it can be realtime.
    ///
    /// # Errors
    ///
    /// The same as [`then`](Self::then).
    #[track_caller]
    pub fn then_with_priority<U, E2>(
        self,
        task_name: &str,
        task_priority: u8,
        fn_to_exec: impl FnOnce(Handoff<T, E>) -> Result<U, E2> + Send + 'static,
    ) -> Result<AdvTaskHandle<U, E2>, ThryveError>
    where
        U: Send + 'static,
        E2: std::error::Error + Send + 'static,
    {
        self.follow_up(task_name, task_priority, false, fn_to_exec, seal_result)
    }

    /// Like [`then`](Self::then), but the follow-up forwards: its closure gets this task's result,
    /// creates and sends another typed task, and returns that task's handle. Whoever follows the
    /// follow-up then gets that task's value or error. See [`AdvTask::forward`] for how
    /// forwarding works.
    ///
    /// The follow-up inherits this task's priority, capped below realtime, like `then`; the task
    /// it forwards to keeps the priority it was created with. An `Err` from the closure is the
    /// follow-up's own error, and nothing is forwarded.
    ///
    /// # Errors
    ///
    /// The same as [`then`](Self::then).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::{bounded, unbounded};
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// use thryve_utils::packets::task::AdvTask;
    /// use thryve_utils::res::Handoff;
    ///
    /// # let (task_tx, _task_rx) = bounded(2);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let handle = PacketHandle::new(task_tx, notif_tx, unbounded().0, false);
    /// let file_size = AdvTask::new("FileSize", 2, || {
    ///     std::fs::metadata("photo.png").map(|meta| meta.len())
    /// })?;
    ///
    /// let sender = handle.clone();
    /// handle
    ///     .send_adv_task(file_size)?
    ///     .then_forward("Prepare", move |handoff| {
    ///         let size = match handoff {
    ///             Handoff::Value(size) => size,
    ///             Handoff::Error(error) => return Err(error),
    ///         };
    ///         let shrink = AdvTask::new("Shrink", 2, move || Ok(size / 4))
    ///             .map_err(std::io::Error::other)?;
    ///
    ///         sender.send_adv_task(shrink).map_err(std::io::Error::other)
    ///     })?
    ///     // Gets `Shrink`'s value.
    ///     .then("Report", |handoff| match handoff {
    ///         Handoff::Value(quarter) => Ok(quarter),
    ///         Handoff::Error(error) => Err(error),
    ///     })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[track_caller]
    pub fn then_forward<U, E2>(
        self,
        task_name: &str,
        fn_to_exec: impl FnOnce(Handoff<T, E>) -> Result<AdvTaskHandle<U, E2>, E2> + Send + 'static,
    ) -> Result<AdvTaskHandle<U, E2>, ThryveError>
    where
        U: Send + 'static,
        E2: std::error::Error + Send + 'static,
    {
        let priority = self.base_priority;
        self.follow_up(task_name, priority, true, fn_to_exec, seal_forward)
    }
}

impl<T, E> AdvTaskHandle<T, E>
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    /// What [`then`](Self::then), [`then_with_priority`](Self::then_with_priority) and
    /// [`then_forward`](Self::then_forward) share: builds the follow-up as a task that waits on
    /// this one, sends it, and returns its handle.
    ///
    /// `sealing_fn` turns what the closure returned into the job's result: [`seal_result`] for a
    /// `Result<U, E2>`, [`seal_forward`] for the handle of a task to forward to.
    ///
    /// It marks this handle as used once the follow-up's name has passed its check, so that
    /// dropping the handle at the end sends no release. A `then` that fails on the name leaves it
    /// unused, and its drop releases the task's result.
    ///
    /// `#[track_caller]`, like them, so graph mode records the plugin's line.
    #[track_caller]
    fn follow_up<U, E2, R>(
        mut self,
        task_name: &str,
        task_priority: u8,
        inherits_priority: bool,
        fn_to_exec: impl FnOnce(Handoff<T, E>) -> R + Send + 'static,
        sealing_fn: impl FnOnce(String, R) -> TaskResult + Send + 'static,
    ) -> Result<AdvTaskHandle<U, E2>, ThryveError>
    where
        U: Send + 'static,
        E2: std::error::Error + Send + 'static,
    {
        let name_as_string = String::from(task_name);
        let job = move |sealed: SealedHandoff| match sealed.open() {
            Some(result) => sealing_fn(name_as_string, fn_to_exec(result)),
            None => TaskError::SealedHandoffFailedDowncast {
                task_name: name_as_string,
            }
            .into(),
        };

        let task = Task::new_wait(
            task_name,
            task_priority,
            self.task_id,
            inherits_priority,
            job,
        )?;

        self.is_used = true;

        let handle = AdvTaskHandle::new(&task, self.tx.clone());

        self.tx.send_task(task)?;
        Ok(handle)
    }
}

/// Generates a unique ID for a new task.
///
/// This function atomically increments the global counter and returns
/// the previous value. It is thread-safe and requires no locks due to
/// the atomic nature of `fetch_add`.
///
/// # Returns
///
/// A unique ID that has not been used by any previous task.
fn next_id() -> u64 {
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Seals what a forwarding closure returned into the [`TaskResult`] its job returns.
///
/// A handle becomes a [`SealedHandoff::Forward`] to its task, which the engine reads. The handle
/// is marked used first, so dropping it here sends no release. An error is sealed like any task's,
/// through [`seal_result`], as the forwarding task's own `Result<U, E2>`.
fn seal_forward<U, E2>(task_name: String, result: Result<AdvTaskHandle<U, E2>, E2>) -> TaskResult
where
    U: Send + 'static,
    E2: std::error::Error + Send + 'static,
{
    match result {
        Ok(mut handle) => {
            handle.is_used = true;
            TaskResult::new_handoff(SealedHandoff::Forward { to: handle.task_id })
        }

        Err(e) => seal_result(task_name, Err::<U, E2>(e)),
    }
}

/// Seals a finished `Result<T, E>` into the [`TaskResult`] a task's job returns, through [`seal`]:
/// for an [`AdvTask`], a follow-up, and a forwarding task that failed.
fn seal_result<T, E>(task_name: String, result: Result<T, E>) -> TaskResult
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    seal(task_name, move || result)()
}

/// Wraps a typed closure into the job a worker runs: the job runs the closure, then seals its whole
/// `Result<T, E>` as the handoff. For an error, it first writes the error's text, while the type is
/// still known: the task's name, the error and its causes.
fn seal<T, E>(
    task_name: String,
    fn_to_exec: impl FnOnce() -> Result<T, E> + Send + 'static,
) -> Box<dyn FnOnce() -> TaskResult + Send + 'static>
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    let wrapper = move || {
        let result = fn_to_exec();
        let error_text = if let Err(e) = &result {
            Some(format!(
                "task '{}' returned an error: {}",
                task_name,
                get_err_oneline(e)
            ))
        } else {
            None
        };

        let sealed = Box::new(result);
        TaskResult::new_handoff(SealedHandoff::Finished { sealed, error_text })
    };

    Box::new(wrapper)
}

/// What [`join`](crate::traits::join::Joinable::join) and
/// [`join_with_priority`](crate::traits::join::Joinable::join_with_priority) share: checks the
/// handles and the name, builds the join as a task that waits on the handles' tasks, sends it, and
/// returns its handle.
///
/// With `inherits_priority`, the join runs at the highest priority among the handles' tasks, and
/// `priority` is only where the search starts; without it, the join runs at `priority`.
///
/// It marks the handles as used once the join's name has passed its check, so that dropping them
/// at the end sends no releases. A join that fails on the name leaves them unused, and their drops
/// release the tasks' results.
///
/// `#[track_caller]`, like them, so graph mode records the plugin's line.
#[track_caller]
pub(crate) fn declare_join<T, E, U, E2>(
    mut slots: Vec<AdvTaskHandle<T, E>>,
    task_name: &str,
    inherits_priority: bool,
    mut priority: u8,
    fn_to_exec: impl FnOnce(JoinHandoff<Vec<T>, E>) -> Result<U, E2> + Send + 'static,
) -> Result<AdvTaskHandle<U, E2>, ThryveError>
where
    T: Send + 'static,
    U: Send + 'static,
    E: std::error::Error + Send + 'static,
    E2: std::error::Error + Send + 'static,
{
    let slots_len = slots.len();

    if slots_len == 0 {
        return Err(JoinError::MonitorsZeroInputs {
            task_name: String::from(task_name),
        }
        .into());
    }

    let name_as_string = String::from(task_name);
    let tx: PacketHandle = slots[0].tx.clone();
    let mut input_ids = vec![0; slots_len];

    for (i, handle) in slots.iter().enumerate() {
        if inherits_priority && priority < handle.base_priority {
            priority = handle.base_priority;
        }

        input_ids[i] = handle.task_id;
    }

    let job = move |mut sealed_handoffs: Vec<Option<SealedHandoff>>| -> TaskResult {
        let mut opened_handoffs: Vec<T> = Vec::new();
        let mut error_opt: Option<JoinError> = None;

        for (i, sealed_handoff_opt) in sealed_handoffs.drain(..).enumerate() {
            if let Some(sealed_handoff) = sealed_handoff_opt {
                if let Some(handoff) = sealed_handoff.open() {
                    match handoff {
                        Handoff::Value(value) => opened_handoffs.push(value),
                        Handoff::Error(e) => {
                            let join_handoff_error = JoinHandoff::Error {
                                position: i,
                                error: e,
                            };
                            return seal_result(
                                name_as_string.clone(),
                                fn_to_exec(join_handoff_error),
                            );
                        }
                    };
                } else {
                    return JoinError::WrongType {
                        task_name: name_as_string.clone(),
                        position: i,
                    }
                    .into();
                }
            } else {
                if error_opt.is_none() {
                    error_opt = Some(JoinError::InputsMissing {
                        task_name: name_as_string.clone(),
                        position: i,
                    });
                }
            }
        }

        if let Some(error) = error_opt {
            return error.into();
        }

        let join_handoff = JoinHandoff::Filled {
            values: opened_handoffs,
        };
        seal_result(name_as_string, fn_to_exec(join_handoff))
    };

    let join_task = Task::new_join_wait(task_name, priority, inherits_priority, input_ids, job)?;

    for handle in slots.iter_mut() {
        handle.is_used = true;
    }

    let join_handle = AdvTaskHandle::new(&join_task, tx.clone());

    tx.send_task(join_task)?;

    Ok(join_handle)
}
