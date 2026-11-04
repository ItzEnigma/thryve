// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Typed tasks for C: tasks whose job returns a value or an error, and everything built on them:
//! follow-ups, forwarding, joins, splits, cancelling and fan-out groups.
//!
//! It's the C side of Rust's `AdvTask`, `AdvTaskHandle`, `Joinable`, `split_into` and
//! `FanoutGroup`, and works the same way: the engine hands each task's result to whoever follows
//! it, so tasks never need to know about each other.
//!
//! # Outcomes
//!
//! A job returns a [`ThryveOutcome`], built with one of:
//!
//! - [`thryve_outcome_value`]: a value, as a pointer and the function that frees it;
//! - [`thryve_outcome_error`]: an error, as a code and a message;
//! - [`thryve_outcome_forward`]: the handle of another typed task, from a forwarding task.
//!
//! Thryve never reads a value: it carries the pointer to whoever follows the task, and frees it
//! once nobody can take it any more. If nobody follows a task that failed, its error's message is
//! written to `Error.log`.
//!
//! # Ownership
//!
//! - **User data** given with a job, and a piece's chunk, are Thryve's from then on. Each is freed
//!   once with the free function given with it: after its job has run, or when its task is
//!   dropped without running, even when the call that took it fails.
//! - **Handles** are used up by every call that takes one, even one that fails. Joins and
//!   cancels set the array entries they took to NULL.
//! - **A handoff** (what a follow-up's job gets) is only valid during that job. A value the job
//!   doesn't take with [`thryve_handoff_take_value`] is freed once the job returns.
//!
//! # Threads
//!
//! - Jobs run on the plugin workers, several at a time.
//! - Free functions run wherever Thryve lets the memory go: on a worker, or on the engine's own
//!   thread. They must only free memory: one that sends a task could make the engine wait for
//!   itself.

use std::collections::HashSet;
use std::ffi::{CStr, CString, c_void};
use std::fmt;
use std::os::raw::c_char;
use std::sync::Arc;

use thryve_utils::errors::{NamingError, ThryveError, get_err_oneline};
use thryve_utils::packets::packet_handle::PacketHandle;
use thryve_utils::packets::task::{AdvTask, AdvTaskHandle, FanoutGroup};
use thryve_utils::res::{Handoff, JoinHandoff};
use thryve_utils::traits::join::Joinable;

use crate::{
    ThryvePacketHandle, ThryveStatus, fail, ffi_guard, set_last_error, status_of, str_arg,
};

// ============================================================================
// Opaque Pointer Types
// ============================================================================

/// Marker type for [`ThryveAdvTask`]. Never instantiated.
pub struct AdvTaskOpaque {}
/// Opaque pointer to a typed task that hasn't been sent yet.
pub type ThryveAdvTask = *mut AdvTaskOpaque;

/// Marker type for [`ThryveAdvHandle`]. Never instantiated.
pub struct AdvHandleOpaque {}
/// Opaque pointer to a sent typed task's handle: what follow-ups, joins, fan-out and cancels take.
pub type ThryveAdvHandle = *mut AdvHandleOpaque;

/// Marker type for [`ThryveFanoutGroup`]. Never instantiated.
pub struct FanoutGroupOpaque {}
/// Opaque pointer to a fan-out group: the followers of one task's result, declared together.
pub type ThryveFanoutGroup = *mut FanoutGroupOpaque;

/// Marker type for [`ThryveOutcome`]. Never instantiated.
pub struct OutcomeOpaque {}
/// Opaque pointer to what a job returns: a value, an error, or a task to forward to.
pub type ThryveOutcome = *mut OutcomeOpaque;

/// Marker type for [`ThryveHandoff`]. Never instantiated.
pub struct HandoffOpaque {}
/// Opaque pointer to what a follow-up's job gets: the result of the task or tasks it follows.
/// Only valid during that job.
pub type ThryveHandoff = *mut HandoffOpaque;

// ============================================================================
// Callback Types and Error Codes
// ============================================================================

/// Frees memory the C side gave Thryve: user data, a value or a chunk. NULL: never freed.
pub type ThryveFreeFn = Option<extern "C" fn(*mut c_void)>;

/// A typed task's job: gets its user data, and returns the task's outcome.
pub type ThryveAdvJob = Option<extern "C" fn(*mut c_void) -> ThryveOutcome>;

/// The job of a follow-up, a join or a fan-out follower: gets the handoff of the task or tasks it
/// follows, and its user data, and returns its own outcome.
pub type ThryveThenJob = Option<extern "C" fn(ThryveHandoff, *mut c_void) -> ThryveOutcome>;

/// The job of a piece from [`thryve_packet_handle_split_into`]: gets where its items start, how
/// many it has, and the user data every piece shares.
pub type ThryveSplitJob = Option<extern "C" fn(usize, usize, *mut c_void) -> ThryveOutcome>;

/// The job of a piece from [`thryve_packet_handle_send_split_tasks`]: gets its chunk, its index,
/// and the user data every piece shares.
pub type ThryveChunkJob = Option<extern "C" fn(*mut c_void, usize, *mut c_void) -> ThryveOutcome>;

/// The error code of a job that returned NULL instead of an outcome.
pub const THRYVE_ERROR_NO_OUTCOME: i32 = -1;

/// The error code of a job that returned the wrong kind of outcome: a value from a forwarding
/// task, or a task to forward to from a task that doesn't forward.
pub const THRYVE_ERROR_WRONG_OUTCOME: i32 = -2;

// ============================================================================
// Values, Errors and Outcomes
// ============================================================================

/// Memory the C side gave Thryve, and the function that frees it: a task's value, its user data,
/// or a piece's chunk.
///
/// Thryve never reads the pointer: it moves it between threads and hands it back to C. Dropping
/// it frees it, unless it's NULL or was taken.
pub(crate) struct FfiValue {
    /// The C memory, or NULL.
    ptr: *mut c_void,
    /// Frees it; `None` if it needs no freeing.
    free: ThryveFreeFn,
}

// Safety: Thryve never reads the pointer. A fan-out group's followers read the value at the same
// time, on different workers, which C allows by getting it as `const`.
unsafe impl Send for FfiValue {}
unsafe impl Sync for FfiValue {}

/// User data and chunks are freed the same way as values.
pub(crate) type UserData = FfiValue;

impl FfiValue {
    pub(crate) fn new(ptr: *mut c_void, free: ThryveFreeFn) -> Self {
        Self { ptr, free }
    }

    /// The pointer. Closures call this instead of reading the field, so that they capture the whole
    /// value, and free it when they're dropped, instead of copying the pointer out.
    pub(crate) fn get(&self) -> *mut c_void {
        self.ptr
    }

    /// Takes the pointer out: the value isn't freed on drop any more.
    fn take(&mut self) -> *mut c_void {
        std::mem::replace(&mut self.ptr, std::ptr::null_mut())
    }
}

impl Drop for FfiValue {
    fn drop(&mut self) {
        if let Some(free) = self.free
            && !self.ptr.is_null()
        {
            free(self.ptr);
        }
    }
}

/// A C task's error: a code and a message.
///
/// Its text, `"<message> (code <code>)"`, is what `Error.log` shows when nobody takes the error.
#[derive(Debug, Clone)]
struct FfiError {
    /// The code the job gave, or one of Thryve's own, which are negative.
    code: i32,
    /// The message, kept as a C string so that C can read it in place.
    message: CString,
}

impl FfiError {
    /// One of Thryve's own errors.
    fn thryve(code: i32, message: &str) -> Self {
        Self {
            code,
            // Thryve's messages never hold a NUL, so this never falls back.
            message: CString::new(message).unwrap_or_default(),
        }
    }
}

impl fmt::Display for FfiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} (code {})", self.message.to_string_lossy(), self.code)
    }
}

impl std::error::Error for FfiError {}

/// A typed task as C sends it.
type FfiTask = AdvTask<FfiValue, FfiError>;
/// A typed task's handle as C holds it.
type FfiHandle = AdvTaskHandle<FfiValue, FfiError>;
/// A fan-out group as C holds it.
type FfiGroup = FanoutGroup<FfiValue, FfiError>;

/// What a job returns, behind a [`ThryveOutcome`].
enum Outcome {
    /// The task's value.
    Value(FfiValue),
    /// The task's error.
    Error(FfiError),
    /// The task forwards to the task with this handle.
    Forward(FfiHandle),
}

impl Outcome {
    /// The result of a task that returns a value or an error. A task to forward to is refused,
    /// and its handle is let go.
    fn into_value(self) -> Result<FfiValue, FfiError> {
        match self {
            Outcome::Value(value) => Ok(value),
            Outcome::Error(error) => Err(error),
            Outcome::Forward(_) => Err(FfiError::thryve(
                THRYVE_ERROR_WRONG_OUTCOME,
                "the job returned a task to forward to, but its task doesn't forward",
            )),
        }
    }

    /// The result of a forwarding task: the task to forward to, or an error. A value is refused,
    /// and freed.
    fn into_forward(self) -> Result<FfiHandle, FfiError> {
        match self {
            Outcome::Forward(handle) => Ok(handle),
            Outcome::Error(error) => Err(error),
            Outcome::Value(_) => Err(FfiError::thryve(
                THRYVE_ERROR_WRONG_OUTCOME,
                "the job returned a value, but its task forwards",
            )),
        }
    }
}

/// Takes back the outcome a job returned. NULL becomes the error [`THRYVE_ERROR_NO_OUTCOME`].
fn take_outcome(outcome: ThryveOutcome) -> Outcome {
    if outcome.is_null() {
        return Outcome::Error(FfiError::thryve(
            THRYVE_ERROR_NO_OUTCOME,
            "the job returned no outcome",
        ));
    }

    // Safety: a non-NULL outcome comes from one of the `thryve_outcome_*` functions.
    *unsafe { Box::from_raw(outcome as *mut Outcome) }
}

/// What a follow-up's job reads its input through, behind a [`ThryveHandoff`].
enum HandoffView {
    /// A follow-up's input: the task's value or error, owned here until the job takes the value.
    Owned(Handoff<FfiValue, FfiError>),
    /// A fan-out follower's input: the one result the group's followers share, read-only.
    Shared(Arc<Handoff<FfiValue, Arc<FfiError>>>),
    /// A join's input: every task's value, or the first error and where it came from.
    Joined(JoinHandoff<Vec<FfiValue>, FfiError>),
}

impl HandoffView {
    /// The error it holds, if the task, or one of the joined tasks, failed.
    fn error(&self) -> Option<&FfiError> {
        match self {
            HandoffView::Owned(Handoff::Error(error)) => Some(error),
            HandoffView::Shared(share) => match &**share {
                Handoff::Error(error) => Some(error),
                Handoff::Value(_) => None,
            },
            HandoffView::Joined(JoinHandoff::Error { error, .. }) => Some(error),
            _ => None,
        }
    }

    /// The values it holds: one for a follow-up or a follower, one per task for a join, and none
    /// for an error.
    fn values(&self) -> &[FfiValue] {
        match self {
            HandoffView::Owned(Handoff::Value(value)) => std::slice::from_ref(value),
            HandoffView::Shared(share) => match &**share {
                Handoff::Value(value) => std::slice::from_ref(value),
                Handoff::Error(_) => &[],
            },
            HandoffView::Joined(JoinHandoff::Filled { values }) => values,
            _ => &[],
        }
    }
}

/// Runs a job that follows other tasks: hands it `view` and its user data, and takes back its
/// outcome. The view is dropped after that, which frees each value the job didn't take.
fn outcome_of(
    job: extern "C" fn(ThryveHandoff, *mut c_void) -> ThryveOutcome,
    mut view: HandoffView,
    user_data: &UserData,
) -> Outcome {
    let handoff = &mut view as *mut HandoffView as ThryveHandoff;
    take_outcome(job(handoff, user_data.get()))
}

// ============================================================================
// Helpers
// ============================================================================

/// Gives a handle to C.
fn give_handle(handle: FfiHandle) -> ThryveAdvHandle {
    Box::into_raw(Box::new(handle)) as ThryveAdvHandle
}

/// Gives the handle of a declared task to C, or records why it couldn't be declared.
fn give_handle_or_fail(declared: Result<FfiHandle, ThryveError>) -> ThryveAdvHandle {
    match declared {
        Ok(handle) => give_handle(handle),
        Err(error) => fail(std::ptr::null_mut(), get_err_oneline(&error)),
    }
}

/// Takes a handle back from C: `None` for NULL, which is recorded.
fn take_handle(handle: ThryveAdvHandle) -> Option<FfiHandle> {
    if handle.is_null() {
        set_last_error("handle is NULL");
        return None;
    }

    // Safety: a non-NULL handle comes from `give_handle`, and C gives each one back once.
    Some(*unsafe { Box::from_raw(handle as *mut FfiHandle) })
}

/// Takes back every handle in a C array, and sets each entry to NULL. Also returns the first
/// problem found: a NULL array, a NULL entry, or a handle that appears twice, which is taken once.
///
/// # Safety
///
/// `handles` must be NULL or point to `count` entries.
unsafe fn take_handles(
    handles: *mut ThryveAdvHandle,
    count: usize,
) -> (Vec<FfiHandle>, Option<String>) {
    if handles.is_null() {
        return (Vec::new(), Some(String::from("handles is NULL")));
    }

    let mut taken = Vec::with_capacity(count);
    let mut seen = HashSet::with_capacity(count);
    let mut problem = None;

    for i in 0..count {
        // Safety: the caller guarantees `count` entries.
        let entry = unsafe { handles.add(i) };
        let handle = unsafe { std::ptr::replace(entry, std::ptr::null_mut()) };

        // A NULL entry, or one seen already, is skipped; the first such problem is kept.
        if handle.is_null() {
            problem.get_or_insert_with(|| format!("handles[{i}] is NULL"));
        } else if !seen.insert(handle as usize) {
            problem.get_or_insert_with(|| format!("handles[{i}] appears twice"));
        } else {
            // Safety: as in `take_handle`.
            taken.push(*unsafe { Box::from_raw(handle as *mut FfiHandle) });
        }
    }

    (taken, problem)
}

/// Writes handles into a C array that has room for all of them.
///
/// # Safety
///
/// `out_handles` must point to at least `handles.len()` entries.
unsafe fn give_handles(handles: Vec<FfiHandle>, out_handles: *mut ThryveAdvHandle) {
    for (i, handle) in handles.into_iter().enumerate() {
        // Safety: the caller guarantees the room.
        unsafe { out_handles.add(i).write(give_handle(handle)) };
    }
}

// ============================================================================
// Outcomes
// ============================================================================

/// Builds the outcome of a job that produced a value.
///
/// Thryve never reads `value`: it carries it to whoever follows the task. It frees it with
/// `free_value` once nobody can take it any more: when the follow-up's job returns without taking
/// it, or when nobody follows the task. A NULL `free_value` means it's never freed, for a value
/// that needs no freeing.
///
/// # Returns
///
/// The outcome, for the job to return. One that isn't returned is freed with
/// [`thryve_outcome_free`].
///
/// # Example
///
/// ```c
/// ThryveOutcome load_img(void* user_data) {
///     Pixels* pixels = decode((const char*)user_data);
///     return thryve_outcome_value(pixels, free_pixels);
/// }
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_outcome_value(
    value: *mut c_void,
    free_value: ThryveFreeFn,
) -> ThryveOutcome {
    ffi_guard(std::ptr::null_mut(), || {
        let outcome = Outcome::Value(FfiValue::new(value, free_value));
        Box::into_raw(Box::new(outcome)) as ThryveOutcome
    })
}

/// Builds the outcome of a job that failed: an error with a code and a message.
///
/// The message is copied, so it can be a buffer the job reuses. NULL becomes `"no message"`. If
/// nobody follows the task, `Error.log` gets the task's name, the message and the code. Negative
/// codes are Thryve's own ([`THRYVE_ERROR_NO_OUTCOME`], [`THRYVE_ERROR_WRONG_OUTCOME`]): use zero
/// or positive codes.
///
/// # Returns
///
/// The outcome, for the job to return. One that isn't returned is freed with
/// [`thryve_outcome_free`].
///
/// # Safety
///
/// `message` must be NULL or a valid null-terminated C string, valid for reads for the duration
/// of this call.
///
/// # Example
///
/// ```c
/// char message[256];
/// snprintf(message, sizeof message, "can't open %s", path);
/// return thryve_outcome_error(2, message);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_outcome_error(code: i32, message: *const c_char) -> ThryveOutcome {
    ffi_guard(std::ptr::null_mut(), || {
        let message = if message.is_null() {
            CString::from(c"no message")
        } else {
            // Safety: the caller guarantees a valid C string.
            unsafe { CStr::from_ptr(message) }.to_owned()
        };

        let outcome = Outcome::Error(FfiError { code, message });
        Box::into_raw(Box::new(outcome)) as ThryveOutcome
    })
}

/// Builds the outcome of a forwarding job: the handle of the typed task to forward to.
///
/// Whoever follows the forwarding task gets that task's value or error. Only the job of a task
/// created with [`thryve_adv_task_create_forward`], or of a follow-up declared with
/// [`thryve_adv_handle_then_forward`], can return it; any other task gets the error
/// [`THRYVE_ERROR_WRONG_OUTCOME`] instead.
///
/// `handle` is used up, even when this fails.
///
/// # Returns
///
/// The outcome, for the job to return, or NULL if `handle` is NULL. One that isn't returned is
/// freed with [`thryve_outcome_free`].
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_outcome_forward(handle: ThryveAdvHandle) -> ThryveOutcome {
    ffi_guard(std::ptr::null_mut(), || {
        let Some(handle) = take_handle(handle) else {
            return std::ptr::null_mut();
        };

        Box::into_raw(Box::new(Outcome::Forward(handle))) as ThryveOutcome
    })
}

/// Frees an outcome that no job returned: a value is freed with its free function, and a task to
/// forward to is let go, as with [`thryve_adv_handle_free`]. NULL does nothing.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_outcome_free(outcome: ThryveOutcome) {
    ffi_guard((), || {
        if !outcome.is_null() {
            drop(take_outcome(outcome));
        }
    })
}

// ============================================================================
// Typed Tasks
// ============================================================================

/// What [`thryve_adv_task_create`] and [`thryve_adv_task_create_forward`] share: takes the user
/// data, checks the name and the job, and builds the task with `create`, which wraps the job.
///
/// # Safety
///
/// `name` must be NULL or a valid C string.
unsafe fn create_task(
    name: *const c_char,
    job: ThryveAdvJob,
    user_data: UserData,
    create: impl FnOnce(
        &str,
        extern "C" fn(*mut c_void) -> ThryveOutcome,
        UserData,
    ) -> Result<FfiTask, NamingError>,
) -> ThryveAdvTask {
    // Safety: the caller guarantees `name`.
    let Ok(name) = (unsafe { str_arg(name, "name") }) else {
        return std::ptr::null_mut();
    };
    let Some(job) = job else {
        return fail(std::ptr::null_mut(), "job is NULL");
    };

    match create(name, job, user_data) {
        Ok(task) => Box::into_raw(Box::new(task)) as ThryveAdvTask,
        Err(error) => fail(std::ptr::null_mut(), get_err_oneline(&error)),
    }
}

/// Creates a typed task: its job returns a value or an error, which Thryve hands to whoever
/// follows the task.
///
/// # Arguments
///
/// * `name` - The task's name (a null-terminated C string), copied
/// * `priority` - Its priority (higher = more urgent)
/// * `job` - Runs once, on a worker, with `user_data`, and returns the task's outcome:
///   [`thryve_outcome_value`] or [`thryve_outcome_error`]
/// * `user_data` - Passed to `job`
/// * `free_user_data` - Frees `user_data` once the job has run, or with the task if it never runs.
///   NULL: never freed
///
/// # Returns
///
/// The task, for [`thryve_packet_handle_send_adv_task`], or NULL if `name` or `job` is NULL, or
/// the name is empty, has a control character or isn't valid UTF-8; [`thryve_last_error`](crate::thryve_last_error) says
/// which. `user_data` is freed either way.
///
/// # Safety
///
/// `name` must be NULL or a valid null-terminated C string, valid for reads for the duration of
/// this call. `job` and `free_user_data` must be safe to call from any thread.
///
/// # Example
///
/// ```c
/// ThryveAdvTask hash = thryve_adv_task_create("Hash", 2, hash_file, strdup("photo.png"), free);
/// ThryveAdvHandle digest = thryve_packet_handle_send_adv_task(packet_handle, hash);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_adv_task_create(
    name: *const c_char,
    priority: u8,
    job: ThryveAdvJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveAdvTask {
    ffi_guard(std::ptr::null_mut(), || {
        let user_data = UserData::new(user_data, free_user_data);

        // Safety: the caller guarantees `name`.
        unsafe {
            create_task(name, job, user_data, |name, job, user_data| {
                AdvTask::new(name, priority, move || {
                    take_outcome(job(user_data.get())).into_value()
                })
            })
        }
    })
}

/// Creates a typed task that forwards: its job creates and sends another typed task, and returns
/// that task's handle with [`thryve_outcome_forward`]. Whoever follows this task then gets that
/// task's value or error.
///
/// It's for a last step that only exists once the task runs, such as one picked by what the task
/// found. If the steps are known up front, chain them with [`thryve_adv_handle_then`] instead. The
/// job can also fail with [`thryve_outcome_error`], and nothing is forwarded.
///
/// # Arguments
///
/// The same as [`thryve_adv_task_create`].
///
/// # Returns
///
/// The same as [`thryve_adv_task_create`].
///
/// # Safety
///
/// The same as [`thryve_adv_task_create`].
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_adv_task_create_forward(
    name: *const c_char,
    priority: u8,
    job: ThryveAdvJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveAdvTask {
    ffi_guard(std::ptr::null_mut(), || {
        let user_data = UserData::new(user_data, free_user_data);

        // Safety: the caller guarantees `name`.
        unsafe {
            create_task(name, job, user_data, |name, job, user_data| {
                AdvTask::forward(name, priority, move || {
                    take_outcome(job(user_data.get())).into_forward()
                })
            })
        }
    })
}

/// Frees a typed task that was never sent, and its user data. NULL does nothing.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_adv_task_free(task: ThryveAdvTask) {
    ffi_guard((), || {
        if !task.is_null() {
            // Safety: a non-NULL task comes from `thryve_adv_task_create*`.
            drop(unsafe { Box::from_raw(task as *mut FfiTask) });
        }
    })
}

/// Sends a typed task to the engine, and returns its handle.
///
/// The task is queued and run like any other. Its result waits in the engine for as long as the
/// handle exists, so a follow-up declared after the task finished still gets it.
///
/// # Arguments
///
/// * `handle` - The engine's packet handle
/// * `task` - The task, used up, even when the send fails
///
/// # Returns
///
/// The task's handle: declare what follows it ([`thryve_adv_handle_then`], a join, a fan-out), or
/// let it go with [`thryve_adv_handle_free`]. NULL if `handle` or `task` is NULL, or the engine
/// isn't running; the task is dropped, and its user data freed.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_packet_handle_send_adv_task(
    handle: ThryvePacketHandle,
    task: ThryveAdvTask,
) -> ThryveAdvHandle {
    ffi_guard(std::ptr::null_mut(), || {
        if task.is_null() {
            return fail(std::ptr::null_mut(), "task is NULL");
        }
        // Safety: a non-NULL task comes from `thryve_adv_task_create*`.
        let task = *unsafe { Box::from_raw(task as *mut FfiTask) };

        if handle.is_null() {
            return fail(std::ptr::null_mut(), "handle is NULL");
        }
        // Safety: a non-NULL packet handle comes from Thryve.
        let tx = unsafe { &*(handle as *const PacketHandle) };

        give_handle_or_fail(tx.send_adv_task(task))
    })
}

// ============================================================================
// Follow-ups
// ============================================================================

/// What every `then` takes, checked: the handle, which is used up whatever happens, the name and
/// the job. `None` once the problem is recorded.
///
/// # Safety
///
/// `name` must be NULL or a valid C string.
unsafe fn follow_up_args<'a>(
    handle: ThryveAdvHandle,
    name: *const c_char,
    job: ThryveThenJob,
) -> Option<(
    FfiHandle,
    &'a str,
    extern "C" fn(ThryveHandoff, *mut c_void) -> ThryveOutcome,
)> {
    let handle = take_handle(handle)?;
    // Safety: the caller guarantees `name`.
    let name = unsafe { str_arg(name, "name") }.ok()?;
    let Some(job) = job else {
        set_last_error("job is NULL");
        return None;
    };

    Some((handle, name, job))
}

/// Declares the task to run after this one: its job gets this task's value or error.
///
/// The follow-up is sent now, and waits in the engine until this task finishes. It inherits this
/// task's priority, capped below realtime. If this task has already finished, the follow-up runs
/// at once with the result the engine kept for it.
///
/// # Arguments
///
/// * `handle` - The task's handle, used up, even when this fails: a task has one follow-up
/// * `name` - The follow-up's name (a null-terminated C string), copied
/// * `job` - Runs once, on a worker, with the task's handoff and `user_data`, and returns the
///   follow-up's outcome. Read the handoff with the `thryve_handoff_*` functions
/// * `user_data` - Passed to `job`
/// * `free_user_data` - Frees `user_data` once the job has run, or if it never runs. NULL: never
///   freed
///
/// # Returns
///
/// The follow-up's handle, so steps can be chained, or NULL if an argument is NULL, the name is
/// invalid, or the engine isn't running; [`thryve_last_error`](crate::thryve_last_error) says which. If the handle was
/// valid, its task's result is let go, as with [`thryve_adv_handle_free`].
///
/// # Safety
///
/// `name` must be NULL or a valid null-terminated C string, valid for reads for the duration of
/// this call. `job` and `free_user_data` must be safe to call from any thread.
///
/// # Example
///
/// ```c
/// ThryveOutcome load_img(ThryveHandoff digest, void* user_data) {
///     if (thryve_handoff_is_error(digest)) {
///         // Hash's error, passed on.
///         return thryve_outcome_error(thryve_handoff_error_code(digest),
///                                     thryve_handoff_error_message(digest));
///     }
///     const Digest* hash = thryve_handoff_value(digest, 0);
///     return thryve_outcome_value(decode(hash), free_pixels);
/// }
///
/// ThryveAdvHandle pixels = thryve_adv_handle_then(digest, "LoadImg", load_img, NULL, NULL);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_adv_handle_then(
    handle: ThryveAdvHandle,
    name: *const c_char,
    job: ThryveThenJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveAdvHandle {
    ffi_guard(std::ptr::null_mut(), || {
        let user_data = UserData::new(user_data, free_user_data);
        // Safety: the caller guarantees `name`.
        let Some((handle, name, job)) = (unsafe { follow_up_args(handle, name, job) }) else {
            return std::ptr::null_mut();
        };

        give_handle_or_fail(handle.then(name, move |handoff| {
            outcome_of(job, HandoffView::Owned(handoff), &user_data).into_value()
        }))
    })
}

/// Like [`thryve_adv_handle_then`], but the follow-up runs at `priority` instead of inheriting the
/// task's priority. It's kept as given, so it can be realtime.
///
/// # Safety
///
/// The same as [`thryve_adv_handle_then`].
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_adv_handle_then_with_priority(
    handle: ThryveAdvHandle,
    name: *const c_char,
    priority: u8,
    job: ThryveThenJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveAdvHandle {
    ffi_guard(std::ptr::null_mut(), || {
        let user_data = UserData::new(user_data, free_user_data);
        // Safety: the caller guarantees `name`.
        let Some((handle, name, job)) = (unsafe { follow_up_args(handle, name, job) }) else {
            return std::ptr::null_mut();
        };

        give_handle_or_fail(handle.then_with_priority(name, priority, move |handoff| {
            outcome_of(job, HandoffView::Owned(handoff), &user_data).into_value()
        }))
    })
}

/// Like [`thryve_adv_handle_then`], but the follow-up forwards: its job gets the task's handoff,
/// creates and sends another typed task, and returns that task's handle with
/// [`thryve_outcome_forward`]. Whoever follows the follow-up then gets that task's value or error.
///
/// The job can also fail with [`thryve_outcome_error`], and nothing is forwarded.
///
/// # Safety
///
/// The same as [`thryve_adv_handle_then`].
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_adv_handle_then_forward(
    handle: ThryveAdvHandle,
    name: *const c_char,
    job: ThryveThenJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveAdvHandle {
    ffi_guard(std::ptr::null_mut(), || {
        let user_data = UserData::new(user_data, free_user_data);
        // Safety: the caller guarantees `name`.
        let Some((handle, name, job)) = (unsafe { follow_up_args(handle, name, job) }) else {
            return std::ptr::null_mut();
        };

        give_handle_or_fail(handle.then_forward(name, move |handoff| {
            outcome_of(job, HandoffView::Owned(handoff), &user_data).into_forward()
        }))
    })
}

/// Cancels the task, and uses the handle up. NULL does nothing.
///
/// A task that hasn't started never runs, and its user data is freed. A running task can't be
/// stopped: it finishes, and its result is let go. The work only this task needed stops too:
/// cancelling a follow-up also cancels the task it follows, if that hasn't started, and so on up
/// the chain. A fan-out follower only withdraws from its group.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_adv_handle_cancel(handle: ThryveAdvHandle) {
    ffi_guard((), || {
        if !handle.is_null()
            && let Some(handle) = take_handle(handle)
        {
            handle.cancel();
        }
    })
}

/// Lets a handle go without declaring anything after its task: the task still runs, and the engine
/// lets its result go. A value is freed with its free function, and an error is written to
/// `Error.log`. NULL does nothing.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_adv_handle_free(handle: ThryveAdvHandle) {
    ffi_guard((), || {
        if !handle.is_null() {
            drop(take_handle(handle));
        }
    })
}

// ============================================================================
// Joins
// ============================================================================

/// What both joins take, checked: the handles, which are used up whatever happens, the name and the
/// job. `None` once the problem is recorded.
///
/// # Safety
///
/// `handles` must be NULL or point to `count` entries, and `name` must be NULL or a valid C string.
unsafe fn join_args<'a>(
    handles: *mut ThryveAdvHandle,
    count: usize,
    name: *const c_char,
    job: ThryveThenJob,
) -> Option<(
    Vec<FfiHandle>,
    &'a str,
    extern "C" fn(ThryveHandoff, *mut c_void) -> ThryveOutcome,
)> {
    // Safety: the caller guarantees `handles`.
    let (handles, problem) = unsafe { take_handles(handles, count) };
    if let Some(problem) = problem {
        set_last_error(problem);
        return None;
    }
    // Safety: the caller guarantees `name`.
    let name = unsafe { str_arg(name, "name") }.ok()?;
    let Some(job) = job else {
        set_last_error("job is NULL");
        return None;
    };

    Some((handles, name, job))
}

/// Joins several typed tasks into one follow-up: its job runs once all of them have finished, with
/// all their values, or as soon as one fails, with its error.
///
/// The follow-up inherits the highest priority among the tasks, capped below realtime.
///
/// # Arguments
///
/// * `handles` - An array of the tasks' handles. Every entry is used up, even when this fails, and
///   set to NULL
/// * `count` - How many handles the array holds
/// * `name` - The join's name (a null-terminated C string), copied
/// * `job` - Runs once, on a worker, with the joined handoff and `user_data`. A handoff with
///   values holds one per task, in the array's order (see [`thryve_handoff_value`]); one with an
///   error says which task failed (see [`thryve_handoff_error_position`])
/// * `user_data` - Passed to `job`
/// * `free_user_data` - Frees `user_data` once the job has run, or if it never runs. NULL: never
///   freed
///
/// # Returns
///
/// The join's handle, or NULL if `handles` or an entry is NULL, a handle appears twice, `count` is
/// 0, `job` is NULL, the name is invalid, or the engine isn't running; [`thryve_last_error`](crate::thryve_last_error) says
/// which. The tasks' results are then let go.
///
/// # Safety
///
/// `handles` must be NULL or point to `count` entries. `name` must be NULL or a valid
/// null-terminated C string, valid for reads for the duration of this call. `job` and
/// `free_user_data` must be safe to call from any thread.
///
/// # Example
///
/// ```c
/// ThryveAdvHandle parts[3] = {archive, thumbnail, metadata};
/// ThryveAdvHandle report = thryve_adv_handles_join(parts, 3, "Report", report_job, NULL, NULL);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_adv_handles_join(
    handles: *mut ThryveAdvHandle,
    count: usize,
    name: *const c_char,
    job: ThryveThenJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveAdvHandle {
    ffi_guard(std::ptr::null_mut(), || {
        let user_data = UserData::new(user_data, free_user_data);
        // Safety: the caller guarantees `handles` and `name`.
        let Some((handles, name, job)) = (unsafe { join_args(handles, count, name, job) }) else {
            return std::ptr::null_mut();
        };

        give_handle_or_fail(handles.join(name, move |joined| {
            outcome_of(job, HandoffView::Joined(joined), &user_data).into_value()
        }))
    })
}

/// Like [`thryve_adv_handles_join`], but the join runs at `priority` instead of inheriting its
/// tasks' highest priority. It's kept as given, so it can be realtime.
///
/// # Safety
///
/// The same as [`thryve_adv_handles_join`].
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_adv_handles_join_with_priority(
    handles: *mut ThryveAdvHandle,
    count: usize,
    name: *const c_char,
    priority: u8,
    job: ThryveThenJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveAdvHandle {
    ffi_guard(std::ptr::null_mut(), || {
        let user_data = UserData::new(user_data, free_user_data);
        // Safety: the caller guarantees `handles` and `name`.
        let Some((handles, name, job)) = (unsafe { join_args(handles, count, name, job) }) else {
            return std::ptr::null_mut();
        };

        give_handle_or_fail(handles.join_with_priority(name, priority, move |joined| {
            outcome_of(job, HandoffView::Joined(joined), &user_data).into_value()
        }))
    })
}

/// Cancels every task in an array of handles, as [`thryve_adv_handle_cancel`] cancels one, such as
/// the pieces a split gave back. Each entry is used up and set to NULL. NULL entries are skipped,
/// and so is a handle's second appearance.
///
/// # Safety
///
/// `handles` must be NULL or point to `count` entries.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_adv_handles_cancel_all(
    handles: *mut ThryveAdvHandle,
    count: usize,
) {
    ffi_guard((), || {
        // Safety: the caller guarantees `handles`.
        let (handles, _) = unsafe { take_handles(handles, count) };
        handles.cancel_all();
    })
}

// ============================================================================
// Splits
// ============================================================================

/// Splits work over several typed tasks: cuts `item_count` items into at most `number_of_pieces`
/// pieces, in order, and sends one task per piece. Every piece runs `job` with where its items
/// start and how many it has.
///
/// The cut is balanced: the pieces' sizes differ by at most one item, and no piece is empty, so
/// there are `min(item_count, number_of_pieces)` pieces. The items stay where they are: keep them
/// in `user_data`, which every piece shares.
///
/// # Arguments
///
/// * `handle` - The engine's packet handle
/// * `item_count` - How many items there are
/// * `name` - Every piece's name (a null-terminated C string), copied
/// * `number_of_pieces` - The most pieces to cut; not 0
/// * `priority` - Every piece's priority
/// * `job` - Runs once per piece, on a worker, with the piece's first item, its number of items
///   and `user_data`
/// * `user_data` - Shared by every piece
/// * `free_user_data` - Frees `user_data` once the last piece is done with it, even if no piece
///   runs. NULL: never freed
/// * `out_handles` - Receives the pieces' handles, in order: it must have room for
///   `number_of_pieces` handles
/// * `out_count` - Receives how many pieces were sent
///
/// # Returns
///
/// * `ThryveStatus::Ok` once every piece is sent
/// * `ThryveStatus::NullArg` if `handle`, `name`, `job`, `out_handles` or `out_count` is NULL
/// * `ThryveStatus::StrConvError` if the name isn't valid UTF-8
/// * `ThryveStatus::InvalidState` if the engine isn't running
/// * `ThryveStatus::OperationFail` if `number_of_pieces` is 0 or the name is invalid
///
/// [`thryve_last_error`](crate::thryve_last_error) says why it failed. Nothing is written to `out_handles` then.
///
/// # Safety
///
/// `name` must be NULL or a valid null-terminated C string, valid for reads for the duration of
/// this call. `out_handles` must be NULL or have room for `number_of_pieces` handles, and
/// `out_count` must be NULL or valid for writes. `job` and `free_user_data` must be safe to call
/// from any thread.
///
/// # Example
///
/// ```c
/// // Four pieces of 250 rows each, in `rows`, which every piece reads.
/// ThryveAdvHandle pieces[4];
/// size_t piece_count = 0;
/// thryve_packet_handle_split_into(packet_handle, 1000, "SumRows", 4, 2, sum_rows, rows, free_rows,
///                                 pieces, &piece_count);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_packet_handle_split_into(
    handle: ThryvePacketHandle,
    item_count: usize,
    name: *const c_char,
    number_of_pieces: usize,
    priority: u8,
    job: ThryveSplitJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
    out_handles: *mut ThryveAdvHandle,
    out_count: *mut usize,
) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        let user_data = UserData::new(user_data, free_user_data);

        if handle.is_null() || out_handles.is_null() || out_count.is_null() {
            return fail(
                ThryveStatus::NullArg,
                "handle, out_handles or out_count is NULL",
            );
        }
        // Safety: the caller guarantees `name`.
        let name = match unsafe { str_arg(name, "name") } {
            Ok(name) => name,
            Err(status) => return status,
        };
        let Some(job) = job else {
            return fail(ThryveStatus::NullArg, "job is NULL");
        };
        // Safety: a non-NULL packet handle comes from Thryve.
        let tx = unsafe { &*(handle as *const PacketHandle) };

        // The pieces only need to know which items they hold, so each item is a `()`: the list
        // carries nothing, and never allocates.
        let units = vec![(); item_count];
        let split = tx.split_into(
            units,
            name,
            number_of_pieces,
            priority,
            move |piece, start| take_outcome(job(start, piece.len(), user_data.get())).into_value(),
        );

        match split {
            Ok(handles) => {
                // Safety: the caller guarantees `out_count`, and room for `number_of_pieces`
                // handles, which is at least as many as the pieces.
                unsafe {
                    out_count.write(handles.len());
                    give_handles(handles, out_handles);
                }
                ThryveStatus::Ok
            }
            Err(error) => fail(status_of(&error), get_err_oneline(&error)),
        }
    })
}

/// Sends one typed task per chunk of work the caller already cut, each running `job` on its own
/// chunk: a cut of your own, by size, at content boundaries, or over things that aren't one
/// array's items.
///
/// # Arguments
///
/// * `handle` - The engine's packet handle
/// * `chunks` - An array of `chunk_count` chunks. Each chunk becomes its piece's, used up even
///   when this fails; the array itself stays the caller's
/// * `chunk_count` - How many chunks there are; 0 sends nothing
/// * `free_chunk` - Frees a chunk once its piece's job has run, or if the piece never runs. NULL:
///   never freed
/// * `name` - Every piece's name (a null-terminated C string), copied
/// * `priority` - Every piece's priority
/// * `job` - Runs once per piece, on a worker, with its chunk, the piece's index and `user_data`
/// * `user_data` - Shared by every piece
/// * `free_user_data` - Frees `user_data` once the last piece is done with it, even if no piece
///   runs. NULL: never freed
/// * `out_handles` - Receives the pieces' handles, in the chunks' order: it must have room for
///   `chunk_count` handles
///
/// # Returns
///
/// * `ThryveStatus::Ok` once every piece is sent
/// * `ThryveStatus::NullArg` if `handle`, `name`, `job` or `out_handles` is NULL, or `chunks` is
///   NULL with a `chunk_count` above 0
/// * `ThryveStatus::StrConvError` if the name isn't valid UTF-8
/// * `ThryveStatus::InvalidState` if the engine isn't running
/// * `ThryveStatus::OperationFail` if the name is invalid
///
/// [`thryve_last_error`](crate::thryve_last_error) says why it failed. Nothing is written to `out_handles` then.
///
/// # Safety
///
/// `chunks` must be NULL or point to `chunk_count` entries, and `name` must be NULL or a valid
/// null-terminated C string, both valid for reads for the duration of this call. `out_handles`
/// must be NULL or have room for `chunk_count` handles. `job`, `free_chunk` and `free_user_data`
/// must be safe to call from any thread.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_packet_handle_send_split_tasks(
    handle: ThryvePacketHandle,
    chunks: *const *mut c_void,
    chunk_count: usize,
    free_chunk: ThryveFreeFn,
    name: *const c_char,
    priority: u8,
    job: ThryveChunkJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
    out_handles: *mut ThryveAdvHandle,
) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        let user_data = UserData::new(user_data, free_user_data);

        // The chunks are Thryve's from here on, so any failure below frees them.
        if chunks.is_null() && chunk_count > 0 {
            return fail(ThryveStatus::NullArg, "chunks is NULL");
        }
        let chunks: Vec<UserData> = (0..chunk_count)
            // Safety: the caller guarantees `chunk_count` entries.
            .map(|i| UserData::new(unsafe { chunks.add(i).read() }, free_chunk))
            .collect();

        if handle.is_null() || out_handles.is_null() {
            return fail(ThryveStatus::NullArg, "handle or out_handles is NULL");
        }
        // Safety: the caller guarantees `name`.
        let name = match unsafe { str_arg(name, "name") } {
            Ok(name) => name,
            Err(status) => return status,
        };
        let Some(job) = job else {
            return fail(ThryveStatus::NullArg, "job is NULL");
        };
        // Safety: a non-NULL packet handle comes from Thryve.
        let tx = unsafe { &*(handle as *const PacketHandle) };

        // Each piece's chunk is dropped once its job returns, which frees it.
        let sent = tx.send_split_tasks(chunks, name, priority, move |chunk, index| {
            take_outcome(job(chunk.get(), index, user_data.get())).into_value()
        });

        match sent {
            Ok(handles) => {
                // Safety: the caller guarantees room for `chunk_count` handles.
                unsafe { give_handles(handles, out_handles) };
                ThryveStatus::Ok
            }
            Err(error) => fail(status_of(&error), get_err_oneline(&error)),
        }
    })
}

// ============================================================================
// Fan-out
// ============================================================================

/// Starts a fan-out group on a task's result, so that several followers can share it: declare
/// each one with [`thryve_fanout_then`], then send them with [`thryve_fanout_send`].
///
/// Each follower reads the one value in place, at the same time as the others, so it gets the
/// value as `const` and can't take it. The value is freed once the last follower is done with it.
///
/// # Arguments
///
/// * `handle` - The task's handle, used up, even when this fails
///
/// # Returns
///
/// The group, or NULL if `handle` is NULL.
///
/// # Example
///
/// ```c
/// ThryveFanoutGroup group = thryve_adv_handle_fan_out(pixels);
/// ThryveAdvHandle thumbnail = thryve_fanout_then(group, "Thumbnail", thumbnail_job, NULL, NULL);
/// ThryveAdvHandle metadata = thryve_fanout_then(group, "Metadata", metadata_job, NULL, NULL);
/// thryve_fanout_send(group);
/// ```
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_adv_handle_fan_out(handle: ThryveAdvHandle) -> ThryveFanoutGroup {
    ffi_guard(std::ptr::null_mut(), || {
        let Some(handle) = take_handle(handle) else {
            return std::ptr::null_mut();
        };

        Box::into_raw(Box::new(handle.fan_out())) as ThryveFanoutGroup
    })
}

/// What both of a group's `then` take, checked: the group, the name and the job. `None` once the
/// problem is recorded.
///
/// # Safety
///
/// `name` must be NULL or a valid C string.
unsafe fn follower_args<'a>(
    group: ThryveFanoutGroup,
    name: *const c_char,
    job: ThryveThenJob,
) -> Option<(
    &'a mut FfiGroup,
    &'a str,
    extern "C" fn(ThryveHandoff, *mut c_void) -> ThryveOutcome,
)> {
    if group.is_null() {
        set_last_error("group is NULL");
        return None;
    }
    // Safety: the caller guarantees `name`.
    let name = unsafe { str_arg(name, "name") }.ok()?;
    let Some(job) = job else {
        set_last_error("job is NULL");
        return None;
    };

    // Safety: a non-NULL group comes from `thryve_adv_handle_fan_out`.
    Some((unsafe { &mut *(group as *mut FfiGroup) }, name, job))
}

/// Declares a follower in a fan-out group: a task that runs with the group's shared result once
/// the group is sent.
///
/// The follower inherits the task's priority, capped below realtime, and its handle comes back at
/// once: it can be followed, joined or cancelled like any other. Its job reads the shared value
/// with [`thryve_handoff_value`], which it must not change.
///
/// # Arguments
///
/// * `group` - The group, which stays the caller's
/// * `name` - The follower's name (a null-terminated C string), copied
/// * `job` - Runs once, on a worker, with the shared handoff and `user_data`
/// * `user_data` - Passed to `job`
/// * `free_user_data` - Frees `user_data` once the job has run, or if it never runs. NULL: never
///   freed
///
/// # Returns
///
/// The follower's handle, or NULL if an argument is NULL or the name is invalid; the group is left
/// as it was.
///
/// # Safety
///
/// `name` must be NULL or a valid null-terminated C string, valid for reads for the duration of
/// this call. `job` and `free_user_data` must be safe to call from any thread.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_fanout_then(
    group: ThryveFanoutGroup,
    name: *const c_char,
    job: ThryveThenJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveAdvHandle {
    ffi_guard(std::ptr::null_mut(), || {
        let user_data = UserData::new(user_data, free_user_data);
        // Safety: the caller guarantees `name`.
        let Some((group, name, job)) = (unsafe { follower_args(group, name, job) }) else {
            return std::ptr::null_mut();
        };

        give_handle_or_fail(group.then(name, move |share| {
            outcome_of(job, HandoffView::Shared(share), &user_data).into_value()
        }))
    })
}

/// Like [`thryve_fanout_then`], but the follower runs at `priority` instead of inheriting the
/// task's priority. It's kept as given, so it can be realtime.
///
/// # Safety
///
/// The same as [`thryve_fanout_then`].
#[cfg_attr(not(test), unsafe(no_mangle))]
pub unsafe extern "C" fn thryve_fanout_then_with_priority(
    group: ThryveFanoutGroup,
    name: *const c_char,
    priority: u8,
    job: ThryveThenJob,
    user_data: *mut c_void,
    free_user_data: ThryveFreeFn,
) -> ThryveAdvHandle {
    ffi_guard(std::ptr::null_mut(), || {
        let user_data = UserData::new(user_data, free_user_data);
        // Safety: the caller guarantees `name`.
        let Some((group, name, job)) = (unsafe { follower_args(group, name, job) }) else {
            return std::ptr::null_mut();
        };

        give_handle_or_fail(group.then_with_priority(name, priority, move |share| {
            outcome_of(job, HandoffView::Shared(share), &user_data).into_value()
        }))
    })
}

/// Sends a fan-out group's followers to the engine, and uses the group up.
///
/// # Returns
///
/// * `ThryveStatus::Ok` once the followers are sent
/// * `ThryveStatus::NullArg` if `group` is NULL
/// * `ThryveStatus::InvalidState` if the group has no follower, or the engine isn't running. A
///   group with no follower sends nothing, and its task's result is let go
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_fanout_send(group: ThryveFanoutGroup) -> ThryveStatus {
    ffi_guard(ThryveStatus::OperationFail, || {
        if group.is_null() {
            return fail(ThryveStatus::NullArg, "group is NULL");
        }

        // Safety: a non-NULL group comes from `thryve_adv_handle_fan_out`.
        let group = *unsafe { Box::from_raw(group as *mut FfiGroup) };
        match group.send_fanout() {
            Ok(()) => ThryveStatus::Ok,
            Err(error) => fail(status_of(&error), get_err_oneline(&error)),
        }
    })
}

// ============================================================================
// Handoffs
// ============================================================================

/// The view behind a handoff, or `None` for NULL, which is recorded.
fn view<'a>(handoff: ThryveHandoff) -> Option<&'a mut HandoffView> {
    if handoff.is_null() {
        set_last_error("handoff is NULL");
        return None;
    }

    // Safety: a non-NULL handoff is the one a job was given, valid while the job runs.
    Some(unsafe { &mut *(handoff as *mut HandoffView) })
}

/// Whether the task (or one of the joined tasks) failed: read the error with
/// [`thryve_handoff_error_code`] and [`thryve_handoff_error_message`]. False for NULL.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_handoff_is_error(handoff: ThryveHandoff) -> bool {
    ffi_guard(false, || {
        view(handoff).is_some_and(|view| view.error().is_some())
    })
}

/// How many values the handoff holds: 1 for a follow-up or a fan-out follower, one per task for a
/// join, and 0 for an error or NULL.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_handoff_value_count(handoff: ThryveHandoff) -> usize {
    ffi_guard(0, || view(handoff).map_or(0, |view| view.values().len()))
}

/// Reads a value in place: index 0 for a follow-up or a fan-out follower, and the task's place in
/// the joined array for a join.
///
/// The pointer is valid until the job returns, unless the job takes the value. A fan-out
/// follower's value is shared with the other followers, so it must not be changed.
///
/// # Returns
///
/// The value, which can itself be NULL. NULL also if `handoff` is NULL, holds an error, or has no
/// value at `index`, or if the value was taken.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_handoff_value(handoff: ThryveHandoff, index: usize) -> *const c_void {
    ffi_guard(std::ptr::null(), || {
        let Some(view) = view(handoff) else {
            return std::ptr::null();
        };

        match view.values().get(index) {
            Some(value) => value.get() as *const c_void,
            None => fail(
                std::ptr::null(),
                format!("the handoff has no value at {index}"),
            ),
        }
    })
}

/// Takes a value out of the handoff: the job owns it from then on, so Thryve won't free it. The
/// job frees it itself, or returns it as its own value with [`thryve_outcome_value`].
///
/// # Returns
///
/// The value, or NULL if `handoff` is NULL, holds an error, has no value at `index`, or belongs to
/// a fan-out follower, whose value is shared and can't be taken. NULL also once it's taken.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_handoff_take_value(handoff: ThryveHandoff, index: usize) -> *mut c_void {
    ffi_guard(std::ptr::null_mut(), || {
        let Some(view) = view(handoff) else {
            return std::ptr::null_mut();
        };

        // Only a follow-up's and a join's values are the job's to take.
        let value = match view {
            HandoffView::Owned(Handoff::Value(value)) if index == 0 => Some(value),
            HandoffView::Joined(JoinHandoff::Filled { values }) => values.get_mut(index),
            HandoffView::Shared(_) => {
                return fail(
                    std::ptr::null_mut(),
                    "a fan-out follower's value is shared with the other followers: it can't be taken",
                );
            }
            _ => None,
        };

        match value {
            Some(value) => value.take(),
            None => fail(
                std::ptr::null_mut(),
                format!("the handoff has no value at {index}"),
            ),
        }
    })
}

/// The error's code: the one the failed task's job gave, or one of Thryve's own, which are
/// negative. 0 if the handoff holds no error, or is NULL.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_handoff_error_code(handoff: ThryveHandoff) -> i32 {
    ffi_guard(0, || {
        view(handoff)
            .and_then(|view| view.error())
            .map_or(0, |error| error.code)
    })
}

/// The error's message (a null-terminated C string), valid until the job returns. NULL if the
/// handoff holds no error, or is NULL.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_handoff_error_message(handoff: ThryveHandoff) -> *const c_char {
    ffi_guard(std::ptr::null(), || {
        view(handoff)
            .and_then(|view| view.error())
            .map_or(std::ptr::null(), |error| error.message.as_ptr())
    })
}

/// For a join that failed: the failed task's place in the joined array, counted from 0. 0 for any
/// other handoff.
#[cfg_attr(not(test), unsafe(no_mangle))]
pub extern "C" fn thryve_handoff_error_position(handoff: ThryveHandoff) -> usize {
    ffi_guard(0, || match view(handoff) {
        Some(HandoffView::Joined(JoinHandoff::Error { position, .. })) => *position,
        _ => 0,
    })
}
