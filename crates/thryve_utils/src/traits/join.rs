// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Joining typed tasks: one follow-up for several of them.
//!
//! [`Joinable`] gives a `Vec` of typed tasks' handles its [`join`](Joinable::join) and
//! [`join_with_priority`](Joinable::join_with_priority), so the handles that splitting work gave
//! back, or that a plugin collected itself, are joined where they are, and the join's handle goes
//! on with [`then`](AdvTaskHandle::then) like any other. [`cancel_all`](Joinable::cancel_all)
//! cancels them all instead.

use crate::errors::ThryveError;
use crate::packets::task::{AdvTaskHandle, declare_join};
use crate::res::JoinHandoff;

/// Handles of typed tasks that can share one follow-up, a join, or be cancelled together.
///
/// Implemented for `Vec<AdvTaskHandle<T, E>>`: the tasks all return `Result<T, E>`, and the join's
/// closure gets their values, or the first error, as a [`JoinHandoff`]. Bring the trait into scope
/// to call its methods.
pub trait Joinable<T, E>
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    /// Declares a join: one follow-up for several tasks, which runs once all of them have finished,
    /// with all their values, or as soon as one of them fails, with its error.
    ///
    /// It's called on the handles of the tasks to join, which each return `Result<T, E>`; each one
    /// fills a slot of the join, in order. The join is sent to the engine now, but it waits there
    /// until those tasks finish or one fails; then its closure runs on a worker with a
    /// [`JoinHandoff`]:
    ///
    /// - [`Filled`](JoinHandoff::Filled) when every task returned a value: the values, in the order of
    ///   the handles;
    /// - [`Error`](JoinHandoff::Error) when one failed: its position among the handles, counted from
    ///   0, and its own error. A join fails fast: it runs as soon as one task fails, without waiting for the
    ///   rest. The tasks it was still waiting for are cancelled: those that haven't started never
    ///   run, and those already running finish, with nobody to take their results: an error is
    ///   written to `Error.log`, and a value is dropped.
    ///
    /// A task that forwards fills its slot with the result of the task it forwarded to, whether it
    /// forwarded before the join was declared or after.
    ///
    /// It uses up the handles and returns the join's own handle, an ordinary [`AdvTaskHandle`], so the
    /// join can be followed with [`then`](AdvTaskHandle::then), or be one of the tasks of another join.
    /// It runs at the highest priority among the tasks it joins, capped just below realtime, so a join
    /// never runs as realtime by inheritance. To give it a priority of its own, use
    /// [`join_with_priority`](Self::join_with_priority).
    ///
    /// Joins of joins split work and split it again, but every piece is a task of its own, with its
    /// own trip through the queue and its own slot, and every join is one more task. Splitting finer
    /// than the work needs adds that overhead without making anything faster: aim for enough pieces to
    /// keep the workers busy, each big enough to be worth a task.
    ///
    /// # Errors
    ///
    /// - [`ThryveError::Join`] if there are no handles;
    /// - [`ThryveError::Naming`] if `task_name` is empty or contains a control character. The handles
    ///   are then released, as if they had been dropped;
    /// - [`ThryveError::EngineNotRunning`] if the engine is gone.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::{bounded, unbounded};
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// use thryve_utils::packets::task::AdvTask;
    /// use thryve_utils::res::JoinHandoff;
    /// use thryve_utils::traits::join::Joinable;
    ///
    /// # let (task_tx, _task_rx) = bounded(4);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let handle = PacketHandle::new(task_tx, notif_tx, unbounded().0, false);
    /// let mut sizes = Vec::new();
    /// for path in ["a.png", "b.png", "c.png"] {
    ///     let file_size = AdvTask::new("FileSize", 2, move || {
    ///         std::fs::metadata(path).map(|meta| meta.len())
    ///     })?;
    ///     sizes.push(handle.send_adv_task(file_size)?);
    /// }
    ///
    /// sizes.join("TotalSize", |joined| match joined {
    ///     // Every file's size, in the order of `sizes`.
    ///     JoinHandoff::Filled { values } => Ok(values.iter().sum::<u64>()),
    ///     // The first `FileSize` that failed, with its own `std::io::Error`.
    ///     JoinHandoff::Error { error, .. } => Err(error),
    /// })?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    #[track_caller]
    fn join<U, E2>(
        self,
        task_name: &str,
        fn_to_exec: impl FnOnce(JoinHandoff<Vec<T>, E>) -> Result<U, E2> + Send + 'static,
    ) -> Result<AdvTaskHandle<U, E2>, ThryveError>
    where
        U: Send + 'static,
        E2: std::error::Error + Send + 'static;

    /// Like [`join`](Self::join), but the join runs at `priority` instead of inheriting the highest
    /// priority among the tasks it joins. It's kept as given, so it can be realtime.
    ///
    /// # Errors
    ///
    /// The same as [`join`](Self::join).
    #[track_caller]
    fn join_with_priority<U, E2>(
        self,
        task_name: &str,
        priority: u8,
        fn_to_exec: impl FnOnce(JoinHandoff<Vec<T>, E>) -> Result<U, E2> + Send + 'static,
    ) -> Result<AdvTaskHandle<U, E2>, ThryveError>
    where
        U: Send + 'static,
        E2: std::error::Error + Send + 'static;

    /// Cancels every task, and uses the handles up: the same as calling
    /// [`cancel`](AdvTaskHandle::cancel) on each one, in order.
    ///
    /// Tasks that haven't started never run. Tasks already running finish, and their results are
    /// let go: an error is still written to `Error.log`, and a value is dropped. Like `cancel`, it
    /// never fails and writes nothing to the logs.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::{bounded, unbounded};
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// use thryve_utils::traits::join::Joinable;
    ///
    /// # let (task_tx, _task_rx) = bounded(8);
    /// # let (notif_tx, _notif_rx) = bounded(1);
    /// # let handle = PacketHandle::new(task_tx, notif_tx, unbounded().0, false);
    /// let rows: Vec<u32> = (0..1_000).collect();
    /// let pieces = handle.split_into(rows, "Row", 8, 2, |chunk, _start| {
    ///     Ok::<u32, std::io::Error>(chunk.iter().sum())
    /// })?;
    ///
    /// // The photo was closed before the rows were processed.
    /// pieces.cancel_all();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    fn cancel_all(self);
}

impl<T, E> Joinable<T, E> for Vec<AdvTaskHandle<T, E>>
where
    T: Send + 'static,
    E: std::error::Error + Send + 'static,
{
    fn join<U, E2>(
        self,
        task_name: &str,
        fn_to_exec: impl FnOnce(JoinHandoff<Vec<T>, E>) -> Result<U, E2> + Send + 'static,
    ) -> Result<AdvTaskHandle<U, E2>, ThryveError>
    where
        U: Send + 'static,
        E2: std::error::Error + Send + 'static,
    {
        declare_join(self, task_name, true, 0, fn_to_exec)
    }

    fn join_with_priority<U, E2>(
        self,
        task_name: &str,
        priority: u8,
        fn_to_exec: impl FnOnce(JoinHandoff<Vec<T>, E>) -> Result<U, E2> + Send + 'static,
    ) -> Result<AdvTaskHandle<U, E2>, ThryveError>
    where
        U: Send + 'static,
        E2: std::error::Error + Send + 'static,
    {
        declare_join(self, task_name, false, priority, fn_to_exec)
    }

    fn cancel_all(mut self) {
        for handle in self.drain(..) {
            handle.cancel();
        }
    }
}
