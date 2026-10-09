// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

//! Application context structure for the Thryve system.
//!
//! This module provides the [`Ctx`] struct, which serves as the central
//! container for configuration and shared resources throughout the
//! application lifecycle. The context is passed to components that need
//! access to configuration, communication channels, or registered
//! observers and plugins.
//!
//! # Purpose
//!
//! The `Ctx` structure bundles all essential resources into a single
//! entity, eliminating the need to pass multiple parameters through
//! the initialization chain. This design promotes cleaner APIs and
//! easier dependency injection.
//!
//! # Contents
//!
//! - Configuration from [`Config`]
//! - Receivers for tasks and notifications
//! - Packet handle for engine communication
//! - Collections for observers and plugins during initialization
//! - Log file names for the logger observer

use crossbeam::channel::Receiver;
use std::sync::Arc;
use std::time::SystemTime;

use crate::bases::notif::Notif;
use crate::bases::observer::Observer;
use crate::bases::plugin::Plugin;
use crate::cfg::config::Config;
use crate::cfg::meta_data::MetaData;
use crate::errors::{ThryveError, get_err_oneline};
use crate::extras::{
    log_format::{log_line, write_line},
    named_input::LogFileNamedInput,
};
use crate::packets::packet_handle::StatusMsg;
use crate::packets::{packet_handle::PacketHandle, task::Task};

/// Central context structure for the Thryve application.
///
/// The `Ctx` struct bundles all essential resources into a single entity,
/// serving as the primary mechanism for dependency injection throughout
/// the application. It is created during initialization and passed to
/// components that need access to configuration or shared resources.
///
/// # Fields
///
/// - `config`: Application configuration
/// - `task_rx`: Receiver for incoming tasks
/// - `notif_rx`: Receiver for incoming notifications
/// - `packet_handle`: Handle for sending to the engine
/// - `observers`: Collection of observers during initialization
/// - `plugins`: Collection of plugins during initialization
/// - `logging_file_names`: Log file names for the logger observer
///
/// # Example
///
/// ```no_run
/// use thryve_utils::cfg::ctx::Ctx;
///
/// // Context is typically created during application initialization
/// // and passed to the engine or service for execution
/// ```
pub struct Ctx {
    /// Application configuration.
    pub config: Config,
    /// Receiver for incoming tasks.
    pub task_rx: Arc<Receiver<Task>>,
    /// Receiver for incoming notifications.
    pub notif_rx: Arc<Receiver<Notif>>,
    /// Receiver for Thryve's own messages to the engine (status lines, and the releases of dropped
    /// task handles), from an unbounded channel.
    pub status_rx: Arc<Receiver<StatusMsg>>,
    /// Handle for sending to the engine (private).
    packet_handle: PacketHandle,
    /// Collection of observers during initialization.
    pub observers: Vec<Observer>,
    /// Collection of plugins during initialization.
    pub plugins: Vec<Plugin>,
    /// Log file names for the logger observer.
    pub logging_file_names: Vec<String>,
    /// Data collected for proper initialization.
    pub meta_data: MetaData,
}

impl Ctx {
    /// Creates a new context with the specified configuration and channels.
    ///
    /// Initializes a new `Ctx` instance with the given configuration,
    /// task receiver, notification receiver, and packet handle. The
    /// observers and plugins collections start empty and are populated
    /// during the initialization phase.
    ///
    /// # Arguments
    ///
    /// * `config` - Application configuration
    /// * `task_rx` - Receiver for incoming tasks
    /// * `notif_rx` - Receiver for incoming notifications
    /// * `status_rx` - Receiver for Thryve's own messages; it must belong to the same channel as
    ///   the status sender in `packet_handle`, or the status lines never reach the logger and
    ///   dropped task handles are never released
    /// * `packet_handle` - Handle for sending to the engine
    ///
    /// # Returns
    ///
    /// A new `Ctx` instance with empty observer and plugin collections.
    pub fn new(
        config: Config,
        task_rx: Receiver<Task>,
        notif_rx: Receiver<Notif>,
        status_rx: Receiver<StatusMsg>,
        packet_handle: PacketHandle,
    ) -> Self {
        Self {
            config,
            task_rx: Arc::new(task_rx),
            notif_rx: Arc::new(notif_rx),
            status_rx: Arc::new(status_rx),
            packet_handle,
            observers: vec![],
            plugins: vec![],
            logging_file_names: vec![],
            meta_data: MetaData::default(),
        }
    }

    /// Extracts and returns all registered observers.
    ///
    /// Returns the complete list of observers and clears the internal
    /// collection. This method is typically called once during
    /// initialization to transfer observers to the engine.
    ///
    /// # Returns
    ///
    /// A vector containing all registered observers.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::cfg::config::Config;
    /// # use thryve_utils::cfg::ctx::Ctx;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// #
    /// # let config = Config::default();
    /// # let (task_tx, task_rx) = bounded(config.pipes.task_pipe_capacity);
    /// # let (notif_tx, notif_rx) = bounded(config.pipes.notif_pipe_capacity);
    /// # let (status_tx, status_rx) = crossbeam::channel::unbounded();
    /// # let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
    /// # let mut ctx = Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle);
    /// // During engine initialization
    /// for observer in ctx.observers() {
    ///     // load the observer into the engine's observer manager
    /// }
    /// ```
    pub fn observers(&mut self) -> Vec<Observer> {
        std::mem::take(&mut self.observers)
    }

    /// Extracts and returns all registered plugins.
    ///
    /// Returns the complete list of plugins and clears the internal
    /// collection. This method is typically called once during
    /// initialization to transfer plugins to the engine.
    ///
    /// # Returns
    ///
    /// A vector containing all registered plugins.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::cfg::config::Config;
    /// # use thryve_utils::cfg::ctx::Ctx;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// #
    /// # let config = Config::default();
    /// # let (task_tx, task_rx) = bounded(config.pipes.task_pipe_capacity);
    /// # let (notif_tx, notif_rx) = bounded(config.pipes.notif_pipe_capacity);
    /// # let (status_tx, status_rx) = crossbeam::channel::unbounded();
    /// # let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
    /// # let mut ctx = Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle);
    /// // During engine initialization
    /// for plugin in ctx.plugins() {
    ///     // load the plugin into the engine's plugin manager
    /// }
    /// ```
    pub fn plugins(&mut self) -> Vec<Plugin> {
        std::mem::take(&mut self.plugins)
    }

    /// Returns a clone of the packet handle.
    ///
    /// Provides convenient access to the packet handle without exposing
    /// it as a public field. The packet handle is cloneable, making
    /// this operation inexpensive.
    ///
    /// # Returns
    ///
    /// A clone of the internal packet handle.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use crossbeam::channel::bounded;
    /// # use thryve_utils::cfg::config::Config;
    /// # use thryve_utils::cfg::ctx::Ctx;
    /// # use thryve_utils::packets::packet_handle::PacketHandle;
    /// # use thryve_utils::packets::task::Task;
    /// # use thryve_utils::res::TaskResult;
    /// #
    /// # let config = Config::default();
    /// # let (task_tx, task_rx) = bounded(config.pipes.task_pipe_capacity);
    /// # let (notif_tx, notif_rx) = bounded(config.pipes.notif_pipe_capacity);
    /// # let (status_tx, status_rx) = crossbeam::channel::unbounded();
    /// # let packet_handle = PacketHandle::new(task_tx, notif_tx, status_tx, false);
    /// # let ctx = Ctx::new(config, task_rx, notif_rx, status_rx, packet_handle);
    /// # let my_task = Task::new("Example", 0, || TaskResult::new("", "".to_string(), "")).expect("valid task name");
    /// let handle = ctx.packet_handle();
    /// handle.send_task(my_task)?;
    /// # Ok::<(), thryve_utils::errors::ThryveError>(())
    /// ```
    pub fn packet_handle(&self) -> PacketHandle {
        self.packet_handle.clone()
    }

    /// Writes an error straight to `Error.log`, without going through the logger.
    ///
    /// This is for reporting a failed startup. The context only lives during startup, and at
    /// that point the logger either hasn't started yet or the cleanup has already stopped it.
    /// While the logger runs, send it a log notification instead, so that each file has only one
    /// writer.
    ///
    /// The line uses the logger's format with the source `STARTUP`. Its message is the error with
    /// its whole chain of causes, as [`get_err_oneline`] renders it:
    ///
    /// ```text
    /// Log_Creation: 2024-01-15 10:30:45, [ STARTUP ], Event_Type: Error, Message: failed to start up: failed to start thread: 'Logging': ...
    /// ```
    ///
    /// Nothing is written until `init_directories` has stored the log files in the context. If
    /// `Error.log` can't be written, the line goes to stderr instead; a failure there is ignored,
    /// because reporting an error must never cause a new one.
    ///
    /// # Arguments
    ///
    /// * `err` - The error to write
    pub fn e_log(&self, err: &ThryveError) {
        let error_log = self
            .meta_data
            .dir_data
            .log_hash_map
            .get(&LogFileNamedInput::new_err());

        if let Some(file) = error_log {
            let line = log_line(SystemTime::now(), "STARTUP", "Error", &get_err_oneline(err));
            write_line(file, &line);
        }
    }
}
