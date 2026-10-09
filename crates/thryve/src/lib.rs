// Thryve — Copyright (c) 2026 Youssef Mostafa Abdellatif
//
// This file is part of Thryve, dual-licensed under the GNU Affero General
// Public License v3.0 (or later) and a separate Commercial License.
// See LICENSE and COMMERCIAL.md in the repository root, or visit
// https://thryve-framework.com for licensing options.
//
// SPDX-License-Identifier: AGPL-3.0-or-later

#![doc = include_str!("../README.md")]

// re-export all crates to be used by the devs.
pub use thryve_run::run;

/// Everything an application uses from Rust: base types, packets, configuration, errors and
/// helpers.
pub mod utils {
    /// The traits your plugin, observer and notification logic implement, and the queue trait.
    pub mod traits {
        pub use thryve_utils::traits::notif_logic::NotifLogic;
        pub use thryve_utils::traits::observer_logic::ObsLogic;
        pub use thryve_utils::traits::plugin_logic::PlgLogic;
        pub use thryve_utils::traits::queue::Queue;
    }
    /// Configuration and the application context.
    pub mod cfg {
        pub use thryve_utils::cfg::config::Config;
        pub use thryve_utils::cfg::ctx;
    }
    /// Tasks, log notifications, and the handle that sends them to the engine.
    pub mod packets {
        pub use thryve_utils::packets::notif_log;
        pub use thryve_utils::packets::packet_handle;
        pub use thryve_utils::packets::task;
    }
    /// Plugins, observers and notifications.
    pub mod bases {
        pub use thryve_utils::bases::notif;
        pub use thryve_utils::bases::observer;
        pub use thryve_utils::bases::plugin;
    }
    /// The errors Thryve returns, and helpers that render an error together with its causes.
    pub mod error {
        pub use thryve_utils::errors::ConfigError;
        pub use thryve_utils::errors::ConfigViolations;
        pub use thryve_utils::errors::EngineNotRunningError;
        pub use thryve_utils::errors::FinalizationError;
        pub use thryve_utils::errors::NameInputType;
        pub use thryve_utils::errors::NamingError;
        pub use thryve_utils::errors::RuleError;
        pub use thryve_utils::errors::StartupError;
        pub use thryve_utils::errors::TaskError;
        pub use thryve_utils::errors::ThryveError;
        pub use thryve_utils::errors::get_err;
        pub use thryve_utils::errors::get_err_oneline;
    }
    pub use thryve_utils::extras::jobs;
    pub use thryve_utils::impl_as_any;
    pub use thryve_utils::res;
}
