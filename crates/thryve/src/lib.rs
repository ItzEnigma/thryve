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

pub mod utils {
    pub mod traits {
        pub use thryve_utils::traits::notif_logic::NotifLogic;
        pub use thryve_utils::traits::observer_logic::ObsLogic;
        pub use thryve_utils::traits::plugin_logic::PlgLogic;
        pub use thryve_utils::traits::queue::Queue;
    }
    pub mod cfg {
        pub use thryve_utils::cfg::config::Config;
        pub use thryve_utils::cfg::ctx;
    }
    pub mod packets {
        pub use thryve_utils::packets::notif_log;
        pub use thryve_utils::packets::packet_handle;
        pub use thryve_utils::packets::task;
    }
    pub mod bases {
        pub use thryve_utils::bases::notif;
        pub use thryve_utils::bases::observer;
        pub use thryve_utils::bases::plugin;
    }
    pub use thryve_utils::extras::{jobs, named_input::NamedInput};
    pub use thryve_utils::impl_as_any;
    pub use thryve_utils::res;
}
