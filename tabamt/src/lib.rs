// re-export all crates to be used by the devs.
pub use tabamt_run::run;


pub mod utils {
    pub mod traits{
        pub use tabamt_utils::traits::observer_logic::ObsLogic;
        pub use tabamt_utils::traits::plugin_logic::PlgLogic;
        pub use tabamt_utils::traits::notif_logic::NotifLogic;
    }
    pub mod cfg{
        pub use tabamt_utils::cfg::config::{load_config_from_file , load_def_config};
        pub use tabamt_utils::cfg::ctx;
    }
    pub mod packets{
        pub use tabamt_utils::packets::notif_log;
        pub use tabamt_utils::packets::packet_handle;
        pub use tabamt_utils::packets::task;
    }
    pub mod bases{
        pub use tabamt_utils::bases::notif;
        pub use tabamt_utils::bases::observer;
        pub use tabamt_utils::bases::plugin;
    }
    pub use tabamt_utils::extras::{named_input::NamedInput , jobs};
    pub use tabamt_utils::res;
    pub use tabamt_utils::impl_as_any;
}