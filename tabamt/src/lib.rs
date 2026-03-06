// re-export all crates to be used by the devs.
pub use tabamt_run::run;


pub mod utils {
    pub mod traits{
        pub use tabamt_utils::traits::observer;
        pub use tabamt_utils::traits::plugin;
        pub use tabamt_utils::traits::notif_ex;
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
    pub use tabamt_utils::extras::named_input::NamedInput;
    pub use tabamt_utils::res;
}