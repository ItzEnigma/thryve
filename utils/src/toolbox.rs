use std::fmt::Display;

use strum_macros::EnumIter;

use crate::notification::Notification;

#[derive(PartialEq, Eq , Hash, Copy , Clone , EnumIter)]
pub enum ObserverToolboxType {
    Logger
}

impl std::str::FromStr for ObserverToolboxType {
    type Err = Notification;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Logger" => Ok(ObserverToolboxType::Logger),
            _ => Err(Notification::err("Input String Is Invalid".to_string())),
        }
    }
}

impl Display for ObserverToolboxType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ObserverToolboxType::Logger => write!(f, "Logger"),
        }
    }
}

#[derive(PartialEq, Eq , Hash, Copy , Clone , EnumIter)]
pub enum PluginToolboxType {
    // ClamAV,
    // Realtime,
    // Static,
    // Dynamic,
    // Amsi,
    Test
}

impl std::str::FromStr for PluginToolboxType {
    type Err = Notification;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Test" => Ok(PluginToolboxType::Test),
            _ => Err(Notification::err("Input String Is Invalid".to_string())),
        }
    }
}

impl Display for PluginToolboxType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PluginToolboxType::Test => write!(f, "Test"),
        }
    }
}