use std::time::SystemTime;

use crate::any_naming_input::ANNamingInput;

pub trait Notif: Send + Sync {
    fn get_as_any(&self) -> &dyn std::any::Any;
    fn get_event_type(&self) -> &ANNamingInput;
    fn get_time_stamp(&self) -> &SystemTime;
    fn get_send_to(&self) -> &ANNamingInput;
}