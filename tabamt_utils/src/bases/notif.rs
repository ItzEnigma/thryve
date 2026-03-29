use crate::impl_as_any;
use crate::packets::task::Task;
use crate::traits::notif_logic::NotifLogic;
use crate::extras::named_input::NamedInput;
use std::time::SystemTime;

pub struct Notif{
    pub notif_type: NamedInput,              // The type of event                        | Read Only
    pub notif_receiver: NamedInput,                 // The module to send the notification to   | Read Only
    pub notif_core: Box<dyn NotifLogic>,
    pub timestamp: SystemTime,                  // The time the event was created           | Read Only
    task: Option<Task>,                     // The task associated with the log         | Read Only
    notification: Option<Box<Notif>>,    // An optional nested notification          | Read Only
}

impl NotifLogic for Notif {
    impl_as_any!(Notif);
}

impl Notif {
    pub fn new(notif_type: &str , notif_receiver: &str , notif_core: Box<dyn NotifLogic>) -> Self {
        Self {
            notif_type: NamedInput::new(notif_type),
            notif_receiver: NamedInput::new(notif_receiver),
            notif_core,
            timestamp: SystemTime::now(),
            task: None,
            notification: None
        }
    }

    // Function to get the task.
    // @return a reference to an Option<Task>.
    pub fn task(&self) -> &Option<Task>{
        &self.task
    }

    // Function to get the notification.
    // @return a reference to an Option<Box<Log>>.
    pub fn notification(&self) -> &Option<Box<Notif>>{
        &self.notification
    }
}