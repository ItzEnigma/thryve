use std::sync::Arc;

use crate::extras::{named_input::NamedInput , jobs::TaskJob};
use crate::packets::packet_handle::PacketHandle;
use crate::res;
use crate::traits::plugin_logic::{PlgLogic, PluginB};

pub struct Plugin {
    pub plg_name: String,
    pub plg_type: NamedInput,
    pub plg_core: Arc<dyn PlgLogic>,
    pub tx: PacketHandle,

    initial_fn_to_run: TaskJob
}

impl PluginB for Plugin {
    fn clean_up(&self) {
        let x = Arc::clone(&self.plg_core);
        x.clean_up();
    }
}

impl Plugin {
    pub fn new(plg_name: &str , plg_type: &str , plg_core: Arc<dyn PlgLogic> , tx: PacketHandle , job: TaskJob) -> Self {
        Self {
            plg_name: plg_name.to_string(),
            plg_type: NamedInput::new(plg_type),
            plg_core,
            tx,
            initial_fn_to_run: job
        }
    }

    pub fn initial_fn_to_run(&mut self) -> TaskJob {
        let temp: TaskJob = self.default_fn();
        std::mem::replace(&mut self.initial_fn_to_run , temp)
    }

    fn default_fn(&self) -> TaskJob{
        let job: TaskJob = Box::new(move || {
            res::Result::new("BIGERR", "If This Is Ever Called, Something is Wrong".to_string(), "Main")
        });

        job
    }
}