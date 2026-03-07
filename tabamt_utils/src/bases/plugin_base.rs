use std::sync::Arc;

use crate::extras::named_input::NamedInput;
use crate::packets::packet_handle::PacketHandle;
use crate::packets::task::Job;
use crate::res;
use crate::traits::plugin::{Plugin, PluginB};

pub struct PluginBase {
    pub plg_name: String,
    plg_type: NamedInput,
    pub plg_core: Arc<dyn Plugin>,
    pub tx: PacketHandle,

    initial_fn_to_run: Job
}

impl PluginB for PluginBase {
    fn clean_up(&self) {
        let x = Arc::clone(&self.plg_core);
        x.clean_up();
    }
}

impl PluginBase {
    pub fn new(plg_name: &str , plg_type: &str , plg_core: Arc<dyn Plugin> , tx: PacketHandle , job: Job) -> Self {
        Self {
            plg_name: plg_name.to_string(),
            plg_type: NamedInput::new(plg_type),
            plg_core,
            tx,
            initial_fn_to_run: job
        }
    }

    pub fn plg_type(&self) -> &str {
        &self.plg_type.inner()
    }

    pub fn initial_fn_to_run(&mut self) -> Job {
        let temp: Job = self.default_fn();
        std::mem::replace(&mut self.initial_fn_to_run , temp)
    }

    fn default_fn(&self) -> Job{
        let job: Job = Box::new(move || {
            res::Result::new("BIGERR", "If This Is Ever Called, Something is Wrong".to_string(), "Main")
        });

        job
    }
}