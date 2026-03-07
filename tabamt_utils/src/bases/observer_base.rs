// Implementation for the observer pattern.
// Observers can be loggers, audits, email sendings, etc.... Each needs to be implemented alone.

use std::sync::Arc;

use crate::extras::named_input::NamedInput;
use crate::packets::task::Job;
use crate::res;
use crate::traits::observer::{ObsB , Observer};

pub struct ObsBase {
    pub obs_name: String,
    pub obs_type: NamedInput,
    pub obs_core: Arc<dyn Observer>,
    pub fn_to_run: Job
}

impl ObsB for ObsBase{}

impl ObsBase {
    pub fn new(obs_name: &str , obs_type: &str , obs_core: Arc<dyn Observer>, func: Job) -> Self{
        Self {
            obs_name: obs_name.to_string(),
            obs_type: NamedInput::new(obs_type),
            obs_core,
            fn_to_run: func
        }
    }

    pub fn fn_to_run(&mut self) -> Job {
        let temp: Job = self.default_fn();
        std::mem::replace(&mut self.fn_to_run , temp)
    }

    fn default_fn(&self) -> Job{
        let job: Job = Box::new(move || {
            res::Result::new("BIGERR", "If This Is Ever Called, Something is Wrong".to_string(), "Main")
        });

        job
    }
}