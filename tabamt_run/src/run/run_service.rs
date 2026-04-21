use crate::service::service::TaBaMT;
use tabamt_utils::cfg::{config::Config, ctx::Ctx};

/// Creates an instance of the TaBaMT service and the context required for its execution.
/// It returns a tuple containing the initialized TaBaMT service and the Ctx with the provided Config.
pub fn create_service<'a> (config: Config) -> (TaBaMT<'a> , Ctx) {
    TaBaMT::new(config)
}

/// Attempts to run the TaBaMT service with the provided Ctx.
/// This is the main entry point if the developer decides to run the program as a service.
pub fn try_run_as_service(mut svc: TaBaMT, ctx: Ctx){
    svc.run_service(ctx);
}