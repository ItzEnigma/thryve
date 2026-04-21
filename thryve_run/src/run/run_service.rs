use crate::service::service::Thryve;
use thryve_utils::cfg::{config::Config, ctx::Ctx};

/// Creates an instance of the Thryve service and the context required for its execution.
/// It returns a tuple containing the initialized Thryve service and the Ctx with the provided Config.
pub fn create_service<'a> (config: Config) -> (Thryve<'a> , Ctx) {
    Thryve::new(config)
}

/// Attempts to run the Thryve service with the provided Ctx.
/// This is the main entry point if the developer decides to run the program as a service.
pub fn try_run_as_service(mut svc: Thryve, ctx: Ctx){
    svc.run_service(ctx);
}