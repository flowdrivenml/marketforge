//! Recovery orchestration controls are separate from data transport.
use super::{context::CallContext, routing::RoutingFuture};
use crate::live::model::{
    error::ControlError,
    operation::{OperationStatus, ResourceVersion},
    snapshot::{CursorInterval, RecoveryCategory},
};
#[derive(Debug, Clone)]
pub struct RecoveryRequest {
    pub target: ResourceVersion,
    pub category: RecoveryCategory,
    pub interval: Option<CursorInterval>,
}
pub trait RecoveryPort: Send + Sync {
    fn request<'a>(
        &'a self,
        context: &'a CallContext,
        request: RecoveryRequest,
    ) -> RoutingFuture<'a, Result<OperationStatus, ControlError>>;
    fn status<'a>(
        &'a self,
        context: &'a CallContext,
        target: ResourceVersion,
    ) -> RoutingFuture<'a, Result<Vec<OperationStatus>, ControlError>>;
}
