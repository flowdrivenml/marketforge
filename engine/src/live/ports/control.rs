//! Management boundary. Authentication is supplied by the session owner.
use super::{context::CallContext, routing::RoutingFuture};
use crate::live::model::{error::ControlError, protocol::*};
pub trait ManagementPort: Send + Sync {
    fn execute<'a>(
        &'a self,
        context: &'a CallContext,
        peer: &'a AuthenticatedPeer,
        command: ControlCommand,
    ) -> RoutingFuture<'a, Result<ControlResult, ControlError>>;
}
/// The transport owns framing/correlation; a client cannot choose its peer grants.
pub trait ManagementClientPort: Send + Sync {
    fn request<'a>(
        &'a self,
        context: &'a CallContext,
        request: ControlRequest,
    ) -> RoutingFuture<'a, Result<ControlResponse, ControlError>>;
}
