//! Registry authority for shared acquisition and independent demand ownership.
use super::{context::CallContext, protocol::AcquisitionPlan, routing::RoutingFuture};
use crate::live::model::{
    cursor::ConnectionGeneration,
    demand::{DemandOwner, StreamHandle},
    derived::StreamRef,
    error::ControlError,
    identity::{SourceStreamKey, StreamId},
    operation::{ConnectionId, ResourceVersion, SubscriptionId},
    snapshot::{AcquisitionReadiness, SourceSnapshot},
};
pub trait SourceRegistryPort: Send + Sync {
    fn acquire<'a>(
        &'a self,
        context: &'a CallContext,
        source: SourceStreamKey,
        owner: DemandOwner,
    ) -> RoutingFuture<'a, Result<StreamHandle, ControlError>>;
    fn release<'a>(
        &'a self,
        context: &'a CallContext,
        handle: StreamHandle,
        owner: DemandOwner,
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
    fn snapshot<'a>(
        &'a self,
        context: &'a CallContext,
        stream: StreamId,
    ) -> RoutingFuture<'a, Result<SourceSnapshot, ControlError>>;
}
pub trait DerivedRegistryPort: Send + Sync {
    fn dependencies<'a>(
        &'a self,
        context: &'a CallContext,
        stream: StreamRef,
    ) -> RoutingFuture<'a, Result<Vec<StreamRef>, ControlError>>;
}
pub trait SubscriptionPort: Send + Sync {
    /// Completion here confirms subscription ownership, not processor readiness.
    fn subscribe<'a>(
        &'a self,
        context: &'a CallContext,
        stream: ResourceVersion,
        plan: AcquisitionPlan,
    ) -> RoutingFuture<'a, Result<SubscriptionId, ControlError>>;
    fn unsubscribe<'a>(
        &'a self,
        context: &'a CallContext,
        subscription: SubscriptionId,
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
}
pub trait ConnectionControlPort: Send + Sync {
    fn restart<'a>(
        &'a self,
        context: &'a CallContext,
        connection: ConnectionId,
        generation: ConnectionGeneration,
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
}
pub trait ReadinessPort: Send + Sync {
    fn readiness(&self, stream: &ResourceVersion) -> Result<AcquisitionReadiness, ControlError>;
}

pub trait ConnectionWriterPort: Send + Sync {
    fn send<'a>(
        &'a self,
        context: &'a CallContext,
        connection: ConnectionId,
        generation: ConnectionGeneration,
        frame: super::transport::WebSocketFrame,
    ) -> RoutingFuture<'a, Result<(), ControlError>>;
}

/// Runtime configuration execution is distinct from acquisition claims. A
/// deactivation releases only the persistent claim; removal must fence and
/// validate all remaining demand/dependency owners atomically.
pub trait SourceConfigurationPort: Send + Sync {
    fn configure<'a>(
        &'a self,
        context: &'a CallContext,
        source: crate::live::model::configuration::ResolvedSource,
        revision: crate::live::model::configuration::ConfigRevisionId,
    ) -> RoutingFuture<'a, Result<crate::live::model::operation::OperationStatus, ControlError>>;
    fn remove<'a>(
        &'a self,
        context: &'a CallContext,
        source: ResourceVersion,
    ) -> RoutingFuture<'a, Result<crate::live::model::operation::OperationStatus, ControlError>>;
}
pub trait DerivedConfigurationPort: Send + Sync {
    fn configure<'a>(
        &'a self,
        context: &'a CallContext,
        derived: crate::live::model::configuration::ResolvedDerived,
        revision: crate::live::model::configuration::ConfigRevisionId,
    ) -> RoutingFuture<'a, Result<crate::live::model::operation::OperationStatus, ControlError>>;
    fn remove<'a>(
        &'a self,
        context: &'a CallContext,
        derived: ResourceVersion,
    ) -> RoutingFuture<'a, Result<crate::live::model::operation::OperationStatus, ControlError>>;
}
