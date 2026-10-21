//! Supervision and deadline shutdown contracts. An acknowledgement of stop is
//! distinct from joined tasks and settled delivery obligations.
use super::{context::CallContext, routing::RoutingFuture};
use crate::live::model::{
    configuration::ConfigRevisionId, cursor::StreamCursor, error::ControlError,
    operation::ResourceVersion,
};
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnfinishedWork {
    pub owner: ResourceVersion,
    pub first: Option<StreamCursor>,
    pub last: Option<StreamCursor>,
    pub events: u64,
    pub bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShutdownReport {
    pub joined: bool,
    pub unfinished: Vec<UnfinishedWork>,
    pub errors: Vec<ControlError>,
}
impl ShutdownReport {
    pub fn complete(&self) -> bool {
        self.joined && self.unfinished.is_empty() && self.errors.is_empty()
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerUpdate {
    AdoptRevision {
        revision: ConfigRevisionId,
        settings: OwnerSettings,
    },
    Fence {
        newer_generation: u64,
    },
    StopAdmission,
    Drain,
}
pub trait OwnerLifecyclePort: Send + Sync {
    fn update<'a>(
        &'a self,
        context: &'a CallContext,
        update: OwnerUpdate,
    ) -> RoutingFuture<'a, Result<UpdateAcknowledgement, ControlError>>;
    /// Return at the deadline, reporting all unfinished work. Joined=false must
    /// not be reported as a clean shutdown, even if cancellation was requested.
    fn shutdown<'a>(&'a self, context: &'a CallContext) -> RoutingFuture<'a, ShutdownReport>;
}

pub trait TaskHandlePort: Send + Sync {
    fn cancel(&self);
    fn join<'a>(&'a self, context: &'a CallContext) -> RoutingFuture<'a, Result<(), ControlError>>;
}
pub trait TaskSpawnerPort: Send + Sync {
    /// The owner registers the task before scheduling. Implementations reject
    /// resource exhaustion and preserve a join handle for deadline shutdown.
    fn spawn(
        &self,
        owner: ResourceVersion,
        task: RoutingFuture<'static, Result<(), ControlError>>,
    ) -> Result<std::sync::Arc<dyn TaskHandlePort>, ControlError>;
}

/// Owners accept only their supported settings variant and declared boundary;
/// unsupported hot changes return UnsupportedCapability without mutation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnerSettings {
    Logging(crate::live::model::configuration::LogLevel),
    Metrics(crate::live::model::configuration::MetricsSettings),
    Recording(crate::live::model::configuration::RecordingSettings),
    Source(crate::live::model::configuration::ResolvedSource),
    Derived(crate::live::model::configuration::ResolvedDerived),
    SubscriberLimit(u32),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateProgress {
    Accepted,
    Adopted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateAcknowledgement {
    pub owner: ResourceVersion,
    pub revision: Option<ConfigRevisionId>,
    pub progress: UpdateProgress,
    pub boundary: crate::live::model::configuration::ReconfigurationBoundary,
}
