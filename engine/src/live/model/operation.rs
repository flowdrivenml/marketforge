//! Process-scoped management identities and honest completion objectives.
use super::{
    configuration::ConfigRevisionId,
    demand::SubscriberId,
    identity::{DerivedStreamId, ExchangeId, StreamId},
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RuntimeInstanceId(pub String);
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct OperationId {
    pub runtime: RuntimeInstanceId,
    pub sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionId {
    pub runtime: RuntimeInstanceId,
    pub sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConnectionId {
    pub runtime: RuntimeInstanceId,
    pub sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SubscriptionId {
    pub connection: ConnectionId,
    pub sequence: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", content = "id", rename_all = "snake_case")]
pub enum ResourceId {
    Runtime,
    Exchange(ExchangeId),
    Connection(ConnectionId),
    Subscription(SubscriptionId),
    Source(StreamId),
    Derived(DerivedStreamId),
    Subscriber(SubscriberId),
    Recording(StreamId),
    Retention(StreamId),
    Recovery(OperationId),
    Configuration,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceVersion {
    pub resource: ResourceId,
    pub generation: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletionObjective {
    DesiredPersisted,
    RuntimeApplied,
    AcquisitionReady,
    Removed,
    RecordingCommitted,
    ProtectionReleased,
    RecoveryVerified,
    Stopped,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CompletionEvidence {
    pub objective: CompletionObjective,
    pub target: ResourceVersion,
    pub revision: Option<ConfigRevisionId>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationState {
    Accepted,
    Applying,
    Completed,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationStatus {
    pub id: OperationId,
    pub request: super::protocol::RequestId,
    pub target: ResourceVersion,
    pub objective: CompletionObjective,
    pub requested_revision: Option<ConfigRevisionId>,
    pub state: OperationState,
    pub created_at_ns: i64,
    pub started_at_ns: Option<i64>,
    pub completed_at_ns: Option<i64>,
    pub completion: Option<CompletionEvidence>,
    pub error: Option<super::error::ControlError>,
}
impl OperationStatus {
    pub fn start(&mut self, now: i64) -> Result<(), super::error::ErrorCode> {
        if self.state != OperationState::Accepted {
            return Err(super::error::ErrorCode::InvalidTransition);
        }
        self.state = OperationState::Applying;
        self.started_at_ns = Some(now);
        Ok(())
    }
    pub fn complete(
        &mut self,
        evidence: CompletionEvidence,
        now: i64,
    ) -> Result<(), super::error::ErrorCode> {
        if !matches!(
            self.state,
            OperationState::Accepted | OperationState::Applying
        ) || evidence.objective != self.objective
            || evidence.target != self.target
            || evidence.revision != self.requested_revision
        {
            return Err(super::error::ErrorCode::InvalidCompletion);
        }
        self.state = OperationState::Completed;
        self.completed_at_ns = Some(now);
        self.completion = Some(evidence);
        Ok(())
    }
    pub fn terminate(
        &mut self,
        error: super::error::ControlError,
        cancelled: bool,
        now: i64,
    ) -> Result<(), super::error::ErrorCode> {
        if !matches!(
            self.state,
            OperationState::Accepted | OperationState::Applying
        ) {
            return Err(super::error::ErrorCode::InvalidTransition);
        }
        self.state = if cancelled {
            OperationState::Cancelled
        } else {
            OperationState::Failed
        };
        self.error = Some(error);
        self.completed_at_ns = Some(now);
        Ok(())
    }
}

impl OperationStatus {
    pub fn validate(&self) -> Result<(), super::error::ErrorCode> {
        use super::error::ErrorCode;
        if self.id.runtime.0.trim().is_empty() || self.request.client_instance.trim().is_empty() {
            return Err(ErrorCode::InvalidInput);
        }
        let valid = match self.state {
            OperationState::Accepted => {
                self.started_at_ns.is_none()
                    && self.completed_at_ns.is_none()
                    && self.completion.is_none()
                    && self.error.is_none()
            }
            OperationState::Applying => {
                self.started_at_ns.is_some()
                    && self.completed_at_ns.is_none()
                    && self.completion.is_none()
                    && self.error.is_none()
            }
            OperationState::Completed => {
                self.completed_at_ns.is_some()
                    && self.error.is_none()
                    && self.completion.as_ref().is_some_and(|e| {
                        e.objective == self.objective
                            && e.target == self.target
                            && e.revision == self.requested_revision
                    })
            }
            OperationState::Failed | OperationState::Cancelled => {
                self.completed_at_ns.is_some() && self.completion.is_none() && self.error.is_some()
            }
        };
        if valid {
            Ok(())
        } else {
            Err(ErrorCode::InvalidCompletion)
        }
    }
}
