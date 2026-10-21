//! Single v1 management vocabulary. Queries, persisted configuration mutations,
//! and session-owned operational demand are deliberately separate commands.
use super::{
    configuration::*, delivery::DeliveryCapacity, demand::StreamHandle, derived::StreamRef,
    error::*, identity::*, observation::*, operation::*, snapshot::*,
};
use serde::{Deserialize, Serialize};
pub const CONTROL_PROTOCOL_VERSION: u16 = 1;
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RequestId {
    pub client_instance: String,
    pub sequence: u64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    Monitor,
    Configure,
    ManageStreams,
    ManageRecording,
    ManageRetention,
    RequestRecovery,
    ManageRuntime,
}
/// Populated by the authenticated Unix-session owner, never decoded from requests.
#[derive(Debug, Clone)]
pub struct AuthenticatedPeer {
    pub session: SessionId,
    pub uid: u32,
    pub grants: Vec<Permission>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Query {
    GetRuntimeStatus,
    ListSourceStreams,
    GetSourceStream {
        stream: StreamId,
    },
    ListDerivedStreams,
    GetDerivedStream {
        stream: DerivedStreamId,
    },
    GetDerivedDependencies {
        stream: DerivedStreamId,
    },
    ListConnections,
    GetConnection {
        connection: ConnectionId,
    },
    ListSubscribers,
    GetSubscriber {
        subscriber: super::demand::SubscriberId,
    },
    GetConfiguration,
    ListConfigurationHistory,
    DiffConfiguration {
        before: ConfigRevisionId,
        after: ConfigRevisionId,
    },
    GetEffectiveConfiguration,
    GetConfigurationRevision {
        revision: ConfigRevisionId,
    },
    GetMetrics {
        resource: Option<ResourceId>,
    },
    GetHealth {
        resource: ResourceId,
    },
    GetRecordingStatus {
        stream: Option<StreamId>,
    },
    GetRecordingCoverage {
        stream: StreamId,
        range: CursorInterval,
    },
    ListRecordingGaps {
        stream: StreamId,
    },
    GetRetentionStatus {
        stream: StreamId,
    },
    GetRetentionCoverage {
        stream: StreamId,
        range: CursorInterval,
    },
    GetRetentionResourceUsage,
    ListRetentionSessions,
    GetRetentionSession {
        id: u64,
    },
    GetParquetSpillStatus,
    ListActiveRecoveries,
    GetRecoveryOperation {
        operation: OperationId,
    },
    GetRecoveryStatus {
        target: ResourceId,
    },
    GetRecoveryPolicy {
        target: ResourceId,
    },
    GetUnresolvedIntegrityFailures {
        target: ResourceId,
    },
    GetOperation {
        operation: OperationId,
    },
    GetProfileCapabilities {
        exchange: ExchangeId,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectionHandle {
    pub owner: super::demand::RetentionSessionId,
    pub id: u64,
    pub generation: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
pub enum ControlCommand {
    Query {
        query: Query,
        page: PageRequest,
    },
    ValidateConfiguration {
        update: ConfigUpdate,
    },
    ApplyConfiguration {
        update: ConfigUpdate,
        idempotency_key: String,
        objective: CompletionObjective,
    },
    RestoreConfiguration {
        expected_revision: ConfigRevisionId,
        revision: ConfigRevisionId,
        idempotency_key: String,
        objective: CompletionObjective,
    },
    AcquireDemand {
        stream: StreamRef,
        idempotency_key: String,
        objective: CompletionObjective,
    },
    ReleaseDemand {
        handle: StreamHandle,
        idempotency_key: String,
    },
    RequestRecovery {
        target: ResourceVersion,
        category: RecoveryCategory,
        interval: Option<CursorInterval>,
        idempotency_key: String,
    },
    RestartConnection {
        connection: ConnectionId,
        generation: super::cursor::ConnectionGeneration,
        idempotency_key: String,
    },
    ResynchronizeBook {
        stream: StreamId,
        generation: super::cursor::BookGeneration,
        idempotency_key: String,
    },
    RequestRecordingFlush {
        stream: StreamId,
        generation: u64,
        idempotency_key: String,
        objective: CompletionObjective,
    },
    RequestRetentionProtection {
        range: CursorInterval,
        lifetime_ns: u64,
        capacity: DeliveryCapacity,
        idempotency_key: String,
    },
    AdvanceRetentionProtection {
        handle: ProtectionHandle,
        through: super::cursor::StreamCursor,
        idempotency_key: String,
    },
    RenewRetentionProtection {
        handle: ProtectionHandle,
        lifetime_ns: u64,
        idempotency_key: String,
    },
    ReleaseRetentionProtection {
        handle: ProtectionHandle,
        idempotency_key: String,
    },
    CancelOperation {
        operation: OperationId,
        idempotency_key: String,
    },
    Shutdown {
        deadline_ns: u64,
        idempotency_key: String,
    },
}
impl ControlCommand {
    pub fn idempotency_key(&self) -> Option<&str> {
        match self {
            Self::Query { .. } | Self::ValidateConfiguration { .. } => None,
            Self::ApplyConfiguration {
                idempotency_key, ..
            }
            | Self::RestoreConfiguration {
                idempotency_key, ..
            }
            | Self::AcquireDemand {
                idempotency_key, ..
            }
            | Self::ReleaseDemand {
                idempotency_key, ..
            }
            | Self::RequestRecovery {
                idempotency_key, ..
            }
            | Self::RestartConnection {
                idempotency_key, ..
            }
            | Self::ResynchronizeBook {
                idempotency_key, ..
            }
            | Self::RequestRecordingFlush {
                idempotency_key, ..
            }
            | Self::RequestRetentionProtection {
                idempotency_key, ..
            }
            | Self::AdvanceRetentionProtection {
                idempotency_key, ..
            }
            | Self::RenewRetentionProtection {
                idempotency_key, ..
            }
            | Self::ReleaseRetentionProtection {
                idempotency_key, ..
            }
            | Self::CancelOperation {
                idempotency_key, ..
            }
            | Self::Shutdown {
                idempotency_key, ..
            } => Some(idempotency_key),
        }
    }
    pub fn session_scoped(&self) -> bool {
        matches!(
            self,
            Self::AcquireDemand { .. }
                | Self::ReleaseDemand { .. }
                | Self::RequestRetentionProtection { .. }
                | Self::AdvanceRetentionProtection { .. }
                | Self::RenewRetentionProtection { .. }
                | Self::ReleaseRetentionProtection { .. }
        )
    }
    pub fn required_permissions(&self) -> Vec<Permission> {
        use Permission::*;
        let mut required = match self {
            Self::Query { .. } | Self::ValidateConfiguration { .. } => vec![Monitor],
            Self::ApplyConfiguration { .. } => vec![Configure],
            Self::RestoreConfiguration { .. } => vec![
                Configure,
                ManageStreams,
                ManageRecording,
                ManageRetention,
                ManageRuntime,
            ],
            Self::AcquireDemand { .. } | Self::ReleaseDemand { .. } => vec![ManageStreams],
            Self::RequestRecovery { .. }
            | Self::RestartConnection { .. }
            | Self::ResynchronizeBook { .. } => vec![RequestRecovery],
            Self::RequestRecordingFlush { .. } => vec![ManageRecording],
            Self::RequestRetentionProtection { .. }
            | Self::AdvanceRetentionProtection { .. }
            | Self::RenewRetentionProtection { .. }
            | Self::ReleaseRetentionProtection { .. } => vec![ManageRetention],
            Self::CancelOperation { .. } | Self::Shutdown { .. } => vec![ManageRuntime],
        };
        if let Self::ApplyConfiguration { update, .. } = self {
            for c in &update.changes {
                let p = match c {
                    ConfigChange::SetSourceRecording { .. }
                    | ConfigChange::SetRecordingDefaults { .. }
                    | ConfigChange::SetRecordingBatch { .. } => ManageRecording,
                    ConfigChange::RegisterSource { .. }
                    | ConfigChange::SetSourceEnabled { .. }
                    | ConfigChange::RemoveSource { .. }
                    | ConfigChange::SetSourceRecovery { .. }
                    | ConfigChange::SetSourceMetrics { .. }
                    | ConfigChange::PutDerived { .. }
                    | ConfigChange::RemoveDerived { .. } => ManageStreams,
                    ConfigChange::SetBudgets { .. } => ManageRetention,
                    _ => ManageRuntime,
                };
                if !required.contains(&p) {
                    required.push(p);
                }
                if matches!(
                    c,
                    ConfigChange::RegisterSource { .. } | ConfigChange::PutDerived { .. }
                ) && !required.contains(&ManageRecording)
                {
                    required.push(ManageRecording);
                }
                if matches!(c, ConfigChange::SetBudgets { .. })
                    && !required.contains(&ManageRuntime)
                {
                    required.push(ManageRuntime);
                }
            }
        }
        required
    }
    pub fn authorize(&self, peer: &AuthenticatedPeer) -> Result<(), ErrorCode> {
        if self
            .required_permissions()
            .iter()
            .all(|p| peer.grants.contains(p))
        {
            Ok(())
        } else {
            Err(ErrorCode::PermissionDenied)
        }
    }
    pub fn validate(&self, max_items: usize) -> Result<(), ErrorCode> {
        if self
            .idempotency_key()
            .is_some_and(|k| k.trim().is_empty() || k.len() > 128)
        {
            return Err(ErrorCode::InvalidInput);
        }
        match self {
            Self::AcquireDemand { objective, .. }
                if !matches!(
                    objective,
                    CompletionObjective::RuntimeApplied | CompletionObjective::AcquisitionReady
                ) =>
            {
                return Err(ErrorCode::InvalidCompletion);
            }
            Self::RequestRecordingFlush { objective, .. }
                if *objective != CompletionObjective::RecordingCommitted =>
            {
                return Err(ErrorCode::InvalidCompletion);
            }
            Self::ApplyConfiguration { objective, .. }
            | Self::RestoreConfiguration { objective, .. }
                if !matches!(
                    objective,
                    CompletionObjective::DesiredPersisted
                        | CompletionObjective::RuntimeApplied
                        | CompletionObjective::AcquisitionReady
                        | CompletionObjective::Removed
                ) =>
            {
                return Err(ErrorCode::InvalidCompletion);
            }
            Self::Query { query, page } => {
                if page.limit == 0 || page.limit as usize > max_items {
                    return Err(ErrorCode::Limit);
                }
                match query {
                    Query::GetRecordingCoverage { range, .. }
                    | Query::GetRetentionCoverage { range, .. } => {
                        range.checked_range()?;
                    }
                    _ => {}
                }
            }
            Self::ApplyConfiguration { update, .. } | Self::ValidateConfiguration { update } => {
                if update.changes.len() > max_items {
                    return Err(ErrorCode::Limit);
                }
                for change in &update.changes {
                    change.validate_structure(max_items)?;
                }
            }
            Self::RequestRecovery {
                interval: Some(range),
                ..
            } => {
                range.checked_range()?;
            }
            Self::RequestRetentionProtection {
                range,
                lifetime_ns,
                capacity,
                ..
            } => {
                range.checked_range()?;
                if *lifetime_ns == 0
                    || capacity.max_pending_events == 0
                    || capacity.max_pending_bytes == 0
                {
                    return Err(ErrorCode::InvalidInput);
                }
            }
            Self::RenewRetentionProtection { lifetime_ns, .. } if *lifetime_ns == 0 => {
                return Err(ErrorCode::InvalidInput);
            }
            Self::Shutdown { deadline_ns, .. } if *deadline_ns == 0 => {
                return Err(ErrorCode::InvalidInput);
            }
            _ => {}
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlRequest {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub command: ControlCommand,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ControlResponse {
    pub protocol_version: u16,
    pub request_id: RequestId,
    pub result: ControlResult,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "result", content = "value", rename_all = "snake_case")]
pub enum ControlResult {
    Runtime(RuntimeSnapshot),
    Sources(Page<SourceSnapshot>),
    Source(Box<SourceSnapshot>),
    Derived(Page<DerivedSnapshot>),
    DerivedStream(Box<DerivedSnapshot>),
    Connections(Page<ConnectionSnapshot>),
    Subscribers(Page<SubscriberSnapshot>),
    Recording(Page<RecordingSnapshot>),
    Retention(Page<RetentionSnapshot>),
    Recoveries(Page<RecoverySnapshot>),
    Metrics {
        stamp: SnapshotStamp,
        samples: Vec<MetricSample>,
    },
    Health(HealthSnapshot),
    Configuration(ConfigurationRevision),
    ConfigurationHistory(Page<ConfigurationRevision>),
    ConfigurationDiff(ConfigurationDiff),
    EffectiveConfiguration(EffectiveConfiguration),
    Validation(ValidationReport),
    OperationAccepted(OperationStatus),
    Operation(OperationStatus),
    DemandAcquired(StreamHandle),
    DemandReleased(StreamHandle),
    Protection(ProtectionSnapshot),
    Intervals(Vec<CursorInterval>),
    Dependencies(Vec<StreamRef>),
    RecoveryPolicy(RecoveryPolicy),
    Capabilities(Vec<Capability>),
    Error(ControlError),
}

impl ControlResult {
    /// Semantic validation is shared by the client and server; bounded frame
    /// decoding separately limits all owned collections and strings.
    pub fn validate(&self) -> Result<(), ErrorCode> {
        match self {
            Self::OperationAccepted(op) | Self::Operation(op) => op.validate()?,
            Self::Metrics { samples, .. } => {
                for s in samples {
                    s.validate()?
                }
            }
            Self::Runtime(s) => {
                for m in &s.metrics {
                    m.validate()?
                }
            }
            Self::Source(s) => s.validate()?,
            Self::Sources(page) => {
                for s in &page.items {
                    s.validate()?
                }
            }
            Self::Derived(page) => {
                for s in &page.items {
                    for m in &s.metrics {
                        m.validate()?
                    }
                }
            }
            Self::DerivedStream(s) => {
                for m in &s.metrics {
                    m.validate()?
                }
            }
            Self::Recording(page) => {
                for s in &page.items {
                    s.validate()?
                }
            }
            Self::Retention(page) => {
                for s in &page.items {
                    s.validate()?
                }
            }
            Self::Intervals(intervals) => {
                for i in intervals {
                    i.checked_range()?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}
