//! Owned, bounded observation DTOs for the five management views. No runtime locks.
use super::{
    codec::ExactDecimal, configuration::*, cursor::*, demand::*, derived::*, error::ErrorCode,
    identity::*, lifecycle::*, observation::*, operation::*,
};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotStamp {
    pub runtime: RuntimeInstanceId,
    pub snapshot_epoch: u64,
    pub sequence: u64,
    pub captured_at_ns: i64,
    pub collection_duration_ns: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Revisions {
    pub desired: ConfigRevisionId,
    pub applied: Option<ConfigRevisionId>,
    pub adopted: Option<ConfigRevisionId>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeLifecycle {
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordingState {
    Disabled,
    Starting,
    Active,
    Draining,
    Failed,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceUsage {
    pub events: u64,
    pub bytes: u64,
    pub limit_events: u64,
    pub limit_bytes: u64,
    pub pending_operations: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeSnapshot {
    pub stamp: SnapshotStamp,
    pub lifecycle: RuntimeLifecycle,
    pub revisions: Revisions,
    pub active_connections: u32,
    pub active_sources: u32,
    pub active_derived: u32,
    pub active_subscribers: u32,
    pub active_recoveries: Vec<OperationId>,
    pub recording: RecordingState,
    pub retention: ResourceUsage,
    pub health: HealthSnapshot,
    pub metrics: Vec<MetricSample>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AcquisitionReadiness {
    pub subscription_confirmed: bool,
    pub processor_initialized: bool,
    pub metadata_available: bool,
    pub integrity_satisfied: bool,
    pub book_synchronized: Option<bool>,
}
impl AcquisitionReadiness {
    pub fn is_ready(&self) -> bool {
        self.subscription_confirmed
            && self.processor_initialized
            && self.metadata_available
            && self.integrity_satisfied
            && self.book_synchronized.unwrap_or(true)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceSnapshot {
    pub stamp: SnapshotStamp,
    pub stream: StreamId,
    pub key: SourceStreamKey,
    pub persistent_enabled: bool,
    pub effective_acquisition_required: bool,
    pub demands: Vec<DemandOwner>,
    pub lifecycle: StreamLifecycle,
    pub readiness: AcquisitionReadiness,
    pub connection: Option<ConnectionId>,
    pub connection_state: Option<ConnectionState>,
    pub connection_generation: Option<ConnectionGeneration>,
    pub subscription: Option<SubscriptionId>,
    pub subscription_state: SubscriptionState,
    pub subscription_generation: Option<SubscriptionGeneration>,
    pub integrity: IntegrityState,
    pub recovery: RecoveryState,
    pub coverage: CoverageState,
    pub revisions: Revisions,
    pub recording: RecordingState,
    pub book_generation: Option<BookGeneration>,
    pub latest_cursor: Option<StreamCursor>,
    pub best_bid: Measurement<ExactDecimal>,
    pub best_ask: Measurement<ExactDecimal>,
    pub health: HealthSnapshot,
    pub metrics: Vec<MetricSample>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceParticipation {
    pub source: StreamRef,
    pub required: bool,
    pub active: bool,
    pub health: HealthSnapshot,
    pub watermark_ns: Measurement<i64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReferenceHealth {
    pub source: StreamRef,
    pub freshness_ns: Measurement<u64>,
    pub health: HealthSnapshot,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedSnapshot {
    pub stamp: SnapshotStamp,
    pub definition: DerivedDefinition,
    pub generation: DerivedGeneration,
    pub lifecycle: StreamLifecycle,
    pub revisions: Revisions,
    pub inputs: Vec<SourceParticipation>,
    pub references: Vec<ReferenceHealth>,
    pub watermark_ns: Measurement<i64>,
    pub buffer: ResourceUsage,
    pub late_events: Measurement<u64>,
    pub latest_cursor: Option<StreamCursor>,
    pub health: HealthSnapshot,
    pub metrics: Vec<MetricSample>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionSnapshot {
    pub stamp: SnapshotStamp,
    pub id: ConnectionId,
    pub generation: ConnectionGeneration,
    pub exchange: ExchangeId,
    pub api_family: String,
    pub state: ConnectionState,
    pub affected_streams: Vec<StreamId>,
    pub subscriptions: u32,
    pub resources: ResourceUsage,
    pub health: HealthSnapshot,
    pub metrics: Vec<MetricSample>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SubscriberSnapshot {
    pub stamp: SnapshotStamp,
    pub id: SubscriberId,
    pub session_generation: u64,
    pub streams: Vec<StreamRef>,
    pub queue: ResourceUsage,
    pub health: HealthSnapshot,
    pub metrics: Vec<MetricSample>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorInterval {
    pub first: StreamCursor,
    pub last: StreamCursor,
}
impl CursorInterval {
    pub fn checked_range(&self) -> Result<super::replay::ReplayRange, ErrorCode> {
        super::replay::ReplayRange::inclusive(self.first.clone(), Some(self.last.clone()))
            .map_err(|_| ErrorCode::InvalidInput)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CursorPositions {
    pub accepted: Option<StreamCursor>,
    pub committed: Option<StreamCursor>,
    pub durable: Option<StreamCursor>,
}
impl CursorPositions {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        match &self.accepted {
            Some(a) => super::replay::RetentionPositions::new(
                a.clone(),
                self.committed.clone(),
                self.durable.clone(),
            )
            .map(|_| ())
            .map_err(|_| ErrorCode::InvalidInput),
            None if self.committed.is_none() && self.durable.is_none() => Ok(()),
            _ => Err(ErrorCode::InvalidInput),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordingSnapshot {
    pub stamp: SnapshotStamp,
    pub stream: StreamId,
    pub generation: u64,
    pub state: RecordingState,
    pub database_connected: Measurement<bool>,
    pub queue: ResourceUsage,
    pub positions: CursorPositions,
    pub committed: Vec<CursorInterval>,
    pub gaps: Vec<CursorInterval>,
    pub integrity: IntegrityState,
    pub recovery: RecoveryState,
    pub commit_latency: Measurement<LatencySummary>,
    pub health: HealthSnapshot,
    pub metrics: Vec<MetricSample>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectionSnapshot {
    pub id: u64,
    pub generation: u64,
    pub owner: RetentionSessionId,
    pub first: StreamCursor,
    pub last: Option<StreamCursor>,
    pub safely_consumed: Option<StreamCursor>,
    pub remaining_lifetime_ns: Measurement<u64>,
    pub active: bool,
    pub failure: Option<ErrorCode>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionSnapshot {
    pub stamp: SnapshotStamp,
    pub stream: StreamId,
    pub positions: CursorPositions,
    pub available: Vec<CursorInterval>,
    pub gaps: Vec<CursorInterval>,
    pub protected: Vec<CursorInterval>,
    pub protections: Vec<ProtectionSnapshot>,
    pub memory: ResourceUsage,
    pub disk: ResourceUsage,
    pub pending_spill_bytes: u64,
    pub coverage: CoverageState,
    pub health: HealthSnapshot,
    pub metrics: Vec<MetricSample>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryCategory {
    Connection,
    Subscription,
    TradeContinuity,
    BookSynchronization,
    Recording,
    Retention,
    Derived,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoverySnapshot {
    pub stamp: SnapshotStamp,
    pub id: OperationId,
    pub target: ResourceVersion,
    pub category: RecoveryCategory,
    pub state: RecoveryState,
    pub attempts: u32,
    pub consecutive_failures: u32,
    pub next_retry_ns: Measurement<i64>,
    pub guarantees: Vec<CapabilityHealth>,
    pub last_error: Option<super::error::ControlError>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PageRequest {
    pub limit: u32,
    pub after: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub continuation: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "view", rename_all = "snake_case")]
pub enum ManagementView {
    Dashboard {
        runtime: RuntimeSnapshot,
    },
    Streams {
        sources: Page<SourceSnapshot>,
    },
    Derived {
        streams: Page<DerivedSnapshot>,
    },
    Recording {
        recording: Page<RecordingSnapshot>,
        retention: Page<RetentionSnapshot>,
    },
    Settings {
        desired: ConfigurationRevision,
        effective: EffectiveConfiguration,
    },
}

impl SourceSnapshot {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        if self.stream.0.trim().is_empty()
            || self.key.validate().has_definite_failures()
            || self
                .latest_cursor
                .as_ref()
                .is_some_and(|c| c.stream_id != self.stream)
        {
            return Err(ErrorCode::InvalidInput);
        }
        if self.key.channel.kind == ChannelKind::L2 && self.readiness.book_synchronized.is_none() {
            return Err(ErrorCode::InvalidInput);
        }
        if self.readiness.book_synchronized == Some(true) && self.book_generation.is_none() {
            return Err(ErrorCode::InvalidInput);
        }
        for sample in &self.metrics {
            sample.validate()?;
        }
        Ok(())
    }
}
impl RecordingSnapshot {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        self.positions.validate()?;
        for cursor in [
            &self.positions.accepted,
            &self.positions.committed,
            &self.positions.durable,
        ]
        .into_iter()
        .flatten()
        {
            if cursor.stream_id != self.stream {
                return Err(ErrorCode::InvalidInput);
            }
        }
        for interval in self.committed.iter().chain(&self.gaps) {
            interval.checked_range()?;
            if interval.first.stream_id != self.stream {
                return Err(ErrorCode::InvalidInput);
            }
        }
        for sample in &self.metrics {
            sample.validate()?;
        }
        match &self.commit_latency {
            Measurement::Available { value, .. } => value.validate()?,
            Measurement::Stale {
                last_value: Some(value),
                ..
            } => value.validate()?,
            _ => {}
        }
        Ok(())
    }
}
impl RetentionSnapshot {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        self.positions.validate()?;
        for cursor in [
            &self.positions.accepted,
            &self.positions.committed,
            &self.positions.durable,
        ]
        .into_iter()
        .flatten()
        {
            if cursor.stream_id != self.stream {
                return Err(ErrorCode::InvalidInput);
            }
        }
        for interval in self
            .available
            .iter()
            .chain(&self.gaps)
            .chain(&self.protected)
        {
            interval.checked_range()?;
            if interval.first.stream_id != self.stream {
                return Err(ErrorCode::InvalidInput);
            }
        }
        for sample in &self.metrics {
            sample.validate()?;
        }
        Ok(())
    }
}
