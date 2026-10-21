//! Frozen native metric vocabulary and availability/health DTOs. No collector,
//! exporter or health policy implementation; measurements never drive integrity.
use super::{codec::ExactDecimal, error::ErrorCode, operation::ResourceId};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "availability", rename_all = "snake_case")]
pub enum Measurement<T> {
    Available {
        value: T,
        observed_at_ns: i64,
    },
    Unavailable {
        reason: MissingMeasurement,
    },
    Stale {
        last_value: Option<T>,
        observed_at_ns: i64,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MissingMeasurement {
    Disabled,
    NotApplicable,
    NotCollected,
    Overflow,
    Unsupported,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Unknown,
    Disabled,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HealthReason {
    NoObservation,
    StaleObservation,
    Disconnected,
    SubscriptionPending,
    MetadataUnavailable,
    IntegrityViolation,
    RecoveryPending,
    Backpressure,
    StorageUnavailable,
    UnverifiedCoverage,
    MissingReference,
    DependencyDegraded,
    Disabled,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityHealth {
    pub capability: Capability,
    pub status: HealthStatus,
    pub required: bool,
    pub reasons: Vec<HealthReason>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capability {
    Acquisition,
    TradeContinuity,
    BookIntegrity,
    Publication,
    Replay,
    Recording,
    DerivedOrdering,
    Conversion,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthSnapshot {
    pub resource: ResourceId,
    pub status: HealthStatus,
    pub reasons: Vec<HealthReason>,
    pub capabilities: Vec<CapabilityHealth>,
    pub evaluated_at_ns: i64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySummary {
    pub count: u64,
    pub sum_ns: u64,
    pub min_ns: Option<u64>,
    pub p50_ns: Option<u64>,
    pub p95_ns: Option<u64>,
    pub p99_ns: Option<u64>,
    pub max_ns: Option<u64>,
}
impl LatencySummary {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        let points = [
            self.min_ns,
            self.p50_ns,
            self.p95_ns,
            self.p99_ns,
            self.max_ns,
        ];
        if self.count == 0 {
            if self.sum_ns != 0 || points.iter().any(Option::is_some) {
                return Err(ErrorCode::InvalidInput);
            }
        } else {
            let mut prior = None;
            for value in points.into_iter().flatten() {
                if prior.is_some_and(|p| p > value) {
                    return Err(ErrorCode::InvalidInput);
                }
                prior = Some(value);
            }
            if self.min_ns.is_none() || self.max_ns.is_none() {
                return Err(ErrorCode::InvalidInput);
            }
            if let (Some(min), Some(max)) = (self.min_ns, self.max_ns) {
                let sum = u128::from(self.sum_ns);
                let count = u128::from(self.count);
                if sum < count * u128::from(min) || sum > count * u128::from(max) {
                    return Err(ErrorCode::InvalidInput);
                }
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Counter,
    UnsignedGauge,
    Bytes,
    DurationNs,
    Timestamp,
    Decimal,
    Ratio,
    Rate,
    Distribution,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricUnit {
    Count,
    Bytes,
    Nanoseconds,
    UnixNanoseconds,
    Price,
    Ratio,
    PerSecond,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum MetricValue {
    Counter(u64),
    UnsignedGauge(u64),
    Bytes(u64),
    DurationNs(u64),
    Timestamp(i64),
    Decimal(ExactDecimal),
    Ratio(ExactDecimal),
    Rate(ExactDecimal),
    Distribution(LatencySummary),
}
impl MetricValue {
    pub fn kind(&self) -> MetricKind {
        match self {
            Self::Counter(..) => MetricKind::Counter,
            Self::UnsignedGauge(..) => MetricKind::UnsignedGauge,
            Self::Bytes(..) => MetricKind::Bytes,
            Self::DurationNs(..) => MetricKind::DurationNs,
            Self::Timestamp(..) => MetricKind::Timestamp,
            Self::Decimal(..) => MetricKind::Decimal,
            Self::Ratio(..) => MetricKind::Ratio,
            Self::Rate(..) => MetricKind::Rate,
            Self::Distribution(..) => MetricKind::Distribution,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricId {
    MessagesReceived,
    BytesReceived,
    ReconnectAttempts,
    RequestsTotal,
    SuccessfulRequestsTotal,
    FailedRequestsTotal,
    RetriesTotal,
    RateLimitResponsesTotal,
    ExchangeUnavailableResponsesTotal,
    SubscriptionAttemptsTotal,
    SubscriptionSuccessesTotal,
    SubscriptionFailuresTotal,
    TradesReceived,
    TradesAccepted,
    DuplicatesDiscarded,
    ConflictingDuplicates,
    UniqueDetectedGaps,
    GapRecoveryAttempts,
    RecoveryRequestedTotal,
    RecoveryStartedTotal,
    RecoverySucceededTotal,
    RecoveryFailedTotal,
    RecoveredExecutionsTotal,
    DepthMessagesReceivedTotal,
    DepthEventsAcceptedTotal,
    LevelsProcessedTotal,
    LevelsChangedTotal,
    LevelsDeletedTotal,
    LevelsUnchangedTotal,
    SequenceGapsDetectedTotal,
    SequenceValidationFailuresTotal,
    SequenceResetsTotal,
    SynchronizationInvalidationsTotal,
    EventsAdmittedToPublisher,
    UniqueSourceEventsPublished,
    SubscriberEventDeliveries,
    SlowConsumerDisconnectsTotal,
    ReplayRequestsTotal,
    ReplayEventsDeliveredTotal,
    ReplayFailuresTotal,
    ReplayCoverageUnavailableTotal,
    ProtectionAdmissionFailuresTotal,
    ReplayEventsReadTotal,
    RetentionReadFailuresTotal,
    SpillWritesTotal,
    SpillCommitsTotal,
    SpillFailuresTotal,
    MergeEventsReleased,
    MergeBufferSaturationEventsTotal,
    LateEventsDetectedTotal,
    LateEventsPublishedTotal,
    LateEventsSideOutputTotal,
    LateEventsCausingFailureTotal,
    DerivedEventsProduced,
    DerivedEventsAdmittedToPublisher,
    DerivedEventsWrittenToSubscribers,
    TransactionsStartedTotal,
    TransactionsCommittedTotal,
    TransactionsFailedTotal,
    TransactionsRetriedTotal,
    AmbiguousCommitOutcomesTotal,
    CommitReconciliationsTotal,
    RetentionCleanupFailuresTotal,
    RecoveryEscalations,
    FailedTasksTotal,
    RestartedTasksTotal,
    ActiveConnections,
    ActiveSourceStreams,
    ActiveDerivedStreams,
    ActiveSubscribers,
    ActiveRequests,
    ActiveSubscriptions,
    BidLevelCount,
    AskLevelCount,
    SubscriberQueueEvents,
    RetainedEvents,
    ProtectedEventCount,
    ActiveBootstrapProtections,
    ActiveRetentionReaders,
    RequiredSourceCount,
    ActiveSourceCount,
    DegradedSourceCount,
    UnavailableSourceCount,
    MergeBufferedEvents,
    RequiredReferenceCount,
    AvailableReferenceCount,
    StaleReferenceCount,
    MissingReferenceCount,
    RecordingPendingEvents,
    RecordingPendingBatches,
    KnownRecordingGapCount,
    UnverifiedRecordingIntervals,
    RecoverableGapCount,
    UnrecoverableGapCount,
    CurrentRetryAttempt,
    ConsecutiveFailures,
    ActiveRuntimeTasks,
    RunningProcessingTasks,
    PendingProcessingTasks,
    OpenFileDescriptorCount,
    ConfiguredFileDescriptorBudget,
    LastAcceptedSequence,
    CurrentBookGeneration,
    ConfiguredWeightLimit,
    ObservedWeightUsage,
    EstimatedRemainingWeight,
    TotalResidentMemory,
    ComponentMemory,
    RegisteredBufferMemory,
    RetainedBytes,
    ProtectedBytes,
    PendingSpillBytes,
    AvailableDiskBytes,
    UsedRetentionDiskBytes,
    ConfiguredDiskBudgetBytes,
    MergeBufferedBytes,
    RecordingPendingBytes,
    RecordingRetentionBytes,
    PublisherQueueBytes,
    ProcessUptime,
    ConnectionDuration,
    CooldownRemaining,
    WeightWindow,
    OldestUnresolvedGapAge,
    OldestRetainedEventAge,
    OldestProtectionAge,
    SubscriberQueueAge,
    WatermarkStallDuration,
    OldestRequiredReferenceAge,
    RecordingOldestPendingAge,
    CurrentBackoffDuration,
    TotalRetryDuration,
    MaximumObservedLateness,
    LatestAcceptedEventTimestamp,
    LatestAcceptedReceiveTimestamp,
    LatestRecoveredEventTimestamp,
    InitializationTimestamp,
    LastValidUpdateTimestamp,
    LastSubscriptionAcknowledgement,
    CombinedWatermarkNs,
    PerSourceWatermarkNs,
    BestBid,
    BestAsk,
    Spread,
    MemoryUtilizationRatio,
    MergeBufferOccupancyRatio,
    RecordingQueueOccupancyRatio,
    ProcessCpuUtilization,
    ProcessingWorkerBusyRatio,
    FileDescriptorPressure,
    InputEventRate,
    OutputEventRate,
    ReceiveRate,
    AcceptedCanonicalTradesPerSecond,
    MergeInputEventsPerSecond,
    MergeOutputEventsPerSecond,
    RecordingEventsAdmittedPerSecond,
    RecordingEventsCommittedPerSecond,
    RecordingBytesCommittedPerSecond,
    WatermarkAdvanceRate,
    ConnectionEstablishmentDuration,
    ReconnectionDuration,
    WebSocketPingRoundTripTime,
    RequestDuration,
    RecoveryDuration,
    ExchangeEventToLocalReceive,
    LocalReceiveToCanonicalAcceptance,
    CanonicalAcceptanceToPublication,
    SnapshotApplyDuration,
    DepthUpdateValidationDuration,
    BookMutationDuration,
    CanonicalLevelEmissionDuration,
    SubscriberWriteLatency,
    ReplayCatchUpDuration,
    ReplayReadLatency,
    ReferenceLookupDuration,
    ConversionPathEvaluationDuration,
    ExactDecimalArithmeticDuration,
    TotalConversionDuration,
    BatchPreparationDuration,
    DatabaseExecutionDuration,
    CommitConfirmationDuration,
    CanonicalAcceptanceToPublisherAdmission,
    PublisherAdmissionToQueue,
    QueueToSocketWrite,
    RecordingAdmissionToBatchSeal,
    BatchSealToTransactionStart,
    TotalRecordingAdmissionToCommit,
    RuntimeSchedulerDelay,
}
impl MetricId {
    pub fn descriptor(self) -> (MetricKind, MetricUnit) {
        match self {
            Self::BytesReceived => (MetricKind::Counter, MetricUnit::Bytes),
            Self::MessagesReceived
            | Self::ReconnectAttempts
            | Self::RequestsTotal
            | Self::SuccessfulRequestsTotal
            | Self::FailedRequestsTotal
            | Self::RetriesTotal
            | Self::RateLimitResponsesTotal
            | Self::ExchangeUnavailableResponsesTotal
            | Self::SubscriptionAttemptsTotal
            | Self::SubscriptionSuccessesTotal
            | Self::SubscriptionFailuresTotal
            | Self::TradesReceived
            | Self::TradesAccepted
            | Self::DuplicatesDiscarded
            | Self::ConflictingDuplicates
            | Self::UniqueDetectedGaps
            | Self::GapRecoveryAttempts
            | Self::RecoveryRequestedTotal
            | Self::RecoveryStartedTotal
            | Self::RecoverySucceededTotal
            | Self::RecoveryFailedTotal
            | Self::RecoveredExecutionsTotal
            | Self::DepthMessagesReceivedTotal
            | Self::DepthEventsAcceptedTotal
            | Self::LevelsProcessedTotal
            | Self::LevelsChangedTotal
            | Self::LevelsDeletedTotal
            | Self::LevelsUnchangedTotal
            | Self::SequenceGapsDetectedTotal
            | Self::SequenceValidationFailuresTotal
            | Self::SequenceResetsTotal
            | Self::SynchronizationInvalidationsTotal
            | Self::EventsAdmittedToPublisher
            | Self::UniqueSourceEventsPublished
            | Self::SubscriberEventDeliveries
            | Self::SlowConsumerDisconnectsTotal
            | Self::ReplayRequestsTotal
            | Self::ReplayEventsDeliveredTotal
            | Self::ReplayFailuresTotal
            | Self::ReplayCoverageUnavailableTotal
            | Self::ProtectionAdmissionFailuresTotal
            | Self::ReplayEventsReadTotal
            | Self::RetentionReadFailuresTotal
            | Self::SpillWritesTotal
            | Self::SpillCommitsTotal
            | Self::SpillFailuresTotal
            | Self::MergeEventsReleased
            | Self::MergeBufferSaturationEventsTotal
            | Self::LateEventsDetectedTotal
            | Self::LateEventsPublishedTotal
            | Self::LateEventsSideOutputTotal
            | Self::LateEventsCausingFailureTotal
            | Self::DerivedEventsProduced
            | Self::DerivedEventsAdmittedToPublisher
            | Self::DerivedEventsWrittenToSubscribers
            | Self::TransactionsStartedTotal
            | Self::TransactionsCommittedTotal
            | Self::TransactionsFailedTotal
            | Self::TransactionsRetriedTotal
            | Self::AmbiguousCommitOutcomesTotal
            | Self::CommitReconciliationsTotal
            | Self::RetentionCleanupFailuresTotal
            | Self::RecoveryEscalations
            | Self::FailedTasksTotal
            | Self::RestartedTasksTotal => (MetricKind::Counter, MetricUnit::Count),
            Self::ActiveConnections
            | Self::ActiveSourceStreams
            | Self::ActiveDerivedStreams
            | Self::ActiveSubscribers
            | Self::ActiveRequests
            | Self::ActiveSubscriptions
            | Self::BidLevelCount
            | Self::AskLevelCount
            | Self::SubscriberQueueEvents
            | Self::RetainedEvents
            | Self::ProtectedEventCount
            | Self::ActiveBootstrapProtections
            | Self::ActiveRetentionReaders
            | Self::RequiredSourceCount
            | Self::ActiveSourceCount
            | Self::DegradedSourceCount
            | Self::UnavailableSourceCount
            | Self::MergeBufferedEvents
            | Self::RequiredReferenceCount
            | Self::AvailableReferenceCount
            | Self::StaleReferenceCount
            | Self::MissingReferenceCount
            | Self::RecordingPendingEvents
            | Self::RecordingPendingBatches
            | Self::KnownRecordingGapCount
            | Self::UnverifiedRecordingIntervals
            | Self::RecoverableGapCount
            | Self::UnrecoverableGapCount
            | Self::CurrentRetryAttempt
            | Self::ConsecutiveFailures
            | Self::ActiveRuntimeTasks
            | Self::RunningProcessingTasks
            | Self::PendingProcessingTasks
            | Self::OpenFileDescriptorCount
            | Self::ConfiguredFileDescriptorBudget
            | Self::LastAcceptedSequence
            | Self::CurrentBookGeneration
            | Self::ConfiguredWeightLimit
            | Self::ObservedWeightUsage
            | Self::EstimatedRemainingWeight => (MetricKind::UnsignedGauge, MetricUnit::Count),
            Self::TotalResidentMemory
            | Self::ComponentMemory
            | Self::RegisteredBufferMemory
            | Self::RetainedBytes
            | Self::ProtectedBytes
            | Self::PendingSpillBytes
            | Self::AvailableDiskBytes
            | Self::UsedRetentionDiskBytes
            | Self::ConfiguredDiskBudgetBytes
            | Self::MergeBufferedBytes
            | Self::RecordingPendingBytes
            | Self::RecordingRetentionBytes
            | Self::PublisherQueueBytes => (MetricKind::Bytes, MetricUnit::Bytes),
            Self::ProcessUptime
            | Self::ConnectionDuration
            | Self::CooldownRemaining
            | Self::WeightWindow
            | Self::OldestUnresolvedGapAge
            | Self::OldestRetainedEventAge
            | Self::OldestProtectionAge
            | Self::SubscriberQueueAge
            | Self::WatermarkStallDuration
            | Self::OldestRequiredReferenceAge
            | Self::RecordingOldestPendingAge
            | Self::CurrentBackoffDuration
            | Self::TotalRetryDuration
            | Self::MaximumObservedLateness => (MetricKind::DurationNs, MetricUnit::Nanoseconds),
            Self::LatestAcceptedEventTimestamp
            | Self::LatestAcceptedReceiveTimestamp
            | Self::LatestRecoveredEventTimestamp
            | Self::InitializationTimestamp
            | Self::LastValidUpdateTimestamp
            | Self::LastSubscriptionAcknowledgement
            | Self::CombinedWatermarkNs
            | Self::PerSourceWatermarkNs => (MetricKind::Timestamp, MetricUnit::UnixNanoseconds),
            Self::BestBid | Self::BestAsk | Self::Spread => {
                (MetricKind::Decimal, MetricUnit::Price)
            }
            Self::MemoryUtilizationRatio
            | Self::MergeBufferOccupancyRatio
            | Self::RecordingQueueOccupancyRatio
            | Self::ProcessCpuUtilization
            | Self::ProcessingWorkerBusyRatio
            | Self::FileDescriptorPressure => (MetricKind::Ratio, MetricUnit::Ratio),
            Self::InputEventRate
            | Self::OutputEventRate
            | Self::ReceiveRate
            | Self::AcceptedCanonicalTradesPerSecond
            | Self::MergeInputEventsPerSecond
            | Self::MergeOutputEventsPerSecond
            | Self::RecordingEventsAdmittedPerSecond
            | Self::RecordingEventsCommittedPerSecond
            | Self::RecordingBytesCommittedPerSecond
            | Self::WatermarkAdvanceRate => (MetricKind::Rate, MetricUnit::PerSecond),
            Self::ConnectionEstablishmentDuration
            | Self::ReconnectionDuration
            | Self::WebSocketPingRoundTripTime
            | Self::RequestDuration
            | Self::RecoveryDuration
            | Self::ExchangeEventToLocalReceive
            | Self::LocalReceiveToCanonicalAcceptance
            | Self::CanonicalAcceptanceToPublication
            | Self::SnapshotApplyDuration
            | Self::DepthUpdateValidationDuration
            | Self::BookMutationDuration
            | Self::CanonicalLevelEmissionDuration
            | Self::SubscriberWriteLatency
            | Self::ReplayCatchUpDuration
            | Self::ReplayReadLatency
            | Self::ReferenceLookupDuration
            | Self::ConversionPathEvaluationDuration
            | Self::ExactDecimalArithmeticDuration
            | Self::TotalConversionDuration
            | Self::BatchPreparationDuration
            | Self::DatabaseExecutionDuration
            | Self::CommitConfirmationDuration
            | Self::CanonicalAcceptanceToPublisherAdmission
            | Self::PublisherAdmissionToQueue
            | Self::QueueToSocketWrite
            | Self::RecordingAdmissionToBatchSeal
            | Self::BatchSealToTransactionStart
            | Self::TotalRecordingAdmissionToCommit
            | Self::RuntimeSchedulerDelay => (MetricKind::Distribution, MetricUnit::Nanoseconds),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricSample {
    pub resource: ResourceId,
    pub generation: u64,
    pub reset_epoch: u64,
    pub metric: MetricId,
    pub measurement: Measurement<MetricValue>,
}
impl MetricSample {
    pub fn validate(&self) -> Result<(), ErrorCode> {
        let v = match &self.measurement {
            Measurement::Available { value, .. } => Some(value),
            Measurement::Stale { last_value, .. } => last_value.as_ref(),
            _ => None,
        };
        if let Some(value) = v {
            if value.kind() != self.metric.descriptor().0 {
                return Err(ErrorCode::InvalidInput);
            }
            match value {
                MetricValue::Ratio(n)
                    if n.0 < rust_decimal::Decimal::ZERO || n.0 > rust_decimal::Decimal::ONE =>
                {
                    return Err(ErrorCode::InvalidInput);
                }
                MetricValue::Rate(n) if n.0 < rust_decimal::Decimal::ZERO => {
                    return Err(ErrorCode::InvalidInput);
                }
                MetricValue::Distribution(d) => d.validate()?,
                _ => {}
            }
        }
        Ok(())
    }
}

/// Registry metadata is fixed before owner startup; there are no free-form
/// metric names, labels or per-instrument series created on the event path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Aggregation {
    Sum,
    Latest,
    BoundedDistribution,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResetBoundary {
    OwnerGeneration,
    RuntimeInstance,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricDescriptor {
    pub kind: MetricKind,
    pub unit: MetricUnit,
    pub aggregation: Aggregation,
    pub reset: ResetBoundary,
}
impl MetricId {
    pub fn metadata(self, reset: ResetBoundary) -> MetricDescriptor {
        let (kind, unit) = self.descriptor();
        MetricDescriptor {
            kind,
            unit,
            reset,
            aggregation: match kind {
                MetricKind::Counter => Aggregation::Sum,
                MetricKind::Distribution => Aggregation::BoundedDistribution,
                _ => Aggregation::Latest,
            },
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MetricBinding {
    pub owner: super::operation::ResourceVersion,
    pub reset_epoch: u64,
    pub reset: ResetBoundary,
    pub metrics: Vec<MetricId>,
}
impl MetricBinding {
    pub fn validate(&self, max_series: usize) -> Result<(), ErrorCode> {
        if self.metrics.is_empty() || self.metrics.len() > max_series {
            return Err(ErrorCode::Limit);
        }
        for (i, m) in self.metrics.iter().enumerate() {
            if self.metrics[..i].contains(m) {
                return Err(ErrorCode::InvalidInput);
            }
        }
        Ok(())
    }
    pub fn sample(
        &self,
        metric: MetricId,
        measurement: Measurement<MetricValue>,
    ) -> Result<MetricSample, ErrorCode> {
        if !self.metrics.contains(&metric) {
            return Err(ErrorCode::UnsupportedCapability);
        }
        let sample = MetricSample {
            resource: self.owner.resource.clone(),
            generation: self.owner.generation,
            reset_epoch: self.reset_epoch,
            metric,
            measurement,
        };
        sample.validate()?;
        Ok(sample)
    }
}
