//! Recording transaction boundaries. An implementation must atomically commit
//! identity registration, parents, ALL ordered children, and coverage. No IO here.
use super::schema::{MappingError, StoredEvent, StreamRow};
use crate::live::{
    model::{
        delivery::{DeliveryFailure, DeliveryStage, DeliveryStatus},
        replay::ReplayRange,
    },
    ports::routing::EventBatch,
};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordingTransactionId(pub u64);
#[derive(Debug)]
pub struct RecordingWritePlan {
    id: RecordingTransactionId,
    stream: StreamRow,
    records: Vec<StoredEvent>,
    ranges: Vec<ReplayRange>,
}
impl RecordingWritePlan {
    pub fn new(
        id: RecordingTransactionId,
        stream: StreamRow,
        batch: &EventBatch,
    ) -> Result<Self, MappingError> {
        let records = batch
            .events()
            .iter()
            .map(|e| StoredEvent::encode(e, &stream))
            .collect::<Result<Vec<_>, _>>()?;
        let mut ranges = vec![];
        let mut first = batch.first_cursor().clone();
        let mut last = first.clone();
        for e in batch.events().iter().skip(1) {
            if last.offset.checked_add(1) != Some(e.cursor.offset) {
                ranges.push(
                    ReplayRange::inclusive(first, Some(last))
                        .map_err(|_| MappingError::InvalidRecord)?,
                );
                first = e.cursor.clone();
            }
            last = e.cursor.clone();
        }
        ranges.push(
            ReplayRange::inclusive(first, Some(last)).map_err(|_| MappingError::InvalidRecord)?,
        );
        Ok(Self {
            id,
            stream,
            records,
            ranges,
        })
    }
    pub fn apply_outcome(
        &self,
        outcome: TransactionOutcome,
        coverage: &mut super::coverage::CommittedCoverage,
    ) -> Result<DeliveryStatus, crate::live::model::replay::ReplayError> {
        if matches!(outcome, TransactionOutcome::Acknowledged(_)) {
            coverage.observe_batch(&self.ranges)?;
        }
        Ok(outcome.delivery_status())
    }
    pub fn id(&self) -> RecordingTransactionId {
        self.id
    }
    pub fn stream(&self) -> &StreamRow {
        &self.stream
    }
    pub fn records(&self) -> &[StoredEvent] {
        &self.records
    }
    pub fn ranges(&self) -> &[ReplayRange] {
        &self.ranges
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitBoundary {
    Committed,
    Durable,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionOutcome {
    Acknowledged(CommitBoundary),
    Unknown,
    Failed(DeliveryFailure),
}
impl TransactionOutcome {
    /// Only the persistence owner can supply Acknowledged after real transaction
    /// acknowledgement. Durable additionally requires its configured WAL policy.
    pub fn delivery_status(self) -> DeliveryStatus {
        match self {
            Self::Acknowledged(CommitBoundary::Committed) => {
                DeliveryStatus::Progress(DeliveryStage::Committed)
            }
            Self::Acknowledged(CommitBoundary::Durable) => {
                DeliveryStatus::Progress(DeliveryStage::Durable)
            }
            Self::Unknown => DeliveryStatus::Failed(DeliveryFailure::CommitOutcomeUnknown),
            Self::Failed(e) => DeliveryStatus::Failed(e),
        }
    }
}
