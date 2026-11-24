use serde::{Deserialize, Serialize};

pub const MAX_SOURCES: usize = 128;
pub const MAX_OBSERVATIONS: usize = 64;
/// Immutable identity of an observation; repeated references must not be summed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EvidenceId {
    pub source: String,
    pub runtime_generation: Option<u64>,
    pub revision: Option<u64>,
    pub record_ordinal: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SequenceObservation {
    pub ordinal: u64,
    pub kind: String,
    pub previous: u64,
    pub observed: u64,
    /// Unavailable for non-contiguous exchange identifiers.
    pub missing_records: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataIntegrityEvidence {
    pub id: EvidenceId,
    pub source_generation: Option<u64>,
    pub book_generation: Option<u64>,
    pub publication_sequence: Option<u64>,
    /// Metadata-only observation occurred after this accepted Router position.
    pub after_publication_sequence: Option<u64>,
    pub coverage: String,
    pub diagnostic: Option<String>,
    pub depth_coverage: Option<DepthCoverage>,
    pub received_records: Option<u64>,
    pub duplicate_records: Option<u64>,
    pub identity_history_truncated: bool,
    pub coverage_history_unverified: bool,
    pub details_truncated: bool,
    pub numeric_trade_id: Option<u64>,
    pub cross_sequence: Option<u64>,
    pub depth_sequence: Option<u64>,
    pub recovered_records: bool,
    pub recovery_from: Option<EvidenceId>,
    /// Recovered records alone do not prove recovery completeness.
    pub recovery_complete: Option<bool>,
    pub observations: Vec<SequenceObservation>,
}
impl DataIntegrityEvidence {
    pub fn unavailable(source: String, generation: Option<u64>) -> Self {
        Self {
            id: EvidenceId {
                source,
                runtime_generation: generation,
                revision: None,
                record_ordinal: None,
            },
            source_generation: None,
            book_generation: None,
            publication_sequence: None,
            after_publication_sequence: None,
            coverage: "unavailable".into(),
            diagnostic: None,
            depth_coverage: None,
            received_records: None,
            duplicate_records: None,
            identity_history_truncated: false,
            coverage_history_unverified: true,
            details_truncated: false,
            numeric_trade_id: None,
            cross_sequence: None,
            depth_sequence: None,
            recovered_records: false,
            recovery_from: None,
            recovery_complete: None,
            observations: vec![],
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataIntegritySnapshot {
    pub stream_id: String,
    pub runtime_generation: Option<u64>,
    /// Router-local observation revision, not a resume cursor.
    pub revision: u64,
    pub sources: Vec<DataIntegrityEvidence>,
    pub scope_truncated: bool,
    pub evidence_history: Vec<DataIntegrityEvidence>,
    pub history_truncated: bool,
    pub discontinuity: Option<StreamDiscontinuity>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataIntegrityUpdate {
    pub revision: u64,
    pub evidence: DataIntegrityEvidence,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DepthCoverage {
    pub subscribed_depth: Option<u64>,
    pub retained_bid_levels: u64,
    pub retained_ask_levels: u64,
    pub internal_synchronized: bool,
    pub snapshot_source: Option<String>,
    pub snapshot_declared_depth: Option<u64>,
}

/// A conclusion from a processing integrity owner, retained through revocation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamDiscontinuity {
    pub revision: Option<u64>,
    pub detail: String,
    pub detail_truncated: bool,
    pub after_publication_sequence: Option<u64>,
}
