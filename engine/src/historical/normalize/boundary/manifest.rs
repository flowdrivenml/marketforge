use serde::{Deserialize, Serialize};

use crate::canonical::Exchange;

use super::{fingerprint::fingerprint_book, state::BoundaryBookState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoundaryContinuity {
    Verified,
    Unverifiable,
    GapDetected,
    RecoveredFromSnapshot,
    BoundaryMismatch,
    Invalid,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundarySnapshot {
    pub sequence: Option<u64>,
    pub timestamp_ns: i64,

    pub coverage: SnapshotCoverage,

    pub state: BoundaryBookState,
    pub fingerprint: String,
}

impl BoundarySnapshot {
    pub fn new(
        sequence: Option<u64>,
        timestamp_ns: i64,
        state: BoundaryBookState,
        depth_per_side: Option<u32>,
    ) -> Self {
        let coverage = SnapshotCoverage {
            depth_per_side,
            bid_levels: u32::try_from(state.bids.len()).expect("bid level count exceeds u32"),
            ask_levels: u32::try_from(state.asks.len()).expect("ask level count exceeds u32"),
        };

        let fingerprint = fingerprint_book(&state);

        Self {
            sequence,
            timestamp_ns,
            coverage,
            state,
            fingerprint,
        }
    }
}

/// Metadata describing a processed depth archive's reconstruction boundaries.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DepthBoundaryManifest {
    pub version: u32,

    pub exchange: Exchange,
    pub instrument_id: i64,
    pub symbol: String,
    pub stream_id: String,

    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,

    pub first_timestamp_ns: Option<i64>,
    pub last_timestamp_ns: Option<i64>,

    pub initial: Option<BoundarySnapshot>,
    pub final_state: Option<BoundarySnapshot>,

    pub continuity: BoundaryContinuity,
}

impl DepthBoundaryManifest {
    pub fn new(exchange: Exchange, instrument_id: i64, symbol: String, stream_id: String) -> Self {
        Self {
            version: 1,

            exchange,
            instrument_id,
            symbol,
            stream_id,

            first_sequence: None,
            last_sequence: None,

            first_timestamp_ns: None,
            last_timestamp_ns: None,

            initial: None,
            final_state: None,

            continuity: BoundaryContinuity::Unverifiable,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotCoverage {
    /// Declared maximum snapshot depth per side.
    ///
    /// None means the source does not provide a reliable depth limit.
    pub depth_per_side: Option<u32>,

    /// Number of levels actually present in the reconstructed state.
    pub bid_levels: u32,
    pub ask_levels: u32,
}
