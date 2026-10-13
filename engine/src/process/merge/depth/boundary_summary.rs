use serde::Deserialize;

use crate::{
    canonical::Exchange,
    process::boundary::{BoundaryContinuity, SnapshotCoverage},
};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MergeBoundarySnapshot {
    pub sequence: Option<u64>,
    pub coverage: SnapshotCoverage,
    pub fingerprint: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MergeBoundarySummary {
    pub version: u32,

    pub exchange: Exchange,
    pub instrument_id: i64,
    pub symbol: String,
    pub stream_id: String,

    pub first_sequence: Option<u64>,
    pub last_sequence: Option<u64>,

    pub first_timestamp_ns: Option<i64>,
    pub last_timestamp_ns: Option<i64>,

    pub initial: Option<MergeBoundarySnapshot>,
    pub final_state: Option<MergeBoundarySnapshot>,

    pub continuity: BoundaryContinuity,
}
use crate::process::boundary::{BoundarySnapshot, DepthBoundaryManifest};

impl From<&BoundarySnapshot> for MergeBoundarySnapshot {
    fn from(snapshot: &BoundarySnapshot) -> Self {
        Self {
            sequence: snapshot.sequence,
            coverage: snapshot.coverage.clone(),
            fingerprint: snapshot.fingerprint.clone(),
        }
    }
}

impl From<&DepthBoundaryManifest> for MergeBoundarySummary {
    fn from(manifest: &DepthBoundaryManifest) -> Self {
        Self {
            version: manifest.version,

            exchange: manifest.exchange,
            instrument_id: manifest.instrument_id,
            symbol: manifest.symbol.clone(),
            stream_id: manifest.stream_id.clone(),

            first_sequence: manifest.first_sequence,
            last_sequence: manifest.last_sequence,

            first_timestamp_ns: manifest.first_timestamp_ns,
            last_timestamp_ns: manifest.last_timestamp_ns,

            initial: manifest.initial.as_ref().map(Into::into),
            final_state: manifest.final_state.as_ref().map(Into::into),

            continuity: manifest.continuity,
        }
    }
}
