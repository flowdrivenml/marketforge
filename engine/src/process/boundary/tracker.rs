use crate::{
    book::SequencePolicy,
    canonical::Exchange,
    formats::depth::{DepthEventBoundary, DepthSourceEventMetadata},
};

use super::{BoundaryBookState, BoundaryContinuity, BoundarySnapshot, DepthBoundaryManifest};

pub struct DepthBoundaryTracker {
    manifest: DepthBoundaryManifest,
    sequence_policy: SequencePolicy,
    discontinuity_detected: bool,
    depth_per_side: Option<u32>,
}

impl DepthBoundaryTracker {
    pub fn new(
        exchange: Exchange,
        instrument_id: i64,
        symbol: String,
        stream_id: String,
        sequence_policy: SequencePolicy,
        depth_per_side: Option<u32>,
    ) -> Self {
        Self {
            manifest: DepthBoundaryManifest::new(exchange, instrument_id, symbol, stream_id),
            sequence_policy,
            depth_per_side,
            discontinuity_detected: false,
        }
    }

    /// Record one successfully accepted source message.
    ///
    /// Only initialization events require a captured book state.
    pub fn record_accepted(
        &mut self,
        source: &DepthSourceEventMetadata,
        boundary: DepthEventBoundary,
        initial_state: Option<BoundaryBookState>,
    ) {
        let timestamp = source.event_timestamp_ns;

        if self.manifest.first_timestamp_ns.is_none() {
            self.manifest.first_timestamp_ns = Some(timestamp);
            self.manifest.first_sequence = source.sequence_start;
        }

        self.manifest.last_timestamp_ns = Some(timestamp);
        self.manifest.last_sequence = source.sequence_end;

        if boundary == DepthEventBoundary::Initialization {
            if let Some(state) = initial_state {
                let snapshot = BoundarySnapshot::new(
                    source.sequence_end,
                    timestamp,
                    state,
                    self.depth_per_side,
                );

                if self.manifest.initial.is_none() {
                    self.manifest.initial = Some(snapshot);
                }
            }
        }
    }

    /// Reconstruction continuity was interrupted.
    pub fn invalidate(&mut self) {
        self.discontinuity_detected = true;
        self.manifest.final_state = None;
    }

    /// Capture the final reconstructed book once, after processing.
    pub fn set_final_state(&mut self, state: Option<BoundaryBookState>) {
        self.manifest.final_state = state.and_then(|state| {
            self.manifest.last_timestamp_ns.map(|timestamp| {
                BoundarySnapshot::new(
                    self.manifest.last_sequence,
                    timestamp,
                    state,
                    self.depth_per_side,
                )
            })
        });
    }

    pub fn finish(mut self) -> DepthBoundaryManifest {
        self.manifest.continuity = if self.discontinuity_detected {
            if self.manifest.final_state.is_some() {
                BoundaryContinuity::RecoveredFromSnapshot
            } else {
                BoundaryContinuity::GapDetected
            }
        } else if self.sequence_policy == SequencePolicy::Consecutive
            && self.manifest.initial.is_some()
            && self.manifest.final_state.is_some()
            && self.manifest.first_sequence.is_some()
            && self.manifest.last_sequence.is_some()
        {
            BoundaryContinuity::Verified
        } else {
            BoundaryContinuity::Unverifiable
        };

        self.manifest
    }
}
