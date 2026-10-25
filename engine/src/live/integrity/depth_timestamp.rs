use crate::live::infrastructure::normalization::{DepthTimestampProvenance, NormalizedDepth};

/// How an authoritative depth publication was constructed.
///
/// This does not describe the age of individual resting
/// orders or price levels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DepthTimestampComposition {
    /// One native message produced the accepted update.
    SingleMessage,

    /// REST snapshot plus buffered WebSocket updates.
    RestReconciliation,

    /// Staged WebSocket snapshot plus bridging delta.
    StagedSnapshotBridge,
}

/// Timestamp of one contributing source message.
///
/// A source timestamp is not necessarily the timestamp
/// of the complete reconstructed order book.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthSourceTimestamp {
    pub timestamp_ns: i64,
    pub provenance: DepthTimestampProvenance,
}

impl DepthSourceTimestamp {
    /// Extract source timestamp metadata established
    /// by the normalization boundary.
    ///
    /// Returns None for legacy messages with unknown
    /// provenance or fallback messages whose timestamp
    /// cannot be recovered, such as empty snapshots.
    pub fn from_normalized(message: &NormalizedDepth) -> Option<Self> {
        match message.timestamp_provenance()? {
            DepthTimestampProvenance::Exchange => {
                let timestamp_ns = message.message_timestamp_ns()?;

                Some(Self {
                    timestamp_ns,
                    provenance: DepthTimestampProvenance::Exchange,
                })
            }

            DepthTimestampProvenance::ReceiveTimeFallback => {
                // The current NormalizedDepth API does not
                // independently retain receive_timestamp_ns.
                //
                // For nonempty messages, all level envelope
                // timestamps were checked when provenance
                // was established.
                let timestamp_ns = message.levels().first()?.envelope.event_timestamp_ns;

                Some(Self {
                    timestamp_ns,
                    provenance: DepthTimestampProvenance::ReceiveTimeFallback,
                })
            }
        }
    }
}

/// Metadata associated with one accepted book operation.
///
/// It describes the latest accepted contributing message,
/// not the timestamp history of every retained level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DepthAcceptedTimestamp {
    pub latest_source: Option<DepthSourceTimestamp>,
    pub composition: DepthTimestampComposition,
}

impl DepthAcceptedTimestamp {
    pub fn single_message(message: &NormalizedDepth) -> Self {
        Self {
            latest_source: DepthSourceTimestamp::from_normalized(message),
            composition: DepthTimestampComposition::SingleMessage,
        }
    }

    pub fn rest_reconciliation(latest_applied: Option<DepthSourceTimestamp>) -> Self {
        Self {
            latest_source: latest_applied,
            composition: DepthTimestampComposition::RestReconciliation,
        }
    }

    pub fn staged_snapshot_bridge(bridging_delta: &NormalizedDepth) -> Self {
        Self {
            latest_source: DepthSourceTimestamp::from_normalized(bridging_delta),
            composition: DepthTimestampComposition::StagedSnapshotBridge,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::{
        canonical::SequenceMetadata,
        live::infrastructure::normalization::{DepthMessageKind, NormalizationSource},
    };

    fn message(source: NormalizationSource, timestamp_ns: Option<i64>) -> NormalizedDepth {
        NormalizedDepth::new(
            "test-depth-stream",
            source,
            DepthMessageKind::Snapshot,
            SequenceMetadata::default(),
            Vec::new(),
            timestamp_ns,
            None,
        )
        .unwrap()
    }

    #[test]
    fn exchange_timestamp_is_preserved() {
        let message = message(NormalizationSource::WebSocket, Some(1_000_000_000))
            .with_timestamp_provenance(DepthTimestampProvenance::Exchange)
            .unwrap();

        let metadata = DepthAcceptedTimestamp::single_message(&message);

        assert_eq!(
            metadata.latest_source,
            Some(DepthSourceTimestamp {
                timestamp_ns: 1_000_000_000,
                provenance: DepthTimestampProvenance::Exchange,
            }),
        );

        assert_eq!(
            metadata.composition,
            DepthTimestampComposition::SingleMessage,
        );
    }

    #[test]
    fn unknown_provenance_is_not_assumed_exchange() {
        let message = message(NormalizationSource::WebSocket, Some(1_000_000_000));

        let metadata = DepthAcceptedTimestamp::single_message(&message);

        assert_eq!(metadata.latest_source, None);
    }

    #[test]
    fn empty_rest_fallback_has_unknown_timestamp() {
        let message = message(NormalizationSource::Rest, None)
            .with_timestamp_provenance(DepthTimestampProvenance::ReceiveTimeFallback)
            .unwrap();

        let metadata = DepthAcceptedTimestamp::single_message(&message);

        assert_eq!(metadata.latest_source, None);
    }

    #[test]
    fn rest_reconciliation_does_not_claim_single_message() {
        let latest = DepthSourceTimestamp {
            timestamp_ns: 2_000_000_000,
            provenance: DepthTimestampProvenance::Exchange,
        };

        let metadata = DepthAcceptedTimestamp::rest_reconciliation(Some(latest));

        assert_eq!(
            metadata.composition,
            DepthTimestampComposition::RestReconciliation,
        );

        assert_eq!(metadata.latest_source, Some(latest));
    }

    #[test]
    fn bridge_composition_is_explicit() {
        let delta = message(NormalizationSource::WebSocket, Some(3_000_000_000))
            .with_timestamp_provenance(DepthTimestampProvenance::Exchange)
            .unwrap();

        let metadata = DepthAcceptedTimestamp::staged_snapshot_bridge(&delta);

        assert_eq!(
            metadata.composition,
            DepthTimestampComposition::StagedSnapshotBridge,
        );

        assert_eq!(metadata.latest_source.unwrap().timestamp_ns, 3_000_000_000,);
    }
}
