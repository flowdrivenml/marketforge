//! Source callback adapters. The dispatch core has no Source runtime dependency.
use super::*;
use crate::live::{
    integrity::{BoundTradeIngressOutcome, IntegrityOutcome, TradeControllerOutcome},
    stream::source::SourceTradePublication,
};
impl ProducerHandle {
    /// Accepted market batches use acknowledged ingress. First coverage-review
    /// observations and empty accepted metadata use the integrity-only path.
    pub fn publish_source_trades(&self, payload: SourceTradePublication<'_>) -> Result<(), String> {
        let (records, recovered, integrity) = match payload {
            SourceTradePublication::Recovery(records) => (records, true, None),
            SourceTradePublication::RecoveryObserved {
                records,
                source_generation,
            } => (
                records,
                true,
                Some(RouterTradeIntegrity {
                    source_generation: Some(source_generation),
                    received_records: records.len(),
                    duplicate_records: 0,
                    identity_history_truncated: false,
                    coverage_history_unverified: true,
                    observations: vec![].into(),
                    baseline: Default::default(),
                }),
            ),
            SourceTradePublication::Integrity(IntegrityOutcome::Trades(
                BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(batch)),
            )) => (
                batch.trades.as_slice(),
                false,
                Some(RouterTradeIntegrity {
                    source_generation: Some(batch.source_generation),
                    received_records: batch.received_records,
                    duplicate_records: batch.duplicate_records,
                    identity_history_truncated: batch.identity_history_truncated,
                    coverage_history_unverified: batch.coverage_history_unverified,
                    observations: batch.observations.as_slice().into(),
                    baseline: batch.baseline,
                }),
            ),
            SourceTradePublication::Integrity(IntegrityOutcome::Trades(
                BoundTradeIngressOutcome::Processed(
                    TradeControllerOutcome::CoverageReviewRequired {
                        observations,
                        source_generation,
                    },
                ),
            )) => {
                return self
                    .publish_trade_integrity(
                        &RouterTradeIntegrity {
                            source_generation: Some(*source_generation),
                            received_records: 0,
                            duplicate_records: 0,
                            identity_history_truncated: false,
                            coverage_history_unverified: true,
                            observations: observations.as_slice().into(),
                            baseline: Default::default(),
                        },
                        true,
                    )
                    .map_err(|e| e.to_string());
            }
            _ => return Ok(()),
        };
        if records.is_empty() {
            return integrity.as_ref().map_or(Ok(()), |m| {
                self.publish_trade_integrity(m, false)
                    .map_err(|e| e.to_string())
            });
        }
        if !recovered {
            if let Some(metadata) = &integrity {
                self.retain_rejected_trade_evidence(metadata);
            }
        }
        self.publish_for_source(RouterPublication::Trades {
            records: records.into(),
            recovered,
            integrity,
        })
        .map(|_| ())
        .map_err(|e| e.to_string())
    }
    pub fn publish_source_depth(
        &self,
        events: &[crate::live::integrity::DepthPublicationEvent<'_>],
    ) -> Result<(), String> {
        if events.is_empty() {
            return Ok(());
        }
        match self.publish_for_source(RouterPublication::depth(events)) {
            Ok(_) => Ok(()),
            // A terminal Router fence already invalidates every consumer. This
            // acknowledges Source cleanup, not acceptance of fresh market data.
            Err(PublishError::Revoked | PublishError::Shutdown)
                if events.iter().all(|e| {
                    matches!(
                        e,
                        crate::live::integrity::DepthPublicationEvent::Invalidated { .. }
                    )
                }) =>
            {
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }
}
