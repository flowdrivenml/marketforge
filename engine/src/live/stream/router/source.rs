//! Source callback adapters. The dispatch core has no Source runtime dependency.
use super::*;
use crate::live::{
    integrity::{BoundTradeIngressOutcome, IntegrityOutcome, TradeControllerOutcome},
    stream::source::SourceTradePublication,
};
impl ProducerHandle {
    /// Adapter for Source's synchronous acknowledgment callback. Non-market
    /// integrity outcomes and empty accepted batches create no publication.
    pub fn publish_source_trades(&self, payload: SourceTradePublication<'_>) -> Result<(), String> {
        let (records, recovered, integrity) = match payload {
            SourceTradePublication::Recovery(records) => (records, true, None),
            SourceTradePublication::Integrity(IntegrityOutcome::Trades(
                BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(batch)),
            )) => (
                batch.trades.as_slice(),
                false,
                Some(RouterTradeIntegrity {
                    received_records: batch.received_records,
                    duplicate_records: batch.duplicate_records,
                    identity_history_truncated: batch.identity_history_truncated,
                    coverage_history_unverified: batch.coverage_history_unverified,
                    observations: batch.observations.as_slice().into(),
                    baseline: batch.baseline,
                }),
            ),
            _ => return Ok(()),
        };
        if records.is_empty() {
            return Ok(());
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
