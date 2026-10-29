use std::collections::BTreeMap;

use thiserror::Error;

use crate::{
    live::infrastructure::normalization::{
        NormalizationSource, NormalizedTrades, TradeRecordSemantics,
    },
    shared::domain::canonical::Trade,
};

use super::SourceTradeLiveBuffer;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SourceTradeReconcileError {
    #[error("REST trade provenance is required")]
    WrongRestSource,

    #[error("individual trade semantics are required")]
    WrongSemantics,

    #[error("recovery input belongs to another stream")]
    WrongStream,

    #[error("recovery requires a numeric accepted baseline")]
    MissingBaseline,

    #[error("trade at input index {0} has an invalid numeric identity")]
    InvalidIdentity(usize),

    #[error("same native trade ID has conflicting canonical contents")]
    ConflictingIdentity,

    #[error("recovery candidate exceeds its record limit")]
    CapacityExceeded,

    #[error("recovery candidate contains no trades beyond the baseline")]
    EmptyCandidate,

    #[error("numeric ordering is discontinuous")]
    DiscontinuousCandidate,

    #[error("REST recovery does not reach the accepted numeric baseline")]
    MissingRestBaselineAnchor,

    #[error("REST recovery does not overlap the triggering coverage batch")]
    MissingRestTriggerOverlap,

    #[error("REST trades do not establish the complete numeric recovery bridge")]
    IncompleteRestBridge,
}

/// Ordered reconciliation input.
///
/// IMPORTANT:
/// A contiguous numeric candidate is not, by itself,
/// proof that an exchange endpoint provides complete
/// execution history.
///
/// No integrity state has been modified.
#[derive(Debug)]
pub struct SourceTradeNumericCandidate {
    pub baseline_id: u64,
    pub highest_id: u64,
    pub trades: Vec<Trade>,
}

impl SourceTradeNumericCandidate {
    pub fn build(
        stream_id: &str,
        baseline_id: Option<u64>,
        rest: &NormalizedTrades,
        triggering: &[Trade],
        buffered: &SourceTradeLiveBuffer,
        max_records: usize,
    ) -> Result<Self, SourceTradeReconcileError> {
        use SourceTradeReconcileError as Error;

        let baseline_id = baseline_id.ok_or(Error::MissingBaseline)?;

        if rest.source() != NormalizationSource::Rest {
            return Err(Error::WrongRestSource);
        }

        if rest.semantics() != TradeRecordSemantics::Individual {
            return Err(Error::WrongSemantics);
        }

        if rest.stream_id() != stream_id {
            return Err(Error::WrongStream);
        }

        if max_records == 0 {
            return Err(Error::CapacityExceeded);
        }
        // REST must provide the first execution immediately
        // following the committed numeric baseline.
        //
        // This is a necessary anchor for our initial
        // consecutive-numeric-ID recovery strategy.
        let first_required = baseline_id
            .checked_add(1)
            .ok_or(Error::MissingRestBaselineAnchor);

        let first_required = first_required?;
        // Native numeric IDs are used only for ordering
        // within this one authoritative logical stream.
        //
        // The map stores complete canonical records,
        // not just their identifiers.
        // Inspect REST identities independently of the
        // merged candidate.
        //
        // Otherwise, buffered WebSocket messages could fill
        // every missing ID while REST contributed insufficient
        // evidence to establish a recovery bridge.
        let mut rest_ids = std::collections::HashSet::new();

        for (index, trade) in rest.trades().iter().enumerate() {
            if trade.envelope.stream_id != stream_id {
                return Err(Error::WrongStream);
            }

            let id = trade
                .trade_id
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(Error::InvalidIdentity(index))?;

            rest_ids.insert(id);
        }

        if !rest_ids.contains(&first_required) {
            return Err(Error::MissingRestBaselineAnchor);
        }

        // At least one trade from the batch that caused the
        // coverage hold must also be present in REST history.
        //
        // This links the recovery response to the actual
        // discontinuity, rather than merely to a later live
        // sequence.
        //
        // The triggering batch must be nonempty.
        let overlaps_trigger = triggering.iter().any(|trade| {
            trade
                .trade_id
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .is_some_and(|id| rest_ids.contains(&id))
        });

        if !overlaps_trigger {
            return Err(Error::MissingRestTriggerOverlap);
        }
        // ============================================================
        // VERIFY REST COVERAGE THROUGH THE TRIGGERING BATCH
        // ============================================================

        // The REST response must independently bridge the interval
        // from the first uncommitted ID through the highest numeric
        // ID in the triggering batch.
        //
        // Buffered WebSocket records may extend the candidate, but
        // cannot fill holes in this REST recovery bridge.
        let mut highest_trigger_id = None::<u64>;

        for (index, trade) in triggering.iter().enumerate() {
            if trade.envelope.stream_id != stream_id {
                return Err(Error::WrongStream);
            }

            let id = trade
                .trade_id
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(Error::InvalidIdentity(index))?;

            if id > baseline_id {
                highest_trigger_id = Some(highest_trigger_id.map_or(id, |highest| highest.max(id)));
            }
        }

        let highest_trigger_id = highest_trigger_id.ok_or(Error::MissingRestTriggerOverlap)?;

        // Bound the interval before iterating over numeric IDs.
        //
        // An exchange-controlled ID must never cause an enormous
        // allocation or unbounded recovery-validation loop.
        let required_records = highest_trigger_id
            .checked_sub(baseline_id)
            .ok_or(Error::IncompleteRestBridge)?;

        if required_records > u64::try_from(max_records).unwrap_or(u64::MAX) {
            return Err(Error::CapacityExceeded);
        }

        // REST itself must provide every ID in this numeric interval.
        //
        // This is specific to the initial consecutive-ID recovery
        // strategy. It must not be applied to opaque IDs or exchange
        // profiles without compatible numbering semantics.
        for id in first_required..=highest_trigger_id {
            if !rest_ids.contains(&id) {
                return Err(Error::IncompleteRestBridge);
            }
        }
        let mut records = BTreeMap::<u64, Trade>::new();

        let inputs = rest
            .trades()
            .iter()
            .chain(triggering.iter())
            .chain(buffered.batches().flat_map(|batch| batch.trades().iter()));

        for (index, trade) in inputs.enumerate() {
            if trade.envelope.stream_id != stream_id {
                return Err(Error::WrongStream);
            }

            let id = trade
                .trade_id
                .as_deref()
                .and_then(|value| value.parse::<u64>().ok())
                .ok_or(Error::InvalidIdentity(index))?;

            // Records at or below the committed baseline
            // are overlap observations, not new trades.
            if id <= baseline_id {
                continue;
            }

            if let Some(existing) = records.get(&id) {
                // Duplicate identity is acceptable only when
                // the canonical execution contents agree.
                //
                // Do not silently prefer REST or WebSocket
                // if their economic contents conflict.
                if existing.envelope.exchange != trade.envelope.exchange
                    || existing.envelope.instrument_id != trade.envelope.instrument_id
                    || existing.envelope.symbol != trade.envelope.symbol
                    || existing.envelope.event_timestamp_ns != trade.envelope.event_timestamp_ns
                    || existing.side != trade.side
                    || existing.price != trade.price
                    || existing.quantity_base != trade.quantity_base
                    || existing.quantity_quote != trade.quantity_quote
                    || existing.quantity_contracts != trade.quantity_contracts
                    || existing.sequence != trade.sequence
                {
                    return Err(Error::ConflictingIdentity);
                }

                continue;
            }

            if records.len() >= max_records {
                return Err(Error::CapacityExceeded);
            }

            records.insert(id, trade.clone());
        }

        if records.is_empty() {
            return Err(Error::EmptyCandidate);
        }

        // This verifies numeric adjacency only.
        //
        // A separate exchange-specific recovery contract
        // must establish whether adjacency is meaningful
        // evidence of execution completeness.
        let mut previous = baseline_id;

        for &current in records.keys() {
            if previous.checked_add(1) != Some(current) {
                return Err(Error::DiscontinuousCandidate);
            }

            previous = current;
        }

        Ok(Self {
            baseline_id,
            highest_id: previous,
            trades: records.into_values().collect(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use rust_decimal::Decimal;

    use crate::{
        live::{integrity::TradeSourceEpoch, stream::source::SourceTradeBufferLimits},
        shared::domain::canonical::{EventEnvelope, Exchange, TradeSide},
    };

    const STREAM: &str = "binance:spot:BTCUSDT:trades";

    fn epoch() -> TradeSourceEpoch {
        TradeSourceEpoch {
            stream_generation: 10,
            connection_generation: 20,
            subscription_generation: 30,
        }
    }

    fn trade(id: u64) -> Trade {
        Trade {
            envelope: EventEnvelope {
                event_timestamp_ns: id as i64 * 1_000_000,
                system_timestamp_ns: None,
                exchange: Exchange::Binance,
                instrument_id: 42,
                symbol: "BTCUSDT".into(),
                stream_id: STREAM.into(),
            },

            trade_id: Some(id.to_string()),
            sequence: None,
            side: TradeSide::Buy,
            price: Decimal::from(80_000),
            quantity_base: Some(Decimal::ONE),
            quantity_quote: None,
            quantity_contracts: None,
            is_rpi: None,
            trade_iv: None,
            mark_iv: None,
            index_price: None,
            mark_price: None,
        }
    }

    fn message(source: NormalizationSource, ids: &[u64]) -> NormalizedTrades {
        NormalizedTrades::new(
            STREAM,
            source,
            TradeRecordSemantics::Individual,
            ids.iter().copied().map(trade).collect(),
        )
        .unwrap()
    }

    fn buffer() -> SourceTradeLiveBuffer {
        SourceTradeLiveBuffer::new(
            STREAM,
            epoch(),
            SourceTradeBufferLimits {
                max_batches: 10,
                max_records: 100,
                max_bytes: 100_000,
            },
        )
        .unwrap()
    }

    #[test]
    fn merges_rest_triggering_and_live_overlap() {
        let mut live = buffer();

        live.push(
            epoch(),
            message(NormalizationSource::WebSocket, &[106, 107]),
            200,
        )
        .unwrap();

        let rest = message(NormalizationSource::Rest, &[101, 102, 103, 104, 105, 106]);

        let triggering = vec![trade(105)];

        let candidate =
            SourceTradeNumericCandidate::build(STREAM, Some(100), &rest, &triggering, &live, 100)
                .unwrap();

        let ids: Vec<u64> = candidate
            .trades
            .iter()
            .map(|trade| trade.trade_id.as_ref().unwrap().parse().unwrap())
            .collect();

        assert_eq!(ids, vec![101, 102, 103, 104, 105, 106, 107]);

        assert_eq!(candidate.baseline_id, 100);
        assert_eq!(candidate.highest_id, 107);
    }

    #[test]
    fn incomplete_rest_coverage_is_rejected() {
        let rest = message(NormalizationSource::Rest, &[103, 104, 105]);

        let result = SourceTradeNumericCandidate::build(
            STREAM,
            Some(100),
            &rest,
            &[trade(105)],
            &buffer(),
            100,
        );

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::MissingRestBaselineAnchor)
        ));
    }

    #[test]
    fn conflicting_duplicate_identity_is_rejected() {
        let rest = message(NormalizationSource::Rest, &[101, 102]);

        let mut conflicting = trade(102);
        conflicting.price = Decimal::from(90_000);

        let result = SourceTradeNumericCandidate::build(
            STREAM,
            Some(100),
            &rest,
            &[conflicting],
            &buffer(),
            100,
        );

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::ConflictingIdentity)
        ));
    }

    #[test]
    fn websocket_input_cannot_impersonate_rest_recovery() {
        let wrong_source = message(NormalizationSource::WebSocket, &[101, 102]);

        let result = SourceTradeNumericCandidate::build(
            STREAM,
            Some(100),
            &wrong_source,
            &[],
            &buffer(),
            100,
        );

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::WrongRestSource)
        ));
    }

    #[test]
    fn candidate_is_bounded() {
        let rest = message(NormalizationSource::Rest, &[101, 102, 103]);

        let result = SourceTradeNumericCandidate::build(
            STREAM,
            Some(100),
            &rest,
            &[trade(102)],
            &buffer(),
            2,
        );

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::CapacityExceeded)
        ));
    }
    #[test]
    fn rest_must_reach_accepted_baseline() {
        let rest = message(NormalizationSource::Rest, &[102, 103, 104, 105]);

        let result = SourceTradeNumericCandidate::build(
            STREAM,
            Some(100),
            &rest,
            &[trade(105)],
            &buffer(),
            100,
        );

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::MissingRestBaselineAnchor)
        ));
    }

    #[test]
    fn rest_must_overlap_triggering_batch() {
        let mut live = buffer();

        live.push(
            epoch(),
            message(NormalizationSource::WebSocket, &[102, 103, 104, 106, 107]),
            200,
        )
        .unwrap();

        let rest = message(NormalizationSource::Rest, &[101]);

        let result =
            SourceTradeNumericCandidate::build(STREAM, Some(100), &rest, &[trade(105)], &live, 100);

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::MissingRestTriggerOverlap)
        ));
    }

    #[test]
    fn rest_bridge_with_trigger_overlap_is_accepted() {
        let mut live = buffer();

        live.push(
            epoch(),
            message(NormalizationSource::WebSocket, &[106, 107]),
            200,
        )
        .unwrap();

        let rest = message(
            NormalizationSource::Rest,
            &[100, 101, 102, 103, 104, 105, 106],
        );

        let candidate =
            SourceTradeNumericCandidate::build(STREAM, Some(100), &rest, &[trade(105)], &live, 100)
                .unwrap();

        assert_eq!(candidate.baseline_id, 100);
        assert_eq!(candidate.highest_id, 107);
        assert_eq!(candidate.trades.len(), 7);
    }

    #[test]
    fn empty_triggering_batch_cannot_authorize_recovery() {
        let rest = message(NormalizationSource::Rest, &[101, 102, 103]);

        let result =
            SourceTradeNumericCandidate::build(STREAM, Some(100), &rest, &[], &buffer(), 100);

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::MissingRestTriggerOverlap)
        ));
    }
    #[test]
    fn rest_with_baseline_anchor_but_missing_intermediate_ids_is_rejected() {
        let rest = message(NormalizationSource::Rest, &[101, 103, 104, 105]);

        let result = SourceTradeNumericCandidate::build(
            STREAM,
            Some(100),
            &rest,
            &[trade(105)],
            &buffer(),
            100,
        );

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::IncompleteRestBridge)
        ));
    }
    #[test]
    fn buffered_websocket_cannot_fill_missing_rest_bridge() {
        let mut live = buffer();

        live.push(
            epoch(),
            message(NormalizationSource::WebSocket, &[102, 103, 104, 106]),
            200,
        )
        .unwrap();

        let rest = message(NormalizationSource::Rest, &[101, 105]);

        let result =
            SourceTradeNumericCandidate::build(STREAM, Some(100), &rest, &[trade(105)], &live, 100);

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::IncompleteRestBridge)
        ));
    }

    #[test]
    fn complete_rest_bridge_allows_live_extension() {
        let mut live = buffer();

        live.push(
            epoch(),
            message(NormalizationSource::WebSocket, &[105, 106, 107]),
            200,
        )
        .unwrap();

        let rest = message(NormalizationSource::Rest, &[101, 102, 103, 104, 105]);

        let candidate =
            SourceTradeNumericCandidate::build(STREAM, Some(100), &rest, &[trade(105)], &live, 100)
                .unwrap();

        let ids: Vec<u64> = candidate
            .trades
            .iter()
            .map(|trade| trade.trade_id.as_deref().unwrap().parse().unwrap())
            .collect();

        assert_eq!(ids, vec![101, 102, 103, 104, 105, 106, 107]);

        assert_eq!(candidate.highest_id, 107);
    }

    #[test]
    fn rest_bridge_exceeding_capacity_is_rejected() {
        let rest = message(NormalizationSource::Rest, &[101, 200]);

        let result = SourceTradeNumericCandidate::build(
            STREAM,
            Some(100),
            &rest,
            &[trade(200)],
            &buffer(),
            10,
        );

        assert!(matches!(
            result,
            Err(SourceTradeReconcileError::CapacityExceeded)
        ));
    }
}
