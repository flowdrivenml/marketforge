//! Router-to-portable schema projection. Does not normalize or calculate prices.
use crate::shared::domain::canonical::{BookSide, Exchange, TradeSide};
use crate::{
    live::stream::router::*,
    stream_protocol::{self as p, market as m},
};
fn decimal(v: crate::shared::conversion::ConversionDecimal) -> m::Decimal {
    m::Decimal {
        coefficient: v.mantissa().to_string(),
        scale: 28,
    }
}
fn trade(t: &crate::shared::domain::canonical::Trade) -> m::Trade {
    m::Trade {
        event_timestamp_ns: t.envelope.event_timestamp_ns,
        system_timestamp_ns: t.envelope.system_timestamp_ns,
        exchange: match t.envelope.exchange {
            Exchange::Bybit => "bybit",
            Exchange::Binance => "binance",
            Exchange::Okx => "okx",
            Exchange::Bitget => "bitget",
            Exchange::GateIo => "gateio",
        }
        .into(),
        instrument_id: t.envelope.instrument_id,
        symbol: t.envelope.symbol.clone(),
        stream_id: t.envelope.stream_id.clone(),
        trade_id: t.trade_id.clone(),
        sequence: t.sequence,
        side: match t.side {
            TradeSide::Buy => "buy",
            TradeSide::Sell => "sell",
        }
        .into(),
        price: t.price.into(),
        quantity_base: t.quantity_base.map(Into::into),
        quantity_quote: t.quantity_quote.map(Into::into),
        quantity_contracts: t.quantity_contracts.map(Into::into),
        is_rpi: t.is_rpi,
        trade_iv: t.trade_iv.map(Into::into),
        mark_iv: t.mark_iv.map(Into::into),
        index_price: t.index_price.map(Into::into),
        mark_price: t.mark_price.map(Into::into),
    }
}
fn depth(d: &RouterDepthEvent, max_records: usize) -> Result<m::Depth, String> {
    let (g, b) = d.generations();
    let mut value = m::Depth {
        kind: "invalidated".into(),
        source_generation: g,
        book_generation: b,
        last_sequence: None,
        cross_sequence: None,
        timestamp_ns: None,
        timestamp_provenance: None,
        composition: None,
        internal_baseline_synchronized: None,
        client_book_initialized: false,
        operations: vec![],
    };
    match d {
        RouterDepthEvent::Invalidated { .. } => (),
        RouterDepthEvent::Snapshot {
            baseline,
            timestamp,
            ..
        }
        | RouterDepthEvent::Delta {
            baseline,
            timestamp,
            ..
        } => {
            value.last_sequence = Some(baseline.last_sequence);
            value.cross_sequence = baseline.cross_sequence;
            value.internal_baseline_synchronized = Some(baseline.synchronized);
            value.timestamp_ns = timestamp.latest_source.map(|t| t.timestamp_ns);
            value.timestamp_provenance=timestamp.latest_source.map(|t|match t.provenance {crate::live::infrastructure::normalization::DepthTimestampProvenance::Exchange=>"exchange",crate::live::infrastructure::normalization::DepthTimestampProvenance::ReceiveTimeFallback=>"receive_time_fallback"}.into());
            value.composition = Some(
                match timestamp.composition {
                crate::live::integrity::DepthTimestampComposition::CurrentBookHandoff => "current_book_handoff",
                    crate::live::integrity::DepthTimestampComposition::SingleMessage => {
                        "single_message"
                    }
                    crate::live::integrity::DepthTimestampComposition::RestReconciliation => {
                        "rest_reconciliation"
                    }
                    crate::live::integrity::DepthTimestampComposition::StagedSnapshotBridge => {
                        "staged_snapshot_bridge"
                    }
                }
                .into(),
            );
            value.kind = if matches!(d, RouterDepthEvent::Snapshot { .. }) {
                "snapshot_omitted"
            } else {
                "delta"
            }
            .into();
        }
    }
    if let RouterDepthEvent::Delta { changes, .. } = d {
        if changes.len() > max_records {
            return Err("publication_too_large".into());
        }
        value.operations = changes
            .iter()
            .map(|(side, price, level)| m::Level {
                operation: if level.is_zero() { "remove" } else { "set" }.into(),
                side: match side {
                    BookSide::Bid => "bid",
                    BookSide::Ask => "ask",
                }
                .into(),
                price: (*price).into(),
                quantity_base: level.quantity_base.map(Into::into),
                quantity_quote: level.quantity_quote.map(Into::into),
                quantity_contracts: level.quantity_contracts.map(Into::into),
                order_count: level.order_count,
            })
            .collect();
    }
    Ok(value)
}
fn origin(o: &EventOrigin) -> m::Position {
    m::Position {
        stream_id: o.stream_id.clone(),
        runtime_generation: o.generation.get(),
        publication_sequence: o.publication_sequence,
        record_ordinal: o.record_ordinal,
    }
}
fn native(n: &NativeMarketEvent, max: usize) -> Result<m::NativeEvent, String> {
    Ok(match n {
        NativeMarketEvent::Trade(t) => m::NativeEvent::Trade(trade(t)),
        NativeMarketEvent::Depth(d) => m::NativeEvent::Depth(depth(d, max)?),
    })
}
fn valuation(v: &EventValuation) -> m::Valuation {
    m::Valuation {
        target_quote: v.target_quote.clone(),
        rate: decimal(v.evaluation.rate),
        numerator: v.evaluation.numerator.to_string(),
        denominator: v.evaluation.denominator.to_string(),
        references: v
            .evaluation
            .references
            .iter()
            .map(|r| m::Reference {
                dataset_id: r.dataset_id,
                stream_id: r.stream_id.clone(),
                runtime_generation: v
                    .reference_generations
                    .iter()
                    .find(|(id, _)| id == &r.stream_id)
                    .map(|(_, g)| g.get()),
                timestamp_ns: r.key.timestamp_ns,
                stream_rank: r.key.stream_rank,
                row_sequence: r.key.task_rank,
                event_ordinal: r.key.event_ordinal,
                direction: match r.direction {
                    crate::shared::conversion::ConversionDirection::Direct => "direct",
                    crate::shared::conversion::ConversionDirection::Inverse => "inverse",
                }
                .into(),
                price: decimal(r.price),
            })
            .collect(),
        reference_integrity: v
            .reference_integrity
            .iter()
            .map(|e| (**e).clone())
            .collect(),
        path: v.path.clone(),
        values: v
            .values
            .iter()
            .map(|v| m::ConvertedValues {
                price: decimal(v.price_target),
                quantity_quote: v.quantity_quote_target.map(decimal),
            })
            .collect(),
    }
}
pub fn project(
    stream: &str,
    message: RouterStreamMessage,
    max: usize,
) -> Result<p::StreamMessage, String> {
    Ok(match message {
        RouterStreamMessage::IntegritySnapshot(s) => p::StreamMessage::IntegritySnapshot(s),
        RouterStreamMessage::IntegrityUpdate(u) => p::StreamMessage::IntegrityUpdate(u),
        RouterStreamMessage::Market(RouterDelivery::ContinuityLost(r)) => {
            p::StreamMessage::End(p::SubscriptionEnd {
                reason: match r {
                    ContinuityFailure::ConsumerCapacity => "consumer_capacity",
                    ContinuityFailure::ProducerRevoked => "producer_revoked",
                    ContinuityFailure::RouterShutdown => "router_shutdown",
                    ContinuityFailure::RouterDropped => "router_dropped",
                }
                .into(),
            })
        }
        RouterStreamMessage::Market(RouterDelivery::Publication {
            sequence,
            runtime_generation,
            publication,
            admission,
            ..
        }) => {
            let count = match publication.as_ref() {
                RouterPublication::Trades { records, .. } => records.len(),
                RouterPublication::Depth(e) => e.len(),
                RouterPublication::Derived(e) => e.len(),
                RouterPublication::ReplayComplete => {
                    return Ok(p::StreamMessage::End(p::SubscriptionEnd {
                        reason: "finite_input_complete".into(),
                    }));
                }
            };
            if count > max {
                return Err("publication_too_large".into());
            }
            let mut records = Vec::with_capacity(count);
            for i in 0..count {
                let mut r = m::Record {
                    ordinal: i as u64,
                    native: match publication.as_ref() {
                        RouterPublication::Trades { records, .. } => {
                            m::NativeEvent::Trade(trade(&records[i]))
                        }
                        RouterPublication::Depth(e) => m::NativeEvent::Depth(depth(&e[i], max)?),
                        RouterPublication::Derived(e) => native(&e[i].native, max)?,
                        _ => unreachable!(),
                    },
                    origin: None,
                    upstream: None,
                    processing_input_position: None,
                    processing_admission_ordinal: None,
                    baseline_barrier: None,
                    integrity: admission.integrity(i).map(|e| (**e).clone()),
                    valuation: None,
                };
                if let RouterPublication::Derived(e) = publication.as_ref() {
                    r.origin = Some(origin(&e[i].origin));
                    r.upstream = Some(origin(&e[i].upstream));
                    r.processing_input_position = Some(e[i].input_position as u64);
                    r.processing_admission_ordinal = Some(e[i].admission_ordinal);
                    r.baseline_barrier = Some(e[i].baseline_barrier);
                    r.integrity = e[i].evidence.integrity().map(|e| (**e).clone());
                    // Omitted snapshots carry no per-level valuations.
                    if !matches!(
                        e[i].native,
                        NativeMarketEvent::Depth(RouterDepthEvent::Snapshot { .. })
                    ) {
                        r.valuation = e[i].valuation.as_ref().map(valuation);
                    }
                }
                records.push(r);
            }
            p::StreamMessage::Publication(m::StreamPublication {
                stream_id: stream.into(),
                runtime_generation: runtime_generation.get(),
                router_sequence: sequence,
                recovered_records: matches!(
                    publication.as_ref(),
                    RouterPublication::Trades {
                        recovered: true,
                        ..
                    }
                ),
                records,
            })
        }
    })
}
