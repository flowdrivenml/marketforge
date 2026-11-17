//! Depth payload [0,2], v1. One envelope per atomic event; source metadata is
//! independent and exact. Compression/framing/checksums remain spool concerns.
use super::typed::{Input, decimal, optional, string};
use crate::shared::{
    domain::canonical::*,
    error::{MarketForgeError, Result},
};
use std::io::{self, Write};
fn bad() -> MarketForgeError {
    MarketForgeError::InvalidConfiguration("corrupt typed scratch depth".into())
}
fn envelope(w: &mut impl Write, e: &EventEnvelope) -> io::Result<()> {
    w.write_all(&e.event_timestamp_ns.to_le_bytes())?;
    optional(w, e.system_timestamp_ns, |w, v| {
        w.write_all(&v.to_le_bytes())
    })?;
    w.write_all(&[match e.exchange {
        Exchange::Bybit => 1,
        Exchange::Binance => 2,
        Exchange::Okx => 3,
        Exchange::Bitget => 4,
        Exchange::GateIo => 5,
    }])?;
    w.write_all(&e.instrument_id.to_le_bytes())?;
    string(w, &e.symbol)?;
    string(w, &e.stream_id)
}
pub(super) fn encode(w: &mut impl Write, o: &DepthProcessingOutcome) -> io::Result<()> {
    if o.events
        .first()
        .is_some_and(|first| o.events.iter().any(|e| e.envelope != first.envelope))
    {
        return serde_json::to_writer(w, o).map_err(io::Error::other);
    }
    w.write_all(&[
        0,
        2,
        match o.boundary {
            DepthEventBoundary::Initialization => 0,
            DepthEventBoundary::Changes => 1,
        },
        match o.operation {
            DepthSourceOperation::Snapshot => 0,
            DepthSourceOperation::AbsoluteUpdate => 1,
            DepthSourceOperation::RelativeUpdate => 2,
        },
    ])?;
    let s = &o.source;
    optional(w, s.source_event_ordinal, |w, v| {
        w.write_all(&v.to_le_bytes())
    })?;
    w.write_all(&s.event_timestamp_ns.to_le_bytes())?;
    optional(w, s.system_timestamp_ns, |w, v| {
        w.write_all(&v.to_le_bytes())
    })?;
    optional(w, s.sequence_start, |w, v| w.write_all(&v.to_le_bytes()))?;
    optional(w, s.sequence_end, |w, v| w.write_all(&v.to_le_bytes()))?;
    w.write_all(&(o.events.len() as u64).to_le_bytes())?;
    if let Some(e) = o.events.first() {
        envelope(w, &e.envelope)?;
    }
    for e in &o.events {
        w.write_all(&[match e.side {
            BookSide::Bid => 1,
            BookSide::Ask => 2,
        }])?;
        decimal(w, e.price)?;
        optional(w, e.quantity_base, decimal)?;
        optional(w, e.quantity_quote, decimal)?;
        optional(w, e.quantity_contracts, decimal)?;
        optional(w, e.order_count, |w, v| w.write_all(&v.to_le_bytes()))?;
    }
    Ok(())
}
fn header(
    r: &mut Input<'_>,
) -> Result<(
    DepthEventBoundary,
    DepthSourceOperation,
    DepthSourceEventMetadata,
    usize,
)> {
    if r.take::<2>()? != [0, 2] {
        return Err(bad());
    }
    let boundary = match r.byte()? {
        0 => DepthEventBoundary::Initialization,
        1 => DepthEventBoundary::Changes,
        _ => return Err(bad()),
    };
    let operation = match r.byte()? {
        0 => DepthSourceOperation::Snapshot,
        1 => DepthSourceOperation::AbsoluteUpdate,
        2 => DepthSourceOperation::RelativeUpdate,
        _ => return Err(bad()),
    };
    let source = DepthSourceEventMetadata {
        source_event_ordinal: r.optional(Input::u64)?,
        event_timestamp_ns: r.i64()?,
        system_timestamp_ns: r.optional(Input::i64)?,
        sequence_start: r.optional(Input::u64)?,
        sequence_end: r.optional(Input::u64)?,
    };
    let count = usize::try_from(r.u64()?).map_err(|_| bad())?;
    if count > r.0.len() / 26 {
        return Err(bad());
    }
    Ok((boundary, operation, source, count))
}
fn read_envelope(r: &mut Input<'_>) -> Result<EventEnvelope> {
    let event_timestamp_ns = r.i64()?;
    let system_timestamp_ns = r.optional(Input::i64)?;
    let exchange = match r.byte()? {
        1 => Exchange::Bybit,
        2 => Exchange::Binance,
        3 => Exchange::Okx,
        4 => Exchange::Bitget,
        5 => Exchange::GateIo,
        _ => return Err(bad()),
    };
    let instrument_id = r.i64()?;
    Ok(EventEnvelope {
        event_timestamp_ns,
        system_timestamp_ns,
        exchange,
        instrument_id,
        symbol: r.string()?,
        stream_id: r.string()?,
    })
}
pub(super) fn allocation(bytes: &[u8]) -> Result<u64> {
    if bytes.first() != Some(&0) {
        return (bytes.len() as u64)
            .checked_mul(64)
            .and_then(|n| n.checked_add(1024))
            .ok_or_else(bad);
    }
    let mut r = Input(bytes);
    let (_, _, _, count) = header(&mut r)?;
    let strings = if count > 0 {
        r.i64()?;
        r.optional(Input::i64)?;
        r.byte()?;
        r.i64()?;
        r.string_ref()?
            .len()
            .checked_add(r.string_ref()?.len())
            .ok_or_else(bad)?
    } else {
        0
    };
    // Own Vec capacity, exact repeated envelope strings and allocator slack.
    (count as u64)
        .checked_mul(
            (std::mem::size_of::<L2LevelUpdate>() as u64)
                .checked_mul(4)
                .and_then(|n| n.checked_add((strings as u64).checked_mul(2)?))
                .ok_or_else(bad)?,
        )
        .and_then(|n| n.checked_add(4096))
        .ok_or_else(bad)
}
pub(super) fn decode(bytes: &[u8]) -> Result<DepthProcessingOutcome> {
    if bytes.first() != Some(&0) {
        return serde_json::from_slice(bytes).map_err(|e| {
            MarketForgeError::InvalidConfiguration(format!("corrupt scratch depth: {e}"))
        });
    }
    let mut r = Input(bytes);
    let (boundary, operation, source, count) = header(&mut r)?;
    let shared = if count > 0 {
        Some(read_envelope(&mut r)?)
    } else {
        None
    };
    if count > r.0.len() / 26 {
        return Err(bad());
    }
    let mut events = Vec::with_capacity(count);
    for _ in 0..count {
        let side = match r.byte()? {
            1 => BookSide::Bid,
            2 => BookSide::Ask,
            _ => return Err(bad()),
        };
        events.push(L2LevelUpdate {
            envelope: shared.as_ref().unwrap().clone(),
            side,
            price: r.decimal()?,
            quantity_base: r.optional(Input::decimal)?,
            quantity_quote: r.optional(Input::decimal)?,
            quantity_contracts: r.optional(Input::decimal)?,
            order_count: r.optional(Input::u64)?,
        });
    }
    if !r.0.is_empty() {
        return Err(bad());
    }
    Ok(DepthProcessingOutcome {
        boundary,
        operation,
        source,
        events,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn sample() -> DepthProcessingOutcome {
        let d: rust_decimal::Decimal = "-0.1234567890123456789012345678".parse().unwrap();
        DepthProcessingOutcome {
            boundary: DepthEventBoundary::Changes,
            operation: DepthSourceOperation::RelativeUpdate,
            source: DepthSourceEventMetadata {
                source_event_ordinal: Some(u64::MAX),
                event_timestamp_ns: i64::MIN,
                system_timestamp_ns: Some(i64::MAX),
                sequence_start: None,
                sequence_end: Some(u64::MAX),
            },
            events: vec![L2LevelUpdate {
                envelope: EventEnvelope {
                    event_timestamp_ns: -3,
                    system_timestamp_ns: None,
                    exchange: Exchange::GateIo,
                    instrument_id: i64::MAX,
                    symbol: "BTC_€".into(),
                    stream_id: "exact-stream".into(),
                },
                side: BookSide::Ask,
                price: d,
                quantity_base: Some(d),
                quantity_quote: None,
                quantity_contracts: Some(rust_decimal::Decimal::ZERO),
                order_count: Some(u64::MAX),
            }],
        }
    }
    #[test]
    fn exact_versioned_roundtrip_empty_mixed_envelopes_and_bad_payloads() {
        let mut event = sample();
        for mode in 0..3 {
            if mode == 1 {
                let mut other = event.events[0].clone();
                other.envelope.symbol = "other".into();
                event.events.push(other);
            }
            if mode == 2 {
                event.events.clear();
            }
            let mut bytes = Vec::new();
            encode(&mut bytes, &event).unwrap();
            assert_eq!(
                serde_json::to_value(decode(&bytes).unwrap()).unwrap(),
                serde_json::to_value(&event).unwrap()
            );
            assert!(allocation(&bytes).unwrap() > 0);
            assert!(decode(&bytes[..bytes.len() - 1]).is_err());
            let mut tail = bytes.clone();
            tail.extend_from_slice(b"invalid");
            assert!(decode(&tail).is_err());
            if bytes[0] == 0 {
                bytes[1] = 99;
                assert!(decode(&bytes).is_err());
            }
        }
        // A hostile count cannot allocate before validating available payload.
        let mut bytes = vec![0, 2, 0, 0, 0];
        bytes.extend_from_slice(&0i64.to_le_bytes());
        bytes.extend_from_slice(&[0, 0, 0]);
        bytes.extend_from_slice(&u64::MAX.to_le_bytes());
        assert!(allocation(&bytes).is_err());
        assert!(decode(&bytes).is_err());
    }
}
