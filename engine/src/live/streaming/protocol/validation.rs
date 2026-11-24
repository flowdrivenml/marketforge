use super::codec::ProtocolError;
use super::{market::*, *};
fn number(s: &str, max: usize) -> bool {
    let s = s.strip_prefix('-').unwrap_or(s);
    !s.is_empty() && s.len() <= max && s.bytes().all(|c| c.is_ascii_digit())
}
fn decimal(d: &Decimal) -> bool {
    d.scale <= 28 && number(&d.coefficient, 78)
}
fn optional(d: &Option<Decimal>) -> bool {
    d.as_ref().is_none_or(decimal)
}
fn evidence(e: &DataIntegrityEvidence) -> bool {
    e.id.source.len() <= 4096
        && e.observations.len() <= MAX_OBSERVATIONS
        && e.id.runtime_generation.is_none_or(|g| g > 0)
        && e.id
            .revision
            .is_none_or(|r| r > 0 && e.id.runtime_generation.is_some())
}
fn record(r: &Record) -> bool {
    let native = match &r.native {
        NativeEvent::Trade(t) => {
            ["buy", "sell"].contains(&t.side.as_str())
                && decimal(&t.price)
                && [
                    &t.quantity_base,
                    &t.quantity_quote,
                    &t.quantity_contracts,
                    &t.trade_iv,
                    &t.mark_iv,
                    &t.index_price,
                    &t.mark_price,
                ]
                .into_iter()
                .all(optional)
        }
        NativeEvent::Depth(d) => {
            !d.client_book_initialized
                && ["delta", "snapshot_omitted", "invalidated"].contains(&d.kind.as_str())
                && (d.kind == "delta" || d.operations.is_empty())
                && d.operations.iter().all(|o| {
                    ["set", "remove"].contains(&o.operation.as_str())
                        && ["bid", "ask"].contains(&o.side.as_str())
                        && decimal(&o.price)
                        && [&o.quantity_base, &o.quantity_quote, &o.quantity_contracts]
                            .into_iter()
                            .all(optional)
                })
        }
    };
    native
        && r.integrity.as_ref().is_none_or(evidence)
        && r.valuation.as_ref().is_none_or(|v| {
            !matches!(&r.native,NativeEvent::Depth(d) if d.kind=="snapshot_omitted")
                && decimal(&v.rate)
                && number(&v.numerator, 4096)
                && number(&v.denominator, 4096)
                && v.denominator.bytes().any(|b| matches!(b, b'1'..=b'9'))
                && v.references.len() <= 128
                && v.reference_integrity.len() <= 128
                && v.path.len() <= 128
                && v.values.len() <= 16384
                && v.references.iter().all(|r| {
                    decimal(&r.price) && ["direct", "inverse"].contains(&r.direction.as_str())
                })
                && v.reference_integrity.iter().all(evidence)
                && v.values
                    .iter()
                    .all(|v| decimal(&v.price) && optional(&v.quantity_quote))
        })
}
pub(super) fn validate(message: &Message) -> Result<(), ProtocolError> {
    let valid = match message {
        Message::Hello { versions, schemas } => versions.len() <= 16 && schemas.len() <= 16,
        Message::Welcome {
            schemas,
            capabilities,
            ..
        } => schemas.len() <= 16 && capabilities.len() <= 16,
        Message::Streams(p) => p.streams.len() <= 128,
        Message::Stream(StreamMessage::Publication(p)) => {
            !p.records.is_empty() && p.records.len() <= 16384 && p.records.iter().all(record)
        }
        Message::Stream(StreamMessage::IntegritySnapshot(s)) => {
            s.sources.len() <= MAX_SOURCES
                && s.evidence_history.len() <= 32
                && s.sources.iter().chain(&s.evidence_history).all(evidence)
        }
        Message::Stream(StreamMessage::IntegrityUpdate(u)) => evidence(&u.evidence),
        _ => true,
    };
    if valid {
        Ok(())
    } else {
        Err(ProtocolError::Malformed)
    }
}
