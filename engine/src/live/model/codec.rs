//! Versioned logical event DTOs and an exact JSON interchange codec.
//! This format is NOT the publisher binary protocol or a PostgreSQL row mapping.
//! Financial numbers are decimal strings, never IEEE floats or Rust memory bytes.
use super::{
    cursor::StreamCursor,
    envelope::{EventProvenance, EventTiming, LiveEvent, LiveL2Group, LivePayload},
};
use crate::canonical::{BookSide, EventEnvelope, L2LevelUpdate, Trade, TradeSide};
use crate::formats::depth::{DepthEventBoundary, DepthSourceEventMetadata, DepthSourceOperation};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::{io::Write, sync::Arc};

pub const EVENT_SCHEMA_VERSION: u16 = 1;
#[derive(Debug, thiserror::Error)]
pub enum CodecError {
    #[error("unsupported event schema version: {0}")]
    Version(u16),
    #[error("event exceeds codec limits")]
    Limit,
    #[error("invalid event: {0:?}")]
    InvalidEvent(super::envelope::LiveEventError),
    #[error("invalid event encoding: {0}")]
    Encoding(#[from] serde_json::Error),
}

pub(crate) mod decimal_text {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        value: &Decimal,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&value.to_string())
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Decimal, D::Error> {
        let text = String::deserialize(d)?;
        // No exponent, whitespace, locale, or floating-point representation.
        if text.is_empty()
            || !text
                .bytes()
                .all(|b| b.is_ascii_digit() || b == b'-' || b == b'.')
        {
            return Err(serde::de::Error::custom("expected exact decimal text"));
        }
        Decimal::from_str_exact(&text).map_err(serde::de::Error::custom)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ExactDecimal(#[serde(with = "decimal_text")] pub Decimal);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TradeDtoV1 {
    pub envelope: EventEnvelope,
    pub trade_id: Option<String>,
    pub sequence: Option<u64>,
    pub side: TradeSide,
    pub price: ExactDecimal,
    pub quantity_base: Option<ExactDecimal>,
    pub quantity_quote: Option<ExactDecimal>,
    pub quantity_contracts: Option<ExactDecimal>,
    pub is_rpi: Option<bool>,
    pub trade_iv: Option<ExactDecimal>,
    pub mark_iv: Option<ExactDecimal>,
    pub index_price: Option<ExactDecimal>,
    pub mark_price: Option<ExactDecimal>,
}
impl From<Trade> for TradeDtoV1 {
    fn from(value: Trade) -> Self {
        Self {
            envelope: value.envelope,
            trade_id: value.trade_id,
            sequence: value.sequence,
            side: value.side,
            price: ExactDecimal(value.price),
            quantity_base: value.quantity_base.map(ExactDecimal),
            quantity_quote: value.quantity_quote.map(ExactDecimal),
            quantity_contracts: value.quantity_contracts.map(ExactDecimal),
            is_rpi: value.is_rpi,
            trade_iv: value.trade_iv.map(ExactDecimal),
            mark_iv: value.mark_iv.map(ExactDecimal),
            index_price: value.index_price.map(ExactDecimal),
            mark_price: value.mark_price.map(ExactDecimal),
        }
    }
}
impl From<TradeDtoV1> for Trade {
    fn from(value: TradeDtoV1) -> Self {
        Self {
            envelope: value.envelope,
            trade_id: value.trade_id,
            sequence: value.sequence,
            side: value.side,
            price: value.price.0,
            quantity_base: value.quantity_base.map(|n| n.0),
            quantity_quote: value.quantity_quote.map(|n| n.0),
            quantity_contracts: value.quantity_contracts.map(|n| n.0),
            is_rpi: value.is_rpi,
            trade_iv: value.trade_iv.map(|n| n.0),
            mark_iv: value.mark_iv.map(|n| n.0),
            index_price: value.index_price.map(|n| n.0),
            mark_price: value.mark_price.map(|n| n.0),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LevelDtoV1 {
    pub envelope: EventEnvelope,
    pub side: BookSide,
    pub price: ExactDecimal,
    pub quantity_base: Option<ExactDecimal>,
    pub quantity_quote: Option<ExactDecimal>,
    pub quantity_contracts: Option<ExactDecimal>,
    pub order_count: Option<u64>,
}
impl From<L2LevelUpdate> for LevelDtoV1 {
    fn from(value: L2LevelUpdate) -> Self {
        Self {
            envelope: value.envelope,
            side: value.side,
            price: ExactDecimal(value.price),
            quantity_base: value.quantity_base.map(ExactDecimal),
            quantity_quote: value.quantity_quote.map(ExactDecimal),
            quantity_contracts: value.quantity_contracts.map(ExactDecimal),
            order_count: value.order_count,
        }
    }
}
impl From<LevelDtoV1> for L2LevelUpdate {
    fn from(value: LevelDtoV1) -> Self {
        Self {
            envelope: value.envelope,
            side: value.side,
            price: value.price.0,
            quantity_base: value.quantity_base.map(|n| n.0),
            quantity_quote: value.quantity_quote.map(|n| n.0),
            quantity_contracts: value.quantity_contracts.map(|n| n.0),
            order_count: value.order_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum PayloadDtoV1 {
    Trade {
        trade: TradeDtoV1,
    },
    L2Group {
        boundary: DepthEventBoundary,
        operation: DepthSourceOperation,
        source: DepthSourceEventMetadata,
        levels: Vec<LevelDtoV1>,
    },
}
/// A schema version is always explicit. Future incompatible versions must use a
/// separate mapping; unknown versions are rejected before event interpretation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRecordV1 {
    pub schema_version: u16,
    pub cursor: StreamCursor,
    pub provenance: EventProvenance,
    pub timing: EventTiming,
    pub payload: PayloadDtoV1,
}
impl TryFrom<&LiveEvent> for EventRecordV1 {
    type Error = CodecError;
    fn try_from(event: &LiveEvent) -> Result<Self, Self::Error> {
        event.check().map_err(CodecError::InvalidEvent)?;
        let payload = match &event.payload {
            LivePayload::Trade(trade) => PayloadDtoV1::Trade {
                trade: trade.clone().into(),
            },
            LivePayload::L2Group(group) => PayloadDtoV1::L2Group {
                boundary: group.grouped_boundary,
                operation: group.grouped_operation,
                source: group.source.clone(),
                levels: group
                    .grouped_levels
                    .iter()
                    .cloned()
                    .map(Into::into)
                    .collect(),
            },
        };
        Ok(Self {
            schema_version: EVENT_SCHEMA_VERSION,
            cursor: event.cursor.clone(),
            provenance: event.provenance.clone(),
            timing: event.timing.clone(),
            payload,
        })
    }
}
impl TryFrom<EventRecordV1> for LiveEvent {
    type Error = CodecError;
    fn try_from(record: EventRecordV1) -> Result<Self, Self::Error> {
        if record.schema_version != EVENT_SCHEMA_VERSION {
            return Err(CodecError::Version(record.schema_version));
        }
        let payload = match record.payload {
            PayloadDtoV1::Trade { trade } => LivePayload::Trade(trade.into()),
            PayloadDtoV1::L2Group {
                boundary,
                operation,
                source,
                levels,
            } => LivePayload::L2Group(LiveL2Group::new(
                boundary,
                operation,
                levels.into_iter().map(Into::into).collect(),
                source,
            )),
        };
        Self::try_new(record.cursor, payload, record.provenance, record.timing)
            .map_err(CodecError::InvalidEvent)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct EventCodec {
    max_bytes: usize,
    max_levels: usize,
}
impl EventCodec {
    pub fn new(max_bytes: usize, max_levels: usize) -> Result<Self, CodecError> {
        if max_bytes == 0 || max_levels == 0 {
            return Err(CodecError::Limit);
        }
        Ok(Self {
            max_bytes,
            max_levels,
        })
    }
    fn check_levels(&self, payload: &LivePayload) -> Result<(), CodecError> {
        if matches!(payload, LivePayload::L2Group(g) if g.grouped_levels.len() > self.max_levels) {
            return Err(CodecError::Limit);
        }
        Ok(())
    }
    pub fn encode(&self, event: &LiveEvent) -> Result<Vec<u8>, CodecError> {
        self.check_levels(&event.payload)?;
        let record = EventRecordV1::try_from(event)?;
        let mut writer = LimitedWriter {
            bytes: Vec::new(),
            limit: self.max_bytes,
        };
        serde_json::to_writer(&mut writer, &record).map_err(|e| {
            if e.is_io() {
                CodecError::Limit
            } else {
                CodecError::Encoding(e)
            }
        })?;
        Ok(writer.bytes)
    }
    pub fn decode(&self, bytes: &[u8]) -> Result<Arc<LiveEvent>, CodecError> {
        if bytes.len() > self.max_bytes {
            return Err(CodecError::Limit);
        }
        // Read the version independently before deserializing any versioned payload.
        #[derive(Deserialize)]
        struct Version {
            schema_version: u16,
        }
        let version: Version = serde_json::from_slice(bytes)?;
        if version.schema_version != EVENT_SCHEMA_VERSION {
            return Err(CodecError::Version(version.schema_version));
        }
        let record: EventRecordV1 = serde_json::from_slice(bytes)?;
        let event = LiveEvent::try_from(record)?;
        self.check_levels(&event.payload)?;
        Ok(Arc::new(event))
    }
}
struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(std::io::Error::other("event size limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
