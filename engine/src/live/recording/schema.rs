//! Mapping to the observed live_canonical tables. No DDL or database IO.
use crate::canonical::BookSide;
use crate::live::model::{
    codec::{EventRecordV1, ExactDecimal},
    cursor::StreamCursor,
    envelope::{EventOrigin, LiveEvent, LivePayload},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum MappingError {
    #[error("invalid canonical event record")]
    InvalidRecord,
    #[error("stream registration does not match the event")]
    StreamMismatch,
    #[error("observed SQL timestamp constraints cannot represent a negative timestamp")]
    UnsupportedTimestamp,
    #[error("level ordinal exceeds PostgreSQL int4")]
    OrdinalOverflow,
    #[error("relational columns conflict with the canonical record")]
    ColumnMismatch,
    #[error("incompatible observed database schema")]
    SchemaMismatch,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StreamKind {
    Trade,
    L2,
    Derived,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRow {
    pub stream_id: crate::live::model::identity::StreamId,
    pub instrument_id: Option<i64>,
    pub stream_kind: StreamKind,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventRow {
    pub cursor: StreamCursor,
    pub record_version: i16,
    pub payload_kind: String,
    pub exchange_timestamp_ns: Option<u64>,
    pub received_timestamp_ns: u64,
    pub book_generation: Option<u64>,
    pub event_record: EventRecordV1,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LevelRow {
    pub cursor: StreamCursor,
    pub level_ordinal: i32,
    pub side: String,
    pub action: String,
    pub price: ExactDecimal,
    pub quantity_base: Option<ExactDecimal>,
    pub quantity_quote: Option<ExactDecimal>,
    pub quantity_contracts: Option<ExactDecimal>,
    pub order_count: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredEvent {
    pub event: EventRow,
    pub levels: Vec<LevelRow>,
}
impl StoredEvent {
    pub fn encode(event: &LiveEvent, stream: &StreamRow) -> Result<Self, MappingError> {
        event.check().map_err(|_| MappingError::InvalidRecord)?;
        let derived = matches!(event.provenance.origin, EventOrigin::Derived(_));
        let (kind, instrument) = match &event.payload {
            LivePayload::Trade(t) => (StreamKind::Trade, Some(t.envelope.instrument_id)),
            LivePayload::L2Group(g) => (
                StreamKind::L2,
                g.grouped_levels.first().map(|l| l.envelope.instrument_id),
            ),
        };
        if stream.stream_id != event.cursor.stream_id
            || stream.stream_kind != if derived { StreamKind::Derived } else { kind }
            || stream
                .instrument_id
                .is_some_and(|id| id <= 0 || instrument.is_some_and(|actual| actual != id))
        {
            return Err(MappingError::StreamMismatch);
        }
        Self::from_event(event)
    }
    fn from_event(event: &LiveEvent) -> Result<Self, MappingError> {
        let mut levels = vec![];
        if let LivePayload::L2Group(group) = &event.payload {
            for (ordinal, l) in group.grouped_levels.iter().enumerate() {
                let deletion = l
                    .quantity_base
                    .into_iter()
                    .chain(l.quantity_quote)
                    .chain(l.quantity_contracts)
                    .all(|q| q.is_zero());
                levels.push(LevelRow {
                    cursor: event.cursor.clone(),
                    level_ordinal: checked_ordinal(ordinal)?,
                    side: match l.side {
                        BookSide::Bid => "bid",
                        BookSide::Ask => "ask",
                    }
                    .into(),
                    action: if deletion { "delete" } else { "set" }.into(),
                    price: ExactDecimal(l.price),
                    quantity_base: l.quantity_base.map(ExactDecimal),
                    quantity_quote: l.quantity_quote.map(ExactDecimal),
                    quantity_contracts: l.quantity_contracts.map(ExactDecimal),
                    order_count: l.order_count,
                });
            }
        }
        let unsigned = |v: i64| u64::try_from(v).map_err(|_| MappingError::UnsupportedTimestamp);
        Ok(Self {
            event: EventRow {
                cursor: event.cursor.clone(),
                record_version: 1,
                payload_kind: match event.payload {
                    LivePayload::Trade(_) => "trade",
                    LivePayload::L2Group(_) => "l2",
                }
                .into(),
                exchange_timestamp_ns: event
                    .timing
                    .exchange_timestamp_ns
                    .map(unsigned)
                    .transpose()?,
                received_timestamp_ns: unsigned(event.timing.received_timestamp_ns)?,
                book_generation: event.provenance.book_generation.map(|g| g.0),
                event_record: EventRecordV1::try_from(event)
                    .map_err(|_| MappingError::InvalidRecord)?,
            },
            levels,
        })
    }
    /// The JSONB record is decoded, then every redundant SQL column and ordered
    /// child is checked. Database CHECK constraints alone do not validate JSONB.
    pub fn decode(&self) -> Result<LiveEvent, MappingError> {
        let decoded = LiveEvent::try_from(self.event.event_record.clone())
            .map_err(|_| MappingError::InvalidRecord)?;
        if Self::from_event(&decoded)? != *self {
            return Err(MappingError::ColumnMismatch);
        }
        Ok(decoded)
    }
}
pub fn checked_ordinal(ordinal: usize) -> Result<i32, MappingError> {
    i32::try_from(ordinal).map_err(|_| MappingError::OrdinalOverflow)
}

#[derive(Debug, Deserialize)]
pub struct SchemaSnapshot {
    pub columns: Vec<Column>,
    pub constraints: Vec<Constraint>,
}
#[derive(Debug, Deserialize)]
pub struct Column {
    pub table_name: String,
    pub column_name: String,
    pub data_type: String,
    pub is_nullable: String,
    pub numeric_precision: Option<u32>,
    pub numeric_scale: Option<u32>,
}
#[derive(Debug, Deserialize)]
pub struct Constraint {
    pub table_name: String,
    pub name: String,
    pub definition: String,
}
impl SchemaSnapshot {
    /// Fail compatibility checking closed when required keys/types/constraints
    /// differ. Discovery never creates or migrates tables.
    pub fn validate(&self) -> Result<(), MappingError> {
        let required = [
            ("streams", "stream_id", "text", false),
            ("streams", "stream_kind", "text", false),
            ("streams", "instrument_id", "bigint", true),
            ("event_identities", "stream_id", "text", false),
            ("event_identities", "log_epoch", "numeric", false),
            ("event_identities", "event_offset", "numeric", false),
            ("events", "stream_id", "text", false),
            ("events", "log_epoch", "numeric", false),
            ("events", "event_offset", "numeric", false),
            ("events", "record_version", "smallint", false),
            ("events", "payload_kind", "text", false),
            ("events", "exchange_timestamp_ns", "numeric", true),
            ("events", "received_timestamp_ns", "numeric", false),
            ("events", "book_generation", "numeric", true),
            ("events", "event_record", "jsonb", false),
            ("l2_levels", "stream_id", "text", false),
            ("l2_levels", "log_epoch", "numeric", false),
            ("l2_levels", "event_offset", "numeric", false),
            ("l2_levels", "level_ordinal", "integer", false),
            ("l2_levels", "side", "text", false),
            ("l2_levels", "action", "text", false),
            ("l2_levels", "price", "numeric", false),
            ("l2_levels", "quantity_base", "numeric", true),
            ("l2_levels", "quantity_quote", "numeric", true),
            ("l2_levels", "quantity_contracts", "numeric", true),
            ("l2_levels", "order_count", "numeric", true),
            ("recording_coverage", "stream_id", "text", false),
            ("recording_coverage", "log_epoch", "numeric", false),
            ("recording_coverage", "first_offset", "numeric", false),
            ("recording_coverage", "last_offset", "numeric", false),
        ];
        for (table, name, kind, nullable) in required {
            let c = self
                .columns
                .iter()
                .find(|c| c.table_name == table && c.column_name == name)
                .ok_or(MappingError::SchemaMismatch)?;
            if c.data_type != kind || (c.is_nullable == "YES") != nullable {
                return Err(MappingError::SchemaMismatch);
            }
            if kind == "numeric" {
                let exact = matches!(
                    name,
                    "price" | "quantity_base" | "quantity_quote" | "quantity_contracts"
                );
                if if exact {
                    c.numeric_precision.is_some() || c.numeric_scale.is_some()
                } else {
                    c.numeric_precision != Some(20) || c.numeric_scale != Some(0)
                } {
                    return Err(MappingError::SchemaMismatch);
                }
            }
        }
        for (table, definition) in [
            ("streams", "PRIMARY KEY (stream_id)"),
            (
                "event_identities",
                "PRIMARY KEY (stream_id, log_epoch, event_offset)",
            ),
            ("events", "PRIMARY KEY (stream_id, log_epoch, event_offset)"),
            (
                "l2_levels",
                "PRIMARY KEY (stream_id, log_epoch, event_offset, level_ordinal)",
            ),
            (
                "recording_coverage",
                "PRIMARY KEY (stream_id, log_epoch, first_offset)",
            ),
            (
                "events",
                "FOREIGN KEY (stream_id, log_epoch, event_offset) REFERENCES live_canonical.event_identities(stream_id, log_epoch, event_offset)",
            ),
            (
                "l2_levels",
                "FOREIGN KEY (stream_id, log_epoch, event_offset) REFERENCES live_canonical.events(stream_id, log_epoch, event_offset) ON DELETE CASCADE",
            ),
        ] {
            if !self
                .constraints
                .iter()
                .any(|c| c.table_name == table && c.definition == definition)
            {
                return Err(MappingError::SchemaMismatch);
            }
        }
        for (table, definition) in [
            (
                "event_identities",
                "CHECK (((log_epoch >= (0)::numeric) AND (log_epoch < '18446744073709551616'::numeric)))",
            ),
            (
                "event_identities",
                "CHECK (((event_offset >= (0)::numeric) AND (event_offset < '18446744073709551616'::numeric)))",
            ),
            (
                "events",
                "CHECK ((((payload_kind = 'l2'::text) AND (book_generation IS NOT NULL)) OR ((payload_kind = 'trade'::text) AND (book_generation IS NULL))))",
            ),
            (
                "events",
                "CHECK (((book_generation IS NULL) OR ((book_generation >= (0)::numeric) AND (book_generation < '18446744073709551616'::numeric))))",
            ),
            (
                "events",
                "CHECK ((payload_kind = ANY (ARRAY['trade'::text, 'l2'::text])))",
            ),
            (
                "events",
                "CHECK (((received_timestamp_ns >= (0)::numeric) AND (received_timestamp_ns < '18446744073709551616'::numeric)))",
            ),
            (
                "events",
                "CHECK ((jsonb_typeof(event_record) = 'object'::text))",
            ),
            ("events", "CHECK ((record_version = 1))"),
            (
                "events",
                "CHECK (((exchange_timestamp_ns IS NULL) OR ((exchange_timestamp_ns >= (0)::numeric) AND (exchange_timestamp_ns < '18446744073709551616'::numeric))))",
            ),
            ("l2_levels", "CHECK ((level_ordinal >= 0))"),
            (
                "l2_levels",
                "CHECK ((action = ANY (ARRAY['set'::text, 'delete'::text])))",
            ),
            (
                "l2_levels",
                "CHECK ((side = ANY (ARRAY['bid'::text, 'ask'::text])))",
            ),
            (
                "l2_levels",
                "CHECK (((order_count IS NULL) OR ((order_count >= (0)::numeric) AND (order_count < '18446744073709551616'::numeric))))",
            ),
            (
                "recording_coverage",
                "CHECK (((log_epoch >= (0)::numeric) AND (log_epoch < '18446744073709551616'::numeric)))",
            ),
            (
                "recording_coverage",
                "CHECK (((first_offset >= (0)::numeric) AND (last_offset >= first_offset) AND (last_offset < '18446744073709551616'::numeric)))",
            ),
            ("streams", "CHECK ((length(btrim(stream_id)) > 0))"),
            (
                "streams",
                "CHECK ((stream_kind = ANY (ARRAY['trade'::text, 'l2'::text, 'derived'::text])))",
            ),
        ] {
            if !self
                .constraints
                .iter()
                .any(|c| c.table_name == table && c.definition == definition)
            {
                return Err(MappingError::SchemaMismatch);
            }
        }
        Ok(())
    }
}
