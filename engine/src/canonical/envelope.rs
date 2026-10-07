use super::enums::Exchange;
use super::numeric::TimestampNs;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventEnvelope {
    pub event_timestamp_ns: TimestampNs,
    pub system_timestamp_ns: Option<TimestampNs>,

    pub exchange: Exchange,
    pub instrument_id: i64,
    pub symbol: String,
    pub stream_id: String,
}
