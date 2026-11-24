use super::DataIntegrityEvidence;
use serde::{Deserialize, Serialize};
/// Exact base-10 coefficient and scale. No floating point values occur on wire.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Decimal {
    pub coefficient: String,
    pub scale: u32,
}
impl From<rust_decimal::Decimal> for Decimal {
    fn from(v: rust_decimal::Decimal) -> Self {
        Self {
            coefficient: v.mantissa().to_string(),
            scale: v.scale(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Trade {
    pub event_timestamp_ns: i64,
    pub system_timestamp_ns: Option<i64>,
    pub exchange: String,
    pub instrument_id: i64,
    pub symbol: String,
    pub stream_id: String,
    pub trade_id: Option<String>,
    pub sequence: Option<u64>,
    pub side: String,
    pub price: Decimal,
    pub quantity_base: Option<Decimal>,
    pub quantity_quote: Option<Decimal>,
    pub quantity_contracts: Option<Decimal>,
    pub is_rpi: Option<bool>,
    pub trade_iv: Option<Decimal>,
    pub mark_iv: Option<Decimal>,
    pub index_price: Option<Decimal>,
    pub mark_price: Option<Decimal>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub stream_id: String,
    pub runtime_generation: u64,
    pub publication_sequence: u64,
    pub record_ordinal: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Level {
    pub operation: String,
    pub side: String,
    pub price: Decimal,
    pub quantity_base: Option<Decimal>,
    pub quantity_quote: Option<Decimal>,
    pub quantity_contracts: Option<Decimal>,
    pub order_count: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Depth {
    /// snapshot_omitted, invalidated, or delta. A delta is one atomic SET/REMOVE batch.
    pub kind: String,
    pub source_generation: u64,
    pub book_generation: u64,
    pub last_sequence: Option<u64>,
    pub cross_sequence: Option<u64>,
    pub timestamp_ns: Option<i64>,
    pub timestamp_provenance: Option<String>,
    pub composition: Option<String>,
    pub internal_baseline_synchronized: Option<bool>,
    /// Always false: this interface never initializes a client book.
    pub client_book_initialized: bool,
    pub operations: Vec<Level>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "snake_case")]
pub enum NativeEvent {
    Trade(Trade),
    Depth(Depth),
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Reference {
    pub dataset_id: u64,
    pub stream_id: String,
    pub runtime_generation: Option<u64>,
    pub timestamp_ns: i64,
    pub stream_rank: u32,
    pub row_sequence: u64,
    pub event_ordinal: u64,
    pub direction: String,
    pub price: Decimal,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConvertedValues {
    pub price: Decimal,
    pub quantity_quote: Option<Decimal>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Valuation {
    pub target_quote: String,
    pub rate: Decimal,
    pub numerator: String,
    pub denominator: String,
    pub references: Vec<Reference>,
    pub reference_integrity: Vec<DataIntegrityEvidence>,
    pub path: Vec<String>,
    pub values: Vec<ConvertedValues>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    pub ordinal: u64,
    pub native: NativeEvent,
    pub origin: Option<Position>,
    pub upstream: Option<Position>,
    pub processing_input_position: Option<u64>,
    pub processing_admission_ordinal: Option<u64>,
    pub baseline_barrier: Option<bool>,
    pub integrity: Option<DataIntegrityEvidence>,
    pub valuation: Option<Valuation>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamPublication {
    pub stream_id: String,
    pub runtime_generation: u64,
    pub router_sequence: u64,
    pub recovered_records: bool,
    pub records: Vec<Record>,
}
