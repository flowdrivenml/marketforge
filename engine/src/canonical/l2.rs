use super::{
    enums::BookSide,
    envelope::EventEnvelope,
    numeric::{Price, Quantity},
};

/// A canonical absolute L2 price-level update.
///
/// Every record describes the resulting quantity at one price level.
///
/// - Nonzero quantity: insert or replace the level.
/// - Zero quantity: remove the level.
/// - None: quantity representation unavailable, not a deletion.
///
/// Source snapshots and incremental updates are normalized into this
/// same representation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2LevelUpdate {
    pub envelope: EventEnvelope,

    /// Book side: bid or ask.
    pub side: BookSide,

    /// Price of the affected level.
    pub price: Price,

    /// Normalized quantity representations.
    pub quantity_base: Option<Quantity>,
    pub quantity_quote: Option<Quantity>,
    pub quantity_contracts: Option<Quantity>,

    /// Number of resting orders, when provided by the source.
    pub order_count: Option<u64>,
}
