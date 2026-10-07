use super::enums::{BookSide, L2Action};
use super::envelope::EventEnvelope;
use super::numeric::{Price, Quantity};
use super::sequence::SequenceMetadata;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2Level {
    pub price: Price,

    pub quantity_base: Option<Quantity>,
    pub quantity_quote: Option<Quantity>,
    pub quantity_contracts: Option<Quantity>,

    pub order_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2LevelUpdate {
    pub side: BookSide,
    pub action: L2Action,
    pub price: Price,

    pub quantity_base: Option<Quantity>,
    pub quantity_quote: Option<Quantity>,
    pub quantity_contracts: Option<Quantity>,

    pub order_count: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2Snapshot {
    pub envelope: EventEnvelope,
    pub sequence: SequenceMetadata,

    pub bids: Vec<L2Level>,
    pub asks: Vec<L2Level>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct L2Update {
    pub envelope: EventEnvelope,
    pub sequence: SequenceMetadata,

    pub changes: Vec<L2LevelUpdate>,
}
