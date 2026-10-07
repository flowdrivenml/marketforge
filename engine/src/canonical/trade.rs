use super::enums::TradeSide;
use super::envelope::EventEnvelope;
use super::numeric::{ImpliedVolatility, Price, Quantity};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Trade {
    pub envelope: EventEnvelope,

    pub trade_id: Option<String>,
    pub sequence: Option<u64>,

    pub side: TradeSide,
    pub price: Price,

    pub quantity_base: Option<Quantity>,
    pub quantity_quote: Option<Quantity>,
    pub quantity_contracts: Option<Quantity>,

    pub is_rpi: Option<bool>,

    pub trade_iv: Option<ImpliedVolatility>,
    pub mark_iv: Option<ImpliedVolatility>,

    pub index_price: Option<Price>,
    pub mark_price: Option<Price>,
}
