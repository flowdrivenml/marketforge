use serde::{Deserialize, Serialize};

use crate::live::config::exchange::{ExchangeId, MarketType};

// ============================================================
// SOURCE STREAM CREATION REQUEST
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateSourceStreamRequest {
    // --------------------------------------------------------
    // Instrument identity
    // --------------------------------------------------------
    pub instrument_id: i64,

    pub exchange: ExchangeId,

    pub symbol: String,

    pub market_category: MarketType,

    pub instrument_type: String,

    // --------------------------------------------------------
    // Instrument assets
    // --------------------------------------------------------
    pub base_asset: Option<String>,

    pub quote_asset: Option<String>,

    // --------------------------------------------------------
    // Quantity and contract semantics
    // --------------------------------------------------------
    pub quantity_type: QuantityType,

    /// Exact decimal value represented as a string.
    ///
    /// Example: "100", "0.001", "0.00000001"
    ///
    /// Semantic validation must parse this as a Decimal.
    /// Do not convert through f64.
    pub contract_value: Option<String>,

    pub contract_value_asset: Option<String>,

    // --------------------------------------------------------
    // Requested stream
    // --------------------------------------------------------
    pub channel: SourceChannel,
    pub settlement_asset: Option<String>,
}

// ============================================================
// QUANTITY TYPE
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QuantityType {
    Base,
    Quote,
    Contracts,
}

impl QuantityType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Base => "base",
            Self::Quote => "quote",
            Self::Contracts => "contracts",
        }
    }
}

// ============================================================
// SOURCE CHANNEL
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceChannel {
    Trades,
    Depth,
}

impl SourceChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::Depth => "depth",
        }
    }
}

// ============================================================
// SOURCE STREAM REQUEST IMPLEMENTATION
// ============================================================

impl CreateSourceStreamRequest {
    /// Validate instrument identity, assets, and quantity semantics.
    ///
    /// Runtime checks such as exchange support, format availability,
    /// and duplicate stream detection happen during stream creation.
    pub fn validate(&self) -> Result<(), String> {
        super::validation::validate(self)
    }
}

#[cfg(test)]
mod tests;
