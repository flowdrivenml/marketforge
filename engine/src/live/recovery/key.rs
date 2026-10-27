use crate::live::{infrastructure::rest::HistoricalTradeCursor, stream::request::QuantityType};

use super::model::{RecoveryOperation, RecoveryRequest};

// ============================================================
// HISTORICAL CURSOR KEY
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RecoveryHistoricalCursorKey {
    Latest,

    TradeId {
        from_id: u64,
    },

    Okx {
        before: Option<u64>,
        after: Option<u64>,
    },

    GateTimeRange {
        from: u64,
        to: u64,
    },
}

impl From<&HistoricalTradeCursor> for RecoveryHistoricalCursorKey {
    fn from(cursor: &HistoricalTradeCursor) -> Self {
        match cursor {
            HistoricalTradeCursor::Latest => Self::Latest,

            HistoricalTradeCursor::TradeId { from_id } => Self::TradeId { from_id: *from_id },

            HistoricalTradeCursor::Okx { before, after } => Self::Okx {
                before: *before,
                after: *after,
            },

            HistoricalTradeCursor::GateTimeRange { from, to } => Self::GateTimeRange {
                from: *from,
                to: *to,
            },
        }
    }
}

// ============================================================
// OPERATION KEY
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RecoveryOperationKey {
    DepthSnapshot {
        depth: Option<u64>,
    },

    RecentTrades {
        limit: Option<u64>,
    },

    HistoricalTrades {
        cursor: RecoveryHistoricalCursorKey,
        limit: Option<u64>,
    },
}

impl From<&RecoveryOperation> for RecoveryOperationKey {
    fn from(operation: &RecoveryOperation) -> Self {
        match operation {
            RecoveryOperation::DepthSnapshot { depth } => Self::DepthSnapshot { depth: *depth },

            RecoveryOperation::RecentTrades { limit } => Self::RecentTrades { limit: *limit },

            RecoveryOperation::HistoricalTrades { page } => Self::HistoricalTrades {
                cursor: (&page.cursor).into(),
                limit: page.limit,
            },
        }
    }
}

// ============================================================
// ACQUISITION KEY
// ============================================================

/// Conservative identity for a potentially shared REST job.
///
/// Source stream ID, WebSocket epochs and recovery attempt
/// tokens are intentionally excluded.
///
/// All native REST binding inputs are retained, including
/// instrument/contract semantics. This may prevent some
/// otherwise safe coalescing, but cannot incorrectly merge
/// requests because those fields were omitted.
///
/// The configuration generation fences configuration reloads.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RecoveryAcquisitionKey {
    pub configuration_generation: u64,

    pub exchange: crate::live::config::exchange::ExchangeId,

    pub market: crate::live::config::exchange::MarketType,

    pub instrument_id: i64,

    pub symbol: String,

    pub instrument_type: String,

    pub base_asset: Option<String>,

    pub quote_asset: Option<String>,

    pub settlement_asset: Option<String>,

    pub quantity_type: QuantityType,

    pub contract_value: Option<String>,

    pub contract_value_asset: Option<String>,

    pub operation: RecoveryOperationKey,
}

impl RecoveryAcquisitionKey {
    pub fn from_request(request: &RecoveryRequest) -> Self {
        let source = &request.source;

        Self {
            configuration_generation: request.configuration_generation,

            exchange: source.exchange,
            market: source.market_category,
            instrument_id: source.instrument_id,

            symbol: source.symbol.clone(),
            instrument_type: source.instrument_type.clone(),

            base_asset: source.base_asset.clone(),
            quote_asset: source.quote_asset.clone(),
            settlement_asset: source.settlement_asset.clone(),

            quantity_type: source.quantity_type,

            contract_value: source.contract_value.clone(),

            contract_value_asset: source.contract_value_asset.clone(),

            operation: (&request.operation).into(),
        }
    }
}
