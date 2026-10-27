use crate::live::{config::exchange::RestEndpoint, infrastructure::rest::ExchangeRestResponse};

use super::{HistoricalPagination, RestFacadeError, RestOperationError, StreamRestHandle};

// ============================================================
// HISTORICAL PAGE CURSOR
// ============================================================

/// Exchange-aware historical trade page selection.
///
/// Identifiers are exchange-native. No ordering or completeness
/// guarantees are inferred from the cursor itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoricalTradeCursor {
    /// Request the endpoint's default historical page.
    Latest,

    /// Binance Spot: begin at the specified native trade ID.
    TradeId { from_id: u64 },

    /// OKX historical trades.
    ///
    /// `before` and `after` retain their native OKX meanings.
    /// No implicit direction conversion is performed.
    Okx {
        before: Option<u64>,
        after: Option<u64>,
    },

    /// Gate.io historical trades.
    ///
    /// Boundaries are Unix timestamps in seconds.
    GateTimeRange { from: u64, to: u64 },
}

// ============================================================
// HISTORICAL PAGE REQUEST
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalTradePage {
    pub cursor: HistoricalTradeCursor,
    pub limit: Option<u64>,
}

impl HistoricalTradePage {
    pub fn latest() -> Self {
        Self {
            cursor: HistoricalTradeCursor::Latest,
            limit: None,
        }
    }

    pub fn with_cursor(cursor: HistoricalTradeCursor) -> Self {
        Self {
            cursor,
            limit: None,
        }
    }

    pub fn with_limit(mut self, limit: u64) -> Self {
        self.limit = Some(limit);
        self
    }
}

// ============================================================
// STREAM HISTORICAL OPERATIONS
// ============================================================

impl<'service, 'config> StreamRestHandle<'service, 'config> {
    /// Fetch one exchange-native historical trade page.
    ///
    /// This does not:
    /// - automatically paginate;
    /// - normalize trades;
    /// - deduplicate results;
    /// - infer whether history is complete.
    pub async fn historical_trades(
        &self,
        page: HistoricalTradePage,
    ) -> Result<ExchangeRestResponse, RestOperationError> {
        // ----------------------------------------------------
        // CAPABILITY
        // ----------------------------------------------------

        let capability = self
            .capabilities()
            .historical_trades
            .as_ref()
            .ok_or_else(|| RestFacadeError::UnsupportedOperation {
                profile: self.profile().to_owned(),
                operation: "historical_trades",
            })?;

        // ----------------------------------------------------
        // ENDPOINT
        // ----------------------------------------------------

        let config = self
            .service()
            .configs()
            .get(self.profile())
            .ok_or_else(|| RestFacadeError::UnknownProfile(self.profile().to_owned()))?;

        let endpoint_name = &capability.endpoint.endpoint;

        let endpoint = config.endpoint(endpoint_name).ok_or_else(|| {
            RestFacadeError::UnsupportedOperation {
                profile: self.profile().to_owned(),
                operation: "historical_trades",
            }
        })?;

        // ----------------------------------------------------
        // REQUEST CONSTRUCTION
        // ----------------------------------------------------

        let mut request = self.build_request(endpoint_name, page.limit)?;

        // ----------------------------------------------------
        // CURSOR BINDING
        // ----------------------------------------------------

        match (capability.pagination, page.cursor) {
            (_, HistoricalTradeCursor::Latest) => {}

            (HistoricalPagination::TradeId, HistoricalTradeCursor::TradeId { from_id }) => {
                if from_id == 0 {
                    return Err(invalid_cursor("trade ID must be greater than zero").into());
                }
                let parameter = endpoint
                    .pagination_parameter
                    .as_deref()
                    .ok_or_else(|| invalid_cursor("missing configured trade-ID parameter"))?;

                request = bind_parameter(request, endpoint, parameter, from_id)?;
            }

            (
                HistoricalPagination::OkxTradeHistory,
                HistoricalTradeCursor::Okx { before, after },
            ) => {
                if before.is_some() && after.is_some() {
                    return Err(invalid_cursor("before and after cannot both be specified").into());
                }

                if let Some(before) = before {
                    request = bind_parameter(request, endpoint, "before", before)?;
                }

                if let Some(after) = after {
                    request = bind_parameter(request, endpoint, "after", after)?;
                }
                if before == Some(0) || after == Some(0) {
                    return Err(invalid_cursor(
                        "OKX pagination trade ID must be greater than zero",
                    )
                    .into());
                }
            }

            (
                HistoricalPagination::GateSpotTrades | HistoricalPagination::GateFuturesTrades,
                HistoricalTradeCursor::GateTimeRange { from, to },
            ) => {
                if from >= to {
                    return Err(invalid_cursor("time-range start must precede end").into());
                }

                request = bind_parameter(request, endpoint, "from", from)?;

                request = bind_parameter(request, endpoint, "to", to)?;
            }

            _ => {
                return Err(
                    invalid_cursor("cursor strategy does not match exchange capability").into(),
                );
            }
        }

        // ----------------------------------------------------
        // SHARED REST EXECUTION
        // ----------------------------------------------------

        Ok(self.service().execute(request).await?)
    }
}

// ============================================================
// CURSOR PARAMETER VALIDATION
// ============================================================

fn bind_parameter(
    request: crate::live::infrastructure::rest::RestRequest,
    endpoint: &RestEndpoint,
    parameter: &str,
    value: u64,
) -> Result<crate::live::infrastructure::rest::RestRequest, RestFacadeError> {
    if !endpoint
        .parameters
        .iter()
        .any(|configured| configured == parameter)
    {
        return Err(invalid_cursor(format!(
            "endpoint does not declare native parameter '{parameter}'"
        )));
    }

    Ok(request.with_parameter(parameter, value.to_string()))
}

fn invalid_cursor(reason: impl Into<String>) -> RestFacadeError {
    RestFacadeError::InvalidParameter {
        name: "historical_cursor",
        reason: reason.into(),
    }
}
