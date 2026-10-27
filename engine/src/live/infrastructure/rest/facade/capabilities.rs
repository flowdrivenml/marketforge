use crate::live::config::exchange::{ExchangeConfig, RestEndpoint};

// ============================================================
// HISTORICAL PAGINATION
// ============================================================

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoricalPagination {
    TradeId,
    OkxTradeHistory,
    GateSpotTrades,
    GateFuturesTrades,
}

// ============================================================
// ENDPOINT CAPABILITY
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestEndpointCapability {
    /// Actual configured endpoint name.
    pub endpoint: String,

    /// Default record or depth limit, when configured.
    pub default_limit: Option<u64>,

    /// Maximum record or depth limit, when configured.
    pub maximum_limit: Option<u64>,
}

impl RestEndpointCapability {
    fn new(name: &str, endpoint: &RestEndpoint) -> Self {
        Self {
            endpoint: name.to_owned(),
            default_limit: endpoint.default_limit,
            maximum_limit: endpoint.maximum_limit,
        }
    }
}

// ============================================================
// HISTORICAL TRADE CAPABILITY
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HistoricalTradesCapability {
    pub endpoint: RestEndpointCapability,

    pub pagination: HistoricalPagination,

    /// Configured history horizon.
    ///
    /// This is metadata, not a guarantee that all requested
    /// records are available.
    pub max_history_days: Option<u64>,
}

// ============================================================
// STREAM REST CAPABILITIES
// ============================================================

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRestCapabilities {
    pub depth_snapshot: Option<RestEndpointCapability>,

    pub recent_trades: Option<RestEndpointCapability>,

    pub historical_trades: Option<HistoricalTradesCapability>,
}

impl StreamRestCapabilities {
    // ========================================================
    // CONSTRUCTION
    // ========================================================

    /// Compile available operations from an immutable
    /// exchange configuration.
    ///
    /// No network requests are performed.
    ///
    /// Authenticated endpoints are excluded.
    pub fn compile(config: &ExchangeConfig) -> Self {
        let depth_snapshot = public_endpoint(config, "depth_snapshot")
            .map(|endpoint| RestEndpointCapability::new("depth_snapshot", endpoint));

        let historical_trades = public_endpoint(config, "historical_trades").and_then(|endpoint| {
            let pagination = parse_pagination(endpoint.pagination_strategy.as_deref())?;

            Some(HistoricalTradesCapability {
                endpoint: RestEndpointCapability::new("historical_trades", endpoint),
                pagination,
                max_history_days: endpoint.max_history_days,
            })
        });

        let recent_trades = public_endpoint(config, "recent_trades")
            .map(|endpoint| RestEndpointCapability::new("recent_trades", endpoint))
            .or_else(|| {
                // Gate.io uses the same native endpoint for
                // recent and historical public trades.
                //
                // Only accept this fallback for the two
                // known Gate.io pagination strategies.
                let history = historical_trades.as_ref()?;

                if !matches!(
                    history.pagination,
                    HistoricalPagination::GateSpotTrades | HistoricalPagination::GateFuturesTrades
                ) {
                    return None;
                }

                Some(history.endpoint.clone())
            });

        Self {
            depth_snapshot,
            recent_trades,
            historical_trades,
        }
    }

    // ========================================================
    // CAPABILITY QUERIES
    // ========================================================

    pub fn supports_depth_snapshot(&self) -> bool {
        self.depth_snapshot.is_some()
    }

    pub fn supports_recent_trades(&self) -> bool {
        self.recent_trades.is_some()
    }

    pub fn supports_historical_trades(&self) -> bool {
        self.historical_trades.is_some()
    }
}

// ============================================================
// AUTHENTICATION
// ============================================================

fn public_endpoint<'a>(config: &'a ExchangeConfig, name: &str) -> Option<&'a RestEndpoint> {
    let endpoint = config.rest.endpoints.get(name)?;

    // Endpoint-specific authentication overrides the
    // exchange-wide authentication setting.
    let authentication = endpoint
        .authentication
        .as_deref()
        .or(config.rest.authentication.as_deref());

    match authentication {
        None | Some("none") | Some("public") => Some(endpoint),
        _ => None,
    }
}

// ============================================================
// PAGINATION STRATEGIES
// ============================================================

fn parse_pagination(strategy: Option<&str>) -> Option<HistoricalPagination> {
    match strategy? {
        "trade_id" => Some(HistoricalPagination::TradeId),

        "okx_trade_history" => Some(HistoricalPagination::OkxTradeHistory),

        "gate_spot_trades" => Some(HistoricalPagination::GateSpotTrades),

        "gate_futures_trades" => Some(HistoricalPagination::GateFuturesTrades),

        _ => None,
    }
}
