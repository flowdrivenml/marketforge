use crate::live::{
    config::exchange::{ExchangeConfig, RestEndpoint},
    infrastructure::rest::{ExchangeRestResponse, RestError, RestRequest},
    stream::request::CreateSourceStreamRequest,
};

use super::{RestFacadeError, StreamRestHandle};

// ============================================================
// OPERATION ERROR
// ============================================================

#[derive(Debug, thiserror::Error)]
pub enum RestOperationError {
    #[error(transparent)]
    Facade(#[from] RestFacadeError),

    #[error(transparent)]
    Transport(#[from] RestError),
}

// ============================================================
// STREAM REST OPERATIONS
// ============================================================

impl<'service, 'config> StreamRestHandle<'service, 'config> {
    // ========================================================
    // DEPTH SNAPSHOTS
    // ========================================================

    pub async fn snapshot(&self) -> Result<ExchangeRestResponse, RestOperationError> {
        let capability = self
            .capabilities()
            .depth_snapshot
            .as_ref()
            .ok_or_else(|| self.unsupported("snapshot"))?;

        let request = self.build_request(&capability.endpoint, None)?;

        Ok(self.service().execute(request).await?)
    }

    pub async fn snapshot_with_depth(
        &self,
        depth: u64,
    ) -> Result<ExchangeRestResponse, RestOperationError> {
        let capability = self
            .capabilities()
            .depth_snapshot
            .as_ref()
            .ok_or_else(|| self.unsupported("snapshot"))?;

        let request = self.build_request(&capability.endpoint, Some(depth))?;

        Ok(self.service().execute(request).await?)
    }

    // ========================================================
    // RECENT TRADES
    // ========================================================

    pub async fn recent_trades(&self) -> Result<ExchangeRestResponse, RestOperationError> {
        let capability = self
            .capabilities()
            .recent_trades
            .as_ref()
            .ok_or_else(|| self.unsupported("recent_trades"))?;

        let request = self.build_request(&capability.endpoint, None)?;

        Ok(self.service().execute(request).await?)
    }

    pub async fn recent_trades_with_limit(
        &self,
        limit: u64,
    ) -> Result<ExchangeRestResponse, RestOperationError> {
        let capability = self
            .capabilities()
            .recent_trades
            .as_ref()
            .ok_or_else(|| self.unsupported("recent_trades"))?;

        let request = self.build_request(&capability.endpoint, Some(limit))?;

        Ok(self.service().execute(request).await?)
    }

    // ========================================================
    // REQUEST CONSTRUCTION
    // ========================================================

    pub(super) fn build_request(
        &self,
        endpoint_name: &str,
        requested_limit: Option<u64>,
    ) -> Result<RestRequest, RestFacadeError> {
        let config = self
            .service()
            .configs()
            .get(self.profile())
            .ok_or_else(|| RestFacadeError::UnknownProfile(self.profile().to_owned()))?;

        let endpoint = config.endpoint(endpoint_name).ok_or_else(|| {
            RestFacadeError::UnsupportedOperation {
                profile: self.profile().to_owned(),
                operation: "configured_endpoint",
            }
        })?;

        let mut request = RestRequest::new(self.profile(), endpoint_name);

        request = bind_symbol(request, endpoint, self.stream_request());

        request = bind_native_category(request, config, endpoint, self.stream_request())?;

        request = bind_native_settlement(request, config, endpoint, self.stream_request())?;

        if let Some(limit) = requested_limit {
            let parameter = endpoint
                .limit_parameter
                .as_deref()
                .or_else(|| {
                    endpoint
                        .parameters
                        .iter()
                        .any(|value| value == "limit")
                        .then_some("limit")
                })
                .ok_or_else(|| RestFacadeError::InvalidParameter {
                    name: "limit",
                    reason: format!(
                        "endpoint '{endpoint_name}' does not \
                             support an explicit limit"
                    ),
                })?;

            if limit == 0 {
                return Err(RestFacadeError::InvalidParameter {
                    name: "limit",
                    reason: "must be greater than zero".into(),
                });
            }

            if let Some(maximum) = endpoint.maximum_limit {
                if limit > maximum {
                    return Err(RestFacadeError::InvalidParameter {
                        name: "limit",
                        reason: format!("requested {limit}, maximum is {maximum}"),
                    });
                }
            }

            request = request.with_parameter(parameter, limit.to_string());
        }

        Ok(request)
    }

    fn unsupported(&self, operation: &'static str) -> RestFacadeError {
        RestFacadeError::UnsupportedOperation {
            profile: self.profile().to_owned(),
            operation,
        }
    }
}

// ============================================================
// NATIVE INSTRUMENT BINDING
// ============================================================

fn bind_symbol(
    mut request: RestRequest,
    endpoint: &RestEndpoint,
    stream: &CreateSourceStreamRequest,
) -> RestRequest {
    // Binance, Bybit and Bitget use "symbol".
    //
    // Gate.io also requires "symbol" as a template input:
    // contract = "{symbol}"
    // currency_pair = "{symbol}"
    //
    // OKX uses "instId".

    let requires_symbol = endpoint
        .parameters
        .iter()
        .any(|parameter| parameter == "symbol")
        || endpoint.contract.as_deref() == Some("{symbol}")
        || endpoint.currency_pair.as_deref() == Some("{symbol}");

    if requires_symbol {
        request = request.with_parameter("symbol", stream.symbol.clone());
    }

    if endpoint
        .parameters
        .iter()
        .any(|parameter| parameter == "instId")
    {
        request = request.with_parameter("instId", stream.symbol.clone());
    }

    request
}

// ============================================================
// NATIVE MARKET CATEGORY
// ============================================================

fn bind_native_category(
    request: RestRequest,
    config: &ExchangeConfig,
    endpoint: &RestEndpoint,
    stream: &CreateSourceStreamRequest,
) -> Result<RestRequest, RestFacadeError> {
    // Fixed categories such as:
    //
    // category = "spot"
    // category = "linear"
    // category = "inverse"
    // category = "COIN-FUTURES"
    //
    // are already handled by RestRequestResolver.

    if endpoint.category.as_deref() != Some("{native_rest_category}") {
        return Ok(request);
    }

    let mapping =
        config
            .market_mapping
            .as_ref()
            .ok_or_else(|| RestFacadeError::InvalidParameter {
                name: "native_rest_category",
                reason: "missing market mapping".into(),
            })?;

    // The instrument type must identify a configured native
    // market type. Do not invent the native REST category
    // using arbitrary symbol parsing.
    let native = mapping
        .native_types
        .iter()
        .find(|(name, native)| {
            name.eq_ignore_ascii_case(&stream.instrument_type)
                || native
                    .websocket_inst_type
                    .as_deref()
                    .is_some_and(|value| value.eq_ignore_ascii_case(&stream.instrument_type))
        })
        .map(|(_, native)| native)
        .ok_or_else(|| RestFacadeError::InvalidParameter {
            name: "native_rest_category",
            reason: format!(
                "instrument type '{}' has no configured \
                 native market mapping",
                stream.instrument_type
            ),
        })?;

    let category =
        native
            .rest_category
            .as_deref()
            .ok_or_else(|| RestFacadeError::InvalidParameter {
                name: "native_rest_category",
                reason: format!(
                    "instrument type '{}' has no REST category",
                    stream.instrument_type
                ),
            })?;

    Ok(request.with_parameter("native_rest_category", category))
}

// ============================================================
// NATIVE SETTLEMENT
// ============================================================

fn bind_native_settlement(
    request: RestRequest,
    config: &ExchangeConfig,
    endpoint: &RestEndpoint,
    stream: &CreateSourceStreamRequest,
) -> Result<RestRequest, RestFacadeError> {
    if endpoint.settlement.as_deref() != Some("{native_settlement}") {
        return Ok(request);
    }

    let settlement = stream
        .settlement_asset
        .as_deref()
        .ok_or_else(|| RestFacadeError::InvalidParameter {
            name: "native_settlement",
            reason: "stream has no settlement asset".into(),
        })?
        .to_ascii_lowercase();

    let mapping =
        config
            .market_mapping
            .as_ref()
            .ok_or_else(|| RestFacadeError::InvalidParameter {
                name: "native_settlement",
                reason: "missing market mapping".into(),
            })?;

    if !mapping.supports_settlement(&settlement) {
        return Err(RestFacadeError::InvalidParameter {
            name: "native_settlement",
            reason: format!(
                "settlement '{settlement}' is not supported \
                 by profile '{}'",
                config.profile(),
            ),
        });
    }

    Ok(request.with_parameter("native_settlement", settlement))
}
