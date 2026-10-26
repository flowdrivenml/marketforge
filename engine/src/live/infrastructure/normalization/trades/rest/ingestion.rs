use thiserror::Error;

use crate::live::config::exchange::RestEndpoint;
use crate::live::infrastructure::rest::{ExchangeRestService, RestError, RestRequest};
use crate::live::stream::request::SourceChannel;

use crate::live::infrastructure::normalization::context::NormalizationContext;
use crate::live::infrastructure::normalization::message::NormalizedTrades;
use crate::live::infrastructure::normalization::trades::decoder::TradeDecoder;
use crate::live::infrastructure::normalization::trades::rest::decoder::{
    RestTradeDecoder, RestTradeResponseError,
};

#[derive(Debug, Error)]
pub enum RestTradeIngestionError {
    #[error("invalid REST trade request binding: {0}")]
    Binding(String),

    #[error(transparent)]
    Transport(#[from] RestError),

    #[error(transparent)]
    Normalization(#[from] RestTradeResponseError),
}

/// Owns one logical REST trade request through execution
/// and canonical normalization.
///
/// No response can be independently supplied by the caller.
pub struct RestTradeIngestion;

impl RestTradeIngestion {
    pub async fn execute_once(
        service: &ExchangeRestService<'_>,
        request: RestRequest,
        decoder: &TradeDecoder,
        context: &NormalizationContext,
    ) -> Result<NormalizedTrades, RestTradeIngestionError> {
        let config = service.configs().get(request.profile()).ok_or_else(|| {
            RestTradeIngestionError::Binding(format!(
                "unknown REST profile '{}'",
                request.profile()
            ))
        })?;

        let endpoint = config.endpoint(request.endpoint()).ok_or_else(|| {
            RestTradeIngestionError::Binding(format!(
                "unknown REST endpoint '{}'",
                request.endpoint()
            ))
        })?;

        // Reject invalid source bindings before issuing
        // a network request.
        validate_request_binding(&request, endpoint, decoder, context)?;

        // The same owned logical request is used for both
        // HTTP execution and response normalization.
        let response = service.execute(request.clone()).await?;

        RestTradeDecoder::decode_bound_exchange_response(
            decoder, &response, &request, endpoint, context,
        )
        .map_err(RestTradeIngestionError::Normalization)
    }
}

/// Verify the request belongs to the compiled trade source
/// before allowing any HTTP execution.
fn validate_request_binding(
    request: &RestRequest,
    endpoint: &RestEndpoint,
    decoder: &TradeDecoder,
    context: &NormalizationContext,
) -> Result<(), RestTradeIngestionError> {
    let reject = |message: &str| RestTradeIngestionError::Binding(message.to_owned());

    if context.channel() != SourceChannel::Trades {
        return Err(reject("normalization context is not a trade stream"));
    }

    let plan = decoder.plan();

    if plan.expected_exchange != context.exchange() {
        return Err(reject("compiled trade exchange differs from context"));
    }

    if plan.expected_market != context.market_category() {
        return Err(reject("compiled trade market differs from context"));
    }

    if plan.format_id != request.profile() {
        return Err(reject(
            "compiled trade format differs from REST request profile",
        ));
    }

    let mapping = endpoint
        .trade_normalization
        .as_ref()
        .ok_or_else(|| reject("REST endpoint has no trade normalization mapping"))?;

    let parameter = mapping.request_symbol_parameter.as_str();

    if parameter.is_empty() || parameter.trim() != parameter {
        return Err(reject("invalid configured native instrument parameter"));
    }

    let requested_symbol = request
        .parameters()
        .get(parameter)
        .ok_or_else(|| reject("REST request has no native instrument binding"))?;

    if requested_symbol != context.symbol() {
        return Err(reject("requested native instrument differs from context"));
    }

    Ok(())
}
