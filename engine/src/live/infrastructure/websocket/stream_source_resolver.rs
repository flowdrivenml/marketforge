use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

use crate::live::config::exchange::ExchangeConfig;

use super::{
    DesiredSubscription, StreamRequest, StreamSource, StreamSourceError,
    SubscriptionResolutionContext, SubscriptionResolutionError, SubscriptionResolver,
};

// ============================================================
// ERRORS
// ============================================================

#[derive(Debug, Error)]
pub enum StreamSourceResolutionError {
    #[error("requested profile does not match exchange configuration")]
    ProfileMismatch,

    #[error("unknown channel: {0}")]
    UnknownChannel(String),

    #[error("disabled channel: {0}")]
    DisabledChannel(String),

    #[error("missing WebSocket routing configuration")]
    MissingRouting,

    #[error("unsupported WebSocket routing mode: {0}")]
    UnsupportedRoutingMode(String),

    #[error("cannot derive unique native routing identity: {0}")]
    IncompleteRouting(String),

    #[error(transparent)]
    Subscription(#[from] SubscriptionResolutionError),

    #[error(transparent)]
    Source(#[from] StreamSourceError),
}

// ============================================================
// RESOLVER
// ============================================================

/// Converts a declarative stream request into the existing
/// transport-facing StreamSource.
///
/// No network I/O.
/// No WebSocket allocation.
/// No limiter construction.
///
/// Uses the existing SubscriptionResolver for native
/// subscription arguments.
///
/// The routing selector is derived from those exact
/// arguments, not independently reconstructed.
pub(crate) struct StreamSourceResolver;

impl StreamSourceResolver {
    pub fn resolve(
        exchange: &ExchangeConfig,
        request: &StreamRequest,
    ) -> Result<StreamSource, StreamSourceResolutionError> {
        if exchange.profile() != request.profile() {
            return Err(StreamSourceResolutionError::ProfileMismatch);
        }

        let channel = exchange.channel(request.channel()).ok_or_else(|| {
            StreamSourceResolutionError::UnknownChannel(request.channel().to_owned())
        })?;

        if !channel.enabled {
            return Err(StreamSourceResolutionError::DisabledChannel(
                request.channel().to_owned(),
            ));
        }

        // ----------------------------------------------------
        // RESOLUTION VARIABLES
        // ----------------------------------------------------

        let mut variables = BTreeMap::new();

        // Channel defaults are used unless overridden.
        if let Some(depth) = channel.default_depth {
            variables.insert("depth".to_owned(), depth.to_string());
        }

        if let Some(interval) = &channel.default_interval {
            variables.insert("interval".to_owned(), interval.clone());
        }

        // Explicit values supplied by the instrument catalog
        // or runtime take precedence over channel defaults.
        for (name, value) in request.parameters() {
            variables.insert(name.clone(), value.clone());
        }

        let mut context = SubscriptionResolutionContext::new(request.native_symbol())?;

        for (name, value) in &variables {
            context = context.with_variable(name.clone(), value.clone())?;
        }

        // ----------------------------------------------------
        // EXISTING SUBSCRIPTION RESOLUTION
        // ----------------------------------------------------

        let resolved = SubscriptionResolver::resolve(exchange, request.channel(), &context)?;

        // ----------------------------------------------------
        // INCOMING MESSAGE ROUTING
        // ----------------------------------------------------

        let routing = exchange
            .websocket
            .routing
            .as_ref()
            .ok_or(StreamSourceResolutionError::MissingRouting)?;

        let argument = resolved.request.arguments.first().ok_or_else(|| {
            StreamSourceResolutionError::IncompleteRouting(
                "subscription has no native arguments".into(),
            )
        })?;

        let fields = match routing.mode.as_str() {
            // Binance combined streams:
            //
            // Subscription argument:
            //   "btcusdt@trade"
            //
            // Incoming message:
            //   {"stream":"btcusdt@trade","data":{...}}
            "combined" => {
                let field = routing
                    .combined_stream_field
                    .as_deref()
                    .ok_or_else(|| incomplete("combined_stream_field"))?;

                vec![(json_pointer(field), argument.clone())]
            }

            // Bybit:
            //
            // Subscription argument:
            //   "publicTrade.BTCUSDT"
            //
            // Incoming message:
            //   {"topic":"publicTrade.BTCUSDT","data":[...]}
            "topic" => {
                let field = routing
                    .topic_field
                    .as_deref()
                    .ok_or_else(|| incomplete("topic_field"))?;

                vec![(json_pointer(field), argument.clone())]
            }

            // OKX / Bitget:
            //
            // Subscription argument:
            //   {"channel":"books","instId":"BTC-USDT-SWAP"}
            //
            // Incoming message:
            //   {"arg":{"channel":"books","instId":"..."}}
            //
            // Routing paths come from ExchangeConfig.
            "arg" => {
                let mut fields = Vec::new();

                for configured_path in [
                    routing.instrument_type_field.as_deref(),
                    routing.channel_field.as_deref(),
                    routing.instrument_field.as_deref(),
                ]
                .into_iter()
                .flatten()
                {
                    let argument_path = configured_path
                        .strip_prefix("arg.")
                        .ok_or_else(|| incomplete(configured_path))?;

                    let expected = read_object_path(argument, argument_path)
                        .ok_or_else(|| incomplete(configured_path))?;

                    fields.push((json_pointer(configured_path), expected.clone()));
                }

                if fields.is_empty() {
                    return Err(incomplete("arg routing has no identity fields"));
                }

                fields
            }

            // Gate.io cannot be safely demultiplexed using
            // the channel alone. Its native payload requires
            // an additional instrument-aware routing contract.
            "channel" => {
                return Err(StreamSourceResolutionError::UnsupportedRoutingMode(
                    "channel requires instrument-aware \
                             payload routing"
                        .into(),
                ));
            }

            other => {
                return Err(StreamSourceResolutionError::UnsupportedRoutingMode(
                    other.into(),
                ));
            }
        };

        // ----------------------------------------------------
        // EXISTING DESIRED SUBSCRIPTION
        // ----------------------------------------------------

        let mut stream_id = format!(
            "{}:{}:{}",
            request.profile(),
            request.native_symbol(),
            request.channel(),
        );

        // Parameterized subscriptions must have distinct
        // logical identities.
        for (name, value) in &variables {
            stream_id.push(':');
            stream_id.push_str(name);
            stream_id.push('=');
            stream_id.push_str(value);
        }

        let mut subscription =
            DesiredSubscription::new(stream_id, request.channel(), request.native_symbol())
                .map_err(|error| {
                    StreamSourceResolutionError::IncompleteRouting(error.to_string())
                })?;

        for (name, value) in variables {
            subscription = subscription.with_variable(name, value);
        }

        StreamSource::new(request.profile(), subscription, fields).map_err(Into::into)
    }
}

// ============================================================
// HELPERS
// ============================================================

fn incomplete(field: impl Into<String>) -> StreamSourceResolutionError {
    StreamSourceResolutionError::IncompleteRouting(field.into())
}

/// Convert configured dotted JSON paths to RFC 6901.
///
/// Examples:
///   arg.channel -> /arg/channel
///   topic       -> /topic
fn json_pointer(path: &str) -> String {
    let mut result = String::new();

    for component in path.split('.') {
        result.push('/');

        result.push_str(&component.replace('~', "~0").replace('/', "~1"));
    }

    result
}

/// Read a field from a structured subscription argument.
///
/// This reads only the supplied native argument object;
/// it never invents missing values.
fn read_object_path<'a>(argument: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = argument;

    for component in path.split('.') {
        current = current.as_object()?.get(component)?;
    }

    Some(current)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::PathBuf;

    use serde_json::json;

    fn profile(name: &str) -> ExchangeConfig {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../data/.config/live/exchanges")
            .join(format!("{name}.toml"));

        ExchangeConfig::load(path).unwrap()
    }

    fn resolve(
        exchange: &str,
        symbol: &str,
        channel: &str,
    ) -> Result<StreamSource, StreamSourceResolutionError> {
        let config = profile(exchange);

        let request = StreamRequest::new(exchange, symbol, channel).unwrap();

        StreamSourceResolver::resolve(&config, &request)
    }

    #[test]
    fn binance_combined_trade_routing() {
        let source = resolve("binance_linear", "BTCUSDT", "trades").unwrap();

        assert!(source.routing().matches(&json!({
            "stream": "btcusdt@trade",
            "data": {}
        })));

        assert!(!source.routing().matches(&json!({
            "stream": "ethusdt@trade",
            "data": {}
        })));
    }

    #[test]
    fn bybit_topic_trade_routing() {
        let source = resolve("bybit_linear", "BTCUSDT", "trades").unwrap();

        assert!(source.routing().matches(&json!({
            "topic": "publicTrade.BTCUSDT",
            "data": []
        })));
    }

    #[test]
    fn okx_arg_trade_routing() {
        let source = resolve("okx_linear", "BTC-USDT-SWAP", "trades").unwrap();

        assert!(source.routing().matches(&json!({
            "arg": {
                "channel": "trades-all",
                "instId": "BTC-USDT-SWAP"
            },
            "data": []
        })));

        assert!(!source.routing().matches(&json!({
            "arg": {
                "channel": "trades-all",
                "instId": "ETH-USDT-SWAP"
            }
        })));
    }

    #[test]
    fn bitget_arg_trade_routing() {
        let config = profile("bitget_linear");

        let request = StreamRequest::new("bitget_linear", "BTCUSDT", "trades")
            .unwrap()
            .with_parameter("native_ws_inst_type", "usdt-futures")
            .unwrap();

        let source = StreamSourceResolver::resolve(&config, &request).unwrap();

        assert!(source.routing().matches(&json!({
            "arg": {
                "instType": "usdt-futures",
                "topic": "publicTrade",
                "symbol": "BTCUSDT"
            }
        })));
    }

    #[test]
    fn bitget_missing_instrument_metadata_fails() {
        let result = resolve("bitget_linear", "BTCUSDT", "trades");

        assert!(matches!(
            result,
            Err(StreamSourceResolutionError::Subscription(
                SubscriptionResolutionError::MissingVariable(_)
            ))
        ));
    }

    #[test]
    fn gate_channel_only_routing_is_rejected() {
        let result = resolve("gateio_linear", "BTC_USDT", "trades");

        assert!(matches!(
            result,
            Err(StreamSourceResolutionError::UnsupportedRoutingMode(_))
        ));
    }
}
