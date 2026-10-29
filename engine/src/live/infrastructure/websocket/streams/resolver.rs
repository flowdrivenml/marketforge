use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

use crate::live::config::exchange::ExchangeConfig;

use super::request::StreamRequest;
use super::source::{StreamSource, StreamSourceError};
use crate::live::infrastructure::websocket::subscriptions::resolver::{
    SubscriptionResolutionContext, SubscriptionResolutionError, SubscriptionResolver,
};
use crate::live::infrastructure::websocket::subscriptions::restoration::DesiredSubscription;

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
        Self::resolve_with_formats(exchange, request, None)
    }

    pub fn resolve_with_formats(
        exchange: &ExchangeConfig,
        request: &StreamRequest,
        formats: Option<&crate::live::config::FormatConfigStore>,
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
            "raw" => {
                let symbol = routing
                    .symbol_field
                    .as_deref()
                    .ok_or_else(|| incomplete("symbol_field"))?;
                let event = routing
                    .event_type_field
                    .as_deref()
                    .ok_or_else(|| incomplete("event_type_field"))?;
                let expected = channel
                    .event_type
                    .as_deref()
                    .ok_or_else(|| incomplete("channel.event_type"))?;
                vec![
                    (
                        json_pointer(symbol),
                        serde_json::Value::String(request.native_symbol().to_owned()),
                    ),
                    (
                        json_pointer(event),
                        serde_json::Value::String(expected.to_owned()),
                    ),
                ]
            }
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

            "channel" => {
                let field = routing
                    .channel_field
                    .as_deref()
                    .ok_or_else(|| incomplete("channel_field"))?;
                let event = routing
                    .event_field
                    .as_deref()
                    .ok_or_else(|| incomplete("event_field"))?;
                vec![
                    (
                        json_pointer(field),
                        Value::String(
                            channel
                                .channel
                                .clone()
                                .ok_or_else(|| incomplete("channel.channel"))?,
                        ),
                    ),
                    (
                        json_pointer(event),
                        Value::String(
                            routing
                                .update_event
                                .clone()
                                .ok_or_else(|| incomplete("update_event"))?,
                        ),
                    ),
                ]
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

        // The persistent Live registry may supply the authoritative
        // logical stream identity.
        //
        // Legacy infrastructure callers retain deterministic,
        // automatically generated identities.
        let stream_id = if let Some(stream_id) = request.stream_id() {
            stream_id.to_owned()
        } else {
            let mut generated = format!(
                "{}:{}:{}",
                request.profile(),
                request.native_symbol(),
                request.channel(),
            );

            // Preserve the existing identity semantics for
            // parameterized subscriptions.
            for (name, value) in &variables {
                generated.push(':');
                generated.push_str(name);
                generated.push('=');
                generated.push_str(value);
            }

            generated
        };

        let mut subscription =
            DesiredSubscription::new(stream_id, request.channel(), request.native_symbol())
                .map_err(|error| {
                    StreamSourceResolutionError::IncompleteRouting(error.to_string())
                })?;

        for (name, value) in variables {
            subscription = subscription.with_variable(name, value);
        }

        let source = StreamSource::new(request.profile(), subscription, fields)?;
        if routing.mode != "channel" {
            return Ok(source);
        }
        let formats = formats
            .ok_or_else(|| incomplete("instrument-aware routing requires normalization formats"))?;
        let (identity, message, records, mappings, instrument) = match request.channel() {
            "trades" => {
                let format = formats
                    .trade(exchange.profile())
                    .ok_or_else(|| incomplete("trade format"))?;
                (
                    &format.format,
                    &format.message,
                    &format.records,
                    &format.fields,
                    &format.instrument,
                )
            }
            "depth" => {
                let format = formats
                    .depth(exchange.profile())
                    .ok_or_else(|| incomplete("depth format"))?;
                (
                    &format.format,
                    &format.message,
                    &format.records,
                    &format.fields,
                    &format.instrument,
                )
            }
            _ => return Err(incomplete("instrument-aware channel format")),
        };
        let records = records.as_ref().ok_or_else(|| incomplete("records"))?;
        if message.root != "$"
            || records.record_type != "object"
            || identity.channel != request.channel()
            || identity.id != exchange.profile()
            || identity.exchange != exchange.exchange.id
            || identity.market != exchange.market()
            || message.channel_value.as_deref() != channel.channel.as_deref()
            || message.event_value != routing.update_event
            || message.channel_field != routing.channel_field
            || message.event_field != routing.event_field
            || instrument.exchange != identity.exchange
            || instrument.market != identity.market
            || message.payload_path.as_deref() != Some(records.path.as_str())
        {
            return Err(incomplete("routing and normalization contracts disagree"));
        }
        let field = mappings
            .get(&instrument.source)
            .ok_or_else(|| incomplete("instrument source mapping"))?;
        let array = match records.representation.as_str() {
            "array" => true,
            "object" => false,
            _ => return Err(incomplete("record representation")),
        };
        source.with_payload(crate::live::infrastructure::websocket::routing::message_router::NativePayloadSelector {
            path: json_pointer(&records.path), instrument: json_pointer(field), symbol: request.native_symbol().to_owned(), array,
        }).map_err(Into::into)
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
    use crate::live::infrastructure::websocket::StreamRequestError;
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
    fn gate_channel_without_format_contract_is_rejected() {
        let result = resolve("gateio_linear", "BTC_USDT", "trades");

        assert!(matches!(
            result,
            Err(StreamSourceResolutionError::IncompleteRouting(ref reason)) if reason.contains("normalization formats")
        ));
    }

    #[test]
    fn explicit_logical_stream_id_is_preserved() {
        let exchange = profile("bybit_spot");

        let request = StreamRequest::new("bybit_spot", "BTCUSDT", "trades")
            .unwrap()
            .with_stream_id("marketforge:btc:trades")
            .unwrap();

        let resolved = StreamSourceResolver::resolve(&exchange, &request).unwrap();

        assert_eq!(resolved.stream_id(), "marketforge:btc:trades");
    }

    #[test]
    fn generated_stream_id_remains_backward_compatible() {
        let exchange = profile("bybit_spot");

        let request = StreamRequest::new("bybit_spot", "BTCUSDT", "trades").unwrap();

        let resolved = StreamSourceResolver::resolve(&exchange, &request).unwrap();

        assert_eq!(resolved.stream_id(), "bybit_spot:BTCUSDT:trades");
    }

    #[test]
    fn invalid_explicit_stream_id_is_rejected() {
        let request = StreamRequest::new("binance_spot", "BTCUSDT", "trades").unwrap();

        assert!(matches!(
            request.with_stream_id(" invalid "),
            Err(StreamRequestError::InvalidStreamId)
        ));
    }
    #[test]
    fn gate_payload_contracts_match_only_the_subscribed_instrument() {
        let formats = crate::live::config::FormatConfigStore::load_dir(
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats"),
        )
        .unwrap();
        for name in ["gateio_spot", "gateio_linear", "gateio_inverse"] {
            for channel in ["trades", "depth"] {
                let config = profile(name);
                let symbol = if name == "gateio_inverse" {
                    "BTC_USD"
                } else {
                    "BTC_USDT"
                };
                let request = StreamRequest::new(name, symbol, channel).unwrap();
                let source =
                    StreamSourceResolver::resolve_with_formats(&config, &request, Some(&formats))
                        .unwrap();
                assert_eq!(source.subscription().native_symbol, symbol);
                let field = if channel == "depth" {
                    "s"
                } else if name == "gateio_spot" {
                    "currency_pair"
                } else {
                    "contract"
                };
                let mut record = json!({});
                record[field] = symbol.into();
                let array = channel == "trades" && name != "gateio_spot";
                let mut frame = json!({"channel":config.channel(channel).unwrap().channel,"event":"update","result":if array {json!([record])} else {record}});
                assert!(source.routing().matches(&frame), "{name}/{channel}");
                if array {
                    frame["result"][0][field] = "ETH_USDT".into();
                } else {
                    frame["result"][field] = "ETH_USDT".into();
                }
                assert!(!source.routing().matches(&frame));
                let mut conflicting = config.clone();
                conflicting.websocket.routing.as_mut().unwrap().update_event =
                    Some("different-event".into());
                assert!(matches!(
                    StreamSourceResolver::resolve_with_formats(
                        &conflicting,
                        &request,
                        Some(&formats)
                    ),
                    Err(StreamSourceResolutionError::IncompleteRouting(_))
                ));
            }
        }
    }
}
