use serde_json::{Value, json};

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        SubscriptionProtocolCompiler, SubscriptionProtocolError, SubscriptionRequest,
        SubscriptionStrategy,
    },
};

fn config(subscription_toml: &str, channel_toml: &str) -> ExchangeConfig {
    let document = format!(
        r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid"

{subscription_toml}

[channels.trades]
enabled = true
format = "trades/binance_spot"
{channel_toml}

[rest]
base_url = "https://example.invalid"
"#
    );

    toml::from_str(&document).unwrap()
}

fn decode(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

#[test]
fn w41_json_rpc_serializes_subscribe_and_unsubscribe() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
unsubscribe_method = "UNSUBSCRIBE"
request_id_field = "id"
request_id_type = "unsigned_integer"
"#,
        "",
    );

    let protocol = SubscriptionProtocolCompiler::compile(&exchange, "trades").unwrap();

    assert_eq!(protocol.strategy(), SubscriptionStrategy::JsonRpc);

    let request = SubscriptionRequest::new(vec![json!("btcusdt@trade")]).with_request_id(json!(42));

    assert_eq!(
        decode(&protocol.subscribe(&request).unwrap()),
        json!({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@trade"],
            "id": 42
        })
    );

    assert_eq!(
        decode(&protocol.unsubscribe(&request).unwrap()),
        json!({
            "method": "UNSUBSCRIBE",
            "params": ["btcusdt@trade"],
            "id": 42
        })
    );
}

#[test]
fn w41_json_operation_preserves_native_objects() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
unsubscribe_method = "unsubscribe"
operation_field = "op"
arguments_field = "args"
request_id_field = "req_id"
request_id_type = "string"
request_id_optional = true
"#,
        "",
    );

    let protocol = SubscriptionProtocolCompiler::compile(&exchange, "trades").unwrap();

    let arguments = vec![json!({
        "channel": "trades-all",
        "instId": "BTC-USDT"
    })];

    let request = SubscriptionRequest::new(arguments).with_request_id(json!("request-7"));

    assert_eq!(
        decode(&protocol.subscribe(&request).unwrap()),
        json!({
            "op": "subscribe",
            "args": [
                {
                    "channel": "trades-all",
                    "instId": "BTC-USDT"
                }
            ],
            "req_id": "request-7"
        })
    );

    // Optional request ID may be omitted.
    let without_id = SubscriptionRequest::new(vec![json!({
        "channel": "trades-all",
        "instId": "BTC-USDT"
    })]);

    assert!(
        decode(&protocol.subscribe(&without_id).unwrap())
            .get("req_id")
            .is_none()
    );
}

#[test]
fn w41_channel_event_serializes_timestamp_and_payload() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "gate_v4_channel_event"
"#,
        r#"
channel = "spot.trades"
"#,
    );

    let protocol = SubscriptionProtocolCompiler::compile(&exchange, "trades").unwrap();

    let request =
        SubscriptionRequest::new(vec![json!("BTC_USDT")]).with_unix_seconds(1_800_000_000);

    assert_eq!(
        decode(&protocol.subscribe(&request).unwrap()),
        json!({
            "time": 1800000000,
            "channel": "spot.trades",
            "event": "subscribe",
            "payload": ["BTC_USDT"]
        })
    );

    assert_eq!(
        decode(&protocol.unsubscribe(&request).unwrap()),
        json!({
            "time": 1800000000,
            "channel": "spot.trades",
            "event": "unsubscribe",
            "payload": ["BTC_USDT"]
        })
    );
}

#[test]
fn w41_rejects_missing_required_request_id() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
request_id_field = "id"
request_id_type = "unsigned_integer"
"#,
        "",
    );

    let protocol = SubscriptionProtocolCompiler::compile(&exchange, "trades").unwrap();

    let request = SubscriptionRequest::new(vec![json!("btcusdt@trade")]);

    assert!(matches!(
        protocol.subscribe(&request),
        Err(SubscriptionProtocolError::MissingRequestId)
    ));
}

#[test]
fn w41_rejects_wrong_request_id_type() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
request_id_field = "id"
request_id_type = "unsigned_integer"
"#,
        "",
    );

    let protocol = SubscriptionProtocolCompiler::compile(&exchange, "trades").unwrap();

    let request = SubscriptionRequest::new(vec![json!("btcusdt@trade")])
        .with_request_id(json!("incorrect-type"));

    assert!(matches!(
        protocol.subscribe(&request),
        Err(SubscriptionProtocolError::InvalidRequestId)
    ));
}

#[test]
fn w41_rejects_empty_arguments() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
operation_field = "op"
arguments_field = "args"
"#,
        "",
    );

    let protocol = SubscriptionProtocolCompiler::compile(&exchange, "trades").unwrap();

    assert!(matches!(
        protocol.subscribe(&SubscriptionRequest::new(vec![])),
        Err(SubscriptionProtocolError::EmptyArguments)
    ));
}

#[test]
fn w41_rejects_unsupported_strategy() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "unknown_future_protocol"
"#,
        "",
    );

    assert!(matches!(
        SubscriptionProtocolCompiler::compile(&exchange, "trades"),
        Err(SubscriptionProtocolError::UnsupportedStrategy(_))
    ));
}

#[test]
fn w41_rejects_disabled_channel() {
    let mut exchange = config(
        r#"
[websocket.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
"#,
        "",
    );

    exchange.channels.get_mut("trades").unwrap().enabled = false;

    assert!(SubscriptionProtocolCompiler::compile(&exchange, "trades").is_err());
}
