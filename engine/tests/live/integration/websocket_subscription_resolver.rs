use serde_json::{Value, json};

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        SubscriptionResolutionContext, SubscriptionResolutionError, SubscriptionResolver,
    },
};

fn exchange(subscription: &str, channel: &str) -> ExchangeConfig {
    let content = format!(
        r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid"

{subscription}

[channels.trades]
enabled = true
format = "trades/binance_spot"

{channel}

[rest]
base_url = "https://example.invalid"
"#
    );

    toml::from_str(&content).unwrap()
}

fn decode(value: &str) -> Value {
    serde_json::from_str(value).unwrap()
}

#[test]
fn w44_resolves_json_rpc_stream_template() {
    let config = exchange(
        r#"
[websocket.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
request_id_field = "id"
request_id_type = "unsigned_integer"
"#,
        r#"
stream_template = "{symbol_lower}@trade"
"#,
    );

    let context = SubscriptionResolutionContext::new("BTCUSDT").unwrap();

    let mut resolved = SubscriptionResolver::resolve(&config, "trades", &context).unwrap();

    assert_eq!(resolved.request.arguments, vec![json!("btcusdt@trade"),]);

    resolved.request.request_id = Some(json!(7));

    assert_eq!(
        decode(&resolved.protocol.subscribe(&resolved.request).unwrap()),
        json!({
            "method": "SUBSCRIBE",
            "params": ["btcusdt@trade"],
            "id": 7
        }),
    );
}

#[test]
fn w44_resolves_json_operation_native_object() {
    let config = exchange(
        r#"
[websocket.subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
operation_field = "op"
arguments_field = "args"
request_id_field = "id"
request_id_type = "string"
request_id_optional = true
"#,
        r#"
route = "public"
subscription_template = '{"channel":"trades","instId":"{symbol}"}'
"#,
    );

    let context = SubscriptionResolutionContext::new("BTC-USDT").unwrap();

    let resolved = SubscriptionResolver::resolve(&config, "trades", &context).unwrap();

    assert_eq!(resolved.route.as_deref(), Some("public"));

    assert_eq!(
        resolved.request.arguments,
        vec![json!({
            "channel": "trades",
            "instId": "BTC-USDT"
        })],
    );

    assert_eq!(
        decode(&resolved.protocol.subscribe(&resolved.request).unwrap()),
        json!({
            "op": "subscribe",
            "args": [{
                "channel": "trades",
                "instId": "BTC-USDT"
            }]
        }),
    );
}

#[test]
fn w44_resolves_gate_channel_payload() {
    let config = exchange(
        r#"
[websocket.subscription]
strategy = "gate_v4_channel_event"
"#,
        r#"
channel = "spot.trades"
subscription_payload = ["{symbol}"]
"#,
    );

    let context = SubscriptionResolutionContext::new("BTC_USDT").unwrap();

    let mut resolved = SubscriptionResolver::resolve(&config, "trades", &context).unwrap();

    resolved.request.unix_seconds = Some(1_800_000_000);

    assert_eq!(
        decode(&resolved.protocol.subscribe(&resolved.request).unwrap()),
        json!({
            "time": 1800000000,
            "channel": "spot.trades",
            "event": "subscribe",
            "payload": ["BTC_USDT"]
        }),
    );
}

#[test]
fn w44_resolves_explicit_depth_and_interval() {
    let config = exchange(
        r#"
[websocket.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
"#,
        r#"
stream_template = "{symbol_lower}@depth{depth}@{interval}"
"#,
    );

    let context = SubscriptionResolutionContext::new("BTCUSDT")
        .unwrap()
        .with_variable("depth", "20")
        .unwrap()
        .with_variable("interval", "100ms")
        .unwrap();

    let resolved = SubscriptionResolver::resolve(&config, "trades", &context).unwrap();

    assert_eq!(
        resolved.request.arguments,
        vec![json!("btcusdt@depth20@100ms")],
    );
}

#[test]
fn w44_missing_template_variable_fails_closed() {
    let config = exchange(
        r#"
[websocket.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
"#,
        r#"
stream_template = "{symbol_lower}@depth{depth}"
"#,
    );

    let context = SubscriptionResolutionContext::new("BTCUSDT").unwrap();

    let result = SubscriptionResolver::resolve(&config, "trades", &context);

    assert!(matches!(
        result,
        Err(
            SubscriptionResolutionError::MissingVariable(
                ref name
            )
        ) if name == "depth"
    ));
}

#[test]
fn w44_invalid_json_template_is_rejected() {
    let config = exchange(
        r#"
[websocket.subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
operation_field = "op"
arguments_field = "args"
"#,
        r#"
subscription_template = '{"channel":"trades","instId":}'
"#,
    );

    let context = SubscriptionResolutionContext::new("BTC-USDT").unwrap();

    assert!(matches!(
        SubscriptionResolver::resolve(&config, "trades", &context,),
        Err(SubscriptionResolutionError::InvalidJson(_))
    ));
}

#[test]
fn w44_rejects_symbol_override() {
    let result = SubscriptionResolutionContext::new("BTCUSDT")
        .unwrap()
        .with_variable("symbol", "ETHUSDT");

    assert!(result.is_err());
}
