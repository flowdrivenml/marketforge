use serde_json::json;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        CompiledSubscriptionAckBinding, SubscriptionAckClassification, SubscriptionAckCompiler,
        SubscriptionAckFieldBinding, SubscriptionIdentityMatch, SubscriptionResolutionContext,
        SubscriptionResolver,
    },
};

// ============================================================
// CONFIGURATION
// ============================================================

fn config(subscription: &str, channel: &str) -> ExchangeConfig {
    let input = format!(
        r#"
schema_version = 1

[exchange]
id = "okx"
market = "spot"
profile = "okx_spot"

[websocket]
url = "wss://example.invalid"

{subscription}

[channels.trades]
enabled = true
format = "trades/okx_spot"

{channel}

[rest]
base_url = "https://example.invalid"
"#
    );

    toml::from_str(&input).unwrap()
}

// ============================================================
// OKX-STYLE OBJECT ARGUMENTS
// ============================================================

fn object_config() -> ExchangeConfig {
    config(
        r#"
[websocket.subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
operation_field = "op"
arguments_field = "args"

request_id_field = "id"
request_id_type = "string"
request_id_optional = true

acknowledgement_field = "event"
subscribe_acknowledgement = "subscribe"
unsubscribe_acknowledgement = "unsubscribe"
"#,
        r#"
channel = "trades-all"
subscription_template = '{"channel":"trades-all","instId":"{symbol}"}'
"#,
    )
}

fn object_binding() -> CompiledSubscriptionAckBinding {
    CompiledSubscriptionAckBinding::new(vec![
        SubscriptionAckFieldBinding::object(0, "channel", "arg.channel"),
        SubscriptionAckFieldBinding::object(0, "instId", "arg.instId"),
    ])
    .unwrap()
}

// ============================================================
// DERIVED IDENTITY MATCHES OUTGOING INSTRUMENT
// ============================================================

#[test]
fn w45_derives_identity_from_resolved_native_arguments() {
    let exchange = object_config();

    let context = SubscriptionResolutionContext::new("BTC-USDT").unwrap();

    let mut resolved = SubscriptionResolver::resolve(&exchange, "trades", &context).unwrap();

    resolved.request.request_id = Some(json!("req-42"));

    let binding = object_binding();

    let expected = binding.expected_subscribe(&resolved).unwrap();

    assert_eq!(expected.request_id, Some(json!("req-42")),);

    let identity = expected.identity.as_ref().unwrap();

    assert_eq!(
        identity.classify(&json!({
            "arg": {
                "channel": "trades-all",
                "instId": "BTC-USDT"
            }
        })),
        SubscriptionIdentityMatch::Matched,
    );

    assert_eq!(
        identity.classify(&json!({
            "arg": {
                "channel": "trades-all",
                "instId": "ETH-USDT"
            }
        })),
        SubscriptionIdentityMatch::Mismatched,
    );
}

// ============================================================
// REQUEST AND EXPECTATION ARE CONSISTENT
// ============================================================

#[test]
fn w45_serialized_request_and_ack_identity_agree() {
    let exchange = object_config();

    let mut resolved = SubscriptionResolver::resolve(
        &exchange,
        "trades",
        &SubscriptionResolutionContext::new("BTC-USDT").unwrap(),
    )
    .unwrap();

    resolved.request.request_id = Some(json!("req-7"));

    let wire = resolved.protocol.subscribe(&resolved.request).unwrap();

    let native: serde_json::Value = serde_json::from_str(&wire).unwrap();

    let expected = object_binding().expected_subscribe(&resolved).unwrap();

    let argument = &native["args"][0];

    assert_eq!(
        expected
            .identity
            .as_ref()
            .unwrap()
            .fields()
            .get("arg.channel"),
        Some(&argument["channel"]),
    );

    assert_eq!(
        expected
            .identity
            .as_ref()
            .unwrap()
            .fields()
            .get("arg.instId"),
        Some(&argument["instId"]),
    );

    let protocol = SubscriptionAckCompiler::compile(&exchange).unwrap();

    assert_eq!(
        protocol.classify(
            &json!({
                "id": "req-7",
                "event": "subscribe",
                "arg": {
                    "channel": "trades-all",
                    "instId": "BTC-USDT"
                }
            }),
            &expected,
        ),
        SubscriptionAckClassification::Confirmed,
    );
}

// ============================================================
// DIFFERENT SYMBOL PRODUCES DIFFERENT ACK EXPECTATION
// ============================================================

#[test]
fn w45_changing_instrument_changes_expected_identity() {
    let exchange = object_config();
    let binding = object_binding();

    let btc = SubscriptionResolver::resolve(
        &exchange,
        "trades",
        &SubscriptionResolutionContext::new("BTC-USDT").unwrap(),
    )
    .unwrap();

    let eth = SubscriptionResolver::resolve(
        &exchange,
        "trades",
        &SubscriptionResolutionContext::new("ETH-USDT").unwrap(),
    )
    .unwrap();

    let btc_identity = binding.derive_identity(&btc).unwrap();
    let eth_identity = binding.derive_identity(&eth).unwrap();

    assert_ne!(btc_identity, eth_identity);
}

// ============================================================
// STRING-ARGUMENT PROTOCOL
// ============================================================

#[test]
fn w45_derives_identity_from_string_argument() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
operation_field = "op"
arguments_field = "args"
"#,
        r#"
stream_template = "publicTrade.{symbol_upper}"
"#,
    );

    let resolved = SubscriptionResolver::resolve(
        &exchange,
        "trades",
        &SubscriptionResolutionContext::new("BTCUSDT").unwrap(),
    )
    .unwrap();

    let binding =
        CompiledSubscriptionAckBinding::new(vec![SubscriptionAckFieldBinding::whole(0, "topic")])
            .unwrap();

    let identity = binding.derive_identity(&resolved).unwrap();

    assert_eq!(
        identity.fields().get("topic"),
        Some(&json!("publicTrade.BTCUSDT")),
    );
}

// ============================================================
// POSITIONAL PAYLOAD
// ============================================================

#[test]
fn w45_derives_identity_from_positional_arguments() {
    let exchange = config(
        r#"
[websocket.subscription]
strategy = "gate_v4_channel_event"
"#,
        r#"
channel = "spot.trades"
subscription_payload = ["{symbol}"]
"#,
    );

    let resolved = SubscriptionResolver::resolve(
        &exchange,
        "trades",
        &SubscriptionResolutionContext::new("BTC_USDT").unwrap(),
    )
    .unwrap();

    let binding = CompiledSubscriptionAckBinding::new(vec![SubscriptionAckFieldBinding::whole(
        0,
        "result.currency_pair",
    )])
    .unwrap();

    let identity = binding.derive_identity(&resolved).unwrap();

    assert_eq!(
        identity.fields().get("result.currency_pair"),
        Some(&json!("BTC_USDT")),
    );
}

// ============================================================
// MISSING ARGUMENT FAILS CLOSED
// ============================================================

#[test]
fn w45_missing_argument_mapping_is_rejected() {
    let exchange = object_config();

    let resolved = SubscriptionResolver::resolve(
        &exchange,
        "trades",
        &SubscriptionResolutionContext::new("BTC-USDT").unwrap(),
    )
    .unwrap();

    let binding = CompiledSubscriptionAckBinding::new(vec![SubscriptionAckFieldBinding::object(
        0,
        "nonexistent",
        "arg.nonexistent",
    )])
    .unwrap();

    assert!(binding.derive_identity(&resolved).is_err());
}

// ============================================================
// INVALID BINDING FAILS DURING CONSTRUCTION
// ============================================================

#[test]
fn w45_invalid_ack_path_is_rejected() {
    assert!(
        CompiledSubscriptionAckBinding::new(vec![SubscriptionAckFieldBinding::whole(
            0,
            "arg..symbol",
        ),])
        .is_err()
    );
}

#[test]
fn w45_duplicate_ack_paths_are_rejected() {
    assert!(
        CompiledSubscriptionAckBinding::new(vec![
            SubscriptionAckFieldBinding::object(0, "channel", "arg.channel",),
            SubscriptionAckFieldBinding::object(0, "instId", "arg.channel",),
        ])
        .is_err()
    );
}
