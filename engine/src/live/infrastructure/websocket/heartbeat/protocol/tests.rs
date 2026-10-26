use crate::live::{
    config::exchange::HeartbeatConfig, infrastructure::websocket::HeartbeatProtocol,
};

fn compile(toml_text: &str) -> HeartbeatProtocol {
    let config: HeartbeatConfig = toml::from_str(toml_text).unwrap();

    HeartbeatProtocol::compile(&config).unwrap()
}

#[test]
fn compiles_text_ping_pong() {
    let protocol = compile(
        r#"
strategy = "text_ping_pong"
request = "ping"
response = "pong"
"#,
    );

    assert_eq!(protocol.request_text(), "ping");

    assert!(protocol.matches_response("pong"));
    assert!(!protocol.matches_response("ping"));
    assert!(!protocol.matches_response("other"));
}

#[test]
fn compiles_json_exact_response() {
    let protocol = compile(
        r#"
strategy = "json_ping_pong"
request = '{"op":"ping"}'
response = '{"op":"pong"}'
"#,
    );

    assert_eq!(protocol.request_text(), r#"{"op":"ping"}"#,);

    assert!(protocol.matches_response(r#"{"op":"pong"}"#));
    assert!(!protocol.matches_response(r#"{"op":"subscribe"}"#));
}

#[test]
fn compiles_json_response_fields() {
    let protocol = compile(
        r#"
strategy = "json_ping_pong"
request = '{"op":"ping"}'

response_op_field = "op"
response_op_value = "pong"

response_success_field = "success"
response_success_value = true
"#,
    );

    assert!(protocol.matches_response(r#"{"op":"pong","success":true,"time":123}"#));

    assert!(!protocol.matches_response(r#"{"op":"pong","success":false}"#));

    assert!(!protocol.matches_response(r#"{"op":"subscribe","success":true}"#));

    assert!(!protocol.matches_response(r#"{"op":"pong"}"#));
}

#[test]
fn does_not_consume_market_data() {
    let protocol = compile(
        r#"
strategy = "json_ping_pong"
request = '{"op":"ping"}'
response_op_field = "op"
response_op_value = "pong"
"#,
    );

    assert!(!protocol.matches_response(r#"{"topic":"trades","data":[{"price":"80000"}]}"#));

    assert!(!protocol.matches_response(r#"{"topic":"orderbook","data":{"bids":[]}}"#));
}

#[test]
fn rejects_missing_response_matcher() {
    let config: HeartbeatConfig = toml::from_str(
        r#"
strategy = "json_ping_pong"
request = '{"op":"ping"}'
"#,
    )
    .unwrap();

    assert!(HeartbeatProtocol::compile(&config).is_err());
}

#[test]
fn rejects_incomplete_matcher() {
    let config: HeartbeatConfig = toml::from_str(
        r#"
strategy = "json_ping_pong"
request = '{"op":"ping"}'
response_op_field = "op"
"#,
    )
    .unwrap();

    assert!(HeartbeatProtocol::compile(&config).is_err());
}

#[test]
fn rejects_invalid_json_request() {
    let config: HeartbeatConfig = toml::from_str(
        r#"
strategy = "json_ping_pong"
request = "invalid-json"
response = '{"op":"pong"}'
"#,
    )
    .unwrap();

    assert!(HeartbeatProtocol::compile(&config).is_err());
}

#[test]
fn rejects_unsupported_channel_protocol() {
    let config: HeartbeatConfig = toml::from_str(
        r#"
strategy = "json_channel_ping"
request = '{"channel":"ping"}'
response = '{"channel":"pong"}'
"#,
    )
    .unwrap();

    assert!(HeartbeatProtocol::compile(&config).is_err());
}
