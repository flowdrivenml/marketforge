use crate::live::config::WebSocketConfig;

use crate::live::config::exchange::HeartbeatStrategy;

// ============================================================
// HELPERS
// ============================================================

fn parse_websocket(input: &str) -> WebSocketConfig {
    toml::from_str(input)
        .unwrap_or_else(|error| panic!("Failed to parse WebSocket config: {error}"))
}

// ============================================================
// CONNECTION ENDPOINTS
// ============================================================

#[test]
fn deserialize_direct_websocket_url() {
    let input = r#"
url = "wss://stream.example.com/ws"
authentication = "none"
symbol_case = "uppercase"
timestamp_unit = "milliseconds"
"#;

    let config = parse_websocket(input);

    assert_eq!(config.url.as_deref(), Some("wss://stream.example.com/ws"));

    assert_eq!(config.authentication.as_deref(), Some("none"));
    assert_eq!(config.symbol_case.as_deref(), Some("uppercase"));
    assert_eq!(config.timestamp_unit.as_deref(), Some("milliseconds"));
}

#[test]
fn deserialize_base_url_and_paths() {
    let input = r#"
base_url = "wss://stream.example.com"
raw_path = "/ws"
combined_path = "/stream"
"#;

    let config = parse_websocket(input);

    assert_eq!(config.base_url.as_deref(), Some("wss://stream.example.com"));

    assert_eq!(config.raw_path.as_deref(), Some("/ws"));
    assert_eq!(config.combined_path.as_deref(), Some("/stream"));
}

#[test]
fn deserialize_alternative_endpoints() {
    let input = r#"
url = "wss://stream.example.com/ws"
alternative_url = "wss://backup.example.com/ws"
market_data_only_url = "wss://data.example.com/ws"
testnet_url = "wss://testnet.example.com/ws"
"#;

    let config = parse_websocket(input);

    assert!(config.alternative_url.is_some());
    assert!(config.market_data_only_url.is_some());
    assert!(config.testnet_url.is_some());
}

#[test]
fn deserialize_named_websocket_routes() {
    let input = r#"
authentication = "none"

[routes.usdt]
url = "wss://stream.example.com/usdt"
authentication = "none"

[routes.btc]
url = "wss://stream.example.com/btc"
authentication = "none"
"#;

    let config = parse_websocket(input);

    assert_eq!(config.routes.len(), 2);

    assert_eq!(
        config.routes["usdt"].url.as_deref(),
        Some("wss://stream.example.com/usdt")
    );

    assert_eq!(
        config.routes["btc"].url.as_deref(),
        Some("wss://stream.example.com/btc")
    );
}

#[test]
fn deserialize_websocket_headers() {
    let input = r#"
url = "wss://stream.example.com/ws"

[headers]
X-Gate-Size-Decimal = "1"
"#;

    let config = parse_websocket(input);

    assert_eq!(
        config
            .headers
            .get("X-Gate-Size-Decimal")
            .map(String::as_str),
        Some("1")
    );
}

// ============================================================
// CONNECTION LIMITS
// ============================================================

#[test]
fn deserialize_connection_limits() {
    let input = r#"
url = "wss://stream.example.com/ws"
connection_lifetime_seconds = 86400
max_subscriptions_per_connection = 1024
max_client_messages_per_second = 10

[limits]
connection_attempt_limit = 300
connection_attempt_window_seconds = 300
max_connections_per_ip = 100
subscription_requests_per_hour = 240
recommended_max_subscriptions = 49
"#;

    let config = parse_websocket(input);

    assert_eq!(config.connection_lifetime_seconds, Some(86400));
    assert_eq!(config.max_subscriptions_per_connection, Some(1024));
    assert_eq!(config.max_client_messages_per_second, Some(10));

    let limits = config.limits.expect("Missing connection limits");

    assert_eq!(limits.connection_attempt_limit, Some(300));
    assert_eq!(limits.connection_attempt_window_seconds, Some(300));
    assert_eq!(limits.max_connections_per_ip, Some(100));
    assert_eq!(limits.subscription_requests_per_hour, Some(240));
    assert_eq!(limits.recommended_max_subscriptions, Some(49));
}

// ============================================================
// HEARTBEAT STRATEGIES
// ============================================================

#[test]
fn deserialize_control_frame_heartbeat() {
    let input = r#"
url = "wss://stream.example.com/ws"

[heartbeat]
strategy = "websocket_control_frames"
respond_to_ping = true
echo_ping_payload = true
server_ping_interval_seconds = 180
pong_deadline_seconds = 600
"#;

    let config = parse_websocket(input);
    let heartbeat = config.heartbeat.expect("Missing heartbeat");

    assert_eq!(
        heartbeat.strategy,
        HeartbeatStrategy::WebsocketControlFrames
    );

    assert_eq!(heartbeat.respond_to_ping, Some(true));
    assert_eq!(heartbeat.echo_ping_payload, Some(true));
    assert_eq!(heartbeat.server_ping_interval_seconds, Some(180));
    assert_eq!(heartbeat.pong_deadline_seconds, Some(600));
}

#[test]
fn deserialize_text_ping_pong_heartbeat() {
    let input = r#"
url = "wss://stream.example.com/ws"

[heartbeat]
strategy = "text_ping_pong"
request = "ping"
response = "pong"
interval_seconds = 30
response_timeout_seconds = 10
reset_timer_on_message = false
"#;

    let config = parse_websocket(input);
    let heartbeat = config.heartbeat.expect("Missing heartbeat");

    assert_eq!(heartbeat.strategy, HeartbeatStrategy::TextPingPong);

    assert_eq!(heartbeat.request.as_deref(), Some("ping"));
    assert_eq!(heartbeat.response.as_deref(), Some("pong"));
    assert_eq!(heartbeat.interval_seconds, Some(30));
    assert_eq!(heartbeat.response_timeout_seconds, Some(10));
}

#[test]
fn deserialize_json_ping_pong_heartbeat() {
    let input = r#"
url = "wss://stream.example.com/ws"

[heartbeat]
strategy = "json_ping_pong"
interval_seconds = 20
request = '{"op":"ping"}'
response_op_field = "op"
response_op_value = "ping"
response_success_field = "success"
response_success_value = true
response_message_field = "ret_msg"
response_message_value = "pong"
timeout_seconds = 30
"#;

    let config = parse_websocket(input);
    let heartbeat = config.heartbeat.expect("Missing heartbeat");

    assert_eq!(heartbeat.strategy, HeartbeatStrategy::JsonPingPong);

    assert_eq!(heartbeat.interval_seconds, Some(20));
    assert_eq!(heartbeat.response_success_value, Some(true));
    assert_eq!(heartbeat.timeout_seconds, Some(30));
}

#[test]
fn deserialize_application_heartbeat() {
    let input = r#"
url = "wss://stream.example.com/ws"

[application_heartbeat]
enabled = false
strategy = "json_channel_ping"
interval_seconds = 30
response_timeout_seconds = 10
request_channel = "futures.ping"
response_channel = "futures.pong"
"#;

    let config = parse_websocket(input);

    let heartbeat = config
        .application_heartbeat
        .expect("Missing application heartbeat");

    assert!(!heartbeat.enabled);

    assert_eq!(heartbeat.strategy, HeartbeatStrategy::JsonChannelPing);

    assert_eq!(heartbeat.request_channel.as_deref(), Some("futures.ping"));
}

#[test]
fn reject_unknown_heartbeat_strategy() {
    let input = r#"
url = "wss://stream.example.com/ws"

[heartbeat]
strategy = "unknown_strategy"
"#;

    assert!(
        toml::from_str::<WebSocketConfig>(input).is_err(),
        "Unknown heartbeat strategy was accepted"
    );
}

// ============================================================
// SUBSCRIPTION PROTOCOLS
// ============================================================

#[test]
fn deserialize_json_rpc_subscription() {
    let input = r#"
url = "wss://stream.example.com/ws"

[subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
unsubscribe_method = "UNSUBSCRIBE"
request_id_field = "id"
request_id_type = "unsigned_integer"
supports_dynamic_subscriptions = true
supports_unsubscribe = true
"#;

    let config = parse_websocket(input);
    let subscription = config.subscription.expect("Missing subscription");

    assert_eq!(subscription.strategy, "json_rpc");
    assert_eq!(subscription.subscribe_method.as_deref(), Some("SUBSCRIBE"));
    assert_eq!(
        subscription.unsubscribe_method.as_deref(),
        Some("UNSUBSCRIBE")
    );
    assert_eq!(subscription.request_id_field.as_deref(), Some("id"));
    assert_eq!(subscription.supports_unsubscribe, Some(true));
}

#[test]
fn deserialize_json_operation_subscription() {
    let input = r#"
url = "wss://stream.example.com/ws"

[subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
unsubscribe_method = "unsubscribe"
operation_field = "op"
arguments_field = "args"
request_id_field = "req_id"
request_id_type = "string"
request_id_optional = true
success_field = "success"
success_value = true
supports_dynamic_subscriptions = true
supports_unsubscribe = true
"#;

    let config = parse_websocket(input);
    let subscription = config.subscription.expect("Missing subscription");

    assert_eq!(subscription.strategy, "json_operation");
    assert_eq!(subscription.operation_field.as_deref(), Some("op"));
    assert_eq!(subscription.arguments_field.as_deref(), Some("args"));

    assert_eq!(subscription.success_value, Some(toml::Value::Boolean(true)));
}

#[test]
fn deserialize_channel_event_subscription() {
    let input = r#"
url = "wss://stream.example.com/ws"

[subscription]
strategy = "gate_v4_channel_event"
subscribe_method = "subscribe"
unsubscribe_method = "unsubscribe"
operation_field = "event"
arguments_field = "payload"
channel_field = "channel"
timestamp_field = "time"
timestamp_unit = "seconds"
acknowledgement_field = "event"
subscribe_acknowledgement = "subscribe"
unsubscribe_acknowledgement = "unsubscribe"
supports_dynamic_subscriptions = true
supports_unsubscribe = true
"#;

    let config = parse_websocket(input);
    let subscription = config.subscription.expect("Missing subscription");

    assert_eq!(subscription.strategy, "gate_v4_channel_event");
    assert_eq!(subscription.channel_field.as_deref(), Some("channel"));
    assert_eq!(subscription.timestamp_field.as_deref(), Some("time"));
}

// ============================================================
// MESSAGE ROUTING
// ============================================================

#[test]
fn deserialize_topic_routing() {
    let input = r#"
url = "wss://stream.example.com/ws"

[routing]
mode = "topic"
topic_field = "topic"
message_type_field = "type"
payload_field = "data"
"#;

    let config = parse_websocket(input);
    let routing = config.routing.expect("Missing routing");

    assert_eq!(routing.mode, "topic");
    assert_eq!(routing.topic_field.as_deref(), Some("topic"));
    assert_eq!(routing.payload_field.as_deref(), Some("data"));
}

#[test]
fn deserialize_argument_routing() {
    let input = r#"
url = "wss://stream.example.com/ws"

[routing]
mode = "arg"
channel_field = "arg.channel"
instrument_field = "arg.instId"
payload_field = "data"
action_field = "action"
"#;

    let config = parse_websocket(input);
    let routing = config.routing.expect("Missing routing");

    assert_eq!(routing.mode, "arg");
    assert_eq!(routing.channel_field.as_deref(), Some("arg.channel"));
    assert_eq!(routing.instrument_field.as_deref(), Some("arg.instId"));
}

#[test]
fn deserialize_channel_routing() {
    let input = r#"
url = "wss://stream.example.com/ws"

[routing]
mode = "channel"
channel_field = "channel"
event_field = "event"
payload_field = "result"
update_event = "update"
"#;

    let config = parse_websocket(input);
    let routing = config.routing.expect("Missing routing");

    assert_eq!(routing.mode, "channel");
    assert_eq!(routing.event_field.as_deref(), Some("event"));
    assert_eq!(routing.update_event.as_deref(), Some("update"));
}

// ============================================================
// CONNECTION LIFECYCLE
// ============================================================

#[test]
fn deserialize_connection_lifecycle() {
    let input = r#"
url = "wss://stream.example.com/ws"

[lifecycle]
rotate_before_expiration_seconds = 300
system_channel = "futures.system"
system_event = "update"
upgrade_action = "reconnect_and_restore"
upgrade_type_field = "result.type"
upgrade_type_value = "upgrade"
"#;

    let config = parse_websocket(input);
    let lifecycle = config.lifecycle.expect("Missing lifecycle");

    assert_eq!(lifecycle.rotate_before_expiration_seconds, Some(300));
    assert_eq!(
        lifecycle.upgrade_action.as_deref(),
        Some("reconnect_and_restore")
    );
}

#[test]
fn deserialize_reconnection_strategy() {
    let input = r#"
url = "wss://stream.example.com/ws"

[connection]
reconnect_strategy = "exponential_backoff"
"#;

    let config = parse_websocket(input);
    let connection = config.connection.expect("Missing connection config");

    assert_eq!(
        connection.reconnect_strategy.as_deref(),
        Some("exponential_backoff")
    );
}

// ============================================================
// STRICT FIELD RECOGNITION
// ============================================================

#[test]
fn reject_unknown_websocket_field() {
    let input = r#"
url = "wss://stream.example.com/ws"
unknown_setting = 123
"#;

    assert!(
        toml::from_str::<WebSocketConfig>(input).is_err(),
        "Unknown WebSocket field was accepted"
    );
}

#[test]
fn reject_unknown_nested_heartbeat_field() {
    let input = r#"
url = "wss://stream.example.com/ws"

[heartbeat]
strategy = "text_ping_pong"
unknown_heartbeat_setting = 123
"#;

    assert!(
        toml::from_str::<WebSocketConfig>(input).is_err(),
        "Unknown heartbeat field was accepted"
    );
}

#[test]
fn reject_invalid_field_type() {
    let input = r#"
url = "wss://stream.example.com/ws"
connection_lifetime_seconds = "not_a_number"
"#;

    assert!(
        toml::from_str::<WebSocketConfig>(input).is_err(),
        "Invalid field type was accepted"
    );
}

// ============================================================
// SERIALIZATION
// ============================================================

#[test]
fn websocket_config_round_trip() {
    let input = r#"
url = "wss://stream.example.com/ws"
authentication = "none"
symbol_case = "uppercase"

[heartbeat]
strategy = "text_ping_pong"
request = "ping"
response = "pong"
interval_seconds = 30

[subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
unsubscribe_method = "unsubscribe"
"#;

    let original: toml::Value = toml::from_str(input).expect("Invalid original TOML");

    let config = parse_websocket(input);

    let serialized = toml::to_string(&config).expect("Failed to serialize WebSocket config");

    let reconstructed: toml::Value = toml::from_str(&serialized).expect("Invalid serialized TOML");

    assert_eq!(
        original, reconstructed,
        "WebSocket configuration changed during round trip"
    );
}

// ============================================================
// REAL EXCHANGE CONFIGURATIONS
// ============================================================

// ============================================================
// FULL CONFIGURATION COMPATIBILITY
// ============================================================
