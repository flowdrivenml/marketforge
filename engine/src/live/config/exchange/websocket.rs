use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// WebSocket connectivity and protocol configuration.
///
/// Supports direct URLs, base URLs with paths, and named routes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketConfig {
    // ---------------------------------------------------------
    // ENDPOINTS
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alternative_url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_data_only_url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub testnet_url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_path: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combined_path: Option<String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub routes: BTreeMap<String, WebSocketRoute>,
    /// Maximum connection attempts within the configured window.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_attempt_limit: Option<u64>,

    /// Connection-attempt window in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_attempt_window_seconds: Option<u64>,
    // ---------------------------------------------------------
    // CONNECTION PROPERTIES
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_case: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_unit: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope_timestamp_unit: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_lifetime_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_subscriptions_per_connection: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_client_messages_per_second: Option<u64>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub headers: BTreeMap<String, String>,

    // ---------------------------------------------------------
    // PROTOCOL CONFIGURATION
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<WebSocketLimits>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub heartbeat: Option<HeartbeatConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_heartbeat: Option<ApplicationHeartbeatConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription: Option<SubscriptionConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routing: Option<RoutingConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lifecycle: Option<LifecycleConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection: Option<ConnectionConfig>,
}

/// Named WebSocket endpoint.
///
/// Primarily useful for exchanges with settlement-specific
/// connections, such as Gate.io futures.
/// Named WebSocket connection route.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketRoute {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_mode: Option<String>,
}

/// Exchange connection and subscription limits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebSocketLimits {
    /// Maximum serialized subscription payload size in bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_subscription_payload_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_attempt_limit: Option<u64>,
    /// Maximum number of arguments in a single subscription request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_args_per_subscribe_request: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_attempt_window_seconds: Option<u64>,
    /// Maximum subscription operations allowed per hour.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_operations_per_hour: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections_per_ip: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_market_data_connections_per_ip: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_requests_per_hour: Option<u64>,
    /// Exchange-native instrument symbol format.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_format: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_subscriptions_per_connection: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recommended_max_subscriptions: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_client_messages_per_second: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_args_characters: Option<u64>,
    /// Maximum WebSocket connection attempts per second.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_attempts_per_second: Option<u64>,
}

/// Supported heartbeat strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HeartbeatStrategy {
    WebsocketControlFrames,
    TextPingPong,
    JsonPingPong,
    JsonChannelPing,
}

/// WebSocket heartbeat configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeartbeatConfig {
    pub strategy: HeartbeatStrategy,
    /// Connection inactivity threshold in seconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inactivity_seconds: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub enabled: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_timeout_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_ping_interval_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_inactivity_limit_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pong_deadline_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub respond_to_ping: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub echo_ping_payload: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_timer_on_message: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_op_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_op_value: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_success_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_success_value: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_message_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_message_value: Option<String>,
}

/// Optional application-level heartbeat.
///
/// Some exchanges support application heartbeats in addition
/// to WebSocket control frames.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApplicationHeartbeatConfig {
    pub enabled: bool,

    pub strategy: HeartbeatStrategy,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_timeout_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_channel: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_channel: Option<String>,
}

/// WebSocket subscription protocol.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionConfig {
    pub strategy: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribe_method: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsubscribe_method: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub operation_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id_optional: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub acknowledgement_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscribe_acknowledgement: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unsubscribe_acknowledgement: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success_value: Option<toml::Value>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_event: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_code_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_id_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_type_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_unit: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_dynamic_subscriptions: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_unsubscribe: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub list_method: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub success_is_null: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub supports_list_subscriptions: Option<bool>,
}

/// Incoming WebSocket message routing.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoutingConfig {
    pub mode: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topic_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_event: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_type_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub action_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_type_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combined_wrapper_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub combined_stream_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub symbol_field: Option<String>,
}

/// Connection lifecycle behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LifecycleConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotate_before_expiration_seconds: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_channel: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_event: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upgrade_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upgrade_type_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub upgrade_type_value: Option<String>,
    /// Exchange event announcing an upcoming server shutdown.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server_shutdown_event: Option<String>,
}

/// Reconnection behavior.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectionConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reconnect_strategy: Option<String>,
}
