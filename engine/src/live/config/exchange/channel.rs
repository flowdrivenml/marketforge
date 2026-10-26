use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Configuration for a single exchange market-data channel.
///
/// The channel name (trades, depth, bbo, etc.) is the key in
/// ExchangeConfig.channels.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ChannelConfig {
    pub enabled: bool,
    /// Field indicating whether a depth update is a complete snapshot.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub full_snapshot_field: Option<String>,

    /// Exchange-native, market-specific trade identifier field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_trade_id_field: Option<String>,
    // Subscription
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub channel: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stream_template: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_template: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subscription_payload: Option<Vec<String>>,

    // Normalization format
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,

    // Trade processing
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_semantics: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub records: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload_path: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub side_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery_endpoint: Option<String>,

    // Order-book depth
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<DepthValue>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_depth: Option<u64>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_depths: Vec<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_interval_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub push_intervals_ms: BTreeMap<String, u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_interval: Option<String>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_intervals: Vec<String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub constraints: BTreeMap<String, Vec<u64>>,

    // Synchronization
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronization: Option<SynchronizationConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub synchronization_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_strategy: Option<String>,

    // Quantity handling
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity: Option<QuantityConfig>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity_mode: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zero_quantity_action: Option<String>,
    // ---------------------------------------------------------
    // EXCHANGE EVENT IDENTIFICATION
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_type: Option<String>,

    // ---------------------------------------------------------
    // BINANCE DEPTH STREAM CONFIGURATION
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fast_stream_template: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_interval_ms: Option<u64>,

    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_intervals_ms: Vec<u64>,

    // ---------------------------------------------------------
    // FLAT L2 SEQUENCE FIELDS
    // ---------------------------------------------------------
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_endpoint: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_update_id_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_update_id_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_update_id_field: Option<String>,

    // ---------------------------------------------------------
    // NATIVE TRADE IDENTIFICATION
    // ---------------------------------------------------------
    /// Field containing the exchange-native trade identifier.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_trade_id_field: Option<String>,

    /// Field containing the exchange-native trade timestamp.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trade_timestamp_field: Option<String>,
}

/// Order-book depth may be a numeric limit or an exchange-defined value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DepthValue {
    Levels(u64),
    Named(String),
}

/// L2 snapshot, sequence, and synchronization configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SynchronizationConfig {
    pub strategy: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_source: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_message_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delta_message_type: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_action: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_endpoint: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_snapshot_endpoint: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub snapshot_sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub previous_sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_update_id_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_update_id_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_id_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cross_sequence_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub continuity_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub initial_bridge_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer_updates_before_snapshot: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_consecutive_update_id: Option<bool>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_update_id: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reset_previous_sequence_id: Option<u64>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checksum_strategy: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_full_snapshot_field: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_full_snapshot_action: Option<String>,
}

/// Quantity representation and order-book update semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuantityConfig {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw_unit: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quantity_semantics: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub update_mode: Option<String>,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zero_quantity_action: Option<String>,
}

#[cfg(test)]
mod tests;
