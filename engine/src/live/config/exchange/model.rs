use super::{
    ChannelConfig, ExchangeId, ExchangeIdentity, MarketMapping, MarketType, RestConfig,
    RestEndpoint, WebSocketConfig, WebSocketRoute,
};
use crate::live::config::{error::ConfigError, loader::load_toml};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, path::Path};

// ---------------------------------------------------------
// EXCHANGE CONFIGURATION
// ---------------------------------------------------------

/// Complete exchange connectivity configuration.
///
/// Each configuration represents one exchange and one
/// canonical MarketForge market category.
///
/// The configuration is loaded, deserialized, and validated
/// before becoming available to runtime components.
///
/// No runtime mutation or hot-reloading is supported.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeConfig {
    /// Exchange configuration schema version.
    pub schema_version: u32,

    /// Exchange identity and market classification.
    pub exchange: ExchangeIdentity,

    /// WebSocket connectivity and protocol configuration.
    pub websocket: WebSocketConfig,

    /// Configured market-data channels.
    ///
    /// Examples:
    /// - trades
    /// - aggregate_trades
    /// - depth
    /// - depth1
    /// - depth5
    /// - depth50
    /// - depth_snapshot
    /// - bbo
    pub channels: BTreeMap<String, ChannelConfig>,

    /// REST connectivity, endpoints, and rate limits.
    pub rest: RestConfig,

    /// Optional exchange-native market classification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub market_mapping: Option<MarketMapping>,
}

impl ExchangeConfig {
    /// Load and validate an exchange configuration.
    ///
    /// Performs:
    /// 1. Filesystem reading.
    /// 2. TOML deserialization.
    /// 3. Structural and semantic validation.
    ///
    /// Returns a fully loaded configuration or ConfigError.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let path = path.as_ref();

        let config: Self = load_toml(path)?;

        config
            .validate()
            .map_err(|reason| ConfigError::Validation {
                path: path.to_path_buf(),
                reason,
            })?;

        Ok(config)
    }

    /// Retrieve a configured market-data channel.
    pub fn channel(&self, name: &str) -> Option<&ChannelConfig> {
        self.channels.get(name)
    }

    /// Retrieve a configured REST endpoint.
    pub fn endpoint(&self, name: &str) -> Option<&RestEndpoint> {
        self.rest.endpoint(name)
    }

    /// Retrieve a named WebSocket route.
    pub fn route(&self, name: &str) -> Option<&WebSocketRoute> {
        self.websocket.routes.get(name)
    }

    /// Iterate over enabled market-data channels.
    pub fn enabled_channels(&self) -> impl Iterator<Item = (&str, &ChannelConfig)> {
        self.channels
            .iter()
            .filter_map(|(name, channel)| channel.enabled.then_some((name.as_str(), channel)))
    }

    /// Return the unique profile identifier.
    pub fn profile(&self) -> &str {
        &self.exchange.profile
    }

    /// Return the canonical exchange identifier.
    pub fn exchange_id(&self) -> ExchangeId {
        self.exchange.id
    }

    /// Return the canonical market category.
    pub fn market(&self) -> MarketType {
        self.exchange.market
    }
}
