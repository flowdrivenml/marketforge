use std::{collections::BTreeMap, path::Path};

use serde::{Deserialize, Serialize};

use super::{error::ConfigError, loader::load_toml};

/// Complete exchange connectivity profile.
///
/// Deserializes the shared configuration structure while preserving
/// exchange-specific fields for later protocol compilation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub schema_version: u32,

    pub exchange: ExchangeIdentity,

    pub websocket: WebSocketConfig,

    pub channels: BTreeMap<String, ChannelConfig>,

    pub rest: RestConfig,

    #[serde(default)]
    pub market_mapping: Option<toml::Table>,
}

/// Exchange and market identification.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeIdentity {
    pub id: String,

    pub market: String,

    pub profile: String,

    #[serde(default)]
    pub api_generation: Option<String>,
}

/// WebSocket connectivity configuration.
///
/// Common connection properties are typed. Exchange-specific
/// settings such as heartbeat, subscriptions, routing, limits,
/// connection lifecycle, and headers are preserved in `extra`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    #[serde(default)]
    pub url: Option<String>,

    #[serde(default)]
    pub base_url: Option<String>,

    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub routes: BTreeMap<String, toml::Table>,

    #[serde(default)]
    pub authentication: Option<String>,

    #[serde(default)]
    pub symbol_case: Option<String>,

    #[serde(default)]
    pub timestamp_unit: Option<String>,

    #[serde(default)]
    pub envelope_timestamp_unit: Option<String>,

    #[serde(flatten)]
    pub extra: toml::Table,
}

/// One configured market-data channel.
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
///
/// Channel-specific parameters remain available through `extra`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelConfig {
    pub enabled: bool,

    #[serde(default)]
    pub format: Option<String>,

    #[serde(flatten)]
    pub extra: toml::Table,
}

/// REST API configuration.
///
/// Endpoint definitions are retained by name, allowing future
/// REST components to compile the appropriate request strategies.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestConfig {
    pub base_url: String,

    #[serde(default)]
    pub authentication: Option<String>,

    #[serde(default)]
    pub request_timeout_seconds: Option<u64>,

    #[serde(default)]
    pub endpoints: BTreeMap<String, toml::Table>,

    #[serde(flatten)]
    pub extra: toml::Table,
}

impl ExchangeConfig {
    /// Load an exchange configuration from TOML.
    ///
    /// Loading and structural validation occur once.
    /// Exchange protocol capabilities are validated separately.
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

    /// Validate the exchange profile's basic structure.
    ///
    /// This does not certify exchange API behavior.
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_version != 1 {
            return Err(format!(
                "unsupported exchange schema version: {}",
                self.schema_version
            ));
        }

        if self.exchange.id.trim().is_empty() {
            return Err("exchange.id cannot be empty".into());
        }

        if !matches!(self.exchange.market.as_str(), "spot" | "linear" | "inverse") {
            return Err(format!(
                "unsupported exchange market: {}",
                self.exchange.market
            ));
        }

        let expected_profile = format!("{}_{}", self.exchange.id, self.exchange.market);

        if self.exchange.profile != expected_profile {
            return Err(format!(
                "exchange.profile '{}' does not match '{}'",
                self.exchange.profile, expected_profile
            ));
        }

        // Exchanges may declare one direct URL, a base URL,
        // or multiple named connection routes.
        let has_url = self
            .websocket
            .url
            .as_ref()
            .is_some_and(|url| !url.trim().is_empty());

        let has_base_url = self
            .websocket
            .base_url
            .as_ref()
            .is_some_and(|url| !url.trim().is_empty());

        let has_routes = !self.websocket.routes.is_empty();

        if !has_url && !has_base_url && !has_routes {
            return Err("websocket requires a URL, base_url, or routes".into());
        }

        if self.rest.base_url.trim().is_empty() {
            return Err("rest.base_url cannot be empty".into());
        }

        if self.channels.is_empty() {
            return Err("exchange must define at least one channel".into());
        }

        for (name, channel) in &self.channels {
            if channel.enabled
                && channel
                    .format
                    .as_ref()
                    .is_none_or(|format| format.trim().is_empty())
            {
                return Err(format!("enabled channel '{name}' requires a format"));
            }
        }

        Ok(())
    }

    /// Retrieve a channel by name.
    pub fn channel(&self, name: &str) -> Option<&ChannelConfig> {
        self.channels.get(name)
    }

    /// Retrieve a REST endpoint by name.
    pub fn endpoint(&self, name: &str) -> Option<&toml::Table> {
        self.rest.endpoints.get(name)
    }

    /// Retrieve a named WebSocket connection route.
    pub fn route(&self, name: &str) -> Option<&toml::Table> {
        self.websocket.routes.get(name)
    }

    /// Iterate over enabled market-data channels.
    pub fn enabled_channels(&self) -> impl Iterator<Item = (&str, &ChannelConfig)> {
        self.channels
            .iter()
            .filter_map(|(name, channel)| channel.enabled.then_some((name.as_str(), channel)))
    }
}
