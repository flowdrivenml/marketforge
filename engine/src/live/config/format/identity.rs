use serde::{Deserialize, Serialize};

use crate::live::config::exchange::{ExchangeId, MarketType};

// ============================================================
// FORMAT KIND
// ============================================================

/// Identifies the family of a market-data format.
///
/// FormatKind is determined by the directory containing
/// the configuration, not by a field inside the TOML file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatKind {
    Trades,
    Depth,
}

impl FormatKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Trades => "trades",
            Self::Depth => "depth",
        }
    }

    pub const fn expected_target_schema(self) -> &'static str {
        match self {
            Self::Trades => "canonical_trade",
            Self::Depth => "canonical_l2_update",
        }
    }
}

// ============================================================
// FORMAT IDENTITY
// ============================================================

/// Identity and output schema of an exchange market-data format.
///
/// Examples:
///
/// - trades/binance_spot
/// - trades/binance_inverse
/// - depth/bybit_linear
/// - depth/okx_spot
///
/// The same format ID may exist in both the trades and depth
/// namespaces because the format kind is part of its identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FormatIdentity {
    pub id: String,

    pub exchange: ExchangeId,

    pub market: MarketType,

    pub channel: String,

    pub encoding: String,

    pub target_schema: String,
}

impl FormatIdentity {
    /// Returns the expected format ID for this exchange and market.
    pub fn expected_id(&self) -> String {
        format!("{}_{}", self.exchange.as_str(), self.market.as_str())
    }

    /// Returns the fully qualified format identifier.
    ///
    /// Examples:
    ///
    /// - trades/binance_spot
    /// - depth/bybit_linear
    pub fn qualified_id(&self, kind: FormatKind) -> String {
        format!("{}/{}", kind.as_str(), self.id)
    }

    /// Validates the format identity against its namespace.
    pub fn validate(&self, kind: FormatKind) -> Result<(), String> {
        let expected_id = self.expected_id();

        if self.id != expected_id {
            return Err(format!(
                "format.id '{}' does not match expected '{}'",
                self.id, expected_id
            ));
        }

        if self.encoding != "json" {
            return Err(format!(
                "format.encoding '{}' is unsupported; expected 'json'",
                self.encoding
            ));
        }

        let expected_schema = kind.expected_target_schema();

        if self.target_schema != expected_schema {
            return Err(format!(
                "format.target_schema '{}' does not match expected '{}' for {}",
                self.target_schema,
                expected_schema,
                kind.as_str()
            ));
        }

        if self.channel.trim().is_empty() {
            return Err("format.channel cannot be empty".into());
        }

        match kind {
            FormatKind::Trades => {
                if !matches!(
                    self.channel.as_str(),
                    "trades" | "aggregate_trades" | "trades-all"
                ) {
                    return Err(format!(
                        "unsupported trade format channel '{}'",
                        self.channel
                    ));
                }
            }

            FormatKind::Depth => {
                if self.channel != "depth" {
                    return Err(format!(
                        "depth format requires channel 'depth', got '{}'",
                        self.channel
                    ));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::live::config::exchange::{ExchangeId, MarketType};
    use crate::live::config::format::{FormatIdentity, FormatKind};

    // ============================================================
    // FORMAT KIND
    // ============================================================

    #[test]
    fn format_kind_names() {
        assert_eq!(FormatKind::Trades.as_str(), "trades");
        assert_eq!(FormatKind::Depth.as_str(), "depth");
    }

    #[test]
    fn format_kind_target_schemas() {
        assert_eq!(
            FormatKind::Trades.expected_target_schema(),
            "canonical_trade"
        );

        assert_eq!(
            FormatKind::Depth.expected_target_schema(),
            "canonical_l2_update"
        );
    }

    // ============================================================
    // FORMAT IDENTITY
    // ============================================================

    fn identity(
        exchange: ExchangeId,
        market: MarketType,
        channel: &str,
        target_schema: &str,
    ) -> FormatIdentity {
        FormatIdentity {
            id: format!("{}_{}", exchange.as_str(), market.as_str()),
            exchange,
            market,
            channel: channel.into(),
            encoding: "json".into(),
            target_schema: target_schema.into(),
        }
    }

    #[test]
    fn expected_format_id() {
        let config = identity(
            ExchangeId::Binance,
            MarketType::Spot,
            "trades",
            "canonical_trade",
        );

        assert_eq!(config.expected_id(), "binance_spot");
    }

    #[test]
    fn qualified_trade_format_id() {
        let config = identity(
            ExchangeId::Binance,
            MarketType::Spot,
            "trades",
            "canonical_trade",
        );

        assert_eq!(
            config.qualified_id(FormatKind::Trades),
            "trades/binance_spot"
        );
    }

    #[test]
    fn qualified_depth_format_id() {
        let config = identity(
            ExchangeId::Bybit,
            MarketType::Linear,
            "depth",
            "canonical_l2_update",
        );

        assert_eq!(config.qualified_id(FormatKind::Depth), "depth/bybit_linear");
    }

    #[test]
    fn valid_trade_identity() {
        let config = identity(
            ExchangeId::Binance,
            MarketType::Spot,
            "trades",
            "canonical_trade",
        );

        assert!(config.validate(FormatKind::Trades).is_ok());
    }

    #[test]
    fn valid_aggregated_trade_identity() {
        let config = identity(
            ExchangeId::Binance,
            MarketType::Linear,
            "aggregate_trades",
            "canonical_trade",
        );

        assert!(config.validate(FormatKind::Trades).is_ok());
    }

    #[test]
    fn valid_okx_trade_identity() {
        let config = identity(
            ExchangeId::Okx,
            MarketType::Spot,
            "trades-all",
            "canonical_trade",
        );

        assert!(config.validate(FormatKind::Trades).is_ok());
    }

    #[test]
    fn valid_depth_identity() {
        let config = identity(
            ExchangeId::Gateio,
            MarketType::Inverse,
            "depth",
            "canonical_l2_update",
        );

        assert!(config.validate(FormatKind::Depth).is_ok());
    }

    // ============================================================
    // INVALID IDENTITIES
    // ============================================================

    #[test]
    fn reject_incorrect_format_id() {
        let mut config = identity(
            ExchangeId::Binance,
            MarketType::Spot,
            "trades",
            "canonical_trade",
        );

        config.id = "binance_linear".into();

        assert!(config.validate(FormatKind::Trades).is_err());
    }

    #[test]
    fn reject_unsupported_encoding() {
        let mut config = identity(
            ExchangeId::Binance,
            MarketType::Spot,
            "trades",
            "canonical_trade",
        );

        config.encoding = "protobuf".into();

        assert!(config.validate(FormatKind::Trades).is_err());
    }

    #[test]
    fn reject_wrong_trade_target_schema() {
        let config = identity(
            ExchangeId::Binance,
            MarketType::Spot,
            "trades",
            "canonical_l2_update",
        );

        assert!(config.validate(FormatKind::Trades).is_err());
    }

    #[test]
    fn reject_wrong_depth_target_schema() {
        let config = identity(
            ExchangeId::Binance,
            MarketType::Spot,
            "depth",
            "canonical_trade",
        );

        assert!(config.validate(FormatKind::Depth).is_err());
    }

    #[test]
    fn reject_empty_channel() {
        let config = identity(ExchangeId::Binance, MarketType::Spot, "", "canonical_trade");

        assert!(config.validate(FormatKind::Trades).is_err());
    }

    #[test]
    fn reject_trade_channel_for_depth() {
        let config = identity(
            ExchangeId::Binance,
            MarketType::Spot,
            "trades",
            "canonical_l2_update",
        );

        assert!(config.validate(FormatKind::Depth).is_err());
    }

    #[test]
    fn reject_depth_channel_for_trades() {
        let config = identity(
            ExchangeId::Binance,
            MarketType::Spot,
            "depth",
            "canonical_trade",
        );

        assert!(config.validate(FormatKind::Trades).is_err());
    }

    // ============================================================
    // STRICT DESERIALIZATION
    // ============================================================

    #[test]
    fn reject_unknown_identity_fields() {
        let toml = r#"
id = "binance_spot"
exchange = "binance"
market = "spot"
channel = "trades"
encoding = "json"
target_schema = "canonical_trade"
unexpected_field = true
"#;

        assert!(toml::from_str::<FormatIdentity>(toml).is_err());
    }

    #[test]
    fn reject_unknown_exchange() {
        let toml = r#"
id = "unknown_spot"
exchange = "unknown"
market = "spot"
channel = "trades"
encoding = "json"
target_schema = "canonical_trade"
"#;

        assert!(toml::from_str::<FormatIdentity>(toml).is_err());
    }

    #[test]
    fn reject_unknown_market() {
        let toml = r#"
id = "binance_unknown"
exchange = "binance"
market = "unknown"
channel = "trades"
encoding = "json"
target_schema = "canonical_trade"
"#;

        assert!(toml::from_str::<FormatIdentity>(toml).is_err());
    }
}
