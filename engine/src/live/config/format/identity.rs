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
