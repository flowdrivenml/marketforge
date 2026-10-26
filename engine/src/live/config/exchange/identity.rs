use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExchangeIdentity {
    pub id: ExchangeId,
    pub market: MarketType,
    pub profile: String,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_generation: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ExchangeId {
    Binance,
    Bybit,
    Okx,
    Bitget,
    Gateio,
}

impl ExchangeId {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Binance => "binance",
            Self::Bybit => "bybit",
            Self::Okx => "okx",
            Self::Bitget => "bitget",
            Self::Gateio => "gateio",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarketType {
    Spot,
    Linear,
    Inverse,
}

impl MarketType {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Spot => "spot",
            Self::Linear => "linear",
            Self::Inverse => "inverse",
        }
    }
}

impl ExchangeIdentity {
    pub fn expected_profile(&self) -> String {
        format!("{}_{}", self.id.as_str(), self.market.as_str())
    }

    pub fn validate(&self) -> Result<(), String> {
        let expected = self.expected_profile();

        if self.profile != expected {
            return Err(format!(
                "exchange profile '{}' does not match expected '{}'",
                self.profile, expected
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
