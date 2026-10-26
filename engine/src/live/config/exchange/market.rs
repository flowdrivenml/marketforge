use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Exchange-specific mapping between MarketForge's canonical
/// market categories and the exchange's native market types.
///
/// Instrument classification is performed externally.
/// This configuration describes how resolved native instrument
/// types map to exchange connectivity and REST parameters.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MarketMapping {
    /// Instrument classification resolver.
    ///
    /// Example: "marketforge_instrument_catalog".
    pub resolver: String,

    // ---------------------------------------------------------
    // NATIVE MARKET CLASSIFICATION
    // ---------------------------------------------------------
    /// Single native market category.
    ///
    /// Example: "COIN-FUTURES".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_category: Option<String>,

    /// Supported native market categories.
    ///
    /// Example: ["USDT-FUTURES", "USDC-FUTURES"].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_native_categories: Vec<String>,

    /// Native WebSocket instrument type.
    ///
    /// Example: "coin-futures".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_inst_type: Option<String>,

    /// Native REST category.
    ///
    /// Example: "COIN-FUTURES".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_category: Option<String>,

    // ---------------------------------------------------------
    // SETTLEMENT CLASSIFICATION
    // ---------------------------------------------------------
    /// Single native settlement.
    ///
    /// Example: "btc".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_settlement: Option<String>,

    /// Supported settlement currencies.
    ///
    /// Example: ["usdt", "usd1"].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub supported_settlements: Vec<String>,

    // ---------------------------------------------------------
    // CONTRACT CLASSIFICATION
    // ---------------------------------------------------------
    /// Whether the resolved instrument must satisfy the
    /// exchange's inverse contract classification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_inverse_contract_classification: Option<bool>,

    /// Whether the resolved instrument must satisfy the
    /// exchange's linear contract classification.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub require_linear_contract_classification: Option<bool>,

    // ---------------------------------------------------------
    // NATIVE TYPE MAPPINGS
    // ---------------------------------------------------------
    /// Named native instrument mappings.
    ///
    /// Examples:
    ///
    /// - USDT-FUTURES
    /// - USDC-FUTURES
    /// - usdt
    /// - usd1
    ///
    /// The key identifies the native classification.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub native_types: BTreeMap<String, NativeMarketType>,
}

/// Configuration for one native exchange market type.
///
/// Different exchanges require different native parameters.
/// All supported parameters are represented explicitly.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NativeMarketType {
    /// Native WebSocket instrument type.
    ///
    /// Example: "usdt-futures".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_inst_type: Option<String>,

    /// Native REST market category.
    ///
    /// Example: "USDT-FUTURES".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rest_category: Option<String>,

    /// Settlement currency.
    ///
    /// Example: "usdt".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub settlement: Option<String>,

    /// Named WebSocket connection route.
    ///
    /// Example: "usdt".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_route: Option<String>,
}

impl MarketMapping {
    /// Retrieve a native market mapping by identifier.
    pub fn native_type(&self, name: &str) -> Option<&NativeMarketType> {
        self.native_types.get(name)
    }

    /// Check whether a native category is supported.
    ///
    /// Supports both a single native category and a list
    /// of supported native categories.
    pub fn supports_category(&self, category: &str) -> bool {
        self.native_category.as_deref() == Some(category)
            || self
                .supported_native_categories
                .iter()
                .any(|supported| supported == category)
    }

    /// Check whether a settlement is supported.
    ///
    /// Supports both a single native settlement and a list
    /// of supported settlements.
    pub fn supports_settlement(&self, settlement: &str) -> bool {
        self.native_settlement.as_deref() == Some(settlement)
            || self
                .supported_settlements
                .iter()
                .any(|supported| supported == settlement)
    }

    /// Iterate over configured native market types.
    pub fn native_types(&self) -> impl Iterator<Item = (&str, &NativeMarketType)> {
        self.native_types
            .iter()
            .map(|(name, config)| (name.as_str(), config))
    }

    /// Validate the basic structure of native market mappings.
    ///
    /// Cross-component validation, including checking whether
    /// WebSocket routes exist, belongs to ExchangeConfig.
    pub fn validate(&self) -> Result<(), String> {
        if self.resolver.trim().is_empty() {
            return Err("market_mapping.resolver cannot be empty".into());
        }

        if self
            .native_category
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err("market_mapping.native_category cannot be empty".into());
        }

        if self
            .native_settlement
            .as_ref()
            .is_some_and(|value| value.trim().is_empty())
        {
            return Err("market_mapping.native_settlement cannot be empty".into());
        }

        for category in &self.supported_native_categories {
            if category.trim().is_empty() {
                return Err(
                    "market_mapping.supported_native_categories contains an empty value".into(),
                );
            }
        }

        for settlement in &self.supported_settlements {
            if settlement.trim().is_empty() {
                return Err("market_mapping.supported_settlements contains an empty value".into());
            }
        }

        for (name, native_type) in &self.native_types {
            if name.trim().is_empty() {
                return Err("market_mapping.native_types contains an empty key".into());
            }

            if native_type
                .websocket_route
                .as_ref()
                .is_some_and(|route| route.trim().is_empty())
            {
                return Err(format!(
                    "market_mapping.native_types.{name}.websocket_route cannot be empty"
                ));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests;
