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
mod tests {
    use crate::live::config::exchange::MarketMapping;

    // ============================================================
    // HELPERS
    // ============================================================

    fn parse_mapping(input: &str) -> MarketMapping {
        toml::from_str(input)
            .unwrap_or_else(|error| panic!("Failed to parse market mapping: {error}"))
    }

    // ============================================================
    // BASIC MARKET MAPPING
    // ============================================================

    #[test]
    fn deserialize_single_native_category() {
        let input = r#"
resolver = "marketforge_instrument_catalog"
native_category = "COIN-FUTURES"
websocket_inst_type = "coin-futures"
rest_category = "COIN-FUTURES"
"#;

        let mapping = parse_mapping(input);

        assert_eq!(mapping.resolver, "marketforge_instrument_catalog");

        assert_eq!(mapping.native_category.as_deref(), Some("COIN-FUTURES"));

        assert_eq!(mapping.websocket_inst_type.as_deref(), Some("coin-futures"));

        assert_eq!(mapping.rest_category.as_deref(), Some("COIN-FUTURES"));

        mapping.validate().expect("Valid mapping rejected");
    }

    #[test]
    fn deserialize_multiple_native_categories() {
        let input = r#"
resolver = "marketforge_instrument_catalog"

supported_native_categories = [
    "USDT-FUTURES",
    "USDC-FUTURES",
]

[native_types.USDT-FUTURES]
websocket_inst_type = "usdt-futures"
rest_category = "USDT-FUTURES"

[native_types.USDC-FUTURES]
websocket_inst_type = "usdc-futures"
rest_category = "USDC-FUTURES"
"#;

        let mapping = parse_mapping(input);

        assert_eq!(mapping.supported_native_categories.len(), 2);
        assert_eq!(mapping.native_types.len(), 2);

        assert!(mapping.supports_category("USDT-FUTURES"));
        assert!(mapping.supports_category("USDC-FUTURES"));
        assert!(!mapping.supports_category("COIN-FUTURES"));

        let usdt = mapping
            .native_type("USDT-FUTURES")
            .expect("Missing USDT-FUTURES mapping");

        assert_eq!(usdt.websocket_inst_type.as_deref(), Some("usdt-futures"));

        assert_eq!(usdt.rest_category.as_deref(), Some("USDT-FUTURES"));

        mapping.validate().expect("Valid mapping rejected");
    }

    // ============================================================
    // SETTLEMENT MAPPINGS
    // ============================================================

    #[test]
    fn deserialize_single_native_settlement() {
        let input = r#"
resolver = "marketforge_instrument_catalog"
native_settlement = "btc"
require_inverse_contract_classification = true
"#;

        let mapping = parse_mapping(input);

        assert_eq!(mapping.native_settlement.as_deref(), Some("btc"));

        assert_eq!(mapping.require_inverse_contract_classification, Some(true));

        assert!(mapping.supports_settlement("btc"));
        assert!(!mapping.supports_settlement("usdt"));

        mapping.validate().expect("Valid mapping rejected");
    }

    #[test]
    fn deserialize_multiple_settlements() {
        let input = r#"
resolver = "marketforge_instrument_catalog"

supported_settlements = [
    "usdt",
    "usd1",
]

[native_types.usdt]
settlement = "usdt"
websocket_route = "usdt"

[native_types.usd1]
settlement = "usd1"
websocket_route = "usd1"
"#;

        let mapping = parse_mapping(input);

        assert_eq!(mapping.supported_settlements.len(), 2);
        assert_eq!(mapping.native_types.len(), 2);

        assert!(mapping.supports_settlement("usdt"));
        assert!(mapping.supports_settlement("usd1"));
        assert!(!mapping.supports_settlement("btc"));

        assert_eq!(
            mapping
                .native_type("usdt")
                .unwrap()
                .websocket_route
                .as_deref(),
            Some("usdt")
        );

        assert_eq!(
            mapping
                .native_type("usd1")
                .unwrap()
                .websocket_route
                .as_deref(),
            Some("usd1")
        );

        mapping.validate().expect("Valid mapping rejected");
    }

    // ============================================================
    // NATIVE MARKET TYPE ACCESS
    // ============================================================

    #[test]
    fn retrieve_native_market_type() {
        let mapping = parse_mapping(
            r#"
resolver = "marketforge_instrument_catalog"

[native_types.USDT-FUTURES]
websocket_inst_type = "usdt-futures"
rest_category = "USDT-FUTURES"
"#,
        );

        let native = mapping.native_type("USDT-FUTURES");

        assert!(native.is_some());
        assert!(mapping.native_type("UNKNOWN").is_none());
    }

    #[test]
    fn iterate_native_market_types() {
        let mapping = parse_mapping(
            r#"
resolver = "marketforge_instrument_catalog"

[native_types.usdt]
settlement = "usdt"
websocket_route = "usdt"

[native_types.usd1]
settlement = "usd1"
websocket_route = "usd1"
"#,
        );

        let names: Vec<&str> = mapping.native_types().map(|(name, _)| name).collect();

        assert_eq!(names, vec!["usd1", "usdt"]);
    }

    // ============================================================
    // CLASSIFICATION REQUIREMENTS
    // ============================================================

    #[test]
    fn deserialize_inverse_classification_requirement() {
        let mapping = parse_mapping(
            r#"
resolver = "marketforge_instrument_catalog"
require_inverse_contract_classification = true
"#,
        );

        assert_eq!(mapping.require_inverse_contract_classification, Some(true));
    }

    #[test]
    fn deserialize_linear_classification_requirement() {
        let mapping = parse_mapping(
            r#"
resolver = "marketforge_instrument_catalog"
require_linear_contract_classification = true
"#,
        );

        assert_eq!(mapping.require_linear_contract_classification, Some(true));
    }

    // ============================================================
    // VALIDATION
    // ============================================================

    #[test]
    fn reject_empty_resolver() {
        let mapping = parse_mapping(
            r#"
resolver = ""
"#,
        );

        assert!(mapping.validate().is_err(), "Empty resolver was accepted");
    }

    #[test]
    fn reject_empty_native_category() {
        let mapping = parse_mapping(
            r#"
resolver = "marketforge_instrument_catalog"
native_category = ""
"#,
        );

        assert!(
            mapping.validate().is_err(),
            "Empty native category was accepted"
        );
    }

    #[test]
    fn reject_empty_native_settlement() {
        let mapping = parse_mapping(
            r#"
resolver = "marketforge_instrument_catalog"
native_settlement = ""
"#,
        );

        assert!(
            mapping.validate().is_err(),
            "Empty native settlement was accepted"
        );
    }

    #[test]
    fn reject_empty_supported_category() {
        let mapping = parse_mapping(
            r#"
resolver = "marketforge_instrument_catalog"
supported_native_categories = ["USDT-FUTURES", ""]
"#,
        );

        assert!(
            mapping.validate().is_err(),
            "Empty supported category was accepted"
        );
    }

    #[test]
    fn reject_empty_supported_settlement() {
        let mapping = parse_mapping(
            r#"
resolver = "marketforge_instrument_catalog"
supported_settlements = ["usdt", ""]
"#,
        );

        assert!(
            mapping.validate().is_err(),
            "Empty supported settlement was accepted"
        );
    }

    #[test]
    fn reject_empty_websocket_route() {
        let mapping = parse_mapping(
            r#"
resolver = "marketforge_instrument_catalog"

[native_types.usdt]
settlement = "usdt"
websocket_route = ""
"#,
        );

        assert!(
            mapping.validate().is_err(),
            "Empty WebSocket route was accepted"
        );
    }

    // ============================================================
    // STRICT FIELD RECOGNITION
    // ============================================================

    #[test]
    fn reject_unknown_market_mapping_field() {
        let input = r#"
resolver = "marketforge_instrument_catalog"
unknown_setting = true
"#;

        assert!(
            toml::from_str::<MarketMapping>(input).is_err(),
            "Unknown market mapping field was accepted"
        );
    }

    #[test]
    fn reject_unknown_native_market_type_field() {
        let input = r#"
resolver = "marketforge_instrument_catalog"

[native_types.usdt]
settlement = "usdt"
unknown_setting = 123
"#;

        assert!(
            toml::from_str::<MarketMapping>(input).is_err(),
            "Unknown native market type field was accepted"
        );
    }

    #[test]
    fn reject_missing_resolver() {
        let input = r#"
native_category = "COIN-FUTURES"
"#;

        assert!(
            toml::from_str::<MarketMapping>(input).is_err(),
            "Market mapping without resolver was accepted"
        );
    }

    #[test]
    fn reject_invalid_classification_type() {
        let input = r#"
resolver = "marketforge_instrument_catalog"
require_inverse_contract_classification = "yes"
"#;

        assert!(
            toml::from_str::<MarketMapping>(input).is_err(),
            "Invalid classification field type was accepted"
        );
    }

    // ============================================================
    // SERIALIZATION
    // ============================================================

    #[test]
    fn market_mapping_round_trip() {
        let input = r#"
resolver = "marketforge_instrument_catalog"

supported_native_categories = [
    "USDT-FUTURES",
    "USDC-FUTURES",
]

[native_types.USDT-FUTURES]
websocket_inst_type = "usdt-futures"
rest_category = "USDT-FUTURES"

[native_types.USDC-FUTURES]
websocket_inst_type = "usdc-futures"
rest_category = "USDC-FUTURES"
"#;

        let original: toml::Value = toml::from_str(input).expect("Invalid original TOML");

        let mapping = parse_mapping(input);

        let serialized = toml::to_string(&mapping).expect("Failed to serialize mapping");

        let reconstructed: toml::Value =
            toml::from_str(&serialized).expect("Invalid reconstructed TOML");

        assert_eq!(
            original, reconstructed,
            "Market mapping changed during round trip"
        );
    }

    // ============================================================
    // REAL EXCHANGE CONFIGURATIONS
    // ============================================================

    // ============================================================
    // FULL CONFIGURATION INTEGRATION
    // ============================================================
}
