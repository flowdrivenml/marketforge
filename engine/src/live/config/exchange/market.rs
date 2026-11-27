use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Exchange-specific mapping between MarketForge's canonical
/// market categories and the exchange's native market types.
///
/// The Source resolver selects a configured mapping using catalogue metadata.
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

/// Native protocol values selected from the loaded profile. Their case is preserved.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ResolvedNativeMarket<'a> {
    pub classification: Option<&'a str>,
    pub websocket_route: Option<&'a str>,
    pub websocket_inst_type: Option<&'a str>,
    pub rest_category: Option<&'a str>,
    pub settlement: Option<&'a str>,
}

impl super::ExchangeConfig {
    pub(crate) fn resolve_native_market(
        &self,
        source: &crate::live::management::protocol::definitions::CreateSourceStreamRequest,
    ) -> Result<ResolvedNativeMarket<'_>, String> {
        use super::{ExchangeId, MarketType};
        let fail = |reason: String| {
            format!(
                "{} instrument {} ({}): {reason}",
                self.profile(),
                source.instrument_id,
                source.symbol
            )
        };
        if source.exchange != self.exchange.id || source.market_category != self.market() {
            return Err(fail(
                "instrument exchange or market differs from profile".into(),
            ));
        }

        let subscription_type = self
            .websocket
            .subscription
            .as_ref()
            .and_then(|subscription| subscription.instrument_type.as_deref());
        let requires_type = subscription_type == Some("{native_ws_inst_type}");
        let requires_route = !self.websocket.routes.is_empty()
            && self
                .rest
                .endpoints
                .values()
                .any(|endpoint| endpoint.settlement.as_deref() == Some("{native_settlement}"));
        let subscription_type = subscription_type.filter(|value| !value.contains('{'));
        let Some(mapping) = &self.market_mapping else {
            if requires_type || requires_route {
                return Err(fail("profile requires a native market mapping".into()));
            }
            return Ok(ResolvedNativeMarket {
                websocket_inst_type: subscription_type,
                ..Default::default()
            });
        };
        let settlement_asset = source.settlement_asset.as_deref();
        if let (Some(expected), Some(actual)) =
            (mapping.native_settlement.as_deref(), settlement_asset)
        {
            if !expected.eq_ignore_ascii_case(actual) {
                return Err(fail(format!(
                    "settlement_asset {actual} conflicts with configured settlement {expected}"
                )));
            }
        }

        let mut resolved = ResolvedNativeMarket {
            classification: mapping.native_category.as_deref(),
            websocket_inst_type: mapping.websocket_inst_type.as_deref().or(subscription_type),
            rest_category: mapping.rest_category.as_deref(),
            settlement: mapping.native_settlement.as_deref(),
            ..Default::default()
        };
        if mapping.native_types.is_empty() {
            if requires_route || (requires_type && resolved.websocket_inst_type.is_none()) {
                return Err(fail(
                    "profile is missing required native market variants".into(),
                ));
            }
            return Ok(resolved);
        }
        let asset = settlement_asset.ok_or_else(|| {
            fail("settlement_asset is required for native market selection".into())
        })?;
        let mut matches = mapping.native_types().filter(|(_, native)| {
            native
                .settlement
                .as_deref()
                .is_some_and(|candidate| candidate.eq_ignore_ascii_case(asset))
        });
        let (name, native) = matches
            .next()
            .ok_or_else(|| fail(format!("unsupported settlement_asset {asset}")))?;
        if matches.next().is_some() {
            return Err(fail(format!(
                "ambiguous native mappings for settlement_asset {asset}"
            )));
        }

        // These stablecoin linear products are quoted and settled in the selected asset.
        if matches!(
            (self.exchange.id, self.market()),
            (ExchangeId::Bitget | ExchangeId::Gateio, MarketType::Linear)
        ) {
            if let Some(quote) = source.quote_asset.as_deref() {
                if !quote.eq_ignore_ascii_case(asset) {
                    return Err(fail(format!(
                        "quote_asset {quote} conflicts with settlement_asset {asset}"
                    )));
                }
            }
        }
        if let Some(route) = native.websocket_route.as_deref() {
            if self.route(route).is_none() {
                return Err(fail(format!(
                    "native mapping {name} has unknown WebSocket route {route}"
                )));
            }
        } else if requires_route
            || mapping
                .native_types
                .values()
                .any(|native| native.websocket_route.is_some())
        {
            return Err(fail(format!(
                "native mapping {name} is missing a WebSocket route"
            )));
        }
        resolved.classification = Some(name);
        resolved.websocket_route = native.websocket_route.as_deref();
        resolved.websocket_inst_type = native
            .websocket_inst_type
            .as_deref()
            .or(resolved.websocket_inst_type);
        if requires_type && resolved.websocket_inst_type.is_none() {
            return Err(fail(format!(
                "native mapping {name} is missing a WebSocket instrument type"
            )));
        }
        resolved.rest_category = native.rest_category.as_deref().or(resolved.rest_category);
        resolved.settlement = native.settlement.as_deref();
        Ok(resolved)
    }
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

    fn profile_and_source(
        name: &str,
    ) -> (
        super::super::ExchangeConfig,
        crate::live::stream::request::CreateSourceStreamRequest,
    ) {
        use std::path::Path;
        let root = Path::new(env!("CARGO_MANIFEST_DIR"));
        let profile = super::super::ExchangeConfig::load(
            root.join(format!("../data/.config/live/exchanges/{name}.toml")),
        )
        .unwrap();
        let fixture: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/live/support/data/instruments/btc_sources.json"
        )))
        .unwrap();
        // The catalogue fixture has no Bitget inverse entry. This pure mapping
        // test substitutes only market metadata and makes no financial assertion.
        let fixture_name = if name == "bitget_inverse" {
            "gateio_inverse"
        } else {
            name
        };
        let mut source = fixture["instruments"]
            .as_array()
            .unwrap()
            .iter()
            .find(|item| {
                format!(
                    "{}_{}",
                    item["exchange"].as_str().unwrap(),
                    item["market_category"].as_str().unwrap()
                ) == fixture_name
            })
            .unwrap()
            .clone();
        for key in [
            "exchange_id",
            "tick_size",
            "qty_step",
            "min_qty",
            "max_qty",
            "min_notional",
        ] {
            source.as_object_mut().unwrap().remove(key);
        }
        source["channel"] = "trades".into();
        if name == "bitget_inverse" {
            source["exchange"] = "bitget".into();
        }
        (profile, serde_json::from_value(source).unwrap())
    }

    #[test]
    fn resolves_settlement_variants_and_native_rest_templates() {
        use crate::live::infrastructure::rest::{RestRequest, RestRequestResolver};
        // The alternate variants reuse a fixture request only as synthetic metadata.
        for (profile, asset, symbol, route, inst_type, category) in [
            (
                "gateio_linear",
                "uSdT",
                "BTC_USDT",
                Some("usdt"),
                None,
                None,
            ),
            (
                "gateio_linear",
                "UsD1",
                "BTC_USD1",
                Some("usd1"),
                None,
                None,
            ),
            (
                "bitget_linear",
                "uSdT",
                "BTCUSDT",
                None,
                Some("usdt-futures"),
                Some("USDT-FUTURES"),
            ),
            (
                "bitget_linear",
                "UsDc",
                "BTCUSDC",
                None,
                Some("usdc-futures"),
                Some("USDC-FUTURES"),
            ),
        ] {
            let (exchange, mut source) = profile_and_source(profile);
            source.symbol = symbol.into();
            source.settlement_asset = Some(asset.into());
            source.quote_asset = Some(asset.to_ascii_uppercase());
            let native = exchange.resolve_native_market(&source).unwrap();
            assert_eq!(native.websocket_route, route);
            assert_eq!(native.websocket_inst_type, inst_type);
            assert_eq!(native.rest_category, category);
            if let Some(route) = route {
                assert_eq!(native.classification, Some(route));
                assert_eq!(native.settlement, Some(route));
                for (name, action) in [
                    ("depth_snapshot", "order_book"),
                    ("historical_trades", "trades"),
                ] {
                    let request = RestRequest::new(profile, name)
                        .with_parameter("symbol", symbol)
                        .with_parameter("native_settlement", native.settlement.unwrap());
                    let resolved = RestRequestResolver::resolve_endpoint(
                        &exchange,
                        exchange.endpoint(name).unwrap(),
                        &request,
                    )
                    .unwrap();
                    assert!(
                        resolved
                            .url()
                            .ends_with(&format!("/futures/{route}/{action}"))
                    );
                    assert_eq!(resolved.parameter("contract"), Some(symbol));
                }
            } else {
                assert_eq!(native.classification, category);
                assert_eq!(native.settlement, Some(asset.to_ascii_uppercase().as_str()));
                for name in ["depth_snapshot", "recent_trades"] {
                    let request = RestRequest::new(profile, name)
                        .with_parameter("symbol", symbol)
                        .with_parameter("native_rest_category", native.rest_category.unwrap());
                    let resolved = RestRequestResolver::resolve_endpoint(
                        &exchange,
                        exchange.endpoint(name).unwrap(),
                        &request,
                    )
                    .unwrap();
                    assert_eq!(resolved.parameter("category"), category);
                    assert_eq!(resolved.parameter("symbol"), Some(symbol));
                }
            }
        }
    }

    #[test]
    fn static_profiles_preserve_mapping_contracts() {
        let (gate, mut source) = profile_and_source("gateio_inverse");
        let native = gate.resolve_native_market(&source).unwrap();
        assert_eq!(native.settlement, Some("btc"));
        assert!(native.websocket_route.is_none());
        assert!(gate.websocket.url.as_deref().unwrap().ends_with("/btc"));
        source.settlement_asset = Some("USD".into());
        assert!(
            gate.resolve_native_market(&source)
                .unwrap_err()
                .contains("conflicts with configured settlement btc")
        );
        source.settlement_asset = None;
        assert_eq!(
            gate.resolve_native_market(&source).unwrap().settlement,
            Some("btc")
        );

        let (gate, mut source) = profile_and_source("gateio_spot");
        source.settlement_asset = None;
        assert_eq!(
            gate.resolve_native_market(&source).unwrap(),
            Default::default()
        );
        let (bitget, mut source) = profile_and_source("bitget_spot");
        source.settlement_asset = None;
        assert_eq!(
            bitget
                .resolve_native_market(&source)
                .unwrap()
                .websocket_inst_type,
            Some("spot")
        );
        let (bitget, mut source) = profile_and_source("bitget_inverse");
        source.settlement_asset = Some("ETH".into());
        let native = bitget.resolve_native_market(&source).unwrap();
        assert_eq!(native.websocket_inst_type, Some("coin-futures"));
        assert_eq!(native.rest_category, Some("COIN-FUTURES"));
    }

    #[test]
    fn rejects_missing_ambiguous_and_contradictory_native_metadata() {
        let (mut exchange, mut source) = profile_and_source("gateio_linear");
        let original = source.clone();
        source.settlement_asset = None;
        assert!(
            exchange
                .resolve_native_market(&source)
                .unwrap_err()
                .contains("settlement_asset is required")
        );
        source.settlement_asset = Some("EUR".into());
        assert!(
            exchange
                .resolve_native_market(&source)
                .unwrap_err()
                .contains("unsupported settlement_asset EUR")
        );
        source = original;
        source.quote_asset = Some("USD".into());
        assert!(
            exchange
                .resolve_native_market(&source)
                .unwrap_err()
                .contains("quote_asset USD conflicts")
        );
        source.quote_asset = Some("USDT".into());
        let types = &mut exchange.market_mapping.as_mut().unwrap().native_types;
        let duplicate = types["usdt"].clone();
        types.insert("duplicate".into(), duplicate);
        assert!(
            exchange
                .resolve_native_market(&source)
                .unwrap_err()
                .contains("ambiguous native mappings")
        );
        exchange
            .market_mapping
            .as_mut()
            .unwrap()
            .native_types
            .remove("duplicate");
        exchange.websocket.routes.remove("usdt");
        assert!(
            exchange
                .resolve_native_market(&source)
                .unwrap_err()
                .contains("unknown WebSocket route usdt")
        );
        exchange
            .market_mapping
            .as_mut()
            .unwrap()
            .native_types
            .get_mut("usdt")
            .unwrap()
            .websocket_route = None;
        assert!(
            exchange
                .resolve_native_market(&source)
                .unwrap_err()
                .contains("missing a WebSocket route")
        );
        exchange.market_mapping = None;
        assert!(
            exchange
                .resolve_native_market(&source)
                .unwrap_err()
                .contains("requires a native market mapping")
        );
    }

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
