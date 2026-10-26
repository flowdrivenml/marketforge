use crate::live::config::{ExchangeId, ExchangeIdentity, MarketType};

// ============================================================
// EXCHANGE IDENTIFIERS
// ============================================================

#[test]
fn deserialize_supported_exchange_ids() {
    let cases = [
        ("binance", ExchangeId::Binance),
        ("bybit", ExchangeId::Bybit),
        ("okx", ExchangeId::Okx),
        ("bitget", ExchangeId::Bitget),
        ("gateio", ExchangeId::Gateio),
    ];

    for (input, expected) in cases {
        let toml = format!("id = \"{input}\"");

        #[derive(serde::Deserialize)]
        struct Wrapper {
            id: ExchangeId,
        }

        let parsed: Wrapper = toml::from_str(&toml).unwrap_or_else(|error| {
            panic!("Failed to deserialize exchange '{input}': {error}");
        });

        assert_eq!(parsed.id, expected);

        println!("PASS: exchange '{input}'");
    }
}

#[test]
fn reject_unknown_exchange_id() {
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct Wrapper {
        id: ExchangeId,
    }

    let input = r#"id = "unknown_exchange""#;

    let result = toml::from_str::<Wrapper>(input);

    assert!(result.is_err(), "Unknown exchange identifier was accepted");
}

#[test]
fn reject_invalid_exchange_case() {
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct Wrapper {
        id: ExchangeId,
    }

    let input = r#"id = "Binance""#;

    assert!(
        toml::from_str::<Wrapper>(input).is_err(),
        "Exchange identifiers should use canonical lowercase names"
    );
}

// ============================================================
// MARKET TYPES
// ============================================================

#[test]
fn deserialize_supported_market_types() {
    let cases = [
        ("spot", MarketType::Spot),
        ("linear", MarketType::Linear),
        ("inverse", MarketType::Inverse),
    ];

    for (input, expected) in cases {
        #[derive(serde::Deserialize)]
        struct Wrapper {
            market: MarketType,
        }

        let toml = format!("market = \"{input}\"");

        let parsed: Wrapper = toml::from_str(&toml).unwrap_or_else(|error| {
            panic!("Failed to deserialize market '{input}': {error}");
        });

        assert_eq!(parsed.market, expected);

        println!("PASS: market '{input}'");
    }
}

#[test]
fn reject_unknown_market_type() {
    #[derive(serde::Deserialize)]
    #[allow(dead_code)]
    struct Wrapper {
        market: MarketType,
    }

    let input = r#"market = "futures""#;

    assert!(
        toml::from_str::<Wrapper>(input).is_err(),
        "Unknown market category was accepted"
    );
}

// ============================================================
// EXCHANGE IDENTITY
// ============================================================

#[test]
fn deserialize_valid_exchange_identity() {
    let input = r#"
id = "binance"
market = "spot"
profile = "binance_spot"
"#;

    let identity: ExchangeIdentity =
        toml::from_str(input).expect("Failed to deserialize valid identity");

    assert_eq!(identity.id, ExchangeId::Binance);
    assert_eq!(identity.market, MarketType::Spot);
    assert_eq!(identity.profile, "binance_spot");
    assert_eq!(identity.api_generation, None);

    identity.validate().expect("Valid identity rejected");
}

#[test]
fn deserialize_identity_with_api_generation() {
    let input = r#"
id = "bitget"
market = "linear"
profile = "bitget_linear"
api_generation = "uta_v3"
"#;

    let identity: ExchangeIdentity = toml::from_str(input).expect("Failed to deserialize identity");

    assert_eq!(identity.id, ExchangeId::Bitget);
    assert_eq!(identity.market, MarketType::Linear);
    assert_eq!(identity.api_generation.as_deref(), Some("uta_v3"));

    identity.validate().expect("Valid identity rejected");
}

#[test]
fn reject_mismatched_profile_name() {
    let input = r#"
id = "binance"
market = "spot"
profile = "binance_linear"
"#;

    let identity: ExchangeIdentity = toml::from_str(input).expect("Failed to deserialize identity");

    assert!(
        identity.validate().is_err(),
        "Mismatched exchange profile was accepted"
    );
}

#[test]
fn reject_empty_profile_name() {
    let input = r#"
id = "binance"
market = "spot"
profile = ""
"#;

    let identity: ExchangeIdentity = toml::from_str(input).expect("Failed to deserialize identity");

    assert!(
        identity.validate().is_err(),
        "Empty exchange profile was accepted"
    );
}

#[test]
fn reject_unknown_identity_fields() {
    let input = r#"
id = "binance"
market = "spot"
profile = "binance_spot"
unknown_field = "unexpected"
"#;

    assert!(
        toml::from_str::<ExchangeIdentity>(input).is_err(),
        "Unknown identity field was accepted"
    );
}

#[test]
fn reject_missing_required_identity_fields() {
    let cases = [
        r#"
market = "spot"
profile = "binance_spot"
"#,
        r#"
id = "binance"
profile = "binance_spot"
"#,
        r#"
id = "binance"
market = "spot"
"#,
    ];

    for input in cases {
        assert!(
            toml::from_str::<ExchangeIdentity>(input).is_err(),
            "Identity with missing required fields was accepted"
        );
    }
}

// ============================================================
// SERIALIZATION
// ============================================================

#[test]
fn exchange_id_string_representation() {
    let cases = [
        (ExchangeId::Binance, "binance"),
        (ExchangeId::Bybit, "bybit"),
        (ExchangeId::Okx, "okx"),
        (ExchangeId::Bitget, "bitget"),
        (ExchangeId::Gateio, "gateio"),
    ];

    for (exchange, expected) in cases {
        assert_eq!(exchange.as_str(), expected);
    }
}

#[test]
fn market_type_string_representation() {
    let cases = [
        (MarketType::Spot, "spot"),
        (MarketType::Linear, "linear"),
        (MarketType::Inverse, "inverse"),
    ];

    for (market, expected) in cases {
        assert_eq!(market.as_str(), expected);
    }
}

#[test]
fn exchange_identity_round_trip() {
    let input = r#"
id = "gateio"
market = "inverse"
profile = "gateio_inverse"
api_generation = "v4"
"#;

    let original: ExchangeIdentity = toml::from_str(input).expect("Failed to deserialize identity");

    let serialized = toml::to_string(&original).expect("Failed to serialize identity");

    let reconstructed: ExchangeIdentity =
        toml::from_str(&serialized).expect("Failed to deserialize round trip");

    assert_eq!(original, reconstructed);

    println!("PASS: exchange identity serialization round trip");
}

#[test]
fn optional_api_generation_is_not_serialized_when_absent() {
    let identity = ExchangeIdentity {
        id: ExchangeId::Binance,
        market: MarketType::Spot,
        profile: "binance_spot".to_string(),
        api_generation: None,
    };

    let serialized = toml::to_string(&identity).expect("Failed to serialize identity");

    assert!(
        !serialized.contains("api_generation"),
        "Absent optional API generation was serialized"
    );
}

// ============================================================
// ALL EXCHANGE / MARKET COMBINATIONS
// ============================================================

#[test]
fn validate_all_exchange_market_combinations() {
    let exchanges = [
        ExchangeId::Binance,
        ExchangeId::Bybit,
        ExchangeId::Okx,
        ExchangeId::Bitget,
        ExchangeId::Gateio,
    ];

    let markets = [MarketType::Spot, MarketType::Linear, MarketType::Inverse];

    let mut validated = 0;

    for exchange in exchanges {
        for market in markets {
            let profile = format!("{}_{}", exchange.as_str(), market.as_str());

            let identity = ExchangeIdentity {
                id: exchange,
                market,
                profile: profile.clone(),
                api_generation: None,
            };

            identity.validate().unwrap_or_else(|error| {
                panic!("Failed to validate '{profile}': {error}");
            });

            assert_eq!(identity.expected_profile(), profile);

            println!("PASS: {profile}");

            validated += 1;
        }
    }

    assert_eq!(validated, 15);

    println!("\nAll {validated} exchange/market combinations validated.");
}
