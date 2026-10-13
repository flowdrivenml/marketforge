use std::io::Write;

use marketforge_engine::instrument::{InstrumentCatalog, InstrumentIdentity};
use marketforge_engine::job::{ContractKind, InstrumentKind};

fn load_catalog(json: &str) -> marketforge_engine::error::Result<InstrumentCatalog> {
    let mut file = tempfile::NamedTempFile::new().unwrap();

    file.write_all(json.as_bytes()).unwrap();

    InstrumentCatalog::load(file.path())
}

fn valid_catalog() -> &'static str {
    r#"
    {
        "version": 1,
        "instruments": [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "instrument_kind": "spot",
                "contract_kind": null,
                "base_currency": "BTC",
                "quote_currency": "USDT"
            },
            {
                "exchange": "bybit",
                "symbol": "BTCUSDT",
                "instrument_kind": "perpetual",
                "contract_kind": "linear",
                "base_currency": "BTC",
                "quote_currency": "USDT"
            }
        ]
    }
    "#
}

#[test]
fn loads_valid_catalog() {
    let catalog = load_catalog(valid_catalog()).unwrap();

    assert_eq!(catalog.len(), 2);
    assert!(!catalog.is_empty());
}

#[test]
fn resolves_exact_instrument_identity() {
    let catalog = load_catalog(valid_catalog()).unwrap();

    let identity = InstrumentIdentity {
        exchange: "binance".to_owned(),
        symbol: "BTCUSDT".to_owned(),
        instrument_kind: InstrumentKind::Spot,
        contract_kind: None,
    };

    let metadata = catalog.resolve(&identity).unwrap();

    assert_eq!(metadata.base_currency.as_str(), "BTC");
    assert_eq!(metadata.quote_currency.as_str(), "USDT");
}

#[test]
fn distinguishes_spot_and_perpetual() {
    let catalog = load_catalog(valid_catalog()).unwrap();

    let spot = InstrumentIdentity {
        exchange: "binance".to_owned(),
        symbol: "BTCUSDT".to_owned(),
        instrument_kind: InstrumentKind::Spot,
        contract_kind: None,
    };

    let perpetual = InstrumentIdentity {
        exchange: "bybit".to_owned(),
        symbol: "BTCUSDT".to_owned(),
        instrument_kind: InstrumentKind::Perpetual,
        contract_kind: Some(ContractKind::Linear),
    };

    assert!(catalog.resolve(&spot).is_some());
    assert!(catalog.resolve(&perpetual).is_some());
}

#[test]
fn returns_none_for_unknown_instrument() {
    let catalog = load_catalog(valid_catalog()).unwrap();

    let identity = InstrumentIdentity {
        exchange: "okx".to_owned(),
        symbol: "BTC-USDT".to_owned(),
        instrument_kind: InstrumentKind::Spot,
        contract_kind: None,
    };

    assert!(catalog.resolve(&identity).is_none());
}

#[test]
fn rejects_duplicate_instrument_identity() {
    let duplicate = r#"
    {
        "version": 1,
        "instruments": [
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "instrument_kind": "spot",
                "base_currency": "BTC",
                "quote_currency": "USDT"
            },
            {
                "exchange": "binance",
                "symbol": "BTCUSDT",
                "instrument_kind": "spot",
                "base_currency": "BTC",
                "quote_currency": "USDT"
            }
        ]
    }
    "#;

    assert!(load_catalog(duplicate).is_err());
}

#[test]
fn rejects_invalid_currency_code() {
    let invalid =
        valid_catalog().replace("\"base_currency\": \"BTC\"", "\"base_currency\": \"btc\"");

    assert!(load_catalog(&invalid).is_err());
}

#[test]
fn rejects_identical_base_and_quote() {
    let invalid = valid_catalog().replace(
        "\"quote_currency\": \"USDT\"",
        "\"quote_currency\": \"BTC\"",
    );

    assert!(load_catalog(&invalid).is_err());
}

#[test]
fn rejects_unsupported_version() {
    let invalid = valid_catalog().replace("\"version\": 1", "\"version\": 999");

    assert!(load_catalog(&invalid).is_err());
}

#[test]
fn rejects_unknown_catalog_fields() {
    let invalid =
        valid_catalog().replace("\"version\": 1,", "\"version\": 1, \"unexpected\": true,");

    assert!(load_catalog(&invalid).is_err());
}

#[test]
fn rejects_empty_exchange() {
    let invalid = valid_catalog().replace("\"exchange\": \"binance\"", "\"exchange\": \"\"");

    assert!(load_catalog(&invalid).is_err());
}
