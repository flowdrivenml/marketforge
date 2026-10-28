#![cfg(feature = "live")]

use rust_decimal::Decimal;
use serde_json::json;

use marketforge_engine::{
    live::{
        config::{
            exchange::{ExchangeId, MarketType},
            format::FormatConfigStore,
        },
        infrastructure::normalization::{
            NormalizationContext, NormalizationSource, QuantityPrecisionPolicy, TradeDecoder,
            TradeNormalizationCompiler,
        },
        stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
    },
    shared::domain::canonical::TradeSide,
};

// ============================================================
// TEST HELPERS
// ============================================================

fn d(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap()
}

fn format_store() -> FormatConfigStore {
    let root = crate::support::data(".config/live/formats");

    FormatConfigStore::load_dir(&root).unwrap()
}

fn decoder(name: &str, formats: &FormatConfigStore) -> TradeDecoder {
    let (_, config) = formats
        .trades()
        .find(|(id, _)| *id == name)
        .unwrap_or_else(|| panic!("missing format {name}"));

    let plan = TradeNormalizationCompiler::compile_websocket(
        config,
        10_000,
        QuantityPrecisionPolicy::Round { decimal_places: 18 },
    )
    .unwrap();

    TradeDecoder::new(plan).unwrap()
}

fn context(
    market: MarketType,
    symbol: &str,
    quantity_type: QuantityType,
    contract_value: Option<&str>,
    contract_value_asset: Option<&str>,
) -> NormalizationContext {
    let request = CreateSourceStreamRequest {
        instrument_id: 42,

        exchange: ExchangeId::Binance,

        symbol: symbol.into(),

        market_category: market,

        instrument_type: "perpetual".into(),

        base_asset: Some("BTC".into()),

        quote_asset: Some(
            if symbol.starts_with("BTCUSD_") {
                "USD"
            } else {
                "USDT"
            }
            .into(),
        ),

        quantity_type,

        contract_value: contract_value.map(str::to_owned),

        contract_value_asset: contract_value_asset.map(str::to_owned),

        channel: SourceChannel::Trades,

        settlement_asset: Some("BTC".into()),
    };

    NormalizationContext::new(&request, format!("test-{symbol}")).unwrap()
}

// ============================================================
// BINANCE LINEAR — BASE QUANTITY
// ============================================================

#[test]
fn decodes_binance_linear_individual_trade() {
    let formats = format_store();

    let decoder = decoder("binance_linear", &formats);

    let context = context(
        MarketType::Linear,
        "BTCUSDT",
        QuantityType::Base,
        None,
        None,
    );

    let native = json!({
        "e": "trade",
        "E": 1792854988545_i64,
        "T": 1792854988544_i64,
        "s": "BTCUSDT",
        "t": 8201275979_u64,
        "p": "79354.70",
        "q": "0.005",
        "X": "MARKET",
        "m": true,
        "st": 1
    });

    let trade = decoder.decode_record(&native, &context).unwrap();

    assert_eq!(trade.trade_id.as_deref(), Some("8201275979"));

    assert_eq!(trade.side, TradeSide::Sell);

    assert_eq!(trade.price, d("79354.70"));

    assert_eq!(trade.quantity_base, Some(d("0.005")));

    assert_eq!(trade.quantity_quote, Some(d("396.77350")));

    assert_eq!(trade.quantity_contracts, None);

    assert_eq!(trade.envelope.event_timestamp_ns, 1_792_854_988_544_000_000);

    println!("PASS Binance Linear individual trade");
}

// ============================================================
// BINANCE INVERSE — CONTRACT QUANTITY
// ============================================================

#[test]
fn decodes_binance_inverse_individual_trade() {
    let formats = format_store();

    let decoder = decoder("binance_inverse", &formats);

    let context = context(
        MarketType::Inverse,
        "BTCUSD_PERP",
        QuantityType::Contracts,
        Some("100"),
        Some("USD"),
    );

    let native = json!({
        "e": "trade",
        "E": 1792854997291_i64,
        "T": 1792854997291_i64,
        "s": "BTCUSD_PERP",
        "t": 1160649918_u64,
        "p": "79324.0",
        "q": "4",
        "X": "MARKET",
        "m": true,
        "st": 2
    });

    let trade = decoder.decode_record(&native, &context).unwrap();

    assert_eq!(trade.trade_id.as_deref(), Some("1160649918"));

    assert_eq!(trade.side, TradeSide::Sell);

    assert_eq!(trade.price, d("79324.0"));

    assert_eq!(trade.quantity_contracts, Some(d("4")));

    assert_eq!(trade.quantity_quote, Some(d("400")));

    // This is rounded to 18 decimal places.
    assert_eq!(trade.quantity_base, Some(d("0.005042610054964450")));

    println!("PASS Binance Inverse contract conversion");
}

// ============================================================
// QUANTITY SEMANTICS MISMATCH
// ============================================================

#[test]
fn rejects_inverse_trade_with_base_quantity_context() {
    let formats = format_store();

    let decoder = decoder("binance_inverse", &formats);

    let context = context(
        MarketType::Inverse,
        "BTCUSD_PERP",
        QuantityType::Base,
        None,
        None,
    );

    let native = json!({
        "e": "trade",
        "E": 1792854997291_i64,
        "T": 1792854997291_i64,
        "s": "BTCUSD_PERP",
        "t": 1160649918_u64,
        "p": "79324.0",
        "q": "4",
        "m": true
    });

    assert!(decoder.decode_record(&native, &context).is_err());

    println!("PASS inverse quantity mismatch rejected");
}

// ============================================================
// NATIVE INSTRUMENT IDENTITY VALIDATION
// ============================================================

#[test]
fn rejects_binance_linear_instrument_mismatch() {
    let formats = format_store();

    let decoder = decoder("binance_linear", &formats);

    let context = context(
        MarketType::Linear,
        "BTCUSDT",
        QuantityType::Base,
        None,
        None,
    );

    let native = json!({
        "e": "trade",
        "E": 1792854988545_i64,
        "T": 1792854988544_i64,
        "s": "ETHUSDT",
        "t": 8201275979_u64,
        "p": "4000",
        "q": "1",
        "m": false
    });

    let result = decoder.decode_record(&native, &context);

    assert!(result.is_err(), "ETHUSDT trade must not decode as BTCUSDT");
}

#[test]
fn rejects_binance_inverse_instrument_mismatch() {
    let formats = format_store();

    let decoder = decoder("binance_inverse", &formats);

    let context = context(
        MarketType::Inverse,
        "BTCUSD_PERP",
        QuantityType::Contracts,
        Some("100"),
        Some("USD"),
    );

    let native = json!({
        "e": "trade",
        "E": 1792854997291_i64,
        "T": 1792854997291_i64,
        "s": "ETHUSD_PERP",
        "t": 1160649918_u64,
        "p": "4000",
        "q": "4",
        "m": true
    });

    let result = decoder.decode_record(&native, &context);

    assert!(
        result.is_err(),
        "ETHUSD_PERP trade must not use BTCUSD_PERP \
         contract metadata"
    );
}

#[test]
fn rejects_missing_native_instrument() {
    let formats = format_store();

    let decoder = decoder("binance_linear", &formats);

    let context = context(
        MarketType::Linear,
        "BTCUSDT",
        QuantityType::Base,
        None,
        None,
    );

    let native = json!({
        "e": "trade",
        "T": 1792854988544_i64,
        "t": 8201275979_u64,
        "p": "79354.70",
        "q": "0.005",
        "m": true
    });

    assert!(decoder.decode_record(&native, &context).is_err());
}

#[test]
fn rejects_non_string_native_instrument() {
    let formats = format_store();

    let decoder = decoder("binance_linear", &formats);

    let context = context(
        MarketType::Linear,
        "BTCUSDT",
        QuantityType::Base,
        None,
        None,
    );

    let native = json!({
        "e": "trade",
        "s": 12345,
        "T": 1792854988544_i64,
        "t": 8201275979_u64,
        "p": "79354.70",
        "q": "0.005",
        "m": true
    });

    assert!(decoder.decode_record(&native, &context).is_err());
}

// ============================================================
// N5J.2 — ENVELOPE VALIDATION
// ============================================================

#[test]
fn rejects_binance_aggregate_message() {
    let formats = format_store();

    let decoder = decoder("binance_linear", &formats);

    let context = context(
        MarketType::Linear,
        "BTCUSDT",
        QuantityType::Base,
        None,
        None,
    );

    let native = json!({
        "e": "aggTrade",
        "s": "BTCUSDT",
        "t": 8201275979_u64,
        "T": 1792854988544_i64,
        "p": "79354.70",
        "q": "0.005",
        "m": true
    });

    let result = decoder.decode_message(&native, &context, NormalizationSource::WebSocket);

    assert!(
        result.is_err(),
        "aggregate message must not decode as an individual trade"
    );
}

#[test]
fn rejects_binance_wrong_market_context() {
    let formats = format_store();

    let decoder = decoder("binance_linear", &formats);

    // Correct native symbol and valid quantity metadata,
    // but the stream belongs to the inverse market.
    let context = context(
        MarketType::Inverse,
        "BTCUSDT",
        QuantityType::Base,
        None,
        None,
    );

    let native = json!({
        "e": "trade",
        "s": "BTCUSDT",
        "t": 8201275979_u64,
        "T": 1792854988544_i64,
        "p": "79354.70",
        "q": "0.005",
        "m": true
    });

    assert!(
        decoder
            .decode_message(&native, &context, NormalizationSource::WebSocket,)
            .is_err()
    );
}

#[test]
fn accepts_valid_binance_message_envelope() {
    let formats = format_store();

    let decoder = decoder("binance_linear", &formats);

    let context = context(
        MarketType::Linear,
        "BTCUSDT",
        QuantityType::Base,
        None,
        None,
    );

    let native = json!({
        "e": "trade",
        "s": "BTCUSDT",
        "t": 8201275979_u64,
        "T": 1792854988544_i64,
        "p": "79354.70",
        "q": "0.005",
        "m": true
    });

    let batch = decoder
        .decode_message(&native, &context, NormalizationSource::WebSocket)
        .unwrap();

    assert_eq!(batch.trades().len(), 1);
}

#[test]
fn compiles_bitget_envelope_rules() {
    let formats = format_store();

    let (_, config) = formats
        .trades()
        .find(|(name, _)| *name == "bitget_linear")
        .unwrap();

    let plan =
        TradeNormalizationCompiler::compile_websocket(config, 1024, QuantityPrecisionPolicy::Exact)
            .unwrap();

    let valid = json!({
        "arg": {
            "topic": "publicTrade",
            "symbol": "BTCUSDT",
            "instType": "usdt-futures"
        },
        "data": []
    });

    assert!(plan.envelope.validate(&valid, "BTCUSDT").is_ok());

    let wrong_market = json!({
        "arg": {
            "topic": "publicTrade",
            "symbol": "BTCUSDT",
            "instType": "coin-futures"
        },
        "data": []
    });

    assert!(plan.envelope.validate(&wrong_market, "BTCUSDT").is_err());

    let wrong_symbol = json!({
        "arg": {
            "topic": "publicTrade",
            "symbol": "ETHUSDT",
            "instType": "usdt-futures"
        },
        "data": []
    });

    assert!(plan.envelope.validate(&wrong_symbol, "BTCUSDT").is_err());
}

#[test]
fn validates_binance_linear_symbol_type_from_real_format() {
    let formats = format_store();

    let decoder = decoder("binance_linear", &formats);

    let context = context(
        MarketType::Linear,
        "BTCUSDT",
        QuantityType::Base,
        None,
        None,
    );

    let native = json!({
        "e": "trade",
        "s": "BTCUSDT",
        "t": 8201275979_u64,
        "T": 1792854988544_i64,
        "p": "79354.70",
        "q": "0.005",
        "m": true,
        "st": 1
    });

    // Correct discriminator.
    assert!(decoder.decode_record(&native, &context).is_ok());

    // Wrong discriminator.
    let mut wrong = native.clone();
    wrong["st"] = json!(2);

    assert!(decoder.decode_record(&wrong, &context).is_err());

    // The existing TOML explicitly permits a missing
    // symbol-type field.
    let mut missing = native.clone();

    missing.as_object_mut().unwrap().remove("st");

    assert!(decoder.decode_record(&missing, &context).is_ok());

    // A present but invalid value must not be treated
    // as a missing value.
    let mut invalid = native;

    invalid["st"] = json!("1");

    assert!(decoder.decode_record(&invalid, &context).is_err());
}
