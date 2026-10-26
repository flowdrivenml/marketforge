#![cfg(feature = "live")]

use std::path::PathBuf;

use rust_decimal::Decimal;
use serde_json::Value;

use marketforge_engine::live::{
    config::{exchange::MarketType, format::FormatConfigStore},
    infrastructure::normalization::{
        NormalizationContext, NormalizationSource, QuantityPrecisionPolicy, TradeDecoder,
        TradeNormalizationCompiler,
    },
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};

// ============================================================
// FIXTURE PATHS
// ============================================================

fn fixture_directory() -> PathBuf {
    crate::support::data(".cache/live/websocket-fixtures/trades")
}

fn format_directory() -> PathBuf {
    crate::support::data(".config/live/formats")
}

// ============================================================
// TEST INSTRUMENT SPECIFICATIONS
// ============================================================

/// These test specifications correspond to the PostgreSQL
/// catalog.instrument_specs results captured on 2026-10-24.
///
/// Bitget Inverse is the ONLY mocked instrument.
///
/// Production metadata remains owned by Python orchestration.
///
/// This function must never be used as a production
/// instrument catalog.
fn test_request(profile: &str) -> CreateSourceStreamRequest {
    let (instrument_id, exchange, market, symbol, quantity_type, contract_value, contract_asset) =
        crate::cases::instrument(profile);

    let (base_asset, quote_asset) = match market {
        MarketType::Inverse => ("BTC", "USD"),
        MarketType::Spot | MarketType::Linear => {
            if symbol == "BTC_USD" || symbol == "BTCUSD" {
                ("BTC", "USD")
            } else {
                ("BTC", "USDT")
            }
        }
    };

    let settlement_asset = match market {
        MarketType::Spot => None,
        MarketType::Linear => Some(quote_asset.to_owned()),
        MarketType::Inverse => Some("BTC".to_owned()),
    };

    let request = CreateSourceStreamRequest {
        instrument_id,
        exchange,
        symbol: symbol.to_owned(),
        market_category: market,

        instrument_type: match market {
            MarketType::Spot => "spot",
            MarketType::Linear | MarketType::Inverse => "perpetual",
        }
        .to_owned(),

        base_asset: Some(base_asset.to_owned()),
        quote_asset: Some(quote_asset.to_owned()),

        quantity_type,

        contract_value: contract_value.map(str::to_owned),

        contract_value_asset: contract_asset.map(str::to_owned),

        settlement_asset,

        channel: SourceChannel::Trades,
    };

    request
        .validate()
        .unwrap_or_else(|error| panic!("{profile}: invalid stream request: {error}"));

    request
}

// ============================================================
// DECIMAL HELPERS
// ============================================================

fn decimal(value: &str) -> Decimal {
    Decimal::from_str_exact(value).unwrap()
}

fn contract_value(request: &CreateSourceStreamRequest) -> Decimal {
    decimal(
        request
            .contract_value
            .as_deref()
            .expect("contract quantity requires contract value"),
    )
}

// ============================================================
// CANONICAL ECONOMIC INVARIANTS
// ============================================================

fn validate_quantities(
    profile: &str,
    request: &CreateSourceStreamRequest,
    trade: &marketforge_engine::canonical::Trade,
) {
    let base = trade
        .quantity_base
        .expect("canonical base quantity missing");

    let quote = trade
        .quantity_quote
        .expect("canonical quote quantity missing");

    assert!(
        base > Decimal::ZERO,
        "{profile}: nonpositive canonical base quantity"
    );

    assert!(
        quote > Decimal::ZERO,
        "{profile}: nonpositive canonical quote quantity"
    );

    match request.quantity_type {
        // ====================================================
        // BASE-DENOMINATED NATIVE QUANTITY
        // ====================================================
        QuantityType::Base => {
            assert!(
                trade.quantity_contracts.is_none(),
                "{profile}: base quantity must not fabricate contracts"
            );

            assert_eq!(
                base.checked_mul(trade.price),
                Some(quote),
                "{profile}: incorrect base-to-quote conversion"
            );
        }

        // ====================================================
        // QUOTE-DENOMINATED NATIVE QUANTITY
        // ====================================================
        QuantityType::Quote => {
            assert!(
                trade.quantity_contracts.is_none(),
                "{profile}: quote quantity must not fabricate contracts"
            );

            // Quotient may have required explicit rounding.
            //
            // Validate the inverse relation within the
            // configured decimal precision.
            let reconstructed = base
                .checked_mul(trade.price)
                .expect("decimal multiplication failed");

            let error = (reconstructed - quote).abs();

            assert!(
                error <= decimal("0.00000001"),
                "{profile}: quote conversion discrepancy: {error}"
            );
        }

        // ====================================================
        // CONTRACT-DENOMINATED NATIVE QUANTITY
        // ====================================================
        QuantityType::Contracts => {
            let contracts = trade
                .quantity_contracts
                .expect("canonical contract count missing");

            assert!(
                contracts > Decimal::ZERO,
                "{profile}: nonpositive contract count"
            );

            let face = contracts
                .checked_mul(contract_value(request))
                .expect("contract face-value overflow");

            match request.contract_value_asset.as_deref() {
                Some("BTC") => {
                    assert_eq!(
                        base, face,
                        "{profile}: incorrect base-denominated contract conversion"
                    );

                    assert_eq!(
                        base.checked_mul(trade.price),
                        Some(quote),
                        "{profile}: incorrect contract quote quantity"
                    );
                }

                Some("USD") | Some("USDT") => {
                    assert_eq!(
                        quote, face,
                        "{profile}: incorrect quote-denominated contract conversion"
                    );

                    let reconstructed = base
                        .checked_mul(trade.price)
                        .expect("decimal multiplication failed");

                    let error = (reconstructed - quote).abs();

                    assert!(
                        error <= decimal("0.00000001"),
                        "{profile}: contract conversion discrepancy: {error}"
                    );
                }

                other => {
                    panic!("{profile}: unsupported test contract denomination: {other:?}");
                }
            }
        }
    }
}

// ============================================================
// MAIN INTEGRATION TEST
// ============================================================

#[test]
fn decodes_all_real_websocket_trades_to_canonical() {
    let formats =
        FormatConfigStore::load_dir(&format_directory()).expect("cannot load trade formats");

    assert_eq!(formats.trade_count(), 15);

    let mut profiles_tested = 0usize;
    let mut messages_tested = 0usize;
    let mut trades_tested = 0usize;

    println!();
    println!("{}", "=".repeat(72));
    println!("MARKETFORGE — CANONICAL WEBSOCKET TRADE VALIDATION");
    println!("{}", "=".repeat(72));

    for (profile, config) in formats.trades() {
        // ====================================================
        // AUTHORITATIVE REQUEST CONTRACT
        // ====================================================

        let request = test_request(profile);

        let context = NormalizationContext::new(&request, format!("fixture-{profile}"))
            .unwrap_or_else(|error| panic!("{profile}: context failed: {error}"));

        // ====================================================
        // COMPILE NATIVE FORMAT
        // ====================================================

        let plan = TradeNormalizationCompiler::compile_websocket(
            config,
            10_000,
            QuantityPrecisionPolicy::Round { decimal_places: 18 },
        )
        .unwrap_or_else(|error| panic!("{profile}: format compilation failed: {error}"));

        let decoder = TradeDecoder::new(plan)
            .unwrap_or_else(|error| panic!("{profile}: decoder construction failed: {error}"));

        // ====================================================
        // LOAD REAL WEBSOCKET FIXTURE
        // ====================================================

        let path = fixture_directory().join(format!("{profile}.json"));

        let contents = crate::fixtures::read(&path)
            .unwrap_or_else(|error| panic!("{profile}: cannot read fixture: {error}"));

        let fixture: Value = serde_json::from_slice(&contents)
            .unwrap_or_else(|error| panic!("{profile}: invalid fixture JSON: {error}"));

        let messages: Vec<&Value> = match &fixture {
            Value::Array(values) => values.iter().collect(),
            Value::Object(_) => vec![&fixture],
            _ => panic!("{profile}: invalid fixture root"),
        };

        assert!(
            !messages.is_empty(),
            "{profile}: fixture contains no messages"
        );

        let mut profile_trades = 0usize;

        // ====================================================
        // COMPLETE CANONICAL MESSAGE DECODING
        // ====================================================

        for (index, message) in messages.iter().enumerate() {
            let batch = decoder
                .decode_message(message, &context, NormalizationSource::WebSocket)
                .unwrap_or_else(|error| {
                    panic!(
                        "{profile}: message {index}: \
                         canonical decoding failed: {error}"
                    )
                });

            assert!(
                !batch.trades().is_empty(),
                "{profile}: decoded empty trade message"
            );

            for trade in batch.trades() {
                // ============================================
                // CANONICAL IDENTITY
                // ============================================

                assert_eq!(trade.envelope.instrument_id, request.instrument_id);

                assert_eq!(trade.envelope.symbol, request.symbol);

                assert_eq!(trade.envelope.stream_id, context.stream_id());

                assert!(
                    trade.trade_id.is_some(),
                    "{profile}: missing native trade identity"
                );

                // ============================================
                // PRICE / TIMESTAMP
                // ============================================

                assert!(
                    trade.price > Decimal::ZERO,
                    "{profile}: invalid trade price"
                );

                assert!(
                    trade.envelope.event_timestamp_ns > 0,
                    "{profile}: invalid event timestamp"
                );

                // ============================================
                // ECONOMIC QUANTITY SEMANTICS
                // ============================================

                validate_quantities(profile, &request, trade);

                profile_trades += 1;
            }

            messages_tested += 1;
        }

        assert!(profile_trades > 0, "{profile}: no canonical trades decoded");

        trades_tested += profile_trades;
        profiles_tested += 1;

        let origin = if profile == "bitget_inverse" {
            "MOCK INSTRUMENT"
        } else {
            "DB SPEC"
        };

        println!(
            "PASS  {profile:<20} \
             trades={profile_trades:<5} \
             quantity={:?} [{origin}]",
            request.quantity_type
        );
    }

    println!();
    println!("{}", "=".repeat(72));
    println!("CANONICAL NORMALIZATION REPORT");
    println!("{}", "=".repeat(72));

    println!("Profiles:       {profiles_tested}");
    println!("Messages:       {messages_tested}");
    println!("Canonical trades: {trades_tested}");

    assert_eq!(profiles_tested, 15);
    assert!(messages_tested >= 15);
    assert!(trades_tested >= 15);

    println!("RESULT: PASS");
}
