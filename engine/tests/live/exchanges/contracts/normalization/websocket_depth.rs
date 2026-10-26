use std::path::PathBuf;

use marketforge_engine::live::{
    config::{exchange::MarketType, format::DepthFormatConfig},
    infrastructure::normalization::{
        DepthDecoder, DepthMessageKind, DepthTimestampProvenance, NormalizationContext,
        NormalizationSource, QuantityPrecisionPolicy,
    },
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};

use serde_json::{Value, json};

// ============================================================
// FIXTURES
// ============================================================

fn fixture_root() -> PathBuf {
    crate::support::data(".cache/live/websocket-fixtures/depth")
}

fn format_root() -> PathBuf {
    crate::support::data(".config/live/formats/depth")
}

fn fixtures(profile: &str) -> Vec<Value> {
    let path = fixture_root().join(format!("{profile}.json"));

    serde_json::from_slice(&crate::fixtures::read(path).unwrap()).unwrap()
}

fn config(profile: &str) -> DepthFormatConfig {
    let path = format_root().join(format!("{profile}.toml"));

    DepthFormatConfig::load(path).unwrap()
}

// ============================================================
// TEST INSTRUMENT CONTEXT
// ============================================================

fn context(profile: &str, symbol: &str) -> NormalizationContext {
    let format = config(profile);

    let market = format.format.market.clone();

    // Test-only instrument metadata.
    //
    // The real runtime must obtain quantity semantics and
    // contract denomination from the instrument catalog.
    //
    // These values exercise normalization mechanics; they
    // do NOT certify actual exchange contract specifications.

    let is_inverse = market == MarketType::Inverse;

    let uses_contracts = is_inverse
        || profile.starts_with("gateio_") && market == MarketType::Linear
        || profile.starts_with("okx_") && market == MarketType::Linear;

    let quantity_type = if uses_contracts {
        QuantityType::Contracts
    } else {
        QuantityType::Base
    };

    let contract_value = if uses_contracts {
        Some("1".to_owned())
    } else {
        None
    };

    let contract_asset = if is_inverse {
        Some("USD".to_owned())
    } else if uses_contracts {
        Some("BTC".to_owned())
    } else {
        None
    };

    let request = CreateSourceStreamRequest {
        instrument_id: 42,
        exchange: format.format.exchange,
        symbol: symbol.into(),
        market_category: market.clone(),

        instrument_type: if market == MarketType::Spot {
            "spot".into()
        } else {
            "perpetual".into()
        },

        base_asset: Some("BTC".into()),
        quote_asset: Some(if is_inverse { "USD" } else { "USDT" }.into()),

        quantity_type,
        contract_value,
        contract_value_asset: contract_asset,

        channel: SourceChannel::Depth,

        settlement_asset: Some(if is_inverse { "BTC" } else { "USDT" }.into()),
    };

    NormalizationContext::new(&request, format!("test:{profile}:depth")).unwrap()
}

// ============================================================
// EXPECTED INSTRUMENT SYMBOLS
// ============================================================

fn symbol(profile: &str) -> &'static str {
    crate::cases::symbol(profile)
}

fn subscription_depth(profile: &str) -> Option<u64> {
    if profile.starts_with("bybit_") {
        Some(200)
    } else {
        None
    }
}

// ============================================================
// DECODER FACTORY
// ============================================================

fn decoder(profile: &str) -> DepthDecoder {
    DepthDecoder::new_websocket(
        &config(profile),
        // A large test budget accommodates full snapshots.
        20_000,
        // Test-only explicit conversion precision.
        QuantityPrecisionPolicy::Round { decimal_places: 18 },
        subscription_depth(profile),
    )
    .unwrap()
}

// ============================================================
// ALL 15 EXCHANGE/MARKET PROFILES
// ============================================================

#[test]
fn decodes_all_real_websocket_depth_fixtures() {
    let mut total_messages = 0;
    let mut total_levels = 0;

    for exchange in ["binance", "bybit", "bitget", "gateio", "okx"] {
        for market in ["spot", "linear", "inverse"] {
            let profile = format!("{exchange}_{market}");

            let decoder = decoder(&profile);

            let context = context(&profile, symbol(&profile));

            let messages = fixtures(&profile);

            assert_eq!(messages.len(), 12, "{profile}: unexpected fixture count",);

            let mut snapshots = 0;
            let mut deltas = 0;

            for (index, native) in messages.iter().enumerate() {
                let normalized = decoder
                    .decode_websocket(native, &context)
                    .unwrap_or_else(|error| panic!("{profile}, message {index}: {error:?}"));

                assert_eq!(normalized.stream_id(), context.stream_id(),);

                assert_eq!(normalized.source(), NormalizationSource::WebSocket,);

                assert!(
                    normalized.sequence().sequence_last.is_some(),
                    "{profile}, message {index}: missing sequence",
                );

                assert!(normalized.message_timestamp_ns().is_some(),);

                for level in normalized.levels() {
                    assert!(level.price > rust_decimal::Decimal::ZERO);

                    assert_eq!(level.envelope.stream_id, context.stream_id(),);

                    assert_eq!(level.envelope.symbol, context.symbol(),);

                    assert_eq!(level.envelope.instrument_id, context.instrument_id(),);
                }

                total_levels += normalized.level_count();

                match normalized.kind() {
                    DepthMessageKind::Snapshot => snapshots += 1,
                    DepthMessageKind::Delta => deltas += 1,
                }
            }

            if matches!(exchange, "bybit" | "bitget" | "okx") {
                assert_eq!(snapshots, 1, "{profile}: expected initial snapshot",);

                assert_eq!(deltas, 11, "{profile}: expected subsequent deltas",);
            } else {
                assert_eq!(snapshots, 0);
                assert_eq!(deltas, 12);
            }

            total_messages += messages.len();

            println!(
                "{profile:<20} PASS \
                 snapshots={snapshots} deltas={deltas}"
            );
        }
    }

    assert_eq!(total_messages, 180);

    println!(
        "Decoded {total_messages} native depth messages \
         containing {total_levels} canonical levels"
    );
}

// ============================================================
// NATIVE MESSAGE IDENTITY
// ============================================================

#[test]
fn rejects_wrong_binance_instrument() {
    let decoder = decoder("binance_spot");

    let context = context("binance_spot", "BTCUSDT");

    let mut native = fixtures("binance_spot").remove(0);

    native["s"] = json!("ETHUSDT");

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

#[test]
fn rejects_wrong_bybit_subscription_topic() {
    let decoder = decoder("bybit_spot");

    let context = context("bybit_spot", "BTCUSDT");

    let mut native = fixtures("bybit_spot").remove(0);

    native["topic"] = json!("orderbook.200.ETHUSDT");

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

#[test]
fn rejects_wrong_okx_instrument() {
    let decoder = decoder("okx_spot");

    let context = context("okx_spot", "BTC-USDT");

    let mut native = fixtures("okx_spot").remove(0);

    native["arg"]["instId"] = json!("ETH-USDT");

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

// ============================================================
// SNAPSHOT / DELTA CLASSIFICATION
// ============================================================

#[test]
fn rejects_unknown_bybit_message_type() {
    let decoder = decoder("bybit_spot");

    let context = context("bybit_spot", "BTCUSDT");

    let mut native = fixtures("bybit_spot").remove(0);

    native["type"] = json!("unknown");

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

#[test]
fn rejects_unknown_bitget_action() {
    let decoder = decoder("bitget_spot");

    let context = context("bitget_spot", "BTCUSDT");

    let mut native = fixtures("bitget_spot").remove(0);

    native["action"] = json!("unknown");

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

// ============================================================
// MULTIPLE NATIVE RECORDS
// ============================================================

#[test]
fn rejects_multiple_okx_depth_records() {
    let decoder = decoder("okx_spot");

    let context = context("okx_spot", "BTC-USDT");

    let mut native = fixtures("okx_spot").remove(0);

    let duplicate = native["data"][0].clone();

    native["data"].as_array_mut().unwrap().push(duplicate);

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

#[test]
fn rejects_multiple_bitget_depth_records() {
    let decoder = decoder("bitget_spot");

    let context = context("bitget_spot", "BTCUSDT");

    let mut native = fixtures("bitget_spot").remove(0);

    let duplicate = native["data"][0].clone();

    native["data"].as_array_mut().unwrap().push(duplicate);

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

// ============================================================
// MALFORMED LEVELS
// ============================================================

#[test]
fn rejects_invalid_price_without_partial_output() {
    let decoder = decoder("binance_spot");

    let context = context("binance_spot", "BTCUSDT");

    let mut native = fixtures("binance_spot").remove(0);

    native["b"][0][0] = json!("-1");

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

#[test]
fn rejects_invalid_quantity_without_partial_output() {
    let decoder = decoder("binance_spot");

    let context = context("binance_spot", "BTCUSDT");

    let mut native = fixtures("binance_spot").remove(0);

    native["b"][0][1] = json!("invalid");

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

// ============================================================
// OKX ORDER COUNT
// ============================================================

#[test]
fn preserves_okx_native_order_counts() {
    let decoder = decoder("okx_spot");

    let context = context("okx_spot", "BTC-USDT");

    let native = fixtures("okx_spot").remove(0);

    let expected = native["data"][0]["bids"][0][3]
        .as_str()
        .unwrap()
        .parse::<u64>()
        .unwrap();

    let normalized = decoder.decode_websocket(&native, &context).unwrap();

    assert_eq!(normalized.levels()[0].order_count, Some(expected),);
}

// ============================================================
// ZERO QUANTITY
// ============================================================

#[test]
fn preserves_zero_quantity_for_depth_deletion() {
    let decoder = decoder("binance_spot");

    let context = context("binance_spot", "BTCUSDT");

    let mut native = fixtures("binance_spot").remove(0);

    native["b"][0][1] = json!("0");

    let normalized = decoder.decode_websocket(&native, &context).unwrap();

    assert_eq!(
        normalized.levels()[0].quantity_base,
        Some(rust_decimal::Decimal::ZERO),
    );
}

// ============================================================
// RECORD-LEVEL INSTRUMENT IDENTITY
// ============================================================

#[test]
fn rejects_gateio_spot_foreign_record_symbol() {
    let decoder = decoder("gateio_spot");
    let context = context("gateio_spot", "BTC_USDT");

    let mut native = fixtures("gateio_spot").remove(0);

    // Valid outer channel, but the native record refers
    // to a different instrument.
    native["result"]["s"] = json!("ETH_USDT");

    assert!(
        decoder.decode_websocket(&native, &context).is_err(),
        "foreign native record must not become canonical BTC data"
    );
}

#[test]
fn rejects_gateio_futures_foreign_record_symbol() {
    for profile in ["gateio_linear", "gateio_inverse"] {
        let decoder = decoder(profile);
        let context = context(profile, symbol(profile));

        let mut native = fixtures(profile).remove(0);

        native["result"]["s"] = json!("WRONG_INSTRUMENT");

        assert!(
            decoder.decode_websocket(&native, &context).is_err(),
            "{profile}: foreign record was accepted"
        );
    }
}

#[test]
fn rejects_bybit_inner_symbol_mismatch() {
    let decoder = decoder("bybit_spot");
    let context = context("bybit_spot", "BTCUSDT");

    let mut native = fixtures("bybit_spot").remove(0);

    // Keep the subscribed topic correct.
    assert_eq!(native["topic"], json!("orderbook.200.BTCUSDT"),);

    // Corrupt only the instrument inside the payload.
    native["data"]["s"] = json!("ETHUSDT");

    assert!(
        decoder.decode_websocket(&native, &context).is_err(),
        "a valid topic must not override conflicting record identity"
    );
}

#[test]
fn rejects_missing_declared_record_symbol() {
    let decoder = decoder("gateio_spot");
    let context = context("gateio_spot", "BTC_USDT");

    let mut native = fixtures("gateio_spot").remove(0);

    native["result"].as_object_mut().unwrap().remove("s");

    assert!(decoder.decode_websocket(&native, &context).is_err());
}

#[test]
fn rejects_wrong_source_channel() {
    let decoder = decoder("binance_spot");

    let format = config("binance_spot");

    let request = CreateSourceStreamRequest {
        instrument_id: 42,
        exchange: format.format.exchange,
        symbol: "BTCUSDT".into(),
        market_category: MarketType::Spot,
        instrument_type: "spot".into(),

        base_asset: Some("BTC".into()),
        quote_asset: Some("USDT".into()),

        quantity_type: QuantityType::Base,
        contract_value: None,
        contract_value_asset: None,

        channel: SourceChannel::Trades,
        settlement_asset: Some("USDT".into()),
    };

    let context = NormalizationContext::new(&request, "wrong-channel-test").unwrap();

    let native = fixtures("binance_spot").remove(0);

    assert!(
        decoder.decode_websocket(&native, &context).is_err(),
        "trade-stream context must not be accepted by depth decoder"
    );
}

#[test]
fn all_websocket_depth_profiles_preserve_exchange_timestamp_provenance() {
    let profiles = [
        "binance_spot",
        "binance_linear",
        "binance_inverse",
        "bybit_spot",
        "bybit_linear",
        "bybit_inverse",
        "bitget_spot",
        "bitget_linear",
        "bitget_inverse",
        "gateio_spot",
        "gateio_linear",
        "gateio_inverse",
        "okx_spot",
        "okx_linear",
        "okx_inverse",
    ];

    for profile in profiles {
        let messages = fixtures(profile);
        let decoder = decoder(profile);
        let context = context(profile, symbol(profile));

        for (index, native) in messages.iter().enumerate() {
            let normalized = decoder
                .decode_websocket(native, &context)
                .unwrap_or_else(|error| panic!("{profile} message {index}: {error:?}"));

            assert_eq!(
                normalized.timestamp_provenance(),
                Some(DepthTimestampProvenance::Exchange),
                "{profile} message {index}",
            );

            let exchange_timestamp = normalized
                .message_timestamp_ns()
                .expect("mapped exchange timestamp");

            for level in normalized.levels() {
                assert_eq!(
                    level.envelope.event_timestamp_ns, exchange_timestamp,
                    "{profile} message {index}",
                );
            }
        }
    }
}
