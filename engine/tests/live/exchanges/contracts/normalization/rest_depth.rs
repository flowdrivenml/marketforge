use std::path::PathBuf;

use marketforge_engine::live::{
    config::{exchange::MarketType, format::DepthFormatConfig},
    infrastructure::normalization::{
        DepthMessageKind, DepthTimestampProvenance, NormalizationContext, NormalizationSource,
        QuantityPrecisionPolicy, RestDepthDecoder,
    },
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};

use rust_decimal::Decimal;
use serde_json::{Value, json};

// ============================================================
// FIXTURE LOCATIONS
// ============================================================

fn data_root() -> PathBuf {
    crate::support::data("")
}

fn depth_format(profile: &str) -> DepthFormatConfig {
    DepthFormatConfig::load(
        data_root()
            .join(".config/live/formats/depth")
            .join(format!("{profile}.toml")),
    )
    .unwrap()
}

fn rest_endpoint(profile: &str) -> Value {
    let path = data_root()
        .join(".config/live/exchanges")
        .join(format!("{profile}.toml"));

    let contents = std::fs::read_to_string(path).unwrap();

    let config: toml::Value = toml::from_str(&contents).unwrap();

    serde_json::to_value(&config["rest"]["endpoints"]["depth_snapshot"]).unwrap()
}

fn fixture_bytes(profile: &str) -> Vec<u8> {
    let path = data_root()
        .join(".cache/live/rest-fixtures")
        .join(format!("{profile}__depth_snapshot.json"));

    crate::fixtures::read(path).unwrap()
}

fn fixture_value(profile: &str) -> Value {
    serde_json::from_slice(&fixture_bytes(profile)).unwrap()
}

// ============================================================
// PROFILE IDENTITIES
// ============================================================

const EXCHANGES: [&str; 5] = ["binance", "bybit", "bitget", "gateio", "okx"];

const MARKETS: [&str; 3] = ["spot", "linear", "inverse"];

fn symbol(profile: &str) -> &'static str {
    crate::cases::symbol(profile)
}

fn requested_depth(profile: &str) -> u64 {
    match profile {
        "binance_spot" => 1000,

        "binance_linear" | "binance_inverse" => 500,

        "gateio_spot" | "gateio_linear" | "gateio_inverse" => 100,

        "okx_spot" | "okx_linear" | "okx_inverse" => 400,

        _ => 200,
    }
}

// ============================================================
// TEST INSTRUMENT CONTEXT
// ============================================================

fn context(profile: &str) -> NormalizationContext {
    let format = depth_format(profile);
    let market = format.format.market.clone();

    let inverse = market == MarketType::Inverse;

    // These are test-only contract specifications.
    //
    // They exercise exact/rounded quantity conversion.
    // They are NOT authoritative exchange contract metadata.

    let contracts = inverse
        || (market == MarketType::Linear
            && (profile.starts_with("gateio_") || profile.starts_with("okx_")));

    let quantity_type = if contracts {
        QuantityType::Contracts
    } else {
        QuantityType::Base
    };

    let request = CreateSourceStreamRequest {
        instrument_id: 42,

        exchange: format.format.exchange,
        symbol: symbol(profile).to_owned(),
        market_category: market.clone(),

        instrument_type: if market == MarketType::Spot {
            "spot".into()
        } else {
            "perpetual".into()
        },

        base_asset: Some("BTC".into()),

        quote_asset: Some(if inverse { "USD" } else { "USDT" }.into()),

        quantity_type,

        contract_value: if contracts { Some("1".into()) } else { None },

        contract_value_asset: if inverse {
            Some("USD".into())
        } else if contracts {
            Some("BTC".into())
        } else {
            None
        },

        channel: SourceChannel::Depth,

        settlement_asset: Some(if inverse { "BTC" } else { "USDT" }.into()),
    };

    NormalizationContext::new(&request, format!("rest-test:{profile}")).unwrap()
}

// ============================================================
// DECODER FACTORY
// ============================================================

fn decoder(profile: &str) -> RestDepthDecoder {
    RestDepthDecoder::compile(
        &depth_format(profile),
        &rest_endpoint(profile),
        // Combined bid/ask level budget.
        20_000,
        // Test-only policy for derived quantity conversions.
        // Original native quantity must remain exact.
        QuantityPrecisionPolicy::Round { decimal_places: 18 },
    )
    .unwrap()
}

fn decode(
    profile: &str,
) -> marketforge_engine::live::infrastructure::normalization::NormalizedDepth {
    decoder(profile)
        .decode_bytes(
            &fixture_bytes(profile),
            &context(profile),
            // Trusted test receive timestamp.
            1_792_900_000_000_000_000,
            requested_depth(profile),
        )
        .unwrap_or_else(|error| panic!("{profile}: {error:?}"))
}

// ============================================================
// EXPECTED RAW SNAPSHOT RECORD
// ============================================================

fn native_record<'a>(profile: &str, response: &'a Value) -> &'a Value {
    if profile.starts_with("bybit_") {
        &response["result"]
    } else if profile.starts_with("bitget_") {
        &response["data"]
    } else if profile.starts_with("okx_") {
        &response["data"][0]
    } else {
        response
    }
}

fn native_sides<'a>(profile: &str, response: &'a Value) -> (&'a Value, &'a Value) {
    let record = native_record(profile, response);

    if profile.starts_with("bybit_") || profile.starts_with("bitget_") {
        (&record["b"], &record["a"])
    } else {
        (&record["bids"], &record["asks"])
    }
}

// ============================================================
// ALL FIFTEEN REAL REST SNAPSHOTS
// ============================================================

#[test]
fn decodes_all_fifteen_real_rest_snapshots() {
    let mut total_snapshots = 0;
    let mut total_levels = 0;

    for exchange in EXCHANGES {
        for market in MARKETS {
            let profile = format!("{exchange}_{market}");

            let normalized = decode(&profile);

            let response = fixture_value(&profile);
            let (native_bids, native_asks) = native_sides(&profile, &response);

            let expected_levels =
                native_bids.as_array().unwrap().len() + native_asks.as_array().unwrap().len();

            assert_eq!(normalized.source(), NormalizationSource::Rest, "{profile}");

            assert_eq!(normalized.kind(), DepthMessageKind::Snapshot, "{profile}");

            assert_eq!(
                normalized.stream_id(),
                context(&profile).stream_id(),
                "{profile}"
            );

            assert_eq!(
                normalized.declared_depth(),
                Some(requested_depth(&profile)),
                "{profile}"
            );

            assert_eq!(
                normalized.level_count(),
                expected_levels,
                "{profile}: missing or duplicated depth levels"
            );

            assert!(
                normalized.level_count() > 0,
                "{profile}: empty captured snapshot"
            );

            // Each canonical level must retain the trusted
            // source-stream and native instrument identity.
            for level in normalized.levels() {
                assert_eq!(
                    level.envelope.stream_id,
                    context(&profile).stream_id(),
                    "{profile}"
                );

                assert_eq!(level.envelope.symbol, symbol(&profile), "{profile}");

                assert_eq!(level.envelope.instrument_id, 42, "{profile}");

                assert!(level.price > Decimal::ZERO, "{profile}");
            }

            // Bitget's REST response has no usable
            // native sequence baseline.
            if exchange == "bitget" {
                assert_eq!(
                    normalized.sequence().sequence_last,
                    None,
                    "{profile}: fabricated REST sequence"
                );
            } else {
                assert!(
                    normalized.sequence().sequence_last.is_some(),
                    "{profile}: native sequence was lost"
                );
            }

            total_levels += normalized.level_count();
            total_snapshots += 1;

            println!(
                "{profile:<20} PASS levels={expected_levels} \
                 sequence={:?}",
                normalized.sequence().sequence_last
            );
        }
    }

    assert_eq!(total_snapshots, 15);

    println!(
        "\nREST DEPTH NORMALIZATION COMPLETE: \
         {total_snapshots} snapshots, {total_levels} levels"
    );
}

// ============================================================
// EXACT BITGET JSON NUMERIC VALUES
// ============================================================

#[test]
fn preserves_exact_bitget_native_numeric_values() {
    for profile in ["bitget_spot", "bitget_linear", "bitget_inverse"] {
        let raw = fixture_value(profile);

        let first_bid = &raw["data"]["b"][0];

        let expected_price = Decimal::from_str_exact(&first_bid[0].to_string()).unwrap();

        let expected_quantity = Decimal::from_str_exact(&first_bid[1].to_string()).unwrap();

        let normalized = decode(profile);

        let actual = &normalized.levels()[0];

        assert_eq!(
            actual.price, expected_price,
            "{profile}: price precision lost"
        );

        // Spot/linear test context uses base quantity.
        // Inverse test context uses contract quantity.
        let native_quantity = if profile.ends_with("inverse") {
            actual.quantity_contracts
        } else {
            actual.quantity_base
        };

        assert_eq!(
            native_quantity,
            Some(expected_quantity),
            "{profile}: original native quantity changed"
        );

        println!(
            "{profile}: exact price={} quantity={}",
            expected_price, expected_quantity
        );
    }
}

// ============================================================
// NATIVE SEQUENCE METADATA
// ============================================================

#[test]
fn preserves_binance_rest_snapshot_sequence() {
    for profile in ["binance_spot", "binance_linear", "binance_inverse"] {
        let raw = fixture_value(profile);
        let normalized = decode(profile);

        assert_eq!(
            normalized.sequence().sequence_last,
            raw["lastUpdateId"].as_u64(),
            "{profile}"
        );

        assert_eq!(normalized.sequence().sequence_first, None, "{profile}");
    }
}

#[test]
fn preserves_bybit_rest_update_and_cross_sequence() {
    for profile in ["bybit_spot", "bybit_linear", "bybit_inverse"] {
        let raw = fixture_value(profile);
        let normalized = decode(profile);

        assert_eq!(
            normalized.sequence().sequence_last,
            raw["result"]["u"].as_u64(),
            "{profile}"
        );

        assert_eq!(
            normalized.sequence().cross_sequence,
            raw["result"]["seq"].as_u64(),
            "{profile}"
        );
    }
}

#[test]
fn preserves_gateio_rest_snapshot_sequence() {
    for profile in ["gateio_spot", "gateio_linear", "gateio_inverse"] {
        let raw = fixture_value(profile);
        let normalized = decode(profile);

        assert_eq!(
            normalized.sequence().sequence_last,
            raw["id"].as_u64(),
            "{profile}"
        );
    }
}

#[test]
fn preserves_okx_rest_snapshot_sequence() {
    for profile in ["okx_spot", "okx_linear", "okx_inverse"] {
        let raw = fixture_value(profile);
        let normalized = decode(profile);

        assert_eq!(
            normalized.sequence().sequence_last,
            raw["data"][0]["seqId"].as_u64(),
            "{profile}"
        );
    }
}

// ============================================================
// OKX ORDER COUNTS
// ============================================================

#[test]
fn preserves_okx_rest_order_counts() {
    for profile in ["okx_spot", "okx_linear", "okx_inverse"] {
        let raw = fixture_value(profile);

        let expected = raw["data"][0]["bids"][0][3]
            .as_str()
            .unwrap()
            .parse::<u64>()
            .unwrap();

        let normalized = decode(profile);

        assert_eq!(
            normalized.levels()[0].order_count,
            Some(expected),
            "{profile}: lost native order count"
        );
    }
}

// ============================================================
// APPLICATION STATUS VALIDATION
// ============================================================

#[test]
fn rejects_rest_application_failures() {
    for (profile, code_field, error_code) in [
        ("bybit_spot", "retCode", json!(10001)),
        ("bitget_spot", "code", json!("10001")),
        ("okx_spot", "code", json!("50001")),
    ] {
        let mut response = fixture_value(profile);

        response[code_field] = error_code;

        let bytes = serde_json::to_vec(&response).unwrap();

        assert!(
            decoder(profile)
                .decode_bytes(
                    &bytes,
                    &context(profile),
                    1_792_900_000_000_000_000,
                    requested_depth(profile),
                )
                .is_err(),
            "{profile}: accepted failed REST application response"
        );
    }
}

// ============================================================
// SNAPSHOT RECORD STRUCTURE
// ============================================================

#[test]
fn rejects_multiple_okx_snapshot_records() {
    let profile = "okx_spot";

    let mut response = fixture_value(profile);

    let duplicate = response["data"][0].clone();

    response["data"].as_array_mut().unwrap().push(duplicate);

    let bytes = serde_json::to_vec(&response).unwrap();

    assert!(
        decoder(profile)
            .decode_bytes(&bytes, &context(profile), 1_792_900_000_000_000_000, 400,)
            .is_err()
    );
}

#[test]
fn rejects_missing_native_sequence() {
    let profile = "binance_spot";

    let mut response = fixture_value(profile);

    response.as_object_mut().unwrap().remove("lastUpdateId");

    let bytes = serde_json::to_vec(&response).unwrap();

    assert!(
        decoder(profile)
            .decode_bytes(&bytes, &context(profile), 1_792_900_000_000_000_000, 1000,)
            .is_err()
    );
}

// ============================================================
// INSTRUMENT IDENTITY
// ============================================================

#[test]
fn rejects_foreign_bybit_rest_instrument() {
    let profile = "bybit_spot";

    let mut response = fixture_value(profile);

    response["result"]["s"] = json!("ETHUSDT");

    let bytes = serde_json::to_vec(&response).unwrap();

    assert!(
        decoder(profile)
            .decode_bytes(&bytes, &context(profile), 1_792_900_000_000_000_000, 200,)
            .is_err()
    );
}

// ============================================================
// DECIMAL SAFETY
// ============================================================

#[test]
fn rejects_nonrepresentable_native_decimal() {
    let profile = "binance_spot";

    let response = br#"{
        "lastUpdateId": 100,
        "bids": [
            ["80000", 0.12345678901234567890123456789]
        ],
        "asks": []
    }"#;

    assert!(
        decoder(profile)
            .decode_bytes(response, &context(profile), 1_792_900_000_000_000_000, 1000,)
            .is_err()
    );
}

// ============================================================
// INVALID DEPTH LIMITS
// ============================================================

#[test]
fn rejects_zero_requested_depth() {
    let profile = "binance_spot";

    assert!(
        decoder(profile)
            .decode_bytes(
                &fixture_bytes(profile),
                &context(profile),
                1_792_900_000_000_000_000,
                0,
            )
            .is_err()
    );
}

#[test]
fn rejects_snapshot_exceeding_requested_depth() {
    let profile = "binance_spot";

    // The fixture contains 1000 levels per side.
    // A request claiming depth 100 cannot legitimately
    // produce this 1000-level snapshot.
    assert!(
        decoder(profile)
            .decode_bytes(
                &fixture_bytes(profile),
                &context(profile),
                1_792_900_000_000_000_000,
                100,
            )
            .is_err()
    );
}

#[test]
fn rest_snapshots_without_native_timestamp_use_receive_time() {
    let receive_timestamp_ns = 1_792_900_000_000_000_000_i64;

    for profile in [
        "binance_spot",
        "binance_linear",
        "binance_inverse",
        "bybit_spot",
        "bybit_linear",
        "bybit_inverse",
        "gateio_spot",
        "gateio_linear",
        "gateio_inverse",
        "okx_spot",
        "okx_linear",
        "okx_inverse",
    ] {
        let normalized = decoder(profile)
            .decode_bytes(
                &fixture_bytes(profile),
                &context(profile),
                receive_timestamp_ns,
                requested_depth(profile),
            )
            .unwrap();

        assert_eq!(
            normalized.timestamp_provenance(),
            Some(DepthTimestampProvenance::ReceiveTimeFallback),
            "{profile}"
        );

        assert_eq!(normalized.message_timestamp_ns(), None, "{profile}");

        for level in normalized.levels() {
            assert_eq!(
                level.envelope.event_timestamp_ns, receive_timestamp_ns,
                "{profile}"
            );
        }
    }
}

#[test]
fn bitget_rest_snapshots_preserve_exchange_timestamp_provenance() {
    for profile in ["bitget_spot", "bitget_linear", "bitget_inverse"] {
        let normalized = decode(profile);

        assert_eq!(
            normalized.timestamp_provenance(),
            Some(DepthTimestampProvenance::Exchange),
            "{profile}"
        );

        let exchange_timestamp_ns = normalized
            .message_timestamp_ns()
            .expect("Bitget native REST timestamp");

        for level in normalized.levels() {
            assert_eq!(
                level.envelope.event_timestamp_ns, exchange_timestamp_ns,
                "{profile}"
            );
        }
    }
}

#[test]
fn rest_snapshot_provenance_cannot_be_rebound() {
    let message = decode("binance_spot");

    let result = message.with_timestamp_provenance(DepthTimestampProvenance::Exchange);

    assert!(
        result.is_err(),
        "REST timestamp provenance was incorrectly rebound"
    );
}

#[test]
fn bitget_rest_timestamp_uses_declared_millisecond_unit() {
    for profile in ["bitget_spot", "bitget_linear", "bitget_inverse"] {
        let response = fixture_value(profile);

        let native_ms = response["data"]["ts"]
            .as_str()
            .expect("Bitget REST timestamp string")
            .parse::<i64>()
            .unwrap();

        let expected_ns = native_ms.checked_mul(1_000_000).unwrap();

        let normalized = decode(profile);

        assert_eq!(
            normalized.message_timestamp_ns(),
            Some(expected_ns),
            "{profile}"
        );

        assert_eq!(
            normalized.timestamp_provenance(),
            Some(DepthTimestampProvenance::Exchange),
            "{profile}"
        );

        for level in normalized.levels() {
            assert_eq!(level.envelope.event_timestamp_ns, expected_ns, "{profile}");
        }
    }
}
