use std::{collections::BTreeMap, path::PathBuf};

use rust_decimal::Decimal;
use serde_json::Value;

use marketforge_engine::{
    canonical::Exchange,
    live::{
        config::{
            exchange::{ExchangeId, MarketType},
            format::DepthFormatConfig,
        },
        infrastructure::normalization::{
            DepthDecoder, NormalizationContext, QuantityPrecisionPolicy,
        },
        integrity::{
            BoundDepthIngress, DepthBufferLimits, DepthContinuityCompiler, DepthControllerOutcome,
            DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch,
        },
        stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
    },
};

const STREAM_ID: &str = "integration:bybit:derivatives";
const INSTRUMENT_ID: i64 = 42;
const DEPTH: u64 = 200;

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

fn fixtures(profile: &str) -> Vec<Value> {
    serde_json::from_slice(
        &crate::fixtures::read(
            data_root()
                .join(".cache/live/websocket-fixtures/depth")
                .join(format!("{profile}.json")),
        )
        .unwrap(),
    )
    .unwrap()
}

fn market(profile: &str) -> MarketType {
    match profile {
        "bybit_linear" => MarketType::Linear,
        "bybit_inverse" => MarketType::Inverse,
        _ => panic!("unexpected profile: {profile}"),
    }
}

fn symbol(profile: &str) -> &'static str {
    match profile {
        "bybit_linear" => "BTCUSDT",
        "bybit_inverse" => "BTCUSD",
        _ => panic!("unexpected profile: {profile}"),
    }
}

fn epoch() -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: 10,
        connection_generation: 20,
        subscription_generation: 30,
    }
}

fn context(profile: &str) -> NormalizationContext {
    let inverse = profile == "bybit_inverse";

    let request = CreateSourceStreamRequest {
        instrument_id: INSTRUMENT_ID,
        exchange: ExchangeId::Bybit,
        symbol: symbol(profile).into(),
        market_category: market(profile),
        instrument_type: "perpetual".into(),

        base_asset: Some("BTC".into()),
        quote_asset: Some(if inverse { "USD" } else { "USDT" }.into()),

        quantity_type: if inverse {
            QuantityType::Contracts
        } else {
            QuantityType::Base
        },

        // Illustrative metadata, not authoritative
        // exchange contract specifications.
        contract_value: if inverse { Some("1".into()) } else { None },

        contract_value_asset: if inverse { Some("USD".into()) } else { None },

        channel: SourceChannel::Depth,

        settlement_asset: Some(if inverse { "BTC" } else { "USDT" }.into()),
    };

    NormalizationContext::new(&request, STREAM_ID).unwrap()
}

fn decoder(profile: &str) -> DepthDecoder {
    DepthDecoder::new_websocket(
        &depth_format(profile),
        20_000,
        QuantityPrecisionPolicy::Round { decimal_places: 18 },
        Some(DEPTH),
    )
    .unwrap()
}

fn ingress(profile: &str) -> BoundDepthIngress {
    let binding = DepthSourceBinding::new(
        STREAM_ID,
        Exchange::Bybit,
        market(profile).as_str(),
        INSTRUMENT_ID,
        symbol(profile),
        Some(DEPTH),
        epoch(),
    )
    .unwrap();

    let plan = DepthContinuityCompiler::compile(&depth_format(profile)).unwrap();

    BoundDepthIngress::new(
        binding,
        plan,
        DepthBufferLimits {
            max_messages: 100,
            max_bytes: 20_000_000,
        },
        DepthResourceLimits::default(),
    )
    .unwrap()
}

// ============================================================
// INDEPENDENT NATIVE REFERENCE BOOK
// ============================================================

#[derive(Default)]
struct ReferenceBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl ReferenceBook {
    fn apply(&mut self, message: &Value) {
        let kind = message["type"].as_str().expect("Bybit message type");

        match kind {
            "snapshot" => {
                self.bids.clear();
                self.asks.clear();
            }

            "delta" => {}

            other => panic!("unexpected message type: {other}"),
        }

        let data = &message["data"];

        Self::apply_side(&mut self.bids, &data["b"]);
        Self::apply_side(&mut self.asks, &data["a"]);
    }

    fn apply_side(book: &mut BTreeMap<Decimal, Decimal>, native: &Value) {
        for record in native.as_array().expect("native levels") {
            let price = Decimal::from_str_exact(record[0].as_str().expect("price string")).unwrap();

            let quantity =
                Decimal::from_str_exact(record[1].as_str().expect("quantity string")).unwrap();

            assert!(price > Decimal::ZERO);
            assert!(quantity >= Decimal::ZERO);

            if quantity.is_zero() {
                book.remove(&price);
            } else {
                book.insert(price, quantity);
            }
        }
    }
}

// ============================================================
// COMPLETE AUTHORITATIVE BOOK COMPARISON
// ============================================================

fn compare_side(
    expected: &BTreeMap<Decimal, Decimal>,
    actual: &BTreeMap<Decimal, marketforge_engine::book::BookLevel>,
    inverse: bool,
    side: &str,
) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{side}: unexpected number of levels"
    );

    for (price, expected_quantity) in expected {
        let level = actual
            .get(price)
            .unwrap_or_else(|| panic!("{side}: missing native price {price}"));

        let native_quantity = if inverse {
            level.quantity_contracts
        } else {
            level.quantity_base
        };

        assert_eq!(
            native_quantity,
            Some(*expected_quantity),
            "{side}: incorrect native quantity at {price}"
        );
    }
}

fn compare_book(ingress: &BoundDepthIngress, expected: &ReferenceBook, profile: &str) {
    assert!(ingress.is_synchronized());

    let book = ingress.controller().book();
    assert!(book.is_initialized());

    let inverse = profile == "bybit_inverse";

    compare_side(&expected.bids, book.bids(), inverse, "bids");

    compare_side(&expected.asks, book.asks(), inverse, "asks");
}

// ============================================================
// REAL END-TO-END VERIFICATION
// ============================================================

#[test]
fn bybit_derivatives_match_independent_reference() {
    for profile in ["bybit_linear", "bybit_inverse"] {
        let native_messages = fixtures(profile);
        assert_eq!(native_messages.len(), 12);

        let decoder = decoder(profile);
        let context = context(profile);
        let mut ingress = ingress(profile);
        let mut reference = ReferenceBook::default();

        let mut previous_update_id = None;

        for (index, native) in native_messages.iter().enumerate() {
            let normalized = decoder
                .decode_websocket(native, &context)
                .unwrap_or_else(|error| {
                    panic!(
                        "{profile} message {index}: \
                         normalization failed: {error:?}"
                    )
                });

            let native_update_id = native["data"]["u"].as_u64().expect("Bybit update ID");

            let native_cross_sequence = native["data"]["seq"]
                .as_u64()
                .expect("Bybit cross-sequence");

            assert_eq!(
                normalized.sequence().sequence_last,
                Some(native_update_id),
                "{profile} message {index}"
            );

            assert_eq!(
                normalized.sequence().cross_sequence,
                Some(native_cross_sequence),
                "{profile} message {index}"
            );

            if let Some(previous) = previous_update_id {
                assert!(
                    native_update_id >= previous,
                    "{profile}: captured update ID regressed"
                );
            }

            let outcome = ingress.on_websocket(epoch(), normalized, 128 * 1024);

            assert!(
                matches!(outcome, DepthControllerOutcome::Accepted { .. }),
                "{profile} message {index}: \
                 Integrity rejected message: {outcome:?}"
            );

            reference.apply(native);

            compare_book(&ingress, &reference, profile);

            let baseline = ingress
                .controller()
                .baseline()
                .expect("accepted book baseline");

            assert_eq!(
                baseline.last_sequence, native_update_id,
                "{profile} message {index}"
            );

            assert_eq!(
                baseline.cross_sequence,
                Some(native_cross_sequence),
                "{profile} message {index}"
            );

            previous_update_id = Some(native_update_id);
        }

        println!(
            "{profile:<16} PASS messages={} bids={} asks={}",
            native_messages.len(),
            reference.bids.len(),
            reference.asks.len(),
        );
    }
}

// ============================================================
// SOURCE IDENTITY FENCING
// ============================================================

#[test]
fn bybit_derivatives_reject_foreign_native_symbols() {
    for profile in ["bybit_linear", "bybit_inverse"] {
        let mut messages = fixtures(profile);

        messages[0]["data"]["s"] = serde_json::json!("ETHUSDT");

        let result = decoder(profile).decode_websocket(&messages[0], &context(profile));

        assert!(
            result.is_err(),
            "{profile}: foreign instrument was normalized"
        );
    }
}
