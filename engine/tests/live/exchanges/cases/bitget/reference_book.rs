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
            DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch, DepthTimestampComposition,
        },
        stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
    },
};

const STREAM_ID: &str = "integration:bitget:depth";
const INSTRUMENT_ID: i64 = 42;

// ============================================================
// PROFILE AND FIXTURE HELPERS
// ============================================================

fn data_root() -> PathBuf {
    crate::support::data("")
}

fn format(profile: &str) -> DepthFormatConfig {
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
        "bitget_spot" => MarketType::Spot,
        "bitget_linear" => MarketType::Linear,
        "bitget_inverse" => MarketType::Inverse,
        _ => panic!("unsupported Bitget profile: {profile}"),
    }
}

fn symbol(profile: &str) -> &'static str {
    match profile {
        "bitget_spot" | "bitget_linear" => "BTCUSDT",
        "bitget_inverse" => "BTCUSD_CM",
        _ => panic!("unsupported Bitget profile: {profile}"),
    }
}

fn epoch() -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: 10,
        connection_generation: 20,
        subscription_generation: 30,
    }
}

// ============================================================
// NORMALIZATION CONTEXT
// ============================================================

fn context(profile: &str) -> NormalizationContext {
    let market = market(profile);
    let inverse = market == MarketType::Inverse;

    // Illustrative test instrument metadata.
    // Actual contract specifications must come from the
    // authoritative instrument catalog.
    let request = CreateSourceStreamRequest {
        instrument_id: INSTRUMENT_ID,
        exchange: ExchangeId::Bitget,
        symbol: symbol(profile).into(),
        market_category: market.clone(),

        instrument_type: if market == MarketType::Spot {
            "spot".into()
        } else {
            "perpetual".into()
        },

        base_asset: Some("BTC".into()),
        quote_asset: Some(if inverse { "USD" } else { "USDT" }.into()),

        quantity_type: if inverse {
            QuantityType::Contracts
        } else {
            QuantityType::Base
        },

        contract_value: if inverse { Some("1".into()) } else { None },

        contract_value_asset: if inverse { Some("USD".into()) } else { None },

        channel: SourceChannel::Depth,

        settlement_asset: Some(if inverse { "BTC" } else { "USDT" }.into()),
    };

    NormalizationContext::new(&request, STREAM_ID).unwrap()
}

fn decoder(profile: &str) -> DepthDecoder {
    DepthDecoder::new_websocket(
        &format(profile),
        20_000,
        QuantityPrecisionPolicy::Round { decimal_places: 18 },
        None,
    )
    .unwrap()
}

fn ingress(profile: &str) -> BoundDepthIngress {
    let binding = DepthSourceBinding::new(
        STREAM_ID,
        Exchange::Bitget,
        market(profile).as_str(),
        INSTRUMENT_ID,
        symbol(profile),
        None,
        epoch(),
    )
    .unwrap();

    let plan = DepthContinuityCompiler::compile(&format(profile)).unwrap();

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

#[derive(Debug, Default)]
struct ReferenceBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl ReferenceBook {
    fn apply(&mut self, message: &Value) {
        let action = message["action"].as_str().expect("Bitget action");

        match action {
            "snapshot" => {
                self.bids.clear();
                self.asks.clear();
            }

            "update" => {}

            other => panic!("unexpected Bitget action: {other}"),
        }

        let records = message["data"].as_array().expect("Bitget records");

        assert_eq!(
            records.len(),
            1,
            "Bitget depth message must contain one record"
        );

        let record = &records[0];

        Self::apply_side(&mut self.bids, &record["b"]);
        Self::apply_side(&mut self.asks, &record["a"]);
    }

    fn apply_side(book: &mut BTreeMap<Decimal, Decimal>, side: &Value) {
        let levels = side.as_array().expect("Bitget levels");

        for level in levels {
            let items = level.as_array().expect("Bitget level");

            let price = Decimal::from_str_exact(items[0].as_str().expect("price string")).unwrap();

            let quantity =
                Decimal::from_str_exact(items[1].as_str().expect("quantity string")).unwrap();

            assert!(price > Decimal::ZERO);
            assert!(quantity >= Decimal::ZERO);

            if quantity == Decimal::ZERO {
                book.remove(&price);
            } else {
                book.insert(price, quantity);
            }
        }
    }
}

// ============================================================
// AUTHORITATIVE BOOK COMPARISON
// ============================================================

fn compare_side(
    expected: &BTreeMap<Decimal, Decimal>,
    actual: &BTreeMap<Decimal, marketforge_engine::book::BookLevel>,
    contracts: bool,
    label: &str,
) {
    assert_eq!(
        actual.len(),
        expected.len(),
        "{label}: unexpected level count"
    );

    for (price, expected_quantity) in expected {
        let actual_level = actual
            .get(price)
            .unwrap_or_else(|| panic!("{label}: missing price {price}"));

        let native_quantity = if contracts {
            actual_level.quantity_contracts
        } else {
            actual_level.quantity_base
        };

        assert_eq!(
            native_quantity,
            Some(*expected_quantity),
            "{label}: incorrect quantity at price {price}"
        );
    }
}

fn compare_book(ingress: &BoundDepthIngress, reference: &ReferenceBook, profile: &str) {
    assert!(ingress.is_synchronized());

    let authoritative = ingress.controller().book();

    assert!(authoritative.is_initialized());

    let contracts = profile == "bitget_inverse";

    compare_side(&reference.bids, authoritative.bids(), contracts, "bids");

    compare_side(&reference.asks, authoritative.asks(), contracts, "asks");
}

// ============================================================
// REAL BITGET SNAPSHOT BRIDGING
// ============================================================

#[test]
fn bitget_all_markets_match_independent_reference() {
    for profile in ["bitget_spot", "bitget_linear", "bitget_inverse"] {
        let messages = fixtures(profile);

        assert_eq!(messages.len(), 12);

        let decoder = decoder(profile);
        let context = context(profile);

        let mut ingress = ingress(profile);
        let mut reference = ReferenceBook::default();

        let mut staged = 0;
        let mut accepted = 0;

        for (index, native) in messages.iter().enumerate() {
            let normalized = decoder
                .decode_websocket(native, &context)
                .unwrap_or_else(|error| {
                    panic!(
                        "{profile} message {index}: \
                         normalization failed: {error:?}"
                    )
                });

            let outcome = ingress.on_websocket(epoch(), normalized, 128 * 1024);

            if index == 0 {
                // --------------------------------------------
                // INITIAL SNAPSHOT MUST REMAIN PRIVATE
                // --------------------------------------------

                let snapshot_sequence = native["data"][0]["seq"]
                    .as_u64()
                    .expect("snapshot sequence");

                assert!(
                    matches!(
                        outcome,
                        DepthControllerOutcome::SnapshotStaged {
                            snapshot_sequence: actual
                        } if actual == snapshot_sequence
                    ),
                    "{profile}: initial snapshot was not staged: \
                     {outcome:?}"
                );

                reference.apply(native);
                staged += 1;

                assert!(!ingress.is_synchronized());

                let controller = ingress.controller();

                assert_eq!(controller.baseline(), None);
                assert_eq!(controller.book_generation(), 0);

                let book = controller.book();

                assert!(!book.is_initialized());
                assert!(book.bids().is_empty());
                assert!(book.asks().is_empty());

                continue;
            }

            if index == 1 {
                // --------------------------------------------
                // VERIFY NATIVE FIRST-DELTA BRIDGE
                // --------------------------------------------

                let snapshot_sequence = messages[0]["data"][0]["seq"].as_u64().unwrap();

                let previous = native["data"][0]["pseq"].as_u64().unwrap();

                let current = native["data"][0]["seq"].as_u64().unwrap();

                assert!(
                    previous <= snapshot_sequence && snapshot_sequence <= current,
                    "{profile}: captured first delta does not \
                     bridge the staged snapshot"
                );
            }

            // -----------------------------------------------
            // ACCEPT DELTA
            // -----------------------------------------------

            assert!(
                matches!(outcome, DepthControllerOutcome::Accepted { .. }),
                "{profile} message {index}: {outcome:?}"
            );
            if index == 1 {
                let timestamp = match &outcome {
                    DepthControllerOutcome::Accepted {
                        timestamp,
                        snapshot: true,
                        ..
                    } => *timestamp,

                    other => panic!("{profile}: expected Bitget bridge snapshot: {other:?}"),
                };

                assert_eq!(
                    timestamp.composition,
                    DepthTimestampComposition::StagedSnapshotBridge,
                    "{profile}",
                );
            }
            accepted += 1;

            // Reconstruct independently from native JSON.
            reference.apply(native);

            // Verify exact authoritative state after each
            // accepted delta.
            compare_book(&ingress, &reference, profile);

            let native_sequence = native["data"][0]["seq"].as_u64().unwrap();

            assert_eq!(
                ingress.controller().baseline().unwrap().last_sequence,
                native_sequence,
                "{profile}: sequence baseline mismatch"
            );
        }

        assert_eq!(staged, 1);
        assert_eq!(accepted, 11);
        assert!(ingress.is_synchronized());

        println!(
            "{profile:<18} PASS staged={staged} \
             accepted={accepted} bids={} asks={}",
            reference.bids.len(),
            reference.asks.len(),
        );
    }
}

// ============================================================
// INVALID FIRST-DELTA BRIDGE
// ============================================================

#[test]
fn invalid_bitget_bridge_never_exposes_candidate_book() {
    for profile in ["bitget_spot", "bitget_linear", "bitget_inverse"] {
        let messages = fixtures(profile);

        let decoder = decoder(profile);
        let context = context(profile);
        let mut ingress = ingress(profile);

        // -----------------------------------------------
        // STAGE REAL SNAPSHOT
        // -----------------------------------------------

        let snapshot = decoder.decode_websocket(&messages[0], &context).unwrap();

        let outcome = ingress.on_websocket(epoch(), snapshot, 128 * 1024);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::SnapshotStaged { .. }
        ));

        // -----------------------------------------------
        // BREAK THE FIRST-DELTA BRIDGE
        // -----------------------------------------------

        let snapshot_sequence = messages[0]["data"][0]["seq"].as_u64().unwrap();

        let mut bad_delta = messages[1].clone();

        bad_delta["data"][0]["pseq"] = Value::from(snapshot_sequence + 1);

        // Keep the delta's current sequence at or above
        // the injected previous sequence. This isolates
        // the bridge violation rather than producing an
        // internally inverted sequence range.
        bad_delta["data"][0]["seq"] = Value::from(snapshot_sequence + 2);

        let normalized = decoder.decode_websocket(&bad_delta, &context).unwrap();

        let outcome = ingress.on_websocket(epoch(), normalized, 128 * 1024);

        assert!(
            matches!(outcome, DepthControllerOutcome::RecoveryRequired { .. }),
            "{profile}: invalid bridge was not rejected: \
             {outcome:?}"
        );

        // The staged candidate must not leak into
        // authoritative state after failure.
        assert!(!ingress.is_synchronized());

        let controller = ingress.controller();

        assert_eq!(controller.baseline(), None);

        let book = controller.book();

        assert!(!book.is_initialized());
        assert!(book.bids().is_empty());
        assert!(book.asks().is_empty());

        println!("{profile}: invalid bridge rejected safely");
    }
}
