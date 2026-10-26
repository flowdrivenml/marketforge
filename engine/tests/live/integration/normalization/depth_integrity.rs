use std::path::PathBuf;

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

use serde_json::Value;

// ============================================================
// TEST SOURCE IDENTITY
// ============================================================

const STREAM_ID: &str = "integration:bybit_spot:BTCUSDT";
const INSTRUMENT_ID: i64 = 42;

fn data_root() -> PathBuf {
    crate::support::data("")
}

fn depth_format() -> DepthFormatConfig {
    DepthFormatConfig::load(data_root().join(".config/live/formats/depth/bybit_spot.toml")).unwrap()
}

fn websocket_messages() -> Vec<Value> {
    let path = data_root().join(".cache/live/websocket-fixtures/depth/bybit_spot.json");

    serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap()
}

fn epoch() -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: 10,
        connection_generation: 20,
        subscription_generation: 30,
    }
}

fn context() -> NormalizationContext {
    let request = CreateSourceStreamRequest {
        instrument_id: INSTRUMENT_ID,
        exchange: ExchangeId::Bybit,
        symbol: "BTCUSDT".into(),
        market_category: MarketType::Spot,
        instrument_type: "spot".into(),

        base_asset: Some("BTC".into()),
        quote_asset: Some("USDT".into()),

        quantity_type: QuantityType::Base,
        contract_value: None,
        contract_value_asset: None,

        channel: SourceChannel::Depth,
        settlement_asset: Some("USDT".into()),
    };

    NormalizationContext::new(&request, STREAM_ID).unwrap()
}

fn decoder() -> DepthDecoder {
    DepthDecoder::new_websocket(
        &depth_format(),
        20_000,
        QuantityPrecisionPolicy::Exact,
        Some(200),
    )
    .unwrap()
}

fn ingress() -> BoundDepthIngress {
    let binding = DepthSourceBinding::new(
        STREAM_ID,
        Exchange::Bybit,
        "spot",
        INSTRUMENT_ID,
        "BTCUSDT",
        Some(200),
        epoch(),
    )
    .unwrap();

    let plan = DepthContinuityCompiler::compile(&depth_format()).unwrap();

    BoundDepthIngress::new(
        binding,
        plan,
        DepthBufferLimits {
            max_messages: 100,
            max_bytes: 10_000_000,
        },
        DepthResourceLimits::default(),
    )
    .unwrap()
}

// ============================================================
// REAL SNAPSHOT → AUTHORITATIVE BOOK
// ============================================================

#[test]
fn real_bybit_snapshot_reaches_authoritative_book() {
    let messages = websocket_messages();

    let normalized = decoder()
        .decode_websocket(&messages[0], &context())
        .unwrap();

    let mut ingress = ingress();

    assert!(!ingress.is_synchronized());

    let outcome = ingress.on_websocket(epoch(), normalized.clone(), 64 * 1024);

    assert!(
        matches!(
            outcome,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ),
        "initial real snapshot rejected: {outcome:?}"
    );

    assert!(ingress.is_synchronized());

    assert_eq!(
        normalized.sequence().sequence_last,
        messages[0]["data"]["u"].as_u64(),
    );

    assert_eq!(
        normalized.sequence().cross_sequence,
        messages[0]["data"]["seq"].as_u64(),
    );
}

// ============================================================
// REAL SNAPSHOT + REAL DELTAS
// ============================================================

#[test]
fn real_bybit_sequence_advances_through_integrity() {
    let messages = websocket_messages();

    assert_eq!(messages.len(), 12);

    let decoder = decoder();
    let context = context();
    let mut ingress = ingress();

    let mut accepted = 0;

    for (index, native) in messages.iter().enumerate() {
        let normalized = decoder
            .decode_websocket(native, &context)
            .unwrap_or_else(|error| panic!("native message {index}: {error:?}"));

        let outcome = ingress.on_websocket(epoch(), normalized, 64 * 1024);

        match outcome {
            DepthControllerOutcome::Accepted { .. } => {
                accepted += 1;
            }

            other => {
                panic!("message {index} rejected by Integrity: {other:?}");
            }
        }
    }

    assert_eq!(accepted, 12);
    assert!(ingress.is_synchronized());
}

// ============================================================
// OBSOLETE SOURCE EPOCH
// ============================================================

#[test]
fn normalized_snapshot_from_obsolete_epoch_is_not_admitted() {
    let messages = websocket_messages();

    let normalized = decoder()
        .decode_websocket(&messages[0], &context())
        .unwrap();

    let mut ingress = ingress();

    let obsolete_epoch = DepthSourceEpoch {
        stream_generation: 9,
        connection_generation: 20,
        subscription_generation: 30,
    };

    let outcome = ingress.on_websocket(obsolete_epoch, normalized, 64 * 1024);

    assert!(
        !matches!(outcome, DepthControllerOutcome::Accepted { .. }),
        "obsolete source epoch was accepted"
    );

    assert!(!ingress.is_synchronized());
}

// ============================================================
// FOREIGN NATIVE INSTRUMENT
// ============================================================

#[test]
fn foreign_native_symbol_never_reaches_integrity() {
    let mut messages = websocket_messages();

    messages[0]["data"]["s"] = serde_json::json!("ETHUSDT");

    let result = decoder().decode_websocket(&messages[0], &context());

    assert!(result.is_err(), "foreign instrument passed normalization");

    // No canonical message exists to hand to Integrity.
    let ingress = ingress();

    assert!(!ingress.is_synchronized());
}
// ============================================================
// INDEPENDENT REFERENCE BOOK
// ============================================================

use rust_decimal::Decimal;
use std::collections::BTreeMap;

#[derive(Debug, Default)]
struct ReferenceBook {
    bids: BTreeMap<Decimal, Decimal>,
    asks: BTreeMap<Decimal, Decimal>,
}

impl ReferenceBook {
    fn apply_native(&mut self, message: &Value) {
        let kind = message["type"].as_str().expect("native message type");

        assert!(
            matches!(kind, "snapshot" | "delta"),
            "unexpected native message type"
        );

        if kind == "snapshot" {
            self.bids.clear();
            self.asks.clear();
        }

        let data = &message["data"];

        Self::apply_side(&mut self.bids, &data["b"]);
        Self::apply_side(&mut self.asks, &data["a"]);
    }

    fn apply_side(book: &mut BTreeMap<Decimal, Decimal>, native_levels: &Value) {
        let levels = native_levels
            .as_array()
            .expect("native side must be an array");

        for level in levels {
            let price =
                Decimal::from_str_exact(level[0].as_str().expect("native price string")).unwrap();

            let quantity =
                Decimal::from_str_exact(level[1].as_str().expect("native quantity string"))
                    .unwrap();

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
// ASSERT COMPLETE AUTHORITATIVE BOOK EQUALITY
// ============================================================

fn assert_book_matches_reference(
    ingress: &BoundDepthIngress,
    reference: &ReferenceBook,
    message_index: usize,
) {
    let authoritative = ingress.controller().book();

    assert!(
        authoritative.is_initialized(),
        "message {message_index}: authoritative book not initialized"
    );

    assert_eq!(
        authoritative.bids().len(),
        reference.bids.len(),
        "message {message_index}: bid level count mismatch"
    );

    assert_eq!(
        authoritative.asks().len(),
        reference.asks.len(),
        "message {message_index}: ask level count mismatch"
    );

    for (price, expected_quantity) in &reference.bids {
        let actual = authoritative
            .bids()
            .get(price)
            .unwrap_or_else(|| panic!("message {message_index}: missing bid at {price}"));

        assert_eq!(
            actual.quantity_base,
            Some(*expected_quantity),
            "message {message_index}: incorrect bid quantity at {price}"
        );
    }

    for (price, expected_quantity) in &reference.asks {
        let actual = authoritative
            .asks()
            .get(price)
            .unwrap_or_else(|| panic!("message {message_index}: missing ask at {price}"));

        assert_eq!(
            actual.quantity_base,
            Some(*expected_quantity),
            "message {message_index}: incorrect ask quantity at {price}"
        );
    }

    // Verify that the authoritative state contains no
    // additional price levels absent from the reference.
    //
    // The count assertions above establish this once
    // every reference price has been found.
}

// ============================================================
// REAL END-TO-END BOOK RECONSTRUCTION
// ============================================================

#[test]
fn real_bybit_book_matches_independent_reference_after_every_message() {
    let messages = websocket_messages();

    assert_eq!(messages.len(), 12);

    let decoder = decoder();
    let context = context();
    let mut ingress = ingress();

    let mut reference = ReferenceBook::default();

    for (index, native) in messages.iter().enumerate() {
        // ----------------------------------------------------
        // INDEPENDENT NATIVE RECONSTRUCTION
        // ----------------------------------------------------

        reference.apply_native(native);

        // ----------------------------------------------------
        // MARKETFORGE NORMALIZATION
        // ----------------------------------------------------

        let normalized = decoder
            .decode_websocket(native, &context)
            .unwrap_or_else(|error| panic!("message {index}: normalization failed: {error:?}"));

        // ----------------------------------------------------
        // MARKETFORGE INTEGRITY
        // ----------------------------------------------------

        let outcome = ingress.on_websocket(epoch(), normalized, 64 * 1024);

        assert!(
            matches!(outcome, DepthControllerOutcome::Accepted { .. }),
            "message {index}: Integrity rejected message: {outcome:?}"
        );

        assert!(
            ingress.is_synchronized(),
            "message {index}: synchronization lost"
        );

        // ----------------------------------------------------
        // COMPARE COMPLETE BOOKS
        // ----------------------------------------------------

        assert_book_matches_reference(&ingress, &reference, index);

        println!(
            "message={index:02} \
             bids={} asks={} \
             sequence={} \
             BOOK_MATCH",
            reference.bids.len(),
            reference.asks.len(),
            native["data"]["u"],
        );
    }

    println!(
        "\nPASS: All 12 native Bybit messages produced \
         exactly matching authoritative books."
    );
}
