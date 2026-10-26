use std::path::PathBuf;

use rust_decimal::Decimal;
use serde_json::{Value, json};

use marketforge_engine::{
    canonical::Exchange,
    live::{
        config::{
            exchange::{ExchangeConfig, ExchangeId, MarketType},
            format::DepthFormatConfig,
        },
        infrastructure::normalization::{
            DepthDecoder, NormalizationContext, QuantityPrecisionPolicy, RestDepthDecoder,
        },
        integrity::{
            BoundDepthIngress, DepthBufferLimits, DepthContinuityCompiler,
            DepthControllerIgnoreReason, DepthControllerOutcome, DepthResourceLimits,
            DepthSourceBinding, DepthSourceEpoch, DepthSyncToken, RestDepthRequestContext,
        },
        stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
    },
};

const STREAM: &str = "integration:binance:rest-sync";
const RECEIVE_NS: i64 = 1_792_900_000_000_000_000;

fn root() -> PathBuf {
    crate::support::data("")
}

fn format() -> DepthFormatConfig {
    DepthFormatConfig::load(root().join(".config/live/formats/depth/binance_spot.toml")).unwrap()
}

fn endpoint() -> Value {
    let text =
        std::fs::read_to_string(root().join(".config/live/exchanges/binance_spot.toml")).unwrap();

    let toml: toml::Value = toml::from_str(&text).unwrap();

    serde_json::to_value(&toml["rest"]["endpoints"]["depth_snapshot"]).unwrap()
}

fn epoch() -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: 10,
        connection_generation: 20,
        subscription_generation: 30,
    }
}

fn token(attempt: u64) -> DepthSyncToken {
    DepthSyncToken {
        stream_generation: 10,
        attempt,
    }
}
fn request(attempt: DepthSyncToken) -> RestDepthRequestContext {
    RestDepthRequestContext {
        source_epoch: epoch(),

        sync_token: attempt,

        endpoint_key: format()
            .synchronization
            .snapshot_endpoint
            .expect("Binance Spot requires a REST snapshot endpoint"),

        requested_limit: 100,
    }
}
fn context() -> NormalizationContext {
    let request = CreateSourceStreamRequest {
        instrument_id: 42,
        exchange: ExchangeId::Binance,
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

    NormalizationContext::new(&request, STREAM).unwrap()
}

fn ws_decoder() -> DepthDecoder {
    DepthDecoder::new_websocket(&format(), 1_000, QuantityPrecisionPolicy::Exact, None).unwrap()
}

fn rest_decoder() -> RestDepthDecoder {
    RestDepthDecoder::compile(
        &format(),
        &endpoint(),
        1_000,
        QuantityPrecisionPolicy::Exact,
    )
    .unwrap()
}

fn ingress() -> BoundDepthIngress {
    let binding = DepthSourceBinding::new(
        STREAM,
        Exchange::Binance,
        "spot",
        42,
        "BTCUSDT",
        None,
        epoch(),
    )
    .unwrap();

    let plan = DepthContinuityCompiler::compile(&format()).unwrap();

    let mut ingress = BoundDepthIngress::new(
        binding,
        plan,
        DepthBufferLimits {
            max_messages: 32,
            max_bytes: 1_000_000,
        },
        DepthResourceLimits::default(),
    )
    .unwrap();

    let exchange_config =
        ExchangeConfig::load(root().join(".config/live/exchanges/binance_spot.toml")).unwrap();

    ingress
        .configure_rest_endpoint(&exchange_config, &format())
        .unwrap();

    ingress
}

// ============================================================
// CONTROLLED NATIVE EXCHANGE RESPONSES
// ============================================================

fn native_snapshot(last_update_id: u64) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "lastUpdateId": last_update_id,
        "bids": [
            ["80000", "3"],
            ["79990", "1"]
        ],
        "asks": [
            ["80010", "2"]
        ]
    }))
    .unwrap()
}

fn native_delta(first: u64, last: u64, bids: Value, asks: Value) -> Value {
    json!({
        "e": "depthUpdate",
        "E": 1792900000000_i64,
        "s": "BTCUSDT",
        "U": first,
        "u": last,
        "b": bids,
        "a": asks
    })
}

fn delta_a() -> Value {
    native_delta(101, 102, json!([["80000", "5"]]), json!([["80020", "3"]]))
}

fn delta_b() -> Value {
    native_delta(103, 104, json!([["79990", "0"]]), json!([["80010", "4"]]))
}

fn decode_ws(
    native: &Value,
) -> marketforge_engine::live::infrastructure::normalization::NormalizedDepth {
    ws_decoder().decode_websocket(native, &context()).unwrap()
}

fn decode_rest(
    last_update_id: u64,
) -> marketforge_engine::live::infrastructure::normalization::NormalizedDepth {
    rest_decoder()
        .decode_bytes(
            &native_snapshot(last_update_id),
            &context(),
            RECEIVE_NS,
            100,
        )
        .unwrap()
}

// ============================================================
// AUTHORITATIVE BOOK ASSERTIONS
// ============================================================

fn quantity(ingress: &BoundDepthIngress, bid: bool, price: i64) -> Option<Decimal> {
    let book = ingress.controller().book();

    let price = Decimal::from(price);

    let level = if bid {
        book.bids().get(&price)
    } else {
        book.asks().get(&price)
    };

    level.and_then(|level| level.quantity_base)
}

fn assert_not_authoritative(ingress: &BoundDepthIngress) {
    assert!(!ingress.is_synchronized());

    let controller = ingress.controller();

    assert_eq!(controller.baseline(), None);

    let book = controller.book();

    assert!(!book.is_initialized());
    assert!(book.bids().is_empty());
    assert!(book.asks().is_empty());
}

fn assert_final_book(ingress: &BoundDepthIngress) {
    assert!(ingress.is_synchronized());

    let controller = ingress.controller();
    let book = controller.book();

    assert!(book.is_initialized());

    assert_eq!(book.bids().len(), 1);
    assert_eq!(book.asks().len(), 2);

    assert_eq!(quantity(ingress, true, 80000), Some(Decimal::from(5)),);

    assert_eq!(quantity(ingress, true, 79990), None,);

    assert_eq!(quantity(ingress, false, 80010), Some(Decimal::from(4)),);

    assert_eq!(quantity(ingress, false, 80020), Some(Decimal::from(3)),);

    assert_eq!(controller.baseline().unwrap().last_sequence, 104,);
}

// ============================================================
// SUCCESSFUL REST SNAPSHOT BRIDGE
// ============================================================

#[test]
fn real_decoders_bridge_binance_rest_snapshot_atomically() {
    let mut ingress = ingress();
    assert_eq!(ingress.accepted_rest_requested_limit(), None);
    let attempt = token(1);

    ingress.begin_rest_attempt(request(attempt)).unwrap();

    // Buffer both WebSocket messages before REST completion.
    for native in [delta_a(), delta_b()] {
        let message = decode_ws(&native);

        let outcome = ingress.on_websocket(epoch(), message, 2048);

        assert!(
            matches!(
                outcome,
                DepthControllerOutcome::Ignored {
                    reason: DepthControllerIgnoreReason::AwaitingSnapshot
                }
            ),
            "unexpected buffering outcome: {outcome:?}",
        );

        assert_not_authoritative(&ingress);
    }

    // Decode a real native REST response shape.
    let snapshot = decode_rest(100);

    assert_eq!(snapshot.sequence().sequence_last, Some(100),);

    let outcome = ingress.on_rest_snapshot(epoch(), attempt, &snapshot);

    assert!(
        matches!(
            outcome,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ),
        "REST bridge failed: {outcome:?}",
    );

    assert_final_book(&ingress);
    // The accepted REST request determines requested coverage.
    assert_eq!(ingress.accepted_rest_requested_limit(), Some(100));

    // Snapshot provenance remains available independently.
    let coverage = ingress.controller().coverage();

    assert!(coverage.synchronized);
    assert_eq!(
        coverage.snapshot_source,
        Some(marketforge_engine::live::infrastructure::normalization::NormalizationSource::Rest),
    );

    // The reconstructed book contains fewer levels than requested.
    assert_eq!(coverage.retained_bid_levels, 1);
    assert_eq!(coverage.retained_ask_levels, 2);
}

// ============================================================
// INVALID INITIAL SEQUENCE BRIDGE
// ============================================================

#[test]
fn missing_initial_update_requires_recovery() {
    let mut ingress = ingress();

    let attempt = token(1);

    ingress.begin_rest_attempt(request(attempt)).unwrap();

    // Snapshot ends at 100.
    // First buffered update starts at 105.
    // Updates 101..104 cannot be reconstructed.
    let bad = native_delta(105, 106, json!([["80000", "5"]]), json!([]));

    let outcome = ingress.on_websocket(epoch(), decode_ws(&bad), 2048);

    assert!(matches!(
        outcome,
        DepthControllerOutcome::Ignored {
            reason: DepthControllerIgnoreReason::AwaitingSnapshot
        }
    ));

    let snapshot = decode_rest(100);

    let outcome = ingress.on_rest_snapshot(epoch(), attempt, &snapshot);

    assert!(
        matches!(outcome, DepthControllerOutcome::RecoveryRequired { .. }),
        "missing update was accepted: {outcome:?}",
    );

    assert_not_authoritative(&ingress);
    // Failed synchronization cannot establish accepted REST coverage.
    assert_eq!(ingress.accepted_rest_requested_limit(), None);
}

// ============================================================
// OBSOLETE REST ATTEMPT
// ============================================================

#[test]
fn obsolete_rest_response_cannot_replace_current_attempt() {
    let mut ingress = ingress();

    let old = token(1);
    let current = token(2);

    ingress.begin_rest_attempt(request(old)).unwrap();

    // A newer recovery attempt supersedes the old one.
    ingress.begin_rest_attempt(request(current)).unwrap();

    let stale_snapshot = decode_rest(100);

    let outcome = ingress.on_rest_snapshot(epoch(), old, &stale_snapshot);

    assert!(
        matches!(
            outcome,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::ObsoleteGeneration
            }
        ),
        "obsolete REST response was admitted: {outcome:?}",
    );

    assert_not_authoritative(&ingress);
    // Only the current accepted attempt establishes coverage.
    // The obsolete response must not establish coverage.
    assert_eq!(ingress.accepted_rest_requested_limit(), None);
    // The current attempt must remain usable.
    for native in [delta_a(), delta_b()] {
        let outcome = ingress.on_websocket(epoch(), decode_ws(&native), 2048);

        assert!(matches!(
            outcome,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::AwaitingSnapshot
            }
        ));
    }

    let outcome = ingress.on_rest_snapshot(epoch(), current, &decode_rest(100));

    assert!(
        matches!(outcome, DepthControllerOutcome::Accepted { .. }),
        "current attempt was damaged by stale response: \
         {outcome:?}",
    );

    assert_final_book(&ingress);

    // Only the current accepted REST attempt establishes coverage.
    assert_eq!(ingress.accepted_rest_requested_limit(), Some(100));
}
