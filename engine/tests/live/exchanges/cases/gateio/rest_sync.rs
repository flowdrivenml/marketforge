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
            DepthDecoder, NormalizationContext, NormalizedDepth, QuantityPrecisionPolicy,
            RestDepthDecoder,
        },
        integrity::{
            BoundDepthIngress, DepthBufferLimits, DepthContinuityCompiler,
            DepthControllerIgnoreReason, DepthControllerOutcome, DepthResourceLimits,
            DepthSourceBinding, DepthSourceEpoch, DepthSyncToken, RestDepthRequestContext,
        },
        stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
    },
};

const STREAM: &str = "integration:gateio:rest-sync";
const RECEIVE_NS: i64 = 1_792_900_000_000_000_000;

const PROFILES: [&str; 3] = ["gateio_spot", "gateio_linear", "gateio_inverse"];

// ============================================================
// CONFIGURATION
// ============================================================

fn root() -> PathBuf {
    crate::support::data("")
}

fn depth_format(profile: &str) -> DepthFormatConfig {
    DepthFormatConfig::load(
        root()
            .join(".config/live/formats/depth")
            .join(format!("{profile}.toml")),
    )
    .unwrap()
}

fn exchange_config(profile: &str) -> ExchangeConfig {
    ExchangeConfig::load(
        root()
            .join(".config/live/exchanges")
            .join(format!("{profile}.toml")),
    )
    .unwrap()
}

fn market(profile: &str) -> MarketType {
    match profile {
        "gateio_spot" => MarketType::Spot,
        "gateio_linear" => MarketType::Linear,
        "gateio_inverse" => MarketType::Inverse,
        _ => panic!("unsupported Gate.io profile"),
    }
}

fn symbol(profile: &str) -> &'static str {
    match profile {
        "gateio_spot" | "gateio_linear" => "BTC_USDT",
        "gateio_inverse" => "BTC_USD",
        _ => panic!("unsupported Gate.io profile"),
    }
}

fn is_futures(profile: &str) -> bool {
    profile != "gateio_spot"
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

fn request(profile: &str, attempt: u64) -> RestDepthRequestContext {
    RestDepthRequestContext {
        source_epoch: epoch(),
        sync_token: token(attempt),

        endpoint_key: depth_format(profile)
            .synchronization
            .snapshot_endpoint
            .expect("REST endpoint required"),

        requested_limit: 100,
    }
}

// ============================================================
// INSTRUMENT CONTEXT
// ============================================================

fn context(profile: &str) -> NormalizationContext {
    let inverse = profile == "gateio_inverse";
    let futures = is_futures(profile);

    let request = CreateSourceStreamRequest {
        instrument_id: 42,

        exchange: ExchangeId::Gateio,
        symbol: symbol(profile).into(),
        market_category: market(profile),

        instrument_type: if futures {
            "perpetual".into()
        } else {
            "spot".into()
        },

        base_asset: Some("BTC".into()),

        quote_asset: Some(if inverse { "USD" } else { "USDT" }.into()),

        quantity_type: if futures {
            QuantityType::Contracts
        } else {
            QuantityType::Base
        },

        // Illustrative test metadata only.
        // Production obtains denomination from the catalog.
        contract_value: if futures { Some("1".into()) } else { None },

        contract_value_asset: if inverse {
            Some("USD".into())
        } else if futures {
            Some("BTC".into())
        } else {
            None
        },

        channel: SourceChannel::Depth,

        settlement_asset: Some(if inverse { "BTC" } else { "USDT" }.into()),
    };

    NormalizationContext::new(&request, STREAM).unwrap()
}

// ============================================================
// DECODERS
// ============================================================

fn ws_decoder(profile: &str) -> DepthDecoder {
    DepthDecoder::new_websocket(
        &depth_format(profile),
        1_000,
        QuantityPrecisionPolicy::Round { decimal_places: 18 },
        None,
    )
    .unwrap()
}

fn rest_decoder(profile: &str) -> RestDepthDecoder {
    let exchange = exchange_config(profile);

    let endpoint_name = depth_format(profile)
        .synchronization
        .snapshot_endpoint
        .expect("REST snapshot endpoint");

    RestDepthDecoder::compile(
        &depth_format(profile),
        exchange.endpoint(&endpoint_name).unwrap(),
        1_000,
        QuantityPrecisionPolicy::Round { decimal_places: 18 },
    )
    .unwrap()
}

// ============================================================
// BOUND INGRESS
// ============================================================

fn ingress(profile: &str) -> BoundDepthIngress {
    let binding = DepthSourceBinding::new(
        STREAM,
        Exchange::GateIo,
        market(profile).as_str(),
        42,
        symbol(profile),
        None,
        epoch(),
    )
    .unwrap();

    let mut ingress = BoundDepthIngress::new(
        binding,
        DepthContinuityCompiler::compile(&depth_format(profile)).unwrap(),
        DepthBufferLimits {
            max_messages: 32,
            max_bytes: 1_000_000,
        },
        DepthResourceLimits::default(),
    )
    .unwrap();

    ingress
        .configure_rest_endpoint(&exchange_config(profile), &depth_format(profile))
        .unwrap();

    ingress
}

// ============================================================
// NATIVE REST SNAPSHOTS
// ============================================================

fn native_snapshot(profile: &str, last_update_id: u64) -> Vec<u8> {
    let response = if is_futures(profile) {
        json!({
            "id": last_update_id,
            "bids": [
                {"p": "80000", "s": 3},
                {"p": "79990", "s": 1}
            ],
            "asks": [
                {"p": "80010", "s": 2}
            ]
        })
    } else {
        json!({
            "id": last_update_id,
            "bids": [
                ["80000", "3"],
                ["79990", "1"]
            ],
            "asks": [
                ["80010", "2"]
            ]
        })
    };

    serde_json::to_vec(&response).unwrap()
}

// ============================================================
// NATIVE WEBSOCKET DELTAS
// ============================================================

fn native_delta(
    profile: &str,
    first: u64,
    last: u64,
    bid_price: &str,
    bid_quantity: i64,
    ask_price: &str,
    ask_quantity: i64,
) -> Value {
    let (bids, asks) = if is_futures(profile) {
        (
            json!([
                {"p": bid_price, "s": bid_quantity}
            ]),
            json!([
                {"p": ask_price, "s": ask_quantity}
            ]),
        )
    } else {
        (
            json!([[bid_price, bid_quantity.to_string()]]),
            json!([[ask_price, ask_quantity.to_string()]]),
        )
    };

    json!({
        "time": 1792900000_i64,
        "time_ms": 1792900000000_i64,

        "channel": if is_futures(profile) {
            "futures.order_book_update"
        } else {
            "spot.order_book_update"
        },

        "event": "update",

        "result": {
            "t": 1792900000000_i64,
            "s": symbol(profile),
            "U": first,
            "u": last,
            "b": bids,
            "a": asks
        }
    })
}

fn first_delta(profile: &str) -> Value {
    native_delta(profile, 101, 102, "80000", 5, "80020", 3)
}

fn second_delta(profile: &str) -> Value {
    native_delta(profile, 103, 104, "79990", 0, "80010", 4)
}

fn decode_ws(profile: &str, native: &Value) -> NormalizedDepth {
    ws_decoder(profile)
        .decode_websocket(native, &context(profile))
        .unwrap()
}

fn decode_rest(profile: &str, last_update_id: u64) -> NormalizedDepth {
    rest_decoder(profile)
        .decode_bytes(
            &native_snapshot(profile, last_update_id),
            &context(profile),
            RECEIVE_NS,
            100,
        )
        .unwrap()
}

// ============================================================
// AUTHORITATIVE STATE ASSERTIONS
// ============================================================

fn assert_uninitialized(ingress: &BoundDepthIngress) {
    assert!(!ingress.is_synchronized());

    let controller = ingress.controller();

    assert_eq!(controller.baseline(), None);

    let book = controller.book();

    assert!(!book.is_initialized());
    assert!(book.bids().is_empty());
    assert!(book.asks().is_empty());
}

fn native_quantity(
    ingress: &BoundDepthIngress,
    profile: &str,
    bid: bool,
    price: i64,
) -> Option<Decimal> {
    let book = ingress.controller().book();

    let price = Decimal::from(price);

    let level = if bid {
        book.bids().get(&price)
    } else {
        book.asks().get(&price)
    }?;

    if is_futures(profile) {
        level.quantity_contracts
    } else {
        level.quantity_base
    }
}

fn assert_final_book(ingress: &BoundDepthIngress, profile: &str) {
    assert!(ingress.is_synchronized());

    let book = ingress.controller().book();

    assert!(book.is_initialized());

    assert_eq!(book.bids().len(), 1);
    assert_eq!(book.asks().len(), 2);

    assert_eq!(
        native_quantity(ingress, profile, true, 80000),
        Some(Decimal::from(5)),
    );

    assert_eq!(native_quantity(ingress, profile, true, 79990), None,);

    assert_eq!(
        native_quantity(ingress, profile, false, 80010),
        Some(Decimal::from(4)),
    );

    assert_eq!(
        native_quantity(ingress, profile, false, 80020),
        Some(Decimal::from(3)),
    );

    assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 104,);
}

fn buffer(ingress: &mut BoundDepthIngress, profile: &str, native: Value) {
    let normalized = decode_ws(profile, &native);

    let outcome = ingress.on_websocket(epoch(), normalized, 2048);

    assert!(
        matches!(
            outcome,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::AwaitingSnapshot
            }
        ),
        "{profile}: unexpected buffering outcome: {outcome:?}"
    );

    assert_uninitialized(ingress);
}

// ============================================================
// TEST 1 — SUCCESSFUL BRIDGE
// ============================================================

#[test]
fn gateio_all_markets_bridge_rest_snapshots() {
    for profile in PROFILES {
        let mut ingress = ingress(profile);

        ingress.begin_rest_attempt(request(profile, 1)).unwrap();

        buffer(&mut ingress, profile, first_delta(profile));
        buffer(&mut ingress, profile, second_delta(profile));

        let snapshot = decode_rest(profile, 100);

        assert_eq!(snapshot.sequence().sequence_last, Some(100),);

        let outcome = ingress.on_rest_snapshot(epoch(), token(1), &snapshot);

        assert!(
            matches!(
                outcome,
                DepthControllerOutcome::Accepted { snapshot: true, .. }
            ),
            "{profile}: REST bridge rejected: {outcome:?}"
        );

        assert_final_book(&ingress, profile);

        println!("{profile}: REST synchronization accepted");
    }
}

// ============================================================
// TEST 2 — MISSING INITIAL UPDATE
// ============================================================

#[test]
fn gateio_all_markets_reject_missing_initial_update() {
    for profile in PROFILES {
        let mut ingress = ingress(profile);

        ingress.begin_rest_attempt(request(profile, 1)).unwrap();

        // Snapshot ID is 100.
        // The next required update is 101.
        //
        // This update starts at 105 and cannot bridge.
        let bad = native_delta(profile, 105, 106, "80000", 5, "80020", 3);

        buffer(&mut ingress, profile, bad);

        let outcome = ingress.on_rest_snapshot(epoch(), token(1), &decode_rest(profile, 100));

        assert!(
            matches!(outcome, DepthControllerOutcome::RecoveryRequired { .. }),
            "{profile}: missing update was accepted: {outcome:?}"
        );

        assert_uninitialized(&ingress);

        println!("{profile}: missing initial update rejected");
    }
}

// ============================================================
// TEST 3 — STALE REST ATTEMPT
// ============================================================

#[test]
fn gateio_all_markets_fence_obsolete_rest_attempts() {
    for profile in PROFILES {
        let mut ingress = ingress(profile);

        ingress.begin_rest_attempt(request(profile, 1)).unwrap();

        ingress.begin_rest_attempt(request(profile, 2)).unwrap();

        let stale_snapshot = decode_rest(profile, 100);

        let outcome = ingress.on_rest_snapshot(epoch(), token(1), &stale_snapshot);

        assert!(
            matches!(
                outcome,
                DepthControllerOutcome::Ignored {
                    reason: DepthControllerIgnoreReason::ObsoleteGeneration
                }
            ),
            "{profile}: obsolete REST response accepted: {outcome:?}"
        );

        assert_uninitialized(&ingress);

        // Verify that the current attempt was not damaged.
        buffer(&mut ingress, profile, first_delta(profile));
        buffer(&mut ingress, profile, second_delta(profile));

        let outcome = ingress.on_rest_snapshot(epoch(), token(2), &decode_rest(profile, 100));

        assert!(
            matches!(outcome, DepthControllerOutcome::Accepted { .. }),
            "{profile}: current attempt failed: {outcome:?}"
        );

        assert_final_book(&ingress, profile);
    }
}
