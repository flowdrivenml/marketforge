use std::path::PathBuf;

use marketforge_engine::live::infrastructure::normalization::DepthTimestampProvenance;
use marketforge_engine::{
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
            DepthControllerIgnoreReason, DepthControllerOutcome, DepthPublicationEvent,
            DepthPublicationFence, DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch,
            DepthSyncToken, DepthTimestampComposition, RestDepthRequestContext,
        },
        stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
    },
    shared::domain::canonical::Exchange,
};
use rust_decimal::Decimal;
use serde_json::{Value, json};

const STREAM: &str = "integration:binance:futures";
const RECEIVE_NS: i64 = 1_792_900_000_000_000_000;

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

fn profile_market(profile: &str) -> MarketType {
    match profile {
        "binance_linear" => MarketType::Linear,
        "binance_inverse" => MarketType::Inverse,
        _ => panic!("unsupported profile: {profile}"),
    }
}

fn symbol(profile: &str) -> &'static str {
    match profile {
        "binance_linear" => "BTCUSDT",
        "binance_inverse" => "BTCUSD_PERP",
        _ => panic!("unsupported profile: {profile}"),
    }
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
            .expect("REST snapshot endpoint required"),

        requested_limit: 100,
    }
}

fn context(profile: &str) -> NormalizationContext {
    let inverse = profile == "binance_inverse";

    let request = CreateSourceStreamRequest {
        instrument_id: 42,
        exchange: ExchangeId::Binance,
        symbol: symbol(profile).into(),
        market_category: profile_market(profile),
        instrument_type: "perpetual".into(),

        base_asset: Some("BTC".into()),
        quote_asset: Some(if inverse { "USD" } else { "USDT" }.into()),

        quantity_type: if inverse {
            QuantityType::Contracts
        } else {
            QuantityType::Base
        },

        // Test-only denomination. Production must resolve
        // this from the authoritative instrument catalog.
        contract_value: if inverse { Some("1".into()) } else { None },

        contract_value_asset: if inverse { Some("USD".into()) } else { None },

        channel: SourceChannel::Depth,

        settlement_asset: Some(if inverse { "BTC" } else { "USDT" }.into()),
    };

    NormalizationContext::new(&request, STREAM).unwrap()
}

fn websocket_decoder(profile: &str) -> DepthDecoder {
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
        .expect("snapshot endpoint");

    RestDepthDecoder::compile(
        &depth_format(profile),
        exchange.endpoint(&endpoint_name).unwrap(),
        1_000,
        QuantityPrecisionPolicy::Round { decimal_places: 18 },
    )
    .unwrap()
}

fn ingress(profile: &str) -> BoundDepthIngress {
    let binding = DepthSourceBinding::new(
        STREAM,
        Exchange::Binance,
        profile_market(profile).as_str(),
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
// NATIVE MESSAGE CONSTRUCTION
// ============================================================

fn native_snapshot(last_update_id: u64) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "lastUpdateId": last_update_id,
        "E": 1792900000000_i64,
        "T": 1792900000000_i64,
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

fn native_delta(
    profile: &str,
    first: u64,
    last: u64,
    previous: u64,
    bids: Value,
    asks: Value,
) -> Value {
    let mut message = json!({
        "e": "depthUpdate",
        "E": 1792900000000_i64,
        "T": 1792900000000_i64,
        "s": symbol(profile),
        "U": first,
        "u": last,
        "pu": previous,
        "b": bids,
        "a": asks
    });

    if profile == "binance_inverse" {
        message["ps"] = json!("BTCUSD");
    }

    message
}

fn first_delta(profile: &str) -> Value {
    native_delta(
        profile,
        101,
        102,
        100,
        json!([["80000", "5"]]),
        json!([["80020", "3"]]),
    )
}

fn second_delta(profile: &str, previous: u64) -> Value {
    native_delta(
        profile,
        103,
        104,
        previous,
        json!([["79990", "0"]]),
        json!([["80010", "4"]]),
    )
}

fn decode_websocket(
    profile: &str,
    native: &Value,
) -> marketforge_engine::live::infrastructure::normalization::NormalizedDepth {
    websocket_decoder(profile)
        .decode_websocket(native, &context(profile))
        .unwrap()
}

fn decode_rest(
    profile: &str,
    last_update_id: u64,
) -> marketforge_engine::live::infrastructure::normalization::NormalizedDepth {
    rest_decoder(profile)
        .decode_bytes(
            &native_snapshot(last_update_id),
            &context(profile),
            RECEIVE_NS,
            100,
        )
        .unwrap()
}

// ============================================================
// BOOK ASSERTIONS
// ============================================================

fn assert_uninitialized(ingress: &BoundDepthIngress) {
    assert!(!ingress.is_synchronized());

    let controller = ingress.controller();

    assert_eq!(controller.baseline(), None);
    assert!(!controller.book().is_initialized());

    assert!(controller.book().bids().is_empty());
    assert!(controller.book().asks().is_empty());
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

    if profile == "binance_inverse" {
        level.quantity_contracts
    } else {
        level.quantity_base
    }
}

fn assert_final_book(ingress: &BoundDepthIngress, profile: &str) {
    assert!(ingress.is_synchronized());

    let book = ingress.controller().book();

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
    let normalized = decode_websocket(profile, &native);

    let outcome = ingress.on_websocket(epoch(), normalized, 2048);

    assert!(
        matches!(
            outcome,
            DepthControllerOutcome::Ignored {
                reason: DepthControllerIgnoreReason::AwaitingSnapshot
            }
        ),
        "{profile}: unexpected buffer outcome: {outcome:?}"
    );

    assert_uninitialized(ingress);
}

// ============================================================
// SUCCESSFUL BRIDGE
// ============================================================

#[test]
fn futures_rest_snapshot_bridges_previous_update_chain() {
    for profile in ["binance_linear", "binance_inverse"] {
        let mut ingress = ingress(profile);

        ingress.begin_rest_attempt(request(profile, 1)).unwrap();

        buffer(&mut ingress, profile, first_delta(profile));
        buffer(&mut ingress, profile, second_delta(profile, 102));

        let snapshot = decode_rest(profile, 100);

        let outcome = ingress.on_rest_snapshot(epoch(), token(1), &snapshot);

        assert!(
            matches!(
                outcome,
                DepthControllerOutcome::Accepted { snapshot: true, .. }
            ),
            "{profile}: REST bridge rejected: {outcome:?}"
        );

        assert_final_book(&ingress, profile);

        println!("{profile}: valid previous-update chain accepted");
    }
}

// ============================================================
// BROKEN PREVIOUS-UPDATE LINK
// ============================================================

#[test]
fn futures_rest_bridge_rejects_broken_previous_update_chain() {
    for profile in ["binance_linear", "binance_inverse"] {
        let mut ingress = ingress(profile);

        ingress.begin_rest_attempt(request(profile, 1)).unwrap();

        buffer(&mut ingress, profile, first_delta(profile));

        // The previous accepted update ends at 102.
        // This delta incorrectly declares pu=99.
        buffer(&mut ingress, profile, second_delta(profile, 99));

        let outcome = ingress.on_rest_snapshot(epoch(), token(1), &decode_rest(profile, 100));

        assert!(
            matches!(outcome, DepthControllerOutcome::RecoveryRequired { .. }),
            "{profile}: broken pu chain was accepted: {outcome:?}"
        );

        assert_uninitialized(&ingress);

        println!("{profile}: broken previous-update chain rejected");
    }
}

// ============================================================
// OBSOLETE REST ATTEMPT
// ============================================================

#[test]
fn futures_obsolete_rest_response_cannot_override_new_attempt() {
    for profile in ["binance_linear", "binance_inverse"] {
        let mut ingress = ingress(profile);

        ingress.begin_rest_attempt(request(profile, 1)).unwrap();
        ingress.begin_rest_attempt(request(profile, 2)).unwrap();

        let old_snapshot = decode_rest(profile, 100);

        let outcome = ingress.on_rest_snapshot(epoch(), token(1), &old_snapshot);

        assert!(
            matches!(
                outcome,
                DepthControllerOutcome::Ignored {
                    reason: DepthControllerIgnoreReason::ObsoleteGeneration
                }
            ),
            "{profile}: old REST attempt was admitted: {outcome:?}"
        );

        assert_uninitialized(&ingress);

        // The new attempt must remain functional.
        buffer(&mut ingress, profile, first_delta(profile));
        buffer(&mut ingress, profile, second_delta(profile, 102));

        let outcome = ingress.on_rest_snapshot(epoch(), token(2), &decode_rest(profile, 100));

        assert!(
            matches!(outcome, DepthControllerOutcome::Accepted { .. }),
            "{profile}: current attempt failed: {outcome:?}"
        );

        assert_final_book(&ingress, profile);
    }
}

#[test]
fn rest_timestamp_provenance_survives_publication() {
    for profile in ["binance_linear", "binance_inverse"] {
        let mut ingress = ingress(profile);

        let attempt = token(1);

        ingress.begin_rest_attempt(request(profile, 1)).unwrap();

        // Two valid, consecutive Binance Futures updates.
        let mut first = first_delta(profile);
        first["T"] = json!(1792900000100_i64);

        let mut second = second_delta(profile, 102);
        second["T"] = json!(1792900000200_i64);

        // Buffer both messages before REST synchronization.
        for native in [&first, &second] {
            let normalized = decode_websocket(profile, native);

            assert_eq!(
                normalized.timestamp_provenance(),
                Some(DepthTimestampProvenance::Exchange),
                "{profile}"
            );

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

            // Buffered messages are not yet authoritative.
            assert!(outcome.accepted_output().is_none());
            assert!(!ingress.is_synchronized());
        }

        // REST snapshot establishes sequence baseline 100.
        let snapshot = decode_rest(profile, 100);

        assert_eq!(
            snapshot.timestamp_provenance(),
            Some(DepthTimestampProvenance::ReceiveTimeFallback),
            "{profile}"
        );

        let outcome = ingress.on_rest_snapshot(epoch(), attempt, &snapshot);

        // Integrity must identify this as a reconstructed
        // order book, not an ordinary native snapshot.
        let accepted_timestamp = match &outcome {
            DepthControllerOutcome::Accepted {
                timestamp,
                snapshot: true,
                ..
            } => *timestamp,

            other => panic!("{profile}: REST synchronization failed: {other:?}"),
        };

        assert_eq!(
            accepted_timestamp.composition,
            DepthTimestampComposition::RestReconciliation,
            "{profile}"
        );

        let source = accepted_timestamp
            .latest_source
            .expect("last applied WebSocket timestamp");

        assert_eq!(
            source.provenance,
            DepthTimestampProvenance::Exchange,
            "{profile}"
        );

        // The latest accepted delta has T=1792900000200 ms.
        assert_eq!(
            source.timestamp_ns, 1_792_900_000_200_000_000_i64,
            "{profile}"
        );

        // ----------------------------------------------
        // PUBLICATION FENCE
        // ----------------------------------------------

        let mut fence = DepthPublicationFence::new(STREAM, epoch().stream_generation).unwrap();

        let prepared = fence.prepare(ingress.controller(), &outcome).unwrap();

        assert_eq!(prepared.events().len(), 1);

        let published_timestamp = match prepared.events()[0] {
            DepthPublicationEvent::Snapshot {
                timestamp,
                baseline,
                ..
            } => {
                assert_eq!(baseline.last_sequence, 104);
                timestamp
            }

            other => panic!("{profile}: expected synchronized snapshot: {other:?}"),
        };

        // Publication must preserve Integrity metadata.
        assert_eq!(published_timestamp, accepted_timestamp, "{profile}");

        // Preparation is not acknowledgement.
        assert!(!fence.has_published_book());

        fence.acknowledge(prepared).unwrap();

        assert!(fence.has_published_book());

        assert_eq!(fence.state().publication_revision, 1,);

        println!(
            "{profile}: REST timestamp provenance \
             survived Integrity and publication"
        );
    }
}
