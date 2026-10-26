use std::path::PathBuf;

use marketforge_engine::live::integrity::DepthBookLimits;
use marketforge_engine::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata},
    live::{
        config::{exchange::ExchangeConfig, format::DepthFormatConfig},
        infrastructure::normalization::{DepthMessageKind, NormalizationSource, NormalizedDepth},
        integrity::{
            BoundDepthIngress, DepthBufferLimits, DepthContinuityCompiler, DepthControllerOutcome,
            DepthPublicationEvent, DepthPublicationFence, DepthResourceLimits, DepthSourceBinding,
            DepthSourceEpoch, DepthSyncToken, RestDepthRequestContext,
        },
    },
};

use rust_decimal::Decimal;

const STREAM: &str = "integration:binance:spot:BTCUSDT:depth";

const GENERATION: u64 = 10;

// ============================================================
// TEST FIXTURES
// ============================================================

fn epoch(generation: u64) -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: generation,
        connection_generation: generation,
        subscription_generation: generation,
    }
}

fn binding(generation: u64) -> DepthSourceBinding {
    DepthSourceBinding::new(
        STREAM,
        Exchange::Binance,
        "spot",
        42,
        "BTCUSDT",
        // Binance Spot REST synchronization does not
        // require a fixed WebSocket subscription depth.
        None,
        epoch(generation),
    )
    .unwrap()
}

fn config_root() -> PathBuf {
    crate::support::data(".config/live")
}

fn depth_format() -> DepthFormatConfig {
    DepthFormatConfig::load(config_root().join("formats/depth/binance_spot.toml")).unwrap()
}

fn exchange_config() -> ExchangeConfig {
    ExchangeConfig::load(config_root().join("exchanges/binance_spot.toml")).unwrap()
}

fn ingress() -> BoundDepthIngress {
    let format = depth_format();

    let plan = DepthContinuityCompiler::compile(&format).unwrap();

    let mut ingress = BoundDepthIngress::new(
        binding(GENERATION),
        plan,
        DepthBufferLimits {
            max_messages: 16,
            max_bytes: 1_000_000,
        },
        DepthResourceLimits::default(),
    )
    .unwrap();

    ingress
        .configure_rest_endpoint(&exchange_config(), &format)
        .unwrap();

    ingress
}

fn token(attempt: u64) -> DepthSyncToken {
    DepthSyncToken {
        stream_generation: GENERATION,
        attempt,
    }
}

fn request(attempt: u64) -> RestDepthRequestContext {
    let format = depth_format();

    RestDepthRequestContext {
        source_epoch: epoch(GENERATION),

        sync_token: token(attempt),

        endpoint_key: format
            .synchronization
            .snapshot_endpoint
            .expect("Binance Spot requires REST snapshot endpoint"),

        requested_limit: 100,
    }
}

fn level(side: BookSide, price: i64, quantity: i64, instrument_id: i64) -> L2LevelUpdate {
    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange: Exchange::Binance,
            instrument_id,
            symbol: "BTCUSDT".into(),
            stream_id: STREAM.into(),
        },

        side,

        price: Decimal::from(price),

        quantity_base: Some(Decimal::from(quantity)),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: None,
    }
}

fn ws_delta(first: u64, last: u64, quantity: i64) -> NormalizedDepth {
    NormalizedDepth::new(
        STREAM,
        NormalizationSource::WebSocket,
        DepthMessageKind::Delta,
        SequenceMetadata {
            sequence_first: Some(first),
            sequence_last: Some(last),
            sequence_previous: None,
            cross_sequence: None,
        },
        vec![level(BookSide::Bid, 80_000, quantity, 42)],
        Some(1_000_000_000),
        None,
    )
    .unwrap()
}

fn rest_snapshot(last_update_id: u64, instrument_id: i64) -> NormalizedDepth {
    NormalizedDepth::new(
        STREAM,
        NormalizationSource::Rest,
        DepthMessageKind::Snapshot,
        SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(last_update_id),
            sequence_previous: None,
            cross_sequence: None,
        },
        vec![
            level(BookSide::Bid, 80_000, 3, instrument_id),
            level(BookSide::Ask, 80_010, 2, instrument_id),
        ],
        Some(1_000_000_000),
        // REST response depth is not automatically the
        // WebSocket subscription depth.
        None,
    )
    .unwrap()
}

fn start_attempt(ingress: &mut BoundDepthIngress, attempt: u64) {
    ingress.begin_rest_attempt(request(attempt)).unwrap();
}

fn buffer(
    ingress: &mut BoundDepthIngress,
    first: u64,
    last: u64,
    quantity: i64,
) -> DepthControllerOutcome {
    ingress.on_websocket(epoch(GENERATION), ws_delta(first, last, quantity), 1024)
}

// ============================================================
// TEST 1 — COMPLETE REST SYNCHRONIZATION
// ============================================================

#[test]
fn rest_snapshot_bridges_buffered_deltas_and_publishes_book() {
    let mut ingress = ingress();

    let mut publication = DepthPublicationFence::new(STREAM, GENERATION).unwrap();

    start_attempt(&mut ingress, 1);

    // Updates arrive while REST is in flight.
    for (first, last, quantity) in [(90, 95, 5), (96, 105, 7), (106, 110, 9)] {
        let outcome = buffer(&mut ingress, first, last, quantity);

        assert!(matches!(outcome, DepthControllerOutcome::Ignored { .. }));
    }

    assert!(!ingress.is_synchronized());

    // The REST snapshot establishes sequence 100.
    // Buffered updates bridge and advance through 110.
    let outcome = ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 42));

    assert!(matches!(
        outcome,
        DepthControllerOutcome::Accepted {
            snapshot: true,
            book_generation: 1,
            ..
        }
    ));

    assert!(ingress.is_synchronized());

    assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 110,);

    // The initial consumer operation must be a
    // complete book snapshot, not the final delta.
    let prepared = publication.prepare(ingress.controller(), &outcome).unwrap();

    assert_eq!(prepared.events().len(), 1);

    let DepthPublicationEvent::Snapshot {
        book_generation,
        baseline,
        levels,
        ..
    } = prepared.events()[0]
    else {
        panic!("REST recovery must publish a full snapshot");
    };

    assert_eq!(book_generation, 1);
    assert_eq!(baseline.last_sequence, 110);

    assert_eq!(
        levels.len(),
        ingress.controller().book().bids().len() + ingress.controller().book().asks().len(),
    );

    // REST snapshot included an ask that was not
    // modified by the buffered bid updates.
    assert!(
        levels
            .iter()
            .any(|(side, price, _)| { *side == BookSide::Ask && *price == Decimal::from(80_010) })
    );

    publication.acknowledge(prepared).unwrap();

    assert_eq!(publication.state().publication_revision, 1,);

    // Now ordinary WebSocket updates may publish.
    let next = buffer(&mut ingress, 111, 115, 11);

    assert!(matches!(
        next,
        DepthControllerOutcome::Accepted {
            snapshot: false,
            ..
        }
    ));

    let prepared = publication.prepare(ingress.controller(), &next).unwrap();

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Delta {
            book_generation: 1,
            ..
        }
    ));

    publication.acknowledge(prepared).unwrap();

    assert_eq!(publication.state().publication_revision, 2,);

    assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 115,);
}

// ============================================================
// TEST 2 — STALE REST RESPONSE
// ============================================================

#[test]
fn obsolete_rest_response_cannot_complete_newer_attempt() {
    let mut ingress = ingress();

    start_attempt(&mut ingress, 1);

    buffer(&mut ingress, 101, 105, 7);

    // Attempt 2 supersedes attempt 1.
    start_attempt(&mut ingress, 2);

    buffer(&mut ingress, 201, 205, 9);

    let stale = ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 42));

    assert!(matches!(stale, DepthControllerOutcome::Ignored { .. }));

    assert_eq!(ingress.controller().active_rest_attempt(), Some(token(2)),);

    assert!(!ingress.is_synchronized());

    let current = ingress.on_rest_snapshot(epoch(GENERATION), token(2), &rest_snapshot(200, 42));

    assert!(matches!(
        current,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    assert!(ingress.is_synchronized());

    assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 205,);

    // The old response cannot replace the accepted book.
    let duplicate_old =
        ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 42));

    assert!(matches!(
        duplicate_old,
        DepthControllerOutcome::Ignored { .. }
    ));

    assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 205,);
}

// ============================================================
// TEST 3 — WRONG REST INSTRUMENT
// ============================================================

#[test]
fn wrong_rest_instrument_invalidates_without_book_commit() {
    let mut ingress = ingress();

    start_attempt(&mut ingress, 1);

    buffer(&mut ingress, 101, 105, 7);

    // Valid sequence, wrong canonical instrument.
    let outcome = ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 999));

    assert!(matches!(
        outcome,
        DepthControllerOutcome::RecoveryRequired { .. }
    ));

    assert!(!ingress.is_synchronized());

    assert!(!ingress.controller().book().is_initialized());

    assert_eq!(ingress.controller().active_rest_attempt(), None,);

    // No message from the failed attempt can revive it.
    let late = ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 42));

    assert!(matches!(late, DepthControllerOutcome::Ignored { .. }));

    // A new attempt is required.
    start_attempt(&mut ingress, 2);

    buffer(&mut ingress, 201, 205, 9);

    let recovered = ingress.on_rest_snapshot(epoch(GENERATION), token(2), &rest_snapshot(200, 42));

    assert!(matches!(
        recovered,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    assert!(ingress.is_synchronized());
}

// ============================================================
// TEST 4 — FAILED INITIAL BRIDGE
// ============================================================

#[test]
fn missing_bridge_cannot_publish_partial_snapshot() {
    let mut ingress = ingress();

    let mut publication = DepthPublicationFence::new(STREAM, GENERATION).unwrap();

    start_attempt(&mut ingress, 1);

    // Snapshot ends at 100; first available delta begins
    // at 110. Updates 101..109 cannot be established.
    buffer(&mut ingress, 110, 115, 7);

    let failure = ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 42));

    assert!(matches!(
        failure,
        DepthControllerOutcome::RecoveryRequired { .. }
    ));

    assert!(!ingress.is_synchronized());

    assert_eq!(ingress.controller().baseline(), None,);

    assert!(!ingress.controller().book().is_initialized());

    // No synchronized book can be published.
    let prepared = publication.prepare(ingress.controller(), &failure).unwrap();

    assert!(prepared.events().is_empty());

    publication.acknowledge(prepared).unwrap();

    assert!(!publication.has_published_book());

    // An explicit newer attempt may recover.
    start_attempt(&mut ingress, 2);

    buffer(&mut ingress, 201, 205, 9);

    let recovered = ingress.on_rest_snapshot(epoch(GENERATION), token(2), &rest_snapshot(200, 42));

    assert!(matches!(
        recovered,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    let prepared = publication
        .prepare(ingress.controller(), &recovered)
        .unwrap();

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Snapshot { .. }
    ));

    publication.acknowledge(prepared).unwrap();

    assert!(publication.has_published_book());
}

// ============================================================
// TEST 5 — INVALID REST REQUEST
// ============================================================

#[test]
fn invalid_rest_request_preserves_current_attempt() {
    let mut ingress = ingress();

    start_attempt(&mut ingress, 1);

    buffer(&mut ingress, 101, 105, 7);

    let mut invalid = request(2);

    // This request does not match the configured
    // authoritative snapshot endpoint.
    invalid.endpoint_key = "unrelated_endpoint".into();

    assert!(ingress.begin_rest_attempt(invalid).is_err());

    // Validation must fail BEFORE replacing attempt 1.
    assert_eq!(ingress.controller().active_rest_attempt(), Some(token(1)),);

    let original = ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 42));

    assert!(matches!(
        original,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    assert!(ingress.is_synchronized());
}

#[test]
fn buffer_overflow_cancels_rest_attempt_and_fences_late_response() {
    let format = depth_format();

    let plan = DepthContinuityCompiler::compile(&format).unwrap();

    let mut ingress = BoundDepthIngress::new(
        binding(GENERATION),
        plan,
        // Permit just one buffered message.
        DepthBufferLimits {
            max_messages: 1,
            max_bytes: 1_000_000,
        },
        DepthResourceLimits::default(),
    )
    .unwrap();

    ingress
        .configure_rest_endpoint(&exchange_config(), &format)
        .unwrap();

    start_attempt(&mut ingress, 1);

    let first = buffer(&mut ingress, 101, 105, 7);

    assert!(matches!(first, DepthControllerOutcome::Ignored { .. }));

    // Second message exceeds max_messages.
    let overflow = buffer(&mut ingress, 106, 110, 9);

    assert!(matches!(
        overflow,
        DepthControllerOutcome::RecoveryRequired { .. }
    ));

    assert!(!ingress.is_synchronized());

    assert_eq!(ingress.controller().active_rest_attempt(), None,);

    // An HTTP response already in flight cannot
    // reactivate the cancelled synchronization.
    let late = ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 42));

    assert!(matches!(late, DepthControllerOutcome::Ignored { .. }));

    assert!(!ingress.controller().book().is_initialized());
}

#[test]
fn retained_book_limits_reject_oversized_rest_snapshot() {
    let mut ingress = ingress();

    let limits = DepthBookLimits {
        max_levels: 1,
        max_estimated_bytes: 32 * 1024 * 1024,
    };

    // Configuration is permitted before synchronization.
    ingress.set_book_limits(limits).unwrap();

    assert_eq!(ingress.book_limits(), limits);

    start_attempt(&mut ingress, 1);

    buffer(&mut ingress, 101, 105, 7);

    // The REST snapshot contains two retained price levels:
    // one bid and one ask.
    //
    // The configured maximum is one level.
    let outcome = ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 42));

    assert!(matches!(
        outcome,
        DepthControllerOutcome::RecoveryRequired { .. }
    ));

    // The oversized candidate must not become authoritative.
    assert!(!ingress.is_synchronized());
    assert!(!ingress.controller().book().is_initialized());
    assert_eq!(ingress.controller().baseline(), None);

    // The rejected snapshot must not change the limits.
    assert_eq!(ingress.book_limits(), limits);
}

#[test]
fn retained_book_limits_cannot_change_during_rest_attempt() {
    let mut ingress = ingress();

    let original = ingress.book_limits();

    start_attempt(&mut ingress, 1);

    let replacement = DepthBookLimits {
        max_levels: 100,
        max_estimated_bytes: 32 * 1024 * 1024,
    };

    // Active REST synchronization forbids configuration
    // changes, even before the snapshot has arrived.
    assert!(ingress.set_book_limits(replacement).is_err());

    assert_eq!(ingress.book_limits(), original);

    // The failed configuration change must not cancel
    // or corrupt the active REST attempt.
    buffer(&mut ingress, 101, 105, 7);

    let outcome = ingress.on_rest_snapshot(epoch(GENERATION), token(1), &rest_snapshot(100, 42));

    assert!(matches!(
        outcome,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    assert!(ingress.is_synchronized());

    // Configuration changes are also forbidden while
    // an authoritative book is synchronized.
    assert!(ingress.set_book_limits(replacement).is_err());

    assert_eq!(ingress.book_limits(), original);
}

#[test]
fn invalid_retained_book_limits_preserve_previous_policy() {
    let mut ingress = ingress();

    let original = ingress.book_limits();

    let invalid = DepthBookLimits {
        max_levels: 0,
        max_estimated_bytes: 32 * 1024 * 1024,
    };

    assert!(ingress.set_book_limits(invalid).is_err());

    assert_eq!(ingress.book_limits(), original);

    // Invalid configuration does not prevent subsequent
    // legitimate configuration.
    let valid = DepthBookLimits {
        max_levels: 500,
        max_estimated_bytes: 16 * 1024 * 1024,
    };

    ingress.set_book_limits(valid).unwrap();

    assert_eq!(ingress.book_limits(), valid);
}
