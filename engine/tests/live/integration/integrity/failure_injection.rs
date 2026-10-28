use std::path::PathBuf;

use marketforge_engine::{
    live::{
        config::{exchange::ExchangeConfig, format::DepthFormatConfig},
        infrastructure::normalization::{DepthMessageKind, NormalizationSource, NormalizedDepth},
        integrity::{
            BoundDepthIngress, DepthBufferLimits, DepthContinuityCompiler, DepthControllerOutcome,
            DepthPublicationEvent, DepthPublicationFence, DepthPublicationPhase,
            DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch, DepthSyncToken,
            RestDepthRequestContext,
        },
    },
    shared::domain::canonical::{
        BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata,
    },
};

use rust_decimal::Decimal;

const STREAM: &str = "integration:binance:spot:BTCUSDT:depth";

const INITIAL_GENERATION: u64 = 10;

// ============================================================
// CONFIGURATION
// ============================================================

fn config_root() -> PathBuf {
    crate::support::data(".config/live")
}

fn depth_format() -> DepthFormatConfig {
    DepthFormatConfig::load(config_root().join("formats/depth/binance_spot.toml")).unwrap()
}

fn exchange_config() -> ExchangeConfig {
    ExchangeConfig::load(config_root().join("exchanges/binance_spot.toml")).unwrap()
}

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
        None,
        epoch(generation),
    )
    .unwrap()
}

fn ingress(generation: u64) -> BoundDepthIngress {
    let format = depth_format();

    let plan = DepthContinuityCompiler::compile(&format).unwrap();

    let mut ingress = BoundDepthIngress::new(
        binding(generation),
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

// ============================================================
// REST REQUEST FIXTURES
// ============================================================

fn token(generation: u64, attempt: u64) -> DepthSyncToken {
    DepthSyncToken {
        stream_generation: generation,
        attempt,
    }
}

fn request(generation: u64, attempt: u64) -> RestDepthRequestContext {
    RestDepthRequestContext {
        source_epoch: epoch(generation),

        sync_token: token(generation, attempt),

        endpoint_key: depth_format()
            .synchronization
            .snapshot_endpoint
            .expect("Binance Spot REST endpoint"),

        requested_limit: 100,
    }
}

fn start_rest(ingress: &mut BoundDepthIngress, generation: u64, attempt: u64) {
    ingress
        .begin_rest_attempt(request(generation, attempt))
        .unwrap();
}

// ============================================================
// NORMALIZED DEPTH FIXTURES
// ============================================================

fn level(side: BookSide, price: i64, quantity: i64) -> L2LevelUpdate {
    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange: Exchange::Binance,
            instrument_id: 42,
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
        vec![level(BookSide::Bid, 80_000, quantity)],
        Some(1_000_000_000),
        None,
    )
    .unwrap()
}

fn rest_snapshot(last: u64) -> NormalizedDepth {
    NormalizedDepth::new(
        STREAM,
        NormalizationSource::Rest,
        DepthMessageKind::Snapshot,
        SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(last),
            sequence_previous: None,
            cross_sequence: None,
        },
        vec![
            level(BookSide::Bid, 80_000, 3),
            level(BookSide::Ask, 80_010, 2),
        ],
        Some(1_000_000_000),
        None,
    )
    .unwrap()
}

fn buffer_delta(
    ingress: &mut BoundDepthIngress,
    generation: u64,
    first: u64,
    last: u64,
    quantity: i64,
) -> DepthControllerOutcome {
    ingress.on_websocket(epoch(generation), ws_delta(first, last, quantity), 1024)
}

fn complete_rest(
    ingress: &mut BoundDepthIngress,
    generation: u64,
    attempt: u64,
    snapshot_sequence: u64,
) -> DepthControllerOutcome {
    ingress.on_rest_snapshot(
        epoch(generation),
        token(generation, attempt),
        &rest_snapshot(snapshot_sequence),
    )
}

fn synchronize(
    ingress: &mut BoundDepthIngress,
    generation: u64,
    attempt: u64,
    snapshot_sequence: u64,
) -> DepthControllerOutcome {
    start_rest(ingress, generation, attempt);

    let buffered = buffer_delta(
        ingress,
        generation,
        snapshot_sequence + 1,
        snapshot_sequence + 5,
        7,
    );

    assert!(matches!(buffered, DepthControllerOutcome::Ignored { .. }));

    let outcome = complete_rest(ingress, generation, attempt, snapshot_sequence);

    assert!(
        matches!(
            outcome,
            DepthControllerOutcome::Accepted { snapshot: true, .. }
        ),
        "REST synchronization must produce a full snapshot"
    );

    assert!(ingress.is_synchronized());

    outcome
}

// ============================================================
// TEST 1
//
// REST RECOVERY SUCCEEDS, PUBLICATION FAILS
// ============================================================

#[test]
fn recovered_book_cannot_publish_deltas_after_failed_snapshot_handoff() {
    let mut ingress = ingress(INITIAL_GENERATION);

    let mut publication = DepthPublicationFence::new(STREAM, INITIAL_GENERATION).unwrap();

    // REST synchronization succeeds.
    let recovered = synchronize(&mut ingress, INITIAL_GENERATION, 1, 100);

    assert!(ingress.is_synchronized());

    let prepared = publication
        .prepare(ingress.controller(), &recovered)
        .unwrap();

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Snapshot { .. }
    ));

    // Simulate publication queue rejecting the handoff.
    publication.handoff_failed(prepared).unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired);

    assert_eq!(publication.state().publication_revision, 0,);

    // The authoritative controller remains synchronized.
    // This does NOT mean that consumers are synchronized.
    assert!(ingress.is_synchronized());

    // The controller receives another valid delta.
    let next = buffer_delta(&mut ingress, INITIAL_GENERATION, 106, 110, 9);

    assert!(matches!(
        next,
        DepthControllerOutcome::Accepted {
            snapshot: false,
            ..
        }
    ));

    // The publisher must not resume incremental output.
    assert!(publication.prepare(ingress.controller(), &next).is_err());

    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired);

    assert_eq!(publication.state().publication_revision, 0,);
}

// ============================================================
// TEST 2
//
// EXPLICIT INVALIDATION AND FRESH REST RECOVERY
// ============================================================

#[test]
fn failed_rest_publication_requires_reset_before_new_snapshot() {
    let mut ingress = ingress(INITIAL_GENERATION);

    let mut publication = DepthPublicationFence::new(STREAM, INITIAL_GENERATION).unwrap();

    let first = synchronize(&mut ingress, INITIAL_GENERATION, 1, 100);

    let prepared = publication.prepare(ingress.controller(), &first).unwrap();

    publication.handoff_failed(prepared).unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired);

    // Explicit consumer invalidation.
    let invalidation = publication.prepare_reset_invalidation().unwrap();

    assert!(
        invalidation
            .events()
            .iter()
            .all(|event| { matches!(event, DepthPublicationEvent::Invalidated { .. }) })
    );

    publication.acknowledge(invalidation).unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired);

    // A fresh controller synchronization is needed.
    ingress.invalidate();

    assert!(!ingress.is_synchronized());

    let replacement = synchronize(&mut ingress, INITIAL_GENERATION, 2, 200);

    let prepared = publication
        .prepare_reset_snapshot(ingress.controller(), &replacement)
        .unwrap();

    assert_eq!(prepared.events().len(), 1);

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Snapshot { .. }
    ));

    publication.acknowledge(prepared).unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::Ready);

    assert!(publication.has_published_book());

    // Only now can subsequent deltas be published.
    let next = buffer_delta(&mut ingress, INITIAL_GENERATION, 206, 210, 11);

    let prepared = publication.prepare(ingress.controller(), &next).unwrap();

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Delta { .. }
    ));

    publication.acknowledge(prepared).unwrap();

    assert!(publication.has_published_book());
}

// ============================================================
// TEST 3
//
// STREAM GENERATION CHANGES DURING REST REQUEST
// ============================================================

#[test]
fn generation_transition_cancels_pending_rest_attempt() {
    let mut ingress = ingress(INITIAL_GENERATION);

    start_rest(&mut ingress, INITIAL_GENERATION, 1);

    buffer_delta(&mut ingress, INITIAL_GENERATION, 101, 105, 7);

    assert!(!ingress.is_synchronized());

    // Connection ownership changes while REST is in flight.
    ingress.begin_generation(binding(11)).unwrap();

    // Old HTTP response arrives after generation transition.
    let stale = complete_rest(&mut ingress, INITIAL_GENERATION, 1, 100);

    assert!(matches!(stale, DepthControllerOutcome::Ignored { .. }));

    assert!(!ingress.is_synchronized());

    assert_eq!(ingress.controller().baseline(), None);

    // New generation starts independently.
    let current = synchronize(&mut ingress, 11, 1, 200);

    assert!(matches!(
        current,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 205,);
}

// ============================================================
// TEST 4
//
// OLD HTTP CALLBACK ARRIVES AFTER NEW SYNCHRONIZATION
// ============================================================

#[test]
fn obsolete_rest_callback_cannot_replace_new_generation_book() {
    let mut ingress = ingress(INITIAL_GENERATION);

    start_rest(&mut ingress, INITIAL_GENERATION, 1);

    buffer_delta(&mut ingress, INITIAL_GENERATION, 101, 105, 7);

    ingress.begin_generation(binding(11)).unwrap();

    synchronize(&mut ingress, 11, 1, 200);

    let baseline_before = ingress.controller().baseline().unwrap();

    let book_generation_before = ingress.controller().book_generation();

    // Old callback contains otherwise valid data.
    let stale = complete_rest(&mut ingress, INITIAL_GENERATION, 1, 100);

    assert!(matches!(stale, DepthControllerOutcome::Ignored { .. }));

    // No mutation of the new authoritative book.
    assert_eq!(ingress.controller().baseline(), Some(baseline_before),);

    assert_eq!(
        ingress.controller().book_generation(),
        book_generation_before,
    );

    assert!(ingress.is_synchronized());
}
