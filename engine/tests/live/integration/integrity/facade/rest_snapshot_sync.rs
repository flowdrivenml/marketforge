use marketforge_engine::live::integrity::{
    IntegrityAdmissionError, IntegrityAdmissionState, IntegrityRecoveryError,
};
use marketforge_engine::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata},
    live::{
        config::{exchange::ExchangeConfig, format::DepthFormatConfig},
        infrastructure::normalization::{DepthMessageKind, NormalizationSource, NormalizedDepth},
        integrity::{
            CreateDepthIntegrity, DepthBufferLimits, DepthContinuityCompiler,
            DepthControllerOutcome, DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch,
            DepthSyncToken, IntegrityDecision, IntegrityFacadeFactory, IntegrityInput,
            IntegrityLifecycleEvent, IntegrityOutcome, IntegrityRecoveryResult,
            RestDepthEndpointConfig, RestDepthRequestContext, StreamIntegrityHandle,
        },
    },
};
use std::path::PathBuf;

use rust_decimal::Decimal;

const STREAM: &str = "stream-42";
const SYMBOL: &str = "BTCUSDT";
const INSTRUMENT_ID: i64 = 42;
const SNAPSHOT_LIMIT: u64 = 1000;

// ============================================================
// REAL EXCHANGE CONFIGURATION
// ============================================================

fn config_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live")
}

fn exchange_config() -> ExchangeConfig {
    ExchangeConfig::load(config_root().join("exchanges/binance_spot.toml")).unwrap()
}

fn format_config() -> DepthFormatConfig {
    DepthFormatConfig::load(config_root().join("formats/depth/binance_spot.toml")).unwrap()
}

// ============================================================
// TRUSTED SOURCE IDENTITY
// ============================================================

fn epoch(stream: u64, connection: u64, subscription: u64) -> DepthSourceEpoch {
    DepthSourceEpoch {
        stream_generation: stream,
        connection_generation: connection,
        subscription_generation: subscription,
    }
}

fn initial_epoch() -> DepthSourceEpoch {
    epoch(10, 20, 30)
}

fn binding(current: DepthSourceEpoch) -> DepthSourceBinding {
    DepthSourceBinding::new(
        STREAM,
        Exchange::Binance,
        "spot",
        INSTRUMENT_ID,
        SYMBOL,
        Some(20),
        current,
    )
    .unwrap()
}

// ============================================================
// INTEGRITY HANDLE
// ============================================================

fn integrity() -> StreamIntegrityHandle {
    let exchange = exchange_config();
    let format = format_config();

    let plan = DepthContinuityCompiler::compile(&format).unwrap();

    IntegrityFacadeFactory::depth(CreateDepthIntegrity {
        binding: binding(initial_epoch()),
        plan,

        buffer_limits: DepthBufferLimits {
            max_messages: 100,
            max_bytes: 1_000_000,
        },

        resource_limits: DepthResourceLimits::default(),

        rest_endpoint: Some(RestDepthEndpointConfig {
            exchange: &exchange,
            format: &format,
        }),
    })
    .unwrap()
}

// ============================================================
// RECOVERY REQUESTS
// ============================================================

fn token(stream_generation: u64, attempt: u64) -> DepthSyncToken {
    DepthSyncToken {
        stream_generation,
        attempt,
    }
}

fn request(current: DepthSourceEpoch, attempt: u64) -> RestDepthRequestContext {
    RestDepthRequestContext {
        source_epoch: current,

        sync_token: token(current.stream_generation, attempt),

        endpoint_key: "depth_snapshot".into(),
        requested_limit: SNAPSHOT_LIMIT,
    }
}

// ============================================================
// CANONICAL DEPTH FIXTURES
// ============================================================

fn level(side: BookSide, price: i64, quantity: i64) -> L2LevelUpdate {
    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange: Exchange::Binance,
            instrument_id: INSTRUMENT_ID,
            symbol: SYMBOL.into(),
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

fn sequence(first: Option<u64>, last: Option<u64>) -> SequenceMetadata {
    SequenceMetadata {
        sequence_first: first,
        sequence_last: last,
        sequence_previous: None,
        cross_sequence: None,
    }
}

fn websocket_delta(first: u64, last: u64) -> NormalizedDepth {
    NormalizedDepth::new(
        STREAM,
        NormalizationSource::WebSocket,
        DepthMessageKind::Delta,
        sequence(Some(first), Some(last)),
        vec![level(BookSide::Bid, 80_000, last as i64)],
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
        sequence(None, Some(last)),
        vec![
            level(BookSide::Bid, 80_000, 3),
            level(BookSide::Ask, 80_010, 2),
        ],
        Some(1_000_000_000),
        Some(200),
    )
    .unwrap()
}

fn websocket_input(current: DepthSourceEpoch, message: NormalizedDepth) -> IntegrityInput<'static> {
    IntegrityInput::Depth {
        epoch: current,
        message,
        accounted_bytes: 1024,
    }
}

// ============================================================
// TEST 1: REST ATTEMPT PREPARATION
// ============================================================

#[test]
fn rest_attempt_starts_without_establishing_book() {
    let mut handle = integrity();
    let current = initial_epoch();

    handle.prepare_depth_recovery(request(current, 1)).unwrap();

    assert!(handle.book().is_none());

    assert_eq!(handle.status().depth_synchronized, Some(false),);
}

// ============================================================
// TEST 2: BUFFERED DELTAS + SNAPSHOT
// ============================================================

#[test]
fn rest_snapshot_bridges_buffered_websocket_deltas() {
    let mut handle = integrity();
    let current = initial_epoch();

    // The recovery attempt begins before the
    // REST request is executed.
    handle.prepare_depth_recovery(request(current, 1)).unwrap();

    let first = handle.process(websocket_input(current, websocket_delta(101, 105)));

    let second = handle.process(websocket_input(current, websocket_delta(106, 110)));

    // The deltas are not yet accepted into
    // an authoritative synchronized book.
    assert_eq!(first.decision(), IntegrityDecision::Ignored,);

    assert_eq!(second.decision(), IntegrityDecision::Ignored,);

    assert!(handle.book().is_none());

    // Recovery returns the normalized REST snapshot.
    let snapshot = rest_snapshot(100);

    let outcome = handle.on_recovery_result(IntegrityRecoveryResult::DepthSnapshot {
        epoch: current,
        token: token(10, 1),
        snapshot: &snapshot,
    });

    assert_eq!(outcome.decision(), IntegrityDecision::Accepted,);

    assert!(matches!(
        outcome,
        IntegrityOutcome::Depth(DepthControllerOutcome::Accepted { snapshot: true, .. })
    ));

    assert!(handle.book().is_some());

    assert_eq!(handle.status().depth_synchronized, Some(true),);

    // A further update should continue from
    // the established sequence baseline.
    let next = handle.process(websocket_input(current, websocket_delta(111, 115)));

    assert_eq!(next.decision(), IntegrityDecision::Accepted,);
}

// ============================================================
// TEST 3: OLD ATTEMPT CANNOT REPLACE NEWER ATTEMPT
// ============================================================

#[test]
fn obsolete_rest_attempt_cannot_replace_current_attempt() {
    let mut handle = integrity();
    let current = initial_epoch();

    handle.prepare_depth_recovery(request(current, 1)).unwrap();

    // Newer attempt supersedes the first.
    handle.prepare_depth_recovery(request(current, 2)).unwrap();

    handle.process(websocket_input(current, websocket_delta(201, 205)));

    // Old response arrives after attempt 2 started.
    let old_snapshot = rest_snapshot(100);

    let obsolete = handle.on_recovery_result(IntegrityRecoveryResult::DepthSnapshot {
        epoch: current,
        token: token(10, 1),
        snapshot: &old_snapshot,
    });

    assert_eq!(obsolete.decision(), IntegrityDecision::Ignored,);

    assert!(handle.book().is_none());

    // The newer attempt can still succeed.
    let current_snapshot = rest_snapshot(200);

    let accepted = handle.on_recovery_result(IntegrityRecoveryResult::DepthSnapshot {
        epoch: current,
        token: token(10, 2),
        snapshot: &current_snapshot,
    });

    assert_eq!(accepted.decision(), IntegrityDecision::Accepted,);

    assert!(handle.book().is_some());
}

// ============================================================
// TEST 4: CONNECTION INTERRUPTION FENCES REST RESULT
// ============================================================

#[test]
fn interruption_rejects_in_flight_rest_completion() {
    let mut handle = integrity();
    let current = initial_epoch();

    handle.prepare_depth_recovery(request(current, 1)).unwrap();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let snapshot = rest_snapshot(100);

    let outcome = handle.on_recovery_result(IntegrityRecoveryResult::DepthSnapshot {
        epoch: current,
        token: token(10, 1),
        snapshot: &snapshot,
    });

    assert_eq!(outcome.decision(), IntegrityDecision::Rejected,);

    assert!(handle.book().is_none());
}

// ============================================================
// TEST 5: NEW GENERATION REQUIRES NEW ATTEMPT
// ============================================================

#[test]
fn new_generation_requires_new_recovery_attempt() {
    let mut handle = integrity();
    let old = initial_epoch();

    handle.prepare_depth_recovery(request(old, 1)).unwrap();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let next = epoch(11, 21, 31);

    handle
        .on_lifecycle(IntegrityLifecycleEvent::DepthGeneration(binding(next)))
        .unwrap();

    // Old generation's REST response must not
    // synchronize the new generation.
    let old_snapshot = rest_snapshot(100);

    let obsolete = handle.on_recovery_result(IntegrityRecoveryResult::DepthSnapshot {
        epoch: old,
        token: token(10, 1),
        snapshot: &old_snapshot,
    });

    assert_eq!(obsolete.decision(), IntegrityDecision::Ignored,);

    assert!(handle.book().is_none());

    // A fresh trusted attempt belongs to generation 11.
    handle.prepare_depth_recovery(request(next, 1)).unwrap();

    handle.process(websocket_input(next, websocket_delta(301, 305)));

    let new_snapshot = rest_snapshot(300);

    let accepted = handle.on_recovery_result(IntegrityRecoveryResult::DepthSnapshot {
        epoch: next,
        token: token(11, 1),
        snapshot: &new_snapshot,
    });

    assert_eq!(accepted.decision(), IntegrityDecision::Accepted,);

    assert!(handle.book().is_some());
}

#[test]
fn interrupted_source_cannot_prepare_recovery() {
    let mut handle = integrity();
    let current = initial_epoch();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let result = handle.prepare_depth_recovery(request(current, 1));

    assert!(matches!(
        result,
        Err(IntegrityRecoveryError::AdmissionBlocked(
            IntegrityAdmissionError::Interrupted
        ))
    ));

    assert_eq!(
        handle.admission_state(),
        IntegrityAdmissionState::Interrupted,
    );

    assert!(handle.book().is_none());
}

#[test]
fn stopped_source_cannot_prepare_or_complete_recovery() {
    let mut handle = integrity();
    let current = initial_epoch();

    handle.prepare_depth_recovery(request(current, 1)).unwrap();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceStopped)
        .unwrap();

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Stopped,);

    let preparation = handle.prepare_depth_recovery(request(current, 2));

    assert!(matches!(
        preparation,
        Err(IntegrityRecoveryError::AdmissionBlocked(
            IntegrityAdmissionError::Stopped
        ))
    ));

    let snapshot = rest_snapshot(100);

    let completion = handle.on_recovery_result(IntegrityRecoveryResult::DepthSnapshot {
        epoch: current,
        token: token(10, 1),
        snapshot: &snapshot,
    });

    assert!(matches!(
        completion,
        IntegrityOutcome::AdmissionBlocked(IntegrityAdmissionError::Stopped)
    ));

    assert!(handle.book().is_none());
}

#[test]
fn recovery_reopens_only_after_trusted_generation() {
    let mut handle = integrity();

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let next = epoch(11, 21, 31);

    handle
        .on_lifecycle(IntegrityLifecycleEvent::DepthGeneration(binding(next)))
        .unwrap();

    assert_eq!(handle.admission_state(), IntegrityAdmissionState::Active,);

    handle.prepare_depth_recovery(request(next, 1)).unwrap();

    handle.process(websocket_input(next, websocket_delta(401, 405)));

    let snapshot = rest_snapshot(400);

    let result = handle.on_recovery_result(IntegrityRecoveryResult::DepthSnapshot {
        epoch: next,
        token: token(11, 1),
        snapshot: &snapshot,
    });

    assert_eq!(result.decision(), IntegrityDecision::Accepted,);

    assert!(handle.book().is_some());
}
