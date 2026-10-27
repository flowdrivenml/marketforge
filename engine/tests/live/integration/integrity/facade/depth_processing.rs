use marketforge_engine::live::integrity::{
    DepthPublicationEvent, DepthPublicationPhase, IntegrityPublicationAdapter,
};
use marketforge_engine::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata},
    live::{
        config::format::DepthFormatConfig,
        infrastructure::normalization::{DepthMessageKind, NormalizationSource, NormalizedDepth},
        integrity::{
            CreateDepthIntegrity, DepthBufferLimits, DepthContinuityCompiler,
            DepthControllerOutcome, DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch,
            IntegrityAdmissionError, IntegrityDecision, IntegrityFacadeFactory, IntegrityInput,
            IntegrityLifecycleEvent, IntegrityOutcome, StreamIntegrityHandle,
        },
    },
};
use std::path::PathBuf;

use rust_decimal::Decimal;

const STREAM: &str = "stream-42";
const INSTRUMENT_ID: i64 = 42;
const SYMBOL: &str = "BTCUSDT";
const DEPTH: u64 = 200;

// ============================================================
// REAL CONFIGURATION
// ============================================================

fn format_config() -> DepthFormatConfig {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/live/formats/depth/bybit_spot.toml");

    DepthFormatConfig::load(path).unwrap()
}

// ============================================================
// SOURCE BINDING
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
        Exchange::Bybit,
        "spot",
        INSTRUMENT_ID,
        SYMBOL,
        Some(DEPTH),
        current,
    )
    .unwrap()
}

// ============================================================
// FACADE FIXTURE
// ============================================================

fn integrity() -> StreamIntegrityHandle {
    let plan = DepthContinuityCompiler::compile(&format_config()).unwrap();

    IntegrityFacadeFactory::depth(CreateDepthIntegrity {
        binding: binding(initial_epoch()),
        plan,

        buffer_limits: DepthBufferLimits {
            max_messages: 100,
            max_bytes: 1_000_000,
        },

        resource_limits: DepthResourceLimits::default(),

        rest_endpoint: None,
    })
    .unwrap()
}

// ============================================================
// NORMALIZED DEPTH FIXTURES
// ============================================================

fn level(side: BookSide, price: i64, quantity: i64) -> L2LevelUpdate {
    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
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

fn depth(
    kind: DepthMessageKind,
    update_id: u64,
    cross_sequence: u64,
    quantity: i64,
) -> NormalizedDepth {
    NormalizedDepth::new(
        STREAM,
        NormalizationSource::WebSocket,
        kind,
        SequenceMetadata {
            sequence_first: None,
            sequence_last: Some(update_id),
            sequence_previous: None,
            cross_sequence: Some(cross_sequence),
        },
        vec![
            level(BookSide::Bid, 80_000, quantity),
            level(BookSide::Ask, 80_001, 2),
        ],
        Some(1_000_000_000),
        Some(DEPTH),
    )
    .unwrap()
}

fn snapshot(update_id: u64, cross_sequence: u64) -> NormalizedDepth {
    depth(DepthMessageKind::Snapshot, update_id, cross_sequence, 3)
}

fn delta(update_id: u64, cross_sequence: u64, quantity: i64) -> NormalizedDepth {
    depth(DepthMessageKind::Delta, update_id, cross_sequence, quantity)
}

// ============================================================
// FACADE INPUT
// ============================================================

fn input(current: DepthSourceEpoch, message: NormalizedDepth) -> IntegrityInput<'static> {
    IntegrityInput::Depth {
        epoch: current,
        message,
        accounted_bytes: 1024,
    }
}

// ============================================================
// TESTS
// ============================================================

#[test]
fn snapshot_establishes_authoritative_book() {
    let mut handle = integrity();

    assert!(handle.book().is_none());

    let outcome = handle.process(input(initial_epoch(), snapshot(100, 200)));

    assert_eq!(outcome.decision(), IntegrityDecision::Accepted,);

    assert!(matches!(
        outcome,
        IntegrityOutcome::Depth(DepthControllerOutcome::Accepted { snapshot: true, .. })
    ));

    let coverage = handle.status().depth_coverage.unwrap();

    assert!(coverage.synchronized);

    assert_eq!(coverage.subscribed_depth, Some(DEPTH),);

    assert_eq!(coverage.retained_bid_levels, 1);
    assert_eq!(coverage.retained_ask_levels, 1);

    assert!(handle.book().is_some());
}

#[test]
fn valid_delta_updates_authoritative_book() {
    let mut handle = integrity();

    handle.process(input(initial_epoch(), snapshot(100, 200)));

    let outcome = handle.process(input(initial_epoch(), delta(101, 201, 5)));

    assert_eq!(outcome.decision(), IntegrityDecision::Accepted,);

    assert!(matches!(
        outcome,
        IntegrityOutcome::Depth(DepthControllerOutcome::Accepted {
            snapshot: false,
            ..
        })
    ));

    assert!(handle.book().is_some());

    let coverage = handle.status().depth_coverage.unwrap();

    assert!(coverage.synchronized);
    assert_eq!(coverage.retained_bid_levels, 1);
    assert_eq!(coverage.retained_ask_levels, 1);
}

#[test]
fn obsolete_connection_cannot_modify_synchronized_book() {
    let mut handle = integrity();

    handle.process(input(initial_epoch(), snapshot(100, 200)));

    let coverage_before = handle.status().depth_coverage.unwrap();

    let obsolete = epoch(10, 19, 30);

    let outcome = handle.process(input(obsolete, delta(101, 201, 5)));

    assert_eq!(outcome.decision(), IntegrityDecision::Ignored,);

    assert!(matches!(
        outcome,
        IntegrityOutcome::Depth(DepthControllerOutcome::Ignored { .. })
    ));

    assert_eq!(handle.status().depth_coverage.unwrap(), coverage_before,);

    assert!(handle.book().is_some());
}

#[test]
fn interruption_invalidates_existing_book() {
    let mut handle = integrity();

    handle.process(input(initial_epoch(), snapshot(100, 200)));

    assert!(handle.book().is_some());

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    assert!(handle.book().is_none());

    assert_eq!(handle.status().depth_synchronized, Some(false),);

    let outcome = handle.process(input(initial_epoch(), delta(101, 201, 5)));

    assert!(matches!(
        outcome,
        IntegrityOutcome::AdmissionBlocked(IntegrityAdmissionError::Interrupted)
    ));
}

#[test]
fn new_generation_requires_fresh_snapshot() {
    let mut handle = integrity();

    handle.process(input(initial_epoch(), snapshot(100, 200)));

    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let next = epoch(11, 21, 31);

    handle
        .on_lifecycle(IntegrityLifecycleEvent::DepthGeneration(binding(next)))
        .unwrap();

    // New transport generation does not establish
    // synchronization by itself.
    assert!(handle.book().is_none());

    assert_eq!(handle.status().depth_synchronized, Some(false),);

    // A new authoritative WebSocket snapshot
    // establishes the replacement book.
    let outcome = handle.process(input(next, snapshot(200, 300)));

    assert_eq!(outcome.decision(), IntegrityDecision::Accepted,);

    assert!(handle.book().is_some());

    assert_eq!(handle.status().depth_synchronized, Some(true),);
}
#[test]
fn sequential_and_batch_processing_are_equivalent() {
    let mut sequential = integrity();
    let mut batched = integrity();

    let messages = vec![
        input(initial_epoch(), snapshot(100, 200)),
        input(initial_epoch(), delta(101, 201, 5)),
        input(initial_epoch(), delta(102, 202, 6)),
        input(initial_epoch(), delta(103, 203, 7)),
    ];

    let batch = batched.evaluate_batch(messages);

    let batch_decisions: Vec<_> = batch
        .outcomes
        .iter()
        .map(IntegrityOutcome::decision)
        .collect();

    let sequential_decisions = vec![
        sequential
            .process(input(initial_epoch(), snapshot(100, 200)))
            .decision(),
        sequential
            .process(input(initial_epoch(), delta(101, 201, 5)))
            .decision(),
        sequential
            .process(input(initial_epoch(), delta(102, 202, 6)))
            .decision(),
        sequential
            .process(input(initial_epoch(), delta(103, 203, 7)))
            .decision(),
    ];

    assert_eq!(batch_decisions, sequential_decisions);

    assert_eq!(
        batched.status().depth_coverage,
        sequential.status().depth_coverage,
    );

    assert_eq!(
        batched.status().depth_synchronized,
        sequential.status().depth_synchronized,
    );
}

#[test]
fn publication_snapshot_then_delta() {
    let mut handle = integrity();

    let mut publication = IntegrityPublicationAdapter::new(&handle).unwrap();

    // First authoritative snapshot.
    let snapshot_outcome = handle.process(input(initial_epoch(), snapshot(100, 200)));

    assert_eq!(snapshot_outcome.decision(), IntegrityDecision::Accepted,);

    let prepared = publication.prepare(&handle, &snapshot_outcome).unwrap();

    assert_eq!(prepared.events().len(), 1);

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Snapshot { .. }
    ));

    // Simulate a successful ordered handoff.
    publication.acknowledge(prepared).unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::Ready,);

    assert_eq!(publication.state().publication_revision, 1,);

    // Next valid depth delta.
    let delta_outcome = handle.process(input(initial_epoch(), delta(101, 201, 5)));

    let prepared = publication.prepare(&handle, &delta_outcome).unwrap();

    assert_eq!(prepared.events().len(), 1);

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Delta { .. }
    ));

    publication.acknowledge(prepared).unwrap();

    assert_eq!(publication.state().publication_revision, 2,);

    assert_eq!(publication.phase(), DepthPublicationPhase::Ready,);
}

#[test]
fn publication_invalidation_after_interruption() {
    let mut handle = integrity();

    let mut publication = IntegrityPublicationAdapter::new(&handle).unwrap();

    // Establish and publish a synchronized book.
    let outcome = handle.process(input(initial_epoch(), snapshot(100, 200)));

    let prepared = publication.prepare(&handle, &outcome).unwrap();

    publication.acknowledge(prepared).unwrap();

    assert!(publication.state().published_book_generation.is_some());

    // The physical connection is interrupted.
    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    assert!(handle.book().is_none());

    // Prepare downstream invalidation.
    let prepared = publication.prepare_state_change(&handle).unwrap();

    assert_eq!(prepared.events().len(), 1);

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Invalidated { .. }
    ));

    publication.acknowledge(prepared).unwrap();

    assert_eq!(publication.state().published_book_generation, None,);

    assert_eq!(publication.phase(), DepthPublicationPhase::Ready,);
}

#[test]
fn failed_handoff_blocks_subsequent_deltas() {
    let mut handle = integrity();

    let mut publication = IntegrityPublicationAdapter::new(&handle).unwrap();

    // Establish and acknowledge the initial snapshot.
    let snapshot_outcome = handle.process(input(initial_epoch(), snapshot(100, 200)));

    let prepared = publication.prepare(&handle, &snapshot_outcome).unwrap();

    publication.acknowledge(prepared).unwrap();

    // Process a valid delta.
    let delta_outcome = handle.process(input(initial_epoch(), delta(101, 201, 5)));

    let prepared = publication.prepare(&handle, &delta_outcome).unwrap();

    // Publication handoff failed or its result is unknown.
    publication.handoff_failed(prepared).unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired,);

    // Integrity can still possess a valid local book.
    assert!(handle.book().is_some());

    // But publication cannot continue normally.
    let next_outcome = handle.process(input(initial_epoch(), delta(102, 202, 6)));

    let result = publication.prepare(&handle, &next_outcome);

    assert!(result.is_err());

    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired,);
}

#[test]
fn failed_publication_requires_invalidation_and_new_snapshot() {
    let mut handle = integrity();

    let mut publication = IntegrityPublicationAdapter::new(&handle).unwrap();

    // Establish initial authoritative snapshot.
    let initial = handle.process(input(initial_epoch(), snapshot(100, 200)));

    let prepared = publication.prepare(&handle, &initial).unwrap();

    publication.acknowledge(prepared).unwrap();

    // Simulate an uncertain delta handoff.
    let update = handle.process(input(initial_epoch(), delta(101, 201, 5)));

    let prepared = publication.prepare(&handle, &update).unwrap();

    publication.handoff_failed(prepared).unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired,);

    // Step 1: invalidate potentially inconsistent
    // downstream book state.
    let invalidation = publication.prepare_reset_invalidation().unwrap();

    assert!(
        invalidation
            .events()
            .iter()
            .any(|event| { matches!(event, DepthPublicationEvent::Invalidated { .. }) })
    );

    publication.acknowledge(invalidation).unwrap();

    // Reset is not complete yet.
    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired,);

    // Step 2: establish a fresh authoritative
    // acquisition generation.
    handle
        .on_lifecycle(IntegrityLifecycleEvent::SourceInterrupted)
        .unwrap();

    let next = epoch(11, 21, 31);

    handle
        .on_lifecycle(IntegrityLifecycleEvent::DepthGeneration(binding(next)))
        .unwrap();

    // The replacement snapshot must be accepted
    // by the current Integrity controller.
    let replacement = handle.process(input(next, snapshot(200, 300)));

    assert_eq!(replacement.decision(), IntegrityDecision::Accepted,);

    // Step 3: prepare the replacement snapshot.
    let prepared = publication
        .prepare_reset_snapshot(&handle, &replacement)
        .unwrap();

    assert_eq!(prepared.events().len(), 1);

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Snapshot { .. }
    ));

    publication.acknowledge(prepared).unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::Ready,);

    assert!(publication.state().published_book_generation.is_some());

    // Publication of new-generation deltas can resume.
    let next_update = handle.process(input(next, delta(201, 301, 7)));

    let prepared = publication.prepare(&handle, &next_update).unwrap();

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Delta { .. }
    ));

    publication.acknowledge(prepared).unwrap();
}

#[test]
fn publication_safe_batch_matches_sequential_processing() {
    const DELTAS: u64 = 1_000;

    let mut sequential = integrity();
    let mut batched = integrity();

    let mut sequential_publication = IntegrityPublicationAdapter::new(&sequential).unwrap();

    let mut batch_publication = IntegrityPublicationAdapter::new(&batched).unwrap();

    let current = initial_epoch();

    let mut sequential_decisions = Vec::new();
    let mut sequential_event_counts = Vec::new();

    // --------------------------------------------------------
    // REFERENCE: IMMEDIATE PROCESSING
    // --------------------------------------------------------

    for index in 0..=DELTAS {
        let message = if index == 0 {
            snapshot(100, 200)
        } else {
            delta(100 + index, 200 + index, 5 + index as i64)
        };

        let outcome = sequential.process(input(current, message));

        sequential_decisions.push(outcome.decision());

        let prepared = sequential_publication
            .prepare(&sequential, &outcome)
            .unwrap();

        sequential_event_counts.push(prepared.events().len());

        sequential_publication.acknowledge(prepared).unwrap();
    }

    // --------------------------------------------------------
    // BULK: ORDERED PROCESSING WITH CALLBACK
    // --------------------------------------------------------

    let inputs = std::iter::once(input(current, snapshot(100, 200))).chain(
        (1..=DELTAS).map(|index| input(current, delta(100 + index, 200 + index, 5 + index as i64))),
    );

    let mut batch_decisions = Vec::new();
    let mut batch_event_counts = Vec::new();

    batched
        .process_batch_with(inputs, |handle, outcome| {
            batch_decisions.push(outcome.decision());

            let prepared = batch_publication.prepare(handle, &outcome).unwrap();

            batch_event_counts.push(prepared.events().len());

            batch_publication.acknowledge(prepared).unwrap();

            Ok::<(), ()>(())
        })
        .unwrap();

    // --------------------------------------------------------
    // EQUIVALENCE
    // --------------------------------------------------------

    assert_eq!(sequential_decisions, batch_decisions,);

    assert!(
        batch_decisions
            .iter()
            .all(|decision| { *decision == IntegrityDecision::Accepted })
    );

    assert_eq!(sequential_event_counts, batch_event_counts,);

    assert_eq!(batch_event_counts.len(), DELTAS as usize + 1,);

    assert!(batch_event_counts.iter().all(|count| *count == 1));

    assert_eq!(
        sequential.status().depth_coverage,
        batched.status().depth_coverage,
    );

    assert_eq!(sequential_publication.state(), batch_publication.state(),);

    assert_eq!(sequential_publication.phase(), batch_publication.phase(),);

    assert_eq!(batch_publication.phase(), DepthPublicationPhase::Ready,);

    assert!(batched.book().is_some());
}

#[test]
fn publication_safe_batch_stops_after_callback_failure() {
    let mut handle = integrity();

    let current = initial_epoch();

    let inputs = vec![
        input(current, snapshot(100, 200)),
        input(current, delta(101, 201, 5)),
        input(current, delta(102, 202, 6)),
    ];

    let mut processed = 0usize;

    let result = handle.process_batch_with(inputs, |_handle, outcome| {
        processed += 1;

        assert_eq!(outcome.decision(), IntegrityDecision::Accepted,);

        if processed == 2 {
            return Err("simulated publication failure");
        }

        Ok(())
    });

    assert_eq!(result, Err("simulated publication failure"),);

    assert_eq!(processed, 2);

    // The third message was never processed.
    //
    // The book remains synchronized, but an actual
    // publication failure must be reported to the
    // publication fence before processing resumes.
    assert_eq!(handle.status().depth_synchronized, Some(true),);
}

#[test]
fn publication_rejects_second_outstanding_preparation() {
    let mut handle = integrity();

    let mut publication = IntegrityPublicationAdapter::new(&handle).unwrap();

    let outcome = handle.process(input(initial_epoch(), snapshot(100, 200)));

    let first = publication.prepare(&handle, &outcome).unwrap();

    assert!(publication.has_outstanding_preparation());

    // Attempt another preparation before acknowledging
    // the first.
    let second = publication.prepare(&handle, &outcome);

    assert!(second.is_err());

    // The first preparation remains authoritative.
    assert!(publication.has_outstanding_preparation());

    publication.acknowledge(first).unwrap();

    assert!(!publication.has_outstanding_preparation());

    assert_eq!(publication.phase(), DepthPublicationPhase::Ready,);
}

#[test]
fn lost_publication_acknowledgement_requires_reset() {
    let mut handle = integrity();

    let mut publication = IntegrityPublicationAdapter::new(&handle).unwrap();

    let initial = handle.process(input(initial_epoch(), snapshot(100, 200)));

    let prepared = publication.prepare(&handle, &initial).unwrap();

    assert!(publication.has_outstanding_preparation());

    // Simulate losing the handoff result.
    //
    // Dropping the preparation alone is not the
    // complete failure protocol. The runtime must
    // explicitly report uncertainty to the fence.
    drop(prepared);

    publication.mark_outstanding_handoff_uncertain().unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired,);

    assert!(!publication.has_outstanding_preparation());

    // Even though the local book remains synchronized,
    // normal publication must be rejected.
    assert!(handle.book().is_some());

    let next = handle.process(input(initial_epoch(), delta(101, 201, 5)));

    assert_eq!(next.decision(), IntegrityDecision::Accepted,);

    let result = publication.prepare(&handle, &next);

    assert!(result.is_err());

    // Publication can only resume through its
    // explicit invalidation and replacement protocol.
    let invalidation = publication.prepare_reset_invalidation().unwrap();

    publication.acknowledge(invalidation).unwrap();

    assert_eq!(publication.phase(), DepthPublicationPhase::ResetRequired,);
}
