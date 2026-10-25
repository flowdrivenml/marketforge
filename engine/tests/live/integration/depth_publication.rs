use marketforge_engine::live::integrity::DepthBookLimits;
use marketforge_engine::live::integrity::DepthTimestampComposition;
use marketforge_engine::{
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate, SequenceMetadata},
    live::{
        config::format::DepthFormatConfig,
        infrastructure::normalization::{DepthMessageKind, NormalizationSource, NormalizedDepth},
        integrity::{
            BoundDepthIngress, DepthBufferLimits, DepthContinuityCompiler, DepthControllerOutcome,
            DepthPublicationError, DepthPublicationEvent, DepthPublicationFence,
            DepthPublicationPhase, DepthResourceLimits, DepthSourceBinding, DepthSourceEpoch,
        },
    },
};
use std::path::PathBuf;

use rust_decimal::Decimal;

const STREAM: &str = "integration:bybit:spot:BTCUSDT:depth";

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
        Exchange::Bybit,
        "spot",
        42,
        "BTCUSDT",
        Some(200),
        epoch(generation),
    )
    .unwrap()
}

fn ingress() -> BoundDepthIngress {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/live/formats/depth/bybit_spot.toml");

    let config = DepthFormatConfig::load(path).unwrap();
    let plan = DepthContinuityCompiler::compile(&config).unwrap();

    BoundDepthIngress::new(
        binding(10),
        plan,
        DepthBufferLimits {
            max_messages: 16,
            max_bytes: 1_000_000,
        },
        DepthResourceLimits::default(),
    )
    .unwrap()
}

fn level(side: BookSide, price: i64, quantity: i64) -> L2LevelUpdate {
    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: 1_000_000_000,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
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

fn message(
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
        match kind {
            DepthMessageKind::Snapshot => vec![
                level(BookSide::Bid, 80_000, quantity),
                level(BookSide::Ask, 80_010, 2),
            ],
            DepthMessageKind::Delta => vec![level(BookSide::Bid, 80_000, quantity)],
        },
        Some(1_000_000_000),
        Some(200),
    )
    .unwrap()
}

fn snapshot(update_id: u64, cross_sequence: u64, quantity: i64) -> NormalizedDepth {
    message(
        DepthMessageKind::Snapshot,
        update_id,
        cross_sequence,
        quantity,
    )
}

fn delta(update_id: u64, cross_sequence: u64, quantity: i64) -> NormalizedDepth {
    message(DepthMessageKind::Delta, update_id, cross_sequence, quantity)
}

#[test]
fn snapshot_and_deltas_reach_publication_in_order() {
    let mut ingress = ingress();

    let mut fence = DepthPublicationFence::new(STREAM, 10).unwrap();

    // --------------------------------------------------------
    // INITIAL SNAPSHOT
    // --------------------------------------------------------

    let initial = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    assert!(matches!(
        initial,
        DepthControllerOutcome::Accepted {
            snapshot: true,
            book_generation: 1,
            ..
        }
    ));

    assert!(ingress.is_synchronized());

    let prepared = fence.prepare(ingress.controller(), &initial).unwrap();

    assert_eq!(prepared.events().len(), 1);

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Snapshot {
            stream_generation: 10,
            book_generation: 1,
            ..
        }
    ));

    // Publication preparation alone does not authorize
    // any future delta publication.
    assert_eq!(fence.phase(), DepthPublicationPhase::Pending);

    fence.acknowledge(prepared).unwrap();

    assert_eq!(fence.state().publication_revision, 1);

    // --------------------------------------------------------
    // FIRST DELTA
    // --------------------------------------------------------

    let first = ingress.on_websocket(epoch(10), delta(101, 201, 5), 1024);

    assert!(matches!(
        first,
        DepthControllerOutcome::Accepted {
            snapshot: false,
            book_generation: 1,
            ..
        }
    ));

    let prepared = fence.prepare(ingress.controller(), &first).unwrap();

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Delta {
            stream_generation: 10,
            book_generation: 1,
            ..
        }
    ));

    fence.acknowledge(prepared).unwrap();

    assert_eq!(fence.state().publication_revision, 2);

    // --------------------------------------------------------
    // SECOND DELTA
    // --------------------------------------------------------

    let second = ingress.on_websocket(epoch(10), delta(102, 202, 7), 1024);

    let prepared = fence.prepare(ingress.controller(), &second).unwrap();

    fence.acknowledge(prepared).unwrap();

    assert_eq!(fence.state().publication_revision, 3);

    assert_eq!(fence.state().published_book_generation, Some(1),);

    assert_eq!(ingress.controller().baseline().unwrap().last_sequence, 102,);

    assert_eq!(
        ingress
            .controller()
            .book()
            .bids()
            .get(&Decimal::from(80_000))
            .unwrap()
            .quantity_base,
        Some(Decimal::from(7)),
    );
}

#[test]
fn pending_publication_prevents_next_handoff() {
    let mut ingress = ingress();

    let mut fence = DepthPublicationFence::new(STREAM, 10).unwrap();

    let initial = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    let pending = fence.prepare(ingress.controller(), &initial).unwrap();

    assert!(fence.has_outstanding_preparation());

    // This test deliberately simulates an integration bug:
    // the source actor advances ingress before resolving
    // the previous publication.
    let next = ingress.on_websocket(epoch(10), delta(101, 201, 5), 1024);

    assert!(matches!(
        fence.prepare(ingress.controller(), &next),
        Err(DepthPublicationError::PreparationOutstanding)
    ));

    // The fence does not silently acknowledge an earlier
    // publication merely because ingress advanced.
    assert_eq!(fence.state().publication_revision, 0);

    fence.handoff_failed(pending).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired,);
}

#[test]
fn failed_publication_requires_invalidation_and_snapshot() {
    let mut ingress = ingress();

    let mut fence = DepthPublicationFence::new(STREAM, 10).unwrap();

    let initial = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    let prepared = fence.prepare(ingress.controller(), &initial).unwrap();

    fence.acknowledge(prepared).unwrap();

    // --------------------------------------------------------
    // FAILED DELTA HANDOFF
    // --------------------------------------------------------

    let failed = ingress.on_websocket(epoch(10), delta(101, 201, 5), 1024);

    let prepared = fence.prepare(ingress.controller(), &failed).unwrap();

    fence.handoff_failed(prepared).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    // --------------------------------------------------------
    // INVALIDATE THE CONSUMER
    // --------------------------------------------------------

    let invalidation = fence.prepare_reset_invalidation().unwrap();

    assert!(invalidation.events().iter().any(|event| {
        matches!(
            event,
            DepthPublicationEvent::Invalidated {
                book_generation: 1,
                ..
            }
        )
    }));

    fence.acknowledge(invalidation).unwrap();

    // Invalidation alone does not permit new deltas.
    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    // --------------------------------------------------------
    // REESTABLISH A COMPLETE BOOK
    // --------------------------------------------------------

    ingress.invalidate();

    let replacement = ingress.on_websocket(epoch(10), snapshot(200, 300, 8), 1024);

    let prepared = fence
        .prepare_reset_snapshot(ingress.controller(), &replacement)
        .unwrap();

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Snapshot {
            book_generation: 2,
            ..
        }
    ));

    fence.acknowledge(prepared).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::Ready);

    assert_eq!(fence.state().published_book_generation, Some(2),);

    // --------------------------------------------------------
    // DELTA PUBLICATION RESUMES
    // --------------------------------------------------------

    let next = ingress.on_websocket(epoch(10), delta(201, 301, 9), 1024);

    let prepared = fence.prepare(ingress.controller(), &next).unwrap();

    fence.acknowledge(prepared).unwrap();

    assert_eq!(fence.state().publication_revision, 4);
}

#[test]
fn obsolete_source_epoch_cannot_mutate_authoritative_book() {
    let mut ingress = ingress();

    ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    let original = ingress.controller().baseline().unwrap();

    // Correct payload, wrong acquisition epoch.
    let stale = ingress.on_websocket(epoch(9), delta(101, 201, 999), 1024);

    assert!(matches!(stale, DepthControllerOutcome::Ignored { .. }));

    assert!(ingress.is_synchronized());

    assert_eq!(ingress.controller().baseline(), Some(original),);

    assert_eq!(
        ingress
            .controller()
            .book()
            .bids()
            .get(&Decimal::from(80_000))
            .unwrap()
            .quantity_base,
        Some(Decimal::from(3)),
    );
}

#[test]
fn invalid_sequence_revokes_publication_authority() {
    let mut ingress = ingress();

    let mut fence = DepthPublicationFence::new(STREAM, 10).unwrap();

    let initial = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    let prepared = fence.prepare(ingress.controller(), &initial).unwrap();

    fence.acknowledge(prepared).unwrap();

    // The update ID advances, but cross-sequence regresses.
    let failure = ingress.on_websocket(epoch(10), delta(101, 199, 5), 1024);

    assert!(matches!(
        failure,
        DepthControllerOutcome::RecoveryRequired { .. }
    ));

    assert!(!ingress.is_synchronized());

    let prepared = fence.prepare(ingress.controller(), &failure).unwrap();

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Invalidated {
            stream_generation: 10,
            book_generation: 1,
        }
    ));

    fence.acknowledge(prepared).unwrap();

    assert!(!fence.has_published_book());
}

#[test]
fn retained_book_limits_apply_to_websocket_snapshots() {
    let mut ingress = ingress();

    let limits = DepthBookLimits {
        max_levels: 1,
        max_estimated_bytes: 32 * 1024 * 1024,
    };

    ingress.set_book_limits(limits).unwrap();

    // The Bybit snapshot contains one bid and one ask.
    // Both count against the combined retained-level budget.
    let outcome = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    assert!(matches!(
        outcome,
        DepthControllerOutcome::RecoveryRequired { .. }
    ));

    assert!(!ingress.is_synchronized());
    assert!(!ingress.controller().book().is_initialized());

    assert_eq!(ingress.book_limits(), limits);
}

#[test]
fn synchronized_websocket_book_rejects_limit_changes() {
    let mut ingress = ingress();

    let original = ingress.book_limits();

    let outcome = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    assert!(matches!(
        outcome,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    assert!(ingress.is_synchronized());

    let replacement = DepthBookLimits {
        max_levels: 100,
        max_estimated_bytes: 16 * 1024 * 1024,
    };

    assert!(ingress.set_book_limits(replacement).is_err());

    assert_eq!(ingress.book_limits(), original);

    // Rejected configuration must leave synchronization
    // and the authoritative book intact.
    assert!(ingress.is_synchronized());
    assert!(ingress.controller().book().is_initialized());
}

#[test]
fn invalidated_websocket_book_allows_limit_reconfiguration() {
    let mut ingress = ingress();

    let initial = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    assert!(matches!(
        initial,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    assert!(ingress.is_synchronized());

    let limits = DepthBookLimits {
        max_levels: 10,
        max_estimated_bytes: 16 * 1024 * 1024,
    };

    // Cannot change while synchronized.
    assert!(ingress.set_book_limits(limits).is_err());

    // Explicitly retire authoritative state.
    ingress.invalidate();

    assert!(!ingress.is_synchronized());

    // A new policy can now be installed.
    ingress.set_book_limits(limits).unwrap();

    assert_eq!(ingress.book_limits(), limits);

    // New synchronization uses the updated policy.
    let recovered = ingress.on_websocket(epoch(10), snapshot(200, 300, 8), 1024);

    assert!(matches!(
        recovered,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    assert!(ingress.is_synchronized());
    assert_eq!(ingress.book_limits(), limits);
}

#[test]
fn publication_fence_rejects_foreign_preparation() {
    let mut ingress = ingress();

    let outcome = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    assert!(matches!(
        outcome,
        DepthControllerOutcome::Accepted { snapshot: true, .. }
    ));

    let mut fence_a = DepthPublicationFence::new(STREAM, 10).unwrap();

    let mut fence_b = DepthPublicationFence::new(STREAM, 10).unwrap();

    // Each fence prepares the same valid snapshot.
    let prepared_a = fence_a.prepare(ingress.controller(), &outcome).unwrap();

    let prepared_b = fence_b.prepare(ingress.controller(), &outcome).unwrap();

    assert_eq!(fence_a.phase(), DepthPublicationPhase::Pending);

    assert_eq!(fence_b.phase(), DepthPublicationPhase::Pending);

    // Attempt to acknowledge A's preparation on B.
    //
    // Matching stream, generation, revision and book
    // identity must not be sufficient authorization.
    assert!(
        fence_b.acknowledge(prepared_a).is_err(),
        "a foreign fence preparation was accepted"
    );

    assert_eq!(fence_b.state().publication_revision, 0);

    assert!(
        !fence_b.has_published_book(),
        "foreign acknowledgement established book authority"
    );

    // Rejected acknowledgement must not consume B's
    // legitimate outstanding preparation.
    assert!(fence_b.has_outstanding_preparation());

    fence_b.acknowledge(prepared_b).unwrap();

    assert_eq!(fence_b.state().publication_revision, 1);

    assert!(fence_b.has_published_book());
}

#[test]
fn repeated_failed_reset_handoffs_preserve_invalidation() {
    let mut ingress = ingress();

    let mut fence = DepthPublicationFence::new(STREAM, 10).unwrap();

    let initial = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    assert!(matches!(
        initial,
        DepthControllerOutcome::Accepted {
            snapshot: true,
            book_generation: 1,
            ..
        }
    ));

    // The snapshot could have reached the consumer,
    // but the queue handoff was not confirmed.
    let prepared = fence.prepare(ingress.controller(), &initial).unwrap();

    fence.handoff_failed(prepared).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    // First reset attempt fails.
    let reset_1 = fence.prepare_reset_invalidation().unwrap();

    assert!(
        reset_1.events().iter().any(|event| {
            matches!(
                event,
                DepthPublicationEvent::Invalidated {
                    book_generation: 1,
                    ..
                }
            )
        }),
        "first reset omitted potentially visible book"
    );

    fence.handoff_failed(reset_1).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    // Second reset attempt fails.
    let reset_2 = fence.prepare_reset_invalidation().unwrap();

    assert!(
        reset_2.events().iter().any(|event| {
            matches!(
                event,
                DepthPublicationEvent::Invalidated {
                    book_generation: 1,
                    ..
                }
            )
        }),
        "second reset lost uncertain book identity"
    );

    fence.handoff_failed(reset_2).unwrap();

    // Third attempt must still carry invalidation.
    let reset_3 = fence.prepare_reset_invalidation().unwrap();

    assert!(
        reset_3.events().iter().any(|event| {
            matches!(
                event,
                DepthPublicationEvent::Invalidated {
                    book_generation: 1,
                    ..
                }
            )
        }),
        "repeated failures discarded invalidation authority"
    );

    fence.acknowledge(reset_3).unwrap();

    // Invalidation has been acknowledged, but no new
    // authoritative consumer snapshot exists.
    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    assert!(!fence.has_published_book());

    // An ordinary delta must still be prohibited.
    let next = ingress.on_websocket(epoch(10), delta(101, 201, 5), 1024);

    assert!(matches!(
        next,
        DepthControllerOutcome::Accepted {
            snapshot: false,
            ..
        }
    ));

    assert!(
        fence.prepare(ingress.controller(), &next).is_err(),
        "delta publication bypassed required reset snapshot"
    );
}

#[test]
fn stale_snapshot_cannot_restore_publication_after_reset() {
    let mut ingress = ingress();

    let mut fence = DepthPublicationFence::new(STREAM, 10).unwrap();

    // --------------------------------------------------------
    // FIRST BOOK
    // --------------------------------------------------------

    let old_snapshot = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    let prepared = fence.prepare(ingress.controller(), &old_snapshot).unwrap();

    fence.handoff_failed(prepared).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    let invalidation = fence.prepare_reset_invalidation().unwrap();

    fence.acknowledge(invalidation).unwrap();

    // --------------------------------------------------------
    // REPLACE CONTROLLER STATE
    // --------------------------------------------------------

    ingress.invalidate();

    let fresh_snapshot = ingress.on_websocket(epoch(10), snapshot(200, 300, 8), 1024);

    assert!(matches!(
        fresh_snapshot,
        DepthControllerOutcome::Accepted {
            snapshot: true,
            book_generation: 2,
            ..
        }
    ));

    // --------------------------------------------------------
    // ATTEMPT TO RESTORE AN OBSOLETE SNAPSHOT
    // --------------------------------------------------------

    let stale = fence.prepare_reset_snapshot(ingress.controller(), &old_snapshot);

    assert!(
        stale.is_err(),
        "stale snapshot restored publication authority"
    );

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    assert!(!fence.has_published_book());

    // --------------------------------------------------------
    // FRESH SNAPSHOT MUST WORK
    // --------------------------------------------------------

    let prepared = fence
        .prepare_reset_snapshot(ingress.controller(), &fresh_snapshot)
        .unwrap();

    assert!(prepared.events().iter().any(|event| {
        matches!(
            event,
            DepthPublicationEvent::Snapshot {
                book_generation: 2,
                ..
            }
        )
    }));

    fence.acknowledge(prepared).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::Ready);

    assert_eq!(fence.state().published_book_generation, Some(2));
}

#[test]
fn publication_fence_rejects_foreign_handoff_failure() {
    let mut ingress = ingress();

    let outcome = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    let mut fence_a = DepthPublicationFence::new(STREAM, 10).unwrap();

    let mut fence_b = DepthPublicationFence::new(STREAM, 10).unwrap();

    let prepared_a = fence_a.prepare(ingress.controller(), &outcome).unwrap();

    let prepared_b = fence_b.prepare(ingress.controller(), &outcome).unwrap();

    // A's preparation cannot report a delivery
    // failure against B's publication state.
    assert!(matches!(
        fence_b.handoff_failed(prepared_a),
        Err(DepthPublicationError::ForeignPreparation)
    ));

    // B must remain unchanged.
    assert_eq!(fence_b.phase(), DepthPublicationPhase::Pending);

    assert_eq!(fence_b.state().publication_revision, 0);

    assert!(fence_b.has_outstanding_preparation());

    // B's legitimate token remains valid.
    fence_b.acknowledge(prepared_b).unwrap();

    assert_eq!(fence_b.phase(), DepthPublicationPhase::Ready);

    assert_eq!(fence_b.state().publication_revision, 1);

    assert!(fence_b.has_published_book());
}

#[test]
fn repeated_reset_failures_preserve_all_uncertain_book_generations() {
    let mut ingress = ingress();

    let mut fence = DepthPublicationFence::new(STREAM, 10).unwrap();

    // --------------------------------------------------------
    // BOOK GENERATION 1 — ACKNOWLEDGED
    // --------------------------------------------------------

    let initial = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    assert!(matches!(
        initial,
        DepthControllerOutcome::Accepted {
            snapshot: true,
            book_generation: 1,
            ..
        }
    ));

    let prepared = fence.prepare(ingress.controller(), &initial).unwrap();

    fence.acknowledge(prepared).unwrap();

    assert_eq!(fence.state().published_book_generation, Some(1));

    // --------------------------------------------------------
    // BOOK GENERATION 2 — UNCERTAIN HANDOFF
    // --------------------------------------------------------

    let replacement = ingress.on_websocket(epoch(10), snapshot(200, 300, 8), 1024);

    assert!(matches!(
        replacement,
        DepthControllerOutcome::Accepted {
            snapshot: true,
            book_generation: 2,
            ..
        }
    ));

    let prepared = fence.prepare(ingress.controller(), &replacement).unwrap();

    // A replacement must revoke the previous book before
    // installing the next snapshot.
    assert!(prepared.events().iter().any(|event| {
        matches!(
            event,
            DepthPublicationEvent::Invalidated {
                book_generation: 1,
                ..
            }
        )
    }));

    assert!(prepared.events().iter().any(|event| {
        matches!(
            event,
            DepthPublicationEvent::Snapshot {
                book_generation: 2,
                ..
            }
        )
    }));

    // The queue handoff fails.
    //
    // Some or all events might have reached the consumer.
    fence.handoff_failed(prepared).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    // The last ACKNOWLEDGED state remains generation 1.
    assert_eq!(fence.state().published_book_generation, Some(1));

    // --------------------------------------------------------
    // RETRY INVALIDATION THREE TIMES
    // --------------------------------------------------------

    for attempt in 1..=3 {
        let prepared = fence.prepare_reset_invalidation().unwrap();

        let invalidated: Vec<u64> = prepared
            .events()
            .iter()
            .filter_map(|event| match event {
                DepthPublicationEvent::Invalidated {
                    book_generation, ..
                } => Some(*book_generation),

                _ => None,
            })
            .collect();

        assert!(
            invalidated.contains(&1),
            "attempt {attempt}: lost acknowledged generation 1"
        );

        assert!(
            invalidated.contains(&2),
            "attempt {attempt}: lost uncertain generation 2"
        );

        if attempt < 3 {
            fence.handoff_failed(prepared).unwrap();

            assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);
        } else {
            // Only the final invalidation is acknowledged.
            fence.acknowledge(prepared).unwrap();
        }
    }

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    assert!(!fence.has_published_book());

    // --------------------------------------------------------
    // INCREMENTAL PUBLICATION MUST REMAIN BLOCKED
    // --------------------------------------------------------

    let next = ingress.on_websocket(epoch(10), delta(201, 301, 9), 1024);

    assert!(matches!(
        next,
        DepthControllerOutcome::Accepted {
            snapshot: false,
            ..
        }
    ));

    assert!(
        fence.prepare(ingress.controller(), &next).is_err(),
        "delta bypassed the reset-snapshot requirement"
    );
}

#[test]
fn current_authoritative_snapshot_can_restore_after_delivery_reset() {
    let mut ingress = ingress();

    let mut fence = DepthPublicationFence::new(STREAM, 10).unwrap();

    // --------------------------------------------------------
    // AUTHORITATIVE SNAPSHOT
    // --------------------------------------------------------

    let accepted = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    assert!(matches!(
        accepted,
        DepthControllerOutcome::Accepted {
            snapshot: true,
            book_generation: 1,
            ..
        }
    ));

    let baseline_before = ingress.controller().baseline();

    // --------------------------------------------------------
    // PUBLICATION FAILS, NOT ACQUISITION
    // --------------------------------------------------------

    let prepared = fence.prepare(ingress.controller(), &accepted).unwrap();

    fence.handoff_failed(prepared).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    // The authoritative acquisition state remains valid.
    assert!(ingress.is_synchronized());

    assert_eq!(ingress.controller().baseline(), baseline_before);

    // --------------------------------------------------------
    // INVALIDATE UNCERTAIN DOWNSTREAM STATE
    // --------------------------------------------------------

    let invalidation = fence.prepare_reset_invalidation().unwrap();

    fence.acknowledge(invalidation).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::ResetRequired);

    // --------------------------------------------------------
    // RESTORE USING STILL-CURRENT FULL SNAPSHOT
    // --------------------------------------------------------

    let prepared = fence
        .prepare_reset_snapshot(ingress.controller(), &accepted)
        .unwrap();

    assert_eq!(prepared.events().len(), 1);

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Snapshot {
            book_generation: 1,
            ..
        }
    ));

    fence.acknowledge(prepared).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::Ready);

    assert_eq!(fence.state().published_book_generation, Some(1));

    // --------------------------------------------------------
    // NORMAL INCREMENTAL PUBLICATION RESUMES
    // --------------------------------------------------------

    let next = ingress.on_websocket(epoch(10), delta(101, 201, 5), 1024);

    let prepared = fence.prepare(ingress.controller(), &next).unwrap();

    assert!(matches!(
        prepared.events()[0],
        DepthPublicationEvent::Delta { .. }
    ));

    fence.acknowledge(prepared).unwrap();

    assert_eq!(fence.phase(), DepthPublicationPhase::Ready);
}

#[test]
fn legacy_depth_publication_does_not_invent_timestamp_provenance() {
    let mut ingress = ingress();

    let initial = ingress.on_websocket(epoch(10), snapshot(100, 200, 3), 1024);

    let accepted_timestamp = match &initial {
        DepthControllerOutcome::Accepted {
            timestamp,
            snapshot: true,
            ..
        } => *timestamp,

        other => panic!("expected accepted snapshot: {other:?}"),
    };

    // This fixture uses the legacy NormalizedDepth::new()
    // constructor, which does not establish provenance.
    assert_eq!(accepted_timestamp.latest_source, None,);

    assert_eq!(
        accepted_timestamp.composition,
        DepthTimestampComposition::SingleMessage,
    );

    let mut fence = DepthPublicationFence::new(STREAM, 10).unwrap();

    let prepared = fence.prepare(ingress.controller(), &initial).unwrap();

    let published_timestamp = match prepared.events()[0] {
        DepthPublicationEvent::Snapshot { timestamp, .. } => timestamp,

        other => panic!("expected snapshot publication: {other:?}"),
    };

    assert_eq!(published_timestamp, accepted_timestamp,);

    assert_eq!(published_timestamp.latest_source, None,);

    fence.acknowledge(prepared).unwrap();
}
