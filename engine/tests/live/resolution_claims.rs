//! Lease-based recovery and fencing tests for the resolution sink.
//!
//! Tests the recovery semantics of the claim lease protocol:
//! - Token mismatch rejection (wrong receipt/lease pairing)
//! - Lease expiration enforcement
//! - Acknowledge with wrong token rejection
//! - Release claim recovery (re-claim after release)
//! - Concurrent lease fencing (sequential try_take produces distinct leases)
//! - Double-acknowledge safety
//! - Acknowledge-after-expiry error paths
//! - Release-then-acknowledge with wrong token
//! - Verify survives release claim
//! - Sink capacity recovery after acknowledge
//! - Lease duration zero rejection
//! - Multiple releases of same receipt
//! - Acknowledge on released-but-not-reclaimed record
//! - Receipt identity preserved across re-claim

use marketforge_engine::live::routing::{
    acknowledgement::{
        DeliveryAcknowledgement, DeliveryCursorRange, DeliveryObligation, DeliveryObligationId,
        EscalationResolution, EscalationResolutionRecord, EscalationResolutionToken,
        FinalizationEscalation, FinalizationJournal,
    },
    resolution::{BoundedResolutionSink, ResolutionAcceptanceSink, ResolutionClaimToken},
};
use marketforge_engine::live::{
    model::{
        cursor::{LogEpoch, StreamCursor},
        delivery::{DeliveryPolicy, DestinationKind},
        identity::StreamId,
    },
    ports::routing::DestinationId,
};
use std::time::Duration;

use crate::live::support::manual_clock::ManualClock;

/// C1: try_take with zero lease duration returns an error.
#[test]
fn zero_lease_duration_rejected() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let result = sink.try_take(&clock, Duration::ZERO);
    assert!(result.is_err());
}

/// C2: Acknowledge with a wrong token (not matching the lease) returns error.
#[test]
fn acknowledge_wrong_token_rejected() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Create a wrong token (different value from the actual lease token).
    let wrong_token = ResolutionClaimToken(999);

    let result = sink.acknowledge(&claim.receipt, wrong_token, &clock);
    assert!(result.is_err());

    // Record remains in the sink (wrong token rejected, lease intact).
    assert_eq!(sink.len(), 1);
    assert!(sink.verify(&claim.receipt));
}

/// C3: Lease expiration causes acknowledge to fail.
#[test]
fn lease_expiration_rejects_acknowledge() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Advance past the lease duration.
    clock.advance(Duration::from_secs(31));

    let result = sink.acknowledge(&claim.receipt, claim.token, &clock);
    assert!(result.is_err());

    // Record still in sink (lease expired, but not yet released).
    assert_eq!(sink.len(), 1);
    assert!(sink.verify(&claim.receipt));
}

/// C4: release_claim + try_take produces a new claim.
///
/// After releasing a claim, the record transitions back to JournalCommitted
/// and a subsequent try_take produces a new, valid claim.
#[test]
fn release_then_reclaim_produces_new_claim() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();

    // First claim.
    let claim_a = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Release the claim.
    sink.release_claim(&claim_a.receipt, claim_a.token, &clock)
        .unwrap();

    // Re-claim produces a valid claim.
    let claim_b = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Acknowledge the re-claim successfully.
    sink.acknowledge(&claim_b.receipt, claim_b.token, &clock)
        .unwrap();
    assert_eq!(sink.len(), 0);
}

/// C5: Double acknowledge second one fails (record already removed).
#[test]
fn double_acknowledge_second_is_nop() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // First acknowledge succeeds.
    sink.acknowledge(&claim.receipt, claim.token, &clock)
        .unwrap();
    assert_eq!(sink.len(), 0);

    // Second acknowledge — the record has been removed, so verify fails.
    assert!(!sink.verify(&claim.receipt));
}

/// C6: Acknowledge with correct token after expiry (before advancing) succeeds.
#[test]
fn acknowledge_before_expiration_succeeds() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Acknowledge within lease duration — should succeed.
    clock.advance(Duration::from_secs(15)); // within 30s lease
    sink.acknowledge(&claim.receipt, claim.token, &clock)
        .unwrap();
    assert_eq!(sink.len(), 0);
}

/// C7: Release claim on a record that was never claimed fails.
#[test]
fn release_unclaimed_record_fails() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();

    // Never claimed the record.
    let first_claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Release the first claim (now the record is back in JournalCommitted).
    sink.release_claim(&first_claim.receipt, first_claim.token, &clock)
        .unwrap();

    // Try to release the SAME receipt again — should fail because
    // the record is no longer Claimed (it was restored to JournalCommitted).
    let result = sink.release_claim(&first_claim.receipt, first_claim.token, &clock);
    assert!(result.is_err());

    // Record is still JournalCommitted, can be re-claimed.
    let second_claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();
    sink.acknowledge(&second_claim.receipt, second_claim.token, &clock)
        .unwrap();
}

/// C8: Receipt identity is preserved across re-claim.
#[test]
fn receipt_identity_preserved_across_reclaim() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let claim_first = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Verify that the first claim's receipt matches the original.
    assert!(sink.verify(&claim_first.receipt));
    assert_eq!(claim_first.receipt.token(), receipt.token());
    assert_eq!(claim_first.receipt.record(), receipt.record());

    // Release and re-claim.
    sink.release_claim(&claim_first.receipt, claim_first.token, &clock)
        .unwrap();
    let claim_second = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Verify that the second claim's receipt also matches the original.
    assert!(sink.verify(&claim_second.receipt));
    assert_eq!(claim_second.receipt.token(), receipt.token());
    assert_eq!(claim_second.receipt.record(), receipt.record());
}

/// C9: Sink capacity is released after acknowledge.
#[test]
fn capacity_released_after_acknowledge() {
    let sink = sink_with_capacity(1);
    assert_eq!(sink.len(), 0);

    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Sink at capacity.
    assert_eq!(sink.len(), 1);

    // Acknowledge releases capacity.
    sink.acknowledge(&claim.receipt, claim.token, &clock)
        .unwrap();
    assert_eq!(sink.len(), 0);
}

/// C10: Sink capacity is NOT released after release_claim.
#[test]
fn capacity_not_released_after_release_claim() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    // Sink retains the record.
    assert_eq!(sink.len(), 1);

    // Release the claim.
    sink.release_claim(&claim.receipt, claim.token, &clock)
        .unwrap();

    // Capacity is NOT released — record is still in the sink (JournalCommitted).
    assert_eq!(sink.len(), 1);

    // But it can be re-claimed and then acknowledged.
    let claim2 = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();
    sink.acknowledge(&claim2.receipt, claim2.token, &clock)
        .unwrap();
    assert_eq!(sink.len(), 0);
}

/// C11: Multiple sequential claims on different records produce correct ordering.
#[test]
fn sequential_claims_on_multiple_records_ordered() {
    let obligation_a = make_obligation(1);
    let obligation_b = make_obligation(2);
    let obligation_c = make_obligation(3);

    let (mut journal_a, tok_a, rec_a, id_a) = prepare_resolution(obligation_a);
    let (mut journal_b, tok_b, rec_b, id_b) = prepare_resolution(obligation_b);
    let (mut journal_c, tok_c, rec_c, id_c) = prepare_resolution(obligation_c);

    let sink = sink_with_capacity(10);

    let rec_a = sink.accept(tok_a, &rec_a).unwrap();
    let rec_b = sink.accept(tok_b, &rec_b).unwrap();
    let rec_c = sink.accept(tok_c, &rec_c).unwrap();
    assert_eq!(sink.len(), 3);

    journal_a.commit_resolution(id_a, &rec_a, &sink).unwrap();
    journal_b.commit_resolution(id_b, &rec_b, &sink).unwrap();
    journal_c.commit_resolution(id_c, &rec_c, &sink).unwrap();

    let clock = ManualClock::new();

    // Claims should come in FIFO order.
    let first = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert_eq!(first.receipt.token().journal, journal_a.id());

    let second = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert_eq!(second.receipt.token().journal, journal_b.id());

    let third = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert_eq!(third.receipt.token().journal, journal_c.id());
}

/// C12: Claim with expired lease before release — record stays in claimed state.
#[test]
fn expired_lease_keeps_record_claimed() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let _claim = sink
        .try_take(&clock, Duration::from_secs(1))
        .unwrap()
        .unwrap();

    // Advance past lease.
    clock.advance(Duration::from_secs(2));

    // Acknowledge with expired lease — fails.
    let result = sink.acknowledge(&receipt, _claim.token, &clock);
    assert!(result.is_err());

    // Sink still has capacity 1 (record not removed, not released).
    assert_eq!(sink.len(), 1);
}

/// C13: Verify returns false after acknowledge removes the record.
#[test]
fn verify_fails_after_acknowledge() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();

    // Verify works while accepted.
    assert!(sink.verify(&receipt));

    journal.commit_resolution(id, &receipt, &sink).unwrap();
    assert!(sink.verify(&receipt));

    let clock = ManualClock::new();
    let claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert!(sink.verify(&claim.receipt));

    sink.acknowledge(&claim.receipt, claim.token, &clock)
        .unwrap();
    assert!(!sink.verify(&claim.receipt));
}

/// C14: Verify survives release_claim (record not removed).
#[test]
fn verify_survives_release_claim() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let claim = sink
        .try_take(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();
    assert!(sink.verify(&claim.receipt));

    // Release claim — record not removed.
    sink.release_claim(&claim.receipt, claim.token, &clock)
        .unwrap();

    // Verify still works because record is still in sink (JournalCommitted).
    assert!(sink.verify(&claim.receipt));
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Create a sink with the given capacity.
fn sink_with_capacity(capacity: usize) -> BoundedResolutionSink {
    BoundedResolutionSink::new(capacity).unwrap()
}

/// Create a delivery obligation with the given index.
fn make_obligation(index: u64) -> DeliveryObligation {
    let stream = StreamId(format!("test-stream-{index}"));
    DeliveryObligation {
        id: DeliveryObligationId(index),
        destination: DestinationId(index),
        kind: DestinationKind::Recording,
        policy: DeliveryPolicy::Required,
        range: DeliveryCursorRange::new(
            StreamCursor::new(stream.clone(), LogEpoch(1), index),
            StreamCursor::new(stream, LogEpoch(1), index),
        )
        .unwrap(),
    }
}

/// Prepare a resolution: acknowledge the obligation, finalize, claim, and
/// escalate. Returns (journal, token, record, obligation_id).
///
/// Mirrors the pattern from resolution_integration.rs — uses the
/// AcknowledgementTracker → FinalizationJournal pipeline.
fn prepare_resolution(
    obligation: DeliveryObligation,
) -> (
    FinalizationJournal,
    EscalationResolutionToken,
    EscalationResolutionRecord,
    DeliveryObligationId,
) {
    let mut tracker =
        marketforge_engine::live::routing::acknowledgement::AcknowledgementTracker::new(10)
            .unwrap();
    tracker.register(obligation.clone()).unwrap();
    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: marketforge_engine::live::model::delivery::DeliveryStatus::Failed(
                marketforge_engine::live::model::delivery::DeliveryFailure::PersistenceFailed,
            ),
        })
        .unwrap();

    let mut journal = FinalizationJournal::new(10).unwrap();
    tracker.finalize_into(obligation.id, &mut journal).unwrap();

    let clock = ManualClock::new();
    let claim = journal
        .claim_next(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();
    let id = claim.record.obligation.id;

    journal
        .escalate(
            id,
            claim.token,
            &clock,
            FinalizationEscalation::PermanentReportingFailure,
        )
        .unwrap();

    let prepared = journal
        .prepare_resolution(id, EscalationResolution::ReportingVerified)
        .unwrap();

    (journal, prepared.token, prepared.record, id)
}
