//! Lifecycle transition tests for the resolution sink protocol.
//!
//! Verifies the state machine enforcement:
//!   Accepted → JournalCommitted → Claimed → removed
//!
//! Records transition between states only through their intended
//! operations, preventing fallthrough that could allow downstream
//! consumers to claim or acknowledge resolution records before the
//! originating FinalizationJournal commits ownership transfer.

use marketforge_engine::live::routing::{
    acknowledgement::{
        DeliveryCursorRange, DeliveryObligation, DeliveryObligationId, EscalationResolution,
        EscalationResolutionRecord, EscalationResolutionToken, FinalizationEscalation,
        FinalizationJournal, FinalizationRecord, ObligationFinalization,
    },
    resolution::{BoundedResolutionSink, ResolutionAcceptanceSink},
};
use marketforge_engine::live::routing::resolution::ResolutionClaim;
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

fn make_obligation(id: u64) -> DeliveryObligation {
    let stream = StreamId("lifecycle-test".to_owned());
    DeliveryObligation {
        id: DeliveryObligationId(id),
        destination: DestinationId(42),
        kind: DestinationKind::Recording,
        policy: DeliveryPolicy::Required,
        range: DeliveryCursorRange::new(
            StreamCursor::new(stream.clone(), LogEpoch(1), id),
            StreamCursor::new(stream, LogEpoch(1), id),
        )
        .unwrap(),
    }
}

fn make_escalation_record(obligation: &DeliveryObligation) -> EscalationResolutionRecord {
    EscalationResolutionRecord {
        record: FinalizationRecord {
            obligation: obligation.clone(),
            outcome: ObligationFinalization::Succeeded {
                id: obligation.id,
                destination: obligation.destination,
            },
        },
        escalation: FinalizationEscalation::PermanentReportingFailure,
        resolution: EscalationResolution::ReportingVerified,
    }
}

/// Helper: prepare a resolution via the full tracker → journal pipeline.
fn prepare_resolution(obligation: DeliveryObligation) -> (
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
        .acknowledge(
            marketforge_engine::live::routing::acknowledgement::DeliveryAcknowledgement {
                obligation: obligation.id,
                destination: obligation.destination,
                range: obligation.range.clone(),
                status: marketforge_engine::live::model::delivery::DeliveryStatus::Failed(
                    marketforge_engine::live::model::delivery::DeliveryFailure::PersistenceFailed,
                ),
            },
        )
        .unwrap();
    let mut journal = FinalizationJournal::new(10).unwrap();
    tracker.finalize_into(obligation.id, &mut journal).unwrap();

    let clock = ManualClock::new();
    let claim = journal
        .claim_next(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();
    journal
        .escalate(
            claim.record.obligation.id,
            claim.token,
            &clock,
            FinalizationEscalation::PermanentReportingFailure,
        )
        .unwrap();
    let id = claim.record.obligation.id;
    let prepared = journal
        .prepare_resolution(id, EscalationResolution::ReportingVerified)
        .unwrap();

    (journal, prepared.token, prepared.record, id)
}

// ── Core life cycle transitions ───────────────────────────────────────────

/// L1: accept() transitions a record to Accepted state (implicit).
///
/// A record in Accepted state cannot be claimed by try_take().
/// It requires an intermediate commit_resolution call.
#[test]
fn accept_transitions_to_accepted_state() {
    let obligation = make_obligation(1);
    let (mut _journal, token, record, _id) = prepare_resolution(obligation);

    let sink = sink_with_capacity(5);
    let clock = ManualClock::new();

    let receipt = sink.accept(token, &record).unwrap();
    assert_eq!(sink.len(), 1);
    assert!(sink.verify(&receipt));

    // try_take() cannot claim an Accepted record.
    assert!(sink.try_take(&clock, Duration::from_secs(30)).unwrap().is_none());
}

/// L2: commit_resolution transitions Accepted → JournalCommitted.
///
/// The guard chain (can_commit → mark_journal_committed) ensures
/// the record moves to JournalCommitted only after the journal
/// validates the receipt.
#[test]
fn commit_resolution_transitions_to_journal_committed() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);
    let clock = ManualClock::new();

    let receipt = sink.accept(token, &record).unwrap();

    // Before commit: try_take returns None.
    assert!(sink.try_take(&clock, Duration::from_secs(30)).unwrap().is_none());

    // Commit transitions to JournalCommitted.
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    // After commit: try_take succeeds.
    let taken = sink.try_take(&clock, Duration::from_secs(30)).unwrap().unwrap();
    assert!(sink.verify(&taken.receipt));
}

/// L3: try_take() transitions JournalCommitted → Claimed.
///
/// Once claimed, the record is retained by the sink (not removed)
/// until acknowledge() is called.
#[test]
fn try_take_transitions_to_claimed_state() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();

    // take transitions the record to Claimed.
    sink.try_take(&clock, Duration::from_secs(30)).unwrap();
    assert_eq!(sink.len(), 1);

    // The record is still retained by the sink (claimed ≠ removed).
    assert!(sink.verify(&receipt));

    // Record remains verifiable while Claimed.
    let claim_receipt =
        sink.try_take(&clock, Duration::from_secs(30)).unwrap();
    assert!(claim_receipt.is_none()); // already claimed
}

/// L4: acknowledge() removes a Claimed record and releases capacity.
#[test]
fn acknowledge_removes_claimed_record_and_releases_capacity() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(1);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();
    let taken = sink.try_take(&clock, Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(sink.len(), 1); // still retained (Claimed).

    sink.acknowledge(&taken.receipt, taken.token, &clock).unwrap();
    assert_eq!(sink.len(), 0);
    assert!(!sink.verify(&receipt));

    // Capacity released: accept a new record.
    let obligation_b = make_obligation(2);
    let (mut _journal_b, token_b, record_b, _id_b) = prepare_resolution(obligation_b);
    let new_receipt = sink.accept(token_b, &record_b).unwrap();
    assert_eq!(sink.len(), 1);
    assert_eq!(new_receipt.token(), token_b);
}

// ── Negative state transition tests ────────────────────────────────────────

/// L5: try_take() on an Accepted record returns None.
///
/// Primary enforcement: downstream consumers cannot claim before
/// the journal commits.
#[test]
fn try_take_rejected_on_accepted_record() {
    let obligation = make_obligation(1);
    let record = make_escalation_record(&obligation);
    let token = EscalationResolutionToken {
        journal: marketforge_engine::live::routing::acknowledgement::FinalizationJournalId(1),
        sequence: 1,
    };

    let sink = sink_with_capacity(5);
    let _receipt = sink.accept(token, &record).unwrap();

    let clock = ManualClock::new();
    assert!(sink.try_take(&clock, Duration::from_secs(30)).unwrap().is_none());
}

/// L6: release_claim() restores JournalCommitted state and re-enables claiming.
///
/// After a recoverable processing failure, the consumer releases its claim.
/// The record is eligible for re-claim by another consumer.
#[test]
fn release_claim_restores_journal_committed_state() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    let clock = ManualClock::new();

    // Claim to get a valid lease.
    let claim_a = sink.try_take(&clock, Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(sink.len(), 1);

    // Release the claim — restores JournalCommitted and removes lease.
    sink.release_claim(&claim_a.receipt, claim_a.token, &clock).unwrap();

    // Record is re-claimable.
    let claim_b = sink.try_take(&clock, Duration::from_secs(30)).unwrap().unwrap();
    // The re-claim produces a new claim (same record, possible new token).
    assert_eq!(sink.len(), 1);

    // Acknowledge to complete the lifecycle.
    sink.acknowledge(&claim_b.receipt, claim_b.token, &clock).unwrap();
    assert_eq!(sink.len(), 0);
}

// ── Multi-record state management tests ────────────────────────────────────

/// L7: FIFO ordering — try_take always claims the oldest JournalCommitted record.
#[test]
fn try_take_fifo_ordering() {
    let obligation_a = make_obligation(1);
    let obligation_b = make_obligation(2);
    let obligation_c = make_obligation(3);

    let (mut journal_a, token_a, record_a, id_a) = prepare_resolution(obligation_a);
    let (mut journal_b, token_b, record_b, id_b) = prepare_resolution(obligation_b);
    let (mut journal_c, token_c, record_c, id_c) = prepare_resolution(obligation_c);

    let sink = sink_with_capacity(10);

    let receipt_a = sink.accept(token_a, &record_a).unwrap();
    let receipt_b = sink.accept(token_b, &record_b).unwrap();
    let receipt_c = sink.accept(token_c, &record_c).unwrap();

    assert_eq!(sink.len(), 3);

    journal_a.commit_resolution(id_a, &receipt_a, &sink).unwrap();
    journal_b.commit_resolution(id_b, &receipt_b, &sink).unwrap();
    journal_c.commit_resolution(id_c, &receipt_c, &sink).unwrap();

    // First taken is the oldest committed.
    let clock = ManualClock::new();
    let taken_first = sink.try_take(&clock, Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(taken_first.receipt.token().journal, journal_a.id());

    let taken_second = sink.try_take(&clock, Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(taken_second.receipt.token().journal, journal_b.id());
    assert_ne!(taken_first.receipt.token().journal, taken_second.receipt.token().journal);
}

/// L8: Acknowledging one record does not affect Accepted state records.
#[test]
fn acknowledge_does_not_affect_accepted_records() {
    let obligation_a = make_obligation(1);
    let obligation_b = make_obligation(2);

    let (mut journal_a, token_a, record_a, id_a) = prepare_resolution(obligation_a);
    let (mut _journal_b, token_b, record_b, _id_b) = prepare_resolution(obligation_b);

    let sink = sink_with_capacity(10);

    let receipt_a = sink.accept(token_a, &record_a).unwrap();
    let _receipt_b = sink.accept(token_b, &record_b).unwrap();

    assert_eq!(sink.len(), 2);

    // Commit only A.
    journal_a.commit_resolution(id_a, &receipt_a, &sink).unwrap();

    // B remains Accepted (not committed).
    assert_eq!(sink.len(), 2);

    let clock = ManualClock::new();

    // Take A (now Claimed).
    let taken = sink.try_take(&clock, Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(taken.receipt.record().record.obligation.id, id_a);

    // Acknowledge A to release it.
    sink.acknowledge(&taken.receipt, taken.token, &clock).unwrap();

    // B (still Accepted) remains in the sink, not removed by A's claim.
    assert_eq!(sink.len(), 1);
}

/// L9: Double commit_resolution is rejected via guard chain.
///
/// After mark_journal_committed() transitions to JournalCommitted,
/// can_commit() returns false, preventing re-commit.
#[test]
fn double_commit_rejected_via_guard_chain() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let receipt = sink.accept(token, &record).unwrap();

    // First commit succeeds via commit_resolution.
    journal.commit_resolution(id, &receipt, &sink).unwrap();

    // The guard chain: can_commit() checks Accepted state.
    let guard = sink.guard_commitment(&receipt).unwrap();
    assert!(!guard.can_commit()); // state is JournalCommitted, not Accepted
}

// ── End-to-end lifecycle test ──────────────────────────────────────────────

/// L10: Full lifecycle chain: Accepted → JournalCommitted → Claimed → removed.
///
/// This is the primary end-to-end lifecycle test, verifying that
/// each state transition occurs correctly and the record is
/// eventually removed from the sink.
#[test]
fn full_lifecycle_chain() {
    let obligation = make_obligation(1);
    let (mut journal, token, record, id) = prepare_resolution(obligation);
    let sink = sink_with_capacity(5);

    let clock = ManualClock::new();

    // Phase 1: Accepted state.
    let receipt = sink.accept(token, &record).unwrap();
    assert_eq!(sink.len(), 1);
    assert!(sink.verify(&receipt));
    assert!(sink.try_take(&clock, Duration::from_secs(30)).unwrap().is_none()); // Accepted ≠ JournalCommitted

    // Phase 2: JournalCommitted state.
    journal.commit_resolution(id, &receipt, &sink).unwrap();
    let taken = sink.try_take(&clock, Duration::from_secs(30)).unwrap().unwrap();
    assert_eq!(sink.len(), 1); // retained (Claimed, not removed)
    assert!(sink.verify(&taken.receipt));

    // Phase 3: Claimed state (before acknowledge).
    assert_eq!(sink.len(), 1);
    assert!(sink.verify(&receipt));

    // Phase 4: Removed via acknowledge.
    sink.acknowledge(&taken.receipt, taken.token, &clock).unwrap();
    assert_eq!(sink.len(), 0);
    assert!(!sink.verify(&taken.receipt));
}

/// Helper: create a sink with the given capacity.
fn sink_with_capacity(capacity: usize) -> BoundedResolutionSink {
    BoundedResolutionSink::new(capacity).unwrap()
}
