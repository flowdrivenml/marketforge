use marketforge_engine::live::routing::acknowledgement::{
    EscalationResolution, EscalationResolutionRecord, EscalationResolutionToken,
    FinalizationEscalation, FinalizationJournalError, FinalizationRecord, ObligationFinalization,
};
use marketforge_engine::live::{
    model::{
        cursor::{LogEpoch, StreamCursor},
        delivery::{DeliveryFailure, DeliveryPolicy, DeliveryStatus, DestinationKind},
        identity::StreamId,
    },
    ports::routing::DestinationId,
    routing::{
        acknowledgement::{
            AcknowledgementTracker, DeliveryAcknowledgement, DeliveryCursorRange,
            DeliveryObligation, DeliveryObligationId, FinalizationJournal, FinalizationRetryPolicy,
        },
        reporting::{
            FinalizationReporter, FinalizationReportingConsumer, ReportingError, ReportingStep,
        },
        resolution::{BoundedResolutionSink, ResolutionAcceptanceSink},
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::live::support::manual_clock::ManualClock;

struct TestReporter {
    calls: Arc<AtomicUsize>,
    fail: bool,
}

impl FinalizationReporter for TestReporter {
    fn report(
        &self,
        _record: &marketforge_engine::live::routing::acknowledgement::FinalizationRecord,
    ) -> Result<(), ReportingError> {
        self.calls.fetch_add(1, Ordering::SeqCst);

        if self.fail {
            Err(ReportingError::TemporarilyUnavailable)
        } else {
            Ok(())
        }
    }
}

fn journal_with_failure() -> FinalizationJournal {
    let mut tracker = AcknowledgementTracker::new(10).unwrap();
    let mut journal = FinalizationJournal::new(10).unwrap();

    let stream = StreamId("reporting-source".to_owned());

    let obligation = DeliveryObligation {
        id: DeliveryObligationId(1),
        destination: DestinationId(7),
        kind: DestinationKind::Recording,
        policy: DeliveryPolicy::Required,
        range: DeliveryCursorRange::new(
            StreamCursor::new(stream.clone(), LogEpoch(1), 1),
            StreamCursor::new(stream, LogEpoch(1), 1),
        )
        .unwrap(),
    };

    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Failed(DeliveryFailure::PersistenceFailed),
        })
        .unwrap();

    tracker.finalize_into(obligation.id, &mut journal).unwrap();

    journal
}

#[test]
fn successful_reporting_acknowledges_journal_record() {
    let calls = Arc::new(AtomicUsize::new(0));

    let reporter = TestReporter {
        calls: calls.clone(),
        fail: false,
    };

    let retry_policy =
        FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(30), 5).unwrap();

    let consumer =
        FinalizationReportingConsumer::new(reporter, Duration::from_secs(30), retry_policy)
            .unwrap();

    let mut journal = journal_with_failure();
    let clock = ManualClock::new();

    assert_eq!(
        consumer.process_one(&mut journal, &clock).unwrap(),
        ReportingStep::Reported
    );

    assert!(journal.is_empty());
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn temporary_reporting_failure_preserves_record() {
    let calls = Arc::new(AtomicUsize::new(0));

    let reporter = TestReporter {
        calls: calls.clone(),
        fail: true,
    };

    let retry_policy =
        FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(30), 5).unwrap();

    let consumer =
        FinalizationReportingConsumer::new(reporter, Duration::from_secs(30), retry_policy)
            .unwrap();

    let mut journal = journal_with_failure();
    let clock = ManualClock::new();

    assert_eq!(
        consumer.process_one(&mut journal, &clock).unwrap(),
        ReportingStep::RetryScheduled
    );

    assert_eq!(journal.len(), 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[test]
fn empty_journal_returns_idle() {
    let reporter = TestReporter {
        calls: Arc::new(AtomicUsize::new(0)),
        fail: false,
    };

    let retry_policy =
        FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(30), 5).unwrap();

    let consumer =
        FinalizationReportingConsumer::new(reporter, Duration::from_secs(30), retry_policy)
            .unwrap();

    let mut journal = FinalizationJournal::new(10).unwrap();
    let clock = ManualClock::new();

    assert_eq!(
        consumer.process_one(&mut journal, &clock).unwrap(),
        ReportingStep::Idle
    );
}

#[test]
fn temporary_failure_does_not_retry_immediately() {
    let calls = Arc::new(AtomicUsize::new(0));

    let reporter = TestReporter {
        calls: calls.clone(),
        fail: true,
    };

    let policy =
        FinalizationRetryPolicy::new(Duration::from_secs(5), Duration::from_secs(30), 3).unwrap();

    let consumer =
        FinalizationReportingConsumer::new(reporter, Duration::from_secs(30), policy).unwrap();

    let mut journal = journal_with_failure();
    let clock = ManualClock::new();

    assert_eq!(
        consumer.process_one(&mut journal, &clock).unwrap(),
        ReportingStep::RetryScheduled
    );

    assert_eq!(
        consumer.process_one(&mut journal, &clock).unwrap(),
        ReportingStep::Idle
    );

    assert_eq!(calls.load(Ordering::SeqCst), 1);

    clock.advance(Duration::from_secs(5));

    assert_eq!(
        consumer.process_one(&mut journal, &clock).unwrap(),
        ReportingStep::RetryScheduled
    );

    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn permanent_failure_is_not_retried_automatically() {
    struct PermanentReporter;

    impl FinalizationReporter for PermanentReporter {
        fn report(
            &self,
            _: &marketforge_engine::live::routing::acknowledgement::FinalizationRecord,
        ) -> Result<(), ReportingError> {
            Err(ReportingError::PermanentFailure)
        }
    }

    let policy =
        FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(10), 3).unwrap();

    let consumer =
        FinalizationReportingConsumer::new(PermanentReporter, Duration::from_secs(30), policy)
            .unwrap();

    let mut journal = journal_with_failure();
    let clock = ManualClock::new();

    assert_eq!(
        consumer.process_one(&mut journal, &clock).unwrap(),
        ReportingStep::PermanentlyFailed
    );

    assert_eq!(journal.len(), 1);

    clock.advance(Duration::from_secs(60));

    assert_eq!(
        consumer.process_one(&mut journal, &clock).unwrap(),
        ReportingStep::Idle
    );
}

#[test]
fn escalated_record_remains_inspectable() {
    let mut journal = journal_with_failure();
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

    let records = journal.escalated_records();

    assert_eq!(records.len(), 1);
    assert_eq!(journal.escalated_count(), 1);

    assert_eq!(
        records[0].reason,
        FinalizationEscalation::PermanentReportingFailure
    );

    assert_eq!(records[0].record.outcome, claim.record.outcome);
}

#[test]
fn authorized_retry_preserves_original_failure() {
    let mut journal = journal_with_failure();
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

    journal.authorize_retry(id).unwrap();

    assert_eq!(journal.escalated_count(), 0);
    assert_eq!(journal.len(), 1);

    let retry = journal
        .claim_next(&clock, Duration::from_secs(30))
        .unwrap()
        .unwrap();

    assert_eq!(retry.record.outcome, claim.record.outcome);
    assert_ne!(retry.token, claim.token);
}

#[test]
fn resolution_preserves_original_delivery_outcome() {
    let mut journal = journal_with_failure();
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
        .prepare_resolution(id, EscalationResolution::FailureAcknowledged)
        .unwrap();

    let sink = BoundedResolutionSink::new(10).unwrap();
    let receipt = sink.accept(prepared.token, &prepared.record).unwrap();

    let committed = journal.commit_resolution(id, &receipt, &sink).unwrap();

    assert_eq!(committed.record.outcome, claim.record.outcome);
    assert_eq!(
        committed.resolution,
        EscalationResolution::FailureAcknowledged
    );

    assert!(journal.is_empty());
}

#[test]
fn non_escalated_record_cannot_prepare_resolution() {
    let mut journal = journal_with_failure();

    assert!(matches!(
        journal.prepare_resolution(
            DeliveryObligationId(1),
            EscalationResolution::FailureAcknowledged,
        ),
        Err(FinalizationJournalError::NotEscalated)
    ));

    assert_eq!(journal.len(), 1);
}

#[test]
fn prepared_resolution_retains_journal_record() {
    let mut journal = journal_with_failure();
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
        .prepare_resolution(id, EscalationResolution::FailureAcknowledged)
        .unwrap();

    assert_eq!(journal.len(), 1);
    assert_eq!(prepared.record.record.outcome, claim.record.outcome);
}

#[test]
fn resolution_commit_preserves_prepared_resolution_kind() {
    let mut journal = journal_with_failure();
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

    // Accept the prepared token into the sink to produce a valid receipt.
    let sink = BoundedResolutionSink::new(10).unwrap();
    let receipt = sink.accept(prepared.token, &prepared.record).unwrap();

    let committed = journal.commit_resolution(id, &receipt, &sink).unwrap();

    assert_eq!(
        committed.resolution,
        EscalationResolution::ReportingVerified
    );

    assert!(journal.is_empty());
}

#[test]
fn stale_resolution_token_cannot_commit() {
    let mut journal = journal_with_failure();
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
        .prepare_resolution(id, EscalationResolution::FailureAcknowledged)
        .unwrap();

    // Create a sink and produce a receipt with a *different* (stale) token.
    let sink = BoundedResolutionSink::new(10).unwrap();
    let stale_token = EscalationResolutionToken {
        journal: prepared.token.journal,
        sequence: prepared.token.sequence + 1,
    };

    let stale_record = EscalationResolutionRecord {
        record: FinalizationRecord {
            outcome: ObligationFinalization::Succeeded {
                id: DeliveryObligationId(999),
                destination: match prepared.record.record.outcome {
                    ObligationFinalization::Succeeded { destination, .. } => destination,
                    _ => DestinationId(0),
                },
            },
            obligation: prepared.record.record.obligation.clone(),
        },
        escalation: FinalizationEscalation::PermanentReportingFailure,
        resolution: EscalationResolution::FailureAcknowledged,
    };

    let stale_receipt = sink.accept(stale_token, &stale_record).unwrap();

    // The journal's commit_resolution verifies receipt.token() == prepared.token.
    // A stale (mismatched) token should be rejected.
    let result = journal.commit_resolution(id, &stale_receipt, &sink);
    assert_eq!(
        result,
        Err(FinalizationJournalError::InvalidResolutionReceipt)
    );

    // The journal should still have the record (not committed).
    assert_eq!(journal.len(), 1);
}
