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
    journal_with_failures(1)
}

fn journal_with_failures(count: u64) -> FinalizationJournal {
    let mut tracker = AcknowledgementTracker::new(10).unwrap();
    let mut journal = FinalizationJournal::new(10).unwrap();

    for id in 1..=count {
        let stream = StreamId("reporting-source".to_owned());

        let obligation = DeliveryObligation {
            id: DeliveryObligationId(id),
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
    }

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

#[test]
fn registered_reporting_retries_without_spin_and_drains_after_close() {
    use marketforge_engine::live::{
        foundation::{clock::ManualClock as RuntimeClock, deadline::ManualDeadlineTimer},
        routing::{
            driver::DeliveryDriver, router::RouterRegistry, supervisor::DeliverySupervisor,
            tracking_recovery::TrackingRecovery, worker::DestinationWorkers,
        },
    };
    use std::{
        num::NonZeroUsize,
        task::{Context, Wake, Waker},
        time::Instant,
    };
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct RecoveringReporter(Arc<AtomicUsize>);
    impl FinalizationReporter for RecoveringReporter {
        fn report(&self, _: &FinalizationRecord) -> Result<(), ReportingError> {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err(ReportingError::TemporarilyUnavailable)
            } else {
                Ok(())
            }
        }
    }
    let now = Instant::now();
    let clock = RuntimeClock::new(now, 0);
    let timer = ManualDeadlineTimer::new(now, 1).unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let consumer = FinalizationReportingConsumer::new(
        RecoveringReporter(calls.clone()),
        Duration::from_secs(10),
        FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(10), 3).unwrap(),
    )
    .unwrap();
    let limit = NonZeroUsize::new(1).unwrap();
    let mut driver = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(1).unwrap(),
        TrackingRecovery::new(1).unwrap(),
        journal_with_failure(),
    );
    driver.close();
    let registry = RouterRegistry::new();
    let wakes = Arc::new(Counter(AtomicUsize::new(0)));
    let waker = Waker::from(wakes.clone());
    let mut cx = Context::from_waker(&waker);
    let mut turn = |driver: &mut DeliveryDriver| {
        let (_, schedule, report) = driver.drive_registered_reporting(
            &registry,
            &mut cx,
            &clock,
            &timer,
            Duration::from_secs(1),
            &consumer,
        );
        assert_eq!(schedule, Ok(()));
        report.unwrap()
    };
    assert_eq!(turn(&mut driver), ReportingStep::RetryScheduled);
    assert_eq!(driver.snapshot().journal_records, 1);
    let before = wakes.0.load(Ordering::SeqCst);
    assert_eq!(turn(&mut driver), ReportingStep::Idle);
    assert_eq!(wakes.0.load(Ordering::SeqCst), before);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    clock.advance(Duration::from_secs(1)).unwrap();
    timer.advance_to(now + Duration::from_secs(1)).unwrap();
    assert_eq!(turn(&mut driver), ReportingStep::Reported);
    assert!(driver.snapshot().is_drained());
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(turn(&mut driver), ReportingStep::Idle);
}

#[test]
fn registered_reporting_preserves_escalation_when_retry_timer_is_unavailable() {
    use marketforge_engine::live::{
        foundation::{clock::ManualClock as RuntimeClock, deadline::ManualDeadlineTimer},
        model::error::ErrorCode,
        routing::{
            admission::DeadlineTimer, driver::DeliveryDriver, router::RouterRegistry,
            supervisor::DeliverySupervisor, tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        num::NonZeroUsize,
        task::{Context, Waker},
        time::Instant,
    };
    struct Permanent(Arc<AtomicUsize>);
    impl FinalizationReporter for Permanent {
        fn report(&self, _: &FinalizationRecord) -> Result<(), ReportingError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(ReportingError::PermanentFailure)
        }
    }
    let now = Instant::now();
    let clock = RuntimeClock::new(now, 0);
    let timer = ManualDeadlineTimer::new(now, 1).unwrap();
    let _reserved = timer
        .try_sleep_until(now + Duration::from_secs(60))
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let consumer = FinalizationReportingConsumer::new(
        Permanent(calls.clone()),
        Duration::from_secs(10),
        FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(10), 3).unwrap(),
    )
    .unwrap();
    let limit = NonZeroUsize::new(1).unwrap();
    let mut driver = DeliveryDriver::new(
        DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
        AcknowledgementTracker::new(1).unwrap(),
        TrackingRecovery::new(1).unwrap(),
        journal_with_failure(),
    );
    let registry = RouterRegistry::new();
    let mut cx = Context::from_waker(Waker::noop());
    let (_, scheduling, reporting) = driver.drive_registered_reporting(
        &registry,
        &mut cx,
        &clock,
        &timer,
        Duration::from_secs(1),
        &consumer,
    );
    assert_eq!(scheduling, Err(ErrorCode::Capacity));
    assert_eq!(reporting, Ok(ReportingStep::PermanentlyFailed));
    driver.close();
    clock.advance(Duration::from_secs(60)).unwrap();
    let (_, scheduling, reporting) = driver.drive_registered_reporting(
        &registry,
        &mut cx,
        &clock,
        &timer,
        Duration::from_secs(1),
        &consumer,
    );
    assert_eq!(scheduling, Err(ErrorCode::Capacity));
    assert_eq!(reporting, Ok(ReportingStep::Idle));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(driver.snapshot().escalated_records, 1);
    assert!(!driver.snapshot().is_drained());
}

#[test]
fn failed_reporting_schedules_one_progress_turn_without_spinning() {
    use marketforge_engine::live::{
        foundation::clock::ManualClock as LiveClock,
        routing::{
            admission::{DeadlineFuture, DeadlineTimer},
            driver::DeliveryDriver,
            router::RouterRegistry,
            supervisor::DeliverySupervisor,
            tracking_recovery::TrackingRecovery,
            worker::DestinationWorkers,
        },
    };
    use std::{
        num::NonZeroUsize,
        task::{Context, Wake, Waker},
        time::Instant,
    };
    struct Counter(AtomicUsize);
    impl Wake for Counter {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        fn wake_by_ref(self: &Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Timer(bool);
    impl DeadlineTimer for Timer {
        fn sleep_until(&self, _: Instant) -> DeadlineFuture {
            Box::pin(std::future::pending())
        }
        fn try_sleep_until(
            &self,
            deadline: Instant,
        ) -> Result<DeadlineFuture, marketforge_engine::live::model::error::ErrorCode> {
            if self.0 {
                Err(marketforge_engine::live::model::error::ErrorCode::Capacity)
            } else {
                Ok(self.sleep_until(deadline))
            }
        }
    }
    struct Reporter(ReportingError, bool, AtomicUsize);
    impl FinalizationReporter for Reporter {
        fn report(&self, _: &FinalizationRecord) -> Result<(), ReportingError> {
            if self.2.fetch_add(1, Ordering::SeqCst) == 0 {
                assert!(!self.1, "injected reporting handler unwind");
                Err(self.0)
            } else {
                Ok(())
            }
        }
    }
    for (failure, closed, timer_full, panics) in [
        (ReportingError::TemporarilyUnavailable, false, false, false),
        (ReportingError::TemporarilyUnavailable, false, true, false),
        (ReportingError::TemporarilyUnavailable, true, false, false),
        (ReportingError::TemporarilyUnavailable, true, true, false),
        (ReportingError::PermanentFailure, false, false, false),
        (ReportingError::PermanentFailure, false, true, false),
        (ReportingError::PermanentFailure, true, false, false),
        (ReportingError::PermanentFailure, true, true, false),
        (ReportingError::PermanentFailure, false, false, true),
        (ReportingError::PermanentFailure, false, true, true),
        (ReportingError::PermanentFailure, true, false, true),
        (ReportingError::PermanentFailure, true, true, true),
    ] {
        let limit = NonZeroUsize::new(1).unwrap();
        let mut driver = DeliveryDriver::new(
            DeliverySupervisor::new(DestinationWorkers::new(limit), limit),
            AcknowledgementTracker::new(1).unwrap(),
            TrackingRecovery::new(1).unwrap(),
            journal_with_failures(2),
        );
        if closed {
            driver.close();
        }
        let registry = RouterRegistry::new();
        let expected_scheduling = if timer_full {
            Err(marketforge_engine::live::model::error::ErrorCode::Capacity)
        } else {
            Ok(())
        };
        let clock = LiveClock::new(Instant::now(), 0);
        let counter = Arc::new(Counter(AtomicUsize::new(0)));
        let waker = Waker::from(counter.clone());
        let mut cx = Context::from_waker(&waker);
        let consumer = FinalizationReportingConsumer::new(
            Reporter(failure, panics, AtomicUsize::new(0)),
            Duration::from_secs(1),
            FinalizationRetryPolicy::new(Duration::from_secs(1), Duration::from_secs(1), 3)
                .unwrap(),
        )
        .unwrap();
        let (_, scheduling, reporting) = driver.drive_registered_reporting(
            &registry,
            &mut cx,
            &clock,
            &Timer(timer_full),
            Duration::from_secs(1),
            &consumer,
        );
        assert_eq!(scheduling, expected_scheduling);
        assert_eq!(
            reporting,
            Ok(match failure {
                ReportingError::TemporarilyUnavailable => ReportingStep::RetryScheduled,
                ReportingError::PermanentFailure => ReportingStep::PermanentlyFailed,
            })
        );
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            1,
            "a failed record must not park unrelated ready reports"
        );
        let (_, scheduling, reporting) = driver.drive_registered_reporting(
            &registry,
            &mut cx,
            &clock,
            &Timer(timer_full),
            Duration::from_secs(1),
            &consumer,
        );
        assert_eq!(scheduling, expected_scheduling);
        assert_eq!(reporting, Ok(ReportingStep::Reported));
        // Successful acknowledgement also releases journal capacity and wakes
        // its independently registered capacity waiter.
        assert_eq!(counter.0.load(Ordering::SeqCst), 3);
        let (_, scheduling, reporting) = driver.drive_registered_reporting(
            &registry,
            &mut cx,
            &clock,
            &Timer(timer_full),
            Duration::from_secs(1),
            &consumer,
        );
        assert_eq!(scheduling, expected_scheduling);
        assert_eq!(reporting, Ok(ReportingStep::Idle));
        assert_eq!(
            counter.0.load(Ordering::SeqCst),
            3,
            "deferred or escalated records must not spin"
        );
        let snapshot = driver.snapshot();
        assert!(!snapshot.is_drained());
        assert_eq!(snapshot.journal_records, 1);
        assert_eq!(
            snapshot.escalated_records,
            usize::from(failure == ReportingError::PermanentFailure)
        );
        assert!(driver.journal_mut().contains(DeliveryObligationId(1)));
        assert!(!driver.journal_mut().contains(DeliveryObligationId(2)));
        if panics {
            let retained = driver.journal_mut().escalated_records();
            assert_eq!(retained.len(), 1);
            assert_eq!(
                retained[0].reason,
                FinalizationEscalation::PermanentReportingFailure
            );
            assert_eq!(retained[0].record.obligation.id, DeliveryObligationId(1));
            // Expiry of the original lease cannot automatically repeat a handler
            // whose side effects are unknown after unwind.
            clock.advance(Duration::from_secs(60)).unwrap();
            assert_eq!(
                consumer.process_one(driver.journal_mut(), &clock),
                Ok(ReportingStep::Idle)
            );
            assert_eq!(driver.journal_mut().escalated_records(), retained);
            driver
                .journal_mut()
                .authorize_retry(DeliveryObligationId(1))
                .unwrap();
            assert_eq!(
                consumer.process_one(driver.journal_mut(), &clock),
                Ok(ReportingStep::Reported)
            );
            assert!(driver.journal_mut().is_empty());
        }
    }
}
