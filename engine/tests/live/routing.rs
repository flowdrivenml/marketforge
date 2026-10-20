use marketforge_engine::live::model::delivery::DeliveryPolicy;
use marketforge_engine::live::routing::acknowledgement::ObligationFinalization;
use marketforge_engine::live::{
    model::{
        cursor::{LogEpoch, StreamCursor},
        delivery::{DeliveryFailure, DeliveryStage, DeliveryStatus, DestinationKind},
        identity::StreamId,
    },
    ports::routing::DestinationId,
    routing::acknowledgement::{
        AcknowledgementError, AcknowledgementTracker, DeliveryAcknowledgement, DeliveryCursorRange,
        DeliveryObligation, DeliveryObligationId,
    },
};

fn test_cursor(offset: u64) -> StreamCursor {
    StreamCursor::new(StreamId("test-source".to_owned()), LogEpoch(1), offset)
}

fn test_obligation() -> DeliveryObligation {
    DeliveryObligation {
        id: DeliveryObligationId(1),
        destination: DestinationId(7),
        kind: DestinationKind::Recording,
        policy: DeliveryPolicy::Required,
        range: DeliveryCursorRange::new(test_cursor(10), test_cursor(12)).unwrap(),
    }
}
#[test]
fn durable_recording_cannot_finalize_at_committed() {
    let mut obligation = test_obligation();
    obligation.policy = DeliveryPolicy::Durable;

    let mut tracker = AcknowledgementTracker::new(10).unwrap();
    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Committed),
        })
        .unwrap();

    assert_eq!(
        tracker.finalize(obligation.id),
        Err(AcknowledgementError::IncompleteObligation)
    );

    assert_eq!(tracker.len(), 1);
}

#[test]
fn durable_recording_finalizes_after_durable_stage() {
    let mut obligation = test_obligation();
    obligation.policy = DeliveryPolicy::Durable;

    let mut tracker = AcknowledgementTracker::new(10).unwrap();
    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Durable),
        })
        .unwrap();

    assert_eq!(
        tracker.finalize(obligation.id),
        Ok(ObligationFinalization::Succeeded {
            id: obligation.id,
            destination: obligation.destination,
        })
    );

    assert!(tracker.is_empty());
}

#[test]
fn required_publication_finalizes_at_delivered() {
    let mut obligation = test_obligation();
    obligation.kind = DestinationKind::Publication;
    obligation.policy = DeliveryPolicy::Required;

    let mut tracker = AcknowledgementTracker::new(10).unwrap();
    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Delivered),
        })
        .unwrap();

    assert!(matches!(
        tracker.finalize(obligation.id),
        Ok(ObligationFinalization::Succeeded { .. })
    ));
}

#[test]
fn required_recording_cannot_finalize_while_queued() {
    let obligation = test_obligation();

    let mut tracker = AcknowledgementTracker::new(10).unwrap();
    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Queued),
        })
        .unwrap();

    assert_eq!(
        tracker.finalize(obligation.id),
        Err(AcknowledgementError::IncompleteObligation)
    );
}

#[test]
fn failed_obligation_returns_explicit_failure_outcome() {
    let obligation = test_obligation();

    let mut tracker = AcknowledgementTracker::new(10).unwrap();
    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Failed(DeliveryFailure::PersistenceFailed),
        })
        .unwrap();

    assert_eq!(
        tracker.finalize(obligation.id),
        Ok(ObligationFinalization::Failed {
            id: obligation.id,
            destination: obligation.destination,
            reason: DeliveryFailure::PersistenceFailed,
        })
    );
}
#[test]
fn acknowledgement_progresses_monotonically() {
    let obligation = test_obligation();
    let mut tracker = AcknowledgementTracker::new(1024).unwrap();

    tracker.register(obligation.clone()).unwrap();

    for stage in [
        DeliveryStage::Accepted,
        DeliveryStage::Queued,
        DeliveryStage::Committed,
        DeliveryStage::Durable,
    ] {
        tracker
            .acknowledge(DeliveryAcknowledgement {
                obligation: obligation.id,
                destination: obligation.destination,
                range: obligation.range.clone(),
                status: DeliveryStatus::Progress(stage),
            })
            .unwrap();
    }

    assert_eq!(
        tracker.status(obligation.id),
        Some(DeliveryStatus::Progress(DeliveryStage::Durable))
    );
}

#[test]
fn acknowledgement_rejects_progress_regression() {
    let obligation = test_obligation();
    let mut tracker = AcknowledgementTracker::new(1024).unwrap();

    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Committed),
        })
        .unwrap();

    let result = tracker.acknowledge(DeliveryAcknowledgement {
        obligation: obligation.id,
        destination: obligation.destination,
        range: obligation.range.clone(),
        status: DeliveryStatus::Progress(DeliveryStage::Queued),
    });

    assert_eq!(result, Err(AcknowledgementError::ProgressRegression));
}

#[test]
fn acknowledgement_rejects_wrong_destination() {
    let obligation = test_obligation();
    let mut tracker = AcknowledgementTracker::new(1024).unwrap();

    tracker.register(obligation.clone()).unwrap();

    let result = tracker.acknowledge(DeliveryAcknowledgement {
        obligation: obligation.id,
        destination: DestinationId(999),
        range: obligation.range.clone(),
        status: DeliveryStatus::Progress(DeliveryStage::Queued),
    });

    assert_eq!(result, Err(AcknowledgementError::DestinationMismatch));
}

#[cfg(test)]
mod finalization_journal_tests {
    use std::time::Duration;

    use crate::live::support::manual_clock::ManualClock;

    use marketforge_engine::live::{
        model::{
            cursor::{LogEpoch, StreamCursor},
            delivery::{DeliveryFailure, DeliveryPolicy, DeliveryStatus, DestinationKind},
            identity::StreamId,
        },
        ports::routing::DestinationId,
        routing::acknowledgement::{
            AcknowledgementTracker, DeliveryAcknowledgement, DeliveryCursorRange,
            DeliveryObligation, DeliveryObligationId, FinalizationClaimToken,
            FinalizationHandoffError, FinalizationJournal, FinalizationJournalError,
            ObligationFinalization,
        },
    };

    #[test]
    fn expired_claim_can_be_reclaimed_with_new_token() {
        let clock = ManualClock::new();

        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(10).unwrap();

        let item = obligation(1);

        tracker.register(item.clone()).unwrap();
        fail(&mut tracker, &item);
        tracker.finalize_into(item.id, &mut journal).unwrap();

        let first = journal
            .claim_next(&clock, Duration::from_secs(10))
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_secs(10));

        let second = journal
            .claim_next(&clock, Duration::from_secs(10))
            .unwrap()
            .unwrap();

        assert_eq!(first.record, second.record);
        assert_ne!(first.token, second.token);
        assert_eq!(journal.len(), 1);
    }

    #[test]
    fn stale_claim_cannot_acknowledge_reclaimed_record() {
        let clock = ManualClock::new();

        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(10).unwrap();

        let item = obligation(1);

        tracker.register(item.clone()).unwrap();
        fail(&mut tracker, &item);
        tracker.finalize_into(item.id, &mut journal).unwrap();

        let first = journal
            .claim_next(&clock, Duration::from_secs(5))
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_secs(5));

        let second = journal
            .claim_next(&clock, Duration::from_secs(5))
            .unwrap()
            .unwrap();

        assert_eq!(
            journal.acknowledge(item.id, first.token, &clock),
            Err(FinalizationJournalError::StaleClaim)
        );

        assert_eq!(journal.len(), 1);

        journal.acknowledge(item.id, second.token, &clock).unwrap();

        assert!(journal.is_empty());
    }

    #[test]
    fn stale_claim_cannot_release_newer_claim() {
        let clock = ManualClock::new();

        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(10).unwrap();

        let item = obligation(1);

        tracker.register(item.clone()).unwrap();
        fail(&mut tracker, &item);
        tracker.finalize_into(item.id, &mut journal).unwrap();

        let first = journal
            .claim_next(&clock, Duration::from_secs(5))
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_secs(5));

        let second = journal
            .claim_next(&clock, Duration::from_secs(5))
            .unwrap()
            .unwrap();

        assert_eq!(
            journal.release_claim(item.id, first.token, &clock),
            Err(FinalizationJournalError::StaleClaim)
        );

        assert_eq!(journal.len(), 1);

        journal
            .release_claim(item.id, second.token, &clock)
            .unwrap();

        let third = journal
            .claim_next(&clock, Duration::from_secs(5))
            .unwrap()
            .unwrap();

        assert_ne!(third.token, second.token);
    }

    #[test]
    fn expired_claim_cannot_acknowledge_without_reclaim() {
        let clock = ManualClock::new();

        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(10).unwrap();

        let item = obligation(1);

        tracker.register(item.clone()).unwrap();
        fail(&mut tracker, &item);
        tracker.finalize_into(item.id, &mut journal).unwrap();

        let claim = journal
            .claim_next(&clock, Duration::from_secs(5))
            .unwrap()
            .unwrap();

        clock.advance(Duration::from_secs(5));

        assert_eq!(
            journal.acknowledge(item.id, claim.token, &clock),
            Err(FinalizationJournalError::LeaseExpired)
        );

        assert_eq!(journal.len(), 1);
    }

    fn obligation(id: u64) -> DeliveryObligation {
        let stream = StreamId("journal-source".to_owned());

        DeliveryObligation {
            id: DeliveryObligationId(id),
            destination: DestinationId(7),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
            range: DeliveryCursorRange::new(
                StreamCursor::new(stream.clone(), LogEpoch(1), id),
                StreamCursor::new(stream, LogEpoch(1), id),
            )
            .unwrap(),
        }
    }

    fn fail(tracker: &mut AcknowledgementTracker, obligation: &DeliveryObligation) {
        tracker
            .acknowledge(DeliveryAcknowledgement {
                obligation: obligation.id,
                destination: obligation.destination,
                range: obligation.range.clone(),
                status: DeliveryStatus::Failed(DeliveryFailure::PersistenceFailed),
            })
            .unwrap();
    }

    #[test]
    fn journal_rejects_zero_capacity() {
        assert!(matches!(
            FinalizationJournal::new(0),
            Err(FinalizationJournalError::InvalidCapacity)
        ));
    }

    #[test]
    fn failed_obligation_transfers_to_journal() {
        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(10).unwrap();

        let item = obligation(1);

        tracker.register(item.clone()).unwrap();
        fail(&mut tracker, &item);

        let outcome = tracker.finalize_into(item.id, &mut journal).unwrap();

        assert_eq!(
            outcome,
            ObligationFinalization::Failed {
                id: item.id,
                destination: item.destination,
                reason: DeliveryFailure::PersistenceFailed,
            }
        );

        assert!(tracker.is_empty());
        assert_eq!(journal.len(), 1);
        assert!(journal.contains(item.id));
    }

    #[test]
    fn full_journal_preserves_active_obligation() {
        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(1).unwrap();

        let first = obligation(1);
        let second = obligation(2);

        tracker.register(first.clone()).unwrap();
        tracker.register(second.clone()).unwrap();

        fail(&mut tracker, &first);
        fail(&mut tracker, &second);

        tracker.finalize_into(first.id, &mut journal).unwrap();

        assert_eq!(
            tracker.finalize_into(second.id, &mut journal),
            Err(FinalizationHandoffError::Journal(
                FinalizationJournalError::Full
            ))
        );

        assert_eq!(tracker.len(), 1);
        assert!(tracker.status(second.id).is_some());
        assert_eq!(journal.len(), 1);
    }

    #[test]
    fn claiming_does_not_release_journal_capacity() {
        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(1).unwrap();

        let item = obligation(1);

        tracker.register(item.clone()).unwrap();
        fail(&mut tracker, &item);

        tracker.finalize_into(item.id, &mut journal).unwrap();

        let clock = ManualClock::new();

        let claim = journal
            .claim_next(&clock, Duration::from_secs(30))
            .unwrap()
            .unwrap();

        assert_eq!(claim.record.obligation.id, item.id);
        assert_eq!(journal.len(), 1);
        assert_eq!(journal.remaining_capacity(), 0);
    }

    #[test]
    fn acknowledgement_releases_journal_capacity() {
        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(1).unwrap();

        let item = obligation(1);

        tracker.register(item.clone()).unwrap();
        fail(&mut tracker, &item);

        tracker.finalize_into(item.id, &mut journal).unwrap();

        let clock = ManualClock::new();

        let claim = journal
            .claim_next(&clock, Duration::from_secs(30))
            .unwrap()
            .unwrap();
        journal.acknowledge(item.id, claim.token, &clock).unwrap();

        assert!(journal.is_empty());
        assert_eq!(journal.remaining_capacity(), 1);
    }

    #[test]
    fn unclaimed_record_cannot_be_acknowledged() {
        let clock = ManualClock::new();
        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(1).unwrap();

        let item = obligation(1);

        tracker.register(item.clone()).unwrap();
        fail(&mut tracker, &item);

        tracker.finalize_into(item.id, &mut journal).unwrap();

        assert!(matches!(
            journal.acknowledge(item.id, FinalizationClaimToken(0), &clock),
            Err(FinalizationJournalError::NotClaimed)
        ));

        assert_eq!(journal.len(), 1);
    }

    #[test]
    fn failed_reporting_can_retry_claim() {
        let mut tracker = AcknowledgementTracker::new(10).unwrap();
        let mut journal = FinalizationJournal::new(1).unwrap();

        let item = obligation(1);

        tracker.register(item.clone()).unwrap();
        fail(&mut tracker, &item);

        tracker.finalize_into(item.id, &mut journal).unwrap();

        let clock = ManualClock::new();

        let first_claim = journal
            .claim_next(&clock, Duration::from_secs(30))
            .unwrap()
            .unwrap();

        journal
            .release_claim(item.id, first_claim.token, &clock)
            .unwrap();

        let second_claim = journal
            .claim_next(&clock, Duration::from_secs(30))
            .unwrap()
            .unwrap();

        assert_eq!(
            first_claim.record.obligation.id,
            second_claim.record.obligation.id
        );
        assert_eq!(journal.len(), 1);
    }
}

#[test]
fn acknowledgement_rejects_wrong_range() {
    let obligation = test_obligation();
    let mut tracker = AcknowledgementTracker::new(1024).unwrap();

    tracker.register(obligation.clone()).unwrap();

    let result = tracker.acknowledge(DeliveryAcknowledgement {
        obligation: obligation.id,
        destination: obligation.destination,
        range: DeliveryCursorRange::new(test_cursor(10), test_cursor(13)).unwrap(),
        status: DeliveryStatus::Progress(DeliveryStage::Queued),
    });

    assert_eq!(result, Err(AcknowledgementError::RangeMismatch));
}

#[test]
fn acknowledgement_rejects_duplicate_obligation() {
    let obligation = test_obligation();
    let mut tracker = AcknowledgementTracker::new(1024).unwrap();

    tracker.register(obligation.clone()).unwrap();

    assert_eq!(
        tracker.register(obligation),
        Err(AcknowledgementError::DuplicateObligation)
    );
}

#[test]
fn acknowledgement_rejects_unknown_obligation() {
    let mut tracker = AcknowledgementTracker::new(1024).unwrap();

    let result = tracker.acknowledge(DeliveryAcknowledgement {
        obligation: DeliveryObligationId(999),
        destination: DestinationId(7),
        range: DeliveryCursorRange::new(test_cursor(10), test_cursor(12)).unwrap(),
        status: DeliveryStatus::Progress(DeliveryStage::Queued),
    });

    assert_eq!(result, Err(AcknowledgementError::UnknownObligation));
}

#[test]
fn acknowledgement_rejects_invalid_range() {
    assert_eq!(
        DeliveryCursorRange::new(test_cursor(12), test_cursor(10),),
        Err(AcknowledgementError::InvalidRange)
    );
}

#[test]
fn publication_rejects_persistence_stages() {
    let mut obligation = test_obligation();
    obligation.kind = DestinationKind::Publication;

    let mut tracker = AcknowledgementTracker::new(1024).unwrap();
    tracker.register(obligation.clone()).unwrap();

    for stage in [DeliveryStage::Committed, DeliveryStage::Durable] {
        let result = tracker.acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(stage),
        });

        assert_eq!(
            result,
            Err(AcknowledgementError::InvalidStageForDestination)
        );
    }
}

#[test]
fn recording_rejects_transport_delivery_stage() {
    let obligation = test_obligation();

    let mut tracker = AcknowledgementTracker::new(1024).unwrap();
    tracker.register(obligation.clone()).unwrap();

    let result = tracker.acknowledge(DeliveryAcknowledgement {
        obligation: obligation.id,
        destination: obligation.destination,
        range: obligation.range.clone(),
        status: DeliveryStatus::Progress(DeliveryStage::Delivered),
    });

    assert_eq!(
        result,
        Err(AcknowledgementError::InvalidStageForDestination)
    );
}

#[test]
fn publication_accepts_transport_delivery() {
    let mut obligation = test_obligation();
    obligation.kind = DestinationKind::Publication;

    let mut tracker = AcknowledgementTracker::new(1024).unwrap();
    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Delivered),
        })
        .unwrap();

    assert_eq!(
        tracker.status(obligation.id),
        Some(DeliveryStatus::Progress(DeliveryStage::Delivered))
    );
}

#[test]
fn retention_accepts_durable_commitment() {
    let mut obligation = test_obligation();
    obligation.kind = DestinationKind::Retention;

    let mut tracker = AcknowledgementTracker::new(1024).unwrap();
    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Progress(DeliveryStage::Durable),
        })
        .unwrap();

    assert_eq!(
        tracker.status(obligation.id),
        Some(DeliveryStatus::Progress(DeliveryStage::Durable))
    );
}

#[test]
fn tracker_rejects_zero_capacity() {
    assert!(matches!(
        AcknowledgementTracker::new(0),
        Err(AcknowledgementError::InvalidCapacity)
    ));
}

#[test]
fn tracker_enforces_obligation_capacity() {
    let mut tracker = AcknowledgementTracker::new(1).unwrap();

    let first = test_obligation();

    tracker.register(first.clone()).unwrap();

    let mut second = test_obligation();
    second.id = DeliveryObligationId(2);

    assert_eq!(
        tracker.register(second),
        Err(AcknowledgementError::CapacityExceeded)
    );

    assert_eq!(tracker.len(), 1);
    assert_eq!(tracker.remaining_capacity(), 0);
}

#[test]
fn tracker_allocates_distinct_ids() {
    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    let first = tracker.allocate_id().unwrap();
    let second = tracker.allocate_id().unwrap();

    assert_ne!(first, second);
    assert_eq!(first, DeliveryObligationId(1));
    assert_eq!(second, DeliveryObligationId(2));
}

#[test]
fn tracker_rejects_removal_of_pending_obligation() {
    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    let obligation = test_obligation();

    tracker.register(obligation.clone()).unwrap();

    assert_eq!(
        tracker.remove(obligation.id),
        Err(AcknowledgementError::IncompleteObligation)
    );

    assert_eq!(tracker.len(), 1);
}

#[test]
fn tracker_can_remove_failed_obligation() {
    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    let obligation = test_obligation();

    tracker.register(obligation.clone()).unwrap();

    tracker
        .acknowledge(DeliveryAcknowledgement {
            obligation: obligation.id,
            destination: obligation.destination,
            range: obligation.range.clone(),
            status: DeliveryStatus::Failed(DeliveryFailure::PersistenceFailed),
        })
        .unwrap();

    assert_eq!(
        tracker.remove(obligation.id),
        Ok(DeliveryStatus::Failed(DeliveryFailure::PersistenceFailed))
    );

    assert!(tracker.is_empty());
}

#[test]
fn registration_rejects_durable_publication() {
    let mut obligation = test_obligation();

    obligation.kind = DestinationKind::Publication;
    obligation.policy = DeliveryPolicy::Durable;

    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    assert_eq!(
        tracker.register(obligation),
        Err(AcknowledgementError::InvalidCompletionPolicy)
    );

    assert!(tracker.is_empty());
}

#[test]
fn registration_rejects_durable_derived_destination() {
    let mut obligation = test_obligation();

    obligation.kind = DestinationKind::Derived;
    obligation.policy = DeliveryPolicy::Durable;

    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    assert_eq!(
        tracker.register(obligation),
        Err(AcknowledgementError::InvalidCompletionPolicy)
    );

    assert_eq!(tracker.remaining_capacity(), 10);
}

#[test]
fn registration_accepts_durable_recording() {
    let mut obligation = test_obligation();

    obligation.kind = DestinationKind::Recording;
    obligation.policy = DeliveryPolicy::Durable;

    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    tracker.register(obligation).unwrap();

    assert_eq!(tracker.len(), 1);
}

#[test]
fn registration_accepts_required_publication() {
    let mut obligation = test_obligation();

    obligation.kind = DestinationKind::Publication;
    obligation.policy = DeliveryPolicy::Required;

    let mut tracker = AcknowledgementTracker::new(10).unwrap();

    tracker.register(obligation).unwrap();

    assert_eq!(tracker.len(), 1);
}

#[cfg(test)]
mod admission_wait_tests {
    use std::{
        future::{Future, pending, ready},
        sync::Arc,
        task::{Context, Poll, Wake, Waker},
        time::{Duration, Instant},
    };

    use marketforge_engine::live::{
        foundation::cancellation::CancellationToken,
        ports::routing::RoutingAdmissionFailure,
        routing::admission::{
            AdmissionContext, AdmissionWait, AdmissionWaitError, AdmissionWaitResult,
            DeadlineFuture, DeadlineTimer,
        },
    };

    struct NoopWake;

    impl Wake for NoopWake {
        fn wake(self: Arc<Self>) {}
    }

    fn test_waker() -> Waker {
        Waker::from(Arc::new(NoopWake))
    }

    struct ImmediateTimer;

    impl DeadlineTimer for ImmediateTimer {
        fn sleep_until(&self, _: Instant) -> DeadlineFuture {
            Box::pin(ready(()))
        }
    }

    struct PendingTimer;

    impl DeadlineTimer for PendingTimer {
        fn sleep_until(&self, _: Instant) -> DeadlineFuture {
            Box::pin(pending())
        }
    }

    #[test]
    fn admission_completes_when_operation_is_ready() {
        let token = CancellationToken::new();

        let context = AdmissionContext::without_deadline(token);

        let mut wait = Box::pin(AdmissionWait::new(ready(42_u64), context, None).unwrap());

        let waker = test_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(
            wait.as_mut().poll(&mut cx),
            Poll::Ready(AdmissionWaitResult::Ready(42))
        );
    }

    #[test]
    fn admission_rejects_preexisting_cancellation() {
        let token = CancellationToken::new();
        token.cancel();

        let context = AdmissionContext::without_deadline(token);

        let mut wait = Box::pin(AdmissionWait::new(pending::<()>(), context, None).unwrap());

        let waker = test_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(
            wait.as_mut().poll(&mut cx),
            Poll::Ready(AdmissionWaitResult::Rejected(
                RoutingAdmissionFailure::Cancelled
            ))
        );
    }

    #[test]
    fn admission_rejects_expired_deadline() {
        let token = CancellationToken::new();

        let context = AdmissionContext::new(token, Some(Instant::now()));

        let mut wait =
            Box::pin(AdmissionWait::new(pending::<()>(), context, Some(&ImmediateTimer)).unwrap());

        let waker = test_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(
            wait.as_mut().poll(&mut cx),
            Poll::Ready(AdmissionWaitResult::Rejected(
                RoutingAdmissionFailure::DeadlineExceeded
            ))
        );
    }

    #[test]
    fn admission_rejects_missing_timer() {
        let token = CancellationToken::new();

        let context = AdmissionContext::new(token, Some(Instant::now() + Duration::from_secs(10)));

        let result = AdmissionWait::new(pending::<()>(), context, None);

        assert!(matches!(result, Err(AdmissionWaitError::MissingTimer)));
    }

    #[test]
    fn admission_remains_pending_without_completion() {
        let token = CancellationToken::new();

        let context = AdmissionContext::new(token, Some(Instant::now() + Duration::from_secs(10)));

        let mut wait =
            Box::pin(AdmissionWait::new(pending::<()>(), context, Some(&PendingTimer)).unwrap());

        let waker = test_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(matches!(wait.as_mut().poll(&mut cx), Poll::Pending));
    }

    #[test]
    fn cancellation_takes_priority_over_ready_operation() {
        let token = CancellationToken::new();
        token.cancel();

        let context = AdmissionContext::without_deadline(token);

        let mut wait = Box::pin(AdmissionWait::new(ready(42_u64), context, None).unwrap());

        let waker = test_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(
            wait.as_mut().poll(&mut cx),
            Poll::Ready(AdmissionWaitResult::Rejected(
                RoutingAdmissionFailure::Cancelled
            ))
        );
    }

    #[test]
    fn cancellation_after_completed_admission_does_not_change_result() {
        let token = CancellationToken::new();

        let context = AdmissionContext::without_deadline(token.clone());

        let mut wait = Box::pin(AdmissionWait::new(ready(42_u64), context, None).unwrap());

        let waker = test_waker();
        let mut cx = Context::from_waker(&waker);

        assert_eq!(
            wait.as_mut().poll(&mut cx),
            Poll::Ready(AdmissionWaitResult::Ready(42))
        );

        token.cancel();

        // The completed admission result remains 42.
        // The future must not be polled again after completion.
    }
}

#[cfg(test)]
mod destination_capacity_tests {
    use std::{
        future::Future,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll, Wake, Waker},
    };

    use marketforge_engine::live::{
        model::delivery::DeliveryCapacity,
        routing::destination::{CapacityError, DestinationCapacity},
    };

    #[derive(Default)]
    struct WakeCounter {
        count: AtomicUsize,
    }

    impl Wake for WakeCounter {
        fn wake(self: Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }

        fn wake_by_ref(self: &Arc<Self>) {
            self.count.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn manager(events: usize, bytes: u64, waiters: usize) -> DestinationCapacity {
        DestinationCapacity::new(DeliveryCapacity::new(events, bytes).unwrap(), waiters).unwrap()
    }

    fn test_waker() -> (Arc<WakeCounter>, Waker) {
        let counter = Arc::new(WakeCounter::default());
        let waker = Waker::from(counter.clone());

        (counter, waker)
    }

    #[test]
    fn reservation_accounts_for_events_and_bytes() {
        let capacity = manager(10, 1000, 10);

        let reservation = capacity.try_reserve(3, 400).unwrap();

        assert_eq!(reservation.events(), 3);
        assert_eq!(reservation.bytes(), 400);

        let usage = capacity.usage();

        assert_eq!(usage.occupied_events, 3);
        assert_eq!(usage.occupied_bytes, 400);
    }

    #[test]
    fn reservation_drop_releases_capacity() {
        let capacity = manager(10, 1000, 10);

        {
            let _reservation = capacity.try_reserve(10, 1000).unwrap();

            assert_eq!(capacity.try_reserve(1, 1).unwrap_err(), CapacityError::Full);
        }

        assert_eq!(capacity.usage().occupied_events, 0);
        assert_eq!(capacity.usage().occupied_bytes, 0);
    }

    #[test]
    fn event_and_byte_limits_are_independent() {
        let capacity = manager(10, 1000, 10);

        let _reservation = capacity.try_reserve(5, 900).unwrap();

        assert_eq!(
            capacity.try_reserve(1, 200).unwrap_err(),
            CapacityError::Full
        );

        assert_eq!(
            capacity.try_reserve(6, 10).unwrap_err(),
            CapacityError::Full
        );
    }

    #[test]
    fn oversized_requests_are_rejected() {
        let capacity = manager(10, 1000, 10);

        assert_eq!(
            capacity.try_reserve(11, 100).unwrap_err(),
            CapacityError::RequestTooLarge
        );

        assert_eq!(
            capacity.try_reserve(1, 1001).unwrap_err(),
            CapacityError::RequestTooLarge
        );

        assert_eq!(
            capacity.try_reserve(0, 100).unwrap_err(),
            CapacityError::RequestTooLarge
        );
    }

    #[test]
    fn committed_capacity_remains_occupied() {
        let capacity = manager(10, 1000, 10);

        let reservation = capacity.try_reserve(10, 1000).unwrap();

        let occupied = reservation.commit();

        assert_eq!(occupied.events(), 10);
        assert_eq!(occupied.bytes(), 1000);

        assert_eq!(capacity.try_reserve(1, 1).unwrap_err(), CapacityError::Full);

        drop(occupied);

        assert_eq!(capacity.usage().occupied_events, 0);
        assert_eq!(capacity.usage().occupied_bytes, 0);
    }

    #[test]
    fn waiting_future_is_woken_when_capacity_is_released() {
        let capacity = manager(1, 100, 10);

        let occupied = capacity.try_reserve(1, 100).unwrap();

        let mut waiting = Box::pin(capacity.reserve(1, 100));

        let (counter, waker) = test_waker();
        let mut context = Context::from_waker(&waker);

        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));

        drop(occupied);

        assert_eq!(counter.count.load(Ordering::SeqCst), 1);

        assert!(matches!(
            waiting.as_mut().poll(&mut context),
            Poll::Ready(Ok(_))
        ));
    }

    #[test]
    fn repeated_polling_does_not_duplicate_waiters() {
        let capacity = manager(1, 100, 10);

        let _occupied = capacity.try_reserve(1, 100).unwrap();

        let mut waiting = Box::pin(capacity.reserve(1, 100));

        let (_, waker) = test_waker();
        let mut context = Context::from_waker(&waker);

        for _ in 0..100 {
            assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));
        }

        assert_eq!(capacity.usage().waiting_operations, 1);
    }

    #[test]
    fn dropped_waiter_unregisters() {
        let capacity = manager(1, 100, 10);

        let _occupied = capacity.try_reserve(1, 100).unwrap();

        let (_, waker) = test_waker();
        let mut context = Context::from_waker(&waker);

        {
            let mut waiting = Box::pin(capacity.reserve(1, 100));

            assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));

            assert_eq!(capacity.usage().waiting_operations, 1);
        }

        assert_eq!(capacity.usage().waiting_operations, 0);
    }

    #[test]
    fn waiter_capacity_is_enforced() {
        let capacity = manager(1, 100, 1);

        let _occupied = capacity.try_reserve(1, 100).unwrap();

        let (_, waker) = test_waker();
        let mut context = Context::from_waker(&waker);

        let mut first = Box::pin(capacity.reserve(1, 100));
        let mut second = Box::pin(capacity.reserve(1, 100));

        assert!(matches!(first.as_mut().poll(&mut context), Poll::Pending));

        assert!(matches!(
            second.as_mut().poll(&mut context),
            Poll::Ready(Err(CapacityError::TooManyWaiters))
        ));
    }

    #[test]
    fn shutdown_rejects_new_reservations() {
        let capacity = manager(10, 1000, 10);

        capacity.shutdown();
        assert_eq!(
            capacity.try_reserve(1, 100).unwrap_err(),
            CapacityError::ShuttingDown
        );
    }

    #[test]
    fn shutdown_wakes_pending_waiters() {
        let capacity = manager(1, 100, 10);

        let _occupied = capacity.try_reserve(1, 100).unwrap();

        let mut waiting = Box::pin(capacity.reserve(1, 100));

        let (counter, waker) = test_waker();
        let mut context = Context::from_waker(&waker);

        assert!(matches!(waiting.as_mut().poll(&mut context), Poll::Pending));

        capacity.shutdown();

        assert_eq!(counter.count.load(Ordering::SeqCst), 1);

        assert!(matches!(
            waiting.as_mut().poll(&mut context),
            Poll::Ready(Err(CapacityError::ShuttingDown))
        ));

        assert_eq!(capacity.usage().waiting_operations, 0);
    }

    #[test]
    fn repeated_shutdown_is_idempotent() {
        let capacity = manager(10, 1000, 10);

        capacity.shutdown();
        capacity.shutdown();

        assert_eq!(
            capacity.try_reserve(1, 100).unwrap_err(),
            CapacityError::ShuttingDown
        );
    }

    #[test]
    fn capacity_is_released_after_shutdown() {
        let capacity = manager(10, 1000, 10);

        let reservation = capacity.try_reserve(5, 500).unwrap();

        capacity.shutdown();

        drop(reservation);

        let usage = capacity.usage();

        assert_eq!(usage.occupied_events, 0);
        assert_eq!(usage.occupied_bytes, 0);
    }

    #[test]
    fn multiple_waiters_are_woken_on_capacity_release() {
        let capacity = manager(2, 200, 10);

        let occupied = capacity.try_reserve(2, 200).unwrap();

        let mut first = Box::pin(capacity.reserve(1, 100));
        let mut second = Box::pin(capacity.reserve(1, 100));

        let (first_counter, first_waker) = test_waker();
        let (second_counter, second_waker) = test_waker();

        let mut first_context = Context::from_waker(&first_waker);
        let mut second_context = Context::from_waker(&second_waker);

        assert!(matches!(
            first.as_mut().poll(&mut first_context),
            Poll::Pending
        ));

        assert!(matches!(
            second.as_mut().poll(&mut second_context),
            Poll::Pending
        ));

        assert_eq!(capacity.usage().waiting_operations, 2);

        drop(occupied);

        assert_eq!(first_counter.count.load(Ordering::SeqCst), 1);

        assert_eq!(second_counter.count.load(Ordering::SeqCst), 1);

        let first_reservation = match first.as_mut().poll(&mut first_context) {
            Poll::Ready(Ok(reservation)) => reservation,
            _ => panic!("first waiter was not admitted"),
        };

        let second_reservation = match second.as_mut().poll(&mut second_context) {
            Poll::Ready(Ok(reservation)) => reservation,
            _ => panic!("second waiter was not admitted"),
        };

        assert_eq!(first_reservation.events(), 1);
        assert_eq!(second_reservation.events(), 1);

        assert_eq!(capacity.usage().occupied_events, 2);
        assert_eq!(capacity.usage().occupied_bytes, 200);
        assert_eq!(capacity.usage().waiting_operations, 0);
    }
}

#[cfg(test)]
mod destination_queue_tests {
    use marketforge_engine::live::model::memory::RetainedSize;
    use marketforge_engine::live::routing::destination::CapacityError;
    use marketforge_engine::live::routing::queue::AsyncCapacityAdmission;
    use marketforge_engine::live::{
        model::delivery::DeliveryCapacity,
        routing::{destination::DestinationCapacity, queue::DestinationQueue},
    };
    use marketforge_engine::live::{ports::routing::EventBatch, routing::queue::QueueError};
    use std::sync::Arc;
    use std::{
        collections::HashSet,
        sync::{Barrier, Mutex},
        thread,
    };

    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };

    use marketforge_engine::live::{
        foundation::cancellation::CancellationToken,
        routing::{admission::AdmissionContext, queue::AsyncQueueAdmissionError},
    };
    use std::pin::Pin;
    use std::time::{Duration, Instant};

    use crate::live::support::live_events::l2_event;
    fn queue() -> DestinationQueue {
        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(10, 1024).unwrap(), 10).unwrap();

        DestinationQueue::new(capacity)
    }

    #[test]
    fn new_queue_is_empty() {
        let queue = queue();
        let snapshot = queue.snapshot();

        assert_eq!(snapshot.queued_batches, 0);
        assert_eq!(snapshot.queued_events, 0);
        assert_eq!(snapshot.queued_bytes, 0);
        assert!(!snapshot.closed);
    }

    #[test]
    fn closing_queue_is_idempotent() {
        let queue = queue();

        queue.close();
        queue.close();

        assert!(queue.snapshot().closed);
    }

    #[test]
    fn shutdown_closes_queue_and_capacity() {
        let queue = queue();

        queue.shutdown();

        assert!(queue.snapshot().closed);
        assert!(queue.capacity().usage().shutting_down);
    }

    #[test]
    fn empty_queue_returns_no_batch() {
        let queue = queue();

        assert!(queue.try_dequeue().is_none());
    }

    #[test]
    fn discarding_empty_queue_is_safe() {
        let queue = queue();

        assert_eq!(queue.discard_queued(), 0);
        assert_eq!(queue.snapshot().queued_batches, 0);
    }

    #[test]
    fn queue_clones_share_state() {
        let queue = queue();
        let clone = queue.clone();

        clone.close();

        assert!(queue.snapshot().closed);
    }

    #[test]
    fn queue_accepts_and_preserves_event_identity() {
        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(10, u64::MAX).unwrap(), 10).unwrap();
        let queue = DestinationQueue::new(capacity);

        let event = l2_event("source-1", 1, 10);
        let expected_cursor = event.cursor.clone();

        let batch = EventBatch::new(vec![event.clone()]).unwrap();

        queue.try_enqueue(batch).unwrap();

        let dequeued = queue.try_dequeue().unwrap();

        assert_eq!(dequeued.batch().first_cursor(), &expected_cursor);

        assert!(Arc::ptr_eq(&event, &dequeued.batch().events()[0]));
    }

    #[test]
    fn queue_preserves_fifo_order() {
        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(10, u64::MAX).unwrap(), 10).unwrap();
        let queue = DestinationQueue::new(capacity);

        for offset in 1..=5 {
            let batch = EventBatch::new(vec![l2_event("source-1", 1, offset)]).unwrap();

            queue.try_enqueue(batch).unwrap();
        }

        for expected_offset in 1..=5 {
            let dequeued = queue.try_dequeue().unwrap();

            assert_eq!(dequeued.batch().first_cursor().offset, expected_offset);
        }

        assert!(queue.try_dequeue().is_none());
    }

    #[test]
    fn queue_retains_capacity_until_consumer_drops_batch() {
        let batch = EventBatch::new(vec![l2_event("source-1", 1, 1)]).unwrap();
        let retained = batch.retained_size_bytes().unwrap();

        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(10, retained * 2).unwrap(), 10).unwrap();
        let queue = DestinationQueue::new(capacity);

        queue.try_enqueue(batch).unwrap();

        let dequeued = queue.try_dequeue().unwrap();

        assert_eq!(queue.capacity().usage().occupied_bytes, retained);

        drop(dequeued);

        assert_eq!(queue.capacity().usage().occupied_bytes, 0);
    }

    #[test]
    fn queue_rejection_preserves_original_batch() {
        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(10, u64::MAX).unwrap(), 10).unwrap();
        let queue = DestinationQueue::new(capacity);

        queue.close();

        let event = l2_event("source-1", 1, 1);

        let batch = EventBatch::new(vec![event.clone()]).unwrap();

        let rejection = queue.try_enqueue(batch).unwrap_err();

        assert_eq!(rejection.reason, QueueError::Closed);

        assert!(Arc::ptr_eq(&event, &rejection.batch.events()[0]));
    }

    #[test]
    fn queue_rejects_when_capacity_is_exhausted() {
        let batch = EventBatch::new(vec![l2_event("source-1", 1, 1)]).unwrap();
        let retained = batch.retained_size_bytes().unwrap();

        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(1, retained * 2).unwrap(), 10).unwrap();
        let queue = DestinationQueue::new(capacity);

        let first = batch;

        queue.try_enqueue(first).unwrap();

        let second = EventBatch::new(vec![l2_event("source-1", 1, 2)]).unwrap();

        let rejection = queue.try_enqueue(second).unwrap_err();

        assert_eq!(
            rejection.reason,
            QueueError::Capacity(
                marketforge_engine::live::routing::destination::CapacityError::Full
            )
        );

        assert_eq!(rejection.batch.first_cursor().offset, 2);
    }
    #[test]
    fn concurrent_producers_preserve_all_accepted_events() {
        const PRODUCERS: usize = 8;
        const EVENTS_PER_PRODUCER: usize = 100;
        const TOTAL_EVENTS: usize = PRODUCERS * EVENTS_PER_PRODUCER;

        let test_batch = EventBatch::new(vec![l2_event("concurrent-source", 1, 0)]).unwrap();
        let retained_per_event = test_batch.retained_size_bytes().unwrap();

        let capacity = DestinationCapacity::new(
            DeliveryCapacity::new(TOTAL_EVENTS, retained_per_event * TOTAL_EVENTS as u64 + 1)
                .unwrap(),
            16,
        )
        .unwrap();

        let queue = DestinationQueue::new(capacity);
        let barrier = Arc::new(Barrier::new(PRODUCERS));

        let mut handles = Vec::new();

        for producer in 0..PRODUCERS {
            let queue = queue.clone();
            let barrier = barrier.clone();

            handles.push(thread::spawn(move || {
                barrier.wait();

                for index in 0..EVENTS_PER_PRODUCER {
                    let offset = (producer * EVENTS_PER_PRODUCER + index) as u64;

                    let batch =
                        EventBatch::new(vec![l2_event("concurrent-source", 1, offset)]).unwrap();

                    queue.try_enqueue(batch).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let snapshot = queue.snapshot();

        let total_retained: u64 = (0..TOTAL_EVENTS as u64)
            .map(|offset| {
                EventBatch::new(vec![l2_event("concurrent-source", 1, offset)])
                    .unwrap()
                    .retained_size_bytes()
                    .unwrap()
            })
            .sum();

        assert_eq!(snapshot.queued_batches, TOTAL_EVENTS);
        assert_eq!(snapshot.queued_events, TOTAL_EVENTS);
        assert_eq!(snapshot.queued_bytes, total_retained);

        let mut observed = HashSet::new();

        while let Some(item) = queue.try_dequeue() {
            let offset = item.batch().first_cursor().offset;

            assert!(observed.insert(offset), "duplicate cursor offset: {offset}");
        }

        assert_eq!(observed.len(), TOTAL_EVENTS);

        for offset in 0..TOTAL_EVENTS as u64 {
            assert!(observed.contains(&offset));
        }

        let usage = queue.capacity().usage();

        assert_eq!(usage.occupied_events, 0);
        assert_eq!(usage.occupied_bytes, 0);
    }

    #[test]
    fn concurrent_admission_never_exceeds_capacity() {
        const PRODUCERS: usize = 16;
        const CAPACITY: usize = 4;

        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(CAPACITY, u64::MAX).unwrap(), 16)
                .unwrap();

        let queue = DestinationQueue::new(capacity);
        let barrier = Arc::new(Barrier::new(PRODUCERS));

        let results = Arc::new(Mutex::new(Vec::new()));
        let mut handles = Vec::new();

        for producer in 0..PRODUCERS {
            let queue = queue.clone();
            let barrier = barrier.clone();
            let results = results.clone();

            handles.push(thread::spawn(move || {
                let batch =
                    EventBatch::new(vec![l2_event("capacity-source", 1, producer as u64)]).unwrap();

                barrier.wait();

                let result = queue.try_enqueue(batch);

                results.lock().unwrap().push(result);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let results = results.lock().unwrap();

        let accepted = results.iter().filter(|result| result.is_ok()).count();

        let rejected = results
            .iter()
            .filter(|result| {
                matches!(
                    result,
                    Err(rejection)
                        if rejection.reason
                            == QueueError::Capacity(CapacityError::Full)
                )
            })
            .count();

        assert_eq!(accepted, CAPACITY);
        assert_eq!(rejected, PRODUCERS - CAPACITY);

        let test_batch = EventBatch::new(vec![l2_event("capacity-source", 1, 0)]).unwrap();
        let retained = test_batch.retained_size_bytes().unwrap();

        let snapshot = queue.snapshot();

        assert_eq!(snapshot.queued_batches, CAPACITY);
        assert_eq!(snapshot.queued_events, CAPACITY);
        assert_eq!(snapshot.queued_bytes, retained * CAPACITY as u64);

        let usage = queue.capacity().usage();

        assert_eq!(usage.occupied_events, CAPACITY);
        assert_eq!(usage.occupied_bytes, retained * CAPACITY as u64);
    }

    #[test]
    fn dequeued_batches_retain_capacity_under_concurrent_consumption() {
        let test_batch = EventBatch::new(vec![l2_event("consumer-source", 1, 1)]).unwrap();
        let retained = test_batch.retained_size_bytes().unwrap();

        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(1, retained * 2).unwrap(), 4).unwrap();

        let queue = DestinationQueue::new(capacity);

        let batch = EventBatch::new(vec![l2_event("consumer-source", 1, 1)]).unwrap();

        queue.try_enqueue(batch).unwrap();

        let consumer_queue = queue.clone();

        let barrier = Arc::new(Barrier::new(2));
        let consumer_barrier = barrier.clone();

        let handle = thread::spawn(move || {
            let item = consumer_queue.try_dequeue().unwrap();

            // Signal that the consumer owns the batch.
            consumer_barrier.wait();

            // Keep the permit alive until the main thread
            // has checked that capacity remains occupied.
            consumer_barrier.wait();

            drop(item);
        });

        barrier.wait();

        assert_eq!(queue.snapshot().queued_batches, 0);
        assert_eq!(queue.capacity().usage().occupied_events, 1);

        assert_eq!(
            queue.capacity().try_reserve(1, 1).unwrap_err(),
            CapacityError::Full
        );

        barrier.wait();
        handle.join().unwrap();

        assert_eq!(queue.capacity().usage().occupied_events, 0);
        assert_eq!(queue.capacity().usage().occupied_bytes, 0);
    }

    #[test]
    fn discarded_batches_release_all_queued_capacity() {
        let batch = EventBatch::new(vec![l2_event("discard-source", 1, 1)]).unwrap();
        let retained = batch.retained_size_bytes().unwrap();

        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(10, retained * 10).unwrap(), 10)
                .unwrap();
        let queue = DestinationQueue::new(capacity);

        for offset in 1..=5 {
            let batch = EventBatch::new(vec![l2_event("discard-source", 1, offset)]).unwrap();

            queue.try_enqueue(batch).unwrap();
        }

        assert_eq!(queue.capacity().usage().occupied_events, 5);

        let total: u64 = (1..=5)
            .map(|offset| {
                EventBatch::new(vec![l2_event("discard-source", 1, offset)])
                    .unwrap()
                    .retained_size_bytes()
                    .unwrap()
            })
            .sum();
        assert_eq!(queue.capacity().usage().occupied_bytes, total);

        let discarded = queue.discard_queued();

        assert_eq!(discarded, 5);
        assert!(queue.try_dequeue().is_none());

        let usage = queue.capacity().usage();

        assert_eq!(usage.occupied_events, 0);
        assert_eq!(usage.occupied_bytes, 0);
    }
    #[test]
    fn retained_size_is_positive() {
        let event = l2_event("source-1", 1, 1);

        let size = event.retained_size_bytes().unwrap();

        assert!(size > 0);
    }

    #[test]
    fn retained_size_increases_with_stream_identity_length() {
        let short = l2_event("a", 1, 1);

        let long = l2_event(
            "a-very-long-stream-identity-used-for-memory-accounting",
            1,
            1,
        );

        assert!(long.retained_size_bytes().unwrap() > short.retained_size_bytes().unwrap());
    }

    #[test]
    fn batch_retained_size_includes_all_events() {
        let first = l2_event("source-1", 1, 1);
        let second = l2_event("source-1", 1, 2);

        let single = EventBatch::new(vec![first.clone()]).unwrap();

        let multiple = EventBatch::new(vec![first, second]).unwrap();

        assert!(multiple.retained_size_bytes().unwrap() > single.retained_size_bytes().unwrap());
    }

    #[test]
    fn retained_size_is_deterministic() {
        let event = l2_event("source-1", 1, 1);

        let first = event.retained_size_bytes().unwrap();
        let second = event.retained_size_bytes().unwrap();

        assert_eq!(first, second);
    }
    #[test]
    fn dropping_pending_reservation_preserves_caller_batch() {
        let event = l2_event("ownership-source", 1, 1);
        let batch = EventBatch::new(vec![event.clone()]).unwrap();

        let bytes = batch.retained_size_bytes().unwrap();

        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(1, bytes).unwrap(), 4).unwrap();

        let queue = DestinationQueue::new(capacity);

        let occupied = queue.capacity().try_reserve(1, bytes).unwrap();

        let context = AdmissionContext::without_deadline(CancellationToken::new());

        let mut future = Box::pin(queue.reserve_async(&batch, context, None));

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(matches!(future.as_mut().poll(&mut cx), Poll::Pending));

        drop(future);

        assert!(Arc::ptr_eq(&event, &batch.events()[0]));

        assert_eq!(queue.capacity().usage().waiting_operations, 0);

        drop(occupied);
    }

    #[test]
    fn cancelled_reservation_preserves_caller_batch() {
        let event = l2_event("ownership-source", 1, 1);
        let batch = EventBatch::new(vec![event.clone()]).unwrap();

        let token = CancellationToken::new();
        token.cancel();

        let context = AdmissionContext::without_deadline(token);

        let queue = queue();
        let mut future = Box::pin(queue.reserve_async(&batch, context, None));

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        assert!(matches!(
            future.as_mut().poll(&mut cx),
            Poll::Ready(AsyncCapacityAdmission::Rejected(
                AsyncQueueAdmissionError::Cancelled
            ))
        ));

        assert!(Arc::ptr_eq(&event, &batch.events()[0]));
    }

    #[test]
    fn reserved_capacity_can_be_committed_by_caller() {
        let event = l2_event("ownership-source", 1, 1);
        let batch = EventBatch::new(vec![event]).unwrap();

        let bytes = batch.retained_size_bytes().unwrap();

        let capacity =
            DestinationCapacity::new(DeliveryCapacity::new(1, bytes).unwrap(), 4).unwrap();

        let queue = DestinationQueue::new(capacity);

        let context = AdmissionContext::without_deadline(CancellationToken::new());

        let mut future = Box::pin(queue.reserve_async(&batch, context, None));

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);

        let reservation = match future.as_mut().poll(&mut cx) {
            Poll::Ready(AsyncCapacityAdmission::Reserved(reservation)) => reservation,

            _ => panic!("expected capacity reservation"),
        };

        drop(future);

        queue.enqueue_reserved(batch, reservation).unwrap();

        assert_eq!(queue.snapshot().queued_batches, 1);
    }
    #[cfg(test)]
    mod admission_transaction_tests {
        use marketforge_engine::live::{
            foundation::cancellation::CancellationToken,
            model::{delivery::DeliveryCapacity, memory::RetainedSize},
            ports::routing::EventBatch,
            routing::{
                admission::AdmissionContext,
                destination::DestinationCapacity,
                queue::DestinationQueue,
                transaction::{AdmissionTransaction, TransactionError},
            },
        };
        use std::{
            future::Future,
            sync::Arc,
            task::{Context, Poll, Waker},
        };
        use std::{sync::Barrier, thread};

        use marketforge_engine::live::routing::queue::QueueError;

        use crate::live::support::live_events::l2_event;

        fn queue_for(batch: &EventBatch) -> DestinationQueue {
            let bytes = batch.retained_size_bytes().unwrap();

            let capacity =
                DestinationCapacity::new(DeliveryCapacity::new(batch.len(), bytes).unwrap(), 4)
                    .unwrap();

            DestinationQueue::new(capacity)
        }

        #[test]
        fn transaction_preparation_preserves_batch_ownership() {
            let event = l2_event("transaction-source", 1, 1);
            let batch = EventBatch::new(vec![event.clone()]).unwrap();

            let queue = queue_for(&batch);

            let context = AdmissionContext::without_deadline(CancellationToken::new());

            let mut future = Box::pin(AdmissionTransaction::prepare(&queue, &batch, context, None));

            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);

            let transaction = match future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(transaction)) => transaction,
                _ => panic!("expected prepared transaction"),
            };

            drop(future);

            assert!(Arc::ptr_eq(&event, &batch.events()[0]));

            drop(transaction);

            assert_eq!(queue.capacity().usage().occupied_events, 0);
        }

        #[test]
        fn transaction_commit_transfers_ownership() {
            let batch = EventBatch::new(vec![l2_event("transaction-source", 1, 1)]).unwrap();

            let queue = queue_for(&batch);

            let context = AdmissionContext::without_deadline(CancellationToken::new());

            let mut future = Box::pin(AdmissionTransaction::prepare(&queue, &batch, context, None));

            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);

            let transaction = match future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(transaction)) => transaction,
                _ => panic!("expected prepared transaction"),
            };

            drop(future);

            transaction.commit(&queue, batch).unwrap();

            assert_eq!(queue.snapshot().queued_batches, 1);
            assert_eq!(queue.capacity().usage().occupied_events, 1);
        }

        #[test]
        fn cancellation_before_commit_preserves_batch() {
            let batch = EventBatch::new(vec![l2_event("transaction-source", 1, 1)]).unwrap();

            let queue = queue_for(&batch);
            let token = CancellationToken::new();

            let context = AdmissionContext::without_deadline(token.clone());

            let mut future = Box::pin(AdmissionTransaction::prepare(&queue, &batch, context, None));

            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);

            let transaction = match future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(transaction)) => transaction,
                _ => panic!("expected prepared transaction"),
            };

            drop(future);

            token.cancel();

            let rejection = transaction.commit(&queue, batch).unwrap_err();

            assert_eq!(rejection.reason, TransactionError::Cancelled);
            assert_eq!(queue.snapshot().queued_batches, 0);

            drop(rejection);

            assert_eq!(queue.capacity().usage().occupied_events, 0);
        }

        #[test]
        fn closed_queue_rejects_prepared_transaction() {
            let batch = EventBatch::new(vec![l2_event("transaction-source", 1, 1)]).unwrap();

            let queue = queue_for(&batch);

            let context = AdmissionContext::without_deadline(CancellationToken::new());

            let mut future = Box::pin(AdmissionTransaction::prepare(&queue, &batch, context, None));

            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);

            let transaction = match future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(transaction)) => transaction,
                _ => panic!("expected prepared transaction"),
            };

            drop(future);

            queue.close();

            let rejection = transaction.commit(&queue, batch).unwrap_err();

            assert_eq!(
                rejection.reason,
                TransactionError::Queue(
                    marketforge_engine::live::routing::queue::QueueError::Closed
                )
            );

            assert_eq!(queue.snapshot().queued_batches, 0);
        }
        #[test]
        fn cancellation_after_commit_does_not_revoke_admission() {
            let batch = EventBatch::new(vec![l2_event("transaction-source", 1, 1)]).unwrap();

            let queue = queue_for(&batch);
            let token = CancellationToken::new();

            let context = AdmissionContext::without_deadline(token.clone());

            let mut future = Box::pin(AdmissionTransaction::prepare(&queue, &batch, context, None));

            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);

            let transaction = match future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(transaction)) => transaction,
                _ => panic!("expected prepared transaction"),
            };

            drop(future);

            transaction.commit(&queue, batch).unwrap();

            token.cancel();

            assert_eq!(queue.snapshot().queued_batches, 1);
            assert_eq!(queue.capacity().usage().occupied_events, 1);
        }

        #[test]
        fn shutdown_before_commit_rejects_without_losing_batch() {
            let event = l2_event("transaction-source", 1, 1);
            let batch = EventBatch::new(vec![event.clone()]).unwrap();

            let queue = queue_for(&batch);

            let context = AdmissionContext::without_deadline(CancellationToken::new());

            let mut future = Box::pin(AdmissionTransaction::prepare(&queue, &batch, context, None));

            let waker = Waker::noop();
            let mut cx = Context::from_waker(waker);

            let transaction = match future.as_mut().poll(&mut cx) {
                Poll::Ready(Ok(transaction)) => transaction,
                _ => panic!("expected prepared transaction"),
            };

            drop(future);

            queue.shutdown();

            let rejection = transaction.commit(&queue, batch).unwrap_err();

            assert_eq!(
                rejection.reason,
                TransactionError::Queue(QueueError::Closed)
            );

            assert!(Arc::ptr_eq(&event, &rejection.batch.events()[0]));

            assert_eq!(queue.snapshot().queued_batches, 0);

            drop(rejection);

            assert_eq!(queue.capacity().usage().occupied_events, 0);
        }

        #[test]
        fn concurrent_cancellation_and_commit_have_consistent_outcome() {
            const ITERATIONS: usize = 100;

            for offset in 0..ITERATIONS {
                let batch =
                    EventBatch::new(vec![l2_event("race-source", 1, offset as u64)]).unwrap();

                let queue = queue_for(&batch);
                let token = CancellationToken::new();

                let context = AdmissionContext::without_deadline(token.clone());

                let mut future =
                    Box::pin(AdmissionTransaction::prepare(&queue, &batch, context, None));

                let waker = Waker::noop();
                let mut cx = Context::from_waker(waker);

                let transaction = match future.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(transaction)) => transaction,
                    _ => panic!("expected prepared transaction"),
                };

                drop(future);

                let barrier = Arc::new(Barrier::new(2));
                let cancellation_barrier = barrier.clone();

                let cancellation_thread = thread::spawn(move || {
                    cancellation_barrier.wait();
                    token.cancel();
                });

                barrier.wait();

                let result = transaction.commit(&queue, batch);

                cancellation_thread.join().unwrap();

                match result {
                    Ok(()) => {
                        assert_eq!(queue.snapshot().queued_batches, 1);
                        assert_eq!(queue.capacity().usage().occupied_events, 1);
                    }

                    Err(rejection) => {
                        assert_eq!(rejection.reason, TransactionError::Cancelled);

                        assert_eq!(queue.snapshot().queued_batches, 0);

                        drop(rejection);

                        assert_eq!(queue.capacity().usage().occupied_events, 0);
                    }
                }
            }
        }
        #[test]
        fn concurrent_shutdown_and_commit_have_consistent_outcome() {
            const ITERATIONS: usize = 100;

            for offset in 0..ITERATIONS {
                let batch =
                    EventBatch::new(vec![l2_event("shutdown-race-source", 1, offset as u64)])
                        .unwrap();

                let queue = queue_for(&batch);

                let context = AdmissionContext::without_deadline(CancellationToken::new());

                let mut future =
                    Box::pin(AdmissionTransaction::prepare(&queue, &batch, context, None));

                let waker = Waker::noop();
                let mut cx = Context::from_waker(waker);

                let transaction = match future.as_mut().poll(&mut cx) {
                    Poll::Ready(Ok(transaction)) => transaction,
                    _ => panic!("expected prepared transaction"),
                };

                drop(future);

                let barrier = Arc::new(Barrier::new(2));
                let shutdown_barrier = barrier.clone();
                let shutdown_queue = queue.clone();

                let shutdown_thread = thread::spawn(move || {
                    shutdown_barrier.wait();
                    shutdown_queue.shutdown();
                });

                barrier.wait();

                let result = transaction.commit(&queue, batch);

                shutdown_thread.join().unwrap();

                match result {
                    Ok(()) => {
                        assert_eq!(queue.snapshot().queued_batches, 1);
                        assert_eq!(queue.capacity().usage().occupied_events, 1);
                    }

                    Err(rejection) => {
                        assert_eq!(
                            rejection.reason,
                            TransactionError::Queue(QueueError::Closed)
                        );

                        assert_eq!(queue.snapshot().queued_batches, 0);

                        drop(rejection);

                        assert_eq!(queue.capacity().usage().occupied_events, 0);
                    }
                }
            }
        }
    }
}
