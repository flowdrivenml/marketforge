use marketforge_engine::live::routing::tracking_recovery::TrackingApplyFailure;
use marketforge_engine::live::routing::tracking_recovery::TrackingRecoveryError;
use marketforge_engine::live::routing::tracking_recovery::TrackingRecoveryValidationError;
use marketforge_engine::live::routing::tracking_recovery::{
    TrackingApplyError, TrackingApplyOutcome,
};
use marketforge_engine::live::routing::{
    acknowledgement::{AcknowledgementError, AcknowledgementTracker, DeliveryObligation},
    tracking_recovery::TrackingRecoveryOutcome,
};
use marketforge_engine::live::{
    model::delivery::{DeliveryPolicy, DeliveryStage, DeliveryStatus, DestinationKind},
    ports::routing::DestinationId,
    routing::{
        acknowledgement::{DeliveryAcknowledgement, DeliveryCursorRange, DeliveryObligationId},
        tracking_recovery::TrackingRecovery,
    },
};

use crate::live::support::live_events::l2_event;

fn acknowledgement() -> DeliveryAcknowledgement {
    let batch = marketforge_engine::live::ports::routing::EventBatch::new(vec![
        l2_event("recovery-source", 1, 10),
        l2_event("recovery-source", 1, 11),
    ])
    .unwrap();

    DeliveryAcknowledgement {
        obligation: DeliveryObligationId(1),
        destination: DestinationId(7),
        range: DeliveryCursorRange::new(batch.first_cursor().clone(), batch.last_cursor().clone())
            .unwrap(),
        status: DeliveryStatus::Pending,
    }
}

#[test]
fn recovery_reservation_occupies_capacity() {
    let recovery = TrackingRecovery::new(2).unwrap();

    let reservation = recovery.reserve().unwrap();

    assert_eq!(recovery.occupied(), 1);
    assert_eq!(recovery.available(), 1);

    drop(reservation);

    assert_eq!(recovery.occupied(), 0);
    assert_eq!(recovery.available(), 2);
}

#[test]
fn retained_update_preserves_capacity() {
    let mut recovery = TrackingRecovery::new(1).unwrap();

    let reservation = recovery.reserve().unwrap();

    recovery.retain(acknowledgement(), reservation).unwrap();

    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery.occupied(), 1);
    assert_eq!(recovery.available(), 0);
}

#[test]
fn recovery_capacity_exhaustion_is_rejected() {
    let recovery = TrackingRecovery::new(1).unwrap();

    let _reservation = recovery.reserve().unwrap();

    assert!(recovery.reserve().is_err());
}

#[test]
fn foreign_recovery_reservation_is_rejected() {
    let recovery_a = TrackingRecovery::new(2).unwrap();
    let mut recovery_b = TrackingRecovery::new(2).unwrap();

    let reservation = recovery_a.reserve().unwrap();

    assert_eq!(recovery_a.occupied(), 1);
    assert_eq!(recovery_b.occupied(), 0);

    let result = recovery_b.retain(acknowledgement(), reservation);

    assert_eq!(result, Err(TrackingRecoveryError::ForeignReservation));

    // The foreign reservation was dropped on rejection.
    assert_eq!(recovery_a.occupied(), 0);

    // No update was inserted into the wrong owner.
    assert_eq!(recovery_b.len(), 0);
    assert_eq!(recovery_b.occupied(), 0);
}

#[test]
fn retry_empty_recovery_returns_empty() {
    let mut recovery = TrackingRecovery::new(2).unwrap();
    let mut tracker = AcknowledgementTracker::new(2).unwrap();

    assert_eq!(
        recovery.retry_next(&mut tracker),
        TrackingRecoveryOutcome::Empty
    );

    assert_eq!(recovery.occupied(), 0);
}

#[test]
fn failed_retry_preserves_update_and_capacity() {
    let mut recovery = TrackingRecovery::new(1).unwrap();
    let mut tracker = AcknowledgementTracker::new(1).unwrap();

    let reservation = recovery.reserve().unwrap();

    recovery.retain(acknowledgement(), reservation).unwrap();

    // No matching obligation exists in the tracker.
    let outcome = recovery.retry_next(&mut tracker);

    assert_eq!(
        outcome,
        TrackingRecoveryOutcome::RetryRequired {
            error: AcknowledgementError::UnknownObligation,
            remaining: 1,
        }
    );

    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery.occupied(), 1);
    assert_eq!(recovery.available(), 0);
}

#[test]
fn successful_retry_releases_recovery_capacity() {
    let mut recovery = TrackingRecovery::new(1).unwrap();
    let mut tracker = AcknowledgementTracker::new(1).unwrap();

    let acknowledgement = acknowledgement();

    tracker
        .register(DeliveryObligation {
            id: acknowledgement.obligation,
            destination: acknowledgement.destination,
            range: acknowledgement.range.clone(),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        })
        .unwrap();

    let reservation = recovery.reserve().unwrap();

    recovery
        .retain(
            DeliveryAcknowledgement {
                status: DeliveryStatus::Progress(DeliveryStage::Queued),
                ..acknowledgement
            },
            reservation,
        )
        .unwrap();

    assert_eq!(recovery.occupied(), 1);

    assert_eq!(
        recovery.retry_next(&mut tracker),
        TrackingRecoveryOutcome::Recovered { remaining: 0 }
    );

    assert_eq!(recovery.len(), 0);
    assert_eq!(recovery.occupied(), 0);
    assert_eq!(recovery.available(), 1);
}

#[test]
fn batch_reservation_acquires_all_slots() {
    let recovery = TrackingRecovery::new(3).unwrap();

    let reservations = recovery.reserve_batch(3).unwrap();

    assert_eq!(reservations.len(), 3);
    assert_eq!(recovery.occupied(), 3);
    assert_eq!(recovery.available(), 0);

    drop(reservations);

    assert_eq!(recovery.occupied(), 0);
    assert_eq!(recovery.available(), 3);
}

#[test]
fn failed_batch_reservation_releases_partial_capacity() {
    let recovery = TrackingRecovery::new(2).unwrap();

    let result = recovery.reserve_batch(3);

    assert!(result.is_err());

    // All partially acquired reservations must be released.
    assert_eq!(recovery.occupied(), 0);
    assert_eq!(recovery.available(), 2);
}

#[test]
fn batch_reservation_respects_existing_occupancy() {
    let mut recovery = TrackingRecovery::new(3).unwrap();

    let reservation = recovery.reserve().unwrap();

    recovery.retain(acknowledgement(), reservation).unwrap();

    assert_eq!(recovery.occupied(), 1);

    // Only two slots remain.
    assert!(recovery.reserve_batch(3).is_err());

    // Failed reservation must not affect retained updates.
    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery.occupied(), 1);

    let reservations = recovery.reserve_batch(2).unwrap();

    assert_eq!(reservations.len(), 2);
    assert_eq!(recovery.occupied(), 3);
}

#[test]
fn valid_recovery_reservations_pass_validation() {
    let recovery = TrackingRecovery::new(2).unwrap();

    let reservations = recovery.reserve_batch(2).unwrap();

    assert_eq!(recovery.validate_reservations(&reservations, 2), Ok(()));

    assert_eq!(recovery.occupied(), 2);
}

#[test]
fn recovery_reservation_count_mismatch_is_rejected() {
    let recovery = TrackingRecovery::new(3).unwrap();

    let reservations = recovery.reserve_batch(2).unwrap();

    assert_eq!(
        recovery.validate_reservations(&reservations, 3),
        Err(TrackingRecoveryValidationError::CountMismatch)
    );

    assert_eq!(recovery.occupied(), 2);
}

#[test]
fn foreign_recovery_reservations_fail_validation() {
    let recovery_a = TrackingRecovery::new(2).unwrap();
    let recovery_b = TrackingRecovery::new(2).unwrap();

    let reservations = recovery_a.reserve_batch(2).unwrap();

    assert_eq!(
        recovery_b.validate_reservations(&reservations, 2),
        Err(TrackingRecoveryValidationError::ForeignReservation)
    );

    assert_eq!(recovery_a.occupied(), 2);
    assert_eq!(recovery_b.occupied(), 0);
}

#[test]
fn recovery_batch_owns_all_reservations() {
    let recovery = TrackingRecovery::new(3).unwrap();

    let batch = recovery.prepare_batch(3).unwrap();

    assert_eq!(batch.len(), 3);
    assert_eq!(recovery.occupied(), 3);
    assert_eq!(recovery.available(), 0);

    drop(batch);

    assert_eq!(recovery.occupied(), 0);
    assert_eq!(recovery.available(), 3);
}

#[test]
fn recovery_batch_preserves_reservations_after_transfer() {
    let recovery = TrackingRecovery::new(2).unwrap();

    let batch = recovery.prepare_batch(2).unwrap();

    let reservations = batch.into_reservations();

    assert_eq!(reservations.len(), 2);
    assert_eq!(recovery.occupied(), 2);

    drop(reservations);

    assert_eq!(recovery.occupied(), 0);
}

#[test]
fn apply_success_releases_recovery_capacity() {
    let mut recovery = TrackingRecovery::new(1).unwrap();
    let mut tracker = AcknowledgementTracker::new(1).unwrap();

    let mut ack = acknowledgement();
    ack.status = DeliveryStatus::Progress(DeliveryStage::Queued);

    tracker
        .register(DeliveryObligation {
            id: ack.obligation,
            destination: ack.destination,
            range: ack.range.clone(),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        })
        .unwrap();

    let reservation = recovery.reserve().unwrap();

    assert_eq!(recovery.occupied(), 1);

    let outcome = recovery
        .apply_or_retain(&mut tracker, ack, reservation)
        .unwrap();

    assert_eq!(outcome, TrackingApplyOutcome::Applied);
    assert_eq!(recovery.len(), 0);
    assert_eq!(recovery.occupied(), 0);
}

#[test]
fn apply_failure_retains_update_and_capacity() {
    let mut recovery = TrackingRecovery::new(1).unwrap();
    let mut tracker = AcknowledgementTracker::new(1).unwrap();

    let mut ack = acknowledgement();
    ack.status = DeliveryStatus::Progress(DeliveryStage::Queued);

    // No matching obligation is registered.
    let reservation = recovery.reserve().unwrap();

    let outcome = recovery
        .apply_or_retain(&mut tracker, ack, reservation)
        .unwrap();

    assert_eq!(
        outcome,
        TrackingApplyOutcome::Retained {
            error: AcknowledgementError::UnknownObligation,
        }
    );

    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery.occupied(), 1);
    assert_eq!(recovery.available(), 0);
}
#[test]
fn apply_rejects_foreign_reservation_before_acknowledgement() {
    let recovery_a = TrackingRecovery::new(1).unwrap();
    let mut recovery_b = TrackingRecovery::new(1).unwrap();
    let mut tracker = AcknowledgementTracker::new(1).unwrap();

    let mut ack = acknowledgement();
    ack.status = DeliveryStatus::Progress(DeliveryStage::Queued);

    tracker
        .register(DeliveryObligation {
            id: ack.obligation,
            destination: ack.destination,
            range: ack.range.clone(),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        })
        .unwrap();

    let reservation = recovery_a.reserve().unwrap();

    let result = recovery_b.apply_or_retain(&mut tracker, ack.clone(), reservation);

    let failure = result.unwrap_err();

    assert_eq!(failure.reason, TrackingApplyError::ForeignReservation);

    assert_eq!(failure.acknowledgement.obligation, ack.obligation);

    // The original reservation is still owned by the failure.
    assert_eq!(recovery_a.occupied(), 1);

    assert_eq!(
        tracker.status(ack.obligation),
        Some(DeliveryStatus::Pending)
    );

    assert_eq!(recovery_b.len(), 0);

    // Explicitly release the rejected reservation.
    drop(failure);

    assert_eq!(recovery_a.occupied(), 0);

    // The acknowledgement must not have been applied.
    assert_eq!(
        tracker.status(ack.obligation),
        Some(DeliveryStatus::Pending)
    );

    assert_eq!(recovery_b.len(), 0);
    assert_eq!(recovery_b.occupied(), 0);
}

#[test]
fn failed_application_returns_reusable_reservation() {
    let recovery_a = TrackingRecovery::new(1).unwrap();
    let mut recovery_b = TrackingRecovery::new(1).unwrap();

    let mut tracker = AcknowledgementTracker::new(1).unwrap();

    let mut ack = acknowledgement();
    ack.status = DeliveryStatus::Progress(DeliveryStage::Queued);

    let reservation = recovery_a.reserve().unwrap();

    let failure = recovery_b
        .apply_or_retain(&mut tracker, ack.clone(), reservation)
        .unwrap_err();

    assert_eq!(failure.reason, TrackingApplyError::ForeignReservation);

    assert_eq!(recovery_a.occupied(), 1);

    // The acknowledgement and reservation can be returned
    // to their original recovery owner.
    let outcome = {
        let TrackingApplyFailure {
            acknowledgement,
            reservation,
            ..
        } = failure;

        // No obligation exists, so the original owner
        // must retain the failed acknowledgement.
        let mut original_owner = recovery_a;

        let outcome = original_owner
            .apply_or_retain(&mut tracker, acknowledgement, reservation)
            .unwrap();

        assert_eq!(original_owner.len(), 1);
        assert_eq!(original_owner.occupied(), 1);

        outcome
    };

    assert_eq!(
        outcome,
        TrackingApplyOutcome::Retained {
            error: AcknowledgementError::UnknownObligation,
        }
    );
}

#[test]
fn unresolved_outcome_retains_recovery_capacity() {
    let mut recovery = TrackingRecovery::new(1).unwrap();

    let reservation = recovery.reserve().unwrap();

    let range = acknowledgement().range;

    recovery
        .retain_unresolved(
            DestinationId(7),
            range.clone(),
            DeliveryStatus::Progress(DeliveryStage::Queued),
            reservation,
        )
        .unwrap();

    assert_eq!(recovery.unresolved_len(), 1);
    assert_eq!(recovery.occupied(), 1);
    assert_eq!(recovery.available(), 0);
}

#[test]
fn unresolved_outcome_rejects_foreign_reservation() {
    let recovery_a = TrackingRecovery::new(1).unwrap();
    let mut recovery_b = TrackingRecovery::new(1).unwrap();

    let reservation = recovery_a.reserve().unwrap();
    let range = acknowledgement().range;

    let failure = recovery_b
        .retain_unresolved(
            DestinationId(7),
            range.clone(),
            DeliveryStatus::Progress(DeliveryStage::Queued),
            reservation,
        )
        .unwrap_err();

    assert_eq!(failure.reason, TrackingApplyError::ForeignReservation);

    assert_eq!(recovery_a.occupied(), 1);
    assert_eq!(recovery_b.unresolved_len(), 0);

    drop(failure);

    assert_eq!(recovery_a.occupied(), 0);
}

#[test]
fn unresolved_retention_failure_preserves_cursor_identity() {
    let recovery_a = TrackingRecovery::new(1).unwrap();
    let mut recovery_b = TrackingRecovery::new(1).unwrap();

    let range = acknowledgement().range;
    let reservation = recovery_a.reserve().unwrap();

    let failure = recovery_b
        .retain_unresolved(
            DestinationId(7),
            range.clone(),
            DeliveryStatus::Progress(DeliveryStage::Queued),
            reservation,
        )
        .unwrap_err();

    assert_eq!(failure.reason, TrackingApplyError::ForeignReservation);

    assert_eq!(failure.destination, DestinationId(7));
    assert_eq!(failure.range, range);

    // Failed retention still owns recovery capacity.
    assert_eq!(recovery_a.occupied(), 1);

    drop(failure);

    assert_eq!(recovery_a.occupied(), 0);
    assert_eq!(recovery_b.unresolved_len(), 0);
}

#[test]
fn recovery_batch_rejects_unbounded_counts_before_allocation() {
    use marketforge_engine::live::routing::destination::CapacityError;
    let recovery = TrackingRecovery::new(2).unwrap();
    for count in [3, usize::MAX] {
        assert_eq!(
            recovery.reserve_batch(count).unwrap_err(),
            CapacityError::Full
        );
        assert_eq!(recovery.occupied(), 0);
    }
    let held = recovery.reserve().unwrap();
    assert_eq!(recovery.reserve_batch(2).unwrap_err(), CapacityError::Full);
    assert_eq!(recovery.occupied(), 1);
    let remaining = recovery.prepare_batch(1).unwrap();
    assert_eq!(recovery.available(), 0);
    assert!(recovery.prepare_batch(0).unwrap().is_empty());
    drop(remaining);
    drop(held);
    assert_eq!(recovery.available(), 2);
    let all = recovery.prepare_batch(2).unwrap();
    assert_eq!(all.len(), 2);
    drop(all);
    assert_eq!(recovery.occupied(), 0);
}

#[test]
fn unresolved_reconciliation_validates_identity_before_releasing_capacity() {
    let mut recovery = TrackingRecovery::new(2).unwrap();
    let mut tracker = AcknowledgementTracker::new(4).unwrap();
    let ack = acknowledgement();
    let queued = DeliveryStatus::Progress(DeliveryStage::Queued);
    for _ in 0..2 {
        let reservation = recovery.reserve().unwrap();
        recovery
            .retain_unresolved(ack.destination, ack.range.clone(), queued, reservation)
            .unwrap();
    }
    assert_eq!(recovery.next_unresolved().unwrap().range(), &ack.range);
    assert_eq!(
        recovery.next_unresolved().unwrap().destination(),
        ack.destination
    );
    assert_eq!(recovery.next_unresolved().unwrap().status(), queued);
    assert_eq!(
        recovery.reconcile_next_unresolved(&mut tracker, ack.obligation),
        TrackingRecoveryOutcome::RetryRequired {
            error: AcknowledgementError::UnknownObligation,
            remaining: 2,
        }
    );
    for (id, destination, range, error) in [
        (
            2,
            DestinationId(8),
            ack.range.clone(),
            AcknowledgementError::DestinationMismatch,
        ),
        (
            3,
            ack.destination,
            DeliveryCursorRange::new(ack.range.first.clone(), ack.range.first.clone()).unwrap(),
            AcknowledgementError::RangeMismatch,
        ),
    ] {
        tracker
            .register(DeliveryObligation {
                id: DeliveryObligationId(id),
                destination,
                range,
                kind: DestinationKind::Recording,
                policy: DeliveryPolicy::Required,
            })
            .unwrap();
        assert_eq!(
            recovery.reconcile_next_unresolved(&mut tracker, DeliveryObligationId(id)),
            TrackingRecoveryOutcome::RetryRequired {
                error,
                remaining: 2
            }
        );
        assert_eq!(
            tracker.status(DeliveryObligationId(id)),
            Some(DeliveryStatus::Pending)
        );
        assert_eq!(recovery.occupied(), 2);
    }
    tracker
        .register(DeliveryObligation {
            id: ack.obligation,
            destination: ack.destination,
            range: ack.range,
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        })
        .unwrap();
    for remaining in [1, 0] {
        assert_eq!(
            recovery.reconcile_next_unresolved(&mut tracker, ack.obligation),
            TrackingRecoveryOutcome::Recovered { remaining }
        );
        assert_eq!(recovery.occupied(), remaining);
        assert_eq!(tracker.status(ack.obligation), Some(queued));
    }
    assert!(recovery.next_unresolved().is_none());
    assert_eq!(
        recovery.reconcile_next_unresolved(&mut tracker, ack.obligation),
        TrackingRecoveryOutcome::Empty
    );
    // Queue progress does not retire the required delivery obligation.
    assert_eq!(tracker.len(), 3);
}

#[test]
fn unresolved_reconciliation_preserves_regressing_outcome_and_pending_updates() {
    let mut recovery = TrackingRecovery::new(2).unwrap();
    let mut tracker = AcknowledgementTracker::new(1).unwrap();
    let mut ack = acknowledgement();
    tracker
        .register(DeliveryObligation {
            id: ack.obligation,
            destination: ack.destination,
            range: ack.range.clone(),
            kind: DestinationKind::Recording,
            policy: DeliveryPolicy::Required,
        })
        .unwrap();
    ack.status = DeliveryStatus::Progress(DeliveryStage::Queued);
    tracker.acknowledge(ack.clone()).unwrap();
    let reservation = recovery.reserve().unwrap();
    recovery
        .retain_unresolved(
            ack.destination,
            ack.range.clone(),
            DeliveryStatus::Pending,
            reservation,
        )
        .unwrap();
    let reservation = recovery.reserve().unwrap();
    recovery.retain(ack.clone(), reservation).unwrap();
    assert_eq!(
        recovery.reconcile_next_unresolved(&mut tracker, ack.obligation),
        TrackingRecoveryOutcome::RetryRequired {
            error: AcknowledgementError::ProgressRegression,
            remaining: 1,
        }
    );
    assert_eq!(recovery.occupied(), 2);
    assert_eq!(recovery.unresolved_len(), 1);
    assert_eq!(recovery.len(), 1);
    assert_eq!(tracker.status(ack.obligation), Some(ack.status));
    assert_eq!(
        recovery.retry_next(&mut tracker),
        TrackingRecoveryOutcome::Recovered { remaining: 0 }
    );
    assert_eq!(recovery.occupied(), 1);
    assert_eq!(
        recovery.next_unresolved().unwrap().status(),
        DeliveryStatus::Pending
    );
}
