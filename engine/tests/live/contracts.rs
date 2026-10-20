//! WP-06 — Delivery and routing contract tests.
//!
//! These tests verify domain semantics and batch validation.
//! They do not assert that a router, recorder, publisher, or
//! retention backend has been implemented.

use marketforge_engine::live::{
    model::{
        cursor::{LogEpoch, StreamCursor},
        delivery::{
            DeliveryCapacity, DeliveryCapacityError, DeliveryFailure, DeliveryPolicy,
            DeliveryStage, DeliveryStatus, DestinationDeliveryResult, DestinationKind,
        },
        identity::StreamId,
    },
    ports::routing::{DestinationAdmission, DestinationCompletion, DestinationId},
};

fn cursor(stream: &str, epoch: u64, offset: u64) -> StreamCursor {
    StreamCursor::new(StreamId(stream.to_owned()), LogEpoch(epoch), offset)
}

#[test]
fn delivery_policies_are_distinct() {
    assert_ne!(DeliveryPolicy::BestEffort, DeliveryPolicy::Required);
    assert_ne!(DeliveryPolicy::Required, DeliveryPolicy::Durable);
    assert_ne!(DeliveryPolicy::BestEffort, DeliveryPolicy::Durable);
}

#[test]
fn required_failure_is_reported_as_violation() {
    let result = DestinationDeliveryResult::failed(
        cursor("source-1", 1, 10),
        DestinationKind::Recording,
        DeliveryPolicy::Required,
        DeliveryFailure::Unavailable,
    );

    assert!(result.is_required_violation());
}

#[test]
fn durable_failure_is_reported_as_violation() {
    let result = DestinationDeliveryResult::failed(
        cursor("source-1", 1, 10),
        DestinationKind::Retention,
        DeliveryPolicy::Durable,
        DeliveryFailure::DurabilityUnavailable,
    );

    assert!(result.is_required_violation());
}

#[test]
fn best_effort_failure_is_not_a_required_violation() {
    let result = DestinationDeliveryResult::failed(
        cursor("source-1", 1, 10),
        DestinationKind::Publication,
        DeliveryPolicy::BestEffort,
        DeliveryFailure::Disconnected,
    );

    assert!(!result.is_required_violation());

    assert_eq!(
        result.status,
        DeliveryStatus::Failed(DeliveryFailure::Disconnected)
    );
}

#[test]
fn queued_does_not_mean_committed() {
    assert_ne!(
        DeliveryStatus::Progress(DeliveryStage::Queued),
        DeliveryStatus::Progress(DeliveryStage::Committed),
    );
}

#[test]
fn committed_does_not_mean_durable() {
    assert_ne!(
        DeliveryStatus::Progress(DeliveryStage::Committed),
        DeliveryStatus::Progress(DeliveryStage::Durable),
    );
}

#[test]
fn delivery_capacity_rejects_zero_event_limit() {
    assert_eq!(
        DeliveryCapacity::new(0, 1024),
        Err(DeliveryCapacityError::ZeroEventLimit)
    );
}

#[test]
fn delivery_capacity_rejects_zero_byte_limit() {
    assert_eq!(
        DeliveryCapacity::new(100, 0),
        Err(DeliveryCapacityError::ZeroByteLimit)
    );
}

#[test]
fn delivery_capacity_accepts_positive_limits() {
    let capacity = DeliveryCapacity::new(100, 4096).unwrap();

    assert_eq!(capacity.max_pending_events, 100);
    assert_eq!(capacity.max_pending_bytes, 4096);
}

#[test]
fn independent_destinations_can_report_different_outcomes() {
    let event_cursor = cursor("source-1", 1, 42);

    let publication = DestinationDeliveryResult::progress(
        event_cursor.clone(),
        DestinationKind::Publication,
        DeliveryPolicy::BestEffort,
        DeliveryStage::Delivered,
    );

    let recording = DestinationDeliveryResult::failed(
        event_cursor.clone(),
        DestinationKind::Recording,
        DeliveryPolicy::Required,
        DeliveryFailure::PersistenceFailed,
    );

    let retention = DestinationDeliveryResult::progress(
        event_cursor.clone(),
        DestinationKind::Retention,
        DeliveryPolicy::Durable,
        DeliveryStage::Queued,
    );

    assert_eq!(publication.cursor, recording.cursor);
    assert_eq!(recording.cursor, retention.cursor);

    assert!(!publication.is_required_violation());
    assert!(recording.is_required_violation());
    assert!(!retention.is_required_violation());

    assert_eq!(
        retention.status,
        DeliveryStatus::Progress(DeliveryStage::Queued)
    );
}

#[test]
fn destination_admission_preserves_cursor_identity() {
    let expected = cursor("source-1", 3, 99);

    let admission = DestinationAdmission::Accepted {
        destination: DestinationId(7),
        through: expected.clone(),
    };

    match admission {
        DestinationAdmission::Accepted {
            destination,
            through,
        } => {
            assert_eq!(destination, DestinationId(7));
            assert_eq!(through, expected);
        }

        DestinationAdmission::Rejected { .. } => {
            panic!("unexpected admission rejection");
        }
    }
}

#[test]
fn destination_completion_preserves_original_cursor() {
    let expected = cursor("source-1", 2, 15);

    let completion = DestinationCompletion {
        destination: DestinationId(11),
        through: expected.clone(),
        status: DeliveryStatus::Progress(DeliveryStage::Committed),
    };

    assert_eq!(completion.through, expected);
    assert_eq!(
        completion.status,
        DeliveryStatus::Progress(DeliveryStage::Committed)
    );
}

#[test]
fn delivery_status_serialization_round_trip() {
    let original = DeliveryStatus::Failed(DeliveryFailure::CommitOutcomeUnknown);

    let encoded = serde_json::to_string(&original).unwrap();

    let decoded: DeliveryStatus = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded, original);
}

#[test]
fn cursor_serialization_preserves_namespace() {
    let original = cursor("source-1", 5, 1234);

    let encoded = serde_json::to_string(&original).unwrap();

    let decoded: StreamCursor = serde_json::from_str(&encoded).unwrap();

    assert_eq!(decoded, original);
}
