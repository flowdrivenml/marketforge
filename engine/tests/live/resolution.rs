use marketforge_engine::live::routing::{
    acknowledgement::{
        DeliveryCursorRange, DeliveryObligation, DeliveryObligationId, EscalationResolution,
        EscalationResolutionRecord, EscalationResolutionToken, FinalizationEscalation,
        FinalizationJournalId, FinalizationRecord, ObligationFinalization,
    },
    resolution::{BoundedResolutionSink, ResolutionAcceptanceError, ResolutionAcceptanceSink},
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

fn resolution(token: u64) -> (EscalationResolutionToken, EscalationResolutionRecord) {
    let stream = StreamId("resolution-source".to_owned());

    let obligation = DeliveryObligation {
        id: DeliveryObligationId(token),
        destination: DestinationId(7),
        kind: DestinationKind::Recording,
        policy: DeliveryPolicy::Required,
        range: DeliveryCursorRange::new(
            StreamCursor::new(stream.clone(), LogEpoch(1), token),
            StreamCursor::new(stream, LogEpoch(1), token),
        )
        .unwrap(),
    };

    (
        EscalationResolutionToken {
            journal: FinalizationJournalId(1),
            sequence: token,
        },
        EscalationResolutionRecord {
            record: FinalizationRecord {
                outcome: ObligationFinalization::Succeeded {
                    id: obligation.id,
                    destination: obligation.destination,
                },
                obligation,
            },
            escalation: FinalizationEscalation::PermanentReportingFailure,
            resolution: EscalationResolution::ReportingVerified,
        },
    )
}

#[test]
fn sink_accepts_and_retains_resolution() {
    let sink = BoundedResolutionSink::new(2).unwrap();
    let (token, record) = resolution(1);

    let receipt = sink.accept(token, &record).unwrap();

    assert!(sink.verify(&receipt));
    assert_eq!(sink.len(), 1);
}

#[test]
fn duplicate_acceptance_is_idempotent() {
    let sink = BoundedResolutionSink::new(2).unwrap();
    let (token, record) = resolution(1);

    let first = sink.accept(token, &record).unwrap();
    let second = sink.accept(token, &record).unwrap();

    assert_eq!(first, second);
    assert_eq!(sink.len(), 1);
}

#[test]
fn sink_enforces_capacity() {
    let sink = BoundedResolutionSink::new(1).unwrap();

    let (first_token, first_record) = resolution(1);
    let (second_token, second_record) = resolution(2);

    sink.accept(first_token, &first_record).unwrap();

    assert_eq!(
        sink.accept(second_token, &second_record),
        Err(ResolutionAcceptanceError::CapacityExceeded)
    );
}

#[test]
fn sink_rejects_conflicting_acceptance() {
    let sink = BoundedResolutionSink::new(2).unwrap();
    let (token, record) = resolution(1);

    sink.accept(token, &record).unwrap();

    let (_, different_record) = resolution(2);

    assert_eq!(
        sink.accept(token, &different_record),
        Err(ResolutionAcceptanceError::ConflictingAcceptance)
    );
}

#[test]
fn receipt_cannot_be_verified_by_another_sink() {
    let first = BoundedResolutionSink::new(2).unwrap();
    let second = BoundedResolutionSink::new(2).unwrap();

    let (token, record) = resolution(1);

    let receipt = first.accept(token, &record).unwrap();

    assert!(first.verify(&receipt));
    assert!(!second.verify(&receipt));
}

#[test]
fn closed_sink_rejects_new_records() {
    let sink = BoundedResolutionSink::new(2).unwrap();
    sink.close();

    let (token, record) = resolution(1);

    assert_eq!(
        sink.accept(token, &record),
        Err(ResolutionAcceptanceError::Unavailable)
    );
}
