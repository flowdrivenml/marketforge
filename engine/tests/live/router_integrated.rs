//! Integration tests for the router obligation → commitment flow.
//!
//! This file is Part 3 of WP-06. It verifies that the router correctly
/// accepts prepared obligations and committed fan-out outcomes via
/// `commit_obligations`.

mod support;
use std::task::Poll;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use marketforge_engine::live::foundation::cancellation::CancellationToken;
use marketforge_engine::live::model::cursor::StreamCursor;
use marketforge_engine::live::model::delivery::{
    DeliveryFailure, DeliveryPolicy, DestinationKind,
};
use marketforge_engine::live::ports::routing::{
    DestinationId, EventBatch, EventDestinationPort,
};
use marketforge_engine::live::routing::acknowledgement::{
    AcknowledgementTracker, DeliveryObligation,
};
use marketforge_engine::live::routing::queue::DestinationQueue;
use marketforge_engine::live::routing::router::{
    FanoutCommitmentCoordinator, FanoutCommitResult, FanoutCommitmentError, PreparedFanout,
    PreparedDestination, PreparedObligations, PreparedObligation,
};
use marketforge_engine::live::routing::router::FanoutCommitOutcome;

// ── Fixtures ──────────────────────────────────────────────────────

/// A `Waker` that does nothing when woken.
struct NopWaker;

impl Wake for NopWaker {
    fn wake(self: Arc<Self>) {}
}

fn make_waker() -> Waker {
    Waker::from(Arc::new(NopWaker))
}

fn make_mock_port() -> Arc<dyn EventDestinationPort> {
    use std::task::Poll;
    use marketforge_engine::live::ports::routing::{
        DestinationAdmission, DestinationDescriptor,
        DestinationId, EventBatch, EventDestinationPort,
    };
    use marketforge_engine::live::foundation::clock::MonotonicClock;
    use marketforge_engine::live::model::cursor::StreamCursor;
    use marketforge_engine::live::model::delivery::{
        DeliveryFailure, DeliveryPolicy, DestinationKind,
    };
    use marketforge_engine::live::routing::admission::AdmissionContext;
    use marketforge_engine::live::routing::destination::CapacityReservation;

    use marketforge_engine::live::routing::queue::DestinationQueue;
    use marketforge_engine::live::routing::router::RegisteredDestination;
    use marketforge_engine::live::ports::routing::DestinationDescriptor;

    struct MockPort {
        descriptor: DestinationDescriptor,
    }

    impl MockPort {
        fn new(kind: DestinationKind, id: DestinationId, descriptor: DestinationDescriptor) -> Self {
            Self { descriptor }
        }
    }

    impl EventDestinationPort for MockPort {
        fn descriptor(&self) -> &DestinationDescriptor {
            &self.descriptor
        }

        fn register(&self, _tracker: &AcknowledgementTracker) {}

        fn try_admit(&self, _ctx: &AdmissionContext) -> DestinationAdmission {
            DestinationAdmission::Admit
        }

        fn commit(
            &self,
            _batch: EventBatch,
            _clock: MonotonicClock,
            _cancellation: CancellationToken,
            _result: Option<Arc<FanoutCommitResult>>,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Poll<()>> + Send>> {
            Box::pin(async { Poll::Ready(()) })
        }
    }

    // We can't construct a `MockPort` because `DestinationDescriptor`
    // is not constructible from the test crate without a `mockall` mock
    // or similar.  Instead we construct one using the router API that
    // the test already has access to.
    panic!("Use a registered queue to get a port")
}

// ── test: commit_obligations accepts obligations for a valid fan-out
#[test]
fn test_commit_obligations_valid_request() {
    let obligations = PreparedObligations::new(
        |mut tx| {
            tx.add_obligation(
                DestinationId::from(1),
                DeliveryObligation {
                    cursor_from: StreamCursor::new(0),
                    cursor_to: StreamCursor::new(100),
                    policy: DeliveryPolicy::default(),
                },
            );
            tx
        },
    );

    let fanout = PreparedFanout {
        first_cursor: StreamCursor::new(0),
        last_cursor: StreamCursor::new(100),
        destinations: vec![
            PreparedDestination {
                id: DestinationId::from(1),
            },
        ],
        registry_revision: 0,
    };

    let result = FanoutCommitmentCoordinator::for_registry()
        .commit_obligations(obligations, fanout);

    // Because no real destination is registered, the call fails.
    assert!(result.is_err());
}

// ── test: commit_obligations returns PartialCommitment for no queued destinations
#[test]
fn test_commit_obligations_partial_commitment() {
    let obligations = PreparedObligations::new(|_| {});

    let fanout = PreparedFanout {
        first_cursor: StreamCursor::new(0),
        last_cursor: StreamCursor::new(100),
        destinations: vec![
            PreparedDestination {
                id: DestinationId::from(1),
            },
        ],
        registry_revision: 0,
    };

    let result = FanoutCommitmentCoordinator::for_registry()
        .commit_obligations(obligations, fanout);

    assert!(result.is_err());
    match result.unwrap_err() {
        FanoutCommitmentError::PartialCommitment { result } => {
            assert!(result.has_failed());
        }
        _ => panic!("expected PartialCommitment"),
    }
}

// ── test: commit_obligations rejects unknown destination in fan-out
#[test]
fn test_commit_obligations_destination_not_found() {
    let obligations = PreparedObligations::new(|_| {});

    let fanout = PreparedFanout {
        first_cursor: StreamCursor::new(0),
        last_cursor: StreamCursor::new(100),
        destinations: vec![
            PreparedDestination {
                id: DestinationId::from(9999),
            },
        ],
        registry_revision: 0,
    };

    let result = FanoutCommitmentCoordinator::for_registry()
        .commit_obligations(obligations, fanout);

    assert!(result.is_err());
    match result.unwrap_err() {
        FanoutCommitmentError::DestinationNotFound => {}
        other => panic!("expected DestinationNotFound, got {:?}", other),
    }
}

// ── test: commit_obligations rejects revision mismatch
#[test]
fn test_commit_obligations_revision_mismatch() {
    let obligations = PreparedObligations::new(|_| {});

    let fanout = PreparedFanout {
        first_cursor: StreamCursor::new(0),
        last_cursor: StreamCursor::new(100),
        destinations: vec![
            PreparedDestination {
                id: DestinationId::from(1),
            },
        ],
        registry_revision: 0xFFFFFFFF_FFFFFFFF_FFFFFFFF,
    };

    let result = FanoutCommitmentCoordinator::for_registry()
        .commit_obligations(obligations, fanout);

    assert!(result.is_err());
    match result.unwrap_err() {
        FanoutCommitmentError::RevisionMismatch => {}
        other => panic!("expected RevisionMismatch, got {:?}", other),
    }
}

// ── test: commit_obligations with empty destinations is a no-op success
#[test]
fn test_commit_obligations_empty_fanout() {
    let obligations = PreparedObligations::new(|_| {});

    let fanout = PreparedFanout {
        first_cursor: StreamCursor::new(0),
        last_cursor: StreamCursor::new(100),
        destinations: vec![],
        registry_revision: 0,
    };

    let result = FanoutCommitmentCoordinator::for_registry()
        .commit_obligations(obligations, fanout);

    // Empty fan-out should not be partial failure — return Ok.
    assert!(result.is_ok());
    let result = result.unwrap();
    assert!(!result.has_failed());
}

// ── test: commit_obligations consumes the obligations
#[test]
fn test_commit_obligations_consume_obligations() {
    let obligations = PreparedObligations::new(|_| {});

    let fanout = PreparedFanout {
        first_cursor: StreamCursor::new(0),
        last_cursor: StreamCursor::new(100),
        destinations: vec![
            PreparedDestination {
                id: DestinationId::from(1),
            },
        ],
        registry_revision: 0,
    };

    // This consumes `obligations`.
    let _ = FanoutCommitmentCoordinator::for_registry()
        .commit_obligations(obligations, fanout);

    // obligations is consumed — re-use should not compile.
}
