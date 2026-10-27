use std::time::Duration;

use marketforge_engine::live::{
    infrastructure::rest::ExchangeRestResponse,
    recovery::{
        RecoveryAttemptId, RecoveryCoordinator, RecoveryCoordinatorConfig, RecoveryExecutionError,
        RecoveryHandle, RecoveryHandleError, RecoverySchedulerLimits, RecoverySourceEpoch,
        StreamRecoveryBinding, StreamRecoveryError, StreamRecoveryHandle,
    },
};

use super::model::depth_request;

fn coordinator() -> (RecoveryCoordinator, RecoveryHandle) {
    RecoveryCoordinator::new(RecoveryCoordinatorConfig {
        command_capacity: 32,
        scheduler: RecoverySchedulerLimits {
            max_requests: 100,
            max_jobs: 100,
            max_concurrent_jobs: 2,
        },
    })
    .unwrap()
}

fn bound_stream(
    manager: RecoveryHandle,
    stream_id: &str,
    stream_generation: u64,
    connection_generation: u64,
    subscription_generation: u64,
) -> StreamRecoveryHandle {
    let source = depth_request(stream_id, stream_generation, 1, Some(100)).source;

    let binding = StreamRecoveryBinding::new(
        stream_id,
        source,
        1,
        RecoverySourceEpoch {
            stream_generation,
            connection_generation,
            subscription_generation,
        },
    )
    .unwrap();

    StreamRecoveryHandle::new(manager, binding)
}

fn assert_cancelled(
    result: &Result<std::sync::Arc<ExchangeRestResponse>, std::sync::Arc<RecoveryExecutionError>>,
) {
    assert!(matches!(
        result,
        Err(error)
            if matches!(
                error.as_ref(),
                RecoveryExecutionError::Cancelled
            )
    ));
}

// ============================================================
// TEST 1 — CROSS-STREAM CANCELLATION IS REJECTED
// ============================================================

#[tokio::test]
async fn facade_cannot_cancel_another_streams_request() {
    let (manager, shared) = coordinator();

    let stream_a = bound_stream(shared.clone(), "stream-a", 10, 1, 1);

    let stream_b = bound_stream(shared, "stream-b", 20, 1, 1);

    let manager_future = manager.run(|_job| async {
        std::future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
    });

    let client = async move {
        let ticket = stream_a
            .request_depth_snapshot(RecoveryAttemptId(7), Some(100))
            .await
            .unwrap();

        // Stream B attempts to cancel stream A's request.
        let result = stream_b
            .cancel(RecoveryAttemptId(7), ticket.request_id)
            .await;

        assert!(matches!(
            result,
            Err(StreamRecoveryError::Manager(
                RecoveryHandleError::RequestNotOwned
            ))
        ));

        // The legitimate owner can still cancel it.
        stream_a
            .cancel(RecoveryAttemptId(7), ticket.request_id)
            .await
            .unwrap();

        let completion = ticket.completion.await.unwrap();

        assert_eq!(completion.requester.stream_id, "stream-a",);

        assert_cancelled(&completion.result);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, client);
    })
    .await
    .expect("cross-stream cancellation test timed out");
}

// ============================================================
// TEST 2 — WRONG ATTEMPT CANNOT CANCEL REQUEST
// ============================================================

#[tokio::test]
async fn incorrect_attempt_identity_cannot_cancel_request() {
    let (manager, shared) = coordinator();

    let stream = bound_stream(shared, "stream-a", 10, 1, 1);

    let manager_future = manager.run(|_job| async {
        std::future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
    });

    let client = async move {
        let ticket = stream
            .request_depth_snapshot(RecoveryAttemptId(3), Some(100))
            .await
            .unwrap();

        let invalid = stream.cancel(RecoveryAttemptId(4), ticket.request_id).await;

        assert!(matches!(
            invalid,
            Err(StreamRecoveryError::Manager(
                RecoveryHandleError::RequestNotOwned
            ))
        ));

        stream
            .cancel(RecoveryAttemptId(3), ticket.request_id)
            .await
            .unwrap();

        let completion = ticket.completion.await.unwrap();

        assert_eq!(completion.requester.attempt, RecoveryAttemptId(3),);

        assert_cancelled(&completion.result);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, client);
    })
    .await
    .expect("attempt ownership test timed out");
}

// ============================================================
// TEST 3 — RECONNECT EPOCHS ARE INDEPENDENT
// ============================================================

#[tokio::test]
async fn cancelling_old_connection_preserves_new_connection() {
    let (manager, shared) = coordinator();

    // Same stream generation, different connection generations.
    let old = bound_stream(shared.clone(), "stream-a", 10, 1, 1);

    let current = bound_stream(shared, "stream-a", 10, 2, 1);

    let manager_future = manager.run(|_job| async {
        std::future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
    });

    let client = async move {
        let old_ticket = old
            .request_depth_snapshot(RecoveryAttemptId(1), Some(100))
            .await
            .unwrap();

        let current_ticket = current
            .request_depth_snapshot(RecoveryAttemptId(2), Some(100))
            .await
            .unwrap();

        // They may share an acquisition, but they
        // never share requester identity.
        let cancelled = old.cancel_epoch().await.unwrap();

        assert_eq!(cancelled.len(), 1);

        assert_eq!(cancelled[0].cancellation.request_id, old_ticket.request_id,);

        let old_completion = old_ticket.completion.await.unwrap();

        assert_eq!(old_completion.requester.epoch.connection_generation, 1,);

        assert_cancelled(&old_completion.result);

        // The current connection retains ownership.
        current
            .cancel(RecoveryAttemptId(2), current_ticket.request_id)
            .await
            .unwrap();

        let current_completion = current_ticket.completion.await.unwrap();

        assert_eq!(current_completion.requester.epoch.connection_generation, 2,);

        assert_cancelled(&current_completion.result);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, client);
    })
    .await
    .expect("connection epoch cancellation test timed out");
}

// ============================================================
// TEST 4 — SUBSCRIPTION EPOCHS ARE INDEPENDENT
// ============================================================

#[tokio::test]
async fn cancelling_old_subscription_preserves_new_subscription() {
    let (manager, shared) = coordinator();

    // Same stream and connection, different subscriptions.
    let old = bound_stream(shared.clone(), "stream-a", 10, 5, 1);

    let current = bound_stream(shared, "stream-a", 10, 5, 2);

    let manager_future = manager.run(|_job| async {
        std::future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
    });

    let client = async move {
        let old_ticket = old
            .request_depth_snapshot(RecoveryAttemptId(1), Some(100))
            .await
            .unwrap();

        let current_ticket = current
            .request_depth_snapshot(RecoveryAttemptId(2), Some(100))
            .await
            .unwrap();

        let cancelled = old.cancel_epoch().await.unwrap();

        assert_eq!(cancelled.len(), 1);

        let old_completion = old_ticket.completion.await.unwrap();

        assert_eq!(old_completion.requester.epoch.subscription_generation, 1,);

        assert_cancelled(&old_completion.result);

        current
            .cancel(RecoveryAttemptId(2), current_ticket.request_id)
            .await
            .unwrap();

        let current_completion = current_ticket.completion.await.unwrap();

        assert_eq!(
            current_completion.requester.epoch.subscription_generation,
            2,
        );

        assert_cancelled(&current_completion.result);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, client);
    })
    .await
    .expect("subscription epoch cancellation test timed out");
}
