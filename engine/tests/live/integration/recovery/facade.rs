use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use marketforge_engine::live::{
    infrastructure::rest::ExchangeRestResponse,
    recovery::{
        RecoveryAttemptId, RecoveryCoordinator, RecoveryCoordinatorConfig, RecoveryExecutionError,
        RecoveryHandle, RecoverySchedulerLimits, RecoverySourceEpoch, StreamRecoveryBinding,
        StreamRecoveryHandle,
    },
};

use super::model::depth_request;

// ============================================================
// FIXTURES
// ============================================================

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

fn bound_stream(manager: RecoveryHandle, stream_id: &str, generation: u64) -> StreamRecoveryHandle {
    // Reuse the already-tested source fixture.
    let source = depth_request(stream_id, generation, 1, Some(100)).source;

    let binding = StreamRecoveryBinding::new(
        stream_id,
        source,
        1,
        RecoverySourceEpoch {
            stream_generation: generation,
            connection_generation: 1,
            subscription_generation: 1,
        },
    )
    .unwrap();

    StreamRecoveryHandle::new(manager, binding)
}

fn response() -> ExchangeRestResponse {
    ExchangeRestResponse::new(
        "binance_spot",
        "depth_snapshot",
        200,
        serde_json::json!({
            "lastUpdateId": 123456,
            "bids": [],
            "asks": []
        }),
        Default::default(),
        Duration::from_millis(1),
        64,
    )
}

// ============================================================
// TEST 1 — FIXED IDENTITY
// ============================================================

#[tokio::test]
async fn facade_preserves_bound_identity() {
    let (manager, shared) = coordinator();

    let stream = bound_stream(shared, "depth-stream-a", 42);

    let manager_future = manager
        .run(|_job| async { Ok::<ExchangeRestResponse, RecoveryExecutionError>(response()) });

    let client = async move {
        let ticket = stream
            .request_depth_snapshot(RecoveryAttemptId(7), Some(100))
            .await
            .unwrap();

        let completion = ticket.completion.await.unwrap();

        assert_eq!(completion.requester.stream_id, "depth-stream-a",);

        assert_eq!(completion.requester.epoch.stream_generation, 42,);

        assert_eq!(completion.requester.epoch.connection_generation, 1,);

        assert_eq!(completion.requester.attempt, RecoveryAttemptId(7),);

        assert!(completion.result.is_ok());
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, client);
    })
    .await
    .expect("stream-bound recovery timed out");
}

// ============================================================
// TEST 2 — TWO FACADES, ONE SHARED ACQUISITION
// ============================================================

#[tokio::test]
async fn distinct_facades_share_the_same_manager_job() {
    let (manager, shared) = coordinator();

    let stream_a = bound_stream(shared.clone(), "stream-a", 10);

    let stream_b = bound_stream(shared, "stream-b", 20);

    let executions = Arc::new(AtomicUsize::new(0));
    let counter = executions.clone();

    let manager_future = manager.run(move |_job| {
        let counter = counter.clone();

        async move {
            counter.fetch_add(1, Ordering::SeqCst);

            Ok::<ExchangeRestResponse, RecoveryExecutionError>(response())
        }
    });

    let client = async move {
        let (first, second) = tokio::join!(
            stream_a.request_depth_snapshot(RecoveryAttemptId(1), Some(100),),
            stream_b.request_depth_snapshot(RecoveryAttemptId(3), Some(100),),
        );

        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(first.job_id, second.job_id);

        let a = first.completion.await.unwrap();
        let b = second.completion.await.unwrap();

        assert_eq!(a.requester.stream_id, "stream-a");
        assert_eq!(b.requester.stream_id, "stream-b");

        let a_response = a.result.unwrap();
        let b_response = b.result.unwrap();

        assert!(Arc::ptr_eq(&a_response, &b_response,));
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, client);
    })
    .await
    .expect("shared facade acquisition timed out");

    assert_eq!(executions.load(Ordering::SeqCst), 1,);
}

// ============================================================
// TEST 3 — FACADES CANNOT ADMIT INVALID OPERATIONS
// ============================================================

#[tokio::test]
async fn facade_rejects_zero_snapshot_depth() {
    let (_manager, shared) = coordinator();

    let stream = bound_stream(shared, "depth-stream", 10);

    let result = stream
        .request_depth_snapshot(RecoveryAttemptId(1), Some(0))
        .await;

    assert!(result.is_err());
}

// ============================================================
// TEST 4 — GENERATION CANCELLATION
// ============================================================

#[tokio::test]
async fn facade_cancels_its_bound_stream_generation() {
    let (manager, shared) = coordinator();

    let old_stream = bound_stream(shared.clone(), "depth-stream", 10);

    let new_stream = bound_stream(shared, "depth-stream", 11);

    let manager_future = manager.run(|_job| async {
        std::future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
    });

    let client = async move {
        let old = old_stream
            .request_depth_snapshot(RecoveryAttemptId(1), Some(100))
            .await
            .unwrap();

        let new = new_stream
            .request_depth_snapshot(RecoveryAttemptId(2), Some(100))
            .await
            .unwrap();

        let cancelled = old_stream.cancel_epoch().await.unwrap();

        assert_eq!(cancelled.len(), 1);

        let old_completion = old.completion.await.unwrap();

        assert_eq!(old_completion.requester.epoch.stream_generation, 10,);

        assert!(matches!(
            old_completion.result,
            Err(error)
                if matches!(
                    error.as_ref(),
                    RecoveryExecutionError::Cancelled
                )
        ));

        // New-generation work remains registered.
        new_stream
            .cancel(RecoveryAttemptId(2), new.request_id)
            .await
            .unwrap();

        let new_completion = new.completion.await.unwrap();

        assert_eq!(new_completion.requester.epoch.stream_generation, 11,);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, client);
    })
    .await
    .expect("stream generation cancellation timed out");
}
