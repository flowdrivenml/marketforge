use std::{sync::Arc, time::Duration};

use marketforge_engine::live::{
    infrastructure::rest::ExchangeRestResponse,
    recovery::{
        RecoveryCompletion, RecoveryCoordinator, RecoveryCoordinatorConfig, RecoveryExecutionError,
        RecoveryHandle, RecoverySchedulerLimits,
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
            max_concurrent_jobs: 4,
        },
    })
    .unwrap()
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

fn assert_identity(
    completion: &RecoveryCompletion,
    stream_id: &str,
    generation: u64,
    attempt: u64,
) {
    assert_eq!(completion.requester.stream_id, stream_id);

    assert_eq!(completion.requester.epoch.stream_generation, generation,);

    assert_eq!(completion.requester.attempt.0, attempt,);
}

// ============================================================
// TEST 1 — COALESCED RESULTS KEEP INDEPENDENT IDENTITIES
// ============================================================

#[tokio::test]
async fn shared_http_result_preserves_requester_identity() {
    let (manager, handle) = coordinator();

    let manager_future = manager
        .run(|_job| async { Ok::<ExchangeRestResponse, RecoveryExecutionError>(response()) });

    let clients = async move {
        let (first, second) = tokio::join!(
            handle.request(depth_request("stream-a", 10, 1, Some(1000),)),
            handle.request(depth_request("stream-b", 20, 3, Some(1000),)),
        );

        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(first.job_id, second.job_id);

        let first_id = first.request_id;
        let second_id = second.request_id;

        let first_completion = first.completion.await.unwrap();

        let second_completion = second.completion.await.unwrap();

        assert_eq!(first_completion.request_id, first_id,);

        assert_eq!(second_completion.request_id, second_id,);

        assert_identity(&first_completion, "stream-a", 10, 1);

        assert_identity(&second_completion, "stream-b", 20, 3);

        let first_response = first_completion.result.unwrap();

        let second_response = second_completion.result.unwrap();

        assert!(Arc::ptr_eq(&first_response, &second_response,));
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("coalesced completion test timed out");
}

// ============================================================
// TEST 2 — CANCELLATION RETAINS ORIGINAL IDENTITY
// ============================================================

#[tokio::test]
async fn cancelled_completion_identifies_original_requester() {
    let (manager, handle) = coordinator();

    let manager_future = manager.run(|_job| async {
        std::future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
    });

    let clients = async move {
        let ticket = handle
            .request(depth_request("cancelled-stream", 31, 7, Some(1000)))
            .await
            .unwrap();

        let request_id = ticket.request_id;

        handle.cancel(request_id).await.unwrap();

        let completion = ticket.completion.await.unwrap();

        assert_eq!(completion.request_id, request_id);

        assert_identity(&completion, "cancelled-stream", 31, 7);

        assert!(matches!(
            completion.result,
            Err(error)
                if matches!(
                    error.as_ref(),
                    RecoveryExecutionError::Cancelled
                )
        ));
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("cancellation completion test timed out");
}

// ============================================================
// TEST 3 — GENERATION CANCELLATION PRESERVES FENCING
// ============================================================

#[tokio::test]
async fn generation_cancellation_preserves_request_identity() {
    let (manager, handle) = coordinator();

    let manager_future = manager.run(|_job| async {
        std::future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
    });

    let clients = async move {
        let old = handle
            .request(depth_request("stream-a", 10, 1, Some(1000)))
            .await
            .unwrap();

        let next = handle
            .request(depth_request("stream-a", 11, 2, Some(1000)))
            .await
            .unwrap();

        handle.cancel_generation("stream-a", 10).await.unwrap();

        let old_completion = old.completion.await.unwrap();

        assert_identity(&old_completion, "stream-a", 10, 1);

        assert!(matches!(
            old_completion.result,
            Err(error)
                if matches!(
                    error.as_ref(),
                    RecoveryExecutionError::Cancelled
                )
        ));

        // The replacement generation remains registered.
        let replacement_id = next.request_id;

        handle.cancel(replacement_id).await.unwrap();

        let replacement_completion = next.completion.await.unwrap();

        assert_identity(&replacement_completion, "stream-a", 11, 2);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("generation completion test timed out");
}
