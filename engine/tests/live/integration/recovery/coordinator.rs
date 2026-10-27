use marketforge_engine::live::recovery::RecoveryExecutionError;
use marketforge_engine::live::{
    infrastructure::rest::ExchangeRestResponse,
    recovery::{RecoveryCoordinator, RecoveryCoordinatorConfig, RecoverySchedulerLimits},
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::model::depth_request;

// ============================================================
// FIXTURES
// ============================================================

fn coordinator() -> (
    RecoveryCoordinator,
    marketforge_engine::live::recovery::RecoveryHandle,
) {
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
            "lastUpdateId": 1000,
            "bids": [],
            "asks": [],
        }),
        Default::default(),
        Duration::from_millis(1),
        64,
    )
}

// ============================================================
// SINGLE-FLIGHT EXECUTION
// ============================================================

#[tokio::test]
async fn coordinator_shares_one_acquisition_across_streams() {
    let (manager, handle) = coordinator();

    let executions = Arc::new(AtomicUsize::new(0));

    let counter = executions.clone();

    let manager_future = manager.run(move |_job| {
        let counter = counter.clone();

        async move {
            counter.fetch_add(1, Ordering::SeqCst);

            Ok(response())
        }
    });

    let clients = async move {
        let a = handle.clone();
        let b = handle.clone();

        let (first, second) = tokio::join!(
            a.request(depth_request("stream-a", 10, 1, Some(1000),)),
            b.request(depth_request("stream-b", 20, 1, Some(1000),)),
        );

        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(first.job_id, second.job_id);

        let first_result = first.completion.await.unwrap().result.unwrap();
        let second_result = second.completion.await.unwrap().result.unwrap();

        assert_eq!(first_result.body()["lastUpdateId"], 1000,);

        assert_eq!(second_result.body()["lastUpdateId"], 1000,);

        assert!(Arc::ptr_eq(&first_result, &second_result,));
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("recovery coordinator did not terminate");

    assert_eq!(executions.load(Ordering::SeqCst), 1,);
}

// ============================================================
// CANCELLATION
// ============================================================

#[tokio::test]
async fn cancelling_running_request_terminates_acquisition() {
    let (manager, handle) = coordinator();

    let executions = Arc::new(AtomicUsize::new(0));
    let counter = executions.clone();

    let manager_future = manager.run(move |_job| {
        let counter = counter.clone();

        async move {
            counter.fetch_add(1, Ordering::SeqCst);

            // Simulate an acquisition that never completes.
            std::future::pending::<
                Result<
                    ExchangeRestResponse,
                    marketforge_engine::live::recovery::RecoveryExecutionError,
                >,
            >()
            .await
        }
    });

    let clients = async move {
        let ticket = handle
            .request(depth_request("stream-a", 10, 1, Some(1000)))
            .await
            .unwrap();

        let cancellation = handle.cancel(ticket.request_id).await.unwrap();

        assert_eq!(cancellation.abort_job, Some(ticket.job_id),);

        // Cancellation closes the requester's completion
        // channel without delivering a stale REST result.
        let completion = ticket.completion.await.unwrap();

        assert_eq!(completion.request_id, ticket.request_id);
        assert_eq!(completion.job_id, ticket.job_id);

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
    .expect("cancelled recovery operation did not terminate");

    assert_eq!(executions.load(Ordering::SeqCst), 1,);
}
