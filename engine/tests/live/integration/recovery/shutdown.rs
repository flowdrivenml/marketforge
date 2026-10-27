use std::{future, sync::Arc, time::Duration};

use marketforge_engine::live::{
    infrastructure::rest::ExchangeRestResponse,
    recovery::{
        RecoveryCoordinator, RecoveryCoordinatorConfig, RecoveryExecutionError, RecoveryHandle,
        RecoverySchedulerLimits,
    },
};

use tokio::sync::Notify;

use super::model::depth_request;

// ============================================================
// FIXTURE
// ============================================================

fn coordinator(concurrency: usize) -> (RecoveryCoordinator, RecoveryHandle) {
    RecoveryCoordinator::new(RecoveryCoordinatorConfig {
        command_capacity: 32,

        scheduler: RecoverySchedulerLimits {
            max_requests: 100,
            max_jobs: 100,
            max_concurrent_jobs: concurrency,
        },
    })
    .unwrap()
}

fn assert_shutdown_error(result: Result<Arc<ExchangeRestResponse>, Arc<RecoveryExecutionError>>) {
    assert!(matches!(
        result,
        Err(error)
            if matches!(
                error.as_ref(),
                RecoveryExecutionError::ManagerShutdown
            )
    ));
}

// ============================================================
// TEST 1 — RUNNING ACQUISITION
// ============================================================

#[tokio::test]
async fn shutdown_cancels_running_acquisition() {
    let (manager, handle) = coordinator(1);

    let started = Arc::new(Notify::new());
    let executor_started = started.clone();

    let manager_future = manager.run(move |_job| {
        let started = executor_started.clone();

        async move {
            started.notify_one();

            future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
        }
    });

    let clients = async move {
        let ticket = handle
            .request(depth_request("stream-a", 10, 1, Some(100)))
            .await
            .unwrap();

        // Ensure the acquisition has started before
        // requesting manager shutdown.
        started.notified().await;

        handle.shutdown().await.unwrap();

        assert_shutdown_error(ticket.completion.await.unwrap().result);

        // The handle still exists here, but the manager
        // must already be able to terminate.
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("shutdown did not terminate running acquisition");
}

// ============================================================
// TEST 2 — QUEUED AND RUNNING JOBS
// ============================================================

#[tokio::test]
async fn shutdown_notifies_queued_and_running_requesters() {
    let (manager, handle) = coordinator(1);

    let started = Arc::new(Notify::new());
    let executor_started = started.clone();

    let manager_future = manager.run(move |_job| {
        let started = executor_started.clone();

        async move {
            started.notify_one();

            future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
        }
    });

    let clients = async move {
        let running = handle
            .request(depth_request("stream-running", 10, 1, Some(100)))
            .await
            .unwrap();

        started.notified().await;

        // Different acquisition parameters create
        // a new job, which must remain queued because
        // concurrency is limited to one.
        let queued = handle
            .request(depth_request("stream-queued", 20, 1, Some(500)))
            .await
            .unwrap();

        assert_ne!(running.job_id, queued.job_id);

        handle.shutdown().await.unwrap();

        assert_shutdown_error(running.completion.await.unwrap().result);

        assert_shutdown_error(queued.completion.await.unwrap().result);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("shutdown failed to clean up pending work");
}

// ============================================================
// TEST 3 — COALESCED REQUESTERS
// ============================================================

#[tokio::test]
async fn shutdown_notifies_every_shared_requester() {
    let (manager, handle) = coordinator(1);

    let started = Arc::new(Notify::new());
    let executor_started = started.clone();

    let manager_future = manager.run(move |_job| {
        let started = executor_started.clone();

        async move {
            started.notify_one();

            future::pending::<Result<ExchangeRestResponse, RecoveryExecutionError>>().await
        }
    });

    let clients = async move {
        let (a, b) = tokio::join!(
            handle.request(depth_request("stream-a", 10, 1, Some(100),)),
            handle.request(depth_request("stream-b", 20, 1, Some(100),)),
        );

        let a = a.unwrap();
        let b = b.unwrap();

        assert_eq!(a.job_id, b.job_id);

        started.notified().await;

        handle.shutdown().await.unwrap();

        assert_shutdown_error(a.completion.await.unwrap().result);

        assert_shutdown_error(b.completion.await.unwrap().result);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("shared requesters were not notified");
}

// ============================================================
// TEST 4 — POST-SHUTDOWN REQUESTS
// ============================================================

#[tokio::test]
async fn shutdown_closes_manager_to_new_requests() {
    let (manager, handle) = coordinator(1);

    let manager_future = manager.run(|_job| async {
        unreachable!("no acquisition should execute");

        #[allow(unreachable_code)]
        Err::<ExchangeRestResponse, RecoveryExecutionError>(RecoveryExecutionError::ManagerShutdown)
    });

    let clients = async move {
        let remaining_handle = handle.clone();

        handle.shutdown().await.unwrap();

        // After shutdown acknowledgement, the
        // coordinator has closed its event loop.
        //
        // The new request must fail rather than wait
        // indefinitely for an acquisition.
        let result = remaining_handle
            .request(depth_request("stream-after-shutdown", 10, 1, Some(100)))
            .await;

        assert!(result.is_err());
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("manager did not stop accepting requests");
}
