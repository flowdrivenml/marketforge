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
        RecoveryCoordinator, RecoveryCoordinatorConfig, RecoveryExecutionError, RecoveryHandle,
        RecoveryRestExecutor, RecoverySchedulerLimits,
    },
};

use tokio::sync::Notify;

use super::{
    http_server::{MockExchange, MockResponse},
    model::depth_request,
    rest_support::{build_limiters, build_service, load_profile},
};

// ============================================================
// CONFIGURATION
// ============================================================

const PROFILE: &str = "binance_spot";

fn coordinator(concurrency: usize) -> (RecoveryCoordinator, RecoveryHandle) {
    RecoveryCoordinator::new(RecoveryCoordinatorConfig {
        command_capacity: 64,

        scheduler: RecoverySchedulerLimits {
            max_requests: 128,
            max_jobs: 64,
            max_concurrent_jobs: concurrency,
        },
    })
    .unwrap()
}

fn snapshot_response() -> MockResponse {
    MockResponse::ok(
        r#"{
            "lastUpdateId": 123456,
            "bids": [["80000", "2"]],
            "asks": [["80001", "3"]]
        }"#,
    )
}

fn synthetic_response() -> ExchangeRestResponse {
    ExchangeRestResponse::new(
        PROFILE,
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
// TEST 1 — TWO STREAMS, ONE REAL HTTP REQUEST
// ============================================================

#[tokio::test]
async fn coalesced_streams_share_one_real_http_acquisition() {
    let server = MockExchange::start(vec![snapshot_response()]);

    let configs = load_profile(PROFILE, server.base_url());
    let limiters = build_limiters(&configs, PROFILE);
    let service = build_service(&configs, &limiters, PROFILE);

    let executor = RecoveryRestExecutor::new(&service);

    let (manager, handle) = coordinator(4);

    let manager_future = manager.run(|job| {
        let executor = &executor;

        async move { executor.execute(&job).await }
    });

    let clients = async move {
        let a = handle.clone();
        let b = handle.clone();

        // Register both requests concurrently so that
        // queued acquisition coalescing can occur.
        let (first, second) = tokio::join!(
            a.request(depth_request("btc-depth-a", 10, 1, Some(100),)),
            b.request(depth_request("btc-depth-b", 20, 2, Some(100),)),
        );

        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(first.job_id, second.job_id);

        assert_ne!(first.request_id, second.request_id);

        let first_response = first.completion.await.unwrap().result.unwrap();

        let second_response = second.completion.await.unwrap().result.unwrap();

        // Both streams receive the same native REST body.
        assert_eq!(first_response.body()["lastUpdateId"], 123456,);

        assert_eq!(second_response.body()["lastUpdateId"], 123456,);

        // The coordinator shares the actual response
        // allocation between coalesced requesters.
        assert!(Arc::ptr_eq(&first_response, &second_response,));

        // All handle clones are dropped at the end
        // of this future, allowing the coordinator
        // to terminate after outstanding work ends.
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("shared REST recovery did not finish");

    assert_eq!(
        server.request_count(),
        1,
        "coalesced requests executed duplicate HTTP calls",
    );

    let requests = server.requests();

    let line = &requests[0].request_line;

    assert!(line.starts_with("GET /api/v3/depth?"), "{line}",);

    assert!(line.contains("symbol=BTCUSDT"), "{line}");
    assert!(line.contains("limit=100"), "{line}");
}

// ============================================================
// TEST 2 — INCOMPATIBLE ACQUISITIONS STAY INDEPENDENT
// ============================================================

#[tokio::test]
async fn different_snapshot_depths_execute_separately() {
    let server = MockExchange::start(vec![snapshot_response(), snapshot_response()]);

    let configs = load_profile(PROFILE, server.base_url());
    let limiters = build_limiters(&configs, PROFILE);
    let service = build_service(&configs, &limiters, PROFILE);

    let executor = RecoveryRestExecutor::new(&service);

    let (manager, handle) = coordinator(2);

    let manager_future = manager.run(|job| {
        let executor = &executor;

        async move { executor.execute(&job).await }
    });

    let clients = async move {
        let a = handle.clone();
        let b = handle.clone();

        let (first, second) = tokio::join!(
            a.request(depth_request("depth-100", 10, 1, Some(100),)),
            b.request(depth_request("depth-500", 10, 1, Some(500),)),
        );

        let first = first.unwrap();
        let second = second.unwrap();

        assert_ne!(first.job_id, second.job_id);

        let (first_result, second_result) = tokio::join!(first.completion, second.completion,);

        assert!(first_result.unwrap().result.is_ok());
        assert!(second_result.unwrap().result.is_ok());
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("independent acquisitions did not finish");

    assert_eq!(server.request_count(), 2);

    let requests = server.requests();

    assert!(
        requests
            .iter()
            .any(|request| { request.request_line.contains("limit=100") }),
    );

    assert!(
        requests
            .iter()
            .any(|request| { request.request_line.contains("limit=500") }),
    );
}

// ============================================================
// TEST 3 — CANCELLING ONE STREAM PRESERVES SHARED JOB
// ============================================================

#[tokio::test]
async fn cancelling_one_stream_preserves_other_completion() {
    let (manager, handle) = coordinator(1);

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let executions = Arc::new(AtomicUsize::new(0));

    let manager_started = started.clone();
    let manager_release = release.clone();
    let manager_executions = executions.clone();

    // This controlled executor pauses after starting.
    //
    // It lets us cancel a requester while the shared
    // acquisition is definitely running.
    let manager_future = manager.run(move |_job| {
        let started = manager_started.clone();
        let release = manager_release.clone();
        let executions = manager_executions.clone();

        async move {
            executions.fetch_add(1, Ordering::SeqCst);

            started.notify_one();

            release.notified().await;

            Ok::<ExchangeRestResponse, RecoveryExecutionError>(synthetic_response())
        }
    });

    let clients = async move {
        let a = handle.clone();
        let b = handle.clone();

        let (first, second) = tokio::join!(
            a.request(depth_request("stream-a", 10, 1, Some(100),)),
            b.request(depth_request("stream-b", 20, 1, Some(100),)),
        );

        let first = first.unwrap();
        let second = second.unwrap();

        assert_eq!(first.job_id, second.job_id);

        // Wait until the acquisition is actually running.
        started.notified().await;

        // Cancel only stream A.
        let cancellation = handle.cancel(first.request_id).await.unwrap();

        // Stream B still needs the HTTP result.
        // Therefore, the running job must survive.
        assert_eq!(cancellation.abort_job, None);

        let completion = first.completion.await.unwrap();

        assert!(matches!(
            completion.result,
            Err(error)
                if matches!(
                    error.as_ref(),
                    RecoveryExecutionError::Cancelled
                )
        ));

        // Allow the single shared acquisition to finish.
        release.notify_one();

        let result = second.completion.await.unwrap().result.unwrap();

        assert_eq!(result.body()["lastUpdateId"], 123456,);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("shared-job cancellation test timed out");

    assert_eq!(executions.load(Ordering::SeqCst), 1,);
}

// ============================================================
// TEST 4 — GENERATION CANCELLATION
// ============================================================

#[tokio::test]
async fn obsolete_generation_cannot_receive_shared_result() {
    let (manager, handle) = coordinator(1);

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());

    let manager_started = started.clone();
    let manager_release = release.clone();

    let manager_future = manager.run(move |_job| {
        let started = manager_started.clone();
        let release = manager_release.clone();

        async move {
            started.notify_one();

            release.notified().await;

            Ok::<ExchangeRestResponse, RecoveryExecutionError>(synthetic_response())
        }
    });

    let clients = async move {
        let a = handle.clone();
        let b = handle.clone();

        let (old, current) = tokio::join!(
            a.request(depth_request("stream-a", 10, 1, Some(100),)),
            b.request(depth_request("stream-b", 20, 1, Some(100),)),
        );

        let old = old.unwrap();
        let current = current.unwrap();

        assert_eq!(old.job_id, current.job_id);

        started.notified().await;

        // Source stream A moved beyond generation 10.
        let cancelled = handle.cancel_generation("stream-a", 10).await.unwrap();

        assert_eq!(cancelled.len(), 1);

        assert_eq!(cancelled[0].cancellation.request_id, old.request_id,);

        // Stream B keeps the shared acquisition alive.
        assert_eq!(cancelled[0].abort_job, None);

        let completion = old.completion.await.unwrap();

        assert_eq!(completion.request_id, old.request_id);
        assert_eq!(completion.job_id, old.job_id);

        assert_eq!(completion.requester.epoch.stream_generation, 10,);

        assert!(matches!(
            completion.result,
            Err(error)
                if matches!(
                    error.as_ref(),
                    RecoveryExecutionError::Cancelled
                )
        ));

        release.notify_one();

        let response = current.completion.await.unwrap().result.unwrap();

        assert_eq!(response.body()["lastUpdateId"], 123456,);
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(manager_future, clients);
    })
    .await
    .expect("generation cancellation test timed out");
}
