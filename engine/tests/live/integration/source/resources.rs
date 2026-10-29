use super::{
    harness::*,
    http_server::{MockExchange, MockResponse},
    pipeline, profiles, rest_support,
};
use marketforge_engine::live::{
    infrastructure::limiter::{
        ConnectionRateLimiter, RateLimitedOperation, RequestPriority, factory::WeightParameters,
    },
    stream::{registry::StreamLifecycleState, source::*},
};
use std::{sync::Arc, time::Duration};
use tokio::{sync::mpsc, time::timeout};

#[tokio::test]
async fn configured_websocket_subscription_quota_and_deadline_are_enforced() {
    let configs = rest_support::load_profile("binance_spot", "http://127.0.0.1:1");
    let limiters = rest_support::build_limiters(&configs, "binance_spot");
    let template = &limiters
        .profile("binance_spot")
        .unwrap()
        .websocket
        .connection;
    let connection = ConnectionRateLimiter::new(template, 1).unwrap();
    for _ in 0..5 {
        connection
            .acquire_subscription(RequestPriority::Normal)
            .await
            .unwrap();
    }
    assert!(
        connection
            .acquire_subscription_with_timeout(RequestPriority::Normal, Duration::from_millis(20))
            .await
            .is_err()
    );
    let snapshot = connection.snapshot();
    assert_eq!(
        snapshot.total_pending_requests, 0,
        "timed-out subscription leaked a pending reservation"
    );
    assert!(
        snapshot
            .budgets
            .iter()
            .any(|budget| budget.capacity == 5 && budget.locally_used == 5)
    );
    let independent = ConnectionRateLimiter::new(template, 2).unwrap();
    independent
        .acquire_subscription_with_timeout(RequestPriority::Normal, Duration::from_millis(20))
        .await
        .unwrap();
    println!(
        "[binance_spot/websocket] five subscriptions admitted; sixth deadline enforced; pending=0; independent connection admitted"
    );
}

#[tokio::test]
async fn source_recovery_obeys_shared_rest_budget_and_cancellation_releases_waiter() {
    let http = MockExchange::start(vec![MockResponse::ok("[]")]);
    let configs = Arc::new(rest_support::load_profile("binance_spot", http.base_url()));
    let limiters = rest_support::build_limiters(&configs, "binance_spot");
    let shared = limiters.manager();
    let mut charge = limiters
        .profile("binance_spot")
        .unwrap()
        .rest
        .operation(
            "historical_trades",
            &WeightParameters::new(),
            RequestPriority::Normal,
        )
        .unwrap();
    let snapshot = shared.snapshot();
    for item in &mut charge.charges {
        item.cost = snapshot
            .budgets
            .iter()
            .find(|budget| budget.id == item.budget)
            .unwrap()
            .capacity;
    }
    shared
        .acquire(&RateLimitedOperation::new(charge.charges).unwrap())
        .await
        .unwrap();
    let mut socket = LocalSocket::start("binance_spot").await;
    let manager = manager(&configs, "binance_spot", &limiters, &socket.url);
    let definition = profiles::definition(profiles::requests().remove(3), "shared-budget");
    let prepared = builder(configs.clone(), SourceInitializationLimits::default())
        .prepare(definition.clone())
        .unwrap();
    let (recovery, task) = recovery(configs, "binance_spot", limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let mut supervisor = supervisor(definition);
    let (tx, mut rx) = mpsc::channel(4);
    supervisor
        .start(
            "shared-budget",
            executor,
            |_| Ok(()),
            move |payload| {
                tx.try_send(pipeline::published(payload))
                    .map_err(|e| e.to_string())
            },
        )
        .unwrap();
    socket.ack().await;
    let mut baseline = pipeline::native("binance_spot");
    baseline["t"] = 10.into();
    socket.send.send(baseline.clone()).await.unwrap();
    timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
    baseline["t"] = 13.into();
    socket.send.send(baseline).await.unwrap();
    timeout(DEADLINE, async {
        while shared.pending_count() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    supervisor.sync_readiness().unwrap();
    assert_ne!(
        supervisor
            .registry()
            .status("shared-budget")
            .unwrap()
            .lifecycle,
        StreamLifecycleState::Running
    );
    assert_eq!(
        http.request_count(),
        0,
        "REST bypassed the shared exhausted budget"
    );
    assert_eq!(shared.pending_count(), 1);
    timeout(DEADLINE, supervisor.stop("shared-budget"))
        .await
        .unwrap()
        .unwrap();
    timeout(DEADLINE, async {
        while shared.pending_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(rx.try_recv().is_err());
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
    println!(
        "[binance_spot/trades] shared REST quota enforced; pending_peak=1; HTTP=0; cancellation released waiter"
    );
}
