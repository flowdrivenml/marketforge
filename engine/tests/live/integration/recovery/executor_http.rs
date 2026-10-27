use marketforge_engine::live::{
    config::exchange::{ExchangeId, MarketType},
    infrastructure::rest::{
        HistoricalTradeCursor, HistoricalTradePage, RestFacadeError, RestOperationError,
    },
    recovery::{
        RecoveryAttemptId, RecoveryExecutionError, RecoveryOperation, RecoveryRequest,
        RecoveryRequester, RecoveryRestExecutor, RecoveryScheduledJob, RecoveryScheduler,
        RecoverySchedulerLimits, RecoverySourceEpoch,
    },
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};

use super::{
    http_server::{MockExchange, MockResponse},
    rest_support::{build_limiters, build_service, load_profile},
};

// ============================================================
// SOURCE REQUEST FIXTURES
// ============================================================

fn source(channel: SourceChannel, symbol: &str) -> CreateSourceStreamRequest {
    CreateSourceStreamRequest {
        instrument_id: 9001,

        exchange: ExchangeId::Binance,
        symbol: symbol.into(),
        market_category: MarketType::Spot,
        instrument_type: "spot".into(),

        base_asset: Some("BTC".into()),
        quote_asset: Some("USDT".into()),
        settlement_asset: None,

        quantity_type: QuantityType::Base,

        contract_value: None,
        contract_value_asset: None,

        channel,
    }
}

fn recovery_request(
    stream_id: &str,
    source: CreateSourceStreamRequest,
    operation: RecoveryOperation,
) -> RecoveryRequest {
    RecoveryRequest::new(
        RecoveryRequester {
            stream_id: stream_id.into(),

            epoch: RecoverySourceEpoch {
                stream_generation: 10,
                connection_generation: 20,
                subscription_generation: 30,
            },

            attempt: RecoveryAttemptId(1),
        },
        source,
        1,
        operation,
    )
    .unwrap()
}

// ============================================================
// SCHEDULER FIXTURE
// ============================================================

fn scheduler() -> RecoveryScheduler {
    RecoveryScheduler::new(RecoverySchedulerLimits {
        max_requests: 100,
        max_jobs: 100,
        max_concurrent_jobs: 8,
    })
    .unwrap()
}

/// Register a single request and reserve its execution.
///
/// The result is the same RecoveryScheduledJob type that
/// the asynchronous coordinator will eventually dispatch.
fn scheduled_job(request: RecoveryRequest) -> RecoveryScheduledJob {
    let mut scheduler = scheduler();

    let registration = scheduler.register(request).unwrap();

    let jobs = scheduler.start_ready_jobs().unwrap();

    assert_eq!(jobs.len(), 1);

    let job = jobs.into_iter().next().unwrap();

    assert_eq!(job.job_id, registration.job_id);

    job
}

// ============================================================
// TEST 1: REAL DEPTH SNAPSHOT EXECUTION
// ============================================================

#[tokio::test]
async fn recovery_executor_fetches_binance_depth_snapshot() {
    let server = MockExchange::start(vec![MockResponse::ok(
        r#"{
                "lastUpdateId": 123456,
                "bids": [["80000", "2"]],
                "asks": [["80001", "3"]]
            }"#,
    )]);

    let profile = "binance_spot";

    let configs = load_profile(profile, server.base_url());

    let limiters = build_limiters(&configs, profile);

    let service = build_service(&configs, &limiters, profile);

    let executor = RecoveryRestExecutor::new(&service);

    let job = scheduled_job(recovery_request(
        "btc-depth",
        source(SourceChannel::Depth, "BTCUSDT"),
        RecoveryOperation::DepthSnapshot { depth: Some(100) },
    ));

    let response = executor.execute(&job).await.unwrap();

    assert_eq!(response.profile(), profile);
    assert_eq!(response.endpoint(), "depth_snapshot");
    assert_eq!(response.status(), 200);

    assert_eq!(response.body()["lastUpdateId"], 123456,);

    assert_eq!(response.body()["bids"][0][0], "80000",);

    assert_eq!(response.body()["asks"][0][1], "3",);

    // The executor performs exactly one acquisition.
    assert_eq!(server.request_count(), 1);

    let requests = server.requests();

    let line = &requests[0].request_line;

    assert!(line.starts_with("GET /api/v3/depth?"), "{line}",);

    assert!(line.contains("symbol=BTCUSDT"), "{line}",);

    assert!(line.contains("limit=100"), "{line}",);
}

// ============================================================
// TEST 2: RECENT TRADE ACQUISITION
// ============================================================

#[tokio::test]
async fn recovery_executor_fetches_recent_trades() {
    let server = MockExchange::start(vec![MockResponse::ok(
        r#"[
                {
                    "id": 501,
                    "price": "80000",
                    "qty": "0.25",
                    "time": 1793000000000,
                    "isBuyerMaker": false
                }
            ]"#,
    )]);

    let profile = "binance_spot";

    let configs = load_profile(profile, server.base_url());

    let limiters = build_limiters(&configs, profile);

    let service = build_service(&configs, &limiters, profile);

    let executor = RecoveryRestExecutor::new(&service);

    let job = scheduled_job(recovery_request(
        "btc-trades",
        source(SourceChannel::Trades, "BTCUSDT"),
        RecoveryOperation::RecentTrades { limit: Some(50) },
    ));

    let response = executor.execute(&job).await.unwrap();

    assert_eq!(response.profile(), profile);
    assert_eq!(response.endpoint(), "recent_trades");

    assert_eq!(response.body()[0]["id"], 501);

    assert_eq!(server.request_count(), 1);

    let line = &server.requests()[0].request_line;

    assert!(line.starts_with("GET /api/v3/trades?"), "{line}",);

    assert!(line.contains("symbol=BTCUSDT"), "{line}",);

    assert!(line.contains("limit=50"), "{line}",);
}

// ============================================================
// TEST 3: INVALID HISTORICAL CURSOR
// ============================================================

#[tokio::test]
async fn recovery_executor_rejects_invalid_cursor_before_http() {
    let server = MockExchange::start(vec![]);

    let profile = "binance_spot";

    let configs = load_profile(profile, server.base_url());

    let limiters = build_limiters(&configs, profile);

    let service = build_service(&configs, &limiters, profile);

    let executor = RecoveryRestExecutor::new(&service);

    // Binance Spot supports historical trade retrieval,
    // but not the OKX pagination cursor strategy.
    let job = scheduled_job(recovery_request(
        "btc-history",
        source(SourceChannel::Trades, "BTCUSDT"),
        RecoveryOperation::HistoricalTrades {
            page: HistoricalTradePage::with_cursor(HistoricalTradeCursor::Okx {
                before: None,
                after: Some(12345),
            }),
        },
    ));

    let result = executor.execute(&job).await;

    assert!(matches!(
        result,
        Err(RecoveryExecutionError::Rest(RestOperationError::Facade(
            RestFacadeError::InvalidParameter {
                name: "historical_cursor",
                ..
            }
        )))
    ));

    assert_eq!(
        server.request_count(),
        0,
        "invalid cursor unexpectedly reached HTTP",
    );
}

// ============================================================
// TEST 4: HTTP RESPONSE VALIDATION
// ============================================================

#[tokio::test]
async fn recovery_executor_preserves_rest_response_failure() {
    let server = MockExchange::start(vec![
        // Valid HTTP 200, invalid JSON body.
        MockResponse::ok("not valid JSON"),
    ]);

    let profile = "binance_spot";

    let configs = load_profile(profile, server.base_url());

    let limiters = build_limiters(&configs, profile);

    let service = build_service(&configs, &limiters, profile);

    let executor = RecoveryRestExecutor::new(&service);

    let job = scheduled_job(recovery_request(
        "btc-depth",
        source(SourceChannel::Depth, "BTCUSDT"),
        RecoveryOperation::DepthSnapshot { depth: Some(100) },
    ));

    let error = executor.execute(&job).await.unwrap_err();

    // Recovery must preserve REST's actual failure
    // classification instead of turning it into a
    // generic recovery-integrity failure.
    let RecoveryExecutionError::Rest(RestOperationError::Transport(rest_error)) = error else {
        panic!("expected classified REST transport error");
    };

    assert_eq!(
        rest_error.kind(),
        marketforge_engine::live::infrastructure::rest::RestErrorKind::InvalidResponse,
    );

    assert_eq!(server.request_count(), 1);
}

// ============================================================
// TEST 5: SHARED RATE LIMIT ACCOUNTING
// ============================================================

#[tokio::test]
async fn recovery_acquisition_uses_existing_shared_limiter() {
    let server = MockExchange::start(vec![MockResponse::ok("[]"), MockResponse::ok("[]")]);

    let profile = "binance_spot";

    let configs = load_profile(profile, server.base_url());

    let limiters = build_limiters(&configs, profile);

    let service = build_service(&configs, &limiters, profile);

    let executor = RecoveryRestExecutor::new(&service);

    let before = limiters.manager().snapshot();

    let first = scheduled_job(recovery_request(
        "btc-trades",
        source(SourceChannel::Trades, "BTCUSDT"),
        RecoveryOperation::RecentTrades { limit: Some(10) },
    ));

    let second = scheduled_job(recovery_request(
        "eth-trades",
        source(SourceChannel::Trades, "ETHUSDT"),
        RecoveryOperation::RecentTrades { limit: Some(10) },
    ));

    executor.execute(&first).await.unwrap();

    executor.execute(&second).await.unwrap();

    assert_eq!(server.request_count(), 2);

    let after = limiters.manager().snapshot();

    let used_before: u64 = before
        .budgets
        .iter()
        .map(|budget| budget.locally_used)
        .sum();

    let used_after: u64 = after.budgets.iter().map(|budget| budget.locally_used).sum();

    assert!(
        used_after > used_before,
        "recovery operations bypassed limiter accounting",
    );

    assert_eq!(after.total_pending_requests, 0,);
}
