use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use marketforge_engine::live::{
    config::{ExchangeConfigStore, exchange::ExchangeConfig},
    infrastructure::rest::{RestFacadeError, RestFacadeFactory, RestOperationError},
    stream::request::SourceChannel,
};

use super::factory::{build_limiters, build_service, spot_request};

use crate::http_server::{MockExchange, MockResponse};

// ============================================================
// REAL CONFIGURATION WITH LOCAL HTTP TRANSPORT
// ============================================================

fn load_profile(server: &MockExchange, profile: &str) -> ExchangeConfigStore {
    let source = PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
    .join(format!("{profile}.toml"));

    let content =
        fs::read_to_string(&source).expect("failed to read real Binance Spot configuration");

    let mut config: ExchangeConfig =
        toml::from_str(&content).expect("failed to parse real Binance Spot configuration");

    // Preserve the real endpoint definitions, limits,
    // authentication rules, weights, and mappings.
    //
    // Redirect only the HTTP destination to our mock.
    config.rest.base_url = server.base_url().to_owned();

    config
        .validate()
        .expect("real configuration is invalid after URL override");

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let directory = std::env::temp_dir().join(format!(
        "marketforge-rest-operations-{}-{unique}",
        std::process::id()
    ));

    fs::create_dir_all(&directory).expect("failed to create temporary directory");

    let destination = directory.join(format!("{profile}.toml"));

    fs::write(&destination, toml::to_string(&config).unwrap())
        .expect("failed to write redirected configuration");

    let result = ExchangeConfigStore::load_dir(&directory);

    fs::remove_dir_all(&directory).expect("failed to remove temporary configuration directory");

    result.expect("failed to load redirected real configuration")
}

// ============================================================
// REQUEST OBSERVATION
// ============================================================

fn assert_request_contains(server: &MockExchange, path: &str, parameters: &[&str]) {
    let requests = server.requests();

    assert_eq!(requests.len(), 1, "expected exactly one HTTP request");

    let request_line = &requests[0].request_line;

    assert!(
        request_line.starts_with(&format!("GET {path}?")),
        "unexpected HTTP request: {request_line}"
    );

    for parameter in parameters {
        assert!(
            request_line.contains(parameter),
            "missing '{parameter}' in {request_line}"
        );
    }
}

// ============================================================
// DEFAULT DEPTH SNAPSHOT
// ============================================================

#[tokio::test]
async fn snapshot_uses_real_binance_endpoint_and_defaults() {
    let server = MockExchange::start(vec![MockResponse::ok(
        r#"{
                "lastUpdateId": 123456,
                "bids": [["100", "2"]],
                "asks": [["101", "3"]]
            }"#,
    )]);

    let configs = load_profile(&server, "binance_spot");
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let request = spot_request(1001, "BTCUSDT", SourceChannel::Depth);

    let handle = factory.create(&request).unwrap();

    let response = handle.snapshot().await.unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.body()["lastUpdateId"], 123456);

    let configured = configs
        .get("binance_spot")
        .unwrap()
        .endpoint("depth_snapshot")
        .unwrap();

    let expected_limit = configured
        .default_limit
        .expect("real snapshot endpoint needs a default limit");

    assert_request_contains(
        &server,
        "/api/v3/depth",
        &["symbol=BTCUSDT", &format!("limit={expected_limit}")],
    );
}

// ============================================================
// EXPLICIT DEPTH
// ============================================================

#[tokio::test]
async fn snapshot_with_depth_binds_explicit_limit() {
    let server = MockExchange::start(vec![MockResponse::ok(
        r#"{
                "lastUpdateId": 200,
                "bids": [],
                "asks": []
            }"#,
    )]);

    let configs = load_profile(&server, "binance_spot");
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let handle = factory
        .create(&spot_request(1002, "ETHUSDT", SourceChannel::Depth))
        .unwrap();

    let response = handle.snapshot_with_depth(100).await.unwrap();

    assert_eq!(response.status(), 200);

    assert_request_contains(&server, "/api/v3/depth", &["symbol=ETHUSDT", "limit=100"]);
}

// ============================================================
// INVALID DEPTH
// ============================================================

#[tokio::test]
async fn invalid_snapshot_depth_does_not_send_http() {
    let server = MockExchange::start(vec![]);

    let configs = load_profile(&server, "binance_spot");
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let handle = factory
        .create(&spot_request(1003, "BTCUSDT", SourceChannel::Depth))
        .unwrap();

    let error = handle.snapshot_with_depth(0).await.unwrap_err();

    assert!(matches!(
        error,
        RestOperationError::Facade(RestFacadeError::InvalidParameter { name: "limit", .. })
    ));

    assert_eq!(server.request_count(), 0);

    let maximum = handle
        .capabilities()
        .depth_snapshot
        .as_ref()
        .unwrap()
        .maximum_limit
        .expect("missing maximum snapshot depth");

    let error = handle.snapshot_with_depth(maximum + 1).await.unwrap_err();

    assert!(matches!(
        error,
        RestOperationError::Facade(RestFacadeError::InvalidParameter { name: "limit", .. })
    ));

    assert_eq!(
        server.request_count(),
        0,
        "invalid depth must be rejected before HTTP execution"
    );
}

// ============================================================
// DEFAULT RECENT TRADES
// ============================================================

#[tokio::test]
async fn recent_trades_uses_real_binance_endpoint() {
    let server = MockExchange::start(vec![MockResponse::ok(
        r#"[
                {
                    "id": 501,
                    "price": "100.5",
                    "qty": "0.2",
                    "time": 123456789,
                    "isBuyerMaker": false
                }
            ]"#,
    )]);

    let configs = load_profile(&server, "binance_spot");
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let handle = factory
        .create(&spot_request(1004, "BTCUSDT", SourceChannel::Trades))
        .unwrap();

    let response = handle.recent_trades().await.unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.body()[0]["id"], 501);

    let configured = configs
        .get("binance_spot")
        .unwrap()
        .endpoint("recent_trades")
        .unwrap();

    let expected_limit = configured
        .default_limit
        .expect("real recent-trades endpoint needs a default limit");

    assert_request_contains(
        &server,
        "/api/v3/trades",
        &["symbol=BTCUSDT", &format!("limit={expected_limit}")],
    );
}

// ============================================================
// EXPLICIT RECENT TRADE LIMIT
// ============================================================

#[tokio::test]
async fn recent_trades_with_limit_binds_explicit_value() {
    let server = MockExchange::start(vec![MockResponse::ok("[]")]);

    let configs = load_profile(&server, "binance_spot");
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let handle = factory
        .create(&spot_request(1005, "ETHUSDT", SourceChannel::Trades))
        .unwrap();

    let response = handle.recent_trades_with_limit(50).await.unwrap();

    assert_eq!(response.status(), 200);

    assert_request_contains(&server, "/api/v3/trades", &["symbol=ETHUSDT", "limit=50"]);
}

// ============================================================
// INVALID RECENT TRADE LIMIT
// ============================================================

#[tokio::test]
async fn invalid_recent_trades_limit_does_not_send_http() {
    let server = MockExchange::start(vec![]);

    let configs = load_profile(&server, "binance_spot");
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let handle = factory
        .create(&spot_request(1006, "BTCUSDT", SourceChannel::Trades))
        .unwrap();

    let error = handle.recent_trades_with_limit(0).await.unwrap_err();

    assert!(matches!(
        error,
        RestOperationError::Facade(RestFacadeError::InvalidParameter { name: "limit", .. })
    ));

    let maximum = handle
        .capabilities()
        .recent_trades
        .as_ref()
        .unwrap()
        .maximum_limit
        .expect("missing maximum recent-trades limit");

    let error = handle
        .recent_trades_with_limit(maximum + 1)
        .await
        .unwrap_err();

    assert!(matches!(
        error,
        RestOperationError::Facade(RestFacadeError::InvalidParameter { name: "limit", .. })
    ));

    assert_eq!(server.request_count(), 0);
}

// ============================================================
// MULTIPLE HANDLES SHARE THE HTTP CLIENT AND LIMITER
// ============================================================

#[tokio::test]
async fn multiple_handles_share_rate_limit_accounting() {
    let server = MockExchange::start(vec![MockResponse::ok("[]"), MockResponse::ok("[]")]);

    let configs = load_profile(&server, "binance_spot");
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let btc = factory
        .create(&spot_request(2001, "BTCUSDT", SourceChannel::Trades))
        .unwrap();

    let eth = factory
        .create(&spot_request(2002, "ETHUSDT", SourceChannel::Trades))
        .unwrap();

    assert!(std::ptr::eq(btc.service(), eth.service()));

    let before = limiters.manager().snapshot();

    btc.recent_trades_with_limit(10).await.unwrap();
    eth.recent_trades_with_limit(10).await.unwrap();

    let after = limiters.manager().snapshot();

    assert_eq!(server.request_count(), 2);

    let requests = server.requests();

    assert!(
        requests
            .iter()
            .any(|request| { request.request_line.contains("symbol=BTCUSDT") }),
        "BTC request was not observed"
    );

    assert!(
        requests
            .iter()
            .any(|request| { request.request_line.contains("symbol=ETHUSDT") }),
        "ETH request was not observed"
    );

    let before_used: u64 = before
        .budgets
        .iter()
        .map(|budget| budget.locally_used)
        .sum();

    let after_used: u64 = after.budgets.iter().map(|budget| budget.locally_used).sum();

    assert!(
        after_used > before_used,
        "two HTTP requests must consume shared rate-limit capacity"
    );
}
