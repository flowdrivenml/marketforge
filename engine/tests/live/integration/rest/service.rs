use marketforge_engine::live::infrastructure::{
    limiter::factory::{ExchangeLimiterFactory, LimiterProfileInput, WebSocketLimiterBindings},
    rest::ExchangeRestBootstrap,
};
use marketforge_engine::live::{
    config::ExchangeConfigStore,
    config::exchange::RestApplicationResponseConfig,
    infrastructure::rest::{
        ExchangeRestClientFactory, ExchangeRestService, RestFactoryFeedbackPolicy,
    },
};
use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::{
        backoff::BackoffConfig,
        limiter::{
            BudgetScope, RateLimitManager,
            factory::{
                CompiledRestLimiters, RestBudgetBinding, RestLimiterBindings, RestLimiterCompiler,
            },
        },
        rest::{
            ExchangeRestClient, RestClientConfig, RestClientFeedbackConfig, RestErrorKind,
            RestRequest, RestRequestResolver, RestRetryPolicy,
        },
    },
};
use std::{collections::BTreeMap, time::Duration};

// ============================================================
// MOCK HTTP RESPONSE
// ============================================================

use crate::http_server::{MockExchange, MockResponse};

// ============================================================
// TEST CONFIGURATION
// ============================================================

fn exchange_config(base_url: &str, capacity: u64) -> ExchangeConfig {
    let toml = format!(
        r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid/ws"

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "{base_url}"
authentication = "none"
request_timeout_seconds = 3

[rest.rate_limiting]
strategy = "ip_window"
ip_max_requests = {capacity}
ip_window_seconds = 60
http_throttled_status = 429
honor_retry_after = true

[rest.endpoints.depth_snapshot]
method = "GET"
path = "/api/v3/depth"
parameters = ["symbol", "limit"]
default_limit = 100
maximum_limit = 1000
authentication = "none"
"#
    );

    toml::from_str(&toml).unwrap()
}

// ============================================================
// TEST RUNTIME
// ============================================================

struct TestRuntime {
    config: ExchangeConfig,
    client: ExchangeRestClient,
    limiter: RateLimitManager,
}

impl TestRuntime {
    fn new(server: &MockExchange, capacity: u64, max_response_bytes: usize) -> Self {
        let config = exchange_config(server.base_url(), capacity);

        let bindings = RestLimiterBindings {
            ip: Some(RestBudgetBinding::new(
                "spot_rest",
                BudgetScope::Egress("test".into()),
            )),
            ..Default::default()
        };

        let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

        let limiter = RateLimitManager::new(compiled.policies.clone(), 32).unwrap();

        let profiles =
            BTreeMap::<String, CompiledRestLimiters>::from([("binance_spot".into(), compiled)]);

        let feedback = BTreeMap::from([(
            "binance_spot".into(),
            RestClientFeedbackConfig {
                rate_limiting: config.rest.rate_limiting.clone(),
                usage_bindings: Vec::new(),
                fallback_cooldown: Duration::from_millis(100),
                application_rules: BTreeMap::new(),
            },
        )]);

        let client = ExchangeRestClient::new(
            limiter.clone(),
            profiles,
            feedback,
            RestClientConfig {
                max_concurrent_requests: 8,
                max_response_bytes,
            },
        )
        .unwrap();

        Self {
            config,
            client,
            limiter,
        }
    }

    fn resolve(&self) -> marketforge_engine::live::infrastructure::rest::ResolvedRestRequest {
        let request =
            RestRequest::new("binance_spot", "depth_snapshot").with_parameter("symbol", "BTCUSDT");

        let endpoint = self.config.endpoint("depth_snapshot").unwrap();

        RestRequestResolver::resolve_endpoint(&self.config, endpoint, &request).unwrap()
    }
}

fn retry_policy(max_attempts: u32, overall_timeout: Duration) -> RestRetryPolicy {
    let backoff =
        BackoffConfig::new(Duration::from_millis(10), Duration::from_millis(100), false).unwrap();

    RestRetryPolicy::new(max_attempts, overall_timeout, backoff).unwrap()
}

// ============================================================
// SUCCESSFUL HTTP EXECUTION
// ============================================================

#[tokio::test]
async fn successful_get_returns_exchange_json() {
    let server = MockExchange::start(vec![MockResponse::ok(
        r#"{"lastUpdateId":123,"bids":[],"asks":[]}"#,
    )]);

    let runtime = TestRuntime::new(&server, 100, 1024 * 1024);

    let response = runtime
        .client
        .execute_once(&runtime.resolve())
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    assert_eq!(response.attempts(), 1);

    assert_eq!(response.body()["lastUpdateId"], 123,);

    let requests = server.requests();

    assert_eq!(requests.len(), 1);

    assert!(requests[0].request_line.starts_with("GET /api/v3/depth?"));

    assert!(requests[0].request_line.contains("symbol=BTCUSDT"));

    assert!(requests[0].request_line.contains("limit=100"));
}

// ============================================================
// PERMANENT HTTP ERRORS
// ============================================================

#[tokio::test]
async fn http_400_is_invalid_request() {
    let server = MockExchange::start(vec![MockResponse::error(
        400,
        r#"{"msg":"Invalid symbol"}"#,
    )]);

    let runtime = TestRuntime::new(&server, 100, 1024 * 1024);

    let error = runtime
        .client
        .execute_once(&runtime.resolve())
        .await
        .unwrap_err();

    assert_eq!(error.kind(), RestErrorKind::InvalidRequest,);

    assert_eq!(error.http_status(), Some(400));

    assert_eq!(server.request_count(), 1);
}

#[tokio::test]
async fn http_400_is_not_retried() {
    let server = MockExchange::start(vec![
        MockResponse::error(400, r#"{"msg":"Invalid request"}"#),
        MockResponse::ok(r#"{"unexpected":true}"#),
    ]);

    let runtime = TestRuntime::new(&server, 100, 1024 * 1024);

    let error = runtime
        .client
        .execute_with_retry(&runtime.resolve(), retry_policy(3, Duration::from_secs(2)))
        .await
        .unwrap_err();

    assert_eq!(error.kind(), RestErrorKind::InvalidRequest,);

    assert_eq!(error.attempts(), 1);
    assert_eq!(server.request_count(), 1);
}

// ============================================================
// AUTOMATIC RETRIES
// ============================================================

#[tokio::test]
async fn retries_two_server_errors_then_succeeds() {
    let server = MockExchange::start(vec![
        MockResponse::error(503, r#"{"msg":"Unavailable"}"#),
        MockResponse::error(503, r#"{"msg":"Unavailable"}"#),
        MockResponse::ok(r#"{"lastUpdateId":999}"#),
    ]);

    let runtime = TestRuntime::new(&server, 100, 1024 * 1024);

    let response = runtime
        .client
        .execute_with_retry(&runtime.resolve(), retry_policy(3, Duration::from_secs(2)))
        .await
        .unwrap();

    assert_eq!(response.attempts(), 3);
    assert_eq!(response.body()["lastUpdateId"], 999);
    assert_eq!(server.request_count(), 3);

    // Each HTTP attempt must consume rate-limit capacity.
    let snapshot = runtime.limiter.snapshot();

    assert_eq!(snapshot.budgets.len(), 1);
    assert_eq!(snapshot.budgets[0].locally_used, 3);
}

// ============================================================
// EXCHANGE COOLDOWN
// ============================================================

#[tokio::test]
async fn http_429_cooldown_blocks_retry() {
    let server = MockExchange::start(vec![
        MockResponse::throttled("1"),
        MockResponse::ok(r#"{"lastUpdateId":456}"#),
    ]);

    let runtime = TestRuntime::new(&server, 100, 1024 * 1024);

    let response = runtime
        .client
        .execute_with_retry(&runtime.resolve(), retry_policy(2, Duration::from_secs(3)))
        .await
        .unwrap();

    assert_eq!(response.attempts(), 2);
    assert_eq!(server.request_count(), 2);

    let requests = server.requests();

    let elapsed = requests[1]
        .received_at
        .duration_since(requests[0].received_at);

    // The retry's 10ms backoff is much shorter than
    // the exchange's 1-second cooldown.
    //
    // Therefore, this verifies that the shared limiter
    // actually blocked the second HTTP request.
    assert!(
        elapsed >= Duration::from_millis(900),
        "retry bypassed exchange cooldown: {elapsed:?}",
    );
}

// ============================================================
// INVALID JSON
// ============================================================

#[tokio::test]
async fn invalid_json_returns_invalid_response() {
    let server = MockExchange::start(vec![MockResponse::ok("not valid JSON")]);

    let runtime = TestRuntime::new(&server, 100, 1024 * 1024);

    let error = runtime
        .client
        .execute_once(&runtime.resolve())
        .await
        .unwrap_err();

    assert_eq!(error.kind(), RestErrorKind::InvalidResponse,);

    assert_eq!(server.request_count(), 1);
}

// ============================================================
// RESPONSE SIZE
// ============================================================

#[tokio::test]
async fn oversized_response_is_rejected() {
    let server = MockExchange::start(vec![MockResponse::ok(
        r#"{"data":"this response exceeds the configured limit"}"#,
    )]);

    let runtime = TestRuntime::new(&server, 100, 16);

    let error = runtime
        .client
        .execute_once(&runtime.resolve())
        .await
        .unwrap_err();

    assert_eq!(error.kind(), RestErrorKind::ResponseTooLarge,);

    assert_eq!(server.request_count(), 1);
}

// ============================================================
// SHARED RATE LIMITING
// ============================================================

#[tokio::test]
async fn exhausted_quota_prevents_second_http_request() {
    let server = MockExchange::start(vec![
        MockResponse::ok(r#"{"value":1}"#),
        MockResponse::ok(r#"{"value":2}"#),
    ]);

    // Only one request per 60 seconds.
    let runtime = TestRuntime::new(&server, 1, 1024 * 1024);

    let request = runtime.resolve();

    runtime.client.execute_once(&request).await.unwrap();

    let second = tokio::time::timeout(
        Duration::from_millis(100),
        runtime.client.execute_once(&request),
    )
    .await;

    assert!(second.is_err(), "second request bypassed shared quota",);

    assert_eq!(server.request_count(), 1);

    // Cancelling the waiting future should also remove
    // its pending admission request.
    assert_eq!(runtime.limiter.pending_count(), 0);
}

// ============================================================
// OVERALL RETRY DEADLINE
// ============================================================

#[tokio::test]
async fn overall_deadline_cancels_quota_wait() {
    let server = MockExchange::start(vec![
        MockResponse::ok(r#"{"value":1}"#),
        MockResponse::ok(r#"{"value":2}"#),
    ]);

    let runtime = TestRuntime::new(&server, 1, 1024 * 1024);

    let request = runtime.resolve();

    runtime.client.execute_once(&request).await.unwrap();

    let result = runtime
        .client
        .execute_with_retry(&request, retry_policy(3, Duration::from_millis(100)))
        .await;

    let error = result.unwrap_err();

    assert_eq!(error.kind(), RestErrorKind::Timeout,);

    // No additional HTTP request reached the server.
    assert_eq!(server.request_count(), 1);

    // Overall deadline cancellation must clean up
    // the rate limiter's pending request.
    assert_eq!(runtime.limiter.pending_count(), 0);
}

// ============================================================
// FACTORY + SERVICE END-TO-END INTEGRATION
// ============================================================

#[tokio::test]
async fn factory_service_executes_and_validates_application_responses() {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    // ========================================================
    // LOCAL MOCK EXCHANGE
    // ========================================================

    let server = MockExchange::start(vec![
        MockResponse::ok(
            r#"{
                "retCode": 0,
                "retMsg": "OK",
                "result": {
                    "lastUpdateId": 123,
                    "bids": [],
                    "asks": []
                }
            }"#,
        ),
        MockResponse::ok(
            r#"{
                "retCode": 10001,
                "retMsg": "Invalid parameter",
                "result": {}
            }"#,
        ),
    ]);

    // ========================================================
    // EXCHANGE CONFIGURATION
    // ========================================================

    let mut config = exchange_config(server.base_url(), 100);

    // Configure an application envelope.
    //
    // We deliberately use Bybit-style response semantics
    // with our existing synthetic Binance profile.
    //
    // This verifies that behavior is driven by configuration,
    // not hardcoded exchange-specific logic.

    let application: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "envelope"
code_field = "retCode"
success_codes = ["0"]
message_field = "retMsg"
data_field = "result"
"#,
    )
    .unwrap();

    config
        .rest
        .endpoints
        .get_mut("depth_snapshot")
        .unwrap()
        .application_response = Some(application);

    config.validate().unwrap();

    // ========================================================
    // BUILD CONFIGURATION STORE
    // ========================================================

    // ExchangeConfigStore::load_dir accepts a directory.
    //
    // Create a temporary directory containing exactly
    // one synthetic profile.

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let directory = std::env::temp_dir().join(format!(
        "marketforge-rest-factory-{}-{unique}",
        std::process::id(),
    ));

    fs::create_dir_all(&directory).unwrap();

    let config_path = directory.join("binance_spot.toml");

    fs::write(&config_path, toml::to_string(&config).unwrap()).unwrap();

    let configs = ExchangeConfigStore::load_dir(&directory).unwrap();

    // The store has loaded the TOML into memory.
    // The temporary configuration directory is no
    // longer needed.
    fs::remove_dir_all(&directory).unwrap();

    // ========================================================
    // COMPILE REST RATE LIMITS
    // ========================================================

    let bindings = RestLimiterBindings {
        ip: Some(RestBudgetBinding::new(
            "spot_rest",
            BudgetScope::Egress("test".into()),
        )),
        ..Default::default()
    };

    let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

    // ========================================================
    // CREATE ONE SHARED LIMITER
    // ========================================================

    let limiter = RateLimitManager::new(compiled.policies.clone(), 32).unwrap();

    // Keep a clone so we can independently inspect
    // the same underlying limiter state.

    let observer = limiter.clone();

    // ========================================================
    // FACTORY INPUTS
    // ========================================================

    let compiled_profiles = BTreeMap::from([("binance_spot".to_string(), compiled)]);

    let rest_profiles = BTreeMap::from([("binance_spot".to_string(), config.rest.clone())]);

    let feedback_policy =
        RestFactoryFeedbackPolicy::new(Vec::new(), Duration::from_millis(100)).unwrap();

    let feedback_policies = BTreeMap::from([("binance_spot".to_string(), feedback_policy)]);

    // ========================================================
    // FACTORY CONSTRUCTION
    // ========================================================

    let client = ExchangeRestClientFactory::build(
        limiter,
        compiled_profiles,
        &rest_profiles,
        feedback_policies,
        RestClientConfig {
            max_concurrent_requests: 8,
            max_response_bytes: 1024 * 1024,
        },
    )
    .unwrap();

    // ========================================================
    // SERVICE CONSTRUCTION
    // ========================================================

    let service = ExchangeRestService::new(&configs, client);

    // ========================================================
    // REQUEST 1: APPLICATION SUCCESS
    // ========================================================

    let request =
        RestRequest::new("binance_spot", "depth_snapshot").with_parameter("symbol", "BTCUSDT");

    let response = service.execute(request).await.unwrap();

    assert_eq!(response.status(), 200);

    assert_eq!(response.body()["retCode"], 0,);

    assert_eq!(response.body()["result"]["lastUpdateId"], 123,);

    assert_eq!(server.request_count(), 1);

    // ========================================================
    // REQUEST 2: APPLICATION FAILURE WITH HTTP 200
    // ========================================================

    let request =
        RestRequest::new("binance_spot", "depth_snapshot").with_parameter("symbol", "BTCUSDT");

    let error = service.execute(request).await.unwrap_err();

    assert_eq!(error.kind(), RestErrorKind::HttpError,);

    assert_eq!(error.http_status(), Some(200),);

    assert_eq!(error.exchange_code(), Some("10001"),);

    assert_eq!(server.request_count(), 2);

    // ========================================================
    // VERIFY SHARED LIMITER
    // ========================================================

    let snapshot = observer.snapshot();

    assert_eq!(snapshot.budgets.len(), 1,);

    assert_eq!(snapshot.budgets[0].locally_used, 2,);

    // ========================================================
    // VERIFY ACTUAL HTTP REQUESTS
    // ========================================================

    for observed in server.requests() {
        assert!(observed.request_line.starts_with("GET /api/v3/depth?"),);

        assert!(observed.request_line.contains("symbol=BTCUSDT"),);

        assert!(observed.request_line.contains("limit=100"),);
    }

    println!();
    println!("==========================================");
    println!("MARKETFORGE REST FACTORY INTEGRATION");
    println!("==========================================");
    println!("Configuration store:      PASS");
    println!("REST limiter compilation: PASS");
    println!("REST client factory:      PASS");
    println!("REST service:             PASS");
    println!("Request resolution:       PASS");
    println!("HTTP execution:           PASS");
    println!("Application success:      PASS");
    println!("Application rejection:    PASS");
    println!("Shared limiter state:     PASS");
    println!("Requests executed:        {}", server.request_count());
}

// ============================================================
// FACTORY REJECTS MISSING APPLICATION CONFIGURATION
// ============================================================

#[test]
fn factory_rejects_missing_application_response() {
    let config = exchange_config("http://127.0.0.1:12345", 100);

    // The synthetic configuration intentionally lacks
    // application_response on depth_snapshot.

    let bindings = RestLimiterBindings {
        ip: Some(RestBudgetBinding::new(
            "spot_rest",
            BudgetScope::Egress("test".into()),
        )),
        ..Default::default()
    };

    let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

    let limiter = RateLimitManager::new(compiled.policies.clone(), 32).unwrap();

    let compiled_profiles = BTreeMap::from([("binance_spot".to_string(), compiled)]);

    let rest_profiles = BTreeMap::from([("binance_spot".to_string(), config.rest.clone())]);

    let feedback_policies = BTreeMap::from([(
        "binance_spot".to_string(),
        RestFactoryFeedbackPolicy::new(Vec::new(), Duration::from_millis(100)).unwrap(),
    )]);

    let result = ExchangeRestClientFactory::build(
        limiter,
        compiled_profiles,
        &rest_profiles,
        feedback_policies,
        RestClientConfig {
            max_concurrent_requests: 8,
            max_response_bytes: 1024 * 1024,
        },
    );

    let error = match result {
        Ok(_) => panic!("factory accepted an endpoint without application_response"),
        Err(error) => error,
    };

    assert!(error.contains("missing application_response"), "{error}");
}

// ============================================================
// FACTORY REJECTS MISSING FEEDBACK POLICY
// ============================================================

#[test]
fn factory_rejects_missing_feedback_policy() {
    let mut config = exchange_config("http://127.0.0.1:12345", 100);

    let application: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "raw"
"#,
    )
    .unwrap();

    config
        .rest
        .endpoints
        .get_mut("depth_snapshot")
        .unwrap()
        .application_response = Some(application);

    let bindings = RestLimiterBindings {
        ip: Some(RestBudgetBinding::new(
            "spot_rest",
            BudgetScope::Egress("test".into()),
        )),
        ..Default::default()
    };

    let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

    let limiter = RateLimitManager::new(compiled.policies.clone(), 32).unwrap();

    let compiled_profiles = BTreeMap::from([("binance_spot".to_string(), compiled)]);

    let rest_profiles = BTreeMap::from([("binance_spot".to_string(), config.rest.clone())]);

    // Deliberately omit the feedback policy.
    let feedback_policies = BTreeMap::new();

    let result = ExchangeRestClientFactory::build(
        limiter,
        compiled_profiles,
        &rest_profiles,
        feedback_policies,
        RestClientConfig {
            max_concurrent_requests: 8,
            max_response_bytes: 1024 * 1024,
        },
    );

    let error = match result {
        Ok(_) => panic!("factory accepted a profile without feedback configuration"),
        Err(error) => error,
    };

    assert!(error.contains("missing REST feedback policy"), "{error}");
}

// ============================================================
// FACTORY-CONSTRUCTED TEST CLIENT
// ============================================================

fn factory_test_client(
    server: &MockExchange,
    capacity: u64,
) -> (ExchangeConfig, ExchangeRestClient, RateLimitManager) {
    let mut config = exchange_config(server.base_url(), capacity);

    // Explicitly declare a raw application response.
    //
    // These tests exercise HTTP retry and throttling,
    // rather than application-envelope parsing.
    let application: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "raw"
"#,
    )
    .unwrap();

    config
        .rest
        .endpoints
        .get_mut("depth_snapshot")
        .unwrap()
        .application_response = Some(application);

    config.validate().unwrap();

    // ========================================================
    // COMPILE EXISTING REST LIMITER
    // ========================================================

    let bindings = RestLimiterBindings {
        ip: Some(RestBudgetBinding::new(
            "spot_rest",
            BudgetScope::Egress("test".into()),
        )),
        ..Default::default()
    };

    let compiled = RestLimiterCompiler::compile(&config, &bindings).unwrap();

    let limiter = RateLimitManager::new(compiled.policies.clone(), 32).unwrap();

    let observer = limiter.clone();

    // ========================================================
    // CONSTRUCT FACTORY INPUTS
    // ========================================================

    let compiled_profiles = BTreeMap::from([("binance_spot".to_string(), compiled)]);

    let rest_profiles = BTreeMap::from([("binance_spot".to_string(), config.rest.clone())]);

    let feedback_policies = BTreeMap::from([(
        "binance_spot".to_string(),
        RestFactoryFeedbackPolicy::new(Vec::new(), Duration::from_millis(100)).unwrap(),
    )]);

    // ========================================================
    // CONSTRUCT THROUGH ACTUAL FACTORY
    // ========================================================

    let client = ExchangeRestClientFactory::build(
        limiter,
        compiled_profiles,
        &rest_profiles,
        feedback_policies,
        RestClientConfig {
            max_concurrent_requests: 8,
            max_response_bytes: 1024 * 1024,
        },
    )
    .unwrap();

    (config, client, observer)
}

// ============================================================
// FACTORY CLIENT — AUTOMATIC RETRIES
// ============================================================

#[tokio::test]
async fn factory_client_retries_transient_http_errors() {
    let server = MockExchange::start(vec![
        MockResponse::error(503, r#"{"msg":"Temporarily unavailable"}"#),
        MockResponse::error(503, r#"{"msg":"Temporarily unavailable"}"#),
        MockResponse::ok(r#"{"lastUpdateId":999,"bids":[],"asks":[]}"#),
    ]);

    let (config, client, limiter) = factory_test_client(&server, 100);

    // ========================================================
    // RESOLVE REQUEST
    // ========================================================

    let request =
        RestRequest::new("binance_spot", "depth_snapshot").with_parameter("symbol", "BTCUSDT");

    let endpoint = config.endpoint("depth_snapshot").unwrap();

    let resolved = RestRequestResolver::resolve_endpoint(&config, endpoint, &request).unwrap();

    // ========================================================
    // EXECUTE WITH RETRIES
    // ========================================================

    let response = client
        .execute_with_retry(&resolved, retry_policy(3, Duration::from_secs(3)))
        .await
        .unwrap();

    // ========================================================
    // VERIFY RESPONSE
    // ========================================================

    assert_eq!(response.status(), 200);

    assert_eq!(response.attempts(), 3);

    assert_eq!(response.body()["lastUpdateId"], 999,);

    // Exactly three HTTP requests were sent.
    assert_eq!(server.request_count(), 3);

    // ========================================================
    // VERIFY SHARED LIMITER ACCOUNTING
    // ========================================================

    let snapshot = limiter.snapshot();

    assert_eq!(snapshot.budgets.len(), 1);

    assert_eq!(snapshot.budgets[0].locally_used, 3,);

    println!(
        "PASS: factory client retried 503 twice, \
         succeeded on attempt 3, and charged quota \
         for all 3 attempts"
    );
}

// ============================================================
// FACTORY CLIENT — EXCHANGE COOLDOWN
// ============================================================

#[tokio::test]
async fn factory_client_enforces_http_429_cooldown() {
    let server = MockExchange::start(vec![
        MockResponse::throttled("1"),
        MockResponse::ok(r#"{"lastUpdateId":456,"bids":[],"asks":[]}"#),
    ]);

    let (config, client, limiter) = factory_test_client(&server, 100);

    // ========================================================
    // RESOLVE REQUEST
    // ========================================================

    let request =
        RestRequest::new("binance_spot", "depth_snapshot").with_parameter("symbol", "BTCUSDT");

    let endpoint = config.endpoint("depth_snapshot").unwrap();

    let resolved = RestRequestResolver::resolve_endpoint(&config, endpoint, &request).unwrap();

    // ========================================================
    // EXECUTE THROUGH FACTORY CLIENT
    // ========================================================

    let response = client
        .execute_with_retry(&resolved, retry_policy(2, Duration::from_secs(4)))
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    assert_eq!(response.attempts(), 2);

    assert_eq!(response.body()["lastUpdateId"], 456,);

    assert_eq!(server.request_count(), 2);

    // ========================================================
    // VERIFY OBSERVED REQUEST TIMING
    // ========================================================

    let requests = server.requests();

    let elapsed = requests[1]
        .received_at
        .duration_since(requests[0].received_at);

    // The configured retry backoff is only 10 ms.
    //
    // The server requested a 1-second cooldown.
    //
    // Therefore, the limiter must delay the second
    // request substantially beyond the retry backoff.

    assert!(
        elapsed >= Duration::from_millis(900),
        "factory client bypassed exchange cooldown: \
         {elapsed:?}",
    );

    // ========================================================
    // VERIFY SHARED QUOTA ACCOUNTING
    // ========================================================

    let snapshot = limiter.snapshot();

    assert_eq!(snapshot.budgets.len(), 1);

    assert_eq!(snapshot.budgets[0].locally_used, 2,);

    println!(
        "PASS: factory client respected HTTP 429 \
         cooldown ({elapsed:?})"
    );
}
// ============================================================
// PRODUCTION BOOTSTRAP — SHARED LIMITER INTEGRATION
// ============================================================

#[tokio::test]
async fn production_bootstrap_reuses_shared_limiter() {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    // ========================================================
    // MOCK EXCHANGE
    // ========================================================

    let server = MockExchange::start(vec![MockResponse::ok(
        r#"{
                "lastUpdateId": 123456,
                "bids": [],
                "asks": []
            }"#,
    )]);

    // ========================================================
    // VALIDATED CONFIGURATION
    // ========================================================

    let mut config = exchange_config(server.base_url(), 100);

    // Explicitly configure a raw JSON response.
    let application: RestApplicationResponseConfig = toml::from_str(
        r#"
strategy = "raw"
"#,
    )
    .unwrap();

    config
        .rest
        .endpoints
        .get_mut("depth_snapshot")
        .unwrap()
        .application_response = Some(application);

    config.validate().unwrap();

    // ========================================================
    // LOAD THROUGH EXCHANGE CONFIG STORE
    // ========================================================

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let directory = std::env::temp_dir().join(format!(
        "marketforge-rest-bootstrap-{}-{unique}",
        std::process::id(),
    ));

    fs::create_dir_all(&directory).unwrap();

    let path = directory.join("binance_spot.toml");

    fs::write(&path, toml::to_string(&config).unwrap()).unwrap();

    let configs = ExchangeConfigStore::load_dir(&directory).unwrap();

    fs::remove_dir_all(&directory).unwrap();

    // Use the configuration owned by the store.
    //
    // Do not compile quotas from the original mutable
    // configuration variable.
    let exchange = configs.get("binance_spot").unwrap();

    // ========================================================
    // REST LIMITER BINDINGS
    // ========================================================

    let rest_bindings = RestLimiterBindings {
        ip: Some(RestBudgetBinding::new(
            "spot_rest",
            BudgetScope::Egress("bootstrap_test".into()),
        )),
        ..Default::default()
    };

    // This synthetic profile has no WebSocket quota
    // declarations requiring external bindings.
    let websocket_bindings = WebSocketLimiterBindings::default();

    // ========================================================
    // COMPILE THROUGH PRODUCTION LIMITER FACTORY
    // ========================================================

    let inputs = [LimiterProfileInput::new(
        exchange,
        &rest_bindings,
        &websocket_bindings,
    )];

    let limiters = ExchangeLimiterFactory::build(&inputs, 32).unwrap();

    assert_eq!(limiters.profile_count(), 1);

    assert_eq!(limiters.shared_budget_count(), 1);

    // This handle shares the same underlying limiter
    // state used by every transport.
    let observer = limiters.manager();

    // ========================================================
    // REST FEEDBACK POLICY
    // ========================================================

    let feedback_policies = BTreeMap::from([(
        "binance_spot".to_string(),
        RestFactoryFeedbackPolicy::new(Vec::new(), Duration::from_millis(100)).unwrap(),
    )]);

    // ========================================================
    // PRODUCTION REST BOOTSTRAP
    // ========================================================

    let service = ExchangeRestBootstrap::build(
        &configs,
        &limiters,
        feedback_policies,
        RestClientConfig {
            max_concurrent_requests: 8,
            max_response_bytes: 1024 * 1024,
        },
    )
    .unwrap();

    // ========================================================
    // EXECUTE THROUGH PUBLIC REST SERVICE
    // ========================================================

    let response = service
        .execute(
            RestRequest::new("binance_spot", "depth_snapshot").with_parameter("symbol", "BTCUSDT"),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), 200);

    assert_eq!(response.body()["lastUpdateId"], 123456,);

    assert_eq!(response.attempts(), 1);

    // ========================================================
    // VERIFY REQUEST REACHED MOCK SERVER
    // ========================================================

    assert_eq!(server.request_count(), 1);

    let requests = server.requests();

    assert!(requests[0].request_line.starts_with("GET /api/v3/depth?"),);

    assert!(requests[0].request_line.contains("symbol=BTCUSDT"),);

    assert!(requests[0].request_line.contains("limit=100"),);

    // ========================================================
    // VERIFY SHARED RATE-LIMIT STATE
    // ========================================================

    let snapshot = observer.snapshot();

    assert_eq!(snapshot.budgets.len(), 1);

    assert_eq!(snapshot.budgets[0].locally_used, 1,);

    // The manager retrieved again from ExchangeLimiters
    // must observe the exact same consumed capacity.
    let second_snapshot = limiters.manager().snapshot();

    assert_eq!(second_snapshot.budgets[0].locally_used, 1,);

    // ========================================================
    // REPORT
    // ========================================================

    println!();
    println!("==========================================");
    println!("MARKETFORGE PRODUCTION REST BOOTSTRAP");
    println!("==========================================");
    println!("Configuration store:       PASS");
    println!("ExchangeLimiterFactory:    PASS");
    println!("Shared RateLimitManager:   PASS");
    println!("ExchangeRestBootstrap:     PASS");
    println!("ExchangeRestService:       PASS");
    println!("HTTP execution:            PASS");
    println!("Shared quota accounting:   PASS");
    println!("Requests executed:         {}", server.request_count());
}
