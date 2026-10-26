#![cfg(feature = "live")]

use marketforge_engine::live::infrastructure::rest::{
    ExchangeRestBootstrap, RestClientConfig, RestFactoryFeedbackPolicy, RestRequest,
};
use marketforge_engine::live::{
    config::{ExchangeConfig, ExchangeConfigStore, ExchangeId},
    infrastructure::limiter::{
        BudgetId, BudgetScope, WindowQuota,
        factory::{
            ExchangeLimiterFactory, ExchangeLimiters, LimiterProfileInput, ResolvedRestBudget,
            RestBudgetBinding, RestLimiterBindings, RestQuotaGrouping, WebSocketBudgetBinding,
            WebSocketLimiterBindings,
        },
    },
};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};

// ============================================================
// CONFIGURATION LOADING
// ============================================================

fn config_directory() -> PathBuf {
    crate::support::data(".config/live/exchanges")
}

fn load_configs() -> ExchangeConfigStore {
    ExchangeConfigStore::load_dir(config_directory()).expect("exchange configurations must load")
}

// ============================================================
// QUOTA IDENTITY
// ============================================================

/// These tests use one known local egress group.
///
/// Production must resolve the actual deployment's
/// external quota-sharing identity.
fn egress() -> BudgetScope {
    BudgetScope::Egress("primary".into())
}

/// REST service identity.
///
/// The service determines which market profiles may
/// share otherwise identical rate-limit budgets.
fn rest_service(config: &ExchangeConfig) -> String {
    match config.exchange.id {
        ExchangeId::Binance => {
            format!("binance_{}_rest", config.exchange.market.as_str())
        }

        ExchangeId::Bybit => "bybit_v5_rest".into(),

        ExchangeId::Okx => "okx_v5_rest".into(),

        ExchangeId::Bitget => "bitget_uta_v3_rest".into(),

        ExchangeId::Gateio => match config.exchange.market.as_str() {
            "spot" => "gateio_spot_rest".into(),
            _ => "gateio_futures_rest".into(),
        },
    }
}

/// WebSocket service identity.
///
/// Connection-attempt quotas may be shared between
/// market profiles when they use the same exchange
/// service and egress identity.
fn websocket_service(config: &ExchangeConfig) -> String {
    match config.exchange.id {
        ExchangeId::Binance => {
            format!("binance_{}_websocket", config.exchange.market.as_str())
        }

        ExchangeId::Bybit => "bybit_public_websocket".into(),

        ExchangeId::Okx => "okx_public_websocket".into(),

        ExchangeId::Bitget => "bitget_uta_v3_websocket".into(),

        ExchangeId::Gateio => {
            format!("gateio_{}_websocket", config.exchange.market.as_str())
        }
    }
}

// ============================================================
// REST BINDINGS
// ============================================================

fn rest_bindings(config: &ExchangeConfig) -> RestLimiterBindings {
    let service = rest_service(config);

    let shared = RestBudgetBinding::new(service.clone(), egress());

    // Endpoint/IP quota groups use native HTTP endpoints.
    //
    // Shared global/IP quotas are represented separately
    // through the `global` and `ip` bindings.
    let endpoint = shared.clone().with_grouping(RestQuotaGrouping::PerEndpoint);

    let endpoint_policies = config
        .rest
        .rate_limit_policies
        .keys()
        .map(|name| (name.clone(), endpoint.clone()))
        .collect::<BTreeMap<_, _>>();

    // Explicit reference quotas for the configured
    // Binance REST API families.
    //
    // These values are supplied by this test.
    // The factory itself does not discover them.
    let binance_capacity = match config.exchange.id {
        ExchangeId::Binance => match config.exchange.market.as_str() {
            "spot" => Some(6_000),
            "linear" | "inverse" => Some(2_400),
            _ => None,
        },

        _ => None,
    };

    let shared_weight = binance_capacity.map(|capacity| {
        let id = BudgetId::new(config.exchange.id, service, "request_weight", egress()).unwrap();

        ResolvedRestBudget {
            id,
            quota: WindowQuota::new(capacity, Duration::from_secs(60)).unwrap(),
        }
    });

    RestLimiterBindings {
        ip: Some(shared.clone()),
        global: Some(shared),
        endpoint_policies,
        shared_weight,
    }
}

// ============================================================
// WEBSOCKET BINDINGS
// ============================================================

fn websocket_bindings(config: &ExchangeConfig) -> WebSocketLimiterBindings {
    let service = websocket_service(config);

    let shared = WebSocketBudgetBinding::new(service.clone(), egress());

    WebSocketLimiterBindings {
        connection_attempts: Some(shared.clone()),

        connection_attempts_per_second: Some(shared),

        connection_service: Some(service),
    }
}

// ============================================================
// FACTORY CONSTRUCTION
// ============================================================

/// Build limiters using selected real exchange profiles.
///
/// Binding values are retained while the factory
/// borrows them during compilation.
///
/// The returned ExchangeLimiters owns its compiled
/// catalogs and shared manager.
fn build_profiles(store: &ExchangeConfigStore, names: &[&str]) -> ExchangeLimiters {
    let bindings = names
        .iter()
        .map(|name| {
            let config = store
                .get(name)
                .unwrap_or_else(|| panic!("missing profile: {name}"));

            (rest_bindings(config), websocket_bindings(config))
        })
        .collect::<Vec<_>>();

    let inputs = names
        .iter()
        .zip(bindings.iter())
        .map(|(name, (rest, websocket))| {
            let config = store.get(name).unwrap();

            LimiterProfileInput::new(config, rest, websocket)
        })
        .collect::<Vec<_>>();

    ExchangeLimiterFactory::build(&inputs, 1_024).unwrap_or_else(|error| {
        if let Some(errors) = error.validation_errors() {
            eprintln!("\nRATE-LIMIT VALIDATION ERRORS:");

            for issue in errors {
                eprintln!("  {issue}");
            }
        }

        panic!("rate-limit factory failed: {error}");
    })
}

// ============================================================
// TEST 1 — ALL EXCHANGE PROFILES LOAD
// ============================================================

// ============================================================
// TEST 2 — COMPLETE FACTORY INITIALIZATION
// ============================================================

// ============================================================
// TEST 3 — BITGET UTA V3 REST SHARING
// ============================================================

// ============================================================
// TEST 4 — BITGET SHARED BUDGET ENFORCEMENT
// ============================================================

// ============================================================
// TEST 5 — BYBIT SHARED HTTP IP BUDGET
// ============================================================

// ============================================================
// TEST 6 — BINANCE SEPARATE API SERVICE BUDGETS
// ============================================================

// ============================================================
// TEST 7 — OKX SHARED NATIVE ENDPOINT BUDGETS
// ============================================================

// ============================================================
// TEST 8 — WEBSOCKET CONNECTION ATTEMPT SHARING
// ============================================================

// ============================================================
// TEST 9 — PHYSICAL WEBSOCKET CONNECTION ISOLATION
// ============================================================

// ============================================================
// PRODUCTION REST BOOTSTRAP — ALL 15 REAL PROFILES
// ============================================================

// ============================================================
// PRODUCTION REST SERVICE — FIRST REAL EXCHANGE REQUEST
// ============================================================

/// Verify real HTTPS execution through the complete
/// MarketForge REST infrastructure.
///
/// This test is deliberately ignored by default because
/// it contacts a real public exchange.
///
/// No authentication.
/// No trading.
/// No automatic retries.
/// No configuration modifications.
#[tokio::test]
#[ignore = "explicit real-exchange network test"]
async fn production_rest_service_executes_live_bybit_trades() {
    // ========================================================
    // LOAD ALL 15 REAL CONFIGURATIONS
    // ========================================================

    let configs = load_configs();

    assert_eq!(configs.len(), 15);

    let names = configs.names().collect::<Vec<_>>();

    // ========================================================
    // INITIALIZE SHARED LIMITER INFRASTRUCTURE
    // ========================================================

    // This is the existing 15-profile factory helper.
    //
    // The resulting manager includes REST and shared
    // WebSocket budgets.

    let limiters = build_profiles(&configs, &names);

    assert_eq!(limiters.profile_count(), 15);

    let observer = limiters.manager();

    // ========================================================
    // RECORD INITIAL RATE-LIMIT STATE
    // ========================================================

    let before = observer.snapshot();

    let initial_usage: u64 = before
        .budgets
        .iter()
        .map(|budget| budget.locally_used)
        .sum();

    // ========================================================
    // BUILD FEEDBACK POLICIES
    // ========================================================

    // These are test settings, not production-verified
    // usage-header mappings.
    //
    // The exercise uses the real quota compiler and
    // shared manager but does not certify the correctness
    // of all production quota bindings.

    let feedback = names
        .iter()
        .map(|name| {
            (
                (*name).to_owned(),
                RestFactoryFeedbackPolicy::new(Vec::new(), Duration::from_secs(5)).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    // ========================================================
    // BOOTSTRAP COMPLETE REST SERVICE
    // ========================================================

    let service = ExchangeRestBootstrap::build(
        &configs,
        &limiters,
        feedback,
        RestClientConfig {
            max_concurrent_requests: 8,
            max_response_bytes: 2 * 1024 * 1024,
        },
    )
    .expect("REST production bootstrap failed");

    // ========================================================
    // CONSTRUCT LOGICAL REQUEST
    // ========================================================

    let request = RestRequest::new("bybit_spot", "recent_trades")
        .with_parameter("symbol", "BTCUSDT")
        // Explicitly request a small response.
        .with_parameter("limit", "10");

    // ========================================================
    // EXECUTE THROUGH MARKETFORGE
    // ========================================================

    println!();
    println!("==========================================");
    println!("MARKETFORGE PRODUCTION REST LIVE TEST");
    println!("==========================================");

    println!("Exchange: Bybit");
    println!("Market:   Spot");
    println!("Endpoint: recent_trades");

    let response = tokio::time::timeout(Duration::from_secs(20), service.execute(request))
        .await
        .expect("live REST request exceeded test deadline")
        .expect("MarketForge REST execution failed");

    // ========================================================
    // VERIFY RESPONSE
    // ========================================================

    assert_eq!(response.status(), 200);

    assert_eq!(response.attempts(), 1);

    let body = response.body();

    assert_eq!(
        body["retCode"], 0,
        "Bybit reported an application-level failure",
    );

    let trades = body["result"]["list"]
        .as_array()
        .expect("missing result.list trade array");

    assert!(!trades.is_empty(), "Bybit returned no trades",);

    assert!(
        trades.len() <= 10,
        "response exceeded requested trade limit",
    );

    // ========================================================
    // VERIFY REPRESENTATIVE TRADE DATA
    // ========================================================

    let first = &trades[0];

    assert!(
        first["execId"].as_str().is_some(),
        "trade is missing execId",
    );

    assert!(first["price"].as_str().is_some(), "trade is missing price",);

    assert!(first["size"].as_str().is_some(), "trade is missing size",);

    assert!(
        first["time"].as_str().is_some(),
        "trade is missing timestamp",
    );

    // ========================================================
    // VERIFY RATE-LIMIT ACCOUNTING
    // ========================================================

    let after = observer.snapshot();

    let final_usage: u64 = after.budgets.iter().map(|budget| budget.locally_used).sum();

    assert!(
        final_usage > initial_usage,
        "REST execution did not consume any shared quota",
    );

    // ========================================================
    // VERIFY THE SAME MANAGER IS STILL SHARED
    // ========================================================

    let second = limiters.manager().snapshot();

    let second_usage: u64 = second
        .budgets
        .iter()
        .map(|budget| budget.locally_used)
        .sum();

    assert_eq!(
        final_usage, second_usage,
        "REST and ExchangeLimiters observe different state",
    );

    // ========================================================
    // REPORT
    // ========================================================

    println!();
    println!("HTTP status:          {}", response.status());
    println!("Trade count:          {}", trades.len());
    println!("Response bytes:       {}", response.body_bytes());
    println!("HTTP attempts:        {}", response.attempts());
    println!("Quota usage before:   {initial_usage}");
    println!("Quota usage after:    {final_usage}");

    println!();
    println!(
        "Sample trade:\n{}",
        serde_json::to_string_pretty(first).unwrap(),
    );

    println!();
    println!("15-profile bootstrap:       PASS");
    println!("Production REST service:    PASS");
    println!("Live HTTPS execution:       PASS");
    println!("Application validation:     PASS");
    println!("Native trade structure:     PASS");
    println!("Shared limiter accounting:  PASS");
}

// ============================================================
// LIVE REST ACCEPTANCE — NATIVE TEST INSTRUMENTS
// ============================================================

fn live_rest_symbol(profile: &str) -> &'static str {
    match profile {
        "binance_spot" | "binance_linear" => "BTCUSDT",
        "binance_inverse" => "BTCUSD_PERP",

        "bybit_spot" | "bybit_linear" => "BTCUSDT",
        "bybit_inverse" => "BTCUSD",

        "okx_spot" => "BTC-USDT",
        "okx_linear" => "BTC-USDT-SWAP",
        "okx_inverse" => "BTC-USD-SWAP",

        "bitget_spot" | "bitget_linear" => "BTCUSDT",
        "bitget_inverse" => "BTCUSD_CM",

        "gateio_spot" | "gateio_linear" => "BTC_USDT",
        "gateio_inverse" => "BTC_USD",

        other => panic!("unknown live REST profile: {other}"),
    }
}

fn live_rest_request(
    profile: &str,
    endpoint_name: &str,
    endpoint: &marketforge_engine::live::config::exchange::RestEndpoint,
) -> RestRequest {
    let symbol = live_rest_symbol(profile);

    let mut request = RestRequest::new(profile, endpoint_name);

    let needs_symbol = endpoint
        .parameters
        .iter()
        .any(|parameter| parameter == "symbol")
        || endpoint
            .contract
            .as_deref()
            .is_some_and(|value| value.contains("{symbol}"))
        || endpoint
            .currency_pair
            .as_deref()
            .is_some_and(|value| value.contains("{symbol}"));

    if needs_symbol {
        request = request.with_parameter("symbol", symbol);
    }

    if endpoint
        .parameters
        .iter()
        .any(|parameter| parameter == "instId")
    {
        request = request.with_parameter("instId", symbol);
    }

    if profile == "bitget_linear" {
        request = request.with_parameter("native_rest_category", "USDT-FUTURES");
    }

    if profile == "gateio_linear" {
        request = request.with_parameter("native_settlement", "usdt");
    }

    request
}

// ============================================================
// PRODUCTION REST SERVICE — BULK LIVE ACCEPTANCE
// ============================================================

/// Exercise real public REST endpoints through the complete
/// MarketForge infrastructure.
///
/// This test is explicitly opt-in.
///
/// All requests use the existing shared RateLimitManager.
/// No independent reqwest client is constructed.
///
/// No authentication.
/// No trading.
/// No automatic retries.
///
/// Successful native JSON responses are saved as fixtures.
#[tokio::test]
#[ignore = "explicit live exchange acceptance test"]
async fn production_rest_service_bulk_live_acceptance() {
    use std::{env, fs};

    use marketforge_engine::live::infrastructure::rest::{
        ExchangeRestBootstrap, RestClientConfig, RestFactoryFeedbackPolicy,
    };

    // ========================================================
    // EXPLICIT LIVE TEST CONTROLS
    // ========================================================

    let profile_filter = env::var("LIVE_REST_PROFILE").ok();

    // Running all profiles requires explicit opt-in.
    //
    // A profile filter also authorizes testing that
    // specific profile.

    let run_all = env::var("LIVE_REST_ALL").ok().as_deref() == Some("1");

    assert!(
        run_all || profile_filter.is_some(),
        "set LIVE_REST_PROFILE=<profile> or LIVE_REST_ALL=1"
    );

    let max_requests = env::var("LIVE_REST_MAX")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("LIVE_REST_MAX must be an integer")
        })
        .unwrap_or(usize::MAX);

    assert!(max_requests > 0, "LIVE_REST_MAX must be positive");

    // Deliberately conservative test pacing.
    //
    // Production admission is handled separately
    // by RateLimitManager.

    const DELAY_BETWEEN_REQUESTS: Duration = Duration::from_secs(2);

    const MAX_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

    // ========================================================
    // LOAD ALL 15 REAL CONFIGURATIONS
    // ========================================================

    let configs = load_configs();

    assert_eq!(configs.len(), 15);

    let names = configs.names().collect::<Vec<_>>();

    // ========================================================
    // INITIALIZE SHARED LIMITER INFRASTRUCTURE
    // ========================================================

    let limiters = build_profiles(&configs, &names);

    assert_eq!(limiters.profile_count(), 15,);

    let observer = limiters.manager();

    let budget_count_before = observer.budget_count();

    // ========================================================
    // FEEDBACK POLICIES
    // ========================================================

    // These values are test inputs.
    //
    // Usage-header reconciliation has not yet been
    // configured for every exchange.
    //
    // The factory still applies rate-limit admission
    // using the compiled quota policies.

    let feedback_policies = names
        .iter()
        .map(|name| {
            (
                (*name).to_owned(),
                RestFactoryFeedbackPolicy::new(Vec::new(), Duration::from_secs(5)).unwrap(),
            )
        })
        .collect::<BTreeMap<_, _>>();

    // ========================================================
    // BOOTSTRAP PRODUCTION REST SERVICE
    // ========================================================

    let service = ExchangeRestBootstrap::build(
        &configs,
        &limiters,
        feedback_policies,
        RestClientConfig {
            max_concurrent_requests: 1,
            max_response_bytes: MAX_RESPONSE_BYTES,
        },
    )
    .expect("production REST bootstrap failed");

    // ========================================================
    // FIXTURE DIRECTORY
    // ========================================================

    // Keep these separate from fixtures collected
    // through standalone reqwest.
    //
    // This lets us compare both execution paths.

    let fixtures_dir =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/live-rest-service-fixtures");

    fs::create_dir_all(&fixtures_dir).expect("cannot create fixture directory");

    // ========================================================
    // COUNTERS
    // ========================================================

    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut skipped_auth = 0usize;

    let mut attempted = 0usize;

    let mut failures = Vec::<String>::new();

    let mut stopped_on_throttle = false;

    println!();
    println!("================================================");
    println!("MARKETFORGE PRODUCTION REST — LIVE BULK TEST");
    println!("================================================");
    println!("Profiles initialized: {}", configs.len());
    println!("Shared budgets:       {budget_count_before}");
    println!("Profile filter:       {:?}", profile_filter);
    println!("Request limit:        {max_requests}");
    println!();

    // ========================================================
    // ITERATE ALL CONFIGURED PROFILES
    // ========================================================

    'profiles: for profile in &names {
        if profile_filter
            .as_deref()
            .is_some_and(|filter| filter != *profile)
        {
            continue;
        }

        let config = configs.get(profile).expect("missing configured profile");

        println!();
        println!("PROFILE: {profile}");

        // ====================================================
        // ITERATE NATIVE REST ENDPOINTS
        // ====================================================

        for (endpoint_name, endpoint) in &config.rest.endpoints {
            let authentication = endpoint.authentication.as_deref().unwrap_or("none");

            // ================================================
            // SKIP AUTHENTICATED ENDPOINTS
            // ================================================

            if authentication != "none" {
                skipped_auth += 1;

                println!(
                    "  SKIP_AUTH  {endpoint_name:<22} \
                     auth={authentication}"
                );

                continue;
            }

            if attempted >= max_requests {
                break 'profiles;
            }

            // ================================================
            // CONSERVATIVE REQUEST PACING
            // ================================================

            if attempted > 0 {
                tokio::time::sleep(DELAY_BETWEEN_REQUESTS).await;
            }

            // ================================================
            // CONSTRUCT LOGICAL REQUEST
            // ================================================

            let request = live_rest_request(profile, endpoint_name, endpoint);

            attempted += 1;

            // ================================================
            // EXECUTE THROUGH MARKETFORGE
            // ================================================

            let result =
                tokio::time::timeout(Duration::from_secs(20), service.execute(request)).await;

            match result {
                // ============================================
                // SUCCESSFUL EXCHANGE RESPONSE
                // ============================================
                Ok(Ok(response)) => {
                    // Application-envelope validation
                    // has already been performed by
                    // ExchangeRestClient.

                    if response.status() != 200 {
                        failed += 1;

                        let error = format!(
                            "{profile}/{endpoint_name}: \
                             unexpected HTTP {}",
                            response.status(),
                        );

                        println!("  FAIL       {error}");

                        failures.push(error);

                        continue;
                    }

                    // ========================================
                    // SAVE NATIVE JSON FIXTURE
                    // ========================================

                    let filename = format!("{profile}__{endpoint_name}.json");

                    let path = fixtures_dir.join(filename);

                    let bytes = serde_json::to_vec_pretty(response.body())
                        .expect("cannot serialize response");

                    fs::write(&path, bytes).expect("cannot save REST fixture");

                    passed += 1;

                    println!(
                        "  PASS       {endpoint_name:<22} \
                         bytes={} attempts={}",
                        response.body_bytes(),
                        response.attempts(),
                    );
                }

                // ============================================
                // MARKETFORGE REST ERROR
                // ============================================
                Ok(Err(error)) => {
                    failed += 1;

                    let detail = format!("{profile}/{endpoint_name}: {error}");

                    println!("  FAIL       {detail}");

                    failures.push(detail);

                    // Stop on rate-limit feedback.
                    //
                    // Do not keep probing exchanges
                    // after an observed throttle.

                    if error.kind()
                        == marketforge_engine::live::
                            infrastructure::rest::
                            RestErrorKind::RateLimited
                        || error.http_status() == Some(429)
                    {
                        stopped_on_throttle = true;

                        break 'profiles;
                    }
                }

                // ============================================
                // TEST DEADLINE EXCEEDED
                // ============================================
                Err(_) => {
                    failed += 1;

                    let detail = format!(
                        "{profile}/{endpoint_name}: \
                         exceeded 20-second test deadline"
                    );

                    println!("  FAIL       {detail}");

                    failures.push(detail);
                }
            }
        }
    }

    // ========================================================
    // VERIFY SHARED LIMITER OWNERSHIP
    // ========================================================

    let snapshot = observer.snapshot();

    assert_eq!(
        observer.budget_count(),
        budget_count_before,
        "REST execution unexpectedly changed budget catalog",
    );

    assert_eq!(snapshot.budgets.len(), budget_count_before,);

    let used_budgets = snapshot
        .budgets
        .iter()
        .filter(|budget| budget.locally_used > 0)
        .count();

    // Any successful HTTP request should have passed
    // through at least one registered limiter budget.

    if passed > 0 {
        assert!(
            used_budgets > 0,
            "successful REST requests did not charge \
             any shared limiter budget",
        );
    }

    // ========================================================
    // REPORT
    // ========================================================

    println!();
    println!("================================================");
    println!("PRODUCTION REST BULK ACCEPTANCE REPORT");
    println!("================================================");

    println!("Profiles initialized:  {}", configs.len());
    println!("Requests attempted:    {attempted}");
    println!("Passed:                {passed}");
    println!("Failed:                {failed}");
    println!("Authentication skips:  {skipped_auth}");
    println!("Registered budgets:    {budget_count_before}");
    println!("Budgets with usage:    {used_budgets}");

    println!("Fixtures directory:    {}", fixtures_dir.display(),);

    if stopped_on_throttle {
        println!();
        println!("STOPPED: exchange throttling detected");
    }

    if !failures.is_empty() {
        println!();
        println!("FAILURE DETAILS:");

        for failure in &failures {
            println!("  {failure}");
        }
    }

    assert_eq!(
        passed + failed,
        attempted,
        "REST acceptance accounting mismatch",
    );

    assert!(
        !stopped_on_throttle,
        "live REST test stopped after throttling",
    );

    assert!(
        failures.is_empty(),
        "{} production REST requests failed",
        failures.len(),
    );

    println!();
    println!("RESULT: PASS");
}
