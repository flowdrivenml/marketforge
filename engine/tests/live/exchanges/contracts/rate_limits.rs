#![cfg(feature = "live")]

use marketforge_engine::live::infrastructure::rest::RestRequestResolver;
use marketforge_engine::live::infrastructure::rest::{
    ExchangeRestBootstrap, RestClientConfig, RestFactoryFeedbackPolicy, RestRequest,
};
use marketforge_engine::live::{
    config::{ExchangeConfig, ExchangeConfigStore, ExchangeId},
    infrastructure::limiter::{
        BudgetId, BudgetScope, RateLimitManagerError, RequestPriority, WindowQuota,
        factory::{
            ExchangeLimiterFactory, ExchangeLimiters, LimiterProfileInput, ResolvedRestBudget,
            RestBudgetBinding, RestLimiterBindings, RestQuotaGrouping, WebSocketBudgetBinding,
            WebSocketLimiterBindings, WeightParameters,
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

fn rest_operation(
    limiters: &ExchangeLimiters,
    profile: &str,
    endpoint: &str,
) -> marketforge_engine::live::infrastructure::limiter::RateLimitedOperation {
    let parameters = if profile.starts_with("binance_") && endpoint == "depth_snapshot" {
        WeightParameters::from([("limit".to_owned(), 100)])
    } else {
        WeightParameters::new()
    };

    limiters
        .profile(profile)
        .unwrap()
        .rest
        .operation(endpoint, &parameters, RequestPriority::Normal)
        .unwrap()
}

fn budget_for_rule<'a>(
    operation: &'a marketforge_engine::live::infrastructure::limiter::RateLimitedOperation,
    rule: &str,
) -> &'a BudgetId {
    &operation
        .charges
        .iter()
        .find(|charge| charge.budget.rule == rule)
        .unwrap_or_else(|| panic!("missing budget rule: {rule}"))
        .budget
}

// ============================================================
// TEST 1 — ALL EXCHANGE PROFILES LOAD
// ============================================================

#[test]
fn loads_all_15_exchange_profiles() {
    let store = load_configs();

    assert_eq!(store.len(), 15);

    println!("\nMARKETFORGE EXCHANGE PROFILES");

    for (name, config) in store.iter() {
        println!("{name:<20} REST endpoints: {}", config.rest.endpoints.len());
    }
}

// ============================================================
// TEST 2 — COMPLETE FACTORY INITIALIZATION
// ============================================================

#[test]
fn builds_all_15_exchange_limiters() {
    let store = load_configs();

    let names = store.names().collect::<Vec<_>>();

    let limiters = build_profiles(&store, &names);

    assert_eq!(limiters.profile_count(), 15);

    println!();
    println!("MARKETFORGE LIMITER FACTORY");
    println!("===========================");
    println!("Profiles:       {}", limiters.profile_count());
    println!("Shared budgets: {}", limiters.shared_budget_count());

    for (name, profile) in limiters.profiles() {
        println!(
            "{name:<20} REST budgets: {:<3} WS budgets: {}",
            profile.rest.policies.len(),
            profile.websocket.shared.policies.len(),
        );
    }
}

// ============================================================
// TEST 3 — BITGET UTA V3 REST SHARING
// ============================================================

#[test]
fn bitget_markets_share_rest_budgets() {
    let store = load_configs();

    let limiters = build_profiles(&store, &["bitget_spot", "bitget_linear", "bitget_inverse"]);

    // Three shared REST budgets:
    //
    // 1. Global REST/IP
    // 2. GET /api/v3/market/orderbook
    // 3. GET /api/v3/market/fills
    //
    // Plus the shared WS connection-attempt budget.
    assert_eq!(limiters.shared_budget_count(), 4);

    let spot = rest_operation(&limiters, "bitget_spot", "depth_snapshot");

    let linear = rest_operation(&limiters, "bitget_linear", "depth_snapshot");

    let inverse = rest_operation(&limiters, "bitget_inverse", "depth_snapshot");

    assert_eq!(spot.charges.len(), 2);

    // All three market categories consume exactly
    // the same global and order-book budgets.
    assert_eq!(spot.charges, linear.charges);
    assert_eq!(spot.charges, inverse.charges);

    let global = budget_for_rule(&spot, "rest_global_requests");

    let orderbook = budget_for_rule(&spot, "rest_endpoint:GET:/api/v3/market/orderbook");

    assert_ne!(global, orderbook);

    let fills = rest_operation(&limiters, "bitget_inverse", "recent_trades");

    // Fills share the same global budget.
    assert_eq!(global, budget_for_rule(&fills, "rest_global_requests",));

    // But use their own endpoint budget.
    assert_ne!(
        orderbook,
        budget_for_rule(&fills, "rest_endpoint:GET:/api/v3/market/fills",)
    );
}

// ============================================================
// TEST 4 — BITGET SHARED BUDGET ENFORCEMENT
// ============================================================

#[tokio::test]
async fn bitget_snapshot_quota_is_shared_across_markets() {
    let store = load_configs();

    let limiters = build_profiles(&store, &["bitget_spot", "bitget_linear", "bitget_inverse"]);

    let manager = limiters.manager();

    let profiles = ["bitget_spot", "bitget_linear", "bitget_inverse"];

    // The configured order-book quota is 20/s.
    //
    // Consume it using requests from all three
    // market profiles.
    for index in 0..20 {
        let profile = profiles[index % profiles.len()];

        let operation = rest_operation(&limiters, profile, "depth_snapshot");

        manager.acquire(&operation).await.unwrap();
    }

    // The 21st request must not be admitted
    // within this short timeout.
    let extra = rest_operation(&limiters, "bitget_inverse", "depth_snapshot");

    let result = manager
        .acquire_with_timeout(&extra, Duration::from_millis(10))
        .await;

    assert!(matches!(result, Err(RateLimitManagerError::Timeout)));

    assert_eq!(manager.pending_count(), 0);

    // The fills endpoint has independent capacity.
    //
    // The global budget still has room, so this
    // operation must be admitted immediately.
    let fills = rest_operation(&limiters, "bitget_spot", "recent_trades");

    manager.acquire(&fills).await.unwrap();
}

// ============================================================
// TEST 5 — BYBIT SHARED HTTP IP BUDGET
// ============================================================

#[test]
fn bybit_markets_share_http_ip_budget() {
    let store = load_configs();

    let limiters = build_profiles(&store, &["bybit_spot", "bybit_linear", "bybit_inverse"]);

    let spot = rest_operation(&limiters, "bybit_spot", "depth_snapshot");

    let linear = rest_operation(&limiters, "bybit_linear", "depth_snapshot");

    let inverse = rest_operation(&limiters, "bybit_inverse", "depth_snapshot");

    assert_eq!(spot.charges.len(), 1);

    assert_eq!(spot.charges, linear.charges);
    assert_eq!(spot.charges, inverse.charges);
}

// ============================================================
// TEST 6 — BINANCE SEPARATE API SERVICE BUDGETS
// ============================================================

#[test]
fn binance_market_families_have_separate_weights() {
    let store = load_configs();

    let limiters = build_profiles(
        &store,
        &["binance_spot", "binance_linear", "binance_inverse"],
    );

    let spot = rest_operation(&limiters, "binance_spot", "depth_snapshot");

    let linear = rest_operation(&limiters, "binance_linear", "depth_snapshot");

    let inverse = rest_operation(&limiters, "binance_inverse", "depth_snapshot");

    let spot_budget = budget_for_rule(&spot, "request_weight");

    let linear_budget = budget_for_rule(&linear, "request_weight");

    let inverse_budget = budget_for_rule(&inverse, "request_weight");

    assert_ne!(spot_budget, linear_budget);
    assert_ne!(spot_budget, inverse_budget);
    assert_ne!(linear_budget, inverse_budget);
}

// ============================================================
// TEST 7 — OKX SHARED NATIVE ENDPOINT BUDGETS
// ============================================================

#[test]
fn okx_matching_endpoints_share_budgets() {
    let store = load_configs();

    let limiters = build_profiles(&store, &["okx_spot", "okx_linear", "okx_inverse"]);

    let spot = rest_operation(&limiters, "okx_spot", "depth_snapshot");

    let linear = rest_operation(&limiters, "okx_linear", "depth_snapshot");

    let inverse = rest_operation(&limiters, "okx_inverse", "depth_snapshot");

    assert_eq!(spot.charges, linear.charges);
    assert_eq!(spot.charges, inverse.charges);
}

// ============================================================
// TEST 8 — WEBSOCKET CONNECTION ATTEMPT SHARING
// ============================================================

#[test]
fn bitget_websocket_attempts_share_ip_budget() {
    let store = load_configs();

    let limiters = build_profiles(&store, &["bitget_spot", "bitget_linear", "bitget_inverse"]);

    let attempts = ["bitget_spot", "bitget_linear", "bitget_inverse"]
        .iter()
        .map(|name| {
            limiters
                .profile(name)
                .unwrap()
                .websocket
                .shared
                .connection_attempt_operation()
                .unwrap()
                .unwrap()
        })
        .collect::<Vec<_>>();

    assert_eq!(attempts[0].charges, attempts[1].charges);
    assert_eq!(attempts[0].charges, attempts[2].charges);
}

// ============================================================
// TEST 9 — PHYSICAL WEBSOCKET CONNECTION ISOLATION
// ============================================================

#[test]
fn websocket_connections_have_independent_message_budgets() {
    let store = load_configs();

    let limiters = build_profiles(&store, &["bitget_spot"]);

    let template = &limiters
        .profile("bitget_spot")
        .unwrap()
        .websocket
        .connection;

    let first = template.instantiate(1).unwrap();
    let second = template.instantiate(2).unwrap();

    let first_operation = first
        .outbound_message_operation(RequestPriority::Normal)
        .unwrap()
        .unwrap();

    let second_operation = second
        .outbound_message_operation(RequestPriority::Normal)
        .unwrap()
        .unwrap();

    assert_ne!(
        first_operation.charges[0].budget,
        second_operation.charges[0].budget,
    );

    // Separate physical connections have separate
    // outbound-message budgets.
    //
    // These are templates only; runtime registration
    // is implemented in the connection-management stage.
    assert_eq!(first.policies.len(), 2);
    assert_eq!(second.policies.len(), 2);
}

// ============================================================
// PRODUCTION REST BOOTSTRAP — ALL 15 REAL PROFILES
// ============================================================

#[test]
fn bootstraps_rest_for_all_real_exchange_profiles() {
    // ========================================================
    // LOAD ALL EXCHANGE CONFIGURATIONS
    // ========================================================

    let configs = load_configs();

    assert_eq!(configs.len(), 15, "expected 15 exchange configurations",);

    let profile_names = configs.names().collect::<Vec<_>>();

    // ========================================================
    // BUILD ONE SHARED LIMITER SUBSYSTEM
    // ========================================================

    // Reuse the existing test helper.
    //
    // It compiles REST and WebSocket limits for all
    // 15 profiles into one ExchangeLimiters instance.

    let limiters = build_profiles(&configs, &profile_names);

    assert_eq!(limiters.profile_count(), 15,);

    let budget_count = limiters.shared_budget_count();

    assert!(
        budget_count > 0,
        "no shared rate-limit budgets were registered",
    );

    // Keep a handle to the original manager.
    //
    // The REST bootstrap must reuse this manager,
    // not initialize another one.

    let original_manager = limiters.manager();

    assert_eq!(original_manager.budget_count(), budget_count,);

    // ========================================================
    // BUILD REST FEEDBACK POLICIES
    // ========================================================

    // These settings are suitable for the offline
    // bootstrap integration test.
    //
    // Empty usage bindings mean header-based quota usage
    // reconciliation is not configured by this test.
    //
    // The cooldown is a test input, not a verified
    // exchange-specific production setting.

    let feedback_policies = profile_names
        .iter()
        .map(|name| {
            let policy =
                RestFactoryFeedbackPolicy::new(Vec::new(), Duration::from_secs(5)).unwrap();

            ((*name).to_owned(), policy)
        })
        .collect::<BTreeMap<_, _>>();

    assert_eq!(feedback_policies.len(), 15,);

    // ========================================================
    // BOOTSTRAP ONE REST SERVICE
    // ========================================================

    let service = ExchangeRestBootstrap::build(
        &configs,
        &limiters,
        feedback_policies,
        RestClientConfig {
            max_concurrent_requests: 32,
            max_response_bytes: 2 * 1024 * 1024,
        },
    )
    .unwrap_or_else(|error| panic!("15-profile REST bootstrap failed: {error}"));

    // ========================================================
    // VERIFY CONFIGURATION OWNERSHIP
    // ========================================================

    assert_eq!(service.configs().len(), 15,);

    for name in &profile_names {
        assert!(
            service.configs().contains(name),
            "REST service missing profile: {name}",
        );
    }

    // ========================================================
    // VERIFY ALL COMPILED REST ENDPOINTS
    // ========================================================

    let mut endpoint_count = 0usize;

    for name in &profile_names {
        let config = configs.get(name).unwrap();

        let compiled = limiters.profile(name).unwrap();

        for endpoint_name in config.rest.endpoints.keys() {
            assert!(
                compiled.rest.endpoint(endpoint_name).is_some(),
                "{name}: missing compiled REST endpoint \
                 '{endpoint_name}'",
            );

            endpoint_count += 1;
        }
    }

    assert_eq!(endpoint_count, 37, "unexpected REST endpoint count",);

    // ========================================================
    // VERIFY ALL APPLICATION RESPONSE DECLARATIONS
    // ========================================================

    let mut raw_count = 0usize;
    let mut envelope_count = 0usize;

    for name in &profile_names {
        let config = configs.get(name).unwrap();

        for (endpoint_name, endpoint) in &config.rest.endpoints {
            let application = endpoint.application_response.as_ref().unwrap_or_else(|| {
                panic!(
                    "{name}/{endpoint_name}: \
                         missing application response strategy"
                )
            });

            application.validate().unwrap_or_else(|error| {
                panic!(
                    "{name}/{endpoint_name}: \
                     invalid application response: {error}"
                )
            });

            use marketforge_engine::live::config::exchange::RestApplicationResponseStrategy;

            match application.strategy {
                RestApplicationResponseStrategy::Raw => {
                    raw_count += 1;
                }

                RestApplicationResponseStrategy::Envelope => {
                    envelope_count += 1;
                }
            }
        }
    }

    assert_eq!(raw_count + envelope_count, 37,);

    // ========================================================
    // VERIFY PUBLIC REQUEST RESOLUTION
    // ========================================================

    // This stage verifies the bootstrap itself.
    //
    // The separate live_rest_resolver_bulk test already
    // verifies native symbols, required parameters,
    // URL templates, and all 35 public requests.
    //
    // Here, verify that the service accepts the same
    // configuration store as the production resolver.

    let resolver = RestRequestResolver::new(service.configs());

    let request =
        RestRequest::new("bybit_spot", "recent_trades").with_parameter("symbol", "BTCUSDT");

    let resolved = resolver.resolve(&request).unwrap();

    assert!(resolved.url().starts_with("https://"),);

    assert_eq!(
        resolved.query().get("category").map(String::as_str),
        Some("spot"),
    );

    // ========================================================
    // VERIFY NO LIMITER STATE WAS REPLACED
    // ========================================================

    // Bootstrap should not register or consume any quota.
    //
    // The existing manager must retain the same budget
    // catalog and remain unused after initialization.

    assert_eq!(original_manager.budget_count(), budget_count,);

    assert_eq!(limiters.manager().budget_count(), budget_count,);

    let snapshot = original_manager.snapshot();

    assert_eq!(snapshot.budgets.len(), budget_count,);

    assert!(
        snapshot
            .budgets
            .iter()
            .all(|budget| budget.locally_used == 0),
        "bootstrap unexpectedly consumed quota",
    );

    // ========================================================
    // REPORT
    // ========================================================

    println!();
    println!("==============================================");
    println!("MARKETFORGE — 15-PROFILE REST BOOTSTRAP");
    println!("==============================================");
    println!("Exchange profiles:      {}", configs.len());
    println!("REST endpoints:         {endpoint_count}");
    println!("Raw responses:          {raw_count}");
    println!("Envelope responses:     {envelope_count}");
    println!("Shared budgets:         {budget_count}");
    println!("Production bootstrap:   PASS");
    println!("Request resolution:     PASS");
    println!("No network requests:    VERIFIED");
}

// ============================================================
// PRODUCTION REST SERVICE — FIRST REAL EXCHANGE REQUEST
// ============================================================

// ============================================================
// LIVE REST ACCEPTANCE — NATIVE TEST INSTRUMENTS
// ============================================================

// ============================================================
// PRODUCTION REST SERVICE — BULK LIVE ACCEPTANCE
// ============================================================
