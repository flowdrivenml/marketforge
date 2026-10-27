use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use marketforge_engine::live::{
    config::{
        ExchangeConfigStore,
        exchange::{ExchangeId, MarketType},
    },
    infrastructure::{
        limiter::{
            BudgetId, BudgetScope, WindowQuota,
            factory::{
                ExchangeLimiterFactory, ExchangeLimiters, LimiterProfileInput, ResolvedRestBudget,
                RestLimiterBindings, WebSocketBudgetBinding, WebSocketLimiterBindings,
            },
        },
        rest::{
            ExchangeRestBootstrap, ExchangeRestService, RestClientConfig, RestFacadeError,
            RestFacadeFactory, RestFactoryFeedbackPolicy,
        },
    },
    stream::request::{CreateSourceStreamRequest, QuantityType, SourceChannel},
};

// ============================================================
// REAL CONFIGURATION
// ============================================================

fn load_binance_spot() -> ExchangeConfigStore {
    let source = PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges/binance_spot.toml"
    ));

    assert!(
        source.is_file(),
        "real Binance Spot configuration is missing"
    );

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let directory = std::env::temp_dir().join(format!(
        "marketforge-rest-facade-{}-{unique}",
        std::process::id()
    ));

    fs::create_dir_all(&directory).unwrap();

    let destination = directory.join("binance_spot.toml");

    fs::copy(&source, &destination).expect("failed to copy real exchange configuration");

    let result = ExchangeConfigStore::load_dir(&directory);

    fs::remove_dir_all(&directory).expect("failed to clean temporary configuration directory");

    let store = result.expect("failed to load real Binance Spot configuration");

    assert_eq!(store.len(), 1);
    assert!(store.get("binance_spot").is_some());

    store
}

// ============================================================
// SHARED LIMITER
// ============================================================

pub(super) fn build_limiters(configs: &ExchangeConfigStore) -> ExchangeLimiters {
    let exchange = configs
        .get("binance_spot")
        .expect("missing Binance Spot configuration");

    // Binance Spot uses a weighted REST budget.
    //
    // This is an isolated TEST quota, not a claim about
    // Binance's actual production request-weight limit.
    //
    // No network requests are executed by these tests.

    let budget = BudgetId::new(
        ExchangeId::Binance,
        "spot_rest",
        "request_weight",
        BudgetScope::Egress("facade_factory_test".into()),
    )
    .unwrap();

    let quota = WindowQuota::new(100_000, Duration::from_secs(60)).unwrap();

    let rest_bindings = RestLimiterBindings {
        shared_weight: Some(ResolvedRestBudget { id: budget, quota }),
        ..Default::default()
    };

    let websocket_bindings = WebSocketLimiterBindings {
        connection_attempts: Some(WebSocketBudgetBinding::new(
            "binance_spot_websocket",
            BudgetScope::Egress("facade_factory_test".into()),
        )),

        connection_attempts_per_second: Some(WebSocketBudgetBinding::new(
            "binance_spot_websocket",
            BudgetScope::Egress("facade_factory_test".into()),
        )),

        connection_service: Some("binance_spot_websocket".into()),
    };

    let inputs = [LimiterProfileInput::new(
        exchange,
        &rest_bindings,
        &websocket_bindings,
    )];

    ExchangeLimiterFactory::build(&inputs, 32)
        .expect("failed to compile real Binance Spot limiter policies")
}

// ============================================================
// PRODUCTION REST SERVICE
// ============================================================

pub(super) fn build_service<'a>(
    configs: &'a ExchangeConfigStore,
    limiters: &ExchangeLimiters,
) -> ExchangeRestService<'a> {
    let feedback = BTreeMap::from([(
        "binance_spot".to_owned(),
        RestFactoryFeedbackPolicy::new(Vec::new(), Duration::from_millis(100)).unwrap(),
    )]);

    ExchangeRestBootstrap::build(
        configs,
        limiters,
        feedback,
        RestClientConfig {
            max_concurrent_requests: 8,
            max_response_bytes: 1024 * 1024,
        },
    )
    .expect("failed to initialize production REST service")
}

// ============================================================
// SOURCE STREAM REQUEST
// ============================================================

pub(super) fn spot_request(
    instrument_id: i64,
    symbol: &str,
    channel: SourceChannel,
) -> CreateSourceStreamRequest {
    CreateSourceStreamRequest {
        instrument_id,
        exchange: ExchangeId::Binance,
        symbol: symbol.to_owned(),
        market_category: MarketType::Spot,
        instrument_type: "spot".to_owned(),
        base_asset: Some("BTC".to_owned()),
        quote_asset: Some("USDT".to_owned()),
        quantity_type: QuantityType::Base,
        contract_value: None,
        contract_value_asset: None,
        channel,
        settlement_asset: None,
    }
}

// ============================================================
// VALID HANDLE CREATION
// ============================================================

#[test]
fn creates_stream_bound_handle() {
    let configs = load_binance_spot();
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let request = spot_request(101, "BTCUSDT", SourceChannel::Trades);

    let handle = factory.create(&request).unwrap();

    assert_eq!(handle.profile(), "binance_spot");
    assert_eq!(handle.symbol(), "BTCUSDT");
    assert_eq!(handle.instrument_id(), 101);

    assert_eq!(handle.stream_request(), &request);
}

// ============================================================
// HANDLE CAPABILITIES
// ============================================================

#[test]
fn handle_exposes_real_profile_capabilities() {
    let configs = load_binance_spot();
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let request = spot_request(102, "BTCUSDT", SourceChannel::Trades);

    let handle = factory.create(&request).unwrap();

    let capabilities = handle.capabilities();

    assert!(capabilities.supports_depth_snapshot());
    assert!(capabilities.supports_recent_trades());
    assert!(capabilities.supports_historical_trades());

    assert_eq!(
        capabilities.recent_trades.as_ref().unwrap().endpoint,
        "recent_trades"
    );

    assert_eq!(
        capabilities
            .historical_trades
            .as_ref()
            .unwrap()
            .endpoint
            .endpoint,
        "historical_trades"
    );
}

// ============================================================
// MULTIPLE STREAM HANDLES
// ============================================================

#[test]
fn creates_independent_stream_bindings() {
    let configs = load_binance_spot();
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let first_request = spot_request(201, "BTCUSDT", SourceChannel::Trades);

    let second_request = spot_request(202, "ETHUSDT", SourceChannel::Depth);

    let first = factory.create(&first_request).unwrap();
    let second = factory.create(&second_request).unwrap();

    assert_eq!(first.symbol(), "BTCUSDT");
    assert_eq!(second.symbol(), "ETHUSDT");

    assert_eq!(first.instrument_id(), 201);
    assert_eq!(second.instrument_id(), 202);

    assert_eq!(first.profile(), second.profile());

    assert_ne!(first.stream_request(), second.stream_request());
}

// ============================================================
// SHARED SERVICE OWNERSHIP
// ============================================================

#[test]
fn multiple_handles_reuse_same_rest_service() {
    let configs = load_binance_spot();
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let first = factory
        .create(&spot_request(301, "BTCUSDT", SourceChannel::Trades))
        .unwrap();

    let second = factory
        .create(&spot_request(302, "ETHUSDT", SourceChannel::Trades))
        .unwrap();

    assert!(
        std::ptr::eq(first.service(), second.service(),),
        "stream handles must share one REST service"
    );

    assert!(
        std::ptr::eq(factory.service(), first.service(),),
        "factory and handles must reference the same service"
    );
}

// ============================================================
// INVALID SOURCE REQUEST
// ============================================================

#[test]
fn rejects_invalid_source_stream_request() {
    let configs = load_binance_spot();
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let request = spot_request(0, "BTCUSDT", SourceChannel::Trades);

    let result = factory.create(&request);

    assert!(
        matches!(result, Err(RestFacadeError::InvalidStreamRequest(_))),
        "factory must reject invalid source-stream identity"
    );
}

// ============================================================
// UNKNOWN PROFILE
// ============================================================

#[test]
fn rejects_unavailable_exchange_profile() {
    let configs = load_binance_spot();
    let limiters = build_limiters(&configs);
    let service = build_service(&configs, &limiters);

    let factory = RestFacadeFactory::new(&service);

    let mut request = spot_request(401, "BTCUSDT", SourceChannel::Trades);

    // The isolated configuration store contains only
    // the real Binance Spot profile.
    request.exchange = ExchangeId::Bybit;

    let result = factory.create(&request);

    assert!(
        matches!(result, Err(RestFacadeError::UnknownProfile(_))),
        "factory must reject an unavailable profile"
    );
}

// ============================================================
// CONSTRUCTION MUST NOT PERFORM HTTP REQUESTS
// ============================================================

#[test]
fn factory_construction_does_not_consume_rate_limit_budget() {
    let configs = load_binance_spot();
    let limiters = build_limiters(&configs);

    let before = limiters.manager().snapshot();

    let service = build_service(&configs, &limiters);
    let factory = RestFacadeFactory::new(&service);

    for instrument_id in 1..=10 {
        let request = spot_request(instrument_id, "BTCUSDT", SourceChannel::Trades);

        factory.create(&request).unwrap();
    }

    let after = limiters.manager().snapshot();

    assert_eq!(before.budgets.len(), after.budgets.len());

    for (before_budget, after_budget) in before.budgets.iter().zip(after.budgets.iter()) {
        assert_eq!(
            before_budget.locally_used, after_budget.locally_used,
            "facade construction must not consume REST quota"
        );
    }
}

