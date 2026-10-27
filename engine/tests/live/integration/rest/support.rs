use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use marketforge_engine::live::{
    config::{ExchangeConfigStore, exchange::ExchangeConfig},
    infrastructure::{
        limiter::{
            BudgetId, BudgetScope, WindowQuota,
            factory::{
                ExchangeLimiterFactory, ExchangeLimiters, LimiterProfileInput, ResolvedRestBudget,
                RestBudgetBinding, RestLimiterBindings, WebSocketBudgetBinding,
                WebSocketLimiterBindings,
            },
        },
        rest::{
            ExchangeRestBootstrap, ExchangeRestService, RestClientConfig, RestFactoryFeedbackPolicy,
        },
    },
};

pub fn load_profile(profile: &str, mock_base_url: &str) -> ExchangeConfigStore {
    let source = PathBuf::from(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../data/.config/live/exchanges"
    ))
    .join(format!("{profile}.toml"));

    let mut config =
        ExchangeConfig::load(&source).unwrap_or_else(|error| panic!("{profile}: {error}"));

    config.rest.base_url = mock_base_url.to_owned();
    config.validate().expect("invalid redirected configuration");

    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();

    let directory = std::env::temp_dir().join(format!(
        "marketforge-rest-profile-{}-{unique}",
        std::process::id()
    ));

    fs::create_dir_all(&directory).unwrap();

    let destination = directory.join(format!("{profile}.toml"));

    fs::write(&destination, toml::to_string(&config).unwrap()).unwrap();

    let loaded = ExchangeConfigStore::load_dir(&directory);

    fs::remove_dir_all(&directory).unwrap();

    loaded.expect("failed to load redirected profile")
}

pub fn build_limiters(configs: &ExchangeConfigStore, profile: &str) -> ExchangeLimiters {
    let exchange = configs
        .get(profile)
        .unwrap_or_else(|| panic!("missing profile: {profile}"));

    let scope = BudgetScope::Egress(format!("rest_facade_integration_{profile}"));

    let binding = RestBudgetBinding::new(format!("{profile}_rest"), scope.clone());

    let rate = exchange.rest.rate_limiting.as_ref();

    let mut rest = RestLimiterBindings::default();

    if rate.is_some_and(|r| r.ip_max_requests.is_some()) {
        rest.ip = Some(binding.clone());
    }

    if rate.is_some_and(|r| r.global_max_requests.is_some()) {
        rest.global = Some(binding.clone());
    }

    for name in exchange.rest.rate_limit_policies.keys() {
        rest.endpoint_policies.insert(name.clone(), binding.clone());
    }

    if rate.is_some_and(|r| r.strategy == "weighted_shared_pool") {
        let budget = BudgetId::new(
            exchange.exchange.id,
            format!("{profile}_rest"),
            "test_weight",
            scope.clone(),
        )
        .unwrap();

        rest.shared_weight = Some(ResolvedRestBudget {
            id: budget,
            quota: WindowQuota::new(100_000, Duration::from_secs(60)).unwrap(),
        });
    }

    let websocket_binding = WebSocketBudgetBinding::new(format!("{profile}_websocket"), scope);

    let websocket = WebSocketLimiterBindings {
        connection_attempts: Some(websocket_binding.clone()),
        connection_attempts_per_second: Some(websocket_binding),
        connection_service: Some(format!("{profile}_websocket")),
    };

    let inputs = [LimiterProfileInput::new(exchange, &rest, &websocket)];

    ExchangeLimiterFactory::build(&inputs, 32)
        .unwrap_or_else(|error| panic!("{profile}: limiter compilation failed: {error:?}"))
}

pub fn build_service<'a>(
    configs: &'a ExchangeConfigStore,
    limiters: &ExchangeLimiters,
    profile: &str,
) -> ExchangeRestService<'a> {
    let feedback = BTreeMap::from([(
        profile.to_owned(),
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
    .expect("failed to bootstrap real-profile REST service")
}
