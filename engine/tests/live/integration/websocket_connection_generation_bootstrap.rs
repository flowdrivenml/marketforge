use std::{collections::BTreeMap, time::Duration};

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        ConnectionBootstrapError, ConnectionBootstrapLimits, ConnectionFence,
        ConnectionGenerationBootstrap, DesiredSubscription, DesiredSubscriptionSnapshot,
        SharedStreamRecovery, StreamRecoveryPhase,
    },
};

// ============================================================
// TEST CONFIGURATION
// ============================================================

fn config() -> ExchangeConfig {
    toml::from_str(
        r#"
schema_version = 1

[exchange]
id = "bybit"
market = "spot"
profile = "bybit_spot"

[websocket]
url = "wss://example.invalid"

[websocket.subscription]
strategy = "json_operation"
subscribe_method = "subscribe"
unsubscribe_method = "unsubscribe"
operation_field = "op"
arguments_field = "args"
request_id_field = "req_id"
request_id_type = "string"
request_id_optional = true

success_field = "success"
success_value = true

[channels.trades]
enabled = true
format = "trades/bybit_spot"
route = "public"
stream_template = "publicTrade.{symbol_upper}"

[channels.depth]
enabled = true
format = "depth/bybit_spot"
route = "public"
stream_template = "orderbook.{depth}.{symbol_upper}"

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap()
}

fn limits() -> ConnectionBootstrapLimits {
    ConnectionBootstrapLimits {
        maximum_pending: 16,
        acknowledgement_timeout: Duration::from_secs(5),
    }
}

fn connected() -> (
    ConnectionFence,
    marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
) {
    let fence = ConnectionFence::new("runtime-1", "public-slot").unwrap();

    let stamp = fence.begin_connect().unwrap();
    fence.connected(&stamp).unwrap();

    (fence, stamp)
}

fn desired(revision: u64, include_eth: bool) -> DesiredSubscriptionSnapshot {
    let mut subscriptions =
        vec![DesiredSubscription::new("BTC:trades", "trades", "BTCUSDT").unwrap()];

    if include_eth {
        subscriptions.push(
            DesiredSubscription::new("ETH:depth", "depth", "ETHUSDT")
                .unwrap()
                .with_variable("depth", "200"),
        );
    }

    DesiredSubscriptionSnapshot::new(revision, subscriptions).unwrap()
}

// ============================================================
// INITIAL BOOTSTRAP
// ============================================================

#[test]
fn bootstraps_current_demand() {
    let (fence, stamp) = connected();

    let recovery = SharedStreamRecovery::new();

    let bootstrap = ConnectionGenerationBootstrap::prepare(
        &config(),
        &fence,
        &stamp,
        Some("public"),
        &desired(7, true),
        7,
        &recovery,
        BTreeMap::new(),
        1_800_000_000,
        limits(),
    )
    .unwrap();

    assert_eq!(bootstrap.demand_revision(), 7);
    assert_eq!(bootstrap.command_count(), 2);
    assert_eq!(bootstrap.stamp(), &stamp);

    assert!(bootstrap.recovery().same_authority(&recovery));

    let guard = recovery.lock().unwrap();

    assert_eq!(
        guard.phase("BTC:trades"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );

    assert_eq!(
        guard.phase("ETH:depth"),
        Some(StreamRecoveryPhase::AwaitingSubscription),
    );

    assert!(!guard.may_publish(&fence, guard.token("BTC:trades").unwrap(),));
}

// ============================================================
// RECONNECT WITH CHANGED DEMAND
// ============================================================

#[test]
fn reconnect_bootstrap_preserves_recovery_authority() {
    let (fence, first_stamp) = connected();

    let recovery = SharedStreamRecovery::new();

    ConnectionGenerationBootstrap::prepare(
        &config(),
        &fence,
        &first_stamp,
        Some("public"),
        &desired(7, true),
        7,
        &recovery,
        BTreeMap::new(),
        1_800_000_000,
        limits(),
    )
    .unwrap();

    let old_token = {
        let guard = recovery.lock().unwrap();

        guard.token("BTC:trades").unwrap().clone()
    };

    fence.failed(&first_stamp).unwrap();

    let second_stamp = fence.begin_connect().unwrap();
    fence.connected(&second_stamp).unwrap();

    // ETH demand has disappeared.
    let new_demand = desired(8, false);

    let bootstrap = ConnectionGenerationBootstrap::prepare(
        &config(),
        &fence,
        &second_stamp,
        Some("public"),
        &new_demand,
        8,
        &recovery,
        BTreeMap::new(),
        1_800_000_001,
        limits(),
    )
    .unwrap();

    assert_eq!(bootstrap.command_count(), 1);

    let guard = recovery.lock().unwrap();

    let new_token = guard.token("BTC:trades").unwrap();

    assert!(new_token.stream_generation() > old_token.stream_generation());

    assert!(new_token.subscription_generation() > old_token.subscription_generation());

    assert_eq!(new_token.connection(), &second_stamp,);

    assert!(guard.kind("ETH:depth").is_none());

    assert!(!guard.may_publish(&fence, &old_token));
    assert!(!guard.may_publish(&fence, new_token));
}

// ============================================================
// STALE DEMAND
// ============================================================

#[test]
fn bootstrap_rejects_stale_revision() {
    let (fence, stamp) = connected();

    let recovery = SharedStreamRecovery::new();

    let result = ConnectionGenerationBootstrap::prepare(
        &config(),
        &fence,
        &stamp,
        Some("public"),
        &desired(7, true),
        8,
        &recovery,
        BTreeMap::new(),
        1_800_000_000,
        limits(),
    );

    assert!(matches!(result, Err(ConnectionBootstrapError::StaleDemand),));

    assert!(recovery.lock().unwrap().kind("BTC:trades").is_none());
}

// ============================================================
// WRONG PHYSICAL ROUTE
// ============================================================

#[test]
fn bootstrap_rejects_wrong_route() {
    let (fence, stamp) = connected();

    let recovery = SharedStreamRecovery::new();

    let result = ConnectionGenerationBootstrap::prepare(
        &config(),
        &fence,
        &stamp,
        Some("business"),
        &desired(7, true),
        7,
        &recovery,
        BTreeMap::new(),
        1_800_000_000,
        limits(),
    );

    assert!(matches!(
        result,
        Err(ConnectionBootstrapError::Preparation(_)),
    ));

    assert!(recovery.lock().unwrap().kind("BTC:trades").is_none());
}

// ============================================================
// STALE PHYSICAL GENERATION
// ============================================================

#[test]
fn bootstrap_rejects_revoked_connection() {
    let (fence, stamp) = connected();

    fence.failed(&stamp).unwrap();

    let recovery = SharedStreamRecovery::new();

    let result = ConnectionGenerationBootstrap::prepare(
        &config(),
        &fence,
        &stamp,
        Some("public"),
        &desired(7, true),
        7,
        &recovery,
        BTreeMap::new(),
        1_800_000_000,
        limits(),
    );

    assert!(matches!(
        result,
        Err(ConnectionBootstrapError::StaleGeneration),
    ));
}

// ============================================================
// INVALID LIMITS
// ============================================================

#[test]
fn bootstrap_rejects_invalid_limits() {
    let mut limits = limits();

    limits.maximum_pending = 0;

    assert!(matches!(
        limits.validate(),
        Err(ConnectionBootstrapError::InvalidLimits),
    ));
}
