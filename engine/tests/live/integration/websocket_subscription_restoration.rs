use serde_json::json;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        ConnectionFence, DesiredSubscription, DesiredSubscriptionSnapshot,
        SubscriptionRestorationError, SubscriptionRestorationPlanner,
    },
};

// ============================================================
// CONFIGURATION
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

[channels.trades]
enabled = true
format = "trades/bybit_spot"
stream_template = "publicTrade.{symbol_upper}"

[channels.depth]
enabled = true
format = "depth/bybit_spot"
stream_template = "orderbook.{depth}.{symbol_upper}"

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap()
}

fn trade(symbol: &str) -> DesiredSubscription {
    DesiredSubscription::new(format!("{symbol}:trades"), "trades", symbol).unwrap()
}

fn depth(symbol: &str) -> DesiredSubscription {
    DesiredSubscription::new(format!("{symbol}:depth"), "depth", symbol)
        .unwrap()
        .with_variable("depth", "200")
}

fn connected() -> (
    ConnectionFence,
    marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
) {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    (fence, stamp)
}

// ============================================================
// INITIAL RESTORATION
// ============================================================

#[test]
fn w46_restores_current_desired_subscriptions() {
    let (fence, stamp) = connected();

    let desired =
        DesiredSubscriptionSnapshot::new(1, [trade("BTCUSDT"), trade("ETHUSDT"), depth("SOLUSDT")])
            .unwrap();

    let plan = SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired).unwrap();

    assert_eq!(plan.entries().len(), 3);
    assert_eq!(plan.demand_revision(), 1);
    assert_eq!(plan.stamp(), &stamp);

    assert!(plan.is_current(&fence, 1));

    let native: Vec<_> = plan
        .entries()
        .iter()
        .map(|entry| {
            (
                entry.stream_id.as_str(),
                entry.resolved.request.arguments.clone(),
            )
        })
        .collect();

    assert!(native.contains(&("BTCUSDT:trades", vec![json!("publicTrade.BTCUSDT")],)));

    assert!(native.contains(&("ETHUSDT:trades", vec![json!("publicTrade.ETHUSDT")],)));

    assert!(native.contains(&("SOLUSDT:depth", vec![json!("orderbook.200.SOLUSDT")],)));
}

// ============================================================
// DEMAND CHANGES WHILE DISCONNECTED
// ============================================================

#[test]
fn w46_reconnect_uses_new_demand_not_old_subscription_queue() {
    let (fence, first) = connected();

    let old_demand =
        DesiredSubscriptionSnapshot::new(1, [trade("BTCUSDT"), trade("ETHUSDT"), depth("SOLUSDT")])
            .unwrap();

    let old_plan =
        SubscriptionRestorationPlanner::plan(&config(), &fence, &first, &old_demand).unwrap();

    assert_eq!(old_plan.entries().len(), 3);

    // Physical connection fails.
    fence.failed(&first).unwrap();

    // Demand changes during backoff:
    //
    // ETH removed.
    // XRP added.
    let current_demand =
        DesiredSubscriptionSnapshot::new(2, [trade("BTCUSDT"), depth("SOLUSDT"), trade("XRPUSDT")])
            .unwrap();

    // The old plan is invalidated by the physical
    // connection generation change.
    assert!(!old_plan.is_current(&fence, 2));

    let second = fence.begin_connect().unwrap();

    fence.connected(&second).unwrap();

    let new_plan =
        SubscriptionRestorationPlanner::plan(&config(), &fence, &second, &current_demand).unwrap();

    let streams: Vec<_> = new_plan
        .entries()
        .iter()
        .map(|entry| entry.stream_id.as_str())
        .collect();

    assert_eq!(new_plan.entries().len(), 3);

    assert!(streams.contains(&"BTCUSDT:trades"));
    assert!(streams.contains(&"SOLUSDT:depth"));
    assert!(streams.contains(&"XRPUSDT:trades"));

    assert!(!streams.contains(&"ETHUSDT:trades"));

    assert_eq!(new_plan.demand_revision(), 2);

    assert!(new_plan.is_current(&fence, 2));

    assert_ne!(first.generation(), second.generation(),);
}

// ============================================================
// STALE DEMAND REVISION
// ============================================================

#[test]
fn w46_plan_is_invalidated_by_new_demand_revision() {
    let (fence, stamp) = connected();

    let desired = DesiredSubscriptionSnapshot::new(10, [trade("BTCUSDT")]).unwrap();

    let plan = SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired).unwrap();

    assert!(plan.is_current(&fence, 10));

    // The authoritative registry advances to revision 11.
    assert!(!plan.is_current(&fence, 11));
}

// ============================================================
// STALE CONNECTION
// ============================================================

#[test]
fn w46_stale_connection_cannot_compile_restoration() {
    let (fence, first) = connected();

    fence.failed(&first).unwrap();

    let second = fence.begin_connect().unwrap();

    fence.connected(&second).unwrap();

    let desired = DesiredSubscriptionSnapshot::new(2, [trade("BTCUSDT")]).unwrap();

    assert!(matches!(
        SubscriptionRestorationPlanner::plan(&config(), &fence, &first, &desired,),
        Err(SubscriptionRestorationError::StaleConnection)
    ));
}

// ============================================================
// NO SUBSCRIPTIONS BEFORE HANDSHAKE
// ============================================================

#[test]
fn w46_cannot_restore_before_connection_established() {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();

    let desired = DesiredSubscriptionSnapshot::new(1, [trade("BTCUSDT")]).unwrap();

    assert!(matches!(
        SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired,),
        Err(SubscriptionRestorationError::ConnectionNotEstablished)
    ));
}

// ============================================================
// PREFLIGHT FAILURE
// ============================================================

#[test]
fn w46_bad_subscription_prevents_partial_plan() {
    let (fence, stamp) = connected();

    let desired = DesiredSubscriptionSnapshot::new(
        1,
        [
            trade("BTCUSDT"),
            // This channel does not exist in the
            // exchange configuration.
            DesiredSubscription::new("INVALID:unknown", "unknown", "INVALID").unwrap(),
        ],
    )
    .unwrap();

    let result = SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired);

    assert!(matches!(
        result,
        Err(SubscriptionRestorationError::Resolution { .. })
    ));

    // No network I/O or stream epoch mutation occurs
    // during planning.
}

// ============================================================
// DUPLICATE STREAM IDENTITIES
// ============================================================

#[test]
fn w46_duplicate_desired_stream_is_rejected() {
    let result = DesiredSubscriptionSnapshot::new(1, [trade("BTCUSDT"), trade("BTCUSDT")]);

    assert!(matches!(
        result,
        Err(SubscriptionRestorationError::DuplicateStream(_))
    ));
}

// ============================================================
// EMPTY DEMAND
// ============================================================

#[test]
fn w46_empty_demand_produces_empty_restoration_plan() {
    let (fence, stamp) = connected();

    let desired = DesiredSubscriptionSnapshot::new(1, Vec::<DesiredSubscription>::new()).unwrap();

    let plan = SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired).unwrap();

    assert!(plan.entries().is_empty());
    assert!(plan.is_current(&fence, 1));
}
