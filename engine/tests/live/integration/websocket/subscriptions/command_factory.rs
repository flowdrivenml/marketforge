use std::collections::BTreeMap;

use serde_json::{Value, json};

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        ConnectionFence, DesiredSubscription, DesiredSubscriptionSnapshot,
        StreamRecoveryCoordinator, SubscriptionCommandFactory, SubscriptionCommandFactoryError,
        SubscriptionRestorationPlanner, SubscriptionRestorationSession,
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

fn connected() -> (
    ConnectionFence,
    marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
) {
    let fence = ConnectionFence::new("runtime-1", "public-slot").unwrap();

    let stamp = fence.begin_connect().unwrap();

    fence.connected(&stamp).unwrap();

    (fence, stamp)
}

fn desired(revision: u64) -> DesiredSubscriptionSnapshot {
    DesiredSubscriptionSnapshot::new(
        revision,
        [
            DesiredSubscription::new("BTC:trades", "trades", "BTCUSDT").unwrap(),
            DesiredSubscription::new("ETH:depth", "depth", "ETHUSDT")
                .unwrap()
                .with_variable("depth", "200"),
        ],
    )
    .unwrap()
}

// ============================================================
// COMMAND GENERATION
// ============================================================

#[test]
fn generates_native_commands_for_prepared_streams() {
    let (fence, stamp) = connected();

    let config = config();

    let plan = SubscriptionRestorationPlanner::plan(&config, &fence, &stamp, &desired(7)).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let prepared =
        SubscriptionRestorationSession::prepare(plan, &fence, 7, Some("public"), &mut recovery)
            .unwrap();

    let mut factory =
        SubscriptionCommandFactory::new(stamp.clone(), Some("public".into()), BTreeMap::new());

    let commands = factory
        .compile(&config, &fence, 7, prepared, 1_800_000_000)
        .unwrap();

    assert_eq!(commands.len(), 2);
    assert_eq!(factory.last_request_id(), 2);

    let messages: Vec<Value> = commands
        .iter()
        .map(|command| serde_json::from_str(&command.wire_text).unwrap())
        .collect();

    assert_eq!(
        messages[0],
        json!({
            "op": "subscribe",
            "args": [
                "publicTrade.BTCUSDT"
            ],
            "req_id": "mf-1-1"
        }),
    );

    assert_eq!(
        messages[1],
        json!({
            "op": "subscribe",
            "args": [
                "orderbook.200.ETHUSDT"
            ],
            "req_id": "mf-1-2"
        }),
    );

    assert_eq!(commands[0].expected.request_id, Some(json!("mf-1-1")),);

    assert_eq!(commands[1].expected.request_id, Some(json!("mf-1-2")),);

    for command in &commands {
        assert_eq!(command.token.connection(), &stamp,);

        assert_eq!(
            recovery.token(command.token.stream_id()),
            Some(&command.token),
        );
    }
}

// ============================================================
// STALE DEMAND REVISION
// ============================================================

#[test]
fn rejects_stale_demand_before_command_generation() {
    let (fence, stamp) = connected();
    let config = config();

    let plan = SubscriptionRestorationPlanner::plan(&config, &fence, &stamp, &desired(7)).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let prepared =
        SubscriptionRestorationSession::prepare(plan, &fence, 7, Some("public"), &mut recovery)
            .unwrap();

    let mut factory =
        SubscriptionCommandFactory::new(stamp, Some("public".into()), BTreeMap::new());

    assert!(matches!(
        factory.compile(&config, &fence, 8, prepared, 1_800_000_000,),
        Err(SubscriptionCommandFactoryError::StaleDemand),
    ));

    assert_eq!(factory.last_request_id(), 0);
}

// ============================================================
// CONNECTION INVALIDATION
// ============================================================

#[test]
fn rejects_revoked_generation_before_compilation() {
    let (fence, stamp) = connected();
    let config = config();

    let plan = SubscriptionRestorationPlanner::plan(&config, &fence, &stamp, &desired(7)).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let prepared =
        SubscriptionRestorationSession::prepare(plan, &fence, 7, Some("public"), &mut recovery)
            .unwrap();

    let mut factory =
        SubscriptionCommandFactory::new(stamp.clone(), Some("public".into()), BTreeMap::new());

    fence.failed(&stamp).unwrap();

    assert!(matches!(
        factory.compile(&config, &fence, 7, prepared, 1_800_000_000,),
        Err(SubscriptionCommandFactoryError::StaleGeneration),
    ));
}

// ============================================================
// ROUTE MISMATCH
// ============================================================

#[test]
fn rejects_wrong_route_before_command_generation() {
    let (fence, stamp) = connected();
    let config = config();

    let plan = SubscriptionRestorationPlanner::plan(&config, &fence, &stamp, &desired(7)).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let prepared =
        SubscriptionRestorationSession::prepare(plan, &fence, 7, Some("public"), &mut recovery)
            .unwrap();

    let mut factory =
        SubscriptionCommandFactory::new(stamp, Some("business".into()), BTreeMap::new());

    assert!(matches!(
        factory.compile(&config, &fence, 7, prepared, 1_800_000_000,),
        Err(SubscriptionCommandFactoryError::WrongRoute),
    ));

    assert_eq!(factory.last_request_id(), 0);
}

// ============================================================
// ID-LESS SUBSCRIPTION WITHOUT ACK BINDING
// ============================================================

#[test]
fn idless_subscription_requires_native_ack_binding() {
    let config: ExchangeConfig = toml::from_str(
        r#"
schema_version = 1

[exchange]
id = "gateio"
market = "spot"
profile = "gateio_spot"

[websocket]
url = "wss://example.invalid"

[websocket.subscription]
strategy = "gate_v4_channel_event"

[channels.trades]
enabled = true
format = "trades/gateio_spot"
channel = "spot.trades"
subscription_payload = ["{symbol}"]

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap();

    let (fence, stamp) = connected();

    let desired = DesiredSubscriptionSnapshot::new(
        1,
        [DesiredSubscription::new("BTC:trades", "trades", "BTC_USDT").unwrap()],
    )
    .unwrap();

    let plan = SubscriptionRestorationPlanner::plan(&config, &fence, &stamp, &desired).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let prepared =
        SubscriptionRestorationSession::prepare(plan, &fence, 1, None, &mut recovery).unwrap();

    let mut factory = SubscriptionCommandFactory::new(stamp, None, BTreeMap::new());

    assert!(matches!(
        factory.compile(&config, &fence, 1, prepared, 1_800_000_000,),
        Err(SubscriptionCommandFactoryError::MissingAckBinding(_)),
    ));
}
