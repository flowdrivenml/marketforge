use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        ConnectionFence, DesiredSubscription, DesiredSubscriptionSnapshot,
        RestorationDispatchError, StreamRecoveryCoordinator, SubscriptionCommandFactory,
        SubscriptionRestorationDispatcher, SubscriptionRestorationPlanner,
        SubscriptionRestorationSession,
    },
};

use tokio::sync::mpsc;

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
operation_field = "op"
arguments_field = "args"

request_id_field = "req_id"
request_id_type = "string"
request_id_optional = true

[channels.trades]
enabled = true
format = "trades/bybit_spot"
stream_template = "publicTrade.{symbol_upper}"

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap()
}

fn commands() -> (
    ConnectionFence,
    marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
    Vec<marketforge_engine::live::infrastructure::websocket::SubscriptionCommand>,
) {
    let fence = ConnectionFence::new("runtime-1", "slot-1").unwrap();

    let stamp = fence.begin_connect().unwrap();
    fence.connected(&stamp).unwrap();

    let desired = DesiredSubscriptionSnapshot::new(
        7,
        [
            DesiredSubscription::new("BTC:trades", "trades", "BTCUSDT").unwrap(),
            DesiredSubscription::new("ETH:trades", "trades", "ETHUSDT").unwrap(),
        ],
    )
    .unwrap();

    let config = config();

    let plan = SubscriptionRestorationPlanner::plan(&config, &fence, &stamp, &desired).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let prepared =
        SubscriptionRestorationSession::prepare(plan, &fence, 7, None, &mut recovery).unwrap();

    let mut factory = SubscriptionCommandFactory::new(stamp.clone(), None, BTreeMap::new());

    let commands = factory
        .compile(&config, &fence, 7, prepared, 1_800_000_000)
        .unwrap();

    (fence, stamp, commands)
}

#[tokio::test]
async fn dispatches_all_commands_to_driver_channel() {
    let (fence, stamp, commands) = commands();

    let (sender, mut receiver) = mpsc::channel(4);

    let dispatcher = SubscriptionRestorationDispatcher::new(Duration::from_millis(100)).unwrap();

    let result = dispatcher
        .dispatch(commands, &sender, &fence, &stamp, 7, || 7)
        .await
        .unwrap();

    assert_eq!(result.admitted, 2);
    assert_eq!(result.demand_revision, 7);

    let first = receiver.recv().await.unwrap();
    let second = receiver.recv().await.unwrap();

    assert!(first.wire_text.contains("BTCUSDT"));
    assert!(second.wire_text.contains("ETHUSDT"));

    assert_eq!(first.token.connection(), &stamp);
    assert_eq!(second.token.connection(), &stamp);
}

#[tokio::test]
async fn rejects_stale_demand_before_admission() {
    let (fence, stamp, commands) = commands();

    let (sender, mut receiver) = mpsc::channel(4);

    let dispatcher = SubscriptionRestorationDispatcher::new(Duration::from_millis(100)).unwrap();

    let result = dispatcher
        .dispatch(commands, &sender, &fence, &stamp, 7, || 8)
        .await;

    assert!(matches!(result, Err(RestorationDispatchError::StaleDemand),));

    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn rejects_revoked_generation() {
    let (fence, stamp, commands) = commands();

    let (sender, mut receiver) = mpsc::channel(4);

    fence.failed(&stamp).unwrap();

    let dispatcher = SubscriptionRestorationDispatcher::new(Duration::from_millis(100)).unwrap();

    let result = dispatcher
        .dispatch(commands, &sender, &fence, &stamp, 7, || 7)
        .await;

    assert!(matches!(
        result,
        Err(RestorationDispatchError::StaleGeneration),
    ));

    assert!(receiver.try_recv().is_err());
}

#[tokio::test]
async fn partial_admission_is_reported_explicitly() {
    let (fence, stamp, commands) = commands();

    // One available slot, but two restoration commands.
    //
    // With no receiver consuming, the second admission
    // must time out.
    let (sender, mut receiver) = mpsc::channel(1);

    let dispatcher = SubscriptionRestorationDispatcher::new(Duration::from_millis(20)).unwrap();

    let result = dispatcher
        .dispatch(commands, &sender, &fence, &stamp, 7, || 7)
        .await;

    assert!(matches!(
        result,
        Err(RestorationDispatchError::PartialAdmission {
            admitted: 1,
            total: 2,
        })
    ));

    // The first command really entered the queue.
    //
    // This proves why a partial admission error must not
    // be interpreted as a rolled-back transaction.
    assert!(receiver.try_recv().is_ok());
}

#[tokio::test]
async fn detects_demand_change_while_waiting_for_capacity() {
    let (fence, stamp, commands) = commands();

    let (sender, mut receiver) = mpsc::channel(1);

    // Occupy the only slot before dispatch starts.
    sender.send(commands_dummy_marker(&stamp)).await.unwrap();

    let revision = AtomicU64::new(7);

    let dispatcher = SubscriptionRestorationDispatcher::new(Duration::from_millis(100)).unwrap();

    let future = dispatcher.dispatch(commands, &sender, &fence, &stamp, 7, || {
        revision.load(Ordering::SeqCst)
    });

    tokio::pin!(future);

    // The dispatcher begins waiting for capacity.
    tokio::select! {
        result = &mut future => {
            panic!("dispatch completed unexpectedly: {result:?}");
        }

        _ = tokio::time::sleep(
            Duration::from_millis(5)
        ) => {}
    }

    revision.store(8, Ordering::SeqCst);

    // Free capacity after advancing demand.
    receiver.recv().await.unwrap();

    let result = future.await;

    assert!(matches!(result, Err(RestorationDispatchError::StaleDemand),));

    assert!(receiver.try_recv().is_err());
}

// Test-only channel marker.
//
// Reuses a valid command generated by the same
// connection's factory rather than inventing tokens.
fn commands_dummy_marker(
    stamp: &marketforge_engine::live::infrastructure::websocket::ConnectionStamp,
) -> marketforge_engine::live::infrastructure::websocket::SubscriptionCommand {
    let (_fence, other_stamp, mut generated) = commands();

    // The marker is never processed by a real driver.
    // Its generation is irrelevant to the capacity test.
    let _ = (stamp, other_stamp);

    generated.remove(0)
}
