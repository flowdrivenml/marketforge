use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::websocket::{
        ConnectionFence, DesiredSubscription, DesiredSubscriptionSnapshot,
        RestorationPreparationError, StreamRecoveryCoordinator, StreamRecoveryPhase,
        SubscriptionRestorationPlanner, SubscriptionRestorationSession,
    },
};

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

fn desired() -> DesiredSubscriptionSnapshot {
    DesiredSubscriptionSnapshot::new(
        4,
        [
            DesiredSubscription::new("BTC:trades", "trades", "BTCUSDT").unwrap(),
            DesiredSubscription::new("ETH:depth", "depth", "ETHUSDT")
                .unwrap()
                .with_variable("depth", "200"),
        ],
    )
    .unwrap()
}

#[test]
fn prepares_fresh_stream_epochs() {
    let (fence, stamp) = connected();

    let plan = SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired()).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let prepared =
        SubscriptionRestorationSession::prepare(plan, &fence, 4, Some("public"), &mut recovery)
            .unwrap();

    assert_eq!(prepared.entries().len(), 2);
    assert_eq!(prepared.demand_revision(), 4);
    assert_eq!(prepared.route(), Some("public"));

    let first = &prepared.entries()[0].token;
    let second = &prepared.entries()[1].token;

    assert_eq!(first.connection(), &stamp);
    assert_eq!(second.connection(), &stamp);

    assert_ne!(first.stream_generation(), second.stream_generation(),);

    assert_ne!(
        first.subscription_generation(),
        second.subscription_generation(),
    );

    for entry in prepared.entries() {
        assert_eq!(recovery.token(entry.token.stream_id()), Some(&entry.token),);

        assert_eq!(
            recovery.phase(entry.token.stream_id()),
            Some(StreamRecoveryPhase::AwaitingSubscription),
        );
    }
}

#[test]
fn rejects_wrong_physical_route() {
    let (fence, stamp) = connected();

    let plan = SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired()).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let result =
        SubscriptionRestorationSession::prepare(plan, &fence, 4, Some("business"), &mut recovery);

    assert!(matches!(
        result,
        Err(RestorationPreparationError::WrongRoute { .. }),
    ));

    assert!(recovery.token("BTC:trades").is_none());
    assert!(recovery.token("ETH:depth").is_none());
}

#[test]
fn rejects_stale_demand_revision() {
    let (fence, stamp) = connected();

    let plan = SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired()).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    let result =
        SubscriptionRestorationSession::prepare(plan, &fence, 5, Some("public"), &mut recovery);

    assert!(matches!(
        result,
        Err(RestorationPreparationError::StalePlan),
    ));

    assert!(recovery.token("BTC:trades").is_none());
}

#[test]
fn rejects_revoked_connection_generation() {
    let (fence, stamp) = connected();

    let plan = SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired()).unwrap();

    fence.failed(&stamp).unwrap();

    let mut recovery = StreamRecoveryCoordinator::new();

    assert!(matches!(
        SubscriptionRestorationSession::prepare(plan, &fence, 4, Some("public"), &mut recovery,),
        Err(RestorationPreparationError::StalePlan),
    ));
}

#[test]
fn rebinds_existing_streams_after_reconnect() {
    let (fence, first_stamp) = connected();

    let mut recovery = StreamRecoveryCoordinator::new();

    let first_plan =
        SubscriptionRestorationPlanner::plan(&config(), &fence, &first_stamp, &desired()).unwrap();

    let first = SubscriptionRestorationSession::prepare(
        first_plan,
        &fence,
        4,
        Some("public"),
        &mut recovery,
    )
    .unwrap();

    let first_tokens: Vec<_> = first
        .entries()
        .iter()
        .map(|entry| entry.token.clone())
        .collect();

    // Revoke generation N.
    fence.failed(&first_stamp).unwrap();

    // Establish generation N+1.
    let second_stamp = fence.begin_connect().unwrap();
    fence.connected(&second_stamp).unwrap();

    let second_plan =
        SubscriptionRestorationPlanner::plan(&config(), &fence, &second_stamp, &desired()).unwrap();

    let second = SubscriptionRestorationSession::prepare(
        second_plan,
        &fence,
        4,
        Some("public"),
        &mut recovery,
    )
    .unwrap();

    assert_eq!(second.entries().len(), 2);

    for (old, new) in first_tokens.iter().zip(second.entries()) {
        assert_eq!(old.stream_id(), new.token.stream_id(),);

        assert!(new.token.stream_generation() > old.stream_generation(),);

        assert!(new.token.subscription_generation() > old.subscription_generation(),);

        assert_eq!(new.token.connection(), &second_stamp,);

        assert_eq!(
            recovery.phase(new.token.stream_id()),
            Some(StreamRecoveryPhase::AwaitingSubscription),
        );

        assert!(!recovery.may_publish(&fence, old));
        assert!(!recovery.may_publish(&fence, &new.token,));
    }
}

#[test]
fn reconnection_retires_removed_demand() {
    let (fence, first_stamp) = connected();

    let mut recovery = StreamRecoveryCoordinator::new();

    let first_plan =
        SubscriptionRestorationPlanner::plan(&config(), &fence, &first_stamp, &desired()).unwrap();

    SubscriptionRestorationSession::prepare(first_plan, &fence, 4, Some("public"), &mut recovery)
        .unwrap();

    let removed_token = recovery.token("ETH:depth").unwrap().clone();

    fence.failed(&first_stamp).unwrap();

    let second_stamp = fence.begin_connect().unwrap();
    fence.connected(&second_stamp).unwrap();

    let next_demand = DesiredSubscriptionSnapshot::new(
        5,
        [
            DesiredSubscription::new("BTC:trades", "trades", "BTCUSDT").unwrap(),
            DesiredSubscription::new("XRP:trades", "trades", "XRPUSDT").unwrap(),
        ],
    )
    .unwrap();

    let plan = SubscriptionRestorationPlanner::plan(&config(), &fence, &second_stamp, &next_demand)
        .unwrap();

    SubscriptionRestorationSession::prepare(plan, &fence, 5, Some("public"), &mut recovery)
        .unwrap();

    assert!(recovery.token("BTC:trades").is_some());
    assert!(recovery.token("XRP:trades").is_some());

    assert!(recovery.kind("ETH:depth").is_none());
    assert!(recovery.token("ETH:depth").is_none());

    assert!(!recovery.may_publish(&fence, &removed_token,));
}

#[test]
fn conflicting_stream_kind_does_not_modify_authority() {
    let (fence, stamp) = connected();

    let mut recovery = StreamRecoveryCoordinator::new();

    recovery
        .register(
            "BTC:trades",
            marketforge_engine::live::infrastructure::websocket::RecoveryStreamKind::Depth,
        )
        .unwrap();

    let plan = SubscriptionRestorationPlanner::plan(&config(), &fence, &stamp, &desired()).unwrap();

    let result =
        SubscriptionRestorationSession::prepare(plan, &fence, 4, Some("public"), &mut recovery);

    assert!(matches!(
        result,
        Err(
            RestorationPreparationError::Recovery(
                marketforge_engine::live::infrastructure::
                    websocket::StreamRecoveryError::
                        ConflictingStreamKind { .. }
            )
        )
    ));

    // Original registration remains unchanged.
    assert_eq!(
        recovery.kind("BTC:trades"),
        Some(marketforge_engine::live::infrastructure::websocket::RecoveryStreamKind::Depth),
    );

    // No unrelated registration was added.
    assert!(recovery.kind("ETH:depth").is_none());

    // No recovery token was allocated.
    assert!(recovery.token("BTC:trades").is_none());
}
