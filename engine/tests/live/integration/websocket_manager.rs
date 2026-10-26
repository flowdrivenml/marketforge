use marketforge_engine::live::infrastructure::websocket::StreamRequest;
use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::{
        limiter::{
            BudgetScope, RateLimitManager,
            factory::{WebSocketBudgetBinding, WebSocketLimiterBindings, WebSocketLimiterCompiler},
        },
        websocket::{
            ConnectionBootstrapLimits, ConnectionSessionLimits, DesiredSubscription,
            HeartbeatPlanCompiler, ReconnectPolicy, StreamInput, StreamSource,
            SubscriptionDriverLimits, WebSocketManager, WebSocketManagerRuntime,
            WebSocketProfileRuntime,
        },
    },
};
use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use serde_json::{Value, json};

use tokio::{
    sync::mpsc,
    time::{self, Instant},
};

use tokio_tungstenite::tungstenite::Message;

// ============================================================
// TEST CONSTANTS
// ============================================================

const PROFILE: &str = "binance_linear";

const EVENT_TIMEOUT: Duration = Duration::from_secs(30);

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

const CHANNEL_CAPACITY: usize = 512;

// ============================================================
// EXCHANGE CONFIGURATION
// ============================================================

fn config_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../data/.config/live/exchanges")
        .join(format!("{PROFILE}.toml"))
}

fn load_config() -> ExchangeConfig {
    ExchangeConfig::load(config_path()).expect("Binance linear configuration must load")
}

fn connection_url(config: &ExchangeConfig, channel: &str) -> String {
    let channel_config = config
        .channel(channel)
        .unwrap_or_else(|| panic!("unknown channel: {channel}"));

    if let Some(route_name) = channel_config.route.as_deref() {
        let route = config
            .route(route_name)
            .unwrap_or_else(|| panic!("unknown WebSocket route: {route_name}"));

        if let Some(url) = &route.url {
            return url.clone();
        }

        if let Some(base_url) = &route.base_url {
            return base_url.clone();
        }

        panic!("WebSocket route {route_name} has no configured URL");
    }

    if let Some(url) = &config.websocket.url {
        return url.clone();
    }

    if let Some(base_url) = &config.websocket.base_url {
        if let Some(path) = &config.websocket.raw_path {
            return format!("{}{}", base_url.trim_end_matches('/'), path,);
        }

        return base_url.clone();
    }

    panic!(
        "No WebSocket URL configured for profile={} channel={}",
        config.profile(),
        channel,
    );
}

// ============================================================
// EXISTING MARKETFORGE LIMITS
// ============================================================

fn session_limits() -> ConnectionSessionLimits {
    ConnectionSessionLimits {
        inbound_capacity: 256,
        outbound_capacity: 64,
        command_capacity: 16,
        event_capacity: 16,

        dispatch_timeout: Duration::from_secs(5),
        shutdown_timeout: Duration::from_secs(3),

        driver: SubscriptionDriverLimits {
            queue_timeout: Duration::from_secs(5),
            receipt_timeout: Duration::from_secs(5),
            maximum_early_frames: 64,
        },
    }
}

fn reconnect_policy() -> ReconnectPolicy {
    ReconnectPolicy::new(
        Duration::from_millis(200),
        Duration::from_secs(1),
        Duration::from_secs(10),
        Duration::from_secs(5),
        None,
    )
    .unwrap()
}

// ============================================================
// MANAGER BOOTSTRAP
// ============================================================

fn build_manager_with_capacity(max_subscriptions_per_connection: Option<u64>) -> WebSocketManager {
    let mut config = load_config();

    if let Some(capacity) = max_subscriptions_per_connection {
        assert!(capacity > 0);

        config.websocket.max_subscriptions_per_connection = Some(capacity);

        if let Some(limits) = config.websocket.limits.as_mut() {
            limits.max_subscriptions_per_connection = Some(capacity);
        }
    }

    let url = connection_url(&config, "trades");

    let heartbeat_plan = HeartbeatPlanCompiler::compile(&config).expect("heartbeat compilation");

    // Same limiter setup pattern as the previous
    // successful bulk live test.
    let scope = BudgetScope::Egress("websocket-manager-live-test".into());

    let binding = WebSocketBudgetBinding::new("websocket-manager-binance-linear", scope);

    let bindings = WebSocketLimiterBindings {
        connection_attempts: Some(binding.clone()),

        connection_attempts_per_second: Some(binding),

        connection_service: Some("websocket-manager-binance-linear".into()),
    };

    let compiled_limiters = WebSocketLimiterCompiler::compile(&config, &bindings)
        .expect("WebSocket limiter compilation");

    let shared_limiter = RateLimitManager::new(compiled_limiters.shared.policies.clone(), 32)
        .expect("shared limiter initialization");

    // Binance linear trades use the default route.
    let route = config
        .channel("trades")
        .expect("trades channel exists")
        .route
        .clone();

    let urls = BTreeMap::from([(route, url)]);

    let exchange = Arc::new(config);
    let profile = WebSocketProfileRuntime {
        exchange,
        urls,
        compiled_limiters,
        heartbeat_plan,

        // The earlier live test used no additional
        // exchange-specific ACK bindings for Binance.
        ack_bindings: BTreeMap::new(),
    };

    let runtime = WebSocketManagerRuntime {
        runtime_id: "websocket-manager-live-test".into(),

        profiles: vec![profile],

        shared_limiter,

        reconnect_policy: reconnect_policy(),

        bootstrap_limits: ConnectionBootstrapLimits {
            maximum_pending: 16,

            acknowledgement_timeout: Duration::from_secs(8),
        },

        session_limits: session_limits(),

        event_capacity: 256,

        maximum_early_frames: 256,
    };

    WebSocketManager::from_runtime(runtime).expect("WebSocketManager bootstrap")
}
fn build_manager() -> WebSocketManager {
    build_manager_with_capacity(None)
}
// ============================================================
// SOURCE DESCRIPTIONS
// ============================================================

fn trade_source(symbol: &str) -> StreamSource {
    let stream_id = format!("{symbol}:trades");

    let subscription = DesiredSubscription::new(stream_id, "trades", symbol).unwrap();

    // Binance combined-stream endpoint:
    //
    // wss://fstream.binance.com/stream
    //
    // Native messages contain:
    //
    // {
    //     "stream": "btcusdt@trade",
    //     "data": {
    //         "e": "trade",
    //         "s": "BTCUSDT",
    //         ...
    //     }
    // }

    StreamSource::new(
        PROFILE,
        subscription,
        vec![(
            "/stream".to_owned(),
            json!(format!("{}@trade", symbol.to_ascii_lowercase())),
        )],
    )
    .unwrap()
}

// ============================================================
// EVENT HELPERS
// ============================================================

async fn wait_connected(rx: &mut mpsc::Receiver<StreamInput>, label: &str) {
    let deadline = Instant::now() + EVENT_TIMEOUT;

    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();

        let event = time::timeout(remaining, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {label} Connected"))
            .unwrap_or_else(|| panic!("{label} channel closed before Connected"));

        match event {
            StreamInput::Connected => return,

            StreamInput::Disconnected => {
                println!("[{label}] Disconnected while awaiting Connected");
            }

            StreamInput::Data(_) => {
                panic!("{label} received data before Connected");
            }
        }
    }
}

async fn wait_disconnected(rx: &mut mpsc::Receiver<StreamInput>, label: &str) {
    let deadline = Instant::now() + EVENT_TIMEOUT;

    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();

        let event = time::timeout(remaining, rx.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {label} Disconnected"))
            .unwrap_or_else(|| panic!("{label} channel closed before Disconnected"));

        match event {
            StreamInput::Disconnected => return,

            // Previously queued data may legitimately precede
            // Disconnected in this stream's ordered channel.
            StreamInput::Data(_) => {}

            StreamInput::Connected => {
                panic!(
                    "{label} received duplicate Connected \
                     without Disconnected"
                );
            }
        }
    }
}

async fn wait_trade(rx: &mut mpsc::Receiver<StreamInput>, expected_symbol: &str) -> Value {
    let deadline = Instant::now() + EVENT_TIMEOUT;

    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .unwrap_or_default();

        let event = time::timeout(remaining, rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "timed out waiting for \
                     {expected_symbol} trade"
                )
            })
            .unwrap_or_else(|| {
                panic!(
                    "{expected_symbol} channel closed \
                     while awaiting trade"
                )
            });

        match event {
            StreamInput::Connected => {
                panic!(
                    "{expected_symbol} received unexpected \
                     Connected while waiting for trade"
                );
            }

            StreamInput::Disconnected => {
                panic!(
                    "{expected_symbol} disconnected \
                     while waiting for trade"
                );
            }

            StreamInput::Data(Message::Text(text)) => {
                let value: Value =
                    serde_json::from_str(text.as_str()).expect("native market-data JSON");

                // Verify the combined-stream identity.
                let expected_stream = format!("{}@trade", expected_symbol.to_ascii_lowercase());

                assert_eq!(
                    value.get("stream").and_then(Value::as_str),
                    Some(expected_stream.as_str()),
                    "wrong combined stream routed to destination"
                );

                // Extract the actual trade event.
                let trade = value
                    .get("data")
                    .expect("combined message must contain data");

                assert_eq!(
                    trade.get("e").and_then(Value::as_str),
                    Some("trade"),
                    "unexpected native event"
                );

                assert_eq!(
                    trade.get("s").and_then(Value::as_str),
                    Some(expected_symbol),
                    "wrong symbol routed to destination"
                );

                return value;
            }

            StreamInput::Data(other) => {
                panic!(
                    "unexpected native frame for \
                     {expected_symbol}: {other:?}"
                );
            }
        }
    }
}

// ============================================================
// END-TO-END LIVE TEST
// ============================================================

#[tokio::test]
#[ignore = "connects to Binance public futures WebSocket"]
async fn websocket_manager_shared_slot_lifecycle_live() {
    let manager = build_manager();

    // ========================================================
    // 1. ATTACH BTC
    // ========================================================

    let (btc_tx, mut btc_rx) = mpsc::channel(CHANNEL_CAPACITY);

    let btc_binding = manager
        .attach_request(
            StreamRequest::new(PROFILE, "BTCUSDT", "trades").unwrap(),
            btc_tx,
        )
        .await
        .expect("attach BTC through automatic resolution");

    assert_eq!(manager.slot_count().await, 1);
    assert_eq!(manager.stream_count().await, 1);

    println!("[1] BTC attached");

    // ========================================================
    // 2. BTC SUBSCRIPTION CONFIRMED
    // ========================================================

    wait_connected(&mut btc_rx, "BTC").await;

    println!("[2] BTC Connected");

    // ========================================================
    // 3. BTC NATIVE MARKET DATA
    // ========================================================

    let btc_trade = wait_trade(&mut btc_rx, "BTCUSDT").await;

    println!("[3] BTC trade received: {}", btc_trade);

    // ========================================================
    // 4. ATTACH ETH TO SAME SLOT
    // ========================================================

    let (eth_tx, mut eth_rx) = mpsc::channel(CHANNEL_CAPACITY);

    let eth_binding = manager
        .attach_request(
            StreamRequest::new(PROFILE, "ETHUSDT", "trades").unwrap(),
            eth_tx,
        )
        .await
        .expect("attach ETH through automatic resolution");

    assert_ne!(btc_binding, eth_binding);

    assert_eq!(
        manager.slot_count().await,
        1,
        "BTC and ETH should share one logical connection slot"
    );

    assert_eq!(manager.stream_count().await, 2);

    println!("[4] ETH attached to same slot");

    // ========================================================
    // 5. ETH SUBSCRIPTION CONFIRMED
    // ========================================================

    wait_connected(&mut eth_rx, "ETH").await;

    println!("[5] ETH Connected");

    // ========================================================
    // 6. ETH NATIVE MARKET DATA
    // ========================================================

    let eth_trade = wait_trade(&mut eth_rx, "ETHUSDT").await;

    println!("[6] ETH trade received: {}", eth_trade);
    // ========================================================
    // VERIFY EXISTING BTC STREAM SURVIVES DEMAND REVISION
    // ========================================================

    // Adding ETH changes the shared connection's desired
    // subscriptions. The current ConnectionSlotRunner
    // recycles its physical generation when demand changes.
    //
    // BTC must observe the old generation ending.
    wait_disconnected(&mut btc_rx, "BTC").await;

    println!("[6A] BTC Disconnected after ETH attachment");

    // BTC must receive a fresh subscription confirmation
    // from the replacement connection.
    wait_connected(&mut btc_rx, "BTC").await;

    println!("[6B] BTC reconnected after ETH attachment");

    // Verify market data continues reaching BTC after
    // the connection generation changes.
    let btc_after_eth = wait_trade(&mut btc_rx, "BTCUSDT").await;

    println!("[6C] BTC trade after ETH attachment: {}", btc_after_eth);

    // Both streams must remain assigned to one slot.
    assert_eq!(manager.slot_count().await, 1);
    assert_eq!(manager.stream_count().await, 2);
    // This verifies ETH's stream receives ETH market data.
    // BTC's separate destination was already independently
    // verified above.

    // ========================================================
    // 7. DETACH BTC
    // ========================================================

    time::timeout(SHUTDOWN_TIMEOUT, manager.detach(btc_binding))
        .await
        .expect("BTC detach timed out")
        .expect("detach BTC");

    assert_eq!(manager.slot_count().await, 1);
    assert_eq!(manager.stream_count().await, 1);

    println!("[7] BTC detached; ETH remains assigned");

    // ========================================================
    // 8. BTC DESTINATION RETIRED
    // ========================================================

    // BTC may still have previously accepted data queued.
    // Drain it, then require terminal channel closure.
    time::timeout(SHUTDOWN_TIMEOUT, async {
        while btc_rx.recv().await.is_some() {}
    })
    .await
    .expect("BTC destination did not close");

    println!("[8] BTC receiver closed");

    // ========================================================
    // 9. ETH OBSERVES CONNECTION REPLACEMENT
    // ========================================================

    wait_disconnected(&mut eth_rx, "ETH").await;

    println!("[9] ETH Disconnected");

    wait_connected(&mut eth_rx, "ETH").await;

    println!("[10] ETH reconnected");

    // ========================================================
    // 10. ETH RECEIVES DATA AFTER REPLACEMENT
    // ========================================================

    let recovered_trade = wait_trade(&mut eth_rx, "ETHUSDT").await;

    println!("[11] ETH trade after reconnect: {}", recovered_trade);

    // ========================================================
    // 11. MANAGER SHUTDOWN
    // ========================================================

    time::timeout(SHUTDOWN_TIMEOUT, manager.shutdown())
        .await
        .expect("manager shutdown timed out")
        .expect("manager shutdown failed");

    assert_eq!(manager.slot_count().await, 0);
    assert_eq!(manager.stream_count().await, 0);

    println!("[12] Manager shut down");

    // ========================================================
    // 12. TERMINAL CHANNEL CLOSURE
    // ========================================================

    time::timeout(SHUTDOWN_TIMEOUT, async {
        while eth_rx.recv().await.is_some() {}
    })
    .await
    .expect("ETH destination did not close");

    println!("[13] ETH receiver closed");

    // Repeated shutdown must remain safe.
    manager.shutdown().await.unwrap();

    println!();
    println!("WEBSOCKET MANAGER LIVE TEST PASSED");
}

#[tokio::test]
#[ignore = "connects to Binance public futures WebSocket"]
async fn websocket_manager_backpressure_must_close_stream() {
    let manager = build_manager();

    // Capacity 1 is intentional.
    //
    // Connected will fill the queue.
    // The next BTC trade should trigger backpressure.
    let (tx, mut rx) = mpsc::channel::<StreamInput>(1);

    let binding = manager
        .attach(trade_source("BTCUSDT"), tx)
        .await
        .expect("attach BTC");

    println!("[1] BTC attached with capacity=1");

    // Do not read from rx yet.
    //
    // Allow live data to fill the queue and trigger
    // a destination-delivery failure.
    time::sleep(Duration::from_secs(5)).await;

    println!("[2] Checking terminal delivery behavior");

    // Connected was the first accepted event.
    assert!(matches!(
        time::timeout(Duration::from_secs(5), rx.recv()).await,
        Ok(Some(StreamInput::Connected))
    ));

    // After a fatal delivery failure, the consumer must
    // eventually observe terminal channel closure.
    //
    // This currently exposes a missing manager-supervision
    // guarantee: the manager still holds the sender.
    let terminal = time::timeout(Duration::from_secs(5), rx.recv()).await;

    assert!(
        matches!(terminal, Ok(None)),
        "fatal backpressure did not close the stream: {terminal:?}"
    );

    println!("[3] BTC destination closed after backpressure");

    manager.shutdown().await.unwrap();

    // Keep the binding in scope until after shutdown.
    let _ = binding;
}

#[tokio::test]
#[ignore = "connects to Binance public futures WebSocket"]
async fn websocket_manager_backpressure_isolated_between_slots() {
    // ========================================================
    // SETUP
    // ========================================================

    // One subscription per physical connection.
    //
    // BTC and ETH must therefore use separate slots.
    let manager = build_manager_with_capacity(Some(1));

    // BTC is intentionally slow.
    let (btc_tx, mut btc_rx) = mpsc::channel::<StreamInput>(1);

    // ETH is a healthy consumer.
    let (eth_tx, mut eth_rx) = mpsc::channel::<StreamInput>(512);

    // ========================================================
    // ATTACH BOTH STREAMS
    // ========================================================

    let btc_binding = manager
        .attach(trade_source("BTCUSDT"), btc_tx)
        .await
        .expect("attach BTC");

    let eth_binding = manager
        .attach(trade_source("ETHUSDT"), eth_tx)
        .await
        .expect("attach ETH");

    assert_ne!(btc_binding, eth_binding);

    assert_eq!(
        manager.slot_count().await,
        2,
        "BTC and ETH must use separate connection slots",
    );

    assert_eq!(manager.stream_count().await, 2);

    println!("[1] BTC and ETH attached to separate slots");

    // ========================================================
    // VERIFY HEALTHY ETH STREAM
    // ========================================================

    wait_connected(&mut eth_rx, "ETH").await;

    println!("[2] ETH Connected");

    let first_eth_trade = wait_trade(&mut eth_rx, "ETHUSDT").await;

    println!("[3] ETH received trade: {}", first_eth_trade);

    // ========================================================
    // TRIGGER BTC BACKPRESSURE
    // ========================================================

    // Do not consume BTC's queue.
    //
    // Connected occupies its only queue slot.
    // The next BTC trade must trigger backpressure.
    //
    // The failure must remain local to BTC's connection.
    println!("[4] Leaving BTC destination unconsumed");

    // Use an eventual assertion rather than assuming that
    // the slot supervisor runs at a particular instant.
    time::timeout(Duration::from_secs(15), async {
        loop {
            if manager.stream_count().await == 1 {
                break;
            }

            time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("BTC failed slot was not retired");

    println!("[5] Failed BTC slot retired");

    // ========================================================
    // VERIFY ONLY BTC WAS RETIRED
    // ========================================================

    assert_eq!(manager.slot_count().await, 1);

    assert_eq!(manager.stream_count().await, 1);

    // BTC's initial Connected event may still be queued.
    assert!(matches!(btc_rx.recv().await, Some(StreamInput::Connected)));

    // After draining the queued event, its channel must
    // close because the manager retired the attachment.
    let btc_terminal = time::timeout(Duration::from_secs(5), btc_rx.recv())
        .await
        .expect("BTC receiver did not terminate");

    assert!(
        btc_terminal.is_none(),
        "BTC received an unexpected event after retirement: \
         {btc_terminal:?}"
    );

    println!("[6] BTC receiver closed");

    // ========================================================
    // VERIFY ETH IS STILL HEALTHY
    // ========================================================

    // ETH must remain registered and operational.
    //
    // The failure of BTC's slot must not terminate
    // ETH's event loop or close its destination.
    let next_eth_trade = wait_trade(&mut eth_rx, "ETHUSDT").await;

    println!("[7] ETH continues receiving trades: {}", next_eth_trade);

    assert_eq!(manager.slot_count().await, 1);
    assert_eq!(manager.stream_count().await, 1);

    // ========================================================
    // SHUTDOWN
    // ========================================================

    time::timeout(SHUTDOWN_TIMEOUT, manager.shutdown())
        .await
        .expect("manager shutdown timed out")
        .expect("manager shutdown failed");

    assert_eq!(manager.slot_count().await, 0);
    assert_eq!(manager.stream_count().await, 0);

    // ETH's remaining destination must close.
    time::timeout(SHUTDOWN_TIMEOUT, async {
        while eth_rx.recv().await.is_some() {}
    })
    .await
    .expect("ETH receiver did not close");

    println!("[8] Manager shutdown completed");

    println!();
    println!("WEBSOCKET FAILURE ISOLATION TEST PASSED");
}

#[tokio::test]
#[ignore = "connects to Binance public futures WebSocket"]
async fn websocket_manager_routing_conflict_retires_entire_slot() {
    let manager = build_manager();

    // ========================================================
    // START A VALID BTC STREAM
    // ========================================================

    let (btc_tx, mut btc_rx) = mpsc::channel::<StreamInput>(512);

    let btc_binding = manager
        .attach(trade_source("BTCUSDT"), btc_tx)
        .await
        .expect("attach BTC");

    wait_connected(&mut btc_rx, "BTC").await;

    let _ = wait_trade(&mut btc_rx, "BTCUSDT").await;

    assert_eq!(manager.slot_count().await, 1);
    assert_eq!(manager.stream_count().await, 1);

    println!("[1] BTC connected and receiving market data");

    // ========================================================
    // CREATE A CONFLICTING SOURCE
    // ========================================================

    // Different logical stream identity, but exactly
    // the same native-message routing identity.
    //
    // Two independently attached logical streams must not
    // claim the same incoming native message.
    let conflicting_source = StreamSource::new(
        PROFILE,
        DesiredSubscription::new("conflicting-eth-stream", "trades", "ETHUSDT").unwrap(),
        vec![("/stream".to_owned(), json!("btcusdt@trade"))],
    )
    .unwrap();

    let (eth_tx, mut eth_rx) = mpsc::channel::<StreamInput>(512);

    // ========================================================
    // REQUIRE FAIL-CLOSED ATTACHMENT
    // ========================================================

    let result = manager.attach(conflicting_source, eth_tx).await;

    assert!(
        matches!(
            result,
            Err(
                marketforge_engine::live::
                    infrastructure::websocket::
                    WebSocketManagerError::ConnectionSlot(_)
            )
        ),
        "conflicting routing identity must fail"
    );

    println!("[2] Conflicting routing update rejected");

    // ========================================================
    // ENTIRE AFFECTED SLOT MUST BE RETIRED
    // ========================================================

    assert_eq!(
        manager.slot_count().await,
        0,
        "failed slot must not remain registered"
    );

    assert_eq!(
        manager.stream_count().await,
        0,
        "failed slot must not retain logical subscriptions"
    );

    println!("[3] Affected connection slot retired");

    // ========================================================
    // BOTH RECEIVERS MUST TERMINATE
    // ========================================================

    // BTC may have valid market data queued before
    // retirement. Drain it, then require EOF.
    time::timeout(Duration::from_secs(10), async {
        while btc_rx.recv().await.is_some() {}
    })
    .await
    .expect("BTC destination remained open");

    println!("[4] Existing BTC destination closed");

    // ETH was never successfully established.
    assert!(
        time::timeout(Duration::from_secs(5), eth_rx.recv(),)
            .await
            .expect("ETH destination remained open")
            .is_none()
    );

    println!("[5] Rejected ETH destination closed");

    // The old binding is no longer valid.
    assert!(
        manager.detach(btc_binding).await.is_err(),
        "retired binding must be invalid"
    );

    manager.shutdown().await.unwrap();

    println!();
    println!("ROUTING CONFLICT FAIL-CLOSED TEST PASSED");
}
