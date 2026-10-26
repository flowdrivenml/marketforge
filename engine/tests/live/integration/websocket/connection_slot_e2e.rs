use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};

use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::{
        limiter::{
            RateLimitManager,
            factory::{WebSocketLimiterBindings, WebSocketLimiterCompiler},
        },
        websocket::{
            ConnectionBootstrapLimits, ConnectionFence, ConnectionSessionLimits,
            ConnectionSessionOutputs, ConnectionSlotRunner, DesiredSubscription,
            DesiredSubscriptionSnapshot, HeartbeatPlanCompiler, ReconnectEvent, ReconnectPolicy,
            SharedStreamRecovery, StreamRecoveryPhase, StreamRecoveryToken,
            SubscriptionDriverEvent, SubscriptionDriverLimits,
        },
    },
};

// ============================================================
// TEST LIMITS
// ============================================================

const TIMEOUT: Duration = Duration::from_secs(5);

// ============================================================
// EXCHANGE CONFIGURATION
// ============================================================

fn config() -> ExchangeConfig {
    toml::from_str(
        r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid"

[websocket.heartbeat]
strategy = "websocket_control_frames"
respond_to_ping = true
echo_ping_payload = true

[websocket.subscription]
strategy = "json_rpc"
subscribe_method = "SUBSCRIBE"
unsubscribe_method = "UNSUBSCRIBE"
request_id_field = "id"
request_id_type = "signed_integer"
success_field = "result"
success_is_null = true

[channels.trades]
enabled = true
format = "trades/binance_spot"
stream_template = "{symbol_lower}@trade"

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap()
}

// ============================================================
// DESIRED DEMAND
// ============================================================

fn demand(revision: u64, symbols: &[&str]) -> DesiredSubscriptionSnapshot {
    let subscriptions = symbols
        .iter()
        .map(|symbol| {
            DesiredSubscription::new(format!("{symbol}:trades"), "trades", *symbol).unwrap()
        })
        .collect::<Vec<_>>();

    DesiredSubscriptionSnapshot::new(revision, subscriptions).unwrap()
}

// ============================================================
// RECONNECT POLICY
// ============================================================

fn reconnect_policy() -> ReconnectPolicy {
    ReconnectPolicy::new(
        Duration::from_millis(30),
        Duration::from_millis(100),
        Duration::from_secs(10),
        Duration::from_secs(2),
        None,
    )
    .unwrap()
}

// ============================================================
// SESSION LIMITS
// ============================================================

fn session_limits() -> ConnectionSessionLimits {
    ConnectionSessionLimits {
        inbound_capacity: 64,
        outbound_capacity: 32,
        command_capacity: 16,
        event_capacity: 16,

        dispatch_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(2),

        driver: SubscriptionDriverLimits {
            queue_timeout: Duration::from_secs(2),
            receipt_timeout: Duration::from_secs(2),
            maximum_early_frames: 64,
        },
    }
}

// ============================================================
// LOCAL EXCHANGE SERVER
// ============================================================

/// Read the exact expected native subscription requests
/// and send matching Binance-style ACKs.
///
/// These ACKs are controlled protocol fixtures, not
/// network captures from Binance.
async fn acknowledge_subscriptions(
    socket: &mut WebSocketStream<TcpStream>,
    expected_streams: &[&str],
) {
    let mut observed = BTreeSet::new();

    for _ in expected_streams {
        let frame = time::timeout(TIMEOUT, socket.next())
            .await
            .expect("subscription timed out")
            .expect("socket closed before subscription")
            .expect("invalid WebSocket frame");

        let Message::Text(text) = frame else {
            panic!("expected subscription text message");
        };

        let request: Value =
            serde_json::from_str(text.as_str()).expect("invalid subscription JSON");

        assert_eq!(request["method"], "SUBSCRIBE",);

        let arguments = request["params"]
            .as_array()
            .expect("subscription params must be an array");

        assert_eq!(
            arguments.len(),
            1,
            "expected one native stream per transaction",
        );

        let stream = arguments[0]
            .as_str()
            .expect("native stream must be a string");

        assert!(
            observed.insert(stream.to_owned()),
            "duplicate subscription: {stream}",
        );

        let request_id = request
            .get("id")
            .expect("missing native request ID")
            .clone();

        assert!(
            request_id.as_i64().is_some(),
            "expected configured signed integer request ID",
        );

        let acknowledgement = json!({
            "result": null,
            "id": request_id,
        });

        socket
            .send(Message::Text(acknowledgement.to_string().into()))
            .await
            .expect("failed to send subscription ACK");
    }

    let expected = expected_streams
        .iter()
        .map(|stream| (*stream).to_owned())
        .collect::<BTreeSet<_>>();

    assert_eq!(
        observed, expected,
        "native subscriptions differed from desired demand",
    );
}

// ============================================================
// RECEIVE CONFIRMED SUBSCRIPTIONS
// ============================================================

async fn confirmations(
    receiver: &mut mpsc::Receiver<SubscriptionDriverEvent>,
    expected: &[&str],
) -> BTreeMap<String, StreamRecoveryToken> {
    let mut confirmed = BTreeMap::new();

    for _ in expected {
        let event = time::timeout(TIMEOUT, receiver.recv())
            .await
            .expect("subscription confirmation timed out")
            .expect("subscription event channel closed");

        let SubscriptionDriverEvent::Confirmed { token, .. } = event else {
            panic!("subscription failed unexpectedly: {event:?}");
        };

        let stream_id = token.stream_id().to_owned();

        assert!(
            confirmed.insert(stream_id, token).is_none(),
            "duplicate confirmation",
        );
    }

    let expected = expected
        .iter()
        .map(|stream| (*stream).to_owned())
        .collect::<BTreeSet<_>>();

    let actual = confirmed.keys().cloned().collect::<BTreeSet<_>>();

    assert_eq!(actual, expected);

    confirmed
}

// ============================================================
// END-TO-END TEST
// ============================================================

#[tokio::test]
async fn connection_slot_reconnects_and_restores_current_demand() {
    // --------------------------------------------------------
    // START LOOPBACK SERVER
    // --------------------------------------------------------

    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let url = format!("ws://{address}");

    // The server cannot disconnect generation 1 until
    // the test confirms that both ACKs were processed.
    let (disconnect_first_tx, disconnect_first_rx) = oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        // ====================================================
        // PHYSICAL GENERATION 1
        // ====================================================

        let (tcp, _) = time::timeout(TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();

        let mut first = accept_async(tcp).await.unwrap();

        acknowledge_subscriptions(&mut first, &["btcusdt@trade", "ethusdt@trade"]).await;

        // Do not disconnect until the test has:
        //
        // - Received both validated confirmations.
        // - Updated authoritative demand.
        disconnect_first_rx.await.unwrap();

        first.close(None).await.unwrap();

        drop(first);

        // ====================================================
        // PHYSICAL GENERATION 2
        // ====================================================

        let (tcp, _) = time::timeout(TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();

        let mut second = accept_async(tcp).await.unwrap();

        acknowledge_subscriptions(&mut second, &["btcusdt@trade", "xrpusdt@trade"]).await;

        // Keep generation 2 alive until the runner shuts down.
        //
        // The test controls shutdown explicitly.
        while let Some(frame) = second.next().await {
            if frame.is_err() || matches!(frame, Ok(Message::Close(_))) {
                break;
            }
        }
    });

    // --------------------------------------------------------
    // COMPILE REAL MARKETFORGE INFRASTRUCTURE
    // --------------------------------------------------------

    let exchange = Arc::new(config());

    let compiled =
        WebSocketLimiterCompiler::compile(&exchange, &WebSocketLimiterBindings::default()).unwrap();

    let shared_limiter = RateLimitManager::new(compiled.shared.policies.clone(), 32).unwrap();

    let heartbeat = HeartbeatPlanCompiler::compile(&exchange).unwrap();

    // --------------------------------------------------------
    // PERSISTENT CONNECTION-SLOT AUTHORITY
    // --------------------------------------------------------

    let fence = ConnectionFence::new("integration-runtime", "binance-public-slot").unwrap();

    let observer_fence = fence.clone();

    let recovery = SharedStreamRecovery::new();

    let observer_recovery = recovery.clone();

    // --------------------------------------------------------
    // AUTHORITATIVE TEST DEMAND
    // --------------------------------------------------------

    let desired = Arc::new(RwLock::new(demand(1, &["BTCUSDT", "ETHUSDT"])));

    let demand_reader = desired.clone();

    // --------------------------------------------------------
    // OBSERVABILITY CHANNELS
    // --------------------------------------------------------

    let (market_tx, _market_rx) = mpsc::channel(64);

    let (subscription_tx, mut subscription_rx) = mpsc::channel(16);

    let (events_tx, mut events_rx) = mpsc::channel(64);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // --------------------------------------------------------
    // LAUNCH ACTUAL CONNECTION SLOT RUNNER
    // --------------------------------------------------------

    let runner = tokio::spawn(async move {
        ConnectionSlotRunner::run(
            &url,
            exchange,
            &compiled,
            &shared_limiter,
            reconnect_policy(),
            shutdown_rx,
            events_tx,
            fence,
            recovery,
            None,
            move || demand_reader.read().unwrap().clone(),
            move || heartbeat.clone(),
            BTreeMap::new(),
            ConnectionSessionOutputs {
                market_data: market_tx,
                subscription_events: subscription_tx,
            },
            ConnectionBootstrapLimits {
                maximum_pending: 16,
                acknowledgement_timeout: Duration::from_secs(3),
            },
            session_limits(),
        )
        .await
    });

    // --------------------------------------------------------
    // VERIFY GENERATION 1 ACKS
    // --------------------------------------------------------

    let first = confirmations(&mut subscription_rx, &["BTCUSDT:trades", "ETHUSDT:trades"]).await;

    let first_btc = first["BTCUSDT:trades"].clone();
    let first_eth = first["ETHUSDT:trades"].clone();

    {
        let guard = observer_recovery.lock().unwrap();

        for token in first.values() {
            assert_eq!(
                guard.phase(token.stream_id()),
                Some(StreamRecoveryPhase::AwaitingRecovery),
            );

            // Subscription confirmed != stream ready.
            assert!(!guard.may_publish(&observer_fence, token,));
        }
    }

    // --------------------------------------------------------
    // CHANGE DEMAND BEFORE DISCONNECTING
    // --------------------------------------------------------

    {
        let mut guard = desired.write().unwrap();

        *guard = demand(2, &["BTCUSDT", "XRPUSDT"]);
    }

    // Release the scripted disconnection.
    disconnect_first_tx.send(()).unwrap();

    // --------------------------------------------------------
    // OBSERVE GENERATION 1 REVOCATION
    // --------------------------------------------------------

    time::timeout(TIMEOUT, async {
        loop {
            match events_rx.recv().await.unwrap() {
                ReconnectEvent::Disconnected { generation, .. }
                    if generation == first_btc.connection().generation() =>
                {
                    break;
                }

                _ => {}
            }
        }
    })
    .await
    .expect("first connection was not revoked");

    assert!(!observer_fence.accepts(first_btc.connection()));

    // --------------------------------------------------------
    // VERIFY GENERATION 2 ACKS
    // --------------------------------------------------------

    let second = confirmations(&mut subscription_rx, &["BTCUSDT:trades", "XRPUSDT:trades"]).await;

    let second_btc = &second["BTCUSDT:trades"];

    assert_ne!(first_btc.connection(), second_btc.connection(),);

    assert!(second_btc.stream_generation() > first_btc.stream_generation());

    assert!(second_btc.subscription_generation() > first_btc.subscription_generation());

    // --------------------------------------------------------
    // VERIFY PERSISTENT RECOVERY AUTHORITY
    // --------------------------------------------------------

    {
        let guard = observer_recovery.lock().unwrap();

        // BTC was rebound to a fresh physical generation.
        assert_eq!(guard.token("BTCUSDT:trades"), Some(second_btc),);

        // XRP is now desired and subscribed.
        assert!(guard.token("XRPUSDT:trades").is_some());

        // ETH demand was removed during reconnection.
        assert!(guard.kind("ETHUSDT:trades").is_none());

        // Old generation tokens have no authority.
        assert!(!guard.may_publish(&observer_fence, &first_btc,));

        assert!(!guard.may_publish(&observer_fence, &first_eth,));

        // New subscription ACK does not authorize
        // market-data publication until integrity recovery.
        assert!(!guard.may_publish(&observer_fence, second_btc,));

        assert_eq!(
            guard.phase("BTCUSDT:trades"),
            Some(StreamRecoveryPhase::AwaitingRecovery),
        );

        assert_eq!(
            guard.phase("XRPUSDT:trades"),
            Some(StreamRecoveryPhase::AwaitingRecovery),
        );
    }

    // --------------------------------------------------------
    // CONTROLLED TERMINAL SHUTDOWN
    // --------------------------------------------------------

    shutdown_tx.send(()).unwrap();

    time::timeout(TIMEOUT, runner)
        .await
        .expect("connection runner did not terminate")
        .expect("runner task panicked")
        .expect("runner returned an error");

    time::timeout(TIMEOUT, server)
        .await
        .expect("server task did not terminate")
        .expect("server task panicked");

    // The stopped runner must revoke its final generation.
    assert!(!observer_fence.accepts(second_btc.connection()));
}

#[tokio::test]
async fn demand_change_reconnects_healthy_connection() {
    // --------------------------------------------------------
    // LOOPBACK SERVER
    // --------------------------------------------------------

    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();
    let url = format!("ws://{address}");

    let (server_first_closed_tx, server_first_closed_rx) = oneshot::channel::<()>();

    let server = tokio::spawn(async move {
        // ====================================================
        // GENERATION 1
        // ====================================================

        let (tcp, _) = time::timeout(TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();

        let mut first = accept_async(tcp).await.unwrap();

        acknowledge_subscriptions(&mut first, &["btcusdt@trade", "ethusdt@trade"]).await;

        // Unlike the previous test, the server does NOT
        // deliberately disconnect this connection.
        //
        // The connection must be closed by MarketForge
        // in response to the demand change.

        let closed = time::timeout(TIMEOUT, first.next())
            .await
            .expect("MarketForge did not close the stale connection");

        assert!(
            matches!(closed, None | Some(Ok(Message::Close(_))) | Some(Err(_))),
            "unexpected frame on old connection: {closed:?}"
        );

        drop(first);

        server_first_closed_tx.send(()).unwrap();

        // ====================================================
        // GENERATION 2
        // ====================================================

        let (tcp, _) = time::timeout(TIMEOUT, listener.accept())
            .await
            .unwrap()
            .unwrap();

        let mut second = accept_async(tcp).await.unwrap();

        acknowledge_subscriptions(&mut second, &["btcusdt@trade", "xrpusdt@trade"]).await;

        // Keep the second generation connected until
        // the test explicitly shuts down the runner.
        while let Some(frame) = second.next().await {
            if frame.is_err() || matches!(frame, Ok(Message::Close(_))) {
                break;
            }
        }
    });

    // --------------------------------------------------------
    // REAL MARKETFORGE INFRASTRUCTURE
    // --------------------------------------------------------

    let exchange = Arc::new(config());

    let compiled =
        WebSocketLimiterCompiler::compile(&exchange, &WebSocketLimiterBindings::default()).unwrap();

    let shared_limiter = RateLimitManager::new(compiled.shared.policies.clone(), 32).unwrap();

    let heartbeat = HeartbeatPlanCompiler::compile(&exchange).unwrap();

    // --------------------------------------------------------
    // PERSISTENT CONNECTION-SLOT AUTHORITY
    // --------------------------------------------------------

    let fence = ConnectionFence::new("demand-reconcile-runtime", "binance-public-slot").unwrap();

    let observer_fence = fence.clone();

    let recovery = SharedStreamRecovery::new();
    let observer_recovery = recovery.clone();

    // --------------------------------------------------------
    // AUTHORITATIVE DEMAND
    // --------------------------------------------------------

    let desired = Arc::new(RwLock::new(demand(1, &["BTCUSDT", "ETHUSDT"])));

    let demand_reader = desired.clone();

    // --------------------------------------------------------
    // RUNTIME CHANNELS
    // --------------------------------------------------------

    let (market_tx, _market_rx) = mpsc::channel(64);

    let (subscription_tx, mut subscription_rx) = mpsc::channel(16);

    let (events_tx, mut events_rx) = mpsc::channel(64);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // --------------------------------------------------------
    // START ACTUAL CONNECTION SLOT RUNNER
    // --------------------------------------------------------

    let runner = tokio::spawn(async move {
        ConnectionSlotRunner::run(
            &url,
            exchange,
            &compiled,
            &shared_limiter,
            reconnect_policy(),
            shutdown_rx,
            events_tx,
            fence,
            recovery,
            None,
            move || demand_reader.read().unwrap().clone(),
            move || heartbeat.clone(),
            BTreeMap::new(),
            ConnectionSessionOutputs {
                market_data: market_tx,
                subscription_events: subscription_tx,
            },
            ConnectionBootstrapLimits {
                maximum_pending: 16,
                acknowledgement_timeout: Duration::from_secs(3),
            },
            session_limits(),
        )
        .await
    });

    // --------------------------------------------------------
    // CONFIRM INITIAL SUBSCRIPTIONS
    // --------------------------------------------------------

    let first = confirmations(&mut subscription_rx, &["BTCUSDT:trades", "ETHUSDT:trades"]).await;

    let first_btc = first["BTCUSDT:trades"].clone();

    let first_eth = first["ETHUSDT:trades"].clone();

    // The first connection is healthy and its
    // subscriptions have been acknowledged.
    //
    // We have not instructed the server to disconnect.

    {
        let guard = observer_recovery.lock().unwrap();

        assert_eq!(
            guard.phase("BTCUSDT:trades"),
            Some(StreamRecoveryPhase::AwaitingRecovery),
        );

        assert_eq!(
            guard.phase("ETHUSDT:trades"),
            Some(StreamRecoveryPhase::AwaitingRecovery),
        );
    }

    // --------------------------------------------------------
    // CHANGE DEMAND WHILE CONNECTION IS HEALTHY
    // --------------------------------------------------------

    {
        let mut guard = desired.write().unwrap();

        *guard = demand(2, &["BTCUSDT", "XRPUSDT"]);
    }

    // --------------------------------------------------------
    // VERIFY AUTOMATIC DISCONNECTION
    // --------------------------------------------------------

    time::timeout(TIMEOUT, server_first_closed_rx)
        .await
        .expect("demand change did not close the old WebSocket")
        .expect("server closure notification failed");

    // --------------------------------------------------------
    // VERIFY OLD GENERATION REVOCATION
    // --------------------------------------------------------

    time::timeout(TIMEOUT, async {
        loop {
            let event = events_rx
                .recv()
                .await
                .expect("reconnect event channel closed");

            if let ReconnectEvent::Disconnected { generation, .. } = event {
                if generation == first_btc.connection().generation() {
                    break;
                }
            }
        }
    })
    .await
    .expect("old generation was not revoked");

    assert!(!observer_fence.accepts(first_btc.connection()));

    // --------------------------------------------------------
    // VERIFY NEW DEMAND WAS RESTORED
    // --------------------------------------------------------

    let second = confirmations(&mut subscription_rx, &["BTCUSDT:trades", "XRPUSDT:trades"]).await;

    let second_btc = &second["BTCUSDT:trades"];

    assert_ne!(first_btc.connection(), second_btc.connection(),);

    assert!(second_btc.stream_generation() > first_btc.stream_generation());

    assert!(second_btc.subscription_generation() > first_btc.subscription_generation());

    // --------------------------------------------------------
    // VERIFY PERSISTENT RECOVERY STATE
    // --------------------------------------------------------

    {
        let guard = observer_recovery.lock().unwrap();

        assert_eq!(guard.token("BTCUSDT:trades"), Some(second_btc),);

        assert!(guard.token("XRPUSDT:trades").is_some());

        // ETH no longer has authoritative demand.
        assert!(guard.kind("ETHUSDT:trades").is_none());

        // Neither generation has completed integrity
        // recovery, so neither may publish.
        assert!(!guard.may_publish(&observer_fence, &first_btc,));

        assert!(!guard.may_publish(&observer_fence, &first_eth,));

        assert!(!guard.may_publish(&observer_fence, second_btc,));

        assert_eq!(
            guard.phase("BTCUSDT:trades"),
            Some(StreamRecoveryPhase::AwaitingRecovery),
        );

        assert_eq!(
            guard.phase("XRPUSDT:trades"),
            Some(StreamRecoveryPhase::AwaitingRecovery),
        );
    }

    // --------------------------------------------------------
    // TERMINAL SHUTDOWN
    // --------------------------------------------------------

    shutdown_tx.send(()).unwrap();

    time::timeout(TIMEOUT, runner)
        .await
        .expect("runner shutdown timed out")
        .expect("runner task panicked")
        .expect("runner failed");

    time::timeout(TIMEOUT, server)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked");

    assert!(!observer_fence.accepts(second_btc.connection()));
}

// ============================================================
// W4.7-A1 — REAL BINANCE SPOT
// ============================================================
