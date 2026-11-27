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
supports_dynamic_subscriptions = true
supports_unsubscribe = true
request_id_field = "id"
request_id_type = "signed_integer"
success_field = "result"
success_is_null = true
error_field = "error"

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
async fn demand_change_reconciles_on_healthy_connection() {
    let listener = crate::websocket_server::listen().await;
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let (release_ack, wait_for_release) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(tcp).await.unwrap();
        acknowledge_subscriptions(&mut socket, &["btcusdt@trade", "ethusdt@trade"]).await;

        let removal = time::timeout(TIMEOUT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let request: Value = serde_json::from_str(removal.to_text().unwrap()).unwrap();
        assert_eq!(request["method"], "UNSUBSCRIBE");
        assert_eq!(request["params"], json!(["ethusdt@trade"]));
        socket
            .send(Message::Text(
                json!({"result":null,"id":request["id"]}).to_string().into(),
            ))
            .await
            .unwrap();

        let addition = time::timeout(TIMEOUT, socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let request: Value = serde_json::from_str(addition.to_text().unwrap()).unwrap();
        assert_eq!(request["method"], "SUBSCRIBE");
        assert_eq!(request["params"], json!(["xrpusdt@trade"]));
        socket
            .send(Message::Text(
                json!({"stream":"btcusdt@trade","price":"1"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        // A wrong request ID cannot authorize the new subscription.
        socket
            .send(Message::Text(
                json!({"result":null,"id":999999}).to_string().into(),
            ))
            .await
            .unwrap();
        wait_for_release.await.unwrap();
        socket
            .send(Message::Text(
                json!({"result":null,"id":request["id"]}).to_string().into(),
            ))
            .await
            .unwrap();
        while let Some(frame) = socket.next().await {
            if frame.is_err() || matches!(frame, Ok(Message::Close(_))) {
                break;
            }
        }
    });

    let exchange = Arc::new(config());
    let compiled =
        WebSocketLimiterCompiler::compile(&exchange, &WebSocketLimiterBindings::default()).unwrap();
    let limiter = RateLimitManager::new(compiled.shared.policies.clone(), 32).unwrap();
    let heartbeat = HeartbeatPlanCompiler::compile(&exchange).unwrap();
    let fence = ConnectionFence::new("reconciliation", "binance-public").unwrap();
    let observer = fence.clone();
    let recovery = SharedStreamRecovery::new();
    let authority = recovery.clone();
    let desired = Arc::new(RwLock::new(demand(1, &["BTCUSDT", "ETHUSDT"])));
    let reader = desired.clone();
    let (market_data, mut frames) = mpsc::channel(32);
    let (subscription_events, mut confirmations_rx) = mpsc::channel(16);
    let (events, _lifecycle) = mpsc::channel(64);
    let (shutdown, stop) = oneshot::channel();
    let runner = tokio::spawn(async move {
        ConnectionSlotRunner::run(
            &url,
            exchange,
            &compiled,
            &limiter,
            reconnect_policy(),
            stop,
            events,
            fence,
            recovery,
            None,
            move || reader.read().unwrap().clone(),
            move || heartbeat.clone(),
            BTreeMap::new(),
            ConnectionSessionOutputs {
                market_data,
                subscription_events,
            },
            ConnectionBootstrapLimits {
                maximum_pending: 16,
                acknowledgement_timeout: Duration::from_secs(3),
            },
            session_limits(),
        )
        .await
    });
    let initial = confirmations(&mut confirmations_rx, &["BTCUSDT:trades", "ETHUSDT:trades"]).await;
    let btc = &initial["BTCUSDT:trades"];
    let eth = &initial["ETHUSDT:trades"];
    *desired.write().unwrap() = demand(2, &["BTCUSDT", "XRPUSDT"]);
    let frame = time::timeout(TIMEOUT, frames.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(frame.to_text().unwrap()).unwrap()["stream"],
        "btcusdt@trade"
    );
    assert!(confirmations_rx.try_recv().is_err());
    release_ack.send(()).unwrap();
    let added = confirmations(&mut confirmations_rx, &["XRPUSDT:trades"]).await;
    let xrp = &added["XRPUSDT:trades"];
    assert_eq!(btc.connection(), xrp.connection());
    assert_eq!(observer.current_stamp().as_ref(), Some(btc.connection()));
    {
        let state = authority.lock().unwrap();
        assert_eq!(state.token("BTCUSDT:trades"), Some(btc));
        assert_eq!(
            state.phase("BTCUSDT:trades"),
            Some(StreamRecoveryPhase::AwaitingRecovery)
        );
        assert!(state.token("ETHUSDT:trades").is_none());
        assert!(!state.may_publish(&observer, eth));
        assert!(!state.may_publish(&observer, btc));
        assert!(!state.may_publish(&observer, xrp));
    }
    shutdown.send(()).unwrap();
    time::timeout(TIMEOUT, runner)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    time::timeout(TIMEOUT, server).await.unwrap().unwrap();
    assert!(!observer.accepts(btc.connection()));
}

// ============================================================
// W4.7-A1 — REAL BINANCE SPOT
// ============================================================

async fn local_socket(listener: &tokio::net::TcpListener) -> WebSocketStream<TcpStream> {
    let (tcp, _) = time::timeout(TIMEOUT, listener.accept())
        .await
        .unwrap()
        .unwrap();
    time::timeout(TIMEOUT, accept_async(tcp))
        .await
        .unwrap()
        .unwrap()
}

async fn native_command(
    socket: &mut WebSocketStream<TcpStream>,
    method: &str,
    stream: &str,
) -> Value {
    let frame = time::timeout(TIMEOUT, socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let request: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
    assert_eq!(request["method"], method);
    assert_eq!(request["params"], json!([stream]));
    request["id"].clone()
}

async fn reply(socket: &mut WebSocketStream<TcpStream>, id: Value) {
    socket
        .send(Message::Text(
            json!({"result":null,"id":id}).to_string().into(),
        ))
        .await
        .unwrap();
}

async fn observation(socket: &mut WebSocketStream<TcpStream>, stream: &str, number: u32) {
    socket
        .send(Message::Text(
            json!({"stream":stream,"number":number}).to_string().into(),
        ))
        .await
        .unwrap();
}

async fn input(
    receiver: &mut mpsc::Receiver<marketforge_engine::live::infrastructure::websocket::StreamInput>,
) -> marketforge_engine::live::infrastructure::websocket::StreamInput {
    time::timeout(TIMEOUT, receiver.recv())
        .await
        .unwrap()
        .unwrap()
}

async fn acquisition(
    receiver: &mut mpsc::Receiver<marketforge_engine::live::infrastructure::websocket::StreamInput>,
) -> marketforge_engine::live::infrastructure::websocket::StreamAcquisition {
    use marketforge_engine::live::infrastructure::websocket::StreamInput;
    loop {
        if let StreamInput::Connected(acquisition) = input(receiver).await {
            return acquisition;
        }
    }
}

fn managed_binance(
    listener: &tokio::net::TcpListener,
    capacity: u32,
) -> marketforge_engine::live::infrastructure::websocket::WebSocketManager {
    use marketforge_engine::live::infrastructure::websocket::{
        WebSocketManager, WebSocketManagerRuntime, WebSocketProfileRuntime,
    };
    let mut exchange = config();
    exchange.websocket.max_subscriptions_per_connection = Some(capacity.into());
    exchange.websocket.routing = Some(
        toml::from_str(
            r#"mode = "combined"
combined_stream_field = "stream""#,
        )
        .unwrap(),
    );
    let exchange = Arc::new(exchange);
    let compiled =
        WebSocketLimiterCompiler::compile(&exchange, &WebSocketLimiterBindings::default()).unwrap();
    let shared_limiter = RateLimitManager::new(compiled.shared.policies.clone(), 32).unwrap();
    WebSocketManager::from_runtime(WebSocketManagerRuntime {
        runtime_id: "managed-reconciliation".into(),
        profiles: vec![WebSocketProfileRuntime {
            heartbeat_plan: HeartbeatPlanCompiler::compile(&exchange).unwrap(),
            exchange,
            urls: BTreeMap::from([(None, format!("ws://{}", listener.local_addr().unwrap()))]),
            compiled_limiters: compiled,
            ack_bindings: BTreeMap::new(),
        }],
        shared_limiter,
        reconnect_policy: reconnect_policy(),
        bootstrap_limits: ConnectionBootstrapLimits {
            maximum_pending: 16,
            acknowledgement_timeout: Duration::from_secs(3),
        },
        session_limits: session_limits(),
        event_capacity: 64,
        maximum_early_frames: 16,
    })
    .unwrap()
}

async fn attach(
    manager: &marketforge_engine::live::infrastructure::websocket::WebSocketManager,
    symbol: &str,
) -> (
    marketforge_engine::live::infrastructure::websocket::StreamBinding,
    mpsc::Receiver<marketforge_engine::live::infrastructure::websocket::StreamInput>,
) {
    use marketforge_engine::live::infrastructure::websocket::StreamRequest;
    let request = StreamRequest::new("binance_spot", symbol, "trades").unwrap();
    let (tx, rx) = mpsc::channel(32);
    let binding = time::timeout(TIMEOUT, manager.attach_request(request, tx))
        .await
        .unwrap()
        .unwrap();
    (binding, rx)
}

async fn expect_observation(
    receiver: &mut mpsc::Receiver<marketforge_engine::live::infrastructure::websocket::StreamInput>,
    expected: &marketforge_engine::live::infrastructure::websocket::StreamAcquisition,
    number: u32,
) {
    use marketforge_engine::live::infrastructure::websocket::StreamInput;
    let StreamInput::Data {
        acquisition,
        message,
    } = input(receiver).await
    else {
        panic!("surviving stream should receive data without a new acquisition");
    };
    assert_eq!(&acquisition, expected);
    let message: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
    assert_eq!(message["number"], number);
}

async fn close_peer(mut socket: WebSocketStream<TcpStream>) {
    while let Some(Ok(frame)) = socket.next().await {
        if matches!(frame, Message::Close(_)) {
            let _ = socket.flush().await;
            break;
        }
    }
}

#[tokio::test]
async fn managed_removal_discards_only_late_retired_frames_and_reattaches_safely() {
    let listener = crate::websocket_server::listen().await;
    let manager = managed_binance(&listener, 2);
    let (btc_binding, mut btc) = attach(&manager, "BTCUSDT").await;
    let mut socket = local_socket(&listener).await;
    acknowledge_subscriptions(&mut socket, &["btcusdt@trade"]).await;
    let btc_epoch = acquisition(&mut btc).await;

    let (eth_binding, mut eth) = attach(&manager, "ETHUSDT").await;
    let eth_subscribe = native_command(&mut socket, "SUBSCRIBE", "ethusdt@trade").await;
    observation(&mut socket, "ethusdt@trade", 1).await;
    observation(&mut socket, "ethusdt@trade", 2).await;
    observation(&mut socket, "btcusdt@trade", 3).await;
    expect_observation(&mut btc, &btc_epoch, 3).await;
    assert!(eth.try_recv().is_err());
    reply(&mut socket, eth_subscribe).await;
    let eth_epoch = acquisition(&mut eth).await;
    assert_eq!(eth_epoch.connection, btc_epoch.connection);
    expect_observation(&mut eth, &eth_epoch, 1).await;
    expect_observation(&mut eth, &eth_epoch, 2).await;

    manager.detach(eth_binding).await.unwrap();
    let retire = native_command(&mut socket, "UNSUBSCRIBE", "ethusdt@trade").await;
    assert!(eth.recv().await.is_none());
    // The old server may still send data before or after processing unsubscribe.
    observation(&mut socket, "ethusdt@trade", 4).await;
    observation(&mut socket, "btcusdt@trade", 5).await;
    expect_observation(&mut btc, &btc_epoch, 5).await;

    // Fresh attachment authority progresses while the old slot waits for an ACK.
    let (replacement, mut new_eth) = attach(&manager, "ETHUSDT").await;
    let mut other = local_socket(&listener).await;
    acknowledge_subscriptions(&mut other, &["ethusdt@trade"]).await;
    let new_epoch = acquisition(&mut new_eth).await;
    assert_ne!(new_epoch.connection, eth_epoch.connection);
    assert_eq!(manager.slot_count().await, 2);
    reply(&mut socket, retire).await;
    observation(&mut socket, "ethusdt@trade", 6).await;
    observation(&mut socket, "btcusdt@trade", 7).await;
    expect_observation(&mut btc, &btc_epoch, 7).await;
    observation(&mut other, "ethusdt@trade", 8).await;
    expect_observation(&mut new_eth, &new_epoch, 8).await;

    let (left, right, _, _) = time::timeout(TIMEOUT, async {
        tokio::join!(
            manager.detach(btc_binding),
            manager.detach(replacement),
            close_peer(socket),
            close_peer(other)
        )
    })
    .await
    .unwrap();
    left.unwrap();
    right.unwrap();
    assert_eq!(manager.slot_count().await, 0);
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn managed_withdrawal_before_ack_and_socket_failure_restore_latest_demand() {
    let listener = crate::websocket_server::listen().await;
    let manager = managed_binance(&listener, 8);
    let (_, mut btc) = attach(&manager, "BTCUSDT").await;
    let mut socket = local_socket(&listener).await;
    let btc_subscribe = native_command(&mut socket, "SUBSCRIBE", "btcusdt@trade").await;
    let (eth_binding, mut eth) = attach(&manager, "ETHUSDT").await;
    // ETH became desired while the initial restoration ACK was outstanding.
    reply(&mut socket, btc_subscribe).await;
    let previous = acquisition(&mut btc).await;

    let eth_subscribe = native_command(&mut socket, "SUBSCRIBE", "ethusdt@trade").await;
    let ((sol_binding, mut sol), (xrp_binding, mut xrp)) =
        tokio::join!(attach(&manager, "SOLUSDT"), attach(&manager, "XRPUSDT"));
    let (eth_removed, sol_removed) =
        tokio::join!(manager.detach(eth_binding), manager.detach(sol_binding));
    eth_removed.unwrap();
    sol_removed.unwrap();
    observation(&mut socket, "ethusdt@trade", 1).await;
    reply(&mut socket, eth_subscribe).await;
    let eth_unsubscribe = native_command(&mut socket, "UNSUBSCRIBE", "ethusdt@trade").await;
    assert!(eth.recv().await.is_none());
    assert!(sol.recv().await.is_none());
    observation(&mut socket, "btcusdt@trade", 2).await;
    expect_observation(&mut btc, &previous, 2).await;
    reply(&mut socket, eth_unsubscribe).await;

    let _unconfirmed = native_command(&mut socket, "SUBSCRIBE", "xrpusdt@trade").await;
    manager.detach(xrp_binding).await.unwrap();
    let (_, mut ada) = attach(&manager, "ADAUSDT").await;
    socket.close(None).await.unwrap();
    drop(socket);

    let mut replacement = local_socket(&listener).await;
    acknowledge_subscriptions(&mut replacement, &["adausdt@trade", "btcusdt@trade"]).await;
    let btc_epoch = acquisition(&mut btc).await;
    let ada_epoch = acquisition(&mut ada).await;
    assert_ne!(previous.connection, btc_epoch.connection);
    assert!(btc_epoch.generation > previous.generation);
    assert_eq!(btc_epoch.connection, ada_epoch.connection);
    assert!(xrp.recv().await.is_none());
    let (shutdown, _) = time::timeout(TIMEOUT, async {
        tokio::join!(manager.shutdown(), close_peer(replacement))
    })
    .await
    .unwrap();
    shutdown.unwrap();
    assert_eq!(manager.stream_count().await, 0);
}

#[tokio::test]
async fn uncertain_native_rejection_or_timeout_recycles_generation() {
    for reject in [true, false] {
        let listener = crate::websocket_server::listen().await;
        let manager = managed_binance(&listener, 4);
        let (_, mut btc) = attach(&manager, "BTCUSDT").await;
        let mut socket = local_socket(&listener).await;
        acknowledge_subscriptions(&mut socket, &["btcusdt@trade"]).await;
        let previous = acquisition(&mut btc).await;

        let (_, mut eth) = attach(&manager, "ETHUSDT").await;
        let id = native_command(&mut socket, "SUBSCRIBE", "ethusdt@trade").await;
        if reject {
            socket
                .send(Message::Text(
                    json!({"id":id,"error":{"code":2,"msg":"rejected synthetic subscription"}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
        }
        let mut replacement = local_socket(&listener).await;
        assert!(eth.try_recv().is_err());
        acknowledge_subscriptions(&mut replacement, &["btcusdt@trade", "ethusdt@trade"]).await;
        let btc_epoch = acquisition(&mut btc).await;
        let eth_epoch = acquisition(&mut eth).await;
        assert_ne!(previous.connection, btc_epoch.connection);
        assert_eq!(btc_epoch.connection, eth_epoch.connection);
        let (shutdown, _) = time::timeout(TIMEOUT, async {
            tokio::join!(manager.shutdown(), close_peer(replacement))
        })
        .await
        .unwrap();
        shutdown.unwrap();
        assert_eq!(manager.slot_count().await, 0);
    }
}

#[tokio::test]
async fn dynamic_early_frame_overflow_fails_closed() {
    let listener = crate::websocket_server::listen().await;
    let manager = managed_binance(&listener, 4);
    let (_, mut btc) = attach(&manager, "BTCUSDT").await;
    let mut socket = local_socket(&listener).await;
    acknowledge_subscriptions(&mut socket, &["btcusdt@trade"]).await;
    acquisition(&mut btc).await;
    let (_, mut eth) = attach(&manager, "ETHUSDT").await;
    native_command(&mut socket, "SUBSCRIBE", "ethusdt@trade").await;
    for number in 0..17 {
        observation(&mut socket, "ethusdt@trade", number).await;
    }
    assert!(time::timeout(TIMEOUT, eth.recv()).await.unwrap().is_none());
    assert!(time::timeout(TIMEOUT, btc.recv()).await.unwrap().is_none());
    assert_eq!(manager.slot_count().await, 0);
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn manager_shutdown_cancels_pending_native_transaction() {
    let listener = crate::websocket_server::listen().await;
    let manager = managed_binance(&listener, 4);
    let (_, mut btc) = attach(&manager, "BTCUSDT").await;
    let mut socket = local_socket(&listener).await;
    native_command(&mut socket, "SUBSCRIBE", "btcusdt@trade").await;
    let (shutdown, _) = time::timeout(TIMEOUT, async {
        tokio::join!(manager.shutdown(), close_peer(socket))
    })
    .await
    .unwrap();
    shutdown.unwrap();
    assert!(btc.recv().await.is_none());
    assert_eq!(manager.slot_count().await, 0);
    assert_eq!(manager.stream_count().await, 0);
}

#[tokio::test]
async fn managed_capacity_is_reusable_after_unsubscribe_acknowledgment() {
    let listener = crate::websocket_server::listen().await;
    let manager = managed_binance(&listener, 3);
    let (_, mut btc) = attach(&manager, "BTCUSDT").await;
    let mut socket = local_socket(&listener).await;
    acknowledge_subscriptions(&mut socket, &["btcusdt@trade"]).await;
    let first = acquisition(&mut btc).await;
    let (eth_binding, mut eth) = attach(&manager, "ETHUSDT").await;
    acknowledge_subscriptions(&mut socket, &["ethusdt@trade"]).await;
    acquisition(&mut eth).await;

    manager.detach(eth_binding).await.unwrap();
    let retired = native_command(&mut socket, "UNSUBSCRIBE", "ethusdt@trade").await;
    let (_, mut sol) = attach(&manager, "SOLUSDT").await;
    reply(&mut socket, retired).await;
    let sol_id = native_command(&mut socket, "SUBSCRIBE", "solusdt@trade").await;

    // SOL uses the previously free slot position. ADA needs the position released by ETH.
    let (_, mut ada) = attach(&manager, "ADAUSDT").await;
    assert_eq!(manager.slot_count().await, 1);
    reply(&mut socket, sol_id).await;
    let ada_id = native_command(&mut socket, "SUBSCRIBE", "adausdt@trade").await;
    reply(&mut socket, ada_id).await;
    assert_eq!(acquisition(&mut sol).await.connection, first.connection);
    assert_eq!(acquisition(&mut ada).await.connection, first.connection);
    let (shutdown, _) = time::timeout(TIMEOUT, async {
        tokio::join!(manager.shutdown(), close_peer(socket))
    })
    .await
    .unwrap();
    shutdown.unwrap();
}
