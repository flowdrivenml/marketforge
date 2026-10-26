use std::{collections::BTreeMap, sync::Arc, time::Duration};

use serde_json::Value;

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::tungstenite::Message;

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
            SharedStreamRecovery, SubscriptionDriverEvent, SubscriptionDriverLimits,
        },
    },
};

// ============================================================
// TEST LIMITS
// ============================================================

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

// ============================================================
// RECEIVE CONFIRMED SUBSCRIPTIONS
// ============================================================

// ============================================================
// END-TO-END TEST
// ============================================================

// ============================================================
// W4.7-A1 — REAL BINANCE SPOT
// ============================================================

#[tokio::test]
#[ignore = "requires internet access to Binance Spot"]
async fn binance_spot_live_trades() {
    const LIVE_URL: &str = "wss://stream.binance.com:443/ws";

    const LIVE_TIMEOUT: Duration = Duration::from_secs(30);

    // --------------------------------------------------------
    // REAL EXCHANGE CONFIGURATIOe
    // --------------------------------------------------------

    let exchange = Arc::new(config());

    let compiled =
        WebSocketLimiterCompiler::compile(&exchange, &WebSocketLimiterBindings::default()).unwrap();

    let shared_limiter = RateLimitManager::new(compiled.shared.policies.clone(), 32).unwrap();

    let heartbeat = HeartbeatPlanCompiler::compile(&exchange).unwrap();

    // --------------------------------------------------------
    // CONNECTION-SLOT AUTHORITY
    // --------------------------------------------------------

    let fence = ConnectionFence::new("binance-live-test", "spot-public").unwrap();

    let recovery = SharedStreamRecovery::new();

    // --------------------------------------------------------
    // DESIRED SUBSCRIPTION
    // --------------------------------------------------------

    let desired = demand(1, &["BTCUSDT"]);

    // --------------------------------------------------------
    // OUTPUT CHANNELS
    // --------------------------------------------------------

    let (market_tx, mut market_rx) = mpsc::channel(1024);

    let (subscription_tx, mut subscription_rx) = mpsc::channel(32);

    let (events_tx, mut events_rx) = mpsc::channel(64);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // --------------------------------------------------------
    // START PRODUCTION CONNECTION SLOT RUNNER
    // --------------------------------------------------------

    let runner = tokio::spawn(async move {
        ConnectionSlotRunner::run(
            LIVE_URL,
            exchange,
            &compiled,
            &shared_limiter,
            reconnect_policy(),
            shutdown_rx,
            events_tx,
            fence,
            recovery,
            None,
            move || desired.clone(),
            move || heartbeat.clone(),
            BTreeMap::new(),
            ConnectionSessionOutputs {
                market_data: market_tx,
                subscription_events: subscription_tx,
            },
            ConnectionBootstrapLimits {
                maximum_pending: 16,
                acknowledgement_timeout: Duration::from_secs(5),
            },
            session_limits(),
        )
        .await
    });

    // --------------------------------------------------------
    // OBSERVE ACTUAL BINANCE CONNECTION
    // --------------------------------------------------------

    let connected = time::timeout(LIVE_TIMEOUT, async {
        loop {
            match events_rx.recv().await {
                Some(ReconnectEvent::Connected {
                    generation,
                    connection_id,
                }) => {
                    println!(
                        "BINANCE CONNECTED: generation={}, connection_id={}",
                        generation.get(),
                        connection_id,
                    );

                    break;
                }

                Some(event) => {
                    println!("RECONNECT EVENT: {event:?}");
                }

                None => {
                    panic!("reconnect event channel closed");
                }
            }
        }
    })
    .await;

    assert!(
        connected.is_ok(),
        "could not establish Binance WebSocket connection"
    );

    // --------------------------------------------------------
    // VERIFY REAL SUBSCRIPTION ACK
    // --------------------------------------------------------

    let confirmed = time::timeout(LIVE_TIMEOUT, async {
        loop {
            match subscription_rx.recv().await {
                Some(SubscriptionDriverEvent::Confirmed { token, .. }) => {
                    println!("SUBSCRIPTION CONFIRMED: {}", token.stream_id(),);

                    assert_eq!(token.stream_id(), "BTCUSDT:trades",);

                    break;
                }

                Some(event) => {
                    panic!("unexpected subscription event: {event:?}");
                }

                None => {
                    panic!("subscription event channel closed");
                }
            }
        }
    })
    .await;

    assert!(
        confirmed.is_ok(),
        "Binance did not confirm the subscription"
    );

    // --------------------------------------------------------
    // VERIFY REAL TRADE MESSAGE
    // --------------------------------------------------------

    let trade = time::timeout(LIVE_TIMEOUT, async {
        loop {
            let frame = market_rx.recv().await.expect("market data channel closed");

            let Message::Text(text) = frame else {
                continue;
            };

            let value: Value = serde_json::from_str(text.as_str()).expect("invalid Binance JSON");

            if value["e"] != "trade" {
                continue;
            }

            assert_eq!(value["s"], "BTCUSDT",);

            assert!(value["p"].as_str().is_some(), "trade price missing",);

            assert!(value["q"].as_str().is_some(), "trade quantity missing",);

            println!(
                "REAL TRADE: price={}, quantity={}, timestamp={}",
                value["p"], value["q"], value["T"],
            );

            break;
        }
    })
    .await;

    assert!(trade.is_ok(), "no valid BTCUSDT trade received");

    // --------------------------------------------------------
    // STOP CLEANLY
    // --------------------------------------------------------

    shutdown_tx.send(()).expect("could not request shutdown");

    time::timeout(Duration::from_secs(10), runner)
        .await
        .expect("runner shutdown timeout")
        .expect("runner task panicked")
        .expect("runner returned error");

    println!("BINANCE SPOT LIVE SMOKE TEST PASSED");
}
