use std::{collections::BTreeMap, path::PathBuf, sync::Arc, time::Duration};

use tokio::{
    sync::{mpsc, oneshot},
    time,
};

use tokio_tungstenite::tungstenite::Message;

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::{
        limiter::{
            BudgetScope, RateLimitManager, RateLimitPolicySet,
            factory::{
                CompiledWebSocketLimiters, WebSocketBudgetBinding, WebSocketLimiterBindings,
                WebSocketLimiterCompiler,
            },
        },
        websocket::{
            CompiledSubscriptionAckBinding, ConnectionBootstrapLimits, ConnectionFence,
            ConnectionSessionLimits, ConnectionSessionOutputs, ConnectionSlotRunner,
            DesiredSubscription, DesiredSubscriptionSnapshot, HeartbeatPlanCompiler,
            ReconnectPolicy, SharedStreamRecovery, SubscriptionAckFieldBinding,
            SubscriptionDriverEvent, SubscriptionDriverLimits,
        },
    },
};

// ============================================================
// TEST CONFIGURATION
// ============================================================

const CHECK_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
struct ProfileCase {
    name: &'static str,
    symbol: &'static str,
    route: Option<&'static str>,
    native_ws_inst_type: Option<&'static str>,
}

const CASES: [ProfileCase; 15] = [
    ProfileCase {
        name: "binance_spot",
        symbol: "BTCUSDT",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "binance_linear",
        symbol: "BTCUSDT",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "binance_inverse",
        symbol: "BTCUSD_PERP",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "bybit_spot",
        symbol: "BTCUSDT",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "bybit_linear",
        symbol: "BTCUSDT",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "bybit_inverse",
        symbol: "BTCUSD",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "okx_spot",
        symbol: "BTC-USDT",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "okx_linear",
        symbol: "BTC-USDT-SWAP",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "okx_inverse",
        symbol: "BTC-USD-SWAP",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "bitget_spot",
        symbol: "BTCUSDT",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "bitget_linear",
        symbol: "BTCUSDT",
        route: None,
        native_ws_inst_type: Some("usdt-futures"),
    },
    ProfileCase {
        name: "bitget_inverse",
        symbol: "BTCUSD_CM",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "gateio_spot",
        symbol: "BTC_USDT",
        route: None,
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "gateio_linear",
        symbol: "BTC_USDT",
        route: Some("usdt"),
        native_ws_inst_type: None,
    },
    ProfileCase {
        name: "gateio_inverse",
        symbol: "BTC_USD",
        route: None,
        native_ws_inst_type: None,
    },
];

// ============================================================
// CONFIGURATION LOADING
// ============================================================

fn config_path(profile: &str) -> PathBuf {
    crate::support::data(".config/live/exchanges").join(format!("{profile}.toml"))
}

fn load_config(profile: &str) -> Result<ExchangeConfig, String> {
    ExchangeConfig::load(config_path(profile)).map_err(|error| error.to_string())
}

// ============================================================
// VERIFIED NATIVE ACK IDENTITY BINDINGS
// ============================================================

fn ack_bindings(
    config: &ExchangeConfig,
    channel: &str,
) -> Result<BTreeMap<String, CompiledSubscriptionAckBinding>, String> {
    let mut bindings = BTreeMap::new();

    if config.exchange.id.as_str() == "bitget" {
        // Bitget UTA V3:
        //
        // Subscription argument:
        // {
        //   "instType": "spot",
        //   "topic": "publicTrade",
        //   "symbol": "BTCUSDT"
        // }
        //
        // Expected ACK identity:
        // {
        //   "arg": {
        //     "instType": "spot",
        //     "topic": "publicTrade",
        //     "symbol": "BTCUSDT"
        //   }
        // }

        let binding = CompiledSubscriptionAckBinding::new(vec![
            SubscriptionAckFieldBinding::object(0, "instType", "arg.instType"),
            SubscriptionAckFieldBinding::object(0, "topic", "arg.topic"),
            SubscriptionAckFieldBinding::object(0, "symbol", "arg.symbol"),
        ])
        .map_err(|error| error.to_string())?;

        bindings.insert(channel.to_owned(), binding);
    }

    Ok(bindings)
}

// ============================================================
// ROUTE RESOLUTION
// ============================================================

fn connection_route(
    config: &ExchangeConfig,
    channel: &str,
    case: ProfileCase,
) -> Result<Option<String>, String> {
    let configured_route = config
        .channel(channel)
        .ok_or_else(|| format!("missing channel '{channel}'"))?
        .route
        .as_deref();

    Ok(configured_route.or(case.route).map(str::to_owned))
}

fn connection_url(config: &ExchangeConfig, route: Option<&str>) -> Result<String, String> {
    if let Some(route_name) = route {
        let selected = config
            .route(route_name)
            .ok_or_else(|| format!("missing WebSocket route '{route_name}'"))?;

        if let Some(url) = &selected.url {
            return Ok(url.clone());
        }

        if let Some(base_url) = &selected.base_url {
            return Ok(base_url.clone());
        }

        return Err(format!("route '{route_name}' has no URL"));
    }

    if let Some(url) = &config.websocket.url {
        return Ok(url.clone());
    }

    if let Some(base_url) = &config.websocket.base_url {
        if let Some(path) = &config.websocket.raw_path {
            return Ok(format!("{}{}", base_url.trim_end_matches('/'), path,));
        }

        return Ok(base_url.clone());
    }

    Err("no WebSocket endpoint could be resolved".into())
}

// ============================================================
// DEMAND
// ============================================================

fn desired(
    case: ProfileCase,
    config: &ExchangeConfig,
    channel: &str,
) -> Result<DesiredSubscriptionSnapshot, String> {
    let channel_config = config
        .channel(channel)
        .ok_or_else(|| format!("missing channel '{channel}'"))?;

    if !channel_config.enabled {
        return Err(format!("channel '{channel}' is disabled"));
    }

    let mut subscription =
        DesiredSubscription::new(format!("{}:{channel}", case.symbol), channel, case.symbol)
            .map_err(|error| error.to_string())?;

    if let Some(inst_type) = case.native_ws_inst_type {
        subscription = subscription.with_variable("native_ws_inst_type", inst_type);
    }

    if let Some(depth) = channel_config.default_depth {
        subscription = subscription.with_variable("depth", depth.to_string());
    }

    if let Some(interval) = &channel_config.default_interval {
        subscription = subscription.with_variable("interval", interval.clone());
    }

    DesiredSubscriptionSnapshot::new(1, [subscription]).map_err(|error| error.to_string())
}

// ============================================================
// CONNECTION LIMITS
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
        Some(1),
    )
    .unwrap()
}

// ============================================================
// ONE LIVE CHECK
// ============================================================

async fn check(
    case: ProfileCase,
    channel: &'static str,
    capture: Option<(&crate::capture::Capture, &RateLimitManager)>,
) -> Result<String, String> {
    let config = load_config(case.name)?;

    let route = connection_route(&config, channel, case)?;
    let url = connection_url(&config, route.as_deref())?;

    let demand = desired(case, &config, channel)?;

    let heartbeat = HeartbeatPlanCompiler::compile(&config)
        .map_err(|error| format!("heartbeat compilation: {error}"))?;
    let native_ack_bindings = ack_bindings(&config, channel)?;
    let (compiled, shared_limiter) = if let Some((_, shared)) = capture {
        (compile_capture_limits(&config)?, shared.clone())
    } else {
        let scope = BudgetScope::Egress("bulk-live-test".into());
        let service = format!("bulk-live-{}", case.name);
        let binding = WebSocketBudgetBinding::new(service.clone(), scope);
        let bindings = WebSocketLimiterBindings {
            connection_attempts: Some(binding.clone()),
            connection_attempts_per_second: Some(binding),
            connection_service: Some(service),
        };
        let compiled = WebSocketLimiterCompiler::compile(&config, &bindings)
            .map_err(|error| format!("limiter compilation: {error}"))?;
        let manager = RateLimitManager::new(compiled.shared.policies.clone(), 32)
            .map_err(|error| error.to_string())?;
        (compiled, manager)
    };

    let exchange = Arc::new(config);

    let fence = ConnectionFence::new("bulk-live-test", format!("{}-{channel}", case.name))
        .map_err(|error| error.to_string())?;

    let recovery = SharedStreamRecovery::new();

    let (market_tx, mut market_rx) = mpsc::channel::<Message>(256);

    let (subscription_tx, mut subscription_rx) = mpsc::channel::<SubscriptionDriverEvent>(16);

    let (events_tx, mut events_rx) = mpsc::channel(64);

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

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
            route,
            move || demand.clone(),
            move || heartbeat.clone(),
            native_ack_bindings,
            ConnectionSessionOutputs {
                market_data: market_tx,
                subscription_events: subscription_tx,
            },
            ConnectionBootstrapLimits {
                maximum_pending: 16,
                acknowledgement_timeout: Duration::from_secs(8),
            },
            session_limits(),
        )
        .await
    });

    // --------------------------------------------------------
    // VALIDATED SUBSCRIPTION ACK
    // --------------------------------------------------------

    let timeout = if capture.is_some() {
        Duration::from_secs(60)
    } else {
        CHECK_TIMEOUT
    };
    let observation = time::timeout(timeout, async {
        let event = subscription_rx
            .recv()
            .await
            .ok_or_else(|| "subscription event channel closed".to_string())?;

        let SubscriptionDriverEvent::Confirmed { token, .. } = event else {
            return Err(format!("subscription rejected: {event:?}"));
        };

        let expected_stream = format!("{}:{channel}", case.symbol,);

        if token.stream_id() != expected_stream {
            return Err(format!(
                "unexpected confirmed stream: {}",
                token.stream_id(),
            ));
        }

        // ------------------------------------------------
        // RAW MARKET DATA
        // ------------------------------------------------

        if capture.is_some() {
            return capture_frames(&mut market_rx, case, channel).await;
        }

        loop {
            let frame = market_rx
                .recv()
                .await
                .ok_or_else(|| "market-data channel closed".to_string())?;

            let Message::Text(text) = frame else {
                continue;
            };

            let value: serde_json::Value = serde_json::from_str(text.as_str())
                .map_err(|error| format!("invalid market-data JSON: {error}"))?;

            let preview = value.to_string();

            return Ok(format!(
                "ACK confirmed; JSON frame: {}",
                preview.chars().take(140).collect::<String>(),
            ));
        }
    })
    .await;

    // --------------------------------------------------------
    // CLEANUP AND FAILURE DIAGNOSTICS
    // --------------------------------------------------------

    let _ = shutdown_tx.send(());

    let mut runner = runner;

    let runner_result = match time::timeout(SHUTDOWN_TIMEOUT, &mut runner).await {
        Ok(Ok(Ok(()))) => "clean shutdown".to_string(),

        Ok(Ok(Err(error))) => {
            format!("runner error: {error:?}")
        }

        Ok(Err(error)) => {
            format!("runner task failure: {error:?}")
        }

        Err(_) => {
            runner.abort();
            let _ = runner.await;
            "runner shutdown timed out".to_string()
        }
    };

    // Drain captured lifecycle diagnostics.
    let mut lifecycle = Vec::new();

    while let Ok(event) = events_rx.try_recv() {
        lifecycle.push(format!("{event:?}"));
    }

    let lifecycle_summary = if lifecycle.is_empty() {
        "no lifecycle events".to_string()
    } else {
        lifecycle.join(" | ")
    };

    let diagnostics = format!("{runner_result}; lifecycle: {lifecycle_summary}");

    match observation {
        Ok(Ok(details)) => {
            if let Some((capture, _)) = capture {
                if runner_result != "clean shutdown" {
                    return Err(diagnostics);
                }
                let relative = format!("websocket-fixtures/{channel}/{}.json", case.name);
                capture
                    .save(&relative, details.as_bytes())
                    .map_err(|error| error.to_string())?;
                Ok(format!("captured {relative} ({} bytes)", details.len()))
            } else {
                Ok(details)
            }
        }

        Ok(Err(error)) => Err(format!("{error}; {diagnostics}")),

        Err(_) => Err(format!(
            "timed out after {} seconds; {diagnostics}",
            timeout.as_secs(),
        )),
    }
}

// ============================================================
// BULK EXECUTION
// ============================================================

#[tokio::test]
#[ignore = "connects to public exchange WebSockets"]
async fn all_exchange_profiles_live() {
    let mut passed = 0_usize;
    let mut failed = Vec::new();

    println!();
    println!("MARKETFORGE BULK LIVE VALIDATION");
    println!("================================");

    for case in CASES {
        let filter = std::env::var("MF_LIVE_FILTER").unwrap_or_default();
        for channel in ["trades", "depth"] {
            if !filter.is_empty() && !case.name.contains(&filter) {
                continue;
            }
            let label = format!("{:<20} {:<7}", case.name, channel,);

            // The individual result is recorded even when
            // another exchange or channel has failed.
            match check(case, channel, None).await {
                Ok(details) => {
                    passed += 1;

                    println!("[PASS] {label} {details}");
                }

                Err(reason) => {
                    println!("[FAIL] {label} {reason}");

                    failed.push(format!("{} {channel}: {reason}", case.name,));
                }
            }
        }
    }

    println!();
    println!("================================");
    println!("PASSED: {passed}/30");
    println!("FAILED: {}", failed.len());

    if !failed.is_empty() {
        println!();
        println!("FAILURE SUMMARY:");

        for reason in &failed {
            println!("  - {reason}");
        }
    }

    assert!(failed.is_empty(), "{} live checks failed", failed.len(),);
}

fn compile_capture_limits(config: &ExchangeConfig) -> Result<CompiledWebSocketLimiters, String> {
    let bindings = crate::rest::service::websocket_bindings(config);
    WebSocketLimiterCompiler::compile(config, &bindings).map_err(|error| error.to_string())
}

async fn capture_frames(
    market: &mut mpsc::Receiver<Message>,
    case: ProfileCase,
    channel: &str,
) -> Result<String, String> {
    const MAX_BYTES: usize = 16 * 1024 * 1024;
    let depth = channel == "depth";
    let count = if depth { 12 } else { 1 };
    let mut frames = Vec::with_capacity(count);
    let mut bytes = 2;
    while frames.len() < count {
        let frame = market.recv().await.ok_or("market-data channel closed")?;
        let Message::Text(text) = frame else { continue };
        let native: serde_json::Value =
            serde_json::from_str(text.as_str()).map_err(|error| error.to_string())?;
        if !native.is_object() {
            return Err("market-data frame is not a native object".into());
        }
        if depth {
            let kind = if case.name.starts_with("bybit_") {
                Some((native["type"].as_str(), "delta"))
            } else if case.name.starts_with("bitget_") || case.name.starts_with("okx_") {
                Some((native["action"].as_str(), "update"))
            } else {
                None
            };
            if let Some((kind, delta)) = kind {
                let expected = if frames.is_empty() { "snapshot" } else { delta };
                if kind != Some(expected) {
                    return Err(format!(
                        "expected native depth {expected}, received {kind:?}"
                    ));
                }
            }
            // The existing Binance Spot fixture test extracts both sides
            // from the opening delta. Start at a suitable native boundary.
            if case.name == "binance_spot"
                && frames.is_empty()
                && ["b", "a"]
                    .iter()
                    .any(|side| native[side].as_array().is_none_or(Vec::is_empty))
            {
                continue;
            }
        }
        bytes += text.len() + 1;
        if bytes > MAX_BYTES {
            return Err("WebSocket capture exceeds 16 MiB".into());
        }
        frames.push(text);
    }
    // Preserve the raw JSON spelling, including native decimal tokens.
    Ok(format!(
        "[{}]",
        frames
            .iter()
            .map(|text| text.as_str())
            .collect::<Vec<_>>()
            .join(",")
    ))
}

#[tokio::test]
#[ignore = "explicit public WebSocket fixture collection via fixtures.py"]
async fn collect_websocket_fixtures() {
    let capture = crate::capture::Capture::from_env();
    let requested = CASES
        .into_iter()
        .flat_map(|case| ["trades", "depth"].map(move |channel| (case, channel)))
        .filter(|(case, channel)| {
            capture.wants(&format!("websocket-fixtures/{channel}/{}.json", case.name))
        })
        .collect::<Vec<_>>();
    assert!(!requested.is_empty(), "no WebSocket fixtures requested");

    let mut policies = RateLimitPolicySet::new();
    for (case, _) in &requested {
        let config = load_config(case.name).unwrap();
        let compiled = compile_capture_limits(&config).unwrap();
        for (id, quota) in compiled.shared.policies.iter() {
            policies.insert(id.clone(), *quota).unwrap();
        }
    }
    let manager = RateLimitManager::new(policies, 32).unwrap();
    let mut failures = Vec::new();
    for (case, channel) in requested {
        match check(case, channel, Some((&capture, &manager))).await {
            Ok(details) => println!("{details}"),
            Err(error) => {
                eprintln!("{} {channel}: {error}", case.name);
                failures.push((case.name, channel, error));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} WebSocket fixture captures failed",
        failures.len()
    );
}

#[tokio::test]
async fn captures_original_depth_frames_without_inventing_a_snapshot() {
    let (send, mut receive) = mpsc::channel(12);
    let native =
        r#"{"e":"depthUpdate","s":"BTCUSDT","b":[["1.000","2"]],"a":[["3","4"]],"time":1.2300}"#;
    for _ in 0..12 {
        send.try_send(Message::Text(native.into())).unwrap();
    }
    let raw = capture_frames(&mut receive, CASES[0], "depth")
        .await
        .unwrap();
    assert_eq!(raw, format!("[{}]", [native; 12].join(",")));
}

#[tokio::test]
async fn rejects_depth_capture_without_its_native_opening_snapshot() {
    let (send, mut receive) = mpsc::channel(1);
    send.try_send(Message::Text(r#"{"type":"delta"}"#.into()))
        .unwrap();
    let error = capture_frames(&mut receive, CASES[3], "depth")
        .await
        .unwrap_err();
    assert!(error.contains("expected native depth snapshot"));
}
