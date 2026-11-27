#[path = "variants.rs"]
mod variants;

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
            RateLimitManager, RateLimitPolicySet,
            factory::{CompiledWebSocketLimiters, WebSocketLimiterCompiler},
        },
        websocket::{
            CompiledSubscriptionAckBinding, ConnectionBootstrapLimits, ConnectionFence,
            ConnectionGeneration, ConnectionSessionLimits, ConnectionSessionOutputs,
            ConnectionSlotRunner, DesiredSubscription, DesiredSubscriptionSnapshot,
            HeartbeatPlanCompiler, ReconnectEvent, ReconnectPolicy, SharedStreamRecovery,
            SubscriptionAckFieldBinding, SubscriptionDriverEvent, SubscriptionDriverLimits,
        },
    },
};

// ============================================================
// TEST CONFIGURATION
// ============================================================

const ACK_TIMEOUT: Duration = Duration::from_secs(30);
const CHECK_TIMEOUT: Duration = Duration::from_secs(30);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone, Copy)]
struct ProfileCase<'a> {
    name: &'static str,
    symbol: &'a str,
    route: Option<&'static str>,
    native_ws_inst_type: Option<&'static str>,
}

const CASES: [ProfileCase<'static>; 15] = [
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

    // Public argument-routed profiles can omit request IDs in their ACKs.
    if let Some(routing) = config
        .websocket
        .routing
        .as_ref()
        .filter(|r| r.mode == "arg")
    {
        let fields = [
            routing.channel_field.as_deref(),
            routing.instrument_field.as_deref(),
            routing.instrument_type_field.as_deref(),
        ]
        .into_iter()
        .flatten()
        .map(|path| {
            let field = path
                .strip_prefix("arg.")
                .ok_or_else(|| format!("unsupported argument ACK path {path}"))?;
            Ok(SubscriptionAckFieldBinding::object(0, field, path))
        })
        .collect::<Result<Vec<_>, String>>()?;
        let binding =
            CompiledSubscriptionAckBinding::new(fields).map_err(|error| error.to_string())?;
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
    case: ProfileCase<'_>,
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
    case: ProfileCase<'_>,
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

#[derive(Debug)]
enum Outcome {
    Pass(String),
    Idle,
    Fail(String),
    Blocked(String),
}

#[derive(Debug, Default)]
struct Evidence {
    connected: bool,
    confirmed: bool,
    data: bool,
}

struct Verification {
    evidence: Evidence,
    outcome: Outcome,
}

impl Verification {
    fn failed(evidence: Evidence, error: String) -> Self {
        Self {
            evidence,
            outcome: Outcome::Fail(error),
        }
    }
}

async fn check(
    case: ProfileCase<'_>,
    channel: &'static str,
    shared_limiter: &RateLimitManager,
    capture: Option<&crate::capture::Capture>,
) -> Verification {
    let (shutdown, runner, mut lifecycle, mut subscriptions, mut market, config) =
        match start_check(case, channel, shared_limiter, None, Duration::from_secs(8)) {
            Ok(session) => session,
            Err(error) => return Verification::failed(Evidence::default(), error),
        };
    let mut verification = verify(
        case,
        channel,
        &config,
        &mut lifecycle,
        &mut subscriptions,
        &mut market,
        ACK_TIMEOUT,
        if capture.is_some() {
            Duration::from_secs(60)
        } else {
            CHECK_TIMEOUT
        },
        capture.is_some(),
    )
    .await;
    let _ = shutdown.send(());
    finish(
        &mut verification,
        runner,
        &mut lifecycle,
        &mut market,
        case,
        channel,
        &config,
    )
    .await;

    if let (Some(capture), Outcome::Pass(details)) = (capture, &verification.outcome) {
        let relative = format!("websocket-fixtures/{channel}/{}.json", case.name);
        verification.outcome = match capture.save(&relative, details.as_bytes()) {
            Ok(()) => Outcome::Pass(format!("captured {relative} ({} bytes)", details.len())),
            Err(error) => Outcome::Fail(error.to_string()),
        };
    }
    verification
}

type TestRunner = tokio::task::JoinHandle<
    Result<(), marketforge_engine::live::infrastructure::websocket::ReconnectError>,
>;
type TestSession = (
    oneshot::Sender<()>,
    TestRunner,
    mpsc::Receiver<ReconnectEvent>,
    mpsc::Receiver<SubscriptionDriverEvent>,
    mpsc::Receiver<Message>,
    ExchangeConfig,
);

fn start_check(
    case: ProfileCase<'_>,
    channel: &'static str,
    shared_limiter: &RateLimitManager,
    local_url: Option<String>,
    acknowledgement_timeout: Duration,
) -> Result<TestSession, String> {
    let config = load_config(case.name)?;

    let route = connection_route(&config, channel, case)?;
    let url = match local_url {
        Some(url) => url,
        None => connection_url(&config, route.as_deref())?,
    };

    let demand = desired(case, &config, channel)?;

    let heartbeat = HeartbeatPlanCompiler::compile(&config)
        .map_err(|error| format!("heartbeat compilation: {error}"))?;
    let native_ack_bindings = ack_bindings(&config, channel)?;
    let compiled = compile_capture_limits(&config)?;
    let shared_limiter = shared_limiter.clone();

    let exchange = Arc::new(config.clone());

    let fence = ConnectionFence::new("bulk-live-test", format!("{}-{channel}", case.name))
        .map_err(|error| error.to_string())?;

    let recovery = SharedStreamRecovery::new();

    let (market_tx, market_rx) = mpsc::channel::<Message>(256);

    let (subscription_tx, subscription_rx) = mpsc::channel::<SubscriptionDriverEvent>(16);

    let (events_tx, events_rx) = mpsc::channel(64);

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
                acknowledgement_timeout,
            },
            session_limits(),
        )
        .await
    });

    Ok((
        shutdown_tx,
        runner,
        events_rx,
        subscription_rx,
        market_rx,
        config,
    ))
}

async fn unexpected_lifecycle(events: &mut mpsc::Receiver<ReconnectEvent>) -> Outcome {
    let event = events.recv().await;
    Outcome::Fail(format!("unexpected connection lifecycle event: {event:?}"))
}

#[allow(clippy::too_many_arguments)]
async fn verify(
    case: ProfileCase<'_>,
    channel: &str,
    config: &ExchangeConfig,
    events: &mut mpsc::Receiver<ReconnectEvent>,
    subscriptions: &mut mpsc::Receiver<SubscriptionDriverEvent>,
    market: &mut mpsc::Receiver<Message>,
    ack_timeout: Duration,
    observation_timeout: Duration,
    capturing: bool,
) -> Verification {
    let mut evidence = Evidence::default();
    let outcome = match confirm_subscription(
        case,
        channel,
        events,
        subscriptions,
        ack_timeout,
        &mut evidence,
    )
    .await
    {
        Ok(()) => {
            observe(
                case,
                channel,
                config,
                events,
                subscriptions,
                market,
                observation_timeout,
                capturing,
            )
            .await
        }
        Err(outcome) => outcome,
    };
    evidence.data = matches!(outcome, Outcome::Pass(_));
    Verification { evidence, outcome }
}

async fn confirm_subscription(
    case: ProfileCase<'_>,
    channel: &str,
    events: &mut mpsc::Receiver<ReconnectEvent>,
    subscriptions: &mut mpsc::Receiver<SubscriptionDriverEvent>,
    ack_timeout: Duration,
    evidence: &mut Evidence,
) -> Result<(), Outcome> {
    let mut generation: Option<ConnectionGeneration> = None;
    time::timeout(ack_timeout, async {
        let expected = format!("{}:{channel}", case.symbol);
        loop {
            tokio::select! {
                biased;
                event = events.recv() => match event {
                    Some(ReconnectEvent::Connecting { .. }) if generation.is_none() => {}
                    Some(ReconnectEvent::Connected { generation: connected, .. }) if generation.is_none() => {
                        generation = Some(connected);
                        evidence.connected = true;
                    }
                    Some(ReconnectEvent::Disconnected { reason, .. }) if !evidence.connected
                        && external_block(&reason) => return Err(Outcome::Blocked(reason)),
                    event => return Err(Outcome::Fail(format!("connection failed during confirmation: {event:?}"))),
                },
                event = subscriptions.recv() => match event {
                    Some(SubscriptionDriverEvent::Confirmed { token, .. })
                        if token.stream_id() == expected && Some(token.connection().generation()) == generation => {
                            evidence.confirmed = true;
                            return Ok(());
                        }
                    event => return Err(Outcome::Fail(format!("subscription was not confirmed: {event:?}"))),
                },
            }
        }
    }).await.map_err(|_| Outcome::Fail("physical connection or internal ACK deadline expired".into()))?
}

#[allow(clippy::too_many_arguments)]
async fn observe(
    case: ProfileCase<'_>,
    channel: &str,
    config: &ExchangeConfig,
    events: &mut mpsc::Receiver<ReconnectEvent>,
    subscriptions: &mut mpsc::Receiver<SubscriptionDriverEvent>,
    market: &mut mpsc::Receiver<Message>,
    observation_timeout: Duration,
    capturing: bool,
) -> Outcome {
    let observation = async {
        if capturing {
            capture_frames(market, case, channel, config).await
        } else {
            let message = market.recv().await.ok_or("market-data channel closed")?;
            let value = validate_observation(case, channel, config, message)?;
            Ok(format!(
                "JSON frame: {}",
                value.to_string().chars().take(140).collect::<String>()
            ))
        }
    };
    tokio::select! {
        biased;
        failure = unexpected_lifecycle(events) => failure,
        event = subscriptions.recv() => Outcome::Fail(format!("unexpected subscription event: {event:?}")),
        result = time::timeout(observation_timeout, observation) => match result {
            Ok(Ok(details)) => {
                Outcome::Pass(details)
            }
            Ok(Err(error)) => Outcome::Fail(error),
            Err(_) if capturing => Outcome::Fail("native observation deadline expired; no fixture saved".into()),
            Err(_) => Outcome::Idle,
        },
    }
}

#[allow(clippy::too_many_arguments)]
async fn finish(
    verification: &mut Verification,
    mut runner: TestRunner,
    events: &mut mpsc::Receiver<ReconnectEvent>,
    market: &mut mpsc::Receiver<Message>,
    case: ProfileCase<'_>,
    channel: &str,
    config: &ExchangeConfig,
) {
    let successful = matches!(verification.outcome, Outcome::Pass(_) | Outcome::Idle);
    let result = time::timeout(SHUTDOWN_TIMEOUT, &mut runner).await;
    if result.is_err() {
        runner.abort();
        let _ = runner.await;
    }
    if successful && !matches!(result, Ok(Ok(Ok(())))) {
        verification.outcome = Outcome::Fail(format!("runner shutdown failed: {result:?}"));
        return;
    }
    if successful {
        let mut stopped = false;
        while let Ok(event) = events.try_recv() {
            match event {
                ReconnectEvent::Stopped => stopped = true,
                event => {
                    verification.outcome =
                        Outcome::Fail(format!("unexpected lifecycle before shutdown: {event:?}"));
                    return;
                }
            }
        }
        if !stopped {
            verification.outcome = Outcome::Fail("missing clean stop event".into());
            return;
        }
        // A frame may have arrived before the deadline but still be queued.
        if matches!(verification.outcome, Outcome::Idle) {
            while let Ok(message) = market.try_recv() {
                match validate_observation(case, channel, config, message) {
                    Ok(_) => {
                        verification.evidence.data = true;
                        verification.outcome =
                            Outcome::Pass("native data queued at shutdown".into());
                    }
                    Err(error) => {
                        verification.outcome = Outcome::Fail(error);
                        return;
                    }
                }
            }
        }
    }
}

// ============================================================
// BULK EXECUTION
// ============================================================

fn selected_cases(filter: &str) -> Result<Vec<(ProfileCase<'static>, &'static str)>, String> {
    let selected: Vec<_> = CASES
        .into_iter()
        .filter(|case| case.name.contains(filter))
        .flat_map(|case| ["trades", "depth"].map(move |channel| (case, channel)))
        .collect();
    if selected.is_empty() {
        return Err(format!(
            "MF_LIVE_FILTER={filter:?} selects no WebSocket cases"
        ));
    }
    Ok(selected)
}

fn shared_limits(cases: &[(ProfileCase<'_>, &str)]) -> Result<RateLimitManager, String> {
    let mut policies = RateLimitPolicySet::new();
    for (case, _) in cases {
        let config = load_config(case.name)?;
        let compiled = compile_capture_limits(&config)?;
        for (id, quota) in compiled.shared.policies.iter() {
            policies
                .insert(id.clone(), *quota)
                .map_err(|error| error.to_string())?;
        }
    }
    RateLimitManager::new(policies, 32).map_err(|error| error.to_string())
}

fn external_block(reason: &str) -> bool {
    // Keep ambiguous timeouts and subscription failures in the failure count.
    ["HTTP error: 403", "HTTP error: 429", "HTTP error: 451"]
        .iter()
        .any(|status| reason.contains(status))
}

#[derive(Default)]
struct Totals {
    selected: usize,
    executed: usize,
    passed: usize,
    idle: usize,
    failed: usize,
    blocked: usize,
    connected: usize,
    confirmed: usize,
    data: usize,
}

impl Totals {
    fn record(&mut self, label: &str, verification: Verification) {
        let Evidence {
            connected,
            confirmed,
            data,
        } = verification.evidence;
        self.connected += usize::from(connected);
        self.confirmed += usize::from(confirmed);
        self.data += usize::from(data);
        let (status, details) = match verification.outcome {
            Outcome::Pass(details) => {
                self.passed += 1;
                ("PASS", details)
            }
            Outcome::Idle => {
                self.idle += 1;
                (
                    "IDLE",
                    "no expected native observations within the window".into(),
                )
            }
            Outcome::Fail(details) => {
                self.failed += 1;
                ("FAIL", details)
            }
            Outcome::Blocked(details) => {
                self.blocked += 1;
                ("BLOCKED", details)
            }
        };
        println!("[{status}] {label} connected={connected} ack={confirmed} data={data} {details}");
    }

    fn report(&self) {
        println!(
            "SELECTED: {}; EXECUTED: {}; PASSED: {}; IDLE: {}; FAILED: {}; BLOCKED: {}",
            self.selected, self.executed, self.passed, self.idle, self.failed, self.blocked,
        );
        println!(
            "PHYSICAL CONNECTIVITY VERIFIED: {}; SUBSCRIPTION CONFIRMATION VERIFIED: {}; NATIVE DATA RECEPTION VERIFIED: {}",
            self.connected, self.confirmed, self.data,
        );
    }

    fn accepted(&self, strict: bool) -> bool {
        self.selected > 0
            && self.failed == 0
            && self.blocked == 0
            && self.passed + if strict { 0 } else { self.idle } == self.selected
    }
}

#[tokio::test]
#[ignore = "connects to public exchange WebSockets; requires coordinated egress"]
async fn all_exchange_profiles_live() {
    let filter = std::env::var("MF_LIVE_FILTER").unwrap_or_default();
    let cases = selected_cases(&filter).unwrap();
    let mut totals = Totals {
        selected: cases.len(),
        ..Totals::default()
    };
    let limiter = shared_limits(&cases).unwrap_or_else(|error| {
        totals.blocked = totals.selected;
        totals.report();
        panic!("WebSocket limiter preflight blocked execution: {error}");
    });

    let mut throttled = std::collections::BTreeSet::new();
    for (case, channel) in cases {
        let label = format!("{:<20} {channel:<7}", case.name);
        let exchange = case.name.split('_').next().unwrap();
        if throttled.contains(exchange) {
            totals.record(
                &label,
                Verification {
                    evidence: Evidence::default(),
                    outcome: Outcome::Blocked(
                        "remaining exchange checks paused after HTTP 429".into(),
                    ),
                },
            );
            continue;
        }
        totals.executed += 1;
        let verification = check(case, channel, &limiter, None).await;
        if matches!(&verification.outcome, Outcome::Blocked(reason) if reason.contains("HTTP error: 429"))
        {
            throttled.insert(exchange);
        }
        totals.record(&label, verification);
    }
    totals.report();
    let strict = std::env::var("MF_LIVE_REQUIRE_DATA").as_deref() == Ok("1");
    assert!(
        totals.accepted(strict),
        "incomplete public WebSocket verification"
    );
}

#[test]
fn bulk_selection_and_shared_limits_are_valid_offline() {
    let all = selected_cases("").unwrap();
    assert_eq!(all.len(), 30);
    shared_limits(&all).unwrap();
    for (case, channel) in all {
        let config = load_config(case.name).unwrap();
        let bindings = ack_bindings(&config, channel).unwrap();
        if matches!(config.exchange.id.as_str(), "okx" | "bitget") {
            assert!(
                bindings.contains_key(channel),
                "missing native ACK identity for {}",
                case.name
            );
        }
    }
    let filtered = selected_cases("gateio_linear").unwrap();
    assert_eq!(filtered.len(), 2);
    assert!(
        filtered
            .iter()
            .all(|(case, _)| case.name == "gateio_linear")
    );
    assert!(selected_cases("no-such-profile").is_err());
    assert!(external_block("Websocket upgrade failed: HTTP error: 451"));
    assert!(!external_block("timed out after 30 seconds"));
}

fn compile_capture_limits(config: &ExchangeConfig) -> Result<CompiledWebSocketLimiters, String> {
    let bindings = crate::rest::service::websocket_bindings(config);
    WebSocketLimiterCompiler::compile(config, &bindings).map_err(|error| error.to_string())
}

fn validate_observation(
    case: ProfileCase<'_>,
    channel: &str,
    config: &ExchangeConfig,
    frame: Message,
) -> Result<serde_json::Value, String> {
    use serde_json::Value;
    let native: Value = match frame {
        Message::Text(text) => serde_json::from_str(text.as_str()),
        Message::Binary(bytes) => serde_json::from_slice(&bytes),
        _ => return Err("unexpected WebSocket market-data frame".into()),
    }
    .map_err(|error| format!("invalid market-data JSON: {error}"))?;
    let trades = channel == "trades";
    let channel_config = config.channel(channel).ok_or("missing channel")?;
    let gate_spot = case.name == "gateio_spot";
    let binance_payload =
        if config.exchange.id.as_str() == "binance" && native.get("stream").is_some() {
            let expected = channel_config
                .stream_template
                .as_deref()
                .ok_or("missing Binance stream template")?
                .replace("{symbol_lower}", &case.symbol.to_ascii_lowercase());
            if native["stream"] != expected {
                return Err("unrelated Binance combined stream".into());
            }
            &native["data"]
        } else {
            &native
        };
    let (payload, rows, price, size, asks, bids) = match config.exchange.id.as_str() {
        "binance"
            if binance_payload["s"] == case.symbol
                && if trades {
                    matches!(binance_payload["e"].as_str(), Some("trade" | "aggTrade"))
                } else {
                    binance_payload["e"] == "depthUpdate"
                } =>
        {
            (binance_payload, false, "p", "q", "a", "b")
        }
        "bybit"
            if native["topic"]
                == if trades {
                    format!("publicTrade.{}", case.symbol)
                } else {
                    format!(
                        "orderbook.{}.{}",
                        channel_config.default_depth.ok_or("missing Bybit depth")?,
                        case.symbol
                    )
                }
                && matches!(native["type"].as_str(), Some("snapshot" | "delta")) =>
        {
            (&native["data"], trades, "p", "v", "a", "b")
        }
        "okx"
            if native["arg"]["instId"] == case.symbol
                && native["arg"]["channel"].as_str() == channel_config.channel.as_deref()
                && (trades || matches!(native["action"].as_str(), Some("snapshot" | "update"))) =>
        {
            (&native["data"], true, "px", "sz", "asks", "bids")
        }
        "bitget"
            if native["arg"]["symbol"] == case.symbol
                && native["arg"]["topic"].as_str() == channel_config.channel.as_deref()
                && native["arg"]["instType"]
                    == case
                        .native_ws_inst_type
                        .unwrap_or(if case.name == "bitget_spot" {
                            "spot"
                        } else {
                            "coin-futures"
                        })
                && matches!(native["action"].as_str(), Some("snapshot" | "update")) =>
        {
            (&native["data"], true, "p", "v", "a", "b")
        }
        "gateio"
            if native["event"] == "update"
                && native["channel"].as_str() == channel_config.channel.as_deref() =>
        {
            (
                &native["result"],
                trades && !gate_spot,
                "price",
                if gate_spot { "amount" } else { "size" },
                "a",
                "b",
            )
        }
        _ => {
            return Err(
                "native frame does not match the requested channel, instrument or product".into(),
            );
        }
    };
    let records = if rows {
        payload
            .as_array()
            .filter(|rows| !rows.is_empty())
            .ok_or("empty or invalid native data array")?
            .as_slice()
    } else {
        std::slice::from_ref(payload)
    };
    let decimal = |value: &Value| {
        use rust_decimal::Decimal;
        match value {
            Value::String(s) => s.parse::<Decimal>().ok(),
            Value::Number(n) => n.to_string().parse::<Decimal>().ok(),
            _ => None,
        }
    };
    for record in records {
        let symbol_field = match (config.exchange.id.as_str(), trades, gate_spot) {
            ("gateio", true, true) => Some("currency_pair"),
            ("gateio", true, false) => Some("contract"),
            ("gateio", false, _) | ("bybit", _, _) => Some("s"),
            ("okx", true, _) => Some("instId"),
            _ => None,
        };
        if symbol_field.is_some_and(|key| record[key] != case.symbol) {
            return Err("native payload contains an unrelated instrument".into());
        }
        let price_valid =
            |value: &Value| decimal(value).is_some_and(|n| n > rust_decimal::Decimal::ZERO);
        if trades {
            if !price_valid(&record[price]) || decimal(&record[size]).is_none_or(|n| n.is_zero()) {
                return Err("invalid native trade price or quantity".into());
            }
        } else {
            let asks = record[asks].as_array().ok_or("missing native asks array")?;
            let bids = record[bids].as_array().ok_or("missing native bids array")?;
            if asks.is_empty() && bids.is_empty() {
                return Err("native depth observation has no levels".into());
            }
            for level in asks.iter().chain(bids) {
                let (price, quantity) =
                    if level.is_object() && config.exchange.id.as_str() == "gateio" {
                        (&level["p"], &level["s"])
                    } else {
                        (&level[0], &level[1])
                    };
                if !price_valid(price) || decimal(quantity).is_none() {
                    return Err("invalid native depth level".into());
                }
            }
        }
    }
    Ok(native)
}

async fn capture_frames(
    market: &mut mpsc::Receiver<Message>,
    case: ProfileCase<'_>,
    channel: &str,
    config: &ExchangeConfig,
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
        validate_observation(case, channel, config, Message::Text(text.clone()))?;
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

    let manager = shared_limits(&requested).unwrap();
    let mut failures = Vec::new();
    for (case, channel) in requested {
        match check(case, channel, &manager, Some(&capture)).await.outcome {
            Outcome::Pass(details) => println!("{details}"),
            outcome => {
                let error = format!("{outcome:?}");
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
    let raw = capture_frames(
        &mut receive,
        CASES[0],
        "depth",
        &load_config(CASES[0].name).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(raw, format!("[{}]", [native; 12].join(",")));
}

#[tokio::test]
async fn rejects_depth_capture_without_its_native_opening_snapshot() {
    let (send, mut receive) = mpsc::channel(1);
    send.try_send(Message::Text(r#"{"type":"delta"}"#.into()))
        .unwrap();
    let error = capture_frames(
        &mut receive,
        CASES[3],
        "depth",
        &load_config(CASES[3].name).unwrap(),
    )
    .await
    .unwrap_err();
    assert!(error.contains("expected native depth snapshot"));
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};

    const ACK: &str = r#"{"channel":"futures.trades","conn_id":"172459d679da70f4","event":"subscribe","id":1,"payload":["BTC_USD"],"result":{"status":"success"},"time":1795807973,"time_ms":1795807973107,"trace_id":"2002e9f84df1465439f227c1bb8ecb1"}"#;
    const TRADE: &str = r#"{"channel":"futures.trades","event":"update","result":[{"contract":"BTC_USD","price":"60000","size":"1"}]}"#;

    async fn loopback(
        ack: Option<&str>,
        frames: &[&str],
        disconnect: bool,
        capture: bool,
    ) -> Verification {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let (close, wait_close) = oneshot::channel();
        let ack = ack.map(str::to_owned);
        let frames: Vec<_> = frames.iter().map(|frame| (*frame).to_owned()).collect();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let request = socket.next().await.unwrap().unwrap().into_text().unwrap();
            let request: serde_json::Value = serde_json::from_str(&request).unwrap();
            assert_eq!(request["id"], 1);
            assert_eq!(request["payload"], serde_json::json!(["BTC_USD"]));
            if let Some(ack) = ack {
                socket.send(Message::Text(ack.into())).await.unwrap();
            }
            for frame in frames {
                socket.send(Message::Text(frame.into())).await.unwrap();
            }
            if disconnect {
                wait_close.await.unwrap();
                socket.send(Message::Close(None)).await.unwrap();
            } else {
                while let Some(Ok(message)) = socket.next().await {
                    if message.is_close() {
                        break;
                    }
                }
            }
        });

        let case = CASES[14];
        let channel = "trades";
        let limiter = shared_limits(&[(case, channel)]).unwrap();
        let (shutdown, runner, mut events, mut subscriptions, mut market, config) =
            start_check(case, channel, &limiter, Some(url), Duration::from_secs(2)).unwrap();
        let mut result = if disconnect {
            let mut evidence = Evidence::default();
            confirm_subscription(
                case,
                channel,
                &mut events,
                &mut subscriptions,
                Duration::from_millis(500),
                &mut evidence,
            )
            .await
            .unwrap();
            close.send(()).unwrap();
            let outcome = observe(
                case,
                channel,
                &config,
                &mut events,
                &mut subscriptions,
                &mut market,
                Duration::from_secs(2),
                capture,
            )
            .await;
            Verification { evidence, outcome }
        } else {
            verify(
                case,
                channel,
                &config,
                &mut events,
                &mut subscriptions,
                &mut market,
                Duration::from_millis(500),
                Duration::from_millis(100),
                capture,
            )
            .await
        };
        let _ = shutdown.send(());
        finish(
            &mut result,
            runner,
            &mut events,
            &mut market,
            case,
            channel,
            &config,
        )
        .await;
        time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
        result
    }

    #[tokio::test]
    async fn exact_gate_ack_confirms_before_pass_or_idle() {
        let data = loopback(Some(ACK), &[TRADE], false, false).await;
        assert!(data.evidence.connected && data.evidence.confirmed && data.evidence.data);
        assert!(
            matches!(data.outcome, Outcome::Pass(_)),
            "{:?}",
            data.outcome
        );

        let idle = loopback(Some(ACK), &[], false, false).await;
        assert!(idle.evidence.connected && idle.evidence.confirmed && !idle.evidence.data);
        assert!(matches!(idle.outcome, Outcome::Idle), "{:?}", idle.outcome);
    }

    #[tokio::test]
    async fn missing_or_uncorrelated_ack_fails() {
        let wrong_id = ACK.replace("\"id\":1", "\"id\":2");
        for ack in [None, Some(wrong_id.as_str())] {
            let result = loopback(ack, &[], false, false).await;
            assert!(result.evidence.connected && !result.evidence.confirmed);
            assert!(
                matches!(result.outcome, Outcome::Fail(error) if error.contains("ACK deadline"))
            );
        }
    }

    #[tokio::test]
    async fn unexpected_disconnect_is_not_idle() {
        let result = loopback(Some(ACK), &[], true, false).await;
        assert!(result.evidence.connected && result.evidence.confirmed);
        assert!(
            matches!(result.outcome, Outcome::Fail(error) if error.contains("unexpected connection lifecycle"))
        );
    }

    #[tokio::test]
    async fn malformed_and_unrelated_data_fail() {
        let unrelated_trade = TRADE.replace("BTC_USD", "ETH_USD");
        for frame in ["{invalid", unrelated_trade.as_str()] {
            let result = loopback(Some(ACK), &[frame], false, false).await;
            assert!(result.evidence.confirmed && !result.evidence.data);
            assert!(matches!(result.outcome, Outcome::Fail(_)));
        }
    }

    #[tokio::test]
    async fn idle_capture_requires_data() {
        let result = loopback(Some(ACK), &[], false, true).await;
        assert!(result.evidence.confirmed && !result.evidence.data);
        assert!(
            matches!(result.outcome, Outcome::Fail(error) if error.contains("no fixture saved"))
        );
    }

    #[tokio::test]
    async fn missing_clean_stop_rejects_idle() {
        let (_, mut events) = mpsc::channel(1);
        let (_, mut market) = mpsc::channel(1);
        let case = CASES[14];
        let config = load_config(case.name).unwrap();
        let mut result = Verification {
            evidence: Evidence {
                connected: true,
                confirmed: true,
                data: false,
            },
            outcome: Outcome::Idle,
        };
        finish(
            &mut result,
            tokio::spawn(async { Ok(()) }),
            &mut events,
            &mut market,
            case,
            "trades",
            &config,
        )
        .await;
        assert!(
            matches!(result.outcome, Outcome::Fail(error) if error.contains("missing clean stop"))
        );
    }

    #[test]
    fn totals_keep_transport_and_financial_evidence_separate() {
        let mut totals = Totals {
            selected: 2,
            executed: 2,
            ..Default::default()
        };
        for (outcome, data) in [(Outcome::Pass(String::new()), true), (Outcome::Idle, false)] {
            totals.record(
                "local",
                Verification {
                    evidence: Evidence {
                        connected: true,
                        confirmed: true,
                        data,
                    },
                    outcome,
                },
            );
        }
        assert_eq!(
            (totals.passed, totals.idle, totals.failed, totals.blocked),
            (1, 1, 0, 0)
        );
        assert_eq!((totals.connected, totals.confirmed, totals.data), (2, 2, 1));
        assert!(totals.accepted(false));
        assert!(!totals.accepted(true));
        totals.record(
            "blocked",
            Verification {
                evidence: Evidence::default(),
                outcome: Outcome::Blocked("external restriction".into()),
            },
        );
        assert!(!totals.accepted(false));
    }
}
