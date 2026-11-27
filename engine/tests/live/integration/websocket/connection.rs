use std::time::Duration;

use futures_util::{SinkExt, StreamExt};

use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        Message,
        handshake::server::{Request, Response},
    },
};

use marketforge_engine::live::{
    config::exchange::ExchangeConfig,
    infrastructure::{
        limiter::{
            BudgetScope, RateLimitManager,
            factory::{
                CompiledWebSocketLimiters, WebSocketBudgetBinding, WebSocketLimiterBindings,
                WebSocketLimiterCompiler,
            },
        },
        websocket::{WebSocketConnector, WebSocketTransportError},
    },
};

// ============================================================
// LIMITER SETUP
// ============================================================

fn compiled_limits() -> (CompiledWebSocketLimiters, RateLimitManager) {
    let config: ExchangeConfig = toml::from_str(
        r#"
schema_version = 1

[exchange]
id = "binance"
market = "spot"
profile = "binance_spot"

[websocket]
url = "wss://example.invalid"
connection_attempt_limit = 10
connection_attempt_window_seconds = 60
max_client_messages_per_second = 2

[channels.trades]
enabled = true
format = "trades/binance_spot"

[rest]
base_url = "https://example.invalid"
"#,
    )
    .unwrap();

    let bindings = WebSocketLimiterBindings {
        connection_attempts: Some(WebSocketBudgetBinding::new(
            "test-websocket",
            BudgetScope::Egress("local".into()),
        )),
        connection_attempts_per_second: None,
        connection_service: Some("test-websocket".into()),
    };

    let compiled = WebSocketLimiterCompiler::compile(&config, &bindings).unwrap();

    let manager = RateLimitManager::new(compiled.shared.policies.clone(), 16).unwrap();

    (compiled, manager)
}

// ============================================================
// CONNECT, SEND, RECEIVE, CLOSE
// ============================================================

#[tokio::test]
async fn connect_send_receive_close() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
            assert!(!request.headers().contains_key("x-gate-size-decimal"));
            assert!(request.headers().contains_key("sec-websocket-key"));
            Ok(response)
        })
        .await
        .unwrap();

        let incoming = socket.next().await.unwrap().unwrap();

        assert_eq!(incoming.into_text().unwrap().as_str(), "subscribe",);

        socket
            .send(Message::Text("trade-message".into()))
            .await
            .unwrap();

        // The client performs a close handshake.
        let close = socket.next().await.unwrap().unwrap();

        assert!(matches!(close, Message::Close(_)));
    });

    let (compiled, shared) = compiled_limits();

    let mut connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        42,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    assert_eq!(connection.connection_id(), 42);

    connection
        .send_subscription("subscribe", Duration::from_secs(2))
        .await
        .unwrap();

    let message = connection.receive(Duration::from_secs(2)).await.unwrap();

    assert_eq!(message.into_text().unwrap().as_str(), "trade-message",);

    connection.close(Duration::from_secs(2)).await.unwrap();

    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();

    let shared_snapshot = shared.snapshot();
    assert_eq!(shared_snapshot.total_pending_requests, 0);
    assert_eq!(shared_snapshot.budgets.len(), 1);
    assert_eq!(shared_snapshot.budgets[0].locally_used, 1);

    let local_snapshot = connection.limiter().snapshot();
    assert_eq!(local_snapshot.budgets.len(), 1);
    assert_eq!(local_snapshot.budgets[0].locally_used, 1);
}

// ============================================================
// INSECURE EXTERNAL URL REJECTION
// ============================================================

#[tokio::test]
async fn rejects_insecure_external_url() {
    let (compiled, shared) = compiled_limits();

    let result = WebSocketConnector::connect(
        "ws://example.com/feed",
        1,
        &compiled,
        &shared,
        Duration::from_secs(1),
    )
    .await;

    assert!(matches!(
        result,
        Err(WebSocketTransportError::Configuration(_)),
    ));

    assert_eq!(shared.snapshot().budgets[0].locally_used, 0);
}

// ============================================================
// OUTBOUND LIMITER
// ============================================================

#[tokio::test]
async fn outbound_limiter_rejects_third_message() {
    let listener = crate::websocket_server::listen().await;

    let address = listener.local_addr().unwrap();

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();

        for _ in 0..2 {
            assert!(socket.next().await.unwrap().is_ok());
        }
    });

    let (compiled, shared) = compiled_limits();

    let mut connection = WebSocketConnector::connect(
        &format!("ws://127.0.0.1:{}", address.port()),
        43,
        &compiled,
        &shared,
        Duration::from_secs(2),
    )
    .await
    .unwrap();

    for _ in 0..2 {
        connection
            .send_text("message", Duration::from_secs(1))
            .await
            .unwrap();
    }

    let third = connection
        .send_text("blocked", Duration::from_millis(50))
        .await;

    assert!(matches!(
        third,
        Err(WebSocketTransportError::OperationTimeout),
    ));

    let snapshot = connection.limiter().snapshot();

    assert_eq!(snapshot.budgets.len(), 1);
    assert_eq!(snapshot.budgets[0].locally_used, 2);
    assert_eq!(snapshot.total_pending_requests, 0);

    drop(connection);

    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
}

async fn check_gateio_upgrade(profile: &str, route: Option<&str>, reconnect: bool) {
    use marketforge_engine::live::{
        application::{compile_limiters, compile_websocket},
        config::{ExchangeConfigStore, FormatConfigStore, general::RecoveryDefaultsConfig},
        infrastructure::websocket::{
            DesiredSubscription, ReconnectPolicy, StreamInput, StreamSource, WebSocketManager,
        },
    };
    use serde_json::{Value, json};
    use std::{collections::BTreeMap, sync::Arc};
    use tokio::sync::mpsc;

    const TIMEOUT: Duration = Duration::from_secs(5);
    let listener = crate::websocket_server::listen().await;
    let endpoint = format!("ws://{}", listener.local_addr().unwrap());
    let configs = ExchangeConfigStore::load_dir(crate::support::data(".config/live/exchanges"))
        .unwrap()
        .selected(&[profile.to_owned()].into())
        .unwrap();
    let limiters = compile_limiters(&configs, "local-test", &BTreeMap::new()).unwrap();
    let mut runtime = compile_websocket(
        &configs,
        &limiters,
        &RecoveryDefaultsConfig {
            initial_backoff_ms: 20,
            max_backoff_seconds: 1,
            jitter: false,
        },
        &BTreeMap::from([(profile.to_owned(), endpoint.clone())]),
    )
    .unwrap();
    runtime.reconnect_policy = ReconnectPolicy::new(
        Duration::from_millis(20),
        Duration::from_millis(20),
        Duration::from_secs(5),
        Duration::from_secs(1),
        Some(3),
    )
    .unwrap();
    let compiled = &mut runtime.profiles[0];
    let route = route.map(str::to_owned);
    assert_eq!(compiled.urls[&route], endpoint);
    let expected_path = format!("/{}", route.as_deref().unwrap_or("default"));
    // Distinguish the selected route on the wire without selecting by settlement metadata.
    compiled
        .urls
        .insert(route.clone(), format!("{endpoint}{expected_path}"));
    Arc::make_mut(&mut compiled.exchange)
        .channels
        .get_mut("trades")
        .unwrap()
        .route = route;

    let formats =
        FormatConfigStore::load_dir(crate::support::data(".config/live/formats")).unwrap();
    let trades = formats.trade(profile).unwrap().protocol.as_ref().unwrap();
    let depth = formats.depth(profile).unwrap().protocol.as_ref().unwrap();
    assert!(trades.require_decimal_size_support && depth.require_decimal_size_support);
    for (name, value) in [
        (&trades.required_header, &trades.required_header_value),
        (&depth.required_header, &depth.required_header_value),
    ] {
        assert!(name.eq_ignore_ascii_case("X-Gate-Size-Decimal"));
        assert_eq!(value, "1");
        assert_eq!(compiled.exchange.websocket.headers.get(name), Some(value));
    }

    let symbol = if profile == "gateio_inverse" {
        "BTC_USD"
    } else {
        "BTC_USDT"
    };
    let (close_first, mut close_request) = mpsc::channel(1);
    let server = tokio::spawn(async move {
        for attempt in 0..if reconnect { 2 } else { 1 } {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
                assert_eq!(request.uri().path(), expected_path);
                let headers = request.headers();
                assert_eq!(headers["x-gate-size-decimal"], "1");
                assert_eq!(headers.get_all("x-gate-size-decimal").iter().count(), 1);
                assert!(headers.contains_key("sec-websocket-key"));
                Ok(response)
            })
            .await
            .unwrap();

            let subscribe: Value = serde_json::from_str(
                socket
                    .next()
                    .await
                    .unwrap()
                    .unwrap()
                    .into_text()
                    .unwrap()
                    .as_str(),
            )
            .unwrap();
            assert_eq!(subscribe["event"], "subscribe");
            assert_eq!(subscribe["channel"], "futures.trades");
            assert_eq!(subscribe["payload"], json!([symbol]));
            assert!(subscribe["id"].as_u64().is_some());
            socket
                .send(Message::Text(
                    json!({
                        "channel": "futures.trades",
                        "event": "subscribe",
                        "id": subscribe["id"],
                        "result": {"status": "success"}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();

            if reconnect && attempt == 0 {
                close_request.recv().await.unwrap();
                socket.close(None).await.unwrap();
            } else {
                assert!(matches!(
                    socket.next().await,
                    Some(Ok(Message::Close(_)))
                        | Some(Err(tokio_tungstenite::tungstenite::Error::Protocol(
                            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake
                        )))
                        | None
                ));
            }
        }
    });

    let manager = WebSocketManager::from_runtime(runtime).unwrap();
    let (destination, mut events) = mpsc::channel(8);
    manager
        .attach(
            StreamSource::new(
                profile,
                DesiredSubscription::new("test-trades", "trades", symbol).unwrap(),
                [("/channel".into(), json!("futures.trades"))],
            )
            .unwrap(),
            destination,
        )
        .await
        .unwrap();

    let StreamInput::Connected(first) = tokio::time::timeout(TIMEOUT, events.recv())
        .await
        .unwrap()
        .unwrap()
    else {
        panic!("expected ACK-confirmed first acquisition");
    };
    if reconnect {
        close_first.send(()).await.unwrap();
        let StreamInput::Disconnected(revoked) = tokio::time::timeout(TIMEOUT, events.recv())
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected first acquisition to lose authority");
        };
        assert_eq!(first, revoked);
        let StreamInput::Connected(second) = tokio::time::timeout(TIMEOUT, events.recv())
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected restored subscription on replacement generation");
        };
        assert_eq!(first.connection.slot_id(), second.connection.slot_id());
        assert!(second.connection.generation().get() > first.connection.generation().get());
        assert!(second.generation > first.generation);
    }

    tokio::time::timeout(TIMEOUT, manager.shutdown())
        .await
        .unwrap()
        .unwrap();
    tokio::time::timeout(TIMEOUT, server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn gateio_inverse_connection_and_reconnect_send_decimal_header_on_default_endpoint() {
    check_gateio_upgrade("gateio_inverse", None, true).await;
}

#[tokio::test]
async fn gateio_linear_connection_sends_decimal_header_on_usdt_route() {
    check_gateio_upgrade("gateio_linear", Some("usdt"), false).await;
}

#[tokio::test]
async fn gateio_linear_reconnect_retains_decimal_header_on_usd1_route() {
    check_gateio_upgrade("gateio_linear", Some("usd1"), true).await;
}

#[test]
fn connection_preflight_rejects_invalid_headers() {
    use marketforge_engine::live::{
        application::{compile_limiters, compile_websocket},
        config::{ExchangeConfigStore, general::RecoveryDefaultsConfig},
    };
    use std::collections::BTreeMap;

    let directory = tempfile::tempdir().unwrap();
    let path = crate::support::data(".config/live/exchanges/gateio_inverse.toml");
    let mut profile: toml::Value = toml::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    profile["websocket"]["headers"]["X-Gate-Size-Decimal"] = "invalid\r\nX-Other: injected".into();
    std::fs::write(
        directory.path().join("gateio_inverse.toml"),
        toml::to_string(&profile).unwrap(),
    )
    .unwrap();

    let configs = ExchangeConfigStore::load_dir(directory.path()).unwrap();
    let limiters = compile_limiters(&configs, "local-test", &BTreeMap::new()).unwrap();
    let error = compile_websocket(
        &configs,
        &limiters,
        &RecoveryDefaultsConfig {
            initial_backoff_ms: 20,
            max_backoff_seconds: 1,
            jitter: false,
        },
        &BTreeMap::new(),
    )
    .err()
    .expect("invalid header must fail compilation");
    assert!(error.contains("gateio_inverse: invalid value for websocket.headers"));
    assert!(!error.contains("injected"));
}
