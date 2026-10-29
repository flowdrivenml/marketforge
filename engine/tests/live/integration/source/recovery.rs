use super::{
    harness::*,
    http_server::{MockExchange, MockResponse},
    pipeline::published,
    profiles, rest_support,
};
use marketforge_engine::live::stream::{registry::StreamLifecycleState, source::*};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::{sync::mpsc, time::timeout};

fn ws(id: u64) -> Value {
    json!({"e":"trade","E":1720000000000_u64,"T":1720000000000_u64,"s":"BTCUSDT","t":id,"p":"80000","q":"0.001","m":false,"M":true})
}
fn rest(ids: &[u64]) -> String {
    serde_json::to_string(&ids.iter().map(|id|json!({"id":id,"price":"80000","qty":"0.001","quoteQty":"80","time":1720000000000_u64,"isBuyerMaker":false,"isBestMatch":true})).collect::<Vec<_>>()).unwrap()
}

async fn scenario(
    name: &str,
    pages: Vec<MockResponse>,
    success: bool,
    live_during: bool,
    fail_publication: bool,
) {
    let (pages, gate) = if live_during {
        let (response, gate) = MockResponse::gated(rest(&[11]));
        let mut pages = pages;
        pages[0] = response;
        (pages, Some(gate))
    } else if name == "http-timeout" {
        let (response, gate) = MockResponse::gated(rest(&[11, 12, 13]));
        (vec![response], Some(gate))
    } else {
        (pages, None)
    };
    let http = MockExchange::start(pages);
    let mut socket = LocalSocket::start("binance_spot").await;
    let configs = Arc::new(rest_support::load_profile_with(
        "binance_spot",
        http.base_url(),
        |config| {
            if name == "http-timeout" {
                config.rest.request_timeout_seconds = Some(1);
            }
        },
    ));
    let limiters = rest_support::build_limiters(&configs, "binance_spot");
    let manager = manager(&configs, "binance_spot", &limiters, &socket.url);
    let definition = profiles::definition(profiles::requests().remove(3), name);
    let limits = SourceInitializationLimits {
        trade_buffer: SourceTradeBufferLimits {
            max_batches: 2,
            max_records: 2,
            max_bytes: 4096,
        },
        ..SourceInitializationLimits::default()
    };
    let prepared = builder(configs.clone(), limits)
        .prepare(definition.clone())
        .unwrap();
    let metrics = prepared.runner().instance().metrics_handle();
    let (recovery, task) = recovery(configs, "binance_spot", limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let mut supervisor = supervisor(definition);
    let (tx, mut rx) = mpsc::channel(8);
    supervisor
        .start(
            name,
            executor,
            |_| Ok(()),
            move |payload| {
                if fail_publication && matches!(payload, SourceTradePublication::Recovery(_)) {
                    return Err("injected recovery publication failure".into());
                }
                let trades = published(payload);
                if !trades.is_empty() {
                    tx.try_send(trades).map_err(|e| e.to_string())?;
                }
                Ok(())
            },
        )
        .unwrap();
    socket.ack().await;
    socket.send.send(ws(10)).await.unwrap();
    let baseline = timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
    assert_eq!(baseline[0].trade_id.as_deref(), Some("10"));
    wait_running(&mut supervisor, name).await;
    socket
        .send
        .send(ws(if name == "page-limit" { 15 } else { 13 }))
        .await
        .unwrap();
    if live_during {
        socket.send.send(ws(14)).await.unwrap();
        timeout(DEADLINE, async {
            while http.request_count() == 0 || metrics.messages_received_total() < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        supervisor.sync_readiness().unwrap();
        assert_ne!(
            supervisor.registry().status(name).unwrap().lifecycle,
            StreamLifecycleState::Running
        );
        gate.as_ref().unwrap().release();
    }
    if success {
        let recovered = timeout(DEADLINE, rx.recv()).await.unwrap();
        if recovered.is_none() {
            supervisor.reap_finished().await.unwrap();
        }
        let recovered = recovered.unwrap_or_else(|| {
            panic!(
                "{name}: recovery publication missing: {:?}",
                supervisor.registry().status(name)
            )
        });
        let ids: Vec<u64> = recovered
            .iter()
            .map(|t| t.trade_id.as_ref().unwrap().parse().unwrap())
            .collect();
        assert!(
            ids == vec![11, 12, 13] || (live_during && ids == vec![11, 12, 13, 14]),
            "{name}: {ids:?}"
        );
        if live_during && ids.len() == 3 {
            assert_eq!(
                timeout(DEADLINE, rx.recv()).await.unwrap().unwrap()[0]
                    .trade_id
                    .as_deref(),
                Some("14")
            );
        }
        wait_running(&mut supervisor, name).await;
        socket
            .send
            .send(ws(if live_during { 15 } else { 14 }))
            .await
            .unwrap();
        let next = timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
        assert_eq!(next.len(), 1);
        assert_eq!(
            next[0].trade_id.as_deref(),
            Some(if live_during { "15" } else { "14" })
        );
        timeout(DEADLINE, supervisor.stop(name))
            .await
            .unwrap()
            .unwrap();
    } else {
        timeout(DEADLINE, async {
            loop {
                supervisor.reap_finished().await.unwrap();
                if !supervisor.is_active(name) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("failed recovery must terminate within deadline");
        let status = supervisor.registry().status(name).unwrap();
        println!("[{name}] terminal error={:?}", status.last_error);
        if name == "http-timeout" {
            assert!(
                status
                    .last_error
                    .as_ref()
                    .unwrap()
                    .to_lowercase()
                    .contains("timeout")
            );
        }
        if let Some(gate) = &gate {
            gate.release();
        }
        assert_eq!(status.lifecycle, StreamLifecycleState::Failed);
        assert!(
            rx.try_recv().is_err(),
            "failed recovery published identities"
        );
    }
    let requests = http.requests();
    assert!(!requests.is_empty());
    assert!(
        requests[0].request_line.contains("fromId=11"),
        "{:?}",
        requests[0]
    );
    assert!(requests[0].request_line.contains("symbol=BTCUSDT"));
    println!(
        "[binance_spot/trades/{name}] Recovery {}; http_requests={}; ws_messages={}; final={:?}",
        if success {
            "completed and published"
        } else {
            "rejected safely"
        },
        requests.len(),
        metrics.messages_received_total(),
        supervisor.registry().status(name).unwrap().lifecycle
    );
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
}

#[tokio::test]
async fn single_page_trade_recovery() {
    scenario(
        "single-page",
        vec![MockResponse::ok(rest(&[11, 12, 13]))],
        true,
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn multi_page_trade_recovery_with_live_overlap() {
    scenario(
        "multi-page",
        vec![
            MockResponse::ok(rest(&[11])),
            MockResponse::ok(rest(&[12, 13])),
        ],
        true,
        true,
        false,
    )
    .await;
}
#[tokio::test]
async fn incomplete_trade_page_is_rejected() {
    scenario(
        "missing-id",
        vec![MockResponse::ok(rest(&[11, 13]))],
        false,
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn second_page_http_failure_is_rejected() {
    scenario(
        "second-page-fails",
        vec![
            MockResponse::ok(rest(&[11])),
            MockResponse::error(503, "{}"),
        ],
        false,
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn empty_trade_response_is_rejected() {
    scenario("empty", vec![MockResponse::ok("[]")], false, false, false).await;
}
#[tokio::test]
async fn malformed_trade_response_is_rejected() {
    scenario(
        "malformed",
        vec![MockResponse::ok("not-json")],
        false,
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn duplicate_trade_page_is_rejected() {
    scenario(
        "duplicate",
        vec![MockResponse::ok(rest(&[11, 11, 12, 13]))],
        false,
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn pagination_limit_rejects_incomplete_recovery() {
    scenario(
        "page-limit",
        vec![
            MockResponse::ok(rest(&[11])),
            MockResponse::ok(rest(&[12])),
            MockResponse::ok(rest(&[13])),
        ],
        false,
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn recovery_publication_failure_is_terminal() {
    scenario(
        "recovery-publication-fails",
        vec![MockResponse::ok(rest(&[11, 12, 13]))],
        false,
        false,
        true,
    )
    .await;
}
#[tokio::test]
async fn rest_websocket_identity_conflict_is_rejected() {
    let mut records: Value = serde_json::from_str(&rest(&[11, 12, 13])).unwrap();
    records[2]["price"] = "81000".into();
    scenario(
        "identity-conflict",
        vec![MockResponse::ok(records.to_string())],
        false,
        false,
        false,
    )
    .await;
}

async fn interrupt_pending(reconnect: bool, overflow: bool) {
    let (response, gate) = MockResponse::gated(rest(&[11, 12, 13]));
    let http = MockExchange::start(vec![response]);
    let mut socket = LocalSocket::start("binance_spot").await;
    let configs = Arc::new(rest_support::load_profile("binance_spot", http.base_url()));
    let limiters = rest_support::build_limiters(&configs, "binance_spot");
    let manager = manager(&configs, "binance_spot", &limiters, &socket.url);
    let definition = profiles::definition(profiles::requests().remove(3), "interrupt-rest");
    let limits = SourceInitializationLimits {
        trade_buffer: SourceTradeBufferLimits {
            max_batches: 2,
            max_records: 2,
            max_bytes: 4096,
        },
        ..SourceInitializationLimits::default()
    };
    let prepared = builder(configs.clone(), limits)
        .prepare(definition.clone())
        .unwrap();
    let metrics = prepared.runner().instance().metrics_handle();
    let (recovery, task) = recovery(configs, "binance_spot", limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let mut supervisor = supervisor(definition);
    let (tx, mut rx) = mpsc::channel(8);
    supervisor
        .start(
            "interrupt-rest",
            executor,
            |_| Ok(()),
            move |payload| tx.try_send(published(payload)).map_err(|e| e.to_string()),
        )
        .unwrap();
    socket.ack().await;
    socket.send.send(ws(10)).await.unwrap();
    assert_eq!(
        timeout(DEADLINE, rx.recv()).await.unwrap().unwrap()[0]
            .trade_id
            .as_deref(),
        Some("10")
    );
    socket.send.send(ws(13)).await.unwrap();
    timeout(DEADLINE, async {
        while http.request_count() == 0 || metrics.messages_received_total() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    if overflow {
        for id in [14, 15, 16] {
            socket.send.send(ws(id)).await.unwrap();
        }
        timeout(DEADLINE, async {
            loop {
                supervisor.reap_finished().await.unwrap();
                if !supervisor.is_active("interrupt-rest") {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let status = supervisor.registry().status("interrupt-rest").unwrap();
        assert_eq!(status.lifecycle, StreamLifecycleState::Failed);
        assert!(
            status
                .last_error
                .as_ref()
                .unwrap()
                .contains("capacity exceeded")
        );
        assert!(rx.try_recv().is_err());
    }
    if reconnect {
        socket.send.send(Value::Null).await.unwrap();
        socket.ack().await;
        socket.send.send(ws(20)).await.unwrap();
        assert_eq!(
            timeout(DEADLINE, rx.recv()).await.unwrap().unwrap()[0]
                .trade_id
                .as_deref(),
            Some("20")
        );
        wait_running(&mut supervisor, "interrupt-rest").await;
        gate.release();
        socket.send.send(ws(21)).await.unwrap();
        assert_eq!(
            timeout(DEADLINE, rx.recv()).await.unwrap().unwrap()[0]
                .trade_id
                .as_deref(),
            Some("21")
        );
        assert!(
            rx.try_recv().is_err(),
            "stale completion published into new acquisition"
        );
    }
    if !overflow {
        timeout(DEADLINE, supervisor.stop("interrupt-rest"))
            .await
            .unwrap()
            .unwrap();
    }
    gate.release();
    assert_eq!(manager.stream_count().await, 0);
    assert_eq!(supervisor.active_count(), 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
    println!(
        "[binance_spot/trades] pending REST interrupted reconnect={reconnect}; stale authority revoked; shutdown completed"
    );
}
#[tokio::test]
async fn shutdown_cancels_pending_trade_recovery() {
    interrupt_pending(false, false).await;
}
#[tokio::test]
async fn reconnect_fences_pending_trade_recovery() {
    interrupt_pending(true, false).await;
}

#[tokio::test]
async fn bounded_live_trade_buffer_exhaustion_fails_closed() {
    interrupt_pending(false, true).await;
}

#[tokio::test]
async fn trade_http_timeout_releases_recovery_resources() {
    scenario("http-timeout", vec![], false, false, false).await;
}
