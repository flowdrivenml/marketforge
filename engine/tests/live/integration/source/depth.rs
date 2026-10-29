use super::{
    harness::*,
    http_server::{MockExchange, MockResponse},
    profiles, rest_support,
};
use marketforge_engine::live::{
    integrity::DepthPublicationEvent,
    stream::{request::SourceChannel, source::*},
};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio::{sync::mpsc, time::timeout};

fn snapshot(profile: &str, symbol: &str, subscription: &Value) -> Value {
    if profile.starts_with("bybit") {
        json!({"topic":subscription["args"][0],"type":"snapshot","ts":1720000000000_u64,"cts":1720000000000_u64,"data":{"s":symbol,"u":100,"seq":200,"b":[["80000","2"]],"a":[["100000","3"]]}})
    } else if profile.starts_with("okx") {
        json!({"arg":subscription["args"][0],"action":"snapshot","data":[{"ts":"1720000000000","seqId":100,"prevSeqId":-1,"checksum":0,"bids":[["80000","2","0","1"]],"asks":[["100000","3","0","1"]]}]})
    } else if profile.starts_with("bitget") {
        json!({"arg":subscription["args"][0],"action":"snapshot","data":[{"ts":"1720000000000","seq":100,"pseq":0,"checksum":0,"b":[["80000","2"]],"a":[["100000","3"]]}]})
    } else {
        panic!("REST profiles use buffered deltas")
    }
}
fn delta(profile: &str, subscription: &Value) -> Value {
    if profile.starts_with("gateio") {
        return super::gateio::depth_update(
            profile,
            subscription["payload"][0].as_str().unwrap(),
            101,
            101,
        );
    }
    if profile.starts_with("binance") {
        let data = json!({"e":"depthUpdate","E":1720000000000_u64,"T":1720000000000_u64,"s":if profile=="binance_inverse" {"BTCUSD_PERP"}else{"BTCUSDT"},"U":100,"u":101,"pu":99,"b":[["80000","4"]],"a":[["100000","3"]]});
        if profile == "binance_spot" {
            data
        } else {
            json!({"stream":subscription["params"][0],"data":data})
        }
    } else {
        json!({"arg":subscription["args"][0],"action":"update","data":[{"ts":"1720000000001","seq":101,"pseq":100,"checksum":0,"b":[["80000","4"]],"a":[["100000","3"]]}]})
    }
}
const REST: &str = r#"{"lastUpdateId":100,"E":1720000000000,"T":1720000000000,"bids":[["80000","2"]],"asks":[["100000","3"]]}"#;

#[tokio::test]
async fn depth_profile_capability_matrix() {
    for mut request in profiles::requests() {
        request.channel = SourceChannel::Depth;
        let profile = format!(
            "{}_{}",
            request.exchange.as_str(),
            request.market_category.as_str()
        );
        let body = if profile.starts_with("gateio") {
            super::gateio::snapshot_body(&profile)
        } else {
            REST.to_owned()
        };
        let (response, gate) = MockResponse::gated(body);
        let http = MockExchange::start(vec![response]);
        let mut socket = LocalSocket::start(&profile).await;
        let configs = Arc::new(rest_support::load_profile(&profile, http.base_url()));
        let limiters = rest_support::build_limiters(&configs, &profile);
        let manager = manager(&configs, &profile, &limiters, &socket.url);
        let definition =
            profiles::definition(request.clone(), &format!("depth-{}", request.instrument_id));
        let id = definition.stream_id.clone();
        let prepared = builder(configs.clone(), SourceInitializationLimits::default())
            .prepare(definition.clone())
            .unwrap();
        let prepared = if profile == "bitget_linear" {
            prepared
                .with_subscription_parameter("native_ws_inst_type", "usdt-futures")
                .unwrap()
        } else {
            prepared
        };
        let metrics = prepared.runner().instance().metrics_handle();
        let (recovery, task) = recovery(configs, &profile, limiters);
        let executor = prepared
            .attach(manager.clone(), recovery.clone())
            .await
            .unwrap();
        let mut supervisor = supervisor(definition);
        let (tx, mut rx) = mpsc::channel(16);
        supervisor
            .start(
                &id,
                executor,
                move |events| {
                    for event in events {
                        match event {
                            DepthPublicationEvent::Snapshot { levels, .. } => {
                                assert_eq!(levels.len(), 2);
                                tx.try_send("snapshot").map_err(|e| e.to_string())?;
                            }
                            DepthPublicationEvent::Delta { changes, .. } => {
                                assert!(!changes.is_empty());
                                tx.try_send("delta").map_err(|e| e.to_string())?;
                            }
                            DepthPublicationEvent::Invalidated { .. } => {
                                tx.try_send("invalidated").map_err(|e| e.to_string())?;
                            }
                        }
                    }
                    Ok(())
                },
                |_| panic!("depth cannot publish trades"),
            )
            .unwrap();
        let subscription = socket.ack().await;
        if profile.starts_with("binance") || profile.starts_with("gateio") {
            socket
                .send
                .send(delta(&profile, &subscription))
                .await
                .unwrap();
            timeout(DEADLINE, async {
                while http.request_count() == 0 || metrics.messages_received_total() == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            gate.release();
        } else {
            socket
                .send
                .send(snapshot(&profile, &request.symbol, &subscription))
                .await
                .unwrap();
            if profile.starts_with("bitget") {
                socket
                    .send
                    .send(delta(&profile, &subscription))
                    .await
                    .unwrap();
            }
        }
        let first = timeout(DEADLINE, rx.recv()).await.unwrap();
        supervisor.reap_finished().await.unwrap();
        let first = first.unwrap_or_else(|| {
            panic!(
                "{profile}: snapshot missing, status={:?}",
                supervisor.registry().status(&id)
            )
        });
        assert_eq!(first, "snapshot", "{profile}");
        wait_running(&mut supervisor, &id).await;
        println!(
            "[{profile}/depth] subscription ACK -> normalization -> synchronized snapshot publication -> Ready; rest_requests={}",
            http.request_count()
        );
        timeout(DEADLINE, supervisor.stop(&id))
            .await
            .unwrap()
            .unwrap();
        // Shutdown must publish acknowledged book invalidation.
        let mut invalidated = false;
        while let Ok(event) = rx.try_recv() {
            invalidated |= event == "invalidated";
        }
        assert!(
            invalidated,
            "{profile}: shutdown did not invalidate published book"
        );
        assert_eq!(manager.stream_count().await, 0);
        manager.shutdown().await.unwrap();
        recovery.shutdown().await.unwrap();
        timeout(DEADLINE, task).await.unwrap().unwrap();
    }
}

async fn depth_failure_case(
    name: &str,
    responses: Vec<MockResponse>,
    fail_publication: bool,
    success: bool,
) {
    let (responses, gate) = if name == "retry-http" {
        let (response, gate) = MockResponse::gated(REST);
        (vec![MockResponse::error(503, "{}"), response], Some(gate))
    } else if fail_publication {
        let (response, gate) = MockResponse::gated(REST);
        (
            vec![response, MockResponse::ok(REST), MockResponse::ok(REST)],
            Some(gate),
        )
    } else {
        (responses, None)
    };
    let http = MockExchange::start(responses);
    let mut socket = LocalSocket::start("binance_spot").await;
    let configs = Arc::new(rest_support::load_profile("binance_spot", http.base_url()));
    let limiters = rest_support::build_limiters(&configs, "binance_spot");
    let manager = manager(&configs, "binance_spot", &limiters, &socket.url);
    let mut request = profiles::requests().remove(3);
    request.channel = SourceChannel::Depth;
    let definition = profiles::definition(request, name);
    let prepared = builder(configs.clone(), SourceInitializationLimits::default())
        .prepare(definition.clone())
        .unwrap();
    let metrics = prepared.runner().instance().metrics_handle();
    let (recovery, task) = recovery(configs, "binance_spot", limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let mut supervisor = supervisor(definition);
    let (tx, mut rx) = mpsc::channel(16);
    let publications = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = publications.clone();
    supervisor
        .start(
            name,
            executor,
            move |events| {
                for event in events {
                    if matches!(event, DepthPublicationEvent::Snapshot { .. }) {
                        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        if fail_publication {
                            return Err("injected snapshot handoff uncertainty".into());
                        }
                        tx.try_send(()).map_err(|e| e.to_string())?;
                    }
                }
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap();
    let subscription = socket.ack().await;
    socket
        .send
        .send(delta("binance_spot", &subscription))
        .await
        .unwrap();
    if let Some(gate) = gate {
        let expected_requests = if name == "retry-http" { 2 } else { 1 };
        timeout(DEADLINE, async {
            while http.request_count() < expected_requests {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if name == "retry-http" {
            socket
                .send
                .send(delta("binance_spot", &subscription))
                .await
                .unwrap();
        }
        let expected_messages = if name == "retry-http" { 2 } else { 1 };
        timeout(DEADLINE, async {
            while metrics.messages_received_total() < expected_messages {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        gate.release();
    }
    if success {
        timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
        wait_running(&mut supervisor, name).await;
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
        .unwrap();
        let status = supervisor.registry().status(name).unwrap();
        assert_eq!(
            status.lifecycle,
            marketforge_engine::live::stream::registry::StreamLifecycleState::Failed
        );
        assert!(rx.try_recv().is_err());
        if fail_publication {
            assert!(publications.load(std::sync::atomic::Ordering::SeqCst) > 0);
        }
    }
    assert!(http.request_count() <= 3, "unbounded retries");
    if name == "retry-http" {
        assert_eq!(http.request_count(), 2);
    }
    println!(
        "[binance_spot/depth/{name}] http_attempts={} snapshot_handoffs={} final={:?}",
        http.request_count(),
        publications.load(std::sync::atomic::Ordering::SeqCst),
        supervisor.registry().status(name).unwrap().lifecycle
    );
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
}
#[tokio::test]
async fn retryable_depth_http_failure_recovers() {
    depth_failure_case(
        "retry-http",
        vec![MockResponse::error(503, "{}"), MockResponse::ok(REST)],
        false,
        true,
    )
    .await;
}
#[tokio::test]
async fn depth_http_unavailable_exhausts_bounded_retry_budget() {
    depth_failure_case("http-unavailable", vec![], false, false).await;
}
#[tokio::test]
async fn malformed_depth_http_response_fails_safely() {
    depth_failure_case(
        "malformed-depth",
        vec![MockResponse::ok("bad-json")],
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn snapshot_cannot_bridge_buffered_depth_fails_safely() {
    let stale = REST.replace("\"lastUpdateId\":100", "\"lastUpdateId\":1");
    depth_failure_case(
        "stale-snapshot",
        vec![
            MockResponse::ok(stale.clone()),
            MockResponse::ok(stale.clone()),
            MockResponse::ok(stale),
        ],
        false,
        false,
    )
    .await;
}
#[tokio::test]
async fn uncertain_depth_snapshot_publication_never_becomes_ready() {
    depth_failure_case(
        "snapshot-handoff-fails",
        vec![
            MockResponse::ok(REST),
            MockResponse::ok(REST),
            MockResponse::ok(REST),
        ],
        true,
        false,
    )
    .await;
}

#[tokio::test]
async fn failed_depth_invalidation_is_not_reported_as_clean_shutdown() {
    let profile = "bybit_spot";
    let mut socket = LocalSocket::start(profile).await;
    let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&configs, profile);
    let manager = manager(&configs, profile, &limiters, &socket.url);
    let mut request = profiles::requests().remove(0);
    request.channel = SourceChannel::Depth;
    let definition = profiles::definition(request, "failed-invalidation");
    let prepared = builder(configs.clone(), SourceInitializationLimits::default())
        .prepare(definition.clone())
        .unwrap();
    let (recovery, task) = recovery(configs, profile, limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let mut supervisor = supervisor(definition);
    let (tx, mut rx) = mpsc::channel(4);
    supervisor
        .start(
            "failed-invalidation",
            executor,
            move |events| {
                for event in events {
                    match event {
                        DepthPublicationEvent::Invalidated { .. } => {
                            return Err("injected invalidation failure".into());
                        }
                        DepthPublicationEvent::Snapshot { .. } => tx.try_send(()).unwrap(),
                        _ => {}
                    }
                }
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap();
    let subscription = socket.ack().await;
    socket
        .send
        .send(snapshot(profile, "BTCUSDT", &subscription))
        .await
        .unwrap();
    timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
    wait_running(&mut supervisor, "failed-invalidation").await;
    let stopped = timeout(DEADLINE, supervisor.stop("failed-invalidation"))
        .await
        .unwrap();
    assert!(stopped.is_err());
    assert_eq!(
        supervisor
            .registry()
            .status("failed-invalidation")
            .unwrap()
            .lifecycle,
        marketforge_engine::live::stream::registry::StreamLifecycleState::Failed
    );
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
}

#[tokio::test]
async fn depth_buffer_overflow_revokes_snapshot_and_restarts_bounded_recovery() {
    let (first, first_gate) = MockResponse::gated(REST);
    let (second, second_gate) = MockResponse::gated(REST);
    let http = MockExchange::start(vec![first, second]);
    let mut socket = LocalSocket::start("binance_spot").await;
    let configs = Arc::new(rest_support::load_profile("binance_spot", http.base_url()));
    let limiters = rest_support::build_limiters(&configs, "binance_spot");
    let manager = manager(&configs, "binance_spot", &limiters, &socket.url);
    let mut request = profiles::requests().remove(3);
    request.channel = SourceChannel::Depth;
    let definition = profiles::definition(request, "depth-buffer-bound");
    let mut limits = SourceInitializationLimits::default();
    limits.depth_buffer = marketforge_engine::live::integrity::DepthBufferLimits {
        max_messages: 2,
        max_bytes: 4096,
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
    let published = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = published.clone();
    supervisor
        .start(
            "depth-buffer-bound",
            executor,
            move |events| {
                for event in events {
                    if matches!(event, DepthPublicationEvent::Snapshot { .. }) {
                        count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    }
                }
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap();
    let subscription = socket.ack().await;
    timeout(DEADLINE, async {
        while http.request_count() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    for sequence in 100..=102 {
        let mut message = delta("binance_spot", &subscription);
        message["U"] = sequence.into();
        message["u"] = sequence.into();
        socket.send.send(message).await.unwrap();
    }
    timeout(DEADLINE, async {
        while metrics.messages_received_total() < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first_gate.release();
    timeout(DEADLINE, async {
        while http.request_count() != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    supervisor.sync_readiness().unwrap();
    assert_ne!(
        supervisor
            .registry()
            .status("depth-buffer-bound")
            .unwrap()
            .lifecycle,
        marketforge_engine::live::stream::registry::StreamLifecycleState::Running
    );
    assert_eq!(
        published.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "overflowed snapshot was published"
    );
    timeout(DEADLINE, supervisor.stop("depth-buffer-bound"))
        .await
        .unwrap()
        .unwrap();
    second_gate.release();
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
    println!(
        "[binance_spot/depth] buffer_limit=2 third_delta invalidated attempt; HTTP attempts=2; obsolete snapshot not published; shutdown during replacement completed"
    );
}

#[tokio::test]
async fn accepted_depth_gap_invalidates_and_recovers_through_rest() {
    let (first, first_gate) = MockResponse::gated(REST);
    let (second, second_gate) =
        MockResponse::gated(REST.replace("\"lastUpdateId\":100", "\"lastUpdateId\":110"));
    let http = MockExchange::start(vec![first, second]);
    let mut socket = LocalSocket::start("binance_spot").await;
    let configs = Arc::new(rest_support::load_profile("binance_spot", http.base_url()));
    let limiters = rest_support::build_limiters(&configs, "binance_spot");
    let manager = manager(&configs, "binance_spot", &limiters, &socket.url);
    let mut request = profiles::requests().remove(3);
    request.channel = SourceChannel::Depth;
    let definition = profiles::definition(request, "depth-gap-rest");
    let prepared = builder(configs.clone(), SourceInitializationLimits::default())
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
            "depth-gap-rest",
            executor,
            move |events| {
                for event in events {
                    tx.try_send(match event {
                        DepthPublicationEvent::Snapshot { .. } => "snapshot",
                        DepthPublicationEvent::Invalidated { .. } => "invalidated",
                        _ => "delta",
                    })
                    .map_err(|e| e.to_string())?;
                }
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap();
    let subscription = socket.ack().await;
    socket
        .send
        .send(delta("binance_spot", &subscription))
        .await
        .unwrap();
    timeout(DEADLINE, async {
        while http.request_count() < 1 || metrics.messages_received_total() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    first_gate.release();
    assert_eq!(
        timeout(DEADLINE, rx.recv()).await.unwrap().unwrap(),
        "snapshot"
    );
    wait_running(&mut supervisor, "depth-gap-rest").await;
    let mut gap = delta("binance_spot", &subscription);
    gap["U"] = 110.into();
    gap["u"] = 110.into();
    socket.send.send(gap).await.unwrap();
    assert_eq!(
        timeout(DEADLINE, rx.recv()).await.unwrap().unwrap(),
        "invalidated"
    );
    timeout(DEADLINE, async {
        while http.request_count() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut bridge = delta("binance_spot", &subscription);
    bridge["U"] = 110.into();
    bridge["u"] = 111.into();
    socket.send.send(bridge).await.unwrap();
    timeout(DEADLINE, async {
        while metrics.messages_received_total() < 3 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    second_gate.release();
    assert_eq!(
        timeout(DEADLINE, rx.recv()).await.unwrap().unwrap(),
        "snapshot"
    );
    wait_running(&mut supervisor, "depth-gap-rest").await;
    timeout(DEADLINE, supervisor.stop("depth-gap-rest"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(http.request_count(), 2);
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
}
