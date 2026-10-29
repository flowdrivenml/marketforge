use super::{harness::*, profiles, rest_support};
use marketforge_engine::{
    live::{
        integrity::{BoundTradeIngressOutcome, IntegrityOutcome, TradeControllerOutcome},
        stream::{registry::StreamLifecycleState, source::*},
    },
    shared::domain::canonical::Trade,
};
use serde_json::{Value, json};
use std::{sync::Arc, time::Instant};
use tokio::{sync::mpsc, time::timeout};

pub fn native(profile: &str) -> Value {
    serde_json::from_str::<Value>(include_str!("../../support/data/source/trades.json")).unwrap()
        [profile]
        .clone()
}
pub fn published(payload: SourceTradePublication<'_>) -> Vec<Trade> {
    match payload {
        SourceTradePublication::Recovery(trades) => trades.to_vec(),
        SourceTradePublication::Integrity(IntegrityOutcome::Trades(
            BoundTradeIngressOutcome::Processed(TradeControllerOutcome::Accepted(batch)),
        )) => batch.trades.clone(),
        _ => panic!("only accepted market data may be published"),
    }
}

#[tokio::test]
async fn trade_profile_capability_matrix() {
    for request in profiles::requests() {
        let started = Instant::now();
        let profile = format!(
            "{}_{}",
            request.exchange.as_str(),
            request.market_category.as_str()
        );
        let mut server = LocalSocket::start(&profile).await;
        let configs = Arc::new(rest_support::load_profile(&profile, "http://127.0.0.1:1"));
        let limiters = rest_support::build_limiters(&configs, &profile);
        let manager = manager(&configs, &profile, &limiters, &server.url);
        let builder = builder(configs.clone(), SourceInitializationLimits::default());
        let definition = profiles::definition(
            request.clone(),
            &format!("matrix-{}", request.instrument_id),
        );
        let id = definition.stream_id.clone();
        let prepared = builder.prepare(definition.clone()).unwrap();
        let prepared = if profile == "bitget_linear" {
            prepared
                .with_subscription_parameter("native_ws_inst_type", "usdt-futures")
                .unwrap()
        } else {
            prepared
        };
        let metrics = prepared.runner().instance().metrics_handle();
        let (recovery, recovery_task) = recovery(configs, &profile, limiters);
        if profile.starts_with("gateio") {
            let result = prepared.attach(manager.clone(), recovery.clone()).await;
            assert!(
                result
                    .err()
                    .unwrap()
                    .to_string()
                    .contains("instrument-aware")
            );
            assert_eq!(manager.stream_count().await, 0);
            manager.shutdown().await.unwrap();
            recovery.shutdown().await.unwrap();
            timeout(DEADLINE, recovery_task).await.unwrap().unwrap();
            println!(
                "[{profile}/trades] UNSUPPORTED: existing channel resolver lacks instrument-aware routing; no acquisition admitted"
            );
            continue;
        }
        let executor = prepared
            .attach(manager.clone(), recovery.clone())
            .await
            .unwrap();
        let mut supervisor = supervisor(definition);
        let (tx, mut rx) = mpsc::channel(8);
        supervisor
            .start(
                &id,
                executor,
                |_| panic!("trade source cannot publish depth"),
                move |payload| {
                    let trades = published(payload);
                    if !trades.is_empty() {
                        tx.try_send(trades).map_err(|e| e.to_string())?;
                    }
                    Ok(())
                },
            )
            .unwrap();
        let subscription = server.ack().await;
        println!(
            "[{profile}/trades] WebSocket connected; subscription acknowledged: {subscription}"
        );
        assert_ne!(
            supervisor.registry().status(&id).unwrap().lifecycle,
            StreamLifecycleState::Running
        );
        let payload = native(&profile);
        let payload = if profile.starts_with("binance") && profile != "binance_spot" {
            json!({"stream": subscription["params"][0], "data":payload})
        } else {
            payload
        };
        server.send.send(payload).await.unwrap();
        let trades = timeout(DEADLINE, rx.recv())
            .await
            .expect("publication deadline")
            .expect("executor exited");
        assert!(!trades.is_empty());
        for trade in &trades {
            assert_eq!(trade.envelope.stream_id, id);
            assert_eq!(trade.envelope.instrument_id, request.instrument_id);
            assert_eq!(trade.envelope.symbol, request.symbol);
            assert_eq!(trade.price, rust_decimal::Decimal::from(80000));
            assert!(trade.trade_id.is_some());
            assert!(trade.quantity_base.is_some() || trade.quantity_quote.is_some());
            if request.quantity_type
                == marketforge_engine::live::stream::request::QuantityType::Contracts
            {
                assert!(trade.quantity_contracts.is_some());
            }
        }
        wait_running(&mut supervisor, &id).await;
        assert_eq!(metrics.messages_received_total(), 1);
        println!(
            "[{profile}/trades] Normalization and integrity completed; canonical_records={}; Final readiness=Ready",
            trades.len()
        );
        timeout(DEADLINE, supervisor.stop(&id))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(supervisor.active_count(), 0);
        assert_eq!(
            supervisor.registry().status(&id).unwrap().lifecycle,
            StreamLifecycleState::Stopped
        );
        assert_eq!(manager.stream_count().await, 0);
        manager.shutdown().await.unwrap();
        recovery.shutdown().await.unwrap();
        timeout(DEADLINE, recovery_task).await.unwrap().unwrap();
        println!(
            "[{profile}/trades] Shutdown completed duration={:?}",
            started.elapsed()
        );
    }
}

#[tokio::test]
async fn publication_capacity_failure_terminates_without_false_readiness() {
    let request = profiles::requests().remove(0);
    let profile = "bybit_spot";
    let mut server = LocalSocket::start(profile).await;
    let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&configs, profile);
    let manager = manager(&configs, profile, &limiters, &server.url);
    let definition = profiles::definition(request, "capacity-failure");
    let prepared = builder(configs.clone(), SourceInitializationLimits::default())
        .prepare(definition.clone())
        .unwrap();
    let (recovery, task) = recovery(configs, profile, limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let mut supervisor = supervisor(definition);
    let (tx, _slow_consumer) = mpsc::channel::<Vec<Trade>>(1);
    tx.try_send(vec![]).unwrap();
    supervisor
        .start(
            "capacity-failure",
            executor,
            |_| Ok(()),
            move |payload| tx.try_send(published(payload)).map_err(|e| e.to_string()),
        )
        .unwrap();
    server.ack().await;
    server.send.send(native(profile)).await.unwrap();
    timeout(DEADLINE, async {
        loop {
            supervisor.reap_finished().await.unwrap();
            if !supervisor.is_active("capacity-failure") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let status = supervisor.registry().status("capacity-failure").unwrap();
    assert_eq!(status.lifecycle, StreamLifecycleState::Failed);
    assert!(status.last_error.as_ref().unwrap().contains("handoff"));
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
    println!(
        "[{profile}/trades] bounded publication capacity rejected; Failed; resources released"
    );
}

#[tokio::test]
async fn repeated_start_stop_and_rapid_reconnect_cycles() {
    let profile = "bybit_spot";
    let mut server = LocalSocket::start(profile).await;
    let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&configs, profile);
    let manager = manager(&configs, profile, &limiters, &server.url);
    let builder = builder(configs.clone(), SourceInitializationLimits::default());
    let definition = profiles::definition(profiles::requests().remove(0), "lifecycle-cycles");
    let (recovery, task) = recovery(configs, profile, limiters);
    let mut supervisor = supervisor(definition.clone());
    for cycle in 0..3 {
        let executor = builder
            .prepare(definition.clone())
            .unwrap()
            .attach(manager.clone(), recovery.clone())
            .await
            .unwrap();
        let (tx, mut rx) = mpsc::channel(8);
        let generation = supervisor
            .start(
                "lifecycle-cycles",
                executor,
                |_| Ok(()),
                move |payload| tx.try_send(published(payload)).map_err(|e| e.to_string()),
            )
            .unwrap();
        server.ack().await;
        server.send.send(native(profile)).await.unwrap();
        assert_eq!(
            timeout(DEADLINE, rx.recv()).await.unwrap().unwrap().len(),
            1
        );
        wait_running(&mut supervisor, "lifecycle-cycles").await;
        for reconnect in 0..3 {
            server.send.send(Value::Null).await.unwrap();
            server.ack().await;
            timeout(DEADLINE, async {
                loop {
                    supervisor.sync_readiness().unwrap();
                    if supervisor
                        .registry()
                        .status("lifecycle-cycles")
                        .unwrap()
                        .lifecycle
                        != StreamLifecycleState::Running
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            server.send.send(native(profile)).await.unwrap();
            assert_eq!(
                timeout(DEADLINE, rx.recv()).await.unwrap().unwrap().len(),
                1
            );
            wait_running(&mut supervisor, "lifecycle-cycles").await;
            println!(
                "[{profile}/trades] cycle={cycle} reconnect={reconnect} readiness revoked and re-established"
            );
        }
        timeout(DEADLINE, supervisor.stop("lifecycle-cycles"))
            .await
            .unwrap()
            .unwrap();
        assert!(
            !supervisor
                .mark_running("lifecycle-cycles", generation)
                .unwrap(),
            "retired generation resurrected"
        );
        assert_eq!(manager.stream_count().await, 0);
    }
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
}

#[tokio::test]
async fn shared_connection_source_failure_is_isolated() {
    use marketforge_engine::live::stream::{
        registry::{StreamGraph, StreamRuntimeRegistry},
        request::SourceChannel,
    };
    let profile = "bybit_spot";
    let mut server = LocalSocket::start(profile).await;
    let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&configs, profile);
    let manager = manager(&configs, profile, &limiters, &server.url);
    let builder = builder(configs.clone(), SourceInitializationLimits::default());
    let request = profiles::requests().remove(0);
    let a = profiles::definition(request.clone(), "isolated-trades");
    let mut depth_request = request;
    depth_request.channel = SourceChannel::Depth;
    let b = profiles::definition(depth_request, "isolated-depth");
    let graph = StreamGraph::restore(vec![a.clone(), b.clone()], vec![]).unwrap();
    let mut supervisor = SourceRuntimeSupervisor::new(StreamRuntimeRegistry::new(&graph));
    let (recovery, task) = recovery(configs, profile, limiters);
    let first = builder
        .prepare(a)
        .unwrap()
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    supervisor
        .start(
            "isolated-trades",
            first,
            |_| Ok(()),
            |_| Err("isolated consumer capacity exhausted".into()),
        )
        .unwrap();
    let second = builder
        .prepare(b)
        .unwrap()
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let (tx, mut rx) = mpsc::channel(16);
    supervisor
        .start(
            "isolated-depth",
            second,
            move |events| {
                for event in events {
                    if matches!(
                        event,
                        marketforge_engine::live::integrity::DepthPublicationEvent::Snapshot { .. }
                    ) {
                        tx.try_send(()).map_err(|e| e.to_string())?;
                    }
                }
                Ok(())
            },
            |_| Ok(()),
        )
        .unwrap();
    server.ack().await;
    server.ack().await;
    let book = json!({"topic":"orderbook.200.BTCUSDT","type":"snapshot","ts":1720000000000_u64,"cts":1720000000000_u64,"data":{"s":"BTCUSDT","u":100,"seq":200,"b":[["80000","2"]],"a":[["100000","3"]]}});
    server.send.send(book.clone()).await.unwrap();
    timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
    wait_running(&mut supervisor, "isolated-depth").await;
    server.send.send(native(profile)).await.unwrap();
    timeout(DEADLINE, async {
        loop {
            supervisor.reap_finished().await.unwrap();
            if !supervisor.is_active("isolated-trades") {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        supervisor
            .registry()
            .status("isolated-trades")
            .unwrap()
            .lifecycle,
        StreamLifecycleState::Failed
    );
    assert!(supervisor.is_active("isolated-depth"));
    assert_eq!(manager.stream_count().await, 1);
    server.ack().await;
    server.send.send(book).await.unwrap();
    timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
    wait_running(&mut supervisor, "isolated-depth").await;
    timeout(DEADLINE, supervisor.stop("isolated-depth"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
}

#[tokio::test]
async fn sustained_ordered_trade_throughput_uses_bounded_handoff() {
    let profile = "bybit_spot";
    let mut server = LocalSocket::start(profile).await;
    let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&configs, profile);
    let manager = manager(&configs, profile, &limiters, &server.url);
    let definition = profiles::definition(profiles::requests().remove(0), "throughput");
    let prepared = builder(configs.clone(), SourceInitializationLimits::default())
        .prepare(definition.clone())
        .unwrap();
    let metrics = prepared.runner().instance().metrics_handle();
    let (recovery, task) = recovery(configs, profile, limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let mut supervisor = supervisor(definition);
    let (tx, mut rx) = mpsc::channel(64);
    supervisor
        .start(
            "throughput",
            executor,
            |_| Ok(()),
            move |payload| tx.try_send(published(payload)).map_err(|e| e.to_string()),
        )
        .unwrap();
    server.ack().await;
    let started = Instant::now();
    // Each acknowledged bounded batch prevents the test driver from hiding a
    // failed consumer behind an unbounded output collector.
    for batch in 0..32 {
        for offset in 0..16 {
            let mut message = native(profile);
            message["data"][0]["i"] = (1000 + batch * 16 + offset).to_string().into();
            server.send.send(message).await.unwrap();
        }
        for offset in 0..16 {
            let records = timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(
                records[0].trade_id.as_ref().unwrap(),
                &(1000 + batch * 16 + offset).to_string()
            );
        }
    }
    wait_running(&mut supervisor, "throughput").await;
    assert_eq!(metrics.messages_received_total(), 512);
    println!(
        "[{profile}/trades] sustained records=512 ordered=true publication_capacity=64 elapsed={:?}",
        started.elapsed()
    );
    timeout(DEADLINE, supervisor.stop("throughput"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, task).await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_publication_waits_for_the_acknowledged_boundary() {
    use tokio::sync::{oneshot, watch};
    let profile = "bybit_spot";
    let mut server = LocalSocket::start(profile).await;
    let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&configs, profile);
    let manager = manager(&configs, profile, &limiters, &server.url);
    let definition = profiles::definition(profiles::requests().remove(0), "publication-shutdown");
    let prepared = builder(configs.clone(), SourceInitializationLimits::default())
        .prepare(definition)
        .unwrap();
    let (recovery, recovery_task) = recovery(configs, profile, limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let (shutdown, shutdown_rx) = oneshot::channel();
    let (readiness_tx, readiness_rx) = watch::channel(SourceReadiness::AwaitingData);
    let (entered, entered_rx) = oneshot::channel();
    let mut entered = Some(entered);
    let (release, release_rx) = std::sync::mpsc::sync_channel(1);
    let task = tokio::spawn(async move {
        executor
            .run(
                shutdown_rx,
                readiness_tx,
                |_| Ok(()),
                move |payload| {
                    assert_eq!(published(payload).len(), 1);
                    entered.take().unwrap().send(()).unwrap();
                    release_rx
                        .recv_timeout(std::time::Duration::from_secs(2))
                        .map_err(|_| "bounded handoff deadline".to_owned())?;
                    Ok(())
                },
            )
            .await
    });
    server.ack().await;
    server.send.send(native(profile)).await.unwrap();
    timeout(DEADLINE, entered_rx).await.unwrap().unwrap();
    assert_ne!(
        *readiness_rx.borrow(),
        SourceReadiness::Ready,
        "unacknowledged publication became ready"
    );
    shutdown.send(()).unwrap();
    release.send(()).unwrap();
    assert_eq!(
        timeout(DEADLINE, task).await.unwrap().unwrap().unwrap(),
        SourceExecutorExit::ShutdownRequested
    );
    assert_ne!(*readiness_rx.borrow(), SourceReadiness::Ready);
    assert_eq!(manager.stream_count().await, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(DEADLINE, recovery_task).await.unwrap().unwrap();
}
