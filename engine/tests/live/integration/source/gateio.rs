use serde_json::{Value, json};

pub fn snapshot_body(profile: &str) -> String {
    if profile == "gateio_spot" {
        json!({"id":100,"bids":[["80000","2"]],"asks":[["100000","3"]]}).to_string()
    } else {
        json!({"id":100,"bids":[{"p":"80000","s":2}],"asks":[{"p":"100000","s":3}]}).to_string()
    }
}
pub fn depth_update(profile: &str, symbol: &str, first: u64, last: u64) -> Value {
    let futures = profile != "gateio_spot";
    let (b, a) = if futures {
        (
            json!([{"p":"80000","s":4 + last.saturating_sub(101)}]),
            json!([{"p":"100000","s":3}]),
        )
    } else {
        (
            json!([["80000", (4 + last.saturating_sub(101)).to_string()]]),
            json!([["100000", "3"]]),
        )
    };
    json!({"channel":if futures {"futures.order_book_update"}else{"spot.order_book_update"},"event":"update","time":1720000000,"time_ms":1720000000000_i64,
        "result":{"s":symbol,"t":1720000000000_i64,"U":first,"u":last,"b":b,"a":a}})
}

use super::{
    harness::*,
    pipeline::{native, published},
    profiles, rest_support,
};
use marketforge_engine::live::stream::{
    registry::{StreamGraph, StreamLifecycleState, StreamRuntimeRegistry},
    request::CreateSourceStreamRequest,
    source::*,
};
use std::sync::Arc;
use tokio::{sync::mpsc, time::timeout};

fn trade(profile: &str, symbol: &str, id: u64) -> Value {
    let mut frame = native(profile);
    let record = if profile == "gateio_spot" {
        &mut frame["result"]
    } else {
        &mut frame["result"][0]
    };
    record["id"] = id.into();
    if profile == "gateio_spot" {
        record["id_market"] = id.into();
        record["range"] = format!("{id}-{id}").into();
    }
    record[if profile == "gateio_spot" {
        "currency_pair"
    } else {
        "contract"
    }] = symbol.into();
    frame
}

#[tokio::test]
async fn shared_instruments_mixed_batches_and_independent_shutdown() {
    for profile in ["gateio_spot", "gateio_linear"] {
        let btc = profiles::requests()
            .into_iter()
            .find(|r| format!("{}_{}", r.exchange.as_str(), r.market_category.as_str()) == profile)
            .unwrap();
        let secondary: Vec<CreateSourceStreamRequest> = serde_json::from_str(include_str!(
            "../../support/data/source/gateio_secondary.json"
        ))
        .unwrap();
        let eth = secondary
            .into_iter()
            .find(|r| r.market_category == btc.market_category && r.symbol == "ETH_USDT")
            .unwrap();
        let mut server = LocalSocket::start(profile).await;
        let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
        let limiters = rest_support::build_limiters(&configs, profile);
        let manager = manager(&configs, profile, &limiters, &server.url);
        let builder = builder(configs.clone(), SourceInitializationLimits::default());
        let definitions = vec![
            profiles::definition(btc.clone(), "gate-btc"),
            profiles::definition(eth.clone(), "gate-eth"),
        ];
        let graph = StreamGraph::restore(definitions.clone(), vec![]).unwrap();
        let mut supervisor = SourceRuntimeSupervisor::new(StreamRuntimeRegistry::new(&graph));
        let (recovery, task) = recovery(configs, profile, limiters);
        let (tx, mut rx) = mpsc::channel(16);
        for definition in definitions {
            let id = definition.stream_id.clone();
            let executor = builder
                .prepare(definition)
                .unwrap()
                .attach(manager.clone(), recovery.clone())
                .await
                .unwrap();
            let tx = tx.clone();
            supervisor
                .start(
                    &id,
                    executor,
                    |_| Ok(()),
                    move |payload| tx.try_send(published(payload)).map_err(|e| e.to_string()),
                )
                .unwrap();
            // Demand updates may replay existing subscriptions. Observe this
            // instrument's ACK rather than consuming an unrelated queued ACK.
            loop {
                let ack = server.ack().await;
                println!("[{profile}] initial ACK {ack}");
                if ack["payload"][0]
                    == if id == "gate-btc" {
                        btc.symbol.as_str()
                    } else {
                        eth.symbol.as_str()
                    }
                {
                    break;
                }
            }
        }
        assert_eq!(manager.stream_count().await, 2);
        assert_eq!(manager.slot_count().await, 1);
        let mut message = trade(profile, &btc.symbol, 100);
        if profile == "gateio_linear" {
            message["result"]
                .as_array_mut()
                .unwrap()
                .push(trade(profile, &eth.symbol, 200)["result"][0].clone());
            message["result"]
                .as_array_mut()
                .unwrap()
                .push(trade(profile, &btc.symbol, 101)["result"][0].clone());
            server.send.send(message).await.unwrap();
        } else {
            server.send.send(message).await.unwrap();
            server
                .send
                .send(trade(profile, &eth.symbol, 200))
                .await
                .unwrap();
        }
        let mut seen = std::collections::BTreeMap::new();
        for _ in 0..2 {
            let records = timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
            let expected = if records[0].envelope.stream_id == "gate-btc" {
                &btc
            } else {
                &eth
            };
            for record in &records {
                assert_eq!(record.envelope.instrument_id, expected.instrument_id);
                assert_eq!(record.envelope.symbol, expected.symbol);
                assert_eq!(record.price, rust_decimal::Decimal::from(80000));
                if profile == "gateio_linear" {
                    assert_eq!(
                        record.quantity_contracts,
                        Some(rust_decimal::Decimal::from(8414))
                    );
                    assert_eq!(
                        record.quantity_base,
                        Some(
                            rust_decimal::Decimal::from(8414)
                                * expected
                                    .contract_value
                                    .as_ref()
                                    .unwrap()
                                    .parse::<rust_decimal::Decimal>()
                                    .unwrap()
                        )
                    );
                }
            }
            seen.insert(
                records[0].envelope.stream_id.clone(),
                records
                    .iter()
                    .map(|r| r.trade_id.clone().unwrap())
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(seen["gate-eth"], ["200"]);
        assert_eq!(
            seen["gate-btc"],
            if profile == "gateio_linear" {
                vec!["100", "101"]
            } else {
                vec!["100"]
            }
        );
        wait_running(&mut supervisor, "gate-btc").await;
        wait_running(&mut supervisor, "gate-eth").await;
        timeout(DEADLINE, supervisor.stop("gate-btc"))
            .await
            .unwrap()
            .unwrap();
        println!("[{profile}] survivor ACK {}", server.ack().await);
        assert_eq!(manager.stream_count().await, 1);
        server
            .send
            .send(trade(profile, &eth.symbol, 201))
            .await
            .unwrap();
        let remaining = timeout(DEADLINE, rx.recv()).await;
        supervisor.reap_finished().await.unwrap();
        assert!(
            remaining.is_ok(),
            "{profile}: survivor status={:?}",
            supervisor.registry().status("gate-eth")
        );
        let remaining = remaining.unwrap().unwrap();
        assert_eq!(remaining[0].envelope.stream_id, "gate-eth");
        assert_eq!(remaining[0].trade_id.as_deref(), Some("201"));
        wait_running(&mut supervisor, "gate-eth").await;
        assert_eq!(
            supervisor.registry().status("gate-btc").unwrap().lifecycle,
            StreamLifecycleState::Stopped
        );
        supervisor.stop("gate-eth").await.unwrap();
        assert_eq!(manager.stream_count().await, 0);
        manager.shutdown().await.unwrap();
        recovery.shutdown().await.unwrap();
        timeout(DEADLINE, task).await.unwrap().unwrap();
        println!(
            "[{profile}/shared] one connection, exact identity/economics/order; independent shutdown and surviving subscription PASS"
        );
    }
}

#[tokio::test]
async fn stale_before_ack_and_reconnection_all_gate_profiles() {
    for profile in ["gateio_spot", "gateio_linear", "gateio_inverse"] {
        let request = profiles::requests()
            .into_iter()
            .find(|r| format!("{}_{}", r.exchange.as_str(), r.market_category.as_str()) == profile)
            .unwrap();
        let mut server =
            LocalSocket::start_scripted(profile, Some(trade(profile, &request.symbol, 900))).await;
        let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
        let limiters = rest_support::build_limiters(&configs, profile);
        let manager = manager(&configs, profile, &limiters, &server.url);
        let definition = profiles::definition(request.clone(), "gate-fenced");
        let prepared = builder(configs.clone(), SourceInitializationLimits::default())
            .prepare(definition.clone())
            .unwrap();
        let (recovery, task) = recovery(configs, profile, limiters);
        let executor = prepared
            .attach(manager.clone(), recovery.clone())
            .await
            .unwrap();
        let mut supervisor = supervisor(definition);
        let (tx, mut rx) = mpsc::channel(8);
        supervisor
            .start(
                "gate-fenced",
                executor,
                |_| Ok(()),
                move |payload| tx.try_send(published(payload)).map_err(|e| e.to_string()),
            )
            .unwrap();
        server.ack().await;
        assert!(rx.try_recv().is_err(), "retired pre-ACK frame escaped");
        for id in [100, 101, 102] {
            server
                .send
                .send(trade(profile, &request.symbol, id))
                .await
                .unwrap();
            let records = timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
            assert_eq!(records.len(), 1);
            assert_eq!(
                records[0].trade_id.as_deref(),
                Some(id.to_string().as_str())
            );
            assert_eq!(records[0].envelope.instrument_id, request.instrument_id);
            wait_running(&mut supervisor, "gate-fenced").await;
            if id != 102 {
                server.send.send(Value::Null).await.unwrap();
                server.ack().await;
                timeout(DEADLINE, async {
                    loop {
                        supervisor.sync_readiness().unwrap();
                        if supervisor
                            .registry()
                            .status("gate-fenced")
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
            }
        }
        assert!(rx.try_recv().is_err());
        supervisor.stop("gate-fenced").await.unwrap();
        assert_eq!(manager.stream_count().await, 0);
        manager.shutdown().await.unwrap();
        recovery.shutdown().await.unwrap();
        timeout(DEADLINE, task).await.unwrap().unwrap();
        println!(
            "[{profile}/fencing] retired pre-ACK data rejected; two reconnects revoke/re-establish readiness PASS"
        );
    }
}

#[tokio::test]
async fn malformed_and_unknown_payloads_fail_closed_without_partial_publication() {
    for profile in ["gateio_spot", "gateio_linear", "gateio_inverse"] {
        for fault in ["unknown", "missing-instrument", "wrong-shape"] {
            let request = profiles::requests()
                .into_iter()
                .find(|r| {
                    format!("{}_{}", r.exchange.as_str(), r.market_category.as_str()) == profile
                })
                .unwrap();
            let mut server = LocalSocket::start(profile).await;
            let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
            let limiters = rest_support::build_limiters(&configs, profile);
            let manager = manager(&configs, profile, &limiters, &server.url);
            let definition = profiles::definition(request.clone(), "gate-reject");
            let prepared = builder(configs.clone(), SourceInitializationLimits::default())
                .prepare(definition.clone())
                .unwrap();
            let (recovery, task) = recovery(configs, profile, limiters);
            let executor = prepared
                .attach(manager.clone(), recovery.clone())
                .await
                .unwrap();
            let mut supervisor = supervisor(definition);
            let (tx, mut rx) = mpsc::channel(8);
            supervisor
                .start(
                    "gate-reject",
                    executor,
                    |_| Ok(()),
                    move |payload| tx.try_send(published(payload)).map_err(|e| e.to_string()),
                )
                .unwrap();
            server.ack().await;
            server
                .send
                .send(trade(profile, &request.symbol, 100))
                .await
                .unwrap();
            assert_eq!(
                timeout(DEADLINE, rx.recv()).await.unwrap().unwrap().len(),
                1
            );
            wait_running(&mut supervisor, "gate-reject").await;
            let mut malformed = trade(profile, &request.symbol, 101);
            if fault == "wrong-shape" {
                malformed["result"] = json!("invalid");
            } else {
                let mut bad = if profile == "gateio_spot" {
                    malformed["result"].clone()
                } else {
                    malformed["result"][0].clone()
                };
                let field = if profile == "gateio_spot" {
                    "currency_pair"
                } else {
                    "contract"
                };
                if fault == "unknown" {
                    bad[field] = "UNSUBSCRIBED_USDT".into();
                } else {
                    bad.as_object_mut().unwrap().remove(field);
                }
                if profile == "gateio_spot" {
                    malformed["result"] = bad;
                } else {
                    malformed["result"].as_array_mut().unwrap().push(bad);
                }
            }
            server.send.send(malformed).await.unwrap();
            timeout(DEADLINE, async {
                loop {
                    supervisor.reap_finished().await.unwrap();
                    if !supervisor.is_active("gate-reject") {
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
                    .status("gate-reject")
                    .unwrap()
                    .lifecycle,
                StreamLifecycleState::Failed
            );
            assert!(
                rx.try_recv().is_err(),
                "valid prefix of invalid native batch escaped"
            );
            assert_eq!(manager.stream_count().await, 0);
            manager.shutdown().await.unwrap();
            recovery.shutdown().await.unwrap();
            timeout(DEADLINE, task).await.unwrap().unwrap();
            println!(
                "[{profile}/{fault}] entire frame rejected; no partial publication; readiness revoked; resources released PASS"
            );
        }
    }
}

#[tokio::test]
async fn shared_depth_instruments_keep_snapshot_and_delta_ownership() {
    use super::http_server::{MockExchange, MockResponse};
    use marketforge_engine::live::{
        integrity::DepthPublicationEvent, stream::request::SourceChannel,
    };
    for profile in ["gateio_spot", "gateio_linear"] {
        let mut btc = profiles::requests()
            .into_iter()
            .find(|r| format!("{}_{}", r.exchange.as_str(), r.market_category.as_str()) == profile)
            .unwrap();
        let secondary: Vec<CreateSourceStreamRequest> = serde_json::from_str(include_str!(
            "../../support/data/source/gateio_secondary.json"
        ))
        .unwrap();
        let mut eth = secondary
            .into_iter()
            .find(|r| r.market_category == btc.market_category && r.symbol == "ETH_USDT")
            .unwrap();
        btc.channel = SourceChannel::Depth;
        eth.channel = SourceChannel::Depth;
        let (response, gate) = MockResponse::gated(snapshot_body(profile));
        let (second_response, second_gate) = MockResponse::gated(snapshot_body(profile));
        let http = MockExchange::start(vec![
            response,
            second_response,
            MockResponse::ok(snapshot_body(profile)),
        ]);
        // Extending demand re-establishes BTC acquisition as well as ETH.
        // Gate responses until both native deltas have reached their owners.
        let gates = [gate, second_gate];
        let mut server = LocalSocket::start(profile).await;
        let configs = Arc::new(rest_support::load_profile(profile, http.base_url()));
        let limiters = rest_support::build_limiters(&configs, profile);
        let manager = manager(&configs, profile, &limiters, &server.url);
        let builder = builder(configs.clone(), SourceInitializationLimits::default());
        let definitions = vec![
            profiles::definition(btc.clone(), "book-btc"),
            profiles::definition(eth.clone(), "book-eth"),
        ];
        let graph = StreamGraph::restore(definitions.clone(), vec![]).unwrap();
        let mut supervisor = SourceRuntimeSupervisor::new(StreamRuntimeRegistry::new(&graph));
        let (recovery, task) = recovery(configs, profile, limiters);
        let (tx, mut rx) = mpsc::channel(32);
        let mut metrics = vec![];
        for ((definition, request), gate) in definitions.into_iter().zip([&btc, &eth]).zip(gates) {
            let id = definition.stream_id.clone();
            let prepared = builder.prepare(definition).unwrap();
            metrics.push(prepared.runner().instance().metrics_handle());
            let executor = prepared
                .attach(manager.clone(), recovery.clone())
                .await
                .unwrap();
            let tx = tx.clone();
            let owner = id.clone();
            supervisor
                .start(
                    &id,
                    executor,
                    move |events| {
                        for event in events {
                            match event {
                                DepthPublicationEvent::Snapshot { levels, .. } => {
                                    assert_eq!(levels.len(), 2);
                                    tx.try_send((owner.clone(), "snapshot"))
                                        .map_err(|e| e.to_string())?;
                                }
                                DepthPublicationEvent::Delta { changes, .. } => {
                                    assert!(!changes.is_empty());
                                    tx.try_send((owner.clone(), "delta"))
                                        .map_err(|e| e.to_string())?;
                                }
                                DepthPublicationEvent::Invalidated { .. } => {
                                    tx.try_send((owner.clone(), "invalidated"))
                                        .map_err(|e| e.to_string())?;
                                }
                            }
                        }
                        Ok(())
                    },
                    |_| panic!("depth source published trades"),
                )
                .unwrap();
            loop {
                let ack = server.ack().await;
                if ack["payload"][0] == request.symbol {
                    break;
                }
            }
            if id == "book-eth" {
                server
                    .send
                    .send(depth_update(profile, &btc.symbol, 101, 101))
                    .await
                    .unwrap();
            }
            server
                .send
                .send(depth_update(profile, &request.symbol, 101, 101))
                .await
                .unwrap();
            timeout(DEADLINE, async {
                while metrics.last().unwrap().messages_received_total() != 1
                    || (id == "book-eth" && metrics[0].messages_received_total() != 2)
                    || http.request_count() < metrics.len()
                {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            gate.release();
            let mut snapshots = std::collections::BTreeSet::new();
            let expected = if id == "book-eth" { 2 } else { 1 };
            while snapshots.len() < expected {
                let (owner, event) = timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
                if event == "snapshot" {
                    assert!(snapshots.insert(owner));
                }
            }
            assert!(snapshots.contains(&id));
            wait_running(&mut supervisor, &id).await;
        }
        wait_running(&mut supervisor, "book-btc").await;
        assert_eq!(manager.slot_count().await, 1);
        server
            .send
            .send(depth_update(profile, &eth.symbol, 102, 102))
            .await
            .unwrap();
        loop {
            let (owner, event) = timeout(DEADLINE, rx.recv()).await.unwrap().unwrap();
            if event == "delta" && metrics[1].messages_received_total() == 2 {
                assert_eq!(owner, "book-eth");
                break;
            }
        }
        assert_eq!(metrics[0].messages_received_total(), 2);
        assert_eq!(http.request_count(), 3);
        supervisor.stop("book-btc").await.unwrap();
        supervisor.stop("book-eth").await.unwrap();
        assert_eq!(manager.stream_count().await, 0);
        manager.shutdown().await.unwrap();
        recovery.shutdown().await.unwrap();
        timeout(DEADLINE, task).await.unwrap().unwrap();
        println!(
            "[{profile}/shared-depth] per-instrument REST snapshots and native deltas; one connection; isolated ownership PASS"
        );
    }
}
