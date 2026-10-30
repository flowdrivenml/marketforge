use super::{
    harness::*,
    http_server::{MockExchange, MockResponse},
    rest_support,
    source_harness::*,
};
use marketforge_engine::live::stream::{registry::*, request::SourceChannel, router::*, source::*};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio::time::timeout;

fn native(profile: &str) -> Value {
    serde_json::from_str::<Value>(include_str!("../../support/data/source/trades.json")).unwrap()
        [profile]
        .clone()
}
async fn run_profile(profile: &str, channel: SourceChannel) {
    let body = r#"{"lastUpdateId":100,"E":1720000000000,"T":1720000000000,"bids":[["80000","2"]],"asks":[["100000","3"]]}"#;
    let (response, gate) = MockResponse::gated(body);
    let http = MockExchange::start(vec![response]);
    let mut socket = LocalSocket::start(profile).await;
    let configs = Arc::new(rest_support::load_profile(profile, http.base_url()));
    let limiters = rest_support::build_limiters(&configs, profile);
    let manager = manager(&configs, profile, &limiters, &socket.url);
    let definition = definition(profile, channel, "pipeline");
    let graph = Arc::new(StreamGraph::restore(vec![definition.clone()], vec![]).unwrap());
    let context = RouterContext {
        graph: graph.clone(),
        demand: Arc::new(Mutex::new(DemandRegistry::new())),
    };
    let router = StreamRouter::new(RouterConfig::default(), context.clone()).unwrap();
    let mut consumers = vec![];
    for i in 0..3 {
        consumers.push(
            router
                .subscribe(SubscribeRequest {
                    stream_id: "pipeline".into(),
                    owner: owner(&i.to_string()),
                })
                .await
                .unwrap(),
        );
    }
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
    let (recovery, task) = recovery(configs, profile, limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let mut supervisor = SourceRuntimeSupervisor::new(StreamRuntimeRegistry::new(&graph));
    supervisor
        .start_routed("pipeline", executor, &router)
        .unwrap();
    let subscription = socket.ack().await;
    if channel == SourceChannel::Trades {
        socket.send.send(native(profile)).await.unwrap();
    } else if profile == "binance_spot" {
        socket.send.send(json!({"e":"depthUpdate","E":1720000000000_u64,"s":"BTCUSDT","U":100,"u":101,"b":[["80000","4"]],"a":[["100000","3"]]})).await.unwrap();
        timeout(super::harness::DEADLINE, async {
            while http.request_count() == 0 || metrics.messages_received_total() == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        gate.release();
    } else if profile == "bybit_spot" {
        socket.send.send(json!({"topic":subscription["args"][0],"type":"snapshot","ts":1720000000000_u64,"cts":1720000000000_u64,"data":{"s":"BTCUSDT","u":100,"seq":200,"b":[["80000","2"]],"a":[["100000","3"]]}})).await.unwrap();
    } else {
        socket.send.send(json!({"arg":subscription["args"][0],"action":"snapshot","data":[{"ts":"1720000000000","seq":100,"pseq":0,"checksum":0,"b":[["80000","2"]],"a":[["100000","3"]]}]})).await.unwrap();
        socket.send.send(json!({"arg":subscription["args"][0],"action":"update","data":[{"ts":"1720000000001","seq":101,"pseq":100,"checksum":0,"b":[["80000","4"]],"a":[["100000","3"]]}]})).await.unwrap();
    }
    let mut first = None;
    for consumer in &mut consumers {
        let (sequence, publication) = records(receive(consumer).await);
        assert_eq!(sequence, 1);
        if let Some(first) = &first {
            assert!(Arc::ptr_eq(first, &publication));
        } else {
            first = Some(publication.clone());
        }
        match publication.as_ref() {
            RouterPublication::Trades {
                records, integrity, ..
            } => {
                assert_eq!(channel, SourceChannel::Trades);
                assert!(!records.is_empty());
                assert!(integrity.is_some());
                for trade in records.iter() {
                    assert_eq!(trade.envelope.stream_id, "pipeline");
                    assert_eq!(
                        trade.envelope.instrument_id,
                        request(profile, channel).instrument_id
                    );
                    assert_eq!(trade.price, rust_decimal::Decimal::from(80000));
                    assert!(trade.trade_id.is_some());
                    assert_eq!(trade.envelope.symbol, request(profile, channel).symbol);
                }
            }
            RouterPublication::Depth(events) => {
                assert_eq!(channel, SourceChannel::Depth);
                match &events[0] {
                    RouterDepthEvent::Snapshot {
                        levels,
                        baseline,
                        timestamp,
                        ..
                    } => {
                        assert_eq!(levels.len(), 2);
                        assert!(baseline.synchronized);
                        assert!(timestamp.latest_source.is_some());
                        assert!(
                            levels
                                .iter()
                                .any(|(_, price, _)| *price == rust_decimal::Decimal::from(80000))
                        );
                    }
                    other => panic!("expected snapshot: {other:?}"),
                }
            }
        }
    }
    wait_running(&mut supervisor, "pipeline").await;
    if profile == "bybit_spot" && channel == SourceChannel::Depth {
        let initial_generation = match first.as_ref().unwrap().as_ref() {
            RouterPublication::Depth(events) => events[0].generations().0,
            _ => unreachable!(),
        };
        socket.send.send(Value::Null).await.unwrap();
        let subscription = socket.ack().await;
        for consumer in &mut consumers {
            match receive(consumer).await {
                RouterDelivery::Publication {
                    publication,
                    synchronized,
                    ..
                } => {
                    assert!(!synchronized);
                    assert!(
                        matches!(publication.as_ref(), RouterPublication::Depth(events) if matches!(events[0], RouterDepthEvent::Invalidated {..}))
                    );
                }
                other => panic!("expected reconnect invalidation: {other:?}"),
            }
        }
        socket.send.send(json!({"topic":subscription["args"][0],"type":"snapshot","ts":1720000000001_u64,"cts":1720000000001_u64,"data":{"s":"BTCUSDT","u":200,"seq":300,"b":[["80000","5"]],"a":[["100000","3"]]}})).await.unwrap();
        for consumer in &mut consumers {
            match receive(consumer).await {
                RouterDelivery::Publication {
                    publication,
                    synchronized,
                    ..
                } => {
                    assert!(synchronized);
                    match publication.as_ref() {
                        RouterPublication::Depth(events) => {
                            assert!(events[0].generations().0 > initial_generation)
                        }
                        _ => panic!(),
                    }
                }
                other => panic!("expected recovery snapshot: {other:?}"),
            }
        }
        wait_running(&mut supervisor, "pipeline").await;
    }
    assert_eq!(context.demand.lock().unwrap().owner_count("pipeline"), 3);
    consumers.pop().unwrap().close().await.unwrap();
    assert_eq!(context.demand.lock().unwrap().owner_count("pipeline"), 2);
    timeout(super::harness::DEADLINE, supervisor.stop("pipeline"))
        .await
        .unwrap()
        .unwrap();
    for consumer in &mut consumers {
        assert!(matches!(
            receive(consumer).await,
            RouterDelivery::ContinuityLost(ContinuityFailure::ProducerRevoked)
        ));
    }
    assert_eq!(manager.stream_count().await, 0);
    assert_eq!(context.demand.lock().unwrap().total_owners(), 0);
    router.shutdown().await.unwrap();
    assert_eq!(router.status().ingress_bytes, 0);
    assert_eq!(router.status().consumer_bytes, 0);
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(super::harness::DEADLINE, task)
        .await
        .unwrap()
        .unwrap();
    println!(
        "[{profile}/{channel:?}] production WS -> Source normalization/integrity -> Router -> 3 consumers: exact shared payload; demand cleanup; shutdown PASS"
    );
}
#[tokio::test]
async fn binance_spot_trades_pipeline() {
    run_profile("binance_spot", SourceChannel::Trades).await;
}
#[tokio::test]
async fn gateio_linear_trades_pipeline() {
    run_profile("gateio_linear", SourceChannel::Trades).await;
}
#[tokio::test]
async fn binance_spot_depth_pipeline() {
    run_profile("binance_spot", SourceChannel::Depth).await;
}
#[tokio::test]
async fn bybit_spot_depth_pipeline() {
    run_profile("bybit_spot", SourceChannel::Depth).await;
}
#[tokio::test]
async fn bitget_linear_depth_pipeline() {
    run_profile("bitget_linear", SourceChannel::Depth).await;
}

#[tokio::test]
async fn source_rejected_router_admission_cannot_establish_readiness() {
    let profile = "binance_spot";
    let mut socket = LocalSocket::start(profile).await;
    let configs = Arc::new(rest_support::load_profile(profile, "http://127.0.0.1:1"));
    let limiters = rest_support::build_limiters(&configs, profile);
    let manager = manager(&configs, profile, &limiters, &socket.url);
    let def = definition(profile, SourceChannel::Trades, "rejected");
    let graph = Arc::new(StreamGraph::restore(vec![def.clone()], vec![]).unwrap());
    let mut config = RouterConfig::default();
    config.ingress.bytes = 1;
    let router = StreamRouter::new(
        config,
        RouterContext {
            graph: graph.clone(),
            demand: Arc::new(Mutex::new(DemandRegistry::new())),
        },
    )
    .unwrap();
    let prepared = builder(configs.clone(), SourceInitializationLimits::default())
        .prepare(def)
        .unwrap();
    let (recovery, task) = recovery(configs, profile, limiters);
    let executor = prepared
        .attach(manager.clone(), recovery.clone())
        .await
        .unwrap();
    let mut supervisor = SourceRuntimeSupervisor::new(StreamRuntimeRegistry::new(&graph));
    supervisor
        .start_routed("rejected", executor, &router)
        .unwrap();
    socket.ack().await;
    socket.send.send(native(profile)).await.unwrap();
    timeout(super::harness::DEADLINE, async {
        while supervisor.is_active("rejected") {
            supervisor.reap_finished().await.unwrap();
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let status = supervisor.registry().status("rejected").unwrap();
    assert_eq!(status.lifecycle, StreamLifecycleState::Failed);
    assert!(status.last_error.as_ref().unwrap().contains("capacity"));
    assert_eq!(router.status().accepted, 0);
    assert_eq!(manager.stream_count().await, 0);
    router.shutdown().await.unwrap();
    manager.shutdown().await.unwrap();
    recovery.shutdown().await.unwrap();
    timeout(super::harness::DEADLINE, task)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn failed_router_admission_leaves_source_trade_identity_uncommitted() {
    use marketforge_engine::live::{
        config::FormatConfigStore,
        infrastructure::normalization::{NormalizerFactory, NormalizerLimits},
        integrity::{
            CreateTradeIntegrity, IntegrityFacadeFactory, TradeIntegrityCompiler,
            TradeSourceBinding, TradeSourceEpoch,
        },
    };
    use marketforge_engine::shared::domain::canonical::Exchange;
    let mut config = RouterConfig::default();
    config.ingress.publications = 1;
    let rig = Rig::new(SourceChannel::Trades, config);
    let mut sub = rig.subscribe("acknowledgment").await;
    let configs = Arc::new(rest_support::load_profile(
        "binance_spot",
        "http://127.0.0.1:1",
    ));
    let formats = Arc::new(
        FormatConfigStore::load_dir(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../data/.config/live/formats"),
        )
        .unwrap(),
    );
    let factory = SourceStreamFactory::new(Arc::new(
        NormalizerFactory::new(configs, formats.clone(), NormalizerLimits::default()).unwrap(),
    ));
    let request = request("binance_spot", SourceChannel::Trades);
    let instance = factory.create("test", request.clone()).unwrap();
    let epoch = TradeSourceEpoch {
        stream_generation: 1,
        connection_generation: 1,
        subscription_generation: 1,
    };
    let integrity = IntegrityFacadeFactory::trades(CreateTradeIntegrity {
        binding: TradeSourceBinding::new(
            "test",
            Exchange::Binance,
            "spot",
            request.instrument_id,
            &request.symbol,
            epoch,
        )
        .unwrap(),
        plan: TradeIntegrityCompiler::compile(formats.trade("binance_spot").unwrap()).unwrap(),
        identity_limits: SourceInitializationLimits::default().trade_identities,
    })
    .unwrap();
    let mut runner =
        SourceStreamRunner::new(instance, SourceProcessor::new(integrity).unwrap()).unwrap();
    runner
        .connected(SourceProcessingEpoch::Trades(epoch), None, |_| Ok(()))
        .unwrap();
    let native = native("binance_spot");
    rig.producer.try_publish(publication(0)).unwrap();
    let failed = runner.data(
        &native,
        native.to_string().len(),
        |_| Ok(()),
        |payload| rig.producer.publish_source_trades(payload),
    );
    assert!(failed.is_err());
    assert_ne!(runner.readiness(), SourceReadiness::Ready);
    receive(&mut sub).await;
    runner
        .data(
            &native,
            native.to_string().len(),
            |_| Ok(()),
            |payload| rig.producer.publish_source_trades(payload),
        )
        .unwrap();
    let (_, published) = records(receive(&mut sub).await);
    assert!(
        matches!(published.as_ref(),RouterPublication::Trades {records,..} if records.len()==1)
    );
    assert_eq!(runner.readiness(), SourceReadiness::Ready);
    // Only now is this native identity a duplicate.
    runner
        .data(
            &native,
            native.to_string().len(),
            |_| Ok(()),
            |payload| rig.producer.publish_source_trades(payload),
        )
        .unwrap();
    assert_eq!(rig.router.status().accepted, 2);
}
