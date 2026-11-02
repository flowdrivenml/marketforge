#![cfg(unix)]
#[path = "live/integration/derived/harness.rs"]
mod harness;
use std::os::unix::fs::PermissionsExt;
fn private_dir() -> tempfile::TempDir {
    let d = tempfile::tempdir().unwrap();
    std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    d
}
use harness::*;
use marketforge_engine::{
    client::streaming::{ClientOptions, MarketForgeStreamClient},
    live::{
        integrity::*, stream::source::SourceTradePublication, transport::unix::UnixStreamingServer,
    },
    stream_protocol::{self as p, StreamMessage, market::NativeEvent},
};
fn config(path: &std::path::Path) -> general::PublicationConfig {
    serde_json::from_value(serde_json::json!({"socket_path":path,"max_connections":8,"subscriber_queue_bytes":1048576})).unwrap()
}
async fn next(sub: &mut marketforge_engine::client::streaming::Subscription) -> StreamMessage {
    bounded(sub.recv()).await.unwrap().unwrap()
}
fn metadata() -> RouterTradeIntegrity {
    RouterTradeIntegrity {
        source_generation: Some(7),
        received_records: 2,
        duplicate_records: 1,
        identity_history_truncated: true,
        coverage_history_unverified: true,
        observations: vec![TradeIntegrityObservation {
            index: 0,
            kind: TradeIntegrityObservationKind::NumericIdDiscontinuity,
            previous: 10,
            current: 20,
        }]
        .into(),
        baseline: TradeIntegrityBaseline {
            numeric_trade_id: Some(20),
            cross_sequence: None,
        },
    }
}
fn source_def(id: &str, base: &str, quote: &str, key: i64) -> PersistedStreamDefinition {
    let mut d = source::definition("binance_spot", SourceChannel::Trades, id);
    d.recording_stream_key = Some(key);
    let StreamDefinition::Source(r) = &mut d.definition else {
        panic!()
    };
    r.base_asset = Some(base.into());
    r.quote_asset = Some(quote.into());
    d
}

fn processor(
    id: &str,
    action: TradeCoverageAction,
) -> marketforge_engine::live::stream::source::SourceProcessor {
    let t = source::trade(1);
    let epoch = TradeSourceEpoch {
        stream_generation: 7,
        connection_generation: 3,
        subscription_generation: 2,
    };
    marketforge_engine::live::stream::source::SourceProcessor::new(
        IntegrityFacadeFactory::trades(CreateTradeIntegrity {
            binding: TradeSourceBinding::new(
                id,
                t.envelope.exchange,
                "spot",
                t.envelope.instrument_id,
                "BTCUSDT",
                epoch,
            )
            .unwrap(),
            plan: CompiledTradeIntegrityPlan {
                format_id: "binance_spot".into(),
                identity_kind: TradeIdentityKind::Numeric,
                sequence_kind: TradeSequenceKind::None,
                detect_duplicates: true,
                detect_id_regression: true,
                detect_id_gaps: true,
                detect_cross_sequence_regression: false,
                allow_equal_cross_sequence: false,
                coverage_action: action,
                supports_individual_trades: true,
            },
            identity_limits: TradeIdentityLimits {
                max_ids: 3,
                max_id_bytes: 128,
                max_batch_records: 8,
            },
        })
        .unwrap(),
    )
    .unwrap()
}
fn source_input(
    processor: &mut marketforge_engine::live::stream::source::SourceProcessor,
    producer: &ProducerHandle,
    id: &str,
    items: &[(u64, i64, i64)],
) {
    use marketforge_engine::live::{
        infrastructure::normalization::*, stream::source::SourceProcessingEpoch,
    };
    let records = items
        .iter()
        .map(|(seq, time, price)| {
            let mut t = source::trade(*seq);
            t.envelope.stream_id = id.into();
            t.envelope.event_timestamp_ns = *time;
            t.price = (*price).into();
            t.quantity_quote = Some((*price * 2).into());
            t
        })
        .collect();
    processor
        .process_normalized(
            NormalizedMessage::Trades(
                NormalizedTrades::new(
                    id,
                    NormalizationSource::WebSocket,
                    TradeRecordSemantics::Individual,
                    records,
                )
                .unwrap(),
            ),
            SourceProcessingEpoch::Trades(TradeSourceEpoch {
                stream_generation: 7,
                connection_generation: 3,
                subscription_generation: 2,
            }),
            1024,
            |_| panic!(),
            |o| producer.publish_source_trades(SourceTradePublication::Integrity(o)),
        )
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_merge_convert_real_socket_integrity_and_reference_witness() {
    let rig = Rig::definitions(
        vec![
            source_def("a", "ETH", "BTC", 1),
            source_def("b", "BTC", "USDT", 2),
            source_def("c", "ETH", "BTC", 3),
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "c"],
                &[],
                Some(MergeOrdering::ArrivalOrder),
            ),
            derived("v", DerivedOperation::Convert, &["m"], &["b"], None),
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let m = bounded(handle.start_derived("m")).await.unwrap();
    let v = bounded(handle.start_derived("v")).await.unwrap();
    let mut merged = subscribe(&rig.router, "m").await;
    let mut a = processor("a", TradeCoverageAction::ReportUnverifiedCoverage);
    source_input(&mut a, &rig.producers[0], "a", &[(1, 100, 3), (2, 100, 3)]);
    let dir = private_dir();
    let path = dir.path().join("stream.sock");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let (page, description) = tokio::join!(
        client.list_streams(p::PageRequest {
            after: None,
            limit: 2
        }),
        client.describe_stream("v")
    );
    assert_eq!(page.unwrap().streams.len(), 2);
    let description = description.unwrap();
    assert_eq!(description.semantics, "derived_market_events");
    assert_eq!(description.reference_inputs, vec!["b"]);
    let source = client.describe_stream("a").await.unwrap().source.unwrap();
    assert_eq!(source.exchange, "binance");
    assert_eq!(source.symbol, "BTCUSDT");
    assert_eq!(source.quote_asset.as_deref(), Some("BTC"));
    let mut sub = client.subscribe("v").await.unwrap();
    let StreamMessage::IntegritySnapshot(snapshot) = next(&mut sub).await else {
        panic!()
    };
    assert_eq!(snapshot.sources.len(), 3);
    assert!(
        snapshot
            .sources
            .iter()
            .any(|e| e.id.source == "a" && e.id.revision.is_some())
    );
    let mut c = processor("c", TradeCoverageAction::RequestRecovery);
    source_input(&mut c, &rig.producers[2], "c", &[(10, 100, 3)]);
    source_input(&mut c, &rig.producers[2], "c", &[(20, 200, 3)]);
    bounded(async {
        loop {
            if let StreamMessage::IntegrityUpdate(update) = next(&mut sub).await {
                if update.evidence.coverage == "coverage_review_required" {
                    assert_eq!(update.evidence.observations[0].observed, 20);
                    assert_eq!(update.evidence.observations[0].missing_records, None);
                    break;
                }
            }
        }
    })
    .await;
    bounded(async {loop{if let Some(RouterDelivery::Publication{publication,..})=merged.recv().await {if matches!(publication.as_ref(),RouterPublication::Derived(events) if events.iter().any(|e|e.origin.stream_id=="c")){break;}}}}).await;
    drop(merged);
    source_input(
        &mut a,
        &rig.producers[0],
        "a",
        &[(10, 100, 3), (10, 100, 3), (20, 100, 3)],
    );
    source_input(&mut a, &rig.producers[0], "a", &[(30, 300, 4)]);
    let mut b = processor("b", TradeCoverageAction::ReportUnverifiedCoverage);
    source_input(&mut b, &rig.producers[1], "b", &[(1, 90, 10)]);
    source_input(&mut b, &rig.producers[1], "b", &[(2, 200, 30)]);
    let publication = bounded(async {
        loop {
            if let StreamMessage::Publication(p) = next(&mut sub).await {
                if matches!(&p.records[0].native, NativeEvent::Trade(t) if t.stream_id=="a" && t.trade_id.as_deref()==Some("10"))
                {
                    break p;
                }
            }
        }
    })
    .await;
    assert_eq!(publication.stream_id, "v");
    let record = &publication.records[0];
    assert_eq!(record.processing_input_position, Some(0));
    assert!(record.processing_admission_ordinal.is_some_and(|n| n > 0));
    assert_eq!(record.baseline_barrier, Some(false));
    assert_eq!(record.origin.as_ref().unwrap().stream_id, "a");
    let NativeEvent::Trade(t) = &record.native else {
        panic!()
    };
    assert_eq!(t.price.coefficient, "3");
    let integrity = record.integrity.as_ref().unwrap();
    assert_eq!(integrity.duplicate_records, Some(1));
    assert_eq!(integrity.source_generation, Some(7));
    assert!(integrity.identity_history_truncated);
    assert!(integrity.recovery_complete.is_none());
    let valuation = record.valuation.as_ref().unwrap();
    assert_eq!(valuation.numerator, "10");
    assert_eq!(valuation.denominator, "1");
    assert_eq!(valuation.references[0].timestamp_ns, 90);
    assert_eq!(valuation.reference_integrity[0].id.source, "b");
    assert_eq!(
        valuation.values[0].price.coefficient,
        "300000000000000000000000000000"
    );
    sub.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    assert!(!path.exists());
    bounded(handle.stop(&v)).await.unwrap();
    handle.take_report("v", v.generation).unwrap().discard();
    bounded(handle.stop(&m)).await.unwrap();
    handle.take_report("m", m.generation).unwrap().discard();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn multiplex_cancel_receive_unsubscribe_and_disconnect() {
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("s");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    assert!(
        UnixStreamingServer::bind(rig.router.clone(), config(&path))
            .await
            .is_err()
    );
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut a = client.subscribe("a").await.unwrap();
    let mut b = client.subscribe("b").await.unwrap();
    assert!(matches!(
        next(&mut a).await,
        StreamMessage::IntegritySnapshot(_)
    ));
    next(&mut b).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1), a.recv())
            .await
            .is_err()
    );
    publish(&rig.producers[0], publication("a", u64::MAX, 100)).await;
    publish(&rig.producers[1], publication("b", 2, 200)).await;
    for sub in [&mut a, &mut b] {
        assert!(matches!(next(sub).await, StreamMessage::IntegrityUpdate(_)));
        assert!(matches!(next(sub).await, StreamMessage::Publication(_)));
    }
    a.close().await.unwrap();
    assert!(a.recv().await.unwrap().is_none());
    client.ping().await.unwrap();
    assert_eq!(rig.router.status().subscriptions, 1);
    server.shutdown().await.unwrap();
    assert!(bounded(b.recv()).await.is_err());
    client.close().await.unwrap();
    assert_eq!(rig.router.status().subscriptions, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn depth_atomic_operations_snapshot_omissions_and_truthful_late_tail() {
    use marketforge_engine::shared::domain::{book::BookLevel, canonical::BookSide};
    let rig = Rig::definitions(
        vec![source::definition(
            "binance_spot",
            SourceChannel::Depth,
            "d",
        )],
        RouterConfig::default(),
    );
    let dir = private_dir();
    let path = dir.path().join("depth");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("d").await.unwrap();
    next(&mut sub).await;
    let baseline = DepthSequenceBaseline {
        last_sequence: 100,
        cross_sequence: Some(u64::MAX),
        synchronized: true,
    };
    let timestamp = DepthAcceptedTimestamp {
        latest_source: None,
        composition: DepthTimestampComposition::RestReconciliation,
    };
    let level = BookLevel {
        quantity_base: None,
        quantity_quote: Some(5.into()),
        quantity_contracts: None,
        order_count: Some(u64::MAX),
    };
    let remove = BookLevel {
        quantity_base: Some(0.into()),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: None,
    };
    let levels = vec![(BookSide::Bid, 10.into(), level.clone()); 20000].into();
    publish(
        &rig.producers[0],
        RouterPublication::Depth(
            vec![
                RouterDepthEvent::Snapshot {
                    stream_generation: 7,
                    book_generation: 9,
                    baseline,
                    levels,
                    timestamp,
                },
                RouterDepthEvent::Delta {
                    stream_generation: 7,
                    book_generation: 9,
                    baseline: DepthSequenceBaseline {
                        last_sequence: 101,
                        ..baseline
                    },
                    changes: vec![
                        (BookSide::Bid, 10.into(), level.clone()),
                        (BookSide::Ask, 12.into(), remove.clone()),
                    ]
                    .into(),
                    timestamp,
                },
                RouterDepthEvent::Snapshot {
                    stream_generation: 7,
                    book_generation: 9,
                    baseline: DepthSequenceBaseline {
                        last_sequence: 102,
                        ..baseline
                    },
                    levels: vec![].into(),
                    timestamp,
                },
            ]
            .into(),
        ),
    )
    .await;
    let p = bounded(async {
        loop {
            if let StreamMessage::Publication(p) = next(&mut sub).await {
                break p;
            }
        }
    })
    .await;
    assert_eq!(p.records.len(), 3);
    for i in [0, 2] {
        let NativeEvent::Depth(d) = &p.records[i].native else {
            panic!()
        };
        assert_eq!(d.kind, "snapshot_omitted");
        assert!(d.operations.is_empty());
        assert_eq!(p.records[i].ordinal, i as u64);
    }
    let NativeEvent::Depth(delta) = &p.records[1].native else {
        panic!()
    };
    assert_eq!(delta.operations.len(), 2);
    assert_eq!(delta.operations[0].operation, "set");
    assert_eq!(delta.operations[1].operation, "remove");
    assert_eq!(delta.operations[1].price.coefficient, "12");
    assert!(!delta.client_book_initialized);
    assert!(serde_json::to_vec(&p).unwrap().len() < 10000);
    let mut late = client.subscribe("d").await.unwrap();
    next(&mut late).await;
    publish(
        &rig.producers[0],
        RouterPublication::Depth(
            vec![RouterDepthEvent::Delta {
                stream_generation: 7,
                book_generation: 9,
                baseline: DepthSequenceBaseline {
                    last_sequence: 103,
                    ..baseline
                },
                changes: vec![(BookSide::Ask, 12.into(), remove)].into(),
                timestamp,
            }]
            .into(),
        ),
    )
    .await;
    let tail = bounded(async {
        loop {
            if let StreamMessage::Publication(p) = next(&mut late).await {
                break p;
            }
        }
    })
    .await;
    let NativeEvent::Depth(delta) = &tail.records[0].native else {
        panic!()
    };
    assert_eq!(delta.kind, "delta");
    assert_eq!(delta.book_generation, 9);
    assert!(!delta.client_book_initialized);
    late.close().await.unwrap();
    sub.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_application_queue_fails_locally_and_releases_router_demand() {
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("slow");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(
        &path,
        ClientOptions {
            subscription_queue_messages: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut slow = client.subscribe("a").await.unwrap();
    let fast_client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut fast = fast_client.subscribe("b").await.unwrap();
    next(&mut fast).await;
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    bounded(async {
        while rig.router.status().subscriptions != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(matches!(
        next(&mut slow).await,
        StreamMessage::IntegritySnapshot(_)
    ));
    assert!(slow.recv().await.is_err());
    publish(&rig.producers[1], publication("b", 1, 100)).await;
    assert!(matches!(
        next(&mut fast).await,
        StreamMessage::IntegrityUpdate(_)
    ));
    assert!(matches!(
        next(&mut fast).await,
        StreamMessage::Publication(_)
    ));
    fast.close().await.unwrap();
    client.close().await.unwrap();
    fast_client.close().await.unwrap();
    server.shutdown().await.unwrap();
    assert_eq!(rig.router.status().subscriptions, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nonreading_socket_deadline_and_duplicate_ids() {
    use marketforge_engine::stream_protocol::codec::{read_frame, write_frame};
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("blocked");
    let mut c = config(&path);
    c.streaming.write_timeout_ms = 50;
    let server = UnixStreamingServer::bind(rig.router.clone(), c)
        .await
        .unwrap();
    let mut raw = tokio::net::UnixStream::connect(&path).await.unwrap();
    write_frame(
        &mut raw,
        &p::Envelope::control(
            1,
            p::Message::Hello {
                versions: vec![1],
                schemas: vec![1],
            },
        ),
        1048576,
    )
    .await
    .unwrap();
    read_frame(&mut raw, 1048576).await.unwrap();
    write_frame(
        &mut raw,
        &p::Envelope::control(
            2,
            p::Message::Subscribe {
                stream_id: "a".into(),
                subscription_id: 1,
            },
        ),
        1048576,
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(&mut raw, 1048576)
            .await
            .unwrap()
            .unwrap()
            .message,
        p::Message::Subscribed { .. }
    ));
    read_frame(&mut raw, 1048576).await.unwrap();
    write_frame(
        &mut raw,
        &p::Envelope::control(
            3,
            p::Message::Subscribe {
                stream_id: "a".into(),
                subscription_id: 1,
            },
        ),
        1048576,
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(&mut raw, 1048576)
            .await
            .unwrap()
            .unwrap()
            .message,
        p::Message::Error { .. }
    ));
    let fast_client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut fast = fast_client.subscribe("b").await.unwrap();
    next(&mut fast).await;
    for i in 0..30 {
        let RouterPublication::Trades { records, .. } = publication("a", i, 100) else {
            panic!()
        };
        let mut t = records[0].clone();
        t.trade_id = Some("x".repeat(2048));
        publish(&rig.producers[0], RouterPublication::trades(vec![t; 32])).await;
    }
    bounded(async {
        while rig.router.status().subscriptions != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    // Do not drain the server's output. Fill the opposite direction with valid
    // bounded commands; only connection termination can unblock this write.
    use tokio::io::AsyncWriteExt;
    let ping = p::codec::encode(&p::Envelope::control(4, p::Message::Ping), 4096).unwrap();
    let mut frame = (ping.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(&ping);
    let flood = frame.repeat(8192);
    assert!(bounded(raw.write_all(&flood)).await.is_err());
    publish(&rig.producers[1], publication("b", 2, 200)).await;
    assert!(matches!(
        next(&mut fast).await,
        StreamMessage::IntegrityUpdate(_)
    ));
    assert!(matches!(
        next(&mut fast).await,
        StreamMessage::Publication(_)
    ));
    fast.close().await.unwrap();
    fast_client.close().await.unwrap();
    drop(raw);
    server.shutdown().await.unwrap();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authoritative_source_processor_gap_without_accepted_data_reaches_socket() {
    use marketforge_engine::live::{infrastructure::normalization::*, stream::source::*};
    let rig = Rig::new(vec![], RouterConfig::default());
    let epoch = TradeSourceEpoch {
        stream_generation: 7,
        connection_generation: 3,
        subscription_generation: 2,
    };
    let t = source::trade(10);
    let integrity = IntegrityFacadeFactory::trades(CreateTradeIntegrity {
        binding: TradeSourceBinding::new(
            "a",
            t.envelope.exchange,
            "spot",
            t.envelope.instrument_id,
            "BTCUSDT",
            epoch,
        )
        .unwrap(),
        plan: CompiledTradeIntegrityPlan {
            format_id: "binance_spot".into(),
            identity_kind: TradeIdentityKind::Numeric,
            sequence_kind: TradeSequenceKind::None,
            detect_duplicates: true,
            detect_id_regression: true,
            detect_id_gaps: true,
            detect_cross_sequence_regression: false,
            allow_equal_cross_sequence: false,
            coverage_action: TradeCoverageAction::RequestRecovery,
            supports_individual_trades: true,
        },
        identity_limits: TradeIdentityLimits {
            max_ids: 2,
            max_id_bytes: 128,
            max_batch_records: 8,
        },
    })
    .unwrap();
    let mut processor = SourceProcessor::new(integrity).unwrap();
    let dir = private_dir();
    let path = dir.path().join("source");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("a").await.unwrap();
    next(&mut sub).await;
    for id in [10, 20] {
        let mut trade = source::trade(id);
        trade.envelope.stream_id = "a".into();
        processor
            .process_normalized(
                NormalizedMessage::Trades(
                    NormalizedTrades::new(
                        "a",
                        NormalizationSource::WebSocket,
                        TradeRecordSemantics::Individual,
                        vec![trade],
                    )
                    .unwrap(),
                ),
                SourceProcessingEpoch::Trades(epoch),
                1024,
                |_| panic!(),
                |o| rig.producers[0].publish_source_trades(SourceTradePublication::Integrity(o)),
            )
            .unwrap();
    }
    let mut saw_gap = false;
    let mut publications = 0;
    bounded(async {
        while !saw_gap {
            match next(&mut sub).await {
                StreamMessage::IntegrityUpdate(u)
                    if u.evidence.coverage == "coverage_review_required" =>
                {
                    assert_eq!(u.evidence.observations[0].previous, 10);
                    assert_eq!(u.evidence.observations[0].observed, 20);
                    assert_eq!(u.evidence.received_records, None);
                    saw_gap = true;
                }
                StreamMessage::Publication(_) => publications += 1,
                _ => (),
            }
        }
    })
    .await;
    // The accepted baseline is still 10, with no fabricated market record for 20.
    assert!(processor.integrity().trade_coverage_hold().is_some());
    assert_eq!(rig.router.status().accepted, 1);
    if publications == 0 {
        assert!(matches!(
            next(&mut sub).await,
            StreamMessage::Publication(_)
        ));
    }
    sub.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn split_publication_keeps_one_evidence_identity_and_bounded_history() {
    let rig = Rig::new(
        vec![
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "b"],
                &[],
                Some(MergeOrdering::ArrivalOrder),
            ),
            derived("v", DerivedOperation::Convert, &["m"], &[], None),
        ],
        RouterConfig::default(),
    );
    let h = rig.service.handle();
    let m = bounded(h.start_derived("m")).await.unwrap();
    let v = bounded(h.start_derived("v")).await.unwrap();
    let dir = private_dir();
    let path = dir.path().join("split");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("v").await.unwrap();
    next(&mut sub).await;
    let mut meta = metadata();
    meta.observations = vec![meta.observations[0].clone(); 100].into();
    let mut records = vec![];
    for (seq, time) in [(1, 100), (2, 200)] {
        let RouterPublication::Trades { records: r, .. } = publication("a", seq, time) else {
            panic!()
        };
        records.push(r[0].clone());
    }
    publish(
        &rig.producers[0],
        RouterPublication::Trades {
            records: records.into(),
            recovered: false,
            integrity: Some(meta.clone()),
        },
    )
    .await;
    let mut evidence = vec![];
    bounded(async {
        while evidence.len() < 2 {
            if let StreamMessage::Publication(p) = next(&mut sub).await {
                evidence.push(p.records[0].integrity.clone().unwrap());
            }
        }
    })
    .await;
    assert_eq!(evidence[0].id, evidence[1].id);
    assert_eq!(evidence[0].duplicate_records, Some(1));
    assert_eq!(evidence[0].observations.len(), 64);
    assert!(evidence[0].details_truncated);
    sub.close().await.unwrap();
    for _ in 0..40 {
        rig.producers[0]
            .publish_trade_integrity(&meta, false)
            .unwrap();
    }
    let mut late = client.subscribe("v").await.unwrap();
    let StreamMessage::IntegritySnapshot(snapshot) = next(&mut late).await else {
        panic!()
    };
    assert!(snapshot.history_truncated);
    assert!(snapshot.evidence_history.len() <= 32);
    assert!(serde_json::to_vec(&snapshot).unwrap().len() <= 256 * 1024);
    late.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    bounded(h.stop(&v)).await.unwrap();
    h.take_report("v", v.generation).unwrap().discard();
    bounded(h.stop(&m)).await.unwrap();
    h.take_report("m", m.generation).unwrap().discard();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_derived_snapshot_omits_native_and_valuation_arrays() {
    use marketforge_engine::shared::{
        conversion::{ConversionDecimal, ConvertedTradeValues, evaluation::EvaluatedConversion},
        domain::{book::BookLevel, canonical::BookSide},
    };
    let rig = Rig::definitions(
        vec![
            source_def("a", "BTC", "USDT", 1),
            source::definition("binance_spot", SourceChannel::Depth, "d"),
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "d"],
                &[],
                Some(MergeOrdering::ArrivalOrder),
            ),
        ],
        RouterConfig::default(),
    );
    let mut a = subscribe(&rig.router, "a").await;
    let mut d = subscribe(&rig.router, "d").await;
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    let trade = DerivedEvent::from_delivery(&bounded(a.recv()).await.unwrap(), 0, 1, 0).unwrap();
    let baseline = DepthSequenceBaseline {
        last_sequence: 10,
        cross_sequence: None,
        synchronized: true,
    };
    let timestamp = DepthAcceptedTimestamp {
        latest_source: None,
        composition: DepthTimestampComposition::SingleMessage,
    };
    let level = (
        BookSide::Bid,
        1.into(),
        BookLevel {
            quantity_base: Some(2.into()),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        },
    );
    publish(
        &rig.producers[1],
        RouterPublication::Depth(
            vec![
                RouterDepthEvent::Snapshot {
                    stream_generation: 1,
                    book_generation: 1,
                    baseline,
                    levels: vec![level.clone(); 20000].into(),
                    timestamp,
                },
                RouterDepthEvent::Delta {
                    stream_generation: 1,
                    book_generation: 1,
                    baseline: DepthSequenceBaseline {
                        last_sequence: 11,
                        ..baseline
                    },
                    changes: vec![level].into(),
                    timestamp,
                },
            ]
            .into(),
        ),
    )
    .await;
    let delivery = bounded(d.recv()).await.unwrap();
    let mut snapshot = DerivedEvent::from_delivery(&delivery, 1, 2, 0).unwrap();
    let delta = DerivedEvent::from_delivery(&delivery, 1, 2, 1).unwrap();
    let one = ConversionDecimal::one();
    snapshot.valuation = Some(EventValuation {
        target_quote: "USDT".into(),
        reference_integrity: vec![],
        evaluation: EvaluatedConversion {
            rate: one,
            numerator: 1.into(),
            denominator: 1.into(),
            references: vec![],
        },
        reference_generations: vec![],
        path: vec!["USDT".into()],
        values: vec![
            ConvertedTradeValues {
                price_target: one,
                quantity_quote_target: None
            };
            20000
        ]
        .into(),
    });
    let authority = {
        let mut r = rig.registry.lock().unwrap();
        let g = r.begin_start("m").unwrap();
        r.mark_running("m", g).unwrap();
        r.publication_authority("m").unwrap()
    };
    let producer = rig
        .router
        .attach_processing_producer(authority, &rig.router.processing_inputs("m").unwrap())
        .unwrap();
    let dir = private_dir();
    let path = dir.path().join("mixed");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("m").await.unwrap();
    next(&mut sub).await;
    publish(
        &producer,
        RouterPublication::Derived(vec![trade, snapshot, delta].into()),
    )
    .await;
    let StreamMessage::Publication(p) = next(&mut sub).await else {
        panic!()
    };
    assert_eq!(p.records.len(), 3);
    assert!(matches!(&p.records[0].native, NativeEvent::Trade(_)));
    assert!(
        matches!(&p.records[1].native,NativeEvent::Depth(d) if d.kind=="snapshot_omitted" && d.operations.is_empty())
    );
    assert!(p.records[1].valuation.is_none());
    assert!(
        matches!(&p.records[2].native,NativeEvent::Depth(d) if d.kind=="delta" && d.operations.len()==1)
    );
    assert!(serde_json::to_vec(&p).unwrap().len() < 10000);
    sub.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    drop(producer);
    drop(a);
    drop(d);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn recovered_records_reference_review_without_claiming_complete_history() {
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("recovery");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("a").await.unwrap();
    next(&mut sub).await;
    rig.producers[0]
        .publish_trade_integrity(&metadata(), true)
        .unwrap();
    let StreamMessage::IntegrityUpdate(gap) = next(&mut sub).await else {
        panic!()
    };
    let mut trade = source::trade(20);
    trade.envelope.stream_id = "a".into();
    rig.producers[0]
        .publish_source_trades(SourceTradePublication::RecoveryObserved {
            records: &[trade],
            source_generation: 7,
        })
        .unwrap();
    let StreamMessage::IntegrityUpdate(recovery) = next(&mut sub).await else {
        panic!()
    };
    assert!(recovery.evidence.recovered_records);
    assert_eq!(recovery.evidence.source_generation, Some(7));
    assert_eq!(recovery.evidence.recovery_from, Some(gap.evidence.id));
    assert_eq!(recovery.evidence.recovery_complete, None);
    assert_eq!(recovery.evidence.received_records, None);
    assert_eq!(recovery.evidence.duplicate_records, None);
    let StreamMessage::Publication(p) = next(&mut sub).await else {
        panic!()
    };
    assert!(p.recovered_records);
    assert!(p.records[0].integrity.as_ref().unwrap().recovered_records);
    sub.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn oversized_publication_ends_one_subscription_without_partial_records() {
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("oversize");
    let mut c = config(&path);
    c.streaming.max_frame_bytes = 4096;
    let server = UnixStreamingServer::bind(rig.router.clone(), c)
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut a = client.subscribe("a").await.unwrap();
    let mut b = client.subscribe("b").await.unwrap();
    next(&mut a).await;
    next(&mut b).await;
    let records = (1..=100)
        .map(|n| {
            let mut t = source::trade(n);
            t.envelope.stream_id = "a".into();
            t
        })
        .collect();
    publish(&rig.producers[0], RouterPublication::trades(records)).await;
    assert!(matches!(
        next(&mut a).await,
        StreamMessage::IntegrityUpdate(_)
    ));
    let StreamMessage::End(end) = next(&mut a).await else {
        panic!("oversized publication was emitted partially")
    };
    assert_eq!(end.reason, "publication_too_large");
    publish(&rig.producers[1], publication("b", 1, 100)).await;
    assert!(matches!(
        next(&mut b).await,
        StreamMessage::IntegrityUpdate(_)
    ));
    assert!(matches!(next(&mut b).await, StreamMessage::Publication(_)));
    a.close().await.unwrap();
    b.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    rig.close().await;
}
#[test]
fn streaming_configuration_defaults_and_resource_validation() {
    let c = config(std::path::Path::new("private/socket"));
    assert!(c.validate_streaming().is_ok());
    assert!(serde_json::to_value(&c).unwrap().get("streaming").is_none());
    for case in 0..5 {
        let mut bad = c.clone();
        match case {
            0 => bad.streaming.max_subscriptions = 0,
            1 => bad.streaming.max_frame_bytes = 1,
            2 => bad.streaming.pending_commands = 0,
            3 => bad.streaming.write_timeout_ms = 0,
            _ => bad.streaming.max_frame_bytes = bad.subscriber_queue_bytes + 1,
        };
        assert!(bad.validate_streaming().is_err());
    }
    let mut explicit = c;
    explicit.streaming.max_subscriptions = 2;
    explicit.streaming.max_frame_bytes = 4096;
    let value = serde_json::to_value(&explicit).unwrap();
    assert_eq!(
        serde_json::from_value::<general::PublicationConfig>(value).unwrap(),
        explicit
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integrity_only_observation_does_not_advance_watermarks_or_reference_state() {
    let rig = Rig::definitions(
        vec![
            source_def("a", "ETH", "BTC", 1),
            source_def("b", "BTC", "USDT", 2),
            source_def("c", "ETH", "BTC", 3),
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "c"],
                &[],
                Some(MergeOrdering::EventTime),
            ),
            derived("v", DerivedOperation::Convert, &["m"], &["b"], None),
        ],
        RouterConfig::default(),
    );
    let h = rig.service.handle();
    let m = bounded(h.start_derived("m")).await.unwrap();
    let v = bounded(h.start_derived("v")).await.unwrap();
    let dir = private_dir();
    let path = dir.path().join("integrity-only");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("v").await.unwrap();
    next(&mut sub).await;
    rig.producers[1]
        .publish_trade_integrity(&metadata(), true)
        .unwrap();
    let StreamMessage::IntegrityUpdate(update) = next(&mut sub).await else {
        panic!()
    };
    assert_eq!(update.evidence.id.source, "b");
    assert_eq!(update.evidence.publication_sequence, None);
    assert_eq!(update.evidence.after_publication_sequence, None);
    assert_eq!(rig.router.status().accepted, 0);
    for association in h.status().associations {
        assert!(association.derived.watermarks.iter().all(Option::is_none));
        assert!(
            association
                .derived
                .last_reference_times
                .iter()
                .all(Option::is_none)
        );
        assert_eq!(association.derived.retained_events, 0);
        assert_eq!(association.input.queued_events, 0);
        assert_eq!(association.input.in_flight_events, 0);
    }
    sub.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    bounded(h.stop(&v)).await.unwrap();
    h.take_report("v", v.generation).unwrap().discard();
    bounded(h.stop(&m)).await.unwrap();
    h.take_report("m", m.generation).unwrap().discard();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unsupported_schema_is_explicit_and_releases_subscriptions() {
    use marketforge_engine::stream_protocol::codec::{read_frame, write_frame};
    use tokio::io::AsyncWriteExt;
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("unsupported");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let mut raw = tokio::net::UnixStream::connect(&path).await.unwrap();
    write_frame(
        &mut raw,
        &p::Envelope::control(
            1,
            p::Message::Hello {
                versions: vec![1],
                schemas: vec![1],
            },
        ),
        4096,
    )
    .await
    .unwrap();
    read_frame(&mut raw, 4096).await.unwrap();
    write_frame(
        &mut raw,
        &p::Envelope::control(
            2,
            p::Message::Subscribe {
                stream_id: "a".into(),
                subscription_id: 1,
            },
        ),
        4096,
    )
    .await
    .unwrap();
    read_frame(&mut raw, 4096).await.unwrap();
    read_frame(&mut raw, 4096).await.unwrap();
    let mut unknown = p::Envelope::control(3, p::Message::Ping);
    unknown.schema_version = 99;
    let mut bytes = vec![];
    ciborium::ser::into_writer(&unknown, &mut bytes).unwrap();
    raw.write_all(&(bytes.len() as u32).to_be_bytes())
        .await
        .unwrap();
    raw.write_all(&bytes).await.unwrap();
    let error = bounded(read_frame(&mut raw, 4096)).await.unwrap().unwrap();
    assert!(matches!(error.message,p::Message::Error{code,..} if code=="unsupported_version"));
    assert!(bounded(read_frame(&mut raw, 4096)).await.unwrap().is_none());
    bounded(async {
        while rig.router.status().subscriptions != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    server.shutdown().await.unwrap();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn terminal_processing_discontinuity_survives_queue_discard() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::EventTime),
        )],
        RouterConfig::default(),
    );
    let h = rig.service.handle();
    let m = bounded(h.start_derived("m")).await.unwrap();
    let dir = private_dir();
    let path = dir.path().join("terminal");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("m").await.unwrap();
    next(&mut sub).await;
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    publish(&rig.producers[0], publication("a", 2, 50)).await;
    let snapshot = bounded(async {
        loop {
            match next(&mut sub).await {
                StreamMessage::IntegritySnapshot(s) if s.discontinuity.is_some() => break s,
                StreamMessage::End(e) => panic!("lost data-integrity explanation: {e:?}"),
                _ => (),
            }
        }
    })
    .await;
    let discontinuity = snapshot.discontinuity.unwrap();
    assert!(
        discontinuity
            .detail
            .starts_with("source-local timestamp/sequence conflict")
    );
    assert!(!discontinuity.detail_truncated);
    assert_eq!(discontinuity.after_publication_sequence, None);
    assert!(matches!(next(&mut sub).await, StreamMessage::End(_)));
    bounded(async {
        loop {
            if let Some(report) = h.take_report("m", m.generation) {
                report.discard();
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    sub.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_market_ack_retains_observed_gap_through_terminal_slot() {
    use marketforge_engine::live::{infrastructure::normalization::*, stream::source::*};
    let mut limits = RouterConfig::default();
    limits.ingress.bytes = 1;
    let mut rig = Rig::new(vec![], limits);
    let mut owner = processor("a", TradeCoverageAction::ReportUnverifiedCoverage);
    let dir = private_dir();
    let path = dir.path().join("reject");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("a").await.unwrap();
    next(&mut sub).await;
    let message = || {
        let mut records = vec![];
        for id in [10, 20] {
            let mut t = source::trade(id);
            t.envelope.stream_id = "a".into();
            records.push(t);
        }
        NormalizedMessage::Trades(
            NormalizedTrades::new(
                "a",
                NormalizationSource::WebSocket,
                TradeRecordSemantics::Individual,
                records,
            )
            .unwrap(),
        )
    };
    let epoch = SourceProcessingEpoch::Trades(TradeSourceEpoch {
        stream_generation: 7,
        connection_generation: 3,
        subscription_generation: 2,
    });
    let rejected = owner.process_normalized(
        message(),
        epoch,
        1024,
        |_| panic!(),
        |outcome| {
            rig.producers[0].publish_source_trades(SourceTradePublication::Integrity(outcome))
        },
    );
    assert!(rejected.is_err());
    assert_eq!(rig.router.status().accepted, 0);
    let mut fresh = 0;
    owner
        .process_normalized(
            message(),
            epoch,
            1024,
            |_| panic!(),
            |outcome| {
                if let IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(
                    TradeControllerOutcome::Accepted(batch),
                )) = outcome
                {
                    fresh = batch.trades.len();
                }
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(fresh, 2, "rejected publication committed Source identities");
    drop(rig.producers.remove(0));
    let StreamMessage::IntegrityUpdate(update) = next(&mut sub).await else {
        panic!()
    };
    assert_eq!(update.evidence.coverage, "publication_rejected_unverified");
    assert_eq!(update.evidence.publication_sequence, None);
    assert_eq!(update.evidence.numeric_trade_id, None);
    assert_eq!(update.evidence.observations[0].previous, 10);
    assert_eq!(update.evidence.observations[0].observed, 20);
    assert!(matches!(next(&mut sub).await, StreamMessage::End(_)));
    sub.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partial_frame_deadline_does_not_expire_idle_live_sessions() {
    use marketforge_engine::stream_protocol::codec::{read_frame, write_frame};
    use tokio::io::AsyncWriteExt;
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("read-deadline");
    let mut c = config(&path);
    c.streaming.read_timeout_ms = 20;
    let server = UnixStreamingServer::bind(rig.router.clone(), c)
        .await
        .unwrap();
    let mut raw = tokio::net::UnixStream::connect(&path).await.unwrap();
    write_frame(
        &mut raw,
        &p::Envelope::control(
            1,
            p::Message::Hello {
                versions: vec![1],
                schemas: vec![1],
            },
        ),
        4096,
    )
    .await
    .unwrap();
    read_frame(&mut raw, 4096).await.unwrap();
    // A live data reader need not keep issuing control commands to stay connected.
    assert!(
        tokio::time::timeout(Duration::from_millis(40), read_frame(&mut raw, 4096))
            .await
            .is_err()
    );
    raw.write_all(&[0]).await.unwrap();
    let error = bounded(read_frame(&mut raw, 4096)).await.unwrap().unwrap();
    assert!(matches!(error.message,p::Message::Error{code,..} if code=="read_failure"));
    assert!(bounded(read_frame(&mut raw, 4096)).await.unwrap().is_none());
    server.shutdown().await.unwrap();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finite_replay_sources_cannot_cross_live_transport() {
    let graph =
        Arc::new(StreamGraph::restore(vec![source_def("a", "BTC", "USDT", 1)], vec![]).unwrap());
    let mut runtime = StreamRuntimeRegistry::new(&graph);
    runtime.begin_start("a").unwrap();
    let router = Arc::new(
        StreamRouter::new(
            RouterConfig::default(),
            RouterContext {
                graph,
                demand: Arc::new(Mutex::new(DemandRegistry::new())),
            },
        )
        .unwrap(),
    );
    let dir = private_dir();
    let path = dir.path().join("no-replay");
    let server = UnixStreamingServer::bind(router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut live = client.subscribe("a").await.unwrap();
    next(&mut live).await;
    let replay = router
        .attach_replay_producer(AttachProducerRequest {
            authority: runtime.publication_authority("a").unwrap(),
        })
        .unwrap();
    assert!(matches!(next(&mut live).await, StreamMessage::End(_)));
    assert!(client.subscribe("a").await.is_err());
    // The existing internal replay seam remains available to its ordinary consumers.
    let mut internal = subscribe(&router, "a").await;
    publish(&replay, publication("a", 1, 100)).await;
    assert!(matches!(
        bounded(internal.recv()).await,
        Some(RouterDelivery::Publication { .. })
    ));
    assert!(live.recv().await.unwrap().is_none());
    internal.close().await.unwrap();
    live.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    drop(replay);
    router.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn depth_generation_change_cannot_reuse_previous_verified_coverage() {
    use marketforge_engine::shared::domain::{book::BookLevel, canonical::BookSide};
    let rig = Rig::definitions(
        vec![source::definition(
            "binance_spot",
            SourceChannel::Depth,
            "d",
        )],
        RouterConfig::default(),
    );
    let dir = private_dir();
    let path = dir.path().join("coverage-reset");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = client.subscribe("d").await.unwrap();
    next(&mut sub).await;
    let baseline = DepthSequenceBaseline {
        last_sequence: 100,
        cross_sequence: None,
        synchronized: true,
    };
    let timestamp = DepthAcceptedTimestamp {
        latest_source: None,
        composition: DepthTimestampComposition::SingleMessage,
    };
    let status = StreamIntegrityStatus {
        channel: IntegrityChannel::Depth,
        stream_generation: 7,
        depth_synchronized: Some(true),
        trades_admissible: None,
        trades_quarantined: None,
        depth_coverage: Some(marketforge_engine::live::integrity::DepthCoverage {
            subscribed_depth: Some(10),
            retained_bid_levels: 1,
            retained_ask_levels: 0,
            synchronized: true,
            snapshot_source: Some(
                marketforge_engine::live::infrastructure::normalization::NormalizationSource::Rest,
            ),
            snapshot_declared_depth: Some(10),
        }),
    };
    rig.producers[0]
        .publish_depth_integrity(
            &DepthControllerOutcome::Accepted {
                changes: vec![],
                snapshot: true,
                book_generation: 9,
                baseline,
                timestamp,
            },
            status,
        )
        .unwrap();
    next(&mut sub).await;
    publish(
        &rig.producers[0],
        RouterPublication::Depth(
            vec![RouterDepthEvent::Snapshot {
                stream_generation: 7,
                book_generation: 9,
                baseline,
                timestamp,
                levels: vec![(
                    BookSide::Bid,
                    1.into(),
                    BookLevel {
                        quantity_base: Some(1.into()),
                        quantity_quote: None,
                        quantity_contracts: None,
                        order_count: None,
                    },
                )]
                .into(),
            }]
            .into(),
        ),
    )
    .await;
    next(&mut sub).await;
    let StreamMessage::Publication(p) = next(&mut sub).await else {
        panic!()
    };
    assert!(
        p.records[0]
            .integrity
            .as_ref()
            .unwrap()
            .depth_coverage
            .as_ref()
            .unwrap()
            .internal_synchronized
    );
    publish(
        &rig.producers[0],
        RouterPublication::Depth(
            vec![RouterDepthEvent::Invalidated {
                stream_generation: 8,
                book_generation: 1,
            }]
            .into(),
        ),
    )
    .await;
    let StreamMessage::IntegrityUpdate(update) = next(&mut sub).await else {
        panic!()
    };
    assert_eq!(update.evidence.source_generation, Some(8));
    assert!(update.evidence.depth_coverage.is_none());
    assert!(update.evidence.depth_sequence.is_none());
    let StreamMessage::Publication(p) = next(&mut sub).await else {
        panic!()
    };
    assert!(
        p.records[0]
            .integrity
            .as_ref()
            .unwrap()
            .depth_coverage
            .is_none()
    );
    sub.close().await.unwrap();
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_connection_churn_releases_unread_subscriptions() {
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("churn");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let healthy = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut live = healthy.subscribe("a").await.unwrap();
    next(&mut live).await;
    for round in 0..12 {
        let mut clients = vec![];
        for _ in 0..3 {
            let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
                .await
                .unwrap();
            let first = client.subscribe("b").await.unwrap();
            let second = client.subscribe("c").await.unwrap();
            clients.push((client, first, second));
        }
        publish(&rig.producers[1], publication("b", round + 1, 100)).await;
        publish(&rig.producers[2], publication("c", round + 1, 100)).await;
        for (client, mut first, second) in clients {
            // Initial and market messages may remain unread. Explicit close
            // drains charges, Drop closes the other receiver without awaiting.
            first.close().await.unwrap();
            assert_eq!(first.queued_payload_bytes(), 0);
            drop(second);
            drop(first);
            drop(client);
        }
        bounded(async {
            while rig.router.status().subscriptions != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        publish(&rig.producers[0], publication("a", round + 1, 100)).await;
        loop {
            if let StreamMessage::Publication(p) = next(&mut live).await {
                assert_eq!(p.router_sequence, round + 1);
                break;
            }
        }
    }
    live.close().await.unwrap();
    assert_eq!(live.queued_payload_bytes(), 0);
    healthy.close().await.unwrap();
    server.shutdown().await.unwrap();
    assert_eq!(rig.router.status().subscriptions, 0);
    rig.close().await;
}

#[tokio::test]
async fn cancelled_subscribe_after_attachment_and_acceptance_releases_ownership() {
    use std::{
        future::{Future, poll_fn},
        task::Poll,
    };
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("cancel-attached");
    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    for accepted in [false, true] {
        let mut request = Box::pin(client.subscribe("a"));
        poll_fn(|cx| {
            assert!(request.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        bounded(async {
            while rig.router.status().subscriptions != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if accepted {
            client.ping().await.unwrap();
        } // Acceptance precedes Pong; initial messages can be pending locally.
        drop(request);
        bounded(async {
            while rig.router.status().subscriptions != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        client.ping().await.unwrap();
    }
    client.close().await.unwrap();
    server.shutdown().await.unwrap();
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_conversion_keeps_event_and_reference_evidence_while_new_updates_overtake() {
    let rig = Rig::definitions(
        vec![
            source_def("a", "ETH", "BTC", 1),
            source_def("b", "BTC", "USDT", 2),
            source_def("c", "ETH", "BTC", 3),
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "c"],
                &[],
                Some(MergeOrdering::ArrivalOrder),
            ),
            derived("v", DerivedOperation::Convert, &["m"], &["b"], None),
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let m = bounded(handle.start_derived("m")).await.unwrap();
    let v = bounded(handle.start_derived("v")).await.unwrap();
    let mut controlled = rig
        .router
        .subscribe_live(SubscribeRequest {
            stream_id: "v".into(),
            owner: source::owner("controlled"),
        })
        .await
        .unwrap();
    controlled.recv_live().await.unwrap();
    let mut output = subscribe(&rig.router, "v").await;
    let mut a = processor("a", TradeCoverageAction::ReportUnverifiedCoverage);
    let mut b = processor("b", TradeCoverageAction::ReportUnverifiedCoverage);
    source_input(&mut b, &rig.producers[1], "b", &[(1, 90, 10)]);
    source_input(&mut a, &rig.producers[0], "a", &[(1, 100, 3), (2, 100, 3)]);
    source_input(&mut a, &rig.producers[0], "a", &[(3, 300, 4)]);
    source_input(&mut b, &rig.producers[1], "b", &[(2, 200, 30)]);
    // A real output delivered to another consumer is the synchronization barrier:
    // the controlled live observer has not drained its older market publications.
    let first = bounded(output.recv()).await.unwrap();
    let RouterDelivery::Publication {
        publication: first, ..
    } = first
    else {
        panic!()
    };
    let RouterPublication::Derived(events) = first.as_ref() else {
        panic!()
    };
    let applicable = events[0].evidence.integrity().unwrap().id.clone();
    let reference = events[0].valuation.as_ref().unwrap().reference_integrity[0]
        .id
        .clone();
    rig.producers[0]
        .publish_trade_integrity(&metadata(), false)
        .unwrap();
    rig.producers[1]
        .publish_trade_integrity(&metadata(), false)
        .unwrap();
    let mut latest_seen = false;
    let mut ordinals = vec![];
    let mut last_sequence = None;
    bounded(async {
        while ordinals.len() < 2 {
            match controlled.recv_live().await.unwrap() {
                RouterStreamMessage::IntegrityUpdate(u) => {
                    if u.evidence.numeric_trade_id == Some(20) {
                        latest_seen = true;
                        assert!(u.evidence.after_publication_sequence.is_some());
                    }
                }
                RouterStreamMessage::Market(RouterDelivery::Publication {
                    sequence,
                    publication,
                    ..
                }) => {
                    assert!(latest_seen);
                    if let Some(last) = last_sequence {
                        assert_eq!(sequence, last + 1);
                    }
                    last_sequence = Some(sequence);
                    let RouterPublication::Derived(events) = publication.as_ref() else {
                        panic!()
                    };
                    for e in events.iter().filter(|e| {
                        e.origin.stream_id == "a" && e.native.timestamp_ns() == Some(100)
                    }) {
                        assert_eq!(e.evidence.integrity().unwrap().id, applicable);
                        assert_eq!(
                            e.valuation.as_ref().unwrap().reference_integrity[0].id,
                            reference
                        );
                        ordinals.push(e.origin.record_ordinal);
                    }
                }
                other => panic!("unexpected {other:?}"),
            }
        }
    })
    .await;
    assert_eq!(ordinals, vec![0, 1]);
    controlled.close().await.unwrap();
    drop(output);
    bounded(handle.stop(&v)).await.unwrap();
    bounded(handle.stop(&m)).await.unwrap();
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_during_partial_publication_and_close_racing_eof() {
    use p::codec::*;
    use tokio::io::AsyncReadExt;
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let path = dir.path().join("partial-shutdown");
    let mut cfg = config(&path);
    cfg.streaming.write_timeout_ms = 50;
    cfg.streaming.shutdown_timeout_ms = 1000;
    let server = UnixStreamingServer::bind(rig.router.clone(), cfg)
        .await
        .unwrap();
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut live = client.subscribe("b").await.unwrap();
    next(&mut live).await;
    let mut raw = tokio::net::UnixStream::connect(&path).await.unwrap();
    write_frame(
        &mut raw,
        &p::Envelope::control(
            1,
            p::Message::Hello {
                versions: vec![1],
                schemas: vec![1],
            },
        ),
        1048576,
    )
    .await
    .unwrap();
    read_frame(&mut raw, 1048576).await.unwrap().unwrap();
    write_frame(
        &mut raw,
        &p::Envelope::control(
            2,
            p::Message::Subscribe {
                stream_id: "a".into(),
                subscription_id: 1,
            },
        ),
        1048576,
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(&mut raw, 1048576)
            .await
            .unwrap()
            .unwrap()
            .message,
        p::Message::Subscribed { .. }
    ));
    read_frame(&mut raw, 1048576).await.unwrap().unwrap();
    let RouterPublication::Trades { records, .. } = publication("a", 1, 100) else {
        panic!()
    };
    publish(
        &rig.producers[0],
        RouterPublication::trades(vec![records[0].clone(); 512]),
    )
    .await;
    // Consume the integrity prerequisite, then only the next frame's prefix.
    read_frame(&mut raw, 1048576).await.unwrap().unwrap();
    let mut prefix = [0; 4];
    bounded(raw.read_exact(&mut prefix)).await.unwrap();
    assert!(u32::from_be_bytes(prefix) > 200_000);
    let (shutdown, close) = tokio::join!(server.shutdown(), client.close());
    shutdown.unwrap();
    close.unwrap();
    assert!(bounded(live.recv()).await.is_err());
    drop(raw);
    drop(live);
    assert_eq!(rig.router.status().subscriptions, 0);
    rig.close().await;
}

#[tokio::test]
async fn socket_lifecycle_rejects_unsafe_paths_and_preserves_replacements() {
    use std::os::unix::fs::{MetadataExt, symlink};
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = private_dir();
    let parent = dir.path().join("private");
    std::fs::create_dir(&parent).unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = parent.join("data.sock");

    // An indirect symlink must not bypass the parent-path check.
    let alias = dir.path().join("alias");
    symlink(dir.path(), &alias).unwrap();
    assert!(
        UnixStreamingServer::bind(rig.router.clone(), config(&alias.join("private/data.sock")))
            .await
            .is_err()
    );
    assert!(!path.exists());

    // Neither stale sockets nor symlink endpoints are removed on startup.
    let stale = std::os::unix::net::UnixListener::bind(&path).unwrap();
    drop(stale);
    assert!(
        UnixStreamingServer::bind(rig.router.clone(), config(&path))
            .await
            .is_err()
    );
    assert!(path.exists());
    std::fs::remove_file(&path).unwrap();
    symlink(parent.join("missing"), &path).unwrap();
    assert!(
        UnixStreamingServer::bind(rig.router.clone(), config(&path))
            .await
            .is_err()
    );
    assert!(std::fs::symlink_metadata(&path).unwrap().is_symlink());
    std::fs::remove_file(&path).unwrap();

    std::fs::write(&path, b"keep file").unwrap();
    assert!(
        UnixStreamingServer::bind(rig.router.clone(), config(&path))
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&path).unwrap(), b"keep file");
    std::fs::remove_file(&path).unwrap();

    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    assert_eq!(std::fs::metadata(&path).unwrap().mode() & 0o777, 0o600);
    assert!(
        UnixStreamingServer::bind(rig.router.clone(), config(&path))
            .await
            .is_err()
    );
    let client = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    client.ping().await.unwrap();
    client.close().await.unwrap();

    // Shutdown owns the original inode, not whatever later occupies its name.
    let moved = parent.join("original.sock");
    std::fs::rename(&path, &moved).unwrap();
    std::fs::write(&path, b"replacement").unwrap();
    server.shutdown().await.unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_file(&moved).unwrap();

    let server = UnixStreamingServer::bind(rig.router.clone(), config(&path))
        .await
        .unwrap();
    server.shutdown().await.unwrap();
    assert!(!path.exists());
    rig.close().await;
}
