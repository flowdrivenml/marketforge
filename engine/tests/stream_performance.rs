//! Opt-in local measurement; no timing assertions. See streaming-v1-verification.md.
#![cfg(unix)]
#[path = "live/integration/derived/harness.rs"]
mod harness;
use harness::*;
use marketforge_engine::{
    live::integrity::*,
    stream_protocol::{self as p, codec::*, market::*},
};
use std::time::Instant;
fn percentiles(mut samples: Vec<u128>) -> (u128, u128, u128) {
    samples.sort_unstable();
    (
        samples[samples.len() / 2],
        samples[samples.len() * 95 / 100],
        samples[samples.len() * 99 / 100],
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in bounded streaming measurement, no timing correctness threshold"]
async fn local_streaming_measurement() {
    for count in [2usize, 128] {
        let ids: Vec<_> = (0..count).map(|i| format!("s{i:03}")).collect();
        let mut defs: Vec<_> = ids
            .iter()
            .map(|id| source::definition("binance_spot", SourceChannel::Trades, id))
            .collect();
        defs.push(derived(
            "all",
            DerivedOperation::Merge,
            &ids.iter().map(String::as_str).collect::<Vec<_>>(),
            &[],
            Some(MergeOrdering::ArrivalOrder),
        ));
        let rig = Rig::definitions(defs, RouterConfig::default());
        let meta = RouterTradeIntegrity {
            source_generation: Some(7),
            received_records: 0,
            duplicate_records: 0,
            identity_history_truncated: true,
            coverage_history_unverified: true,
            baseline: TradeIntegrityBaseline::default(),
            observations: (0..64)
                .map(|i| TradeIntegrityObservation {
                    index: i,
                    kind: TradeIntegrityObservationKind::NumericIdDiscontinuity,
                    previous: 10,
                    current: 20,
                })
                .collect::<Vec<_>>()
                .into(),
        };
        for prod in &rig.producers {
            prod.publish_trade_integrity(&meta, false).unwrap();
        }
        let mut retained_producers = rig.producers;
        let prod = Arc::new(retained_producers.remove(0));
        let publisher = prod.clone();
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let gate = barrier.clone();
        let work = tokio::spawn(async move {
            gate.wait().await;
            let mut times = vec![];
            for i in 1..=100 {
                let start = Instant::now();
                publish(&publisher, publication("s000", i, i as i64)).await;
                times.push(start.elapsed().as_nanos());
                tokio::task::yield_now().await;
            }
            percentiles(times)
        });
        barrier.wait().await;
        let mut times = vec![];
        for i in 0..if count == 128 { 5 } else { 20 } {
            let now = Instant::now();
            let mut s = rig
                .router
                .subscribe_live(SubscribeRequest {
                    stream_id: "all".into(),
                    owner: source::owner(&format!("perf{i}")),
                })
                .await
                .unwrap();
            let Some(RouterStreamMessage::IntegritySnapshot(snap)) = s.recv_live().await else {
                panic!()
            };
            assert!(serde_json::to_vec(&snap).unwrap().len() <= 256 * 1024);
            std::hint::black_box(snap);
            times.push(now.elapsed().as_nanos());
            s.close().await.unwrap();
        }
        println!(
            "snapshot sources={count} attach+receive_ns median/p95/p99={:?} concurrent_source_admission_ns={:?}",
            percentiles(times),
            work.await.unwrap()
        );
        rig.router.shutdown().await.unwrap();
    }
    // Transport-only codec work: no normalization or conversion calculations.
    for (family, n) in [
        ("trade", 1),
        ("trade", 64),
        ("depth", 256),
        ("depth", 1500),
        ("derived", 32),
    ] {
        let d = Decimal {
            coefficient: "1234567890123456789".into(),
            scale: 8,
        };
        let native = if family == "depth" {
            NativeEvent::Depth(Depth {
                kind: "delta".into(),
                source_generation: 7,
                book_generation: 2,
                last_sequence: Some(100),
                cross_sequence: None,
                timestamp_ns: Some(100),
                timestamp_provenance: None,
                composition: None,
                internal_baseline_synchronized: Some(true),
                client_book_initialized: false,
                operations: vec![
                    Level {
                        operation: "set".into(),
                        side: "bid".into(),
                        price: d.clone(),
                        quantity_base: Some(d.clone()),
                        quantity_quote: None,
                        quantity_contracts: None,
                        order_count: None
                    };
                    n
                ],
            })
        } else {
            NativeEvent::Trade(Trade {
                event_timestamp_ns: 100,
                system_timestamp_ns: None,
                exchange: "binance".into(),
                instrument_id: 1,
                symbol: "BTCUSDT".into(),
                stream_id: "s000".into(),
                trade_id: Some("100".into()),
                sequence: Some(100),
                side: "buy".into(),
                price: d.clone(),
                quantity_base: Some(d.clone()),
                quantity_quote: None,
                quantity_contracts: None,
                is_rpi: None,
                trade_iv: None,
                mark_iv: None,
                index_price: None,
                mark_price: None,
            })
        };
        let evidence = p::DataIntegrityEvidence::unavailable("s000".into(), Some(1));
        let valuation = (family == "derived").then(|| Valuation {
            target_quote: "USD".into(),
            rate: d.clone(),
            numerator: "1".into(),
            denominator: "1".into(),
            references: vec![Reference {
                dataset_id: 1,
                stream_id: "ref".into(),
                runtime_generation: Some(1),
                timestamp_ns: 90,
                stream_rank: 0,
                row_sequence: 1,
                event_ordinal: 0,
                direction: "direct".into(),
                price: d.clone(),
            }],
            reference_integrity: vec![evidence.clone()],
            path: vec!["USDT".into(), "USD".into()],
            values: vec![ConvertedValues {
                price: d,
                quantity_quote: None,
            }],
        });
        let records = if family == "depth" { 1 } else { n };
        let message = p::StreamMessage::Publication(StreamPublication {
            stream_id: "s000".into(),
            runtime_generation: 1,
            router_sequence: 1,
            recovered_records: false,
            records: vec![
                Record {
                    ordinal: 0,
                    native,
                    origin: None,
                    upstream: None,
                    processing_input_position: None,
                    processing_admission_ordinal: None,
                    baseline_barrier: None,
                    integrity: Some(evidence),
                    valuation
                };
                records
            ],
        });
        let mut e = p::Envelope::control(0, p::Message::Stream(message));
        e.subscription_id = 1;
        e.subscription_sequence = 1;
        let mut times = vec![];
        let start = Instant::now();
        let mut bytes = 0;
        for _ in 0..200 {
            let t = Instant::now();
            let encoded = encode(&e, 1048576).unwrap();
            bytes += encoded.len();
            std::hint::black_box(decode(&encoded, 1048576).unwrap());
            times.push(t.elapsed().as_nanos());
        }
        let seconds = start.elapsed().as_secs_f64();
        println!(
            "codec family={family} size={n} publications/s={:.0} records/s={:.0} encoded_bytes/s={:.0} roundtrip_ns={:?}",
            200. / seconds,
            (200 * records) as f64 / seconds,
            bytes as f64 / seconds,
            percentiles(times)
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "opt-in local delivery/isolation measurement, no timing threshold"]
async fn unix_delivery_and_slow_reader_measurement() {
    use marketforge_engine::{client::streaming::*, live::transport::unix::UnixStreamingServer};
    use p::{Envelope, Message};
    use std::os::unix::fs::PermissionsExt;
    let rig = Rig::new(vec![], RouterConfig::default());
    let dir = tempfile::tempdir().unwrap();
    std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = dir.path().join("measure");
    let config=serde_json::from_value(serde_json::json!({"socket_path":path,"max_connections":4,"subscriber_queue_bytes":1048576})).unwrap();
    let server = UnixStreamingServer::bind(rig.router.clone(), config)
        .await
        .unwrap();
    let healthy = MarketForgeStreamClient::connect_unix(&path, ClientOptions::default())
        .await
        .unwrap();
    let mut sub = healthy.subscribe("a").await.unwrap();
    sub.recv().await.unwrap();
    let slow = MarketForgeStreamClient::connect_unix(
        &path,
        ClientOptions {
            subscription_queue_messages: 4,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut unread = slow.subscribe("a").await.unwrap();
    unread.recv().await.unwrap();
    let mut times = vec![];
    let mut peak = 0;
    let mut slow_peak = 0;
    let start = Instant::now();
    let mut encoded_bytes = 0;
    for i in 1..=200 {
        let now = Instant::now();
        publish(&rig.producers[0], publication("a", i, 100)).await;
        loop {
            let message = bounded(sub.recv()).await.unwrap().unwrap();
            peak = peak.max(sub.queued_payload_bytes());
            slow_peak = slow_peak.max(unread.queued_payload_bytes());
            if let StreamMessage::Publication(p) = message {
                let mut e = Envelope::control(0, Message::Stream(StreamMessage::Publication(p)));
                e.subscription_id = 1;
                e.subscription_sequence = 1;
                encoded_bytes += encode(&e, 1048576).unwrap().len();
                break;
            }
        }
        times.push(now.elapsed().as_nanos());
    }
    let elapsed = start.elapsed().as_secs_f64();
    println!(
        "unix sequential trades publications/s={:.0} records/s={:.0} market_encoded_bytes/s={:.0} publish_to_recv_ns={:?} sampled_healthy_queued_payload_bytes={peak} sampled_slow_queued_payload_bytes={slow_peak}",
        200. / elapsed,
        200. / elapsed,
        encoded_bytes as f64 / elapsed,
        percentiles(times)
    );
    drop(unread);
    drop(slow);
    sub.close().await.unwrap();
    healthy.close().await.unwrap();
    server.shutdown().await.unwrap();
    assert_eq!(rig.router.status().subscriptions, 0);
    rig.close().await;
}
