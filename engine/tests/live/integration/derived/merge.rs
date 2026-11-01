use super::harness::*;
async fn receive(sub: &mut SubscriptionHandle, handle: &ProcessingServiceHandle) -> DerivedEvent {
    match bounded(sub.recv()).await.unwrap() {
        RouterDelivery::Publication { publication, .. } => match publication.as_ref() {
            RouterPublication::Derived(events) => {
                assert_eq!(events.len(), 1);
                events[0].clone()
            }
            other => panic!("unexpected {other:?}"),
        },
        other => panic!("{other:?}; status {:?}", handle.status()),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arrival_merge_uses_admission_order_without_waiting_for_idle_input() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b", "c"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "m").await;
    let stream = bounded(handle.start_derived("m")).await.unwrap();
    let mut previous = 0;
    for n in 1..=6 {
        let rank = (n as usize) % 2;
        let id = ["a", "b"][rank];
        publish(&rig.producers[rank], publication(id, n, 100 - n as i64)).await;
        let e = receive(&mut sub, &handle).await;
        assert_eq!(e.origin.stream_id, id);
        assert_eq!(
            e.origin.publication_sequence,
            if n % 2 == 1 { n.div_ceil(2) } else { n / 2 }
        );
        assert!(e.admission_ordinal > previous);
        previous = e.admission_ordinal;
        assert!(matches!(e.native,NativeMarketEvent::Trade(t) if t.sequence==Some(n)));
    }
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("m", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn event_time_waits_for_all_frontiers_and_orders_ties_by_declared_rank() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b", "c"],
            &[],
            Some(MergeOrdering::EventTime),
        )],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "m").await;
    let stream = bounded(handle.start_derived("m")).await.unwrap();
    publish(&rig.producers[2], publication("c", 1, 100)).await;
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    bounded(async {
        while handle.status().associations[0]
            .derived
            .retained_publications
            < 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(handle.status().associations[0].router_accepted_outputs, 0);
    publish(&rig.producers[1], publication("b", 1, 90)).await;
    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        publish(&rig.producers[i], publication(id, 2, 200)).await;
    }
    let mut order = vec![];
    for _ in 0..3 {
        let e = receive(&mut sub, &handle).await;
        order.push((e.native.timestamp_ns().unwrap(), e.origin.stream_id));
    }
    assert_eq!(
        order,
        vec![(90, "b".into()), (100, "a".into()), (100, "c".into())]
    );
    bounded(handle.stop(&stream)).await.unwrap();
    let report = handle.take_report("m", stream.generation).unwrap();
    assert_eq!(report.processor_retained.len(), 3);
    assert!(handle.status().accounted_events >= 3);
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequence_timestamp_conflict_fails_and_retains_unresolved_publication() {
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
    let handle = rig.service.handle();
    let stream = bounded(handle.start_derived("m")).await.unwrap();
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    publish(&rig.producers[0], publication("a", 2, 90)).await;
    let terminal = bounded(handle.wait_terminal("m", stream.generation))
        .await
        .unwrap();
    assert!(
        terminal
            .reason
            .as_ref()
            .unwrap()
            .contains("sequence conflict"),
        "{terminal:?}"
    );
    let report = handle.take_report("m", stream.generation).unwrap();
    assert_eq!(report.processor_retained.len(), 2);
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stalled_frontier_fails_on_monotonic_deadline_and_keeps_work() {
    let defs = vec![
        source::definition("binance_spot", SourceChannel::Trades, "a"),
        source::definition("binance_spot", SourceChannel::Trades, "b"),
        derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::EventTime),
        ),
    ];
    let rig = Rig::configured(
        defs,
        RouterConfig::default(),
        general::DerivedProcessingConfig {
            stall_timeout_ms: 100,
            ..Default::default()
        },
    );
    let handle = rig.service.handle();
    let stream = bounded(handle.start_derived("m")).await.unwrap();
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    let terminal = bounded(handle.wait_terminal("m", stream.generation))
        .await
        .unwrap();
    assert!(terminal.reason.unwrap().contains("stalled"));
    let report = handle.take_report("m", stream.generation).unwrap();
    assert_eq!(report.processor_retained.len(), 1);
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reorder_count_limit_retains_whole_partially_traversable_publication() {
    let defs = vec![
        source::definition("binance_spot", SourceChannel::Trades, "a"),
        source::definition("binance_spot", SourceChannel::Trades, "b"),
        derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::EventTime),
        ),
    ];
    let rig = Rig::configured(
        defs,
        RouterConfig::default(),
        general::DerivedProcessingConfig {
            state_events: 2,
            ..Default::default()
        },
    );
    let handle = rig.service.handle();
    let stream = bounded(handle.start_derived("m")).await.unwrap();
    let records = (1..=3)
        .map(|n| {
            let RouterPublication::Trades { records, .. } = publication("a", n, n as i64) else {
                panic!()
            };
            records[0].clone()
        })
        .collect();
    publish(&rig.producers[0], RouterPublication::trades(records)).await;
    let terminal = bounded(handle.wait_terminal("m", stream.generation))
        .await
        .unwrap();
    assert!(terminal.reason.unwrap().contains("capacity"));
    let report = handle.take_report("m", stream.generation).unwrap();
    assert_eq!(report.processor_retained.len(), 1);
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
pub fn depth(snapshot: bool, time: Option<i64>, price: i64, quantity: i64) -> RouterPublication {
    use marketforge_engine::{
        live::{infrastructure::normalization::DepthTimestampProvenance, integrity::*},
        shared::domain::{book::BookLevel, canonical::BookSide},
    };
    let baseline = DepthSequenceBaseline {
        last_sequence: if snapshot { 1 } else { 2 },
        cross_sequence: None,
        synchronized: true,
    };
    let timestamp = DepthAcceptedTimestamp {
        latest_source: time.map(|timestamp_ns| DepthSourceTimestamp {
            timestamp_ns,
            provenance: DepthTimestampProvenance::Exchange,
        }),
        composition: DepthTimestampComposition::SingleMessage,
    };
    let levels = vec![(
        BookSide::Bid,
        price.into(),
        BookLevel {
            quantity_base: Some(quantity.into()),
            quantity_quote: None,
            quantity_contracts: None,
            order_count: None,
        },
    )]
    .into();
    RouterPublication::Depth(
        vec![if snapshot {
            RouterDepthEvent::Snapshot {
                stream_generation: 1,
                book_generation: 1,
                baseline,
                levels,
                timestamp,
            }
        } else {
            RouterDepthEvent::Delta {
                stream_generation: 1,
                book_generation: 1,
                baseline,
                changes: levels,
                timestamp,
            }
        }]
        .into(),
    )
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_merge_preserves_atomic_depth_and_requires_all_baselines() {
    let defs = vec![
        source::definition("binance_spot", SourceChannel::Trades, "a"),
        source::definition("bybit_spot", SourceChannel::Depth, "b"),
        derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        ),
    ];
    let rig = Rig::definitions(defs, RouterConfig::default());
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "m").await;
    let starter = handle.clone();
    let start = tokio::spawn(async move { starter.start_derived("m").await });
    bounded(async {
        while handle.status().associations.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(
        handle.status().associations[0].phase,
        ProcessingPhase::Starting
    );
    publish(&rig.producers[1], depth(true, None, 10, 2)).await;
    let stream = bounded(start).await.unwrap().unwrap();
    let snapshot = receive(&mut sub, &handle).await;
    assert!(
        matches!(snapshot.native,NativeMarketEvent::Depth(RouterDepthEvent::Snapshot{levels,..}) if levels.len()==1)
    );
    assert_eq!(snapshot.origin.stream_id, "b");
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    let trade = receive(&mut sub, &handle).await;
    assert_eq!(trade.origin.stream_id, "a");
    publish(&rig.producers[1], depth(false, Some(110), 10, 0)).await;
    let delta = receive(&mut sub, &handle).await;
    assert!(
        matches!(delta.native,NativeMarketEvent::Depth(RouterDepthEvent::Delta{changes,book_generation:1,..}) if changes[0].1==10.into() && changes[0].2.quantity_base==Some(0.into()))
    );
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("m", stream.generation)
        .unwrap()
        .discard();
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ordered_depth_delta_without_exchange_time_fails_after_native_baseline() {
    let defs = vec![
        source::definition("binance_spot", SourceChannel::Trades, "a"),
        source::definition("bybit_spot", SourceChannel::Depth, "b"),
        derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::EventTime),
        ),
    ];
    let rig = Rig::definitions(defs, RouterConfig::default());
    let handle = rig.service.handle();
    let starting = handle.clone();
    let task = tokio::spawn(async move { starting.start_derived("m").await });
    bounded(async {
        while handle.status().associations.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[1], depth(true, None, 10, 2)).await;
    let stream = bounded(task).await.unwrap().unwrap();
    bounded(async {
        while handle.status().associations[0].router_accepted_outputs != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[1], depth(false, None, 10, 0)).await;
    let terminal = bounded(handle.wait_terminal("m", stream.generation))
        .await
        .unwrap();
    assert!(
        terminal
            .reason
            .unwrap()
            .contains("missing exchange timestamp")
    );
    let report = handle.take_report("m", stream.generation).unwrap();
    assert_eq!(report.processor_retained.len(), 1);
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_arrival_input_cannot_violate_outer_event_time_lateness_contract() {
    let rig = Rig::new(
        vec![
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "b"],
                &[],
                Some(MergeOrdering::ArrivalOrder),
            ),
            derived(
                "o",
                DerivedOperation::Merge,
                &["m", "c"],
                &[],
                Some(MergeOrdering::EventTime),
            ),
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let m = bounded(handle.start_derived("m")).await.unwrap();
    let o = bounded(handle.start_derived("o")).await.unwrap();
    publish(&rig.producers[0], publication("a", 1, 200)).await;
    publish(&rig.producers[0], publication("a", 2, 300)).await;
    publish(&rig.producers[2], publication("c", 1, 500)).await;
    bounded(async {
        while handle
            .status()
            .associations
            .iter()
            .find(|a| a.stream_id == "o")
            .unwrap()
            .router_accepted_outputs
            < 1
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[1], publication("b", 1, 100)).await;
    let terminal = bounded(handle.wait_terminal("o", o.generation))
        .await
        .unwrap();
    assert!(terminal.reason.unwrap().contains("late event"));
    handle.take_report("o", o.generation).unwrap().discard();
    bounded(handle.stop(&m)).await.unwrap();
    handle.take_report("m", m.generation).unwrap().discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn depth_only_event_time_merge_keeps_book_boundaries_and_declared_ties() {
    let defs = vec![
        source::definition("binance_spot", SourceChannel::Depth, "a"),
        source::definition("bybit_spot", SourceChannel::Depth, "b"),
        derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::EventTime),
        ),
    ];
    let rig = Rig::definitions(defs, RouterConfig::default());
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "m").await;
    let starting = handle.clone();
    let task = tokio::spawn(async move { starting.start_derived("m").await });
    bounded(async {
        while handle.status().associations.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[1], depth(true, Some(100), 20, 2)).await;
    assert_eq!(
        handle.status().associations[0].phase,
        ProcessingPhase::Starting
    );
    publish(&rig.producers[0], depth(true, Some(100), 10, 2)).await;
    let stream = bounded(task).await.unwrap().unwrap();
    for i in 0..2 {
        publish(&rig.producers[i], depth(false, Some(200), 10, 0)).await;
    }
    for id in ["a", "b"] {
        let e = receive(&mut sub, &handle).await;
        assert_eq!(e.origin.stream_id, id);
        assert!(
            matches!(e.native,NativeMarketEvent::Depth(RouterDepthEvent::Snapshot{levels,book_generation:1,..}) if levels.len()==1)
        );
    }
    assert!(
        handle.status().associations[0]
            .derived
            .worker_thread
            .as_ref()
            .unwrap()
            .starts_with("marketforge-cpu-")
    );
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("m", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn later_record_in_derived_publication_cannot_cross_emitted_frontier() {
    let defs = vec![
        source::definition("binance_spot", SourceChannel::Trades, "a"),
        source::definition("binance_spot", SourceChannel::Trades, "b"),
        source::definition("binance_spot", SourceChannel::Trades, "c"),
        derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        ),
        derived(
            "o",
            DerivedOperation::Merge,
            &["m", "c"],
            &[],
            Some(MergeOrdering::EventTime),
        ),
    ];
    let rig = Rig::configured(
        defs,
        RouterConfig::default(),
        general::DerivedProcessingConfig {
            stall_timeout_ms: 100,
            ..Default::default()
        },
    );
    let handle = rig.service.handle();
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
    let stream = bounded(handle.start_derived("o")).await.unwrap();
    let mut source_a = subscribe(&rig.router, "a").await;
    let mut source_b = subscribe(&rig.router, "b").await;
    publish(&rig.producers[0], publication("a", 1, 200)).await;
    let a = DerivedEvent::from_delivery(&bounded(source_a.recv()).await.unwrap(), 0, 1, 0).unwrap();
    publish(&rig.producers[1], publication("b", 1, 195)).await;
    let b = DerivedEvent::from_delivery(&bounded(source_b.recv()).await.unwrap(), 1, 2, 0).unwrap();
    publish(&rig.producers[0], publication("a", 2, 300)).await;
    let progress =
        DerivedEvent::from_delivery(&bounded(source_a.recv()).await.unwrap(), 0, 3, 0).unwrap();
    publish(&producer, RouterPublication::Derived(vec![a, b].into())).await;
    publish(&producer, RouterPublication::Derived(vec![progress].into())).await;
    publish(&rig.producers[2], publication("c", 1, 500)).await;
    let terminal = bounded(handle.wait_terminal("o", stream.generation))
        .await
        .unwrap();
    assert!(
        terminal
            .reason
            .as_ref()
            .unwrap()
            .contains("emission frontier"),
        "{terminal:?}"
    );
    let report = handle.take_report("o", stream.generation).unwrap();
    assert_eq!(
        report.router_accepted_outputs + report.unpublished.len() as u64,
        1
    );
    assert!(report.processor_retained.iter().any(|p|p.next_record_ordinal==1 && matches!(&p.batch[0].delivery,RouterDelivery::Publication{publication,..} if matches!(publication.as_ref(),RouterPublication::Derived(events) if events.len()==2))));
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    drop(source_a);
    drop(source_b);
    drop(producer);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn arrival_merge_preserves_native_instruments_across_spot_linear_and_inverse_markets() {
    use marketforge_engine::shared::domain::canonical::Exchange;
    let definitions = vec![
        source::definition("binance_spot", SourceChannel::Trades, "a"),
        source::definition("bybit_linear", SourceChannel::Trades, "b"),
        source::definition("okx_inverse", SourceChannel::Trades, "c"),
        derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b", "c"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        ),
    ];
    let rig = Rig::definitions(definitions, RouterConfig::default());
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "m").await;
    let stream = bounded(handle.start_derived("m")).await.unwrap();
    for (rank, id) in ["a", "b", "c"].iter().enumerate() {
        let graph = rig.router.graph();
        let StreamDefinition::Source(metadata) = &graph.get(id).unwrap().definition else {
            panic!()
        };
        let mut native = source::trade(1);
        native.envelope.stream_id = id.to_string();
        native.envelope.instrument_id = metadata.instrument_id;
        native.envelope.symbol = metadata.symbol.clone();
        native.envelope.exchange = [Exchange::Binance, Exchange::Bybit, Exchange::Okx][rank];
        native.quantity_base = None;
        native.quantity_quote = None;
        native.quantity_contracts = None;
        match metadata.quantity_type {
            QuantityType::Base => native.quantity_base = Some(2.into()),
            QuantityType::Quote => native.quantity_quote = Some(3.into()),
            QuantityType::Contracts => native.quantity_contracts = Some(9.into()),
        }
        publish(
            &rig.producers[rank],
            RouterPublication::trades(vec![native.clone()]),
        )
        .await;
        let output = receive(&mut sub, &handle).await;
        assert_eq!(output.origin.stream_id, *id);
        assert_eq!(output.input_position, rank);
        assert!(output.valuation.is_none());
        let NativeMarketEvent::Trade(actual) = output.native else {
            panic!()
        };
        assert_eq!(actual, native);
    }
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("m", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
