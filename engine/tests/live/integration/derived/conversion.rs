use super::harness::*;
use marketforge_engine::shared::conversion::{
    ConversionDecimal, evaluation::*, planner::*, reference::*,
};
fn source_def(id: &str, base: &str, quote: &str, index: i64) -> PersistedStreamDefinition {
    let mut d = source::definition("binance_spot", SourceChannel::Trades, id);
    d.recording_stream_key = Some(index);
    let StreamDefinition::Source(r) = &mut d.definition else {
        panic!()
    };
    r.base_asset = Some(base.into());
    r.quote_asset = Some(quote.into());
    d
}
fn priced(id: &str, sequence: u64, time: i64, price: i64) -> RouterPublication {
    let RouterPublication::Trades { records, .. } = publication(id, sequence, time) else {
        panic!()
    };
    let mut t = records[0].clone();
    t.price = price.into();
    t.quantity_quote = Some((price * 2).into());
    RouterPublication::trades(vec![t])
}
fn decimal(n: i128) -> ConversionDecimal {
    ConversionDecimal::from_ratio(n.into(), 1.into()).unwrap()
}
async fn receive(sub: &mut SubscriptionHandle, handle: &ProcessingServiceHandle) -> DerivedEvent {
    match bounded(sub.recv()).await.unwrap() {
        RouterDelivery::Publication { publication, .. } => match publication.as_ref() {
            RouterPublication::Derived(events) => events[0].clone(),
            other => panic!("{other:?}"),
        },
        other => panic!("{other:?}; status {:?}", handle.status()),
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn identity_conversion_preserves_native_quantities_and_exact_values() {
    let rig = Rig::definitions(
        vec![
            source_def("a", "ETH", "USDT", 1),
            derived("v", DerivedOperation::Convert, &["a"], &[], None),
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "v").await;
    let stream = bounded(handle.start_derived("v")).await.unwrap();
    publish(&rig.producers[0], priced("a", 1, 100, 7)).await;
    let e = receive(&mut sub, &handle).await;
    let v = e.valuation.unwrap();
    assert_eq!(v.evaluation.rate, ConversionDecimal::one());
    assert_eq!(v.values[0].price_target, decimal(7));
    assert_eq!(v.values[0].quantity_quote_target, Some(decimal(14)));
    assert!(v.evaluation.references.is_empty());
    assert!(
        matches!(e.native,NativeMarketEvent::Trade(t) if t.quantity_base==Some(2.into()) && t.price==7.into())
    );
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("v", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_direct_reference_uses_asof_history_and_excludes_equal_time_later_rank() {
    let rig = Rig::definitions(
        vec![
            source_def("a", "ETH", "BTC", 1),
            source_def("b", "BTC", "USDT", 2),
            derived("v", DerivedOperation::Convert, &["a"], &["b"], None),
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "v").await;
    let stream = bounded(handle.start_derived("v")).await.unwrap();
    publish(&rig.producers[0], priced("a", 1, 100, 3)).await;
    publish(&rig.producers[0], priced("a", 2, 300, 4)).await;
    publish(&rig.producers[1], priced("b", 1, 90, 10)).await;
    publish(&rig.producers[1], priced("b", 2, 100, 20)).await;
    publish(&rig.producers[1], priced("b", 3, 200, 30)).await;
    let e = receive(&mut sub, &handle).await;
    let v = e.valuation.unwrap();
    assert_eq!(v.values[0].price_target, decimal(30));
    assert_eq!(v.values[0].quantity_quote_target, Some(decimal(60)));
    assert_eq!(v.evaluation.references.len(), 1);
    assert_eq!(v.evaluation.references[0].key.timestamp_ns, 90);
    assert_eq!(
        v.reference_generations[0].1,
        rig.registry.lock().unwrap().status("b").unwrap().generation
    );
    assert_eq!(e.origin.stream_id, "a");
    bounded(handle.stop(&stream)).await.unwrap();
    let report = handle.take_report("v", stream.generation).unwrap();
    assert!(report.processor_retained.len() >= 2);
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inverse_and_multihop_match_shared_plan_and_evaluation() {
    for inverse in [false, true] {
        let refs = if inverse {
            vec![source_def("b", "USDT", "BTC", 2)]
        } else {
            vec![
                source_def("b", "BTC", "USD", 2),
                source_def("c", "USD", "USDT", 3),
            ]
        };
        let refids = if inverse { vec!["b"] } else { vec!["b", "c"] };
        let mut defs = vec![source_def("a", "ETH", "BTC", 1)];
        defs.extend(refs);
        defs.push(derived(
            "v",
            DerivedOperation::Convert,
            &["a"],
            &refids,
            None,
        ));
        let rig = Rig::definitions(defs, RouterConfig::default());
        let handle = rig.service.handle();
        let mut sub = subscribe(&rig.router, "v").await;
        let stream = bounded(handle.start_derived("v")).await.unwrap();
        for (i, id) in refids.iter().enumerate() {
            publish(
                &rig.producers[i + 1],
                priced(id, 1, 90, if i == 0 { 2 } else { 5 }),
            )
            .await;
            publish(&rig.producers[i + 1], priced(id, 2, 200, 9)).await;
        }
        publish(&rig.producers[0], priced("a", 1, 100, 4)).await;
        publish(&rig.producers[0], priced("a", 2, 300, 4)).await;
        let e = receive(&mut sub, &handle).await;
        let v = e.valuation.unwrap();
        assert_eq!(
            v.values[0].price_target,
            decimal(if inverse { 2 } else { 40 })
        );
        assert_eq!(v.evaluation.references.len(), if inverse { 1 } else { 2 });
        // Re-evaluate the frozen edge list with exactly the output witness keys.
        let graph = rig.router.graph();
        let sources = refids
            .iter()
            .enumerate()
            .map(|(i, id)| {
                let StreamDefinition::Source(s) = &graph.get(id).unwrap().definition else {
                    panic!()
                };
                ResolvedConversionInstrument {
                    dataset_id: (i + 2) as u64,
                    stream_rank: (i + 1) as u32,
                    stream_id: id.to_string(),
                    identity: marketforge_engine::shared::domain::instrument::InstrumentIdentity {
                        exchange: "binance".into(),
                        symbol: s.symbol.clone(),
                        instrument_kind:
                            marketforge_engine::shared::domain::instrument::InstrumentKind::Spot,
                        contract_kind: None,
                    },
                    base_currency: marketforge_engine::shared::domain::instrument::Currency::new(
                        s.base_asset.clone().unwrap(),
                    )
                    .unwrap(),
                    quote_currency: marketforge_engine::shared::domain::instrument::Currency::new(
                        s.quote_asset.clone().unwrap(),
                    )
                    .unwrap(),
                }
            })
            .collect::<Vec<_>>();
        let paths = marketforge_engine::shared::conversion::ConversionGraph::new(&sources)
            .unwrap()
            .find_paths(
                &marketforge_engine::shared::domain::instrument::Currency::new("BTC").unwrap(),
                &marketforge_engine::shared::domain::instrument::Currency::new("USDT").unwrap(),
                3,
            )
            .unwrap();
        let mut state = ReferenceState::new();
        for r in &v.evaluation.references {
            state
                .update(ReferenceObservation {
                    dataset_id: r.dataset_id,
                    stream_id: r.stream_id.clone(),
                    key: r.key,
                    price: r.price,
                })
                .unwrap();
        }
        let witness = evaluate_conversion_path(
            &paths[0],
            &state,
            ReferenceKey::new(100, 0, u64::MAX, 0),
            60_000_000_000,
        )
        .unwrap();
        assert_eq!(witness.rate, v.evaluation.rate);
        assert_eq!(witness.references, v.evaluation.references);
        #[cfg(feature = "historical")]
        {
            let historical =
                marketforge_engine::historical::convert::reference::evaluate_conversion_path(
                    &paths[0],
                    &state,
                    ReferenceKey::new(100, 0, u64::MAX, 0),
                    60_000_000_000,
                )
                .unwrap();
            assert_eq!(historical.references, v.evaluation.references);
            let values = marketforge_engine::historical::convert::convert_trade_values(
                decimal(4),
                Some(decimal(8)),
                &historical,
            )
            .unwrap();
            assert_eq!(values, v.values[0]);
        }
        bounded(handle.stop(&stream)).await.unwrap();
        handle
            .take_report("v", stream.generation)
            .unwrap()
            .discard();
        rig.close().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unavailable_stale_future_and_invalid_reference_fail_with_owned_work() {
    for mode in 0..3 {
        let mut def = derived("v", DerivedOperation::Convert, &["a"], &["b"], None);
        if mode == 0 {
            let StreamDefinition::Derived(r) = &mut def.definition else {
                panic!()
            };
            r.conversion.as_mut().unwrap().max_staleness_ns = 5;
        }
        let rig = Rig::definitions(
            vec![
                source_def("a", "ETH", "BTC", 1),
                source_def("b", "BTC", "USDT", 2),
                def,
            ],
            RouterConfig::default(),
        );
        let handle = rig.service.handle();
        let stream = bounded(handle.start_derived("v")).await.unwrap();
        publish(
            &rig.producers[1],
            priced(
                "b",
                1,
                if mode == 1 { 110 } else { 90 },
                if mode == 2 { 0 } else { 2 },
            ),
        )
        .await;
        publish(&rig.producers[1], priced("b", 2, 200, 3)).await;
        publish(&rig.producers[0], priced("a", 1, 100, 4)).await;
        publish(&rig.producers[0], priced("a", 2, 300, 4)).await;
        let terminal = bounded(handle.wait_terminal("v", stream.generation))
            .await
            .unwrap();
        let reason = terminal.reason.unwrap();
        assert!(
            reason.contains(if mode == 0 {
                "StaleReference"
            } else {
                "MissingReference"
            }),
            "{reason}"
        );
        let report = handle.take_report("v", stream.generation).unwrap();
        assert!(!report.processor_retained.is_empty());
        report.discard();
        assert_eq!(handle.status().accounted_events, 0);
        rig.close().await;
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn source_merge_convert_chain_preserves_depth_deletion_key_across_rate_change() {
    let mut depth = source_def("b", "ETH", "BTC", 2);
    let StreamDefinition::Source(d) = &mut depth.definition else {
        panic!()
    };
    d.channel = SourceChannel::Depth;
    let rig = Rig::definitions(
        vec![
            source_def("a", "ETH", "BTC", 1),
            depth,
            source_def("c", "BTC", "USDT", 3),
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "b"],
                &[],
                Some(MergeOrdering::ArrivalOrder),
            ),
            derived("v", DerivedOperation::Convert, &["m"], &["c"], None),
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "v").await;
    let starting = handle.clone();
    let merge = tokio::spawn(async move { starting.start_derived("m").await });
    bounded(async {
        while handle.status().associations.len() < 1 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let starting = handle.clone();
    let conversion = tokio::spawn(async move { starting.start_derived("v").await });
    bounded(async {
        while handle.status().associations.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[1], super::merge::depth(true, None, 10, 2)).await;
    let m = bounded(merge).await.unwrap().unwrap();
    let v = bounded(conversion).await.unwrap().unwrap();
    let baseline = receive(&mut sub, &handle).await;
    assert!(baseline.valuation.is_none());
    assert!(matches!(
        baseline.native,
        NativeMarketEvent::Depth(RouterDepthEvent::Snapshot { .. })
    ));
    publish(&rig.producers[2], priced("c", 1, 90, 2)).await;
    publish(&rig.producers[2], priced("c", 2, 150, 3)).await;
    publish(&rig.producers[2], priced("c", 3, 300, 4)).await;
    publish(&rig.producers[0], priced("a", 1, 100, 5)).await;
    bounded(async {
        while handle
            .status()
            .associations
            .iter()
            .find(|a| a.stream_id == "m")
            .unwrap()
            .router_accepted_outputs
            < 2
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(
        &rig.producers[1],
        super::merge::depth(false, Some(110), 10, 3),
    )
    .await;
    let mut deletion = super::merge::depth(false, Some(200), 10, 0);
    let RouterPublication::Depth(events) = &mut deletion else {
        panic!()
    };
    let RouterDepthEvent::Delta { baseline, .. } = &mut Arc::make_mut(events)[0] else {
        panic!()
    };
    baseline.last_sequence = 3;
    publish(&rig.producers[1], deletion).await;
    bounded(async {
        while handle
            .status()
            .associations
            .iter()
            .find(|a| a.stream_id == "m")
            .unwrap()
            .router_accepted_outputs
            < 4
        {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[0], priced("a", 2, 400, 5)).await;
    let trade = receive(&mut sub, &handle).await;
    assert_eq!(trade.origin.stream_id, "a");
    assert_eq!(trade.upstream.stream_id, "m");
    assert_eq!(trade.valuation.unwrap().values[0].price_target, decimal(10));
    let insertion = receive(&mut sub, &handle).await;
    assert_eq!(insertion.origin.stream_id, "b");
    assert_eq!(
        insertion.valuation.unwrap().values[0].price_target,
        decimal(20)
    );
    assert!(
        matches!(insertion.native,NativeMarketEvent::Depth(RouterDepthEvent::Delta{changes,book_generation:1,..}) if changes[0].1==10.into() && changes[0].2.quantity_base==Some(3.into()))
    );
    let deletion = receive(&mut sub, &handle).await;
    assert_eq!(deletion.origin.stream_id, "b");
    assert_eq!(deletion.upstream.stream_id, "m");
    let valuation = deletion.valuation.unwrap();
    assert_eq!(valuation.values[0].price_target, decimal(30));
    assert_eq!(valuation.evaluation.references[0].key.timestamp_ns, 150);
    assert!(
        matches!(deletion.native,NativeMarketEvent::Depth(RouterDepthEvent::Delta{changes,book_generation:1,..}) if changes[0].1==10.into() && changes[0].2.quantity_base==Some(0.into()))
    );
    bounded(handle.stop(&v)).await.unwrap();
    handle.take_report("v", v.generation).unwrap().discard();
    bounded(handle.stop(&m)).await.unwrap();
    handle.take_report("m", m.generation).unwrap().discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn exact_arithmetic_failure_publishes_no_partial_valuation() {
    let rig = Rig::definitions(
        vec![
            source_def("a", "ETH", "BTC", 1),
            source_def("b", "BTC", "USDT", 2),
            derived("v", DerivedOperation::Convert, &["a"], &["b"], None),
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let stream = bounded(handle.start_derived("v")).await.unwrap();
    for (i, id) in ["a", "b"].iter().enumerate() {
        let RouterPublication::Trades { records, .. } =
            priced(id, 1, if i == 0 { 100 } else { 90 }, 1)
        else {
            panic!()
        };
        let mut t = records[0].clone();
        t.price = rust_decimal::Decimal::MAX;
        t.quantity_quote = None;
        publish(&rig.producers[i], RouterPublication::trades(vec![t])).await;
        publish(&rig.producers[i], priced(id, 2, 300, 1)).await;
    }
    let terminal = bounded(handle.wait_terminal("v", stream.generation))
        .await
        .unwrap();
    assert!(terminal.reason.unwrap().contains("ArithmeticFailure"));
    let report = handle.take_report("v", stream.generation).unwrap();
    assert_eq!(report.router_accepted_outputs, 0);
    assert!(!report.processor_retained.is_empty());
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_multihop_edge_must_be_fresh() {
    let mut def = derived("v", DerivedOperation::Convert, &["a"], &["b", "c"], None);
    let StreamDefinition::Derived(r) = &mut def.definition else {
        panic!()
    };
    r.conversion.as_mut().unwrap().max_staleness_ns = 20;
    let rig = Rig::definitions(
        vec![
            source_def("a", "ETH", "BTC", 1),
            source_def("b", "BTC", "USD", 2),
            source_def("c", "USD", "USDT", 3),
            def,
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let stream = bounded(handle.start_derived("v")).await.unwrap();
    for (i, id) in ["a", "b", "c"].iter().enumerate() {
        publish(&rig.producers[i], priced(id, 1, [100, 90, 70][i], 2)).await;
        publish(&rig.producers[i], priced(id, 2, 300, 3)).await;
    }
    let terminal = bounded(handle.wait_terminal("v", stream.generation))
        .await
        .unwrap();
    assert!(terminal.reason.unwrap().contains("StaleReference(\"c\")"));
    handle
        .take_report("v", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reference_traffic_cannot_extend_older_data_wait_deadline() {
    let defs = vec![
        source_def("a", "ETH", "BTC", 1),
        source_def("b", "BTC", "USDT", 2),
        derived("v", DerivedOperation::Convert, &["a"], &["b"], None),
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
    let stream = bounded(handle.start_derived("v")).await.unwrap();
    publish(&rig.producers[0], priced("a", 1, 100, 4)).await;
    let feeding = async {
        let mut ticks = tokio::time::interval(Duration::from_millis(5));
        for n in 1..=200 {
            ticks.tick().await;
            publish(&rig.producers[1], priced("b", n, 50, 2)).await;
        }
    };
    tokio::select! {result=handle.wait_terminal("v",stream.generation)=>{assert!(result.unwrap().reason.unwrap().contains("stalled"));},_=feeding=>panic!("reference traffic kept unresolved data alive past its deadline")}
    handle
        .take_report("v", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn depth_arithmetic_failure_after_first_level_cannot_publish_partial_update() {
    let mut data = source_def("a", "ETH", "BTC", 1);
    let StreamDefinition::Source(s) = &mut data.definition else {
        panic!()
    };
    s.channel = SourceChannel::Depth;
    let rig = Rig::definitions(
        vec![
            data,
            source_def("b", "BTC", "USDT", 2),
            derived("v", DerivedOperation::Convert, &["a"], &["b"], None),
        ],
        RouterConfig::default(),
    );
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "v").await;
    let starting = handle.clone();
    let task = tokio::spawn(async move { starting.start_derived("v").await });
    bounded(async {
        while handle.status().associations.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[0], super::merge::depth(true, None, 1, 2)).await;
    let stream = bounded(task).await.unwrap().unwrap();
    assert!(receive(&mut sub, &handle).await.baseline_barrier);
    let RouterPublication::Trades { records, .. } = priced("b", 1, 90, 1) else {
        panic!()
    };
    let mut rate = records[0].clone();
    rate.price = rust_decimal::Decimal::MAX;
    rate.quantity_quote = None;
    publish(&rig.producers[1], RouterPublication::trades(vec![rate])).await;
    publish(&rig.producers[1], priced("b", 2, 200, 1)).await;
    let mut update = super::merge::depth(true, Some(100), 1, 2);
    let RouterPublication::Depth(events) = &mut update else {
        panic!()
    };
    let RouterDepthEvent::Snapshot {
        levels,
        book_generation,
        baseline,
        ..
    } = &mut Arc::make_mut(events)[0]
    else {
        panic!()
    };
    let first = levels[0].clone();
    let mut second = first.clone();
    second.1 = rust_decimal::Decimal::MAX;
    *levels = vec![first, second].into();
    *book_generation = 1;
    baseline.last_sequence = 2;
    publish(&rig.producers[0], update).await;
    let mut progress = super::merge::depth(false, Some(300), 1, 0);
    let RouterPublication::Depth(events) = &mut progress else {
        panic!()
    };
    let RouterDepthEvent::Delta {
        book_generation,
        baseline,
        ..
    } = &mut Arc::make_mut(events)[0]
    else {
        panic!()
    };
    *book_generation = 1;
    baseline.last_sequence = 3;
    publish(&rig.producers[0], progress).await;
    let terminal = bounded(handle.wait_terminal("v", stream.generation))
        .await
        .unwrap();
    assert!(terminal.reason.unwrap().contains("ArithmeticFailure"));
    let report = handle.take_report("v", stream.generation).unwrap();
    assert_eq!(
        report.router_accepted_outputs, 1,
        "only the unvalued baseline entered Router custody"
    );
    assert!(report.unpublished.is_empty());
    assert!(report.processor_retained.iter().any(|p|matches!(&p.batch[0].delivery,RouterDelivery::Publication{publication,..} if matches!(publication.as_ref(),RouterPublication::Depth(events) if matches!(&events[0],RouterDepthEvent::Snapshot{levels,..} if levels.len()==2)))));
    assert!(matches!(
        bounded(sub.recv()).await,
        Some(RouterDelivery::ContinuityLost(_))
    ));
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
