use super::harness::*;

async fn attempt(
    producer: &ProducerHandle,
    publication: RouterPublication,
) -> Result<RouterAcceptance, PublishError> {
    bounded(async {
        loop {
            match producer.try_publish(publication.clone()) {
                Err(PublishError::Busy) => tokio::task::yield_now().await,
                result => break result,
            }
        }
    })
    .await
}

async fn healthy_idle(operation: DerivedOperation, ordering: Option<MergeOrdering>) {
    let inputs = if operation == DerivedOperation::Merge {
        vec!["a", "b"]
    } else {
        vec!["a"]
    };
    let defs = vec![
        source::definition("binance_spot", SourceChannel::Trades, "a"),
        source::definition("binance_spot", SourceChannel::Trades, "b"),
        derived("idle", operation, &inputs, &[], ordering),
    ];
    let rig = Rig::configured(
        defs,
        RouterConfig::default(),
        general::DerivedProcessingConfig {
            stall_timeout_ms: 50,
            ..Default::default()
        },
    );
    let handle = rig.service.handle();
    let mut subscriber = subscribe(&rig.router, "idle").await;
    let stream = bounded(handle.start_derived("idle")).await.unwrap();
    // This is an observation deadline, not a sleep that assumes processing completed.
    assert!(
        tokio::time::timeout(
            Duration::from_millis(150),
            handle.wait_terminal("idle", stream.generation)
        )
        .await
        .is_err(),
        "healthy idle processor reached terminal state: {:?}",
        handle.status()
    );
    assert_eq!(
        handle.status().associations[0].phase,
        ProcessingPhase::Ready
    );
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    if ordering == Some(MergeOrdering::EventTime) {
        publish(&rig.producers[1], publication("b", 1, 100)).await;
        publish(&rig.producers[0], publication("a", 2, 200)).await;
        publish(&rig.producers[1], publication("b", 2, 200)).await;
    }
    assert!(
        matches!(
            bounded(subscriber.recv()).await,
            Some(RouterDelivery::Publication { .. })
        ),
        "{:?}",
        handle.status()
    );
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("idle", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_arrival_merge_survives_computation_timeout() {
    healthy_idle(DerivedOperation::Merge, Some(MergeOrdering::ArrivalOrder)).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_identity_conversion_survives_computation_timeout() {
    healthy_idle(DerivedOperation::Convert, None).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_event_time_merge_survives_computation_timeout() {
    healthy_idle(DerivedOperation::Merge, Some(MergeOrdering::EventTime)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn derived_provenance_rejects_forged_fields_and_allows_delayed_source_sequence() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        RouterConfig::default(),
    );
    let mut input = subscribe(&rig.router, "a").await;
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    let delivery = bounded(input.recv()).await.unwrap();
    let valid = DerivedEvent::from_delivery(&delivery, 0, 1, 0).unwrap();
    let (authority, wrong_generation) = {
        let mut r = rig.registry.lock().unwrap();
        let wrong = r.begin_stop("c").unwrap();
        let g = r.begin_start("m").unwrap();
        r.mark_running("m", g).unwrap();
        (r.publication_authority("m").unwrap(), wrong)
    };
    let producer = rig
        .router
        .attach_processing_producer(authority, &rig.router.processing_inputs("m").unwrap())
        .unwrap();
    publish(&rig.producers[0], publication("a", 2, 200)).await;
    let before = rig.router.status().accepted;
    for case in 0..15 {
        let mut event = valid.clone();
        match case {
            0 => event.upstream.stream_id = "c".into(),
            1 => event.input_position = 1,
            2 => event.upstream.generation = wrong_generation,
            3 => event.origin.generation = wrong_generation,
            4 => event.origin.publication_sequence = 3,
            5 => event.upstream.publication_sequence = 3,
            6 => event.origin.record_ordinal = 1,
            7 => event.upstream.record_ordinal = 1,
            9..=14 => {
                let NativeMarketEvent::Trade(t) = &mut event.native else {
                    panic!()
                };
                match case {
                    9 => t.price += rust_decimal::Decimal::ONE,
                    10 => t.side = marketforge_engine::shared::domain::canonical::TradeSide::Sell,
                    11 => t.quantity_base = Some(999.into()),
                    12 => t.quantity_quote = Some(999.into()),
                    13 => t.quantity_contracts = Some(999.into()),
                    _ => t.price.rescale(2),
                }
            }
            _ => {
                let NativeMarketEvent::Trade(t) = &mut event.native else {
                    panic!()
                };
                t.envelope.instrument_id += 1;
            }
        }
        let result = attempt(&producer, RouterPublication::Derived(vec![event].into())).await;
        assert!(
            matches!(
                result,
                Err(PublishError::InvalidPublication | PublishError::Revoked)
            ),
            "forged provenance case {case}: {result:?}"
        );
        assert_eq!(rig.router.status().accepted, before);
    }
    assert_eq!(
        attempt(&producer, RouterPublication::Derived(vec![valid].into()))
            .await
            .unwrap()
            .sequence,
        1
    );
    drop(input);
    drop(producer);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn late_mixed_subscriber_requires_two_books_but_can_acquire_snapshots() {
    let defs = vec![
        source::definition("binance_spot", SourceChannel::Trades, "a"),
        source::definition("bybit_spot", SourceChannel::Depth, "b"),
        source::definition("binance_spot", SourceChannel::Depth, "c"),
        derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b", "c"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        ),
        derived("v", DerivedOperation::Convert, &["m"], &[], None),
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
    publish(&rig.producers[1], super::merge::depth(true, None, 10, 2)).await;
    publish(&rig.producers[2], super::merge::depth(true, None, 20, 2)).await;
    let m = bounded(task).await.unwrap().unwrap();
    bounded(async {
        while handle.status().associations[0].router_accepted_outputs < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let mut late = subscribe(&rig.router, "m").await;
    let starting = handle.clone();
    let child = tokio::spawn(async move { starting.start_derived("v").await });
    bounded(async {
        while handle.status().associations.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    assert!(matches!(
        bounded(late.recv()).await,
        Some(RouterDelivery::Publication {
            synchronized: false,
            event_valid: true,
            ..
        })
    ));
    assert!(!late.status().synchronized);
    publish(&rig.producers[1], super::merge::depth(true, None, 10, 2)).await;
    assert!(matches!(
        bounded(late.recv()).await,
        Some(RouterDelivery::Publication {
            synchronized: false,
            event_valid: true,
            ..
        })
    ));
    assert!(!late.status().synchronized);
    let mut missing_b = rig
        .router
        .subscribe(SubscribeRequest {
            stream_id: "m".into(),
            owner: source::owner("missing-b"),
        })
        .await
        .unwrap();
    publish(
        &rig.producers[1],
        super::merge::depth(false, Some(200), 10, 0),
    )
    .await;
    assert!(matches!(
        bounded(late.recv()).await,
        Some(RouterDelivery::Publication {
            synchronized: false,
            event_valid: true,
            ..
        })
    ));
    assert!(matches!(
        bounded(missing_b.recv()).await,
        Some(RouterDelivery::Publication {
            synchronized: false,
            event_valid: false,
            ..
        })
    ));
    assert!(!missing_b.status().synchronized);
    drop(missing_b);
    publish(&rig.producers[2], super::merge::depth(true, None, 20, 2)).await;
    assert!(matches!(
        bounded(late.recv()).await,
        Some(RouterDelivery::Publication {
            synchronized: true,
            ..
        })
    ));
    assert!(late.status().synchronized);
    let v = bounded(child).await.unwrap().unwrap();
    assert_eq!(
        handle
            .status()
            .associations
            .iter()
            .find(|a| a.stream_id == "v")
            .unwrap()
            .phase,
        ProcessingPhase::Ready
    );
    publish(
        &rig.producers[2],
        super::merge::depth(false, Some(210), 20, 0),
    )
    .await;
    assert!(matches!(
        bounded(late.recv()).await,
        Some(RouterDelivery::Publication {
            synchronized: true,
            event_valid: true,
            ..
        })
    ));
    {
        let mut r = rig.registry.lock().unwrap();
        let g = r.status("b").unwrap().generation;
        r.mark_failed("b", g, "required book lost").unwrap();
    }
    bounded(handle.wait_terminal("m", m.generation))
        .await
        .unwrap();
    bounded(handle.wait_terminal("v", v.generation))
        .await
        .unwrap();
    handle.take_report("v", v.generation).unwrap().discard();
    handle.take_report("m", m.generation).unwrap().discard();
    drop(late);
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn production_convert_cannot_publish_canonical_passthrough() {
    let rig = Rig::new(
        vec![derived("v", DerivedOperation::Convert, &["a"], &[], None)],
        RouterConfig::default(),
    );
    let authority = {
        let mut r = rig.registry.lock().unwrap();
        let g = r.begin_start("v").unwrap();
        r.mark_running("v", g).unwrap();
        r.publication_authority("v").unwrap()
    };
    let producer = rig
        .router
        .attach_processing_producer(authority, &rig.router.processing_inputs("v").unwrap())
        .unwrap();
    assert_eq!(
        attempt(&producer, publication("a", 1, 100)).await,
        Err(PublishError::InvalidPublication)
    );
    drop(producer);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn derived_depth_provenance_rejection_does_not_commit_partial_book_or_sequence() {
    let rig = Rig::definitions(
        vec![
            source::definition("binance_spot", SourceChannel::Trades, "a"),
            source::definition("bybit_spot", SourceChannel::Depth, "b"),
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "b"],
                &[],
                Some(MergeOrdering::ArrivalOrder),
            ),
        ],
        RouterConfig::default(),
    );
    let mut input = subscribe(&rig.router, "b").await;
    publish(&rig.producers[1], super::merge::depth(true, None, 10, 2)).await;
    let snapshot =
        DerivedEvent::from_delivery(&bounded(input.recv()).await.unwrap(), 1, 1, 0).unwrap();
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
    publish(
        &rig.producers[1],
        super::merge::depth(false, Some(100), 10, 0),
    )
    .await;
    let delta =
        DerivedEvent::from_delivery(&bounded(input.recv()).await.unwrap(), 1, 2, 0).unwrap();
    for case in 0..5 {
        let mut forged = delta.clone();
        let NativeMarketEvent::Depth(RouterDepthEvent::Delta {
            stream_generation,
            book_generation,
            baseline,
            changes,
            ..
        }) = &mut forged.native
        else {
            panic!()
        };
        match case {
            0 => *stream_generation += 1,
            1 => *book_generation += 1,
            2 => baseline.last_sequence += 1,
            3 => baseline.cross_sequence = Some(9),
            _ => {
                let mut altered = changes.to_vec();
                altered[0].1 += rust_decimal::Decimal::ONE;
                *changes = altered.into();
            }
        };
        // A valid prefix must not establish a book when a later record is invalid.
        assert_eq!(
            attempt(
                &producer,
                RouterPublication::Derived(vec![snapshot.clone(), forged].into())
            )
            .await,
            Err(PublishError::InvalidPublication)
        );
        assert_eq!(
            attempt(
                &producer,
                RouterPublication::Derived(vec![delta.clone()].into())
            )
            .await,
            Err(PublishError::InvalidPublication)
        );
    }
    assert_eq!(
        attempt(&producer, RouterPublication::Derived(vec![snapshot].into()))
            .await
            .unwrap()
            .sequence,
        1
    );
    assert_eq!(
        attempt(
            &producer,
            RouterPublication::Derived(vec![delta.clone()].into())
        )
        .await
        .unwrap()
        .sequence,
        2
    );
    assert_eq!(
        attempt(&producer, RouterPublication::Derived(vec![delta].into())).await,
        Err(PublishError::InvalidPublication)
    );
    // A complete publication may repair a missing baseline after an unusable
    // delta prefix. Overall readiness and whole-publication validity differ.
    let mut late = subscribe(&rig.router, "m").await;
    let mut next_delta = super::merge::depth(false, Some(200), 10, 0);
    let RouterPublication::Depth(events) = &mut next_delta else {
        panic!()
    };
    let RouterDepthEvent::Delta { baseline, .. } = &mut Arc::make_mut(events)[0] else {
        panic!()
    };
    baseline.last_sequence = 3;
    publish(&rig.producers[1], next_delta).await;
    let next_delta =
        DerivedEvent::from_delivery(&bounded(input.recv()).await.unwrap(), 1, 3, 0).unwrap();
    let mut snapshot = super::merge::depth(true, None, 10, 2);
    let RouterPublication::Depth(events) = &mut snapshot else {
        panic!()
    };
    let RouterDepthEvent::Snapshot { baseline, .. } = &mut Arc::make_mut(events)[0] else {
        panic!()
    };
    baseline.last_sequence = 3;
    publish(&rig.producers[1], snapshot).await;
    let snapshot =
        DerivedEvent::from_delivery(&bounded(input.recv()).await.unwrap(), 1, 4, 0).unwrap();
    assert_eq!(
        attempt(
            &producer,
            RouterPublication::Derived(vec![next_delta, snapshot].into())
        )
        .await
        .unwrap()
        .sequence,
        3
    );
    assert!(matches!(
        bounded(late.recv()).await,
        Some(RouterDelivery::Publication {
            synchronized: true,
            event_valid: false,
            ..
        })
    ));
    assert!(late.status().synchronized);
    drop(late);
    drop(input);
    drop(producer);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoked_required_dependency_rejects_already_calculated_output_with_live_own_authority() {
    let rig = Rig::new(
        vec![derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::ArrivalOrder),
        )],
        RouterConfig::default(),
    );
    let mut input = subscribe(&rig.router, "a").await;
    publish(&rig.producers[0], publication("a", 1, 100)).await;
    let calculated =
        DerivedEvent::from_delivery(&bounded(input.recv()).await.unwrap(), 0, 1, 0).unwrap();
    let authority = {
        let mut r = rig.registry.lock().unwrap();
        let g = r.begin_start("m").unwrap();
        r.mark_running("m", g).unwrap();
        r.publication_authority("m").unwrap()
    };
    let producer = rig
        .router
        .attach_processing_producer(
            authority.clone(),
            &rig.router.processing_inputs("m").unwrap(),
        )
        .unwrap();
    let before = rig.router.status().accepted;
    let (ready, publication_ready) = tokio::sync::oneshot::channel();
    let (resume, revoked) = tokio::sync::oneshot::channel();
    let publisher = tokio::spawn(async move {
        ready.send(()).unwrap();
        revoked.await.unwrap();
        let result = attempt(
            &producer,
            RouterPublication::Derived(vec![calculated].into()),
        )
        .await;
        drop(producer);
        result
    });
    bounded(publication_ready).await.unwrap();
    rig.registry.lock().unwrap().begin_stop("b").unwrap();
    assert_eq!(
        rig.registry.lock().unwrap().status("m").unwrap().lifecycle,
        StreamLifecycleState::Running,
        "downstream has not yet been fenced by service"
    );
    resume.send(()).unwrap();
    assert_eq!(
        bounded(publisher).await.unwrap(),
        Err(PublishError::Revoked)
    );
    assert_eq!(rig.router.status().accepted, before);
    drop(input);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canonical_compatibility_is_explicit_and_cannot_supply_typed_conversion() {
    let rig = Rig::new(
        vec![
            derived("legacy", DerivedOperation::Convert, &["a"], &[], None),
            derived("v", DerivedOperation::Convert, &["legacy"], &[], None),
        ],
        RouterConfig::default(),
    );
    let (authority, child) = {
        let mut r = rig.registry.lock().unwrap();
        for id in ["legacy", "v"] {
            let g = r.begin_start(id).unwrap();
            r.mark_running(id, g).unwrap();
        }
        (
            r.publication_authority("legacy").unwrap(),
            r.publication_authority("v").unwrap(),
        )
    };
    let producer = rig.router.attach_derived_producer(authority, "a").unwrap();
    assert_eq!(
        producer.semantics(),
        PublicationSemantics::CanonicalCompatibility
    );
    let mut sub = subscribe(&rig.router, "legacy").await;
    assert_eq!(
        attempt(&producer, publication("a", 1, 100))
            .await
            .unwrap()
            .sequence,
        1
    );
    let delivery = bounded(sub.recv()).await.unwrap();
    let RouterDelivery::Publication { admission, .. } = &delivery else {
        panic!()
    };
    assert_eq!(
        admission.semantics(),
        PublicationSemantics::CanonicalCompatibility
    );
    assert!(DerivedEvent::from_delivery(&delivery, 0, 1, 0).is_none());
    let typed = rig
        .router
        .attach_processing_producer(child, &rig.router.processing_inputs("v").unwrap())
        .unwrap();
    assert_eq!(
        attempt(&typed, publication("a", 2, 200)).await,
        Err(PublishError::InvalidPublication)
    );
    drop(typed);
    drop(sub);
    drop(producer);
    rig.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_book_invalidation_fences_merge_and_conversion_chain() {
    let rig = Rig::definitions(
        vec![
            source::definition("binance_spot", SourceChannel::Trades, "a"),
            source::definition("bybit_spot", SourceChannel::Depth, "b"),
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
    let handle = rig.service.handle();
    let starter = handle.clone();
    let task = tokio::spawn(async move { starter.start_derived("m").await });
    bounded(async {
        while handle.status().associations.is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[1], super::merge::depth(true, None, 10, 2)).await;
    let m = bounded(task).await.unwrap().unwrap();
    let starter = handle.clone();
    let task = tokio::spawn(async move { starter.start_derived("v").await });
    bounded(async {
        while handle.status().associations.len() < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    publish(&rig.producers[1], super::merge::depth(true, None, 10, 2)).await;
    let v = bounded(task).await.unwrap().unwrap();
    publish(
        &rig.producers[1],
        RouterPublication::Depth(
            vec![RouterDepthEvent::Invalidated {
                stream_generation: 1,
                book_generation: 1,
            }]
            .into(),
        ),
    )
    .await;
    for (id, stream) in [("m", m), ("v", v)] {
        let terminal = bounded(handle.wait_terminal(id, stream.generation))
            .await
            .unwrap();
        assert!(terminal.reason.is_some(), "{terminal:?}");
        handle.take_report(id, stream.generation).unwrap().discard();
    }
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}

#[test]
fn persisted_merge_conversion_request_is_rejected_after_json_roundtrip_and_graph_restore() {
    let mut invalid = derived(
        "m",
        DerivedOperation::Merge,
        &["a", "b"],
        &[],
        Some(MergeOrdering::ArrivalOrder),
    );
    let StreamDefinition::Derived(request) = &mut invalid.definition else {
        panic!()
    };
    request.target_quote_asset = Some("USDT".into());
    let restored_request: CreateDerivedStreamRequest =
        serde_json::from_str(&serde_json::to_string(request).unwrap()).unwrap();
    let restored = PersistedStreamDefinition {
        definition: StreamDefinition::Derived(restored_request),
        ..invalid
    };
    assert!(
        restored
            .definition
            .validate()
            .unwrap_err()
            .contains("target_quote_asset")
    );
    let result = StreamGraph::restore(
        vec![
            source::definition("binance_spot", SourceChannel::Trades, "a"),
            source::definition("bybit_spot", SourceChannel::Trades, "b"),
            restored,
        ],
        vec![
            StreamDependency {
                derived_stream_id: "m".into(),
                input_stream_id: "a".into(),
                input_position: 0,
            },
            StreamDependency {
                derived_stream_id: "m".into(),
                input_stream_id: "b".into(),
                input_position: 1,
            },
        ],
    );
    assert!(
        matches!(result,Err(StreamGraphError::InvalidDefinition(id,reason)) if id=="m" && reason.contains("target_quote_asset"))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn short_data_stall_budget_does_not_expire_pending_startup() {
    struct Idle(ProcessorContext);
    impl Processor for Idle {
        fn run_turn(&mut self, _: usize) -> bool {
            self.0.input.take_work(1).complete();
            false
        }
    }
    let rig = Rig::configured(
        vec![
            source::definition("binance_spot", SourceChannel::Trades, "a"),
            source::definition("binance_spot", SourceChannel::Trades, "b"),
            derived(
                "m",
                DerivedOperation::Merge,
                &["a", "b"],
                &[],
                Some(MergeOrdering::ArrivalOrder),
            ),
        ],
        RouterConfig::default(),
        general::DerivedProcessingConfig {
            stall_timeout_ms: 10,
            startup_timeout_ms: 5000,
            ..Default::default()
        },
    );
    let handle = rig.service.handle();
    let (entered, constructed) = tokio::sync::oneshot::channel();
    let (release, ready) = tokio::sync::oneshot::channel();
    let spec = rig.spec("m", move |context| {
        entered.send(()).unwrap();
        Ok(PreparedProcessor {
            processor: Box::new(Idle(context)),
            readiness: Box::pin(async move { ready.await.map_err(|e| e.to_string()) }),
        })
    });
    let starter = handle.clone();
    let mut start = tokio::spawn(async move { starter.start(spec).await });
    bounded(constructed).await.unwrap();
    // Keep readiness explicitly gated beyond the data-stall budget. No timing
    // assumption about construction or scheduler progress establishes readiness.
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut start)
            .await
            .is_err()
    );
    release.send(()).unwrap();
    let stream = bounded(start).await.unwrap().unwrap();
    bounded(handle.stop(&stream)).await.unwrap();
    handle
        .take_report("m", stream.generation)
        .unwrap()
        .discard();
    assert_eq!(rig.router.status().subscriptions, 0);
    rig.close().await;
}
