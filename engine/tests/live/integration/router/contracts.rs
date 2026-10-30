use super::harness::*;
use marketforge_engine::{
    live::{
        integrity::{DepthAcceptedTimestamp, DepthSequenceBaseline, DepthTimestampComposition},
        stream::{registry::*, request::SourceChannel, router::*},
    },
    shared::domain::book::BookChange,
};
use std::sync::Arc;

#[tokio::test]
async fn exact_fanout_1000_records_and_failed_consumer_isolation() {
    let mut config = RouterConfig::default();
    config.consumer.publications = 1;
    let rig = Rig::new(SourceChannel::Trades, config);
    let mut a = rig.subscribe("a").await;
    let mut b = rig.subscribe("b").await;
    let mut c = rig.subscribe("c").await;
    for start in (0..1000).step_by(10) {
        let batch = RouterPublication::trades((start..start + 10).map(trade).collect());
        let accepted = rig.producer.try_publish(batch).unwrap();
        let (seq, first) = records(receive(&mut a).await);
        assert_eq!(seq, accepted.sequence);
        for consumer in [&mut b, &mut c] {
            let (other_seq, other) = records(receive(consumer).await);
            assert_eq!(seq, other_seq);
            assert!(Arc::ptr_eq(&first, &other));
        }
        match first.as_ref() {
            RouterPublication::Trades { records, .. } => {
                assert_eq!(records.len(), 10);
                for (i, actual) in records.iter().enumerate() {
                    assert_eq!(actual, &trade(start + i as u64));
                }
            }
            _ => panic!(),
        }
    }
    // Leave C's queue full, then deliberately overflow it while A/B keep up.
    for id in 1000..1002 {
        rig.producer.try_publish(publication(id)).unwrap();
        assert_eq!(records(receive(&mut a).await).0, id - 899);
        assert_eq!(records(receive(&mut b).await).0, id - 899);
    }
    assert!(matches!(
        receive(&mut c).await,
        RouterDelivery::ContinuityLost(ContinuityFailure::ConsumerCapacity)
    ));
    assert_eq!(c.status().queued_bytes, 0);
    assert_eq!(rig.context.demand.lock().unwrap().owner_count("test"), 2);
    assert_eq!(rig.router.status().accepted, 102);
}

#[tokio::test]
async fn atomic_rejection_identity_type_and_ingress_count_and_bytes() {
    let mut config = RouterConfig::default();
    config.ingress.publications = 1;
    let rig = Rig::new(SourceChannel::Trades, config);
    let mut sub = rig.subscribe("one").await;
    let mut bad = trade(2);
    bad.envelope.instrument_id += 1;
    assert_eq!(
        rig.producer
            .try_publish(RouterPublication::trades(vec![trade(1), bad])),
        Err(PublishError::InvalidPublication)
    );
    assert_eq!(rig.router.status().accepted, 0);
    assert_eq!(
        rig.producer
            .try_publish(RouterPublication::Depth(vec![invalidation(1, 1)].into())),
        Err(PublishError::InvalidPublication)
    );
    assert_eq!(
        rig.producer.try_publish(RouterPublication::trades(vec![])),
        Err(PublishError::InvalidPublication)
    );
    assert_eq!(
        rig.producer.try_publish(publication(1)).unwrap().sequence,
        1
    );
    assert_eq!(
        rig.producer.try_publish(publication(2)),
        Err(PublishError::Capacity)
    );
    assert_eq!(rig.router.status().ingress_publications, 1);
    assert_eq!(records(receive(&mut sub).await).0, 1);
    assert_eq!(
        rig.producer.try_publish(publication(2)).unwrap().sequence,
        2
    );
    receive(&mut sub).await;
    let bytes = publication(3).retained_bytes().unwrap();
    let mut config = RouterConfig::default();
    config.ingress.bytes = bytes - 1;
    let bounded = Rig::new(SourceChannel::Trades, config);
    assert_eq!(
        bounded.producer.try_publish(publication(3)),
        Err(PublishError::Capacity)
    );
    assert_eq!(bounded.router.status().ingress_bytes, 0);
}

#[tokio::test]
async fn slow_consumer_failure_releases_demand_and_capacity() {
    let mut config = RouterConfig::default();
    config.consumer.publications = 1;
    let rig = Rig::new(SourceChannel::Trades, config);
    let mut slow = rig.subscribe("slow").await;
    let mut fast = rig.subscribe("fast").await;
    rig.producer.try_publish(publication(1)).unwrap();
    receive(&mut fast).await;
    rig.producer.try_publish(publication(2)).unwrap();
    receive(&mut fast).await;
    assert_eq!(
        slow.state(),
        SubscriptionState::Failed(ContinuityFailure::ConsumerCapacity)
    );
    assert!(matches!(
        receive(&mut slow).await,
        RouterDelivery::ContinuityLost(ContinuityFailure::ConsumerCapacity)
    ));
    assert!(slow.recv().await.is_none());
    assert_eq!(rig.context.demand.lock().unwrap().owner_count("test"), 1);
    assert_eq!(rig.router.status().consumer_bytes, 0);
    assert_eq!(rig.router.status().consumer_failures, 1);
}

#[tokio::test]
async fn byte_saturation_is_independent_of_publication_count() {
    let bytes = publication(1).retained_bytes().unwrap() + "test".len();
    let mut config = RouterConfig::default();
    config.consumer.bytes = bytes;
    config.consumer.publications = 100;
    let rig = Rig::new(SourceChannel::Trades, config);
    let mut sub = rig.subscribe("bytes").await;
    rig.producer.try_publish(publication(1)).unwrap();
    wait_dispatch(&rig.router).await;
    assert_eq!(rig.router.status().consumer_bytes, bytes);
    rig.producer.try_publish(publication(2)).unwrap();
    wait_dispatch(&rig.router).await;
    assert!(matches!(
        receive(&mut sub).await,
        RouterDelivery::ContinuityLost(ContinuityFailure::ConsumerCapacity)
    ));
    assert_eq!(rig.router.status().consumer_bytes, 0);
}

#[tokio::test]
async fn ownership_duplicate_drop_final_owner_and_always_on() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let a = rig.subscribe("a").await;
    let b = rig.subscribe("b").await;
    assert!(matches!(
        rig.router
            .subscribe(SubscribeRequest {
                stream_id: "test".into(),
                owner: owner("a")
            })
            .await,
        Err(RouterError::Demand(_))
    ));
    drop(a);
    assert_eq!(rig.context.demand.lock().unwrap().owner_count("test"), 1);
    drop(b);
    assert_eq!(rig.context.demand.lock().unwrap().total_owners(), 0);
    assert!(
        rig.context
            .demand
            .lock()
            .unwrap()
            .required_streams(&rig.context.graph)
            .unwrap()
            .is_empty()
    );
    let mut def = definition("binance_spot", SourceChannel::Trades, "always");
    def.desired.activation_policy =
        marketforge_engine::live::stream::persistence::ActivationPolicy::AlwaysOn;
    let graph = StreamGraph::restore(vec![def], vec![]).unwrap();
    assert!(
        DemandRegistry::new()
            .required_streams(&graph)
            .unwrap()
            .contains("always")
    );
}

#[tokio::test]
async fn runtime_revocation_fences_queued_ingress_and_delivery_and_replacement() {
    let mut rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let mut sub = rig.subscribe("old").await;
    let old_authority = rig.runtime.publication_authority("test").unwrap();
    assert!(matches!(
        rig.router.attach_producer(AttachProducerRequest {
            authority: old_authority.clone()
        }),
        Err(RouterError::Unauthorized)
    ));
    rig.producer.try_publish(publication(1)).unwrap();
    let stopping = rig.runtime.begin_stop("test").unwrap();
    assert!(matches!(
        receive(&mut sub).await,
        RouterDelivery::ContinuityLost(ContinuityFailure::ProducerRevoked)
    ));
    assert_eq!(
        rig.producer.try_publish(publication(2)),
        Err(PublishError::Revoked)
    );
    rig.runtime.mark_stopped("test", stopping).unwrap();
    rig.runtime.begin_start("test").unwrap();
    let new = rig
        .router
        .attach_producer(AttachProducerRequest {
            authority: rig.runtime.publication_authority("test").unwrap(),
        })
        .unwrap();
    assert!(matches!(
        rig.router.attach_producer(AttachProducerRequest {
            authority: old_authority
        }),
        Err(RouterError::Unauthorized)
    ));
    let mut current = rig.subscribe("new").await;
    drop(rig.producer);
    new.try_publish(publication(3)).unwrap();
    assert_eq!(records(receive(&mut current).await).0, 1);
    drop(new);
    assert!(matches!(
        receive(&mut current).await,
        RouterDelivery::ContinuityLost(ContinuityFailure::ProducerRevoked)
    ));
    assert_eq!(rig.router.status().producers, 0);
}

#[tokio::test]
async fn revocation_of_idle_binding_wakes_waiting_consumer_and_releases_resources() {
    let mut rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let mut sub = rig.subscribe("idle").await;
    rig.runtime.begin_stop("test").unwrap();
    assert!(matches!(
        receive(&mut sub).await,
        RouterDelivery::ContinuityLost(ContinuityFailure::ProducerRevoked)
    ));
    assert_eq!(rig.context.demand.lock().unwrap().total_owners(), 0);
}

pub fn invalidation(g: u64, b: u64) -> RouterDepthEvent {
    RouterDepthEvent::Invalidated {
        stream_generation: g,
        book_generation: b,
    }
}
pub fn depth(g: u64, b: u64, snapshot: bool) -> RouterDepthEvent {
    let baseline = DepthSequenceBaseline {
        last_sequence: 100,
        cross_sequence: None,
        synchronized: true,
    };
    let timestamp = DepthAcceptedTimestamp {
        latest_source: None,
        composition: DepthTimestampComposition::SingleMessage,
    };
    let levels: Arc<[BookChange]> = Arc::from([]);
    if snapshot {
        RouterDepthEvent::Snapshot {
            stream_generation: g,
            book_generation: b,
            baseline,
            levels,
            timestamp,
        }
    } else {
        RouterDepthEvent::Delta {
            stream_generation: g,
            book_generation: b,
            baseline,
            changes: levels,
            timestamp,
        }
    }
}
#[tokio::test]
async fn depth_continuity_recovery_late_subscription_and_stale_generations() {
    let rig = Rig::new(SourceChannel::Depth, RouterConfig::default());
    let mut sub = rig.subscribe("initial").await;
    for (index, event) in [
        depth(1, 1, true),
        depth(1, 1, false),
        depth(1, 1, false),
        invalidation(1, 1),
        depth(2, 2, true),
        depth(2, 2, false),
    ]
    .into_iter()
    .enumerate()
    {
        rig.producer
            .try_publish(RouterPublication::Depth(vec![event.clone()].into()))
            .unwrap();
        match receive(&mut sub).await {
            RouterDelivery::Publication {
                sequence,
                synchronized,
                publication,
                ..
            } => {
                assert_eq!(sequence, index as u64 + 1);
                assert_eq!(synchronized, index != 3);
                match publication.as_ref() {
                    RouterPublication::Depth(events) => {
                        assert_eq!(events[0].generations(), event.generations())
                    }
                    _ => panic!(),
                }
            }
            _ => panic!(),
        }
    }
    let mut late = rig.subscribe("late").await;
    rig.producer
        .try_publish(RouterPublication::Depth(vec![depth(2, 2, false)].into()))
        .unwrap();
    assert!(matches!(
        receive(&mut late).await,
        RouterDelivery::Publication {
            synchronized: false,
            ..
        }
    ));
    assert_eq!(
        rig.producer
            .try_publish(RouterPublication::Depth(vec![depth(1, 1, false)].into())),
        Err(PublishError::InvalidPublication)
    );
    rig.producer
        .try_publish(RouterPublication::Depth(vec![invalidation(2, 2)].into()))
        .unwrap();
    receive(&mut late).await;
    assert_eq!(
        rig.producer
            .try_publish(RouterPublication::Depth(vec![depth(2, 2, false)].into())),
        Err(PublishError::InvalidPublication)
    );
    rig.producer
        .try_publish(RouterPublication::Depth(
            vec![depth(2, 3, true), depth(2, 3, false)].into(),
        ))
        .unwrap();
    assert!(matches!(
        receive(&mut late).await,
        RouterDelivery::Publication {
            synchronized: true,
            ..
        }
    ));
}

#[tokio::test]
async fn subscriptions_do_not_receive_pre_admission_backlog() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    rig.producer.try_publish(publication(1)).unwrap();
    let mut late = rig.subscribe("late").await;
    rig.producer.try_publish(publication(2)).unwrap();
    assert_eq!(records(receive(&mut late).await).0, 2);
}

#[tokio::test]
async fn shutdown_and_drop_are_terminal_and_cancel_all_resources() {
    for explicit in [true, false] {
        let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
        let mut sub = rig.subscribe("shutdown").await;
        rig.producer.try_publish(publication(1)).unwrap();
        if explicit {
            rig.router.shutdown().await.unwrap();
            rig.router.shutdown().await.unwrap();
        } else {
            drop(rig.router);
        }
        assert!(matches!(
            receive(&mut sub).await,
            RouterDelivery::ContinuityLost(_)
        ));
        assert!(sub.recv().await.is_none());
        assert_eq!(
            rig.producer.try_publish(publication(2)),
            Err(PublishError::Shutdown)
        );
        assert_eq!(rig.context.demand.lock().unwrap().total_owners(), 0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_publications_follow_admission_sequence() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let mut sub = rig.subscribe("concurrent").await;
    let producer = Arc::new(rig.producer);
    let mut threads = vec![];
    for index in 0..8 {
        let producer = producer.clone();
        threads.push(std::thread::spawn(move || {
            let mut admitted = vec![];
            for offset in 0..16 {
                let id = index * 16 + offset;
                loop {
                    match producer.try_publish(publication(id)) {
                        Ok(acceptance) => {
                            admitted.push((acceptance.sequence, id));
                            break;
                        }
                        Err(PublishError::Busy) => std::thread::yield_now(),
                        other => panic!("{other:?}"),
                    }
                }
            }
            admitted
        }));
    }
    let mut expected: Vec<_> = threads
        .into_iter()
        .flat_map(|t| t.join().unwrap())
        .collect();
    expected.sort_unstable();
    for (sequence, id) in expected {
        let (actual, publication) = records(receive(&mut sub).await);
        assert_eq!(actual, sequence);
        match publication.as_ref() {
            RouterPublication::Trades { records, .. } => assert_eq!(records[0].sequence, Some(id)),
            _ => panic!(),
        }
    }
}

#[tokio::test]
async fn binding_and_subscription_limits_and_invalid_identities() {
    let mut config = RouterConfig::default();
    config.max_subscriptions = 1;
    config.max_producers = 1;
    let rig = Rig::new(SourceChannel::Trades, config);
    assert!(matches!(
        rig.router
            .subscribe(SubscribeRequest {
                stream_id: "test".into(),
                owner: owner("")
            })
            .await,
        Err(RouterError::InvalidIdentity)
    ));
    let sub = rig.subscribe("one").await;
    assert!(matches!(
        rig.router
            .subscribe(SubscribeRequest {
                stream_id: "test".into(),
                owner: owner("two")
            })
            .await,
        Err(RouterError::Capacity)
    ));
    drop(sub);
    assert_eq!(rig.router.status().subscriptions, 0);
    let sub = rig.subscribe("two").await;
    assert_ne!(sub.id(), 0);
    let other_graph = StreamGraph::restore(
        vec![definition("binance_spot", SourceChannel::Trades, "missing")],
        vec![],
    )
    .unwrap();
    let mut runtime = StreamRuntimeRegistry::new(&other_graph);
    runtime.begin_start("missing").unwrap();
    assert!(matches!(
        rig.router.attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("missing").unwrap()
        }),
        Err(RouterError::UnknownStream)
    ));
    // A valid authority for the same name but a different persisted identity is not interchangeable.
    let foreign = StreamGraph::restore(
        vec![definition("bybit_spot", SourceChannel::Trades, "test")],
        vec![],
    )
    .unwrap();
    let mut runtime = StreamRuntimeRegistry::new(&foreign);
    runtime.begin_start("test").unwrap();
    assert!(matches!(
        rig.router.attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("test").unwrap()
        }),
        Err(RouterError::Unauthorized)
    ));
}

#[tokio::test]
async fn revocation_discards_already_dispatched_data() {
    let mut rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let mut sub = rig.subscribe("stale").await;
    rig.producer.try_publish(publication(1)).unwrap();
    wait_dispatch(&rig.router).await;
    assert_eq!(sub.status().queued_publications, 1);
    rig.runtime.begin_stop("test").unwrap();
    assert!(matches!(
        receive(&mut sub).await,
        RouterDelivery::ContinuityLost(ContinuityFailure::ProducerRevoked)
    ));
    assert_eq!(sub.status().queued_bytes, 0);
    assert!(!sub.status().synchronized);
}

#[tokio::test]
async fn cancelled_receive_preserves_next_delivery() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let mut sub = rig.subscribe("cancel").await;
    {
        let receive = sub.recv();
        tokio::pin!(receive);
        use std::future::Future;
        assert!(
            std::future::poll_fn(|cx| std::task::Poll::Ready(
                receive.as_mut().poll(cx).is_pending()
            ))
            .await
        );
    }
    // Cancellation of a polled recv owns no data or capacity.
    drop(sub);
    assert_eq!(rig.context.demand.lock().unwrap().total_owners(), 0);
    let mut sub = rig.subscribe("after-cancel").await;
    rig.producer.try_publish(publication(1)).unwrap();
    assert_eq!(records(super::harness::receive(&mut sub).await).0, 1);
}

#[tokio::test]
async fn cleanup_failure_is_reported_instead_of_clean_shutdown() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let _sub = rig.subscribe("removed-externally").await;
    rig.context
        .demand
        .lock()
        .unwrap()
        .release(&rig.context.graph, &owner("removed-externally"), "test")
        .unwrap();
    assert!(matches!(
        rig.router.shutdown().await,
        Err(RouterError::Demand(_))
    ));
    assert!(rig.router.status().cleanup_error.is_some());
}

#[tokio::test]
async fn invalid_depth_batch_is_atomic_and_does_not_advance_authority() {
    let rig = Rig::new(SourceChannel::Depth, RouterConfig::default());
    let mut sub = rig.subscribe("atomic-depth").await;
    let bad = RouterPublication::Depth(vec![depth(1, 1, true), depth(1, 2, false)].into());
    assert_eq!(
        rig.producer.try_publish(bad),
        Err(PublishError::InvalidPublication)
    );
    assert_eq!(rig.router.status().accepted, 0);
    assert!(!sub.status().synchronized);
    assert_eq!(
        rig.producer
            .try_publish(RouterPublication::Depth(vec![depth(1, 1, false)].into())),
        Err(PublishError::InvalidPublication)
    );
    let accepted = rig
        .producer
        .try_publish(RouterPublication::Depth(
            vec![depth(1, 1, true), depth(1, 1, false)].into(),
        ))
        .unwrap();
    assert_eq!(accepted.sequence, 1);
    assert!(matches!(
        receive(&mut sub).await,
        RouterDelivery::Publication {
            synchronized: true,
            ..
        }
    ));
}

#[tokio::test]
async fn multiple_streams_have_independent_sequences_and_bounded_bindings() {
    use marketforge_engine::live::stream::persistence::PersistedStreamDefinition;
    use std::sync::Mutex;
    let definitions: Vec<PersistedStreamDefinition> = ["test", "other"]
        .iter()
        .map(|id| definition("binance_spot", SourceChannel::Trades, id))
        .collect();
    let graph = Arc::new(StreamGraph::restore(definitions, vec![]).unwrap());
    let mut runtime = StreamRuntimeRegistry::new(&graph);
    for id in ["test", "other"] {
        runtime.begin_start(id).unwrap();
    }
    let context = RouterContext {
        graph: graph.clone(),
        demand: Arc::new(Mutex::new(DemandRegistry::new())),
    };
    let mut config = RouterConfig::default();
    config.max_producers = 1;
    let router = StreamRouter::new(config, context).unwrap();
    let first = router
        .attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("test").unwrap(),
        })
        .unwrap();
    assert!(matches!(
        router.attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("other").unwrap()
        }),
        Err(RouterError::Capacity)
    ));
    drop(first);
    let other = router
        .attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("other").unwrap(),
        })
        .unwrap();
    let mut sub = router
        .subscribe(SubscribeRequest {
            stream_id: "other".into(),
            owner: owner("other"),
        })
        .await
        .unwrap();
    let mut record = trade(8);
    record.envelope.stream_id = "other".into();
    assert_eq!(
        other
            .try_publish(RouterPublication::trades(vec![record]))
            .unwrap()
            .sequence,
        1
    );
    assert_eq!(records(receive(&mut sub).await).0, 1);
    drop(other);
    let stopping = runtime.begin_stop("test").unwrap();
    runtime.mark_stopped("test", stopping).unwrap();
    runtime.begin_start("test").unwrap();
    let first = router
        .attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("test").unwrap(),
        })
        .unwrap();
    let mut sub = router
        .subscribe(SubscribeRequest {
            stream_id: "test".into(),
            owner: owner("test"),
        })
        .await
        .unwrap();
    assert_eq!(first.try_publish(publication(7)).unwrap().sequence, 1);
    assert_eq!(records(receive(&mut sub).await).0, 1);
}

#[tokio::test]
async fn runtime_lease_has_one_binding_across_routers_and_cannot_be_reused_after_detach() {
    let rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let authority = rig.runtime.publication_authority("test").unwrap();
    let other = StreamRouter::new(RouterConfig::default(), rig.context.clone()).unwrap();
    assert!(matches!(
        other.attach_producer(AttachProducerRequest {
            authority: authority.clone()
        }),
        Err(RouterError::Unauthorized)
    ));
    drop(rig.producer);
    assert!(matches!(
        rig.router
            .attach_producer(AttachProducerRequest { authority }),
        Err(RouterError::Unauthorized)
    ));
    assert_eq!(rig.router.status().producers, 0);
}

#[tokio::test]
async fn interleaved_streams_keep_independent_order_and_failure_boundaries() {
    use std::sync::Mutex;
    let graph = Arc::new(
        StreamGraph::restore(
            ["test", "other"]
                .iter()
                .map(|id| definition("binance_spot", SourceChannel::Trades, id))
                .collect(),
            vec![],
        )
        .unwrap(),
    );
    let mut runtime = StreamRuntimeRegistry::new(&graph);
    let router = StreamRouter::new(
        RouterConfig::default(),
        RouterContext {
            graph: graph.clone(),
            demand: Arc::new(Mutex::new(DemandRegistry::new())),
        },
    )
    .unwrap();
    let mut producers = vec![];
    let mut consumers = vec![];
    for id in ["test", "other"] {
        runtime.begin_start(id).unwrap();
        producers.push(
            router
                .attach_producer(AttachProducerRequest {
                    authority: runtime.publication_authority(id).unwrap(),
                })
                .unwrap(),
        );
        consumers.push(
            router
                .subscribe(SubscribeRequest {
                    stream_id: id.into(),
                    owner: owner(id),
                })
                .await
                .unwrap(),
        );
    }
    for offset in 0..16 {
        for index in [1, 0] {
            let mut record = trade(offset);
            record.envelope.stream_id = if index == 0 { "test" } else { "other" }.into();
            assert_eq!(
                producers[index]
                    .try_publish(RouterPublication::trades(vec![record]))
                    .unwrap()
                    .sequence,
                offset + 1
            );
        }
    }
    runtime.begin_stop("test").unwrap();
    assert!(matches!(
        receive(&mut consumers[0]).await,
        RouterDelivery::ContinuityLost(ContinuityFailure::ProducerRevoked)
    ));
    for offset in 0..16 {
        let (sequence, payload) = records(receive(&mut consumers[1]).await);
        assert_eq!(sequence, offset + 1);
        assert!(
            matches!(payload.as_ref(),RouterPublication::Trades {records,..} if records[0].sequence==Some(offset))
        );
    }
    assert_eq!(router.status().producers, 1);
}

#[tokio::test]
async fn replacement_atomically_revokes_old_backlog_before_dispatch() {
    let mut rig = Rig::new(SourceChannel::Trades, RouterConfig::default());
    let mut old = rig.subscribe("old-backlog").await;
    rig.producer.try_publish(publication(1)).unwrap();
    let generation = rig.runtime.status("test").unwrap().generation;
    rig.runtime
        .mark_failed("test", generation, "restart")
        .unwrap();
    rig.runtime.begin_start("test").unwrap();
    let next = rig
        .router
        .attach_producer(AttachProducerRequest {
            authority: rig.runtime.publication_authority("test").unwrap(),
        })
        .unwrap();
    assert_eq!(rig.router.status().ingress_publications, 0);
    assert!(matches!(
        receive(&mut old).await,
        RouterDelivery::ContinuityLost(ContinuityFailure::ProducerRevoked)
    ));
    let mut current = rig.subscribe("replacement").await;
    let accepted = next.try_publish(publication(2)).unwrap();
    assert_eq!(
        accepted.runtime_generation,
        rig.runtime.status("test").unwrap().generation
    );
    match receive(&mut current).await {
        RouterDelivery::Publication {
            sequence,
            runtime_generation,
            publication,
            ..
        } => {
            assert_eq!(sequence, 1);
            assert_eq!(runtime_generation, accepted.runtime_generation);
            assert!(
                matches!(publication.as_ref(),RouterPublication::Trades {records,..} if records[0].sequence==Some(2))
            );
        }
        other => panic!("{other:?}"),
    }
    assert_eq!(
        rig.producer.try_publish(publication(3)),
        Err(PublishError::Revoked)
    );
}
