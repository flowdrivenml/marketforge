use super::*;
fn context() -> RouterContext {
    RouterContext {
        graph: Arc::new(StreamGraph::restore(vec![], vec![]).unwrap()),
        demand: Arc::new(Mutex::new(DemandRegistry::new())),
    }
}
#[test]
fn capacity_uses_checked_arithmetic() {
    let limits = QueueLimits {
        publications: usize::MAX,
        bytes: usize::MAX,
    };
    assert!(!limits.admits(0, usize::MAX, 1));
    assert!(!limits.admits(usize::MAX, 0, 1));
    assert!(limits.admits(0, usize::MAX - 1, 1));
}
#[test]
fn construction_requires_runtime() {
    assert!(matches!(
        StreamRouter::new(RouterConfig::default(), context()),
        Err(RouterError::NoRuntime)
    ));
}
#[tokio::test]
async fn every_zero_resource_limit_is_invalid() {
    for index in 0..6 {
        let mut config = RouterConfig::default();
        match index {
            0 => config.ingress.publications = 0,
            1 => config.ingress.bytes = 0,
            2 => config.consumer.publications = 0,
            3 => config.consumer.bytes = 0,
            4 => config.max_subscriptions = 0,
            _ => config.max_producers = 0,
        }
        assert!(matches!(
            StreamRouter::new(config, context()),
            Err(RouterError::InvalidConfig)
        ));
    }
}
#[test]
fn identity_exhaustion_does_not_wrap() {
    let mut state = State {
        bindings: BTreeMap::new(),
        consumers: BTreeMap::new(),
        ingress: VecDeque::new(),
        ingress_bytes: 0,
        next_id: u64::MAX,
        status: RouterStatus::default(),
    };
    assert_eq!(next_id(&mut state), Err(RouterError::Exhausted));
    assert_eq!(state.next_id, u64::MAX);
}
#[test]
fn terminal_failure_delivered_exactly_once_and_clears_sync() {
    let consumer = Consumer {
        id: 1,
        stream: "x".into(),
        owner: DemandOwner::Manual {
            session_id: "x".into(),
        },
        queue: Mutex::new(ConsumerQueue {
            queue: VecDeque::new(),
            bytes: 0,
            state: SubscriptionState::Failed(ContinuityFailure::ConsumerCapacity),
            terminal_pending: true,
            synchronized: false,
        }),
        notify: Notify::new(),
    };
    assert!(matches!(
        take_delivery(&consumer),
        Some(Some(RouterDelivery::ContinuityLost(
            ContinuityFailure::ConsumerCapacity
        )))
    ));
    assert!(matches!(take_delivery(&consumer), Some(None)));
}
#[tokio::test]
async fn unknown_subscription_acquires_no_demand() {
    let context = context();
    let router = StreamRouter::new(RouterConfig::default(), context.clone()).unwrap();
    assert!(matches!(
        router
            .subscribe(SubscribeRequest {
                stream_id: "missing".into(),
                owner: DemandOwner::Manual {
                    session_id: "one".into()
                }
            })
            .await,
        Err(RouterError::UnknownStream)
    ));
    assert_eq!(lock(&context.demand).total_owners(), 0);
    router.shutdown().await.unwrap();
    assert!(router.status().shutdown);
}

#[tokio::test]
async fn aggregate_configuration_overflow_is_rejected() {
    let mut config = RouterConfig::default();
    config.consumer.bytes = usize::MAX;
    assert!(matches!(
        StreamRouter::new(config, context()),
        Err(RouterError::InvalidConfig)
    ));
}

fn router_with_source(
    channel: &str,
) -> (
    StreamRouter,
    crate::live::stream::registry::StreamRuntimeRegistry,
) {
    use crate::live::stream::{persistence::*, registry::StreamRuntimeRegistry};
    let request = serde_json::from_value(serde_json::json!({
        "instrument_id":42,"exchange":"binance","symbol":"BTCUSDT","market_category":"spot",
        "instrument_type":"spot","base_asset":"BTC","quote_asset":"USDT","quantity_type":"base",
        "contract_value":null,"contract_value_asset":null,"settlement_asset":null,"channel":channel
    }))
    .unwrap();
    let definition = PersistedStreamDefinition {
        stream_id: "unit".into(),
        recording_stream_key: Some(1),
        definition: StreamDefinition::Source(request),
        desired: DesiredStreamState::default(),
        revision: 1,
    };
    let graph = Arc::new(StreamGraph::restore(vec![definition], vec![]).unwrap());
    let runtime = StreamRuntimeRegistry::new(&graph);
    let router = StreamRouter::new(
        RouterConfig::default(),
        RouterContext {
            graph,
            demand: Arc::new(Mutex::new(DemandRegistry::new())),
        },
    )
    .unwrap();
    (router, runtime)
}
fn snapshot() -> RouterPublication {
    use crate::live::integrity::{
        DepthAcceptedTimestamp, DepthSequenceBaseline, DepthTimestampComposition,
    };
    RouterPublication::Depth(
        vec![RouterDepthEvent::Snapshot {
            stream_generation: 1,
            book_generation: 1,
            baseline: DepthSequenceBaseline {
                last_sequence: 1,
                cross_sequence: None,
                synchronized: true,
            },
            levels: Arc::from([]),
            timestamp: DepthAcceptedTimestamp {
                latest_source: None,
                composition: DepthTimestampComposition::SingleMessage,
            },
        }]
        .into(),
    )
}
#[tokio::test]
async fn nonblocking_contention_rejects_without_admission() {
    let (router, mut runtime) = router_with_source("depth");
    runtime.begin_start("unit").unwrap();
    let producer = router
        .attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("unit").unwrap(),
        })
        .unwrap();
    {
        let state = lock(&router.shared.state);
        assert_eq!(producer.try_publish(snapshot()), Err(PublishError::Busy));
        assert_eq!(state.status.accepted, 0);
        assert!(state.ingress.is_empty());
    }
    assert_eq!(producer.try_publish(snapshot()).unwrap().sequence, 1);
}
#[tokio::test]
async fn sequence_exhaustion_does_not_commit_depth_authority_or_capacity() {
    let (router, mut runtime) = router_with_source("depth");
    runtime.begin_start("unit").unwrap();
    let producer = router
        .attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("unit").unwrap(),
        })
        .unwrap();
    lock(&router.shared.state)
        .bindings
        .get_mut("unit")
        .unwrap()
        .sequence = u64::MAX;
    assert_eq!(
        producer.try_publish(snapshot()),
        Err(PublishError::Exhausted)
    );
    let state = lock(&router.shared.state);
    let binding = &state.bindings["unit"];
    assert_eq!(binding.book, None);
    assert_eq!(binding.source_generation, None);
    assert_eq!(state.ingress_bytes, 0);
    assert_eq!(state.status.accepted, 0);
}
#[tokio::test]
async fn runtime_owner_drop_revokes_publication() {
    let (router, mut runtime) = router_with_source("depth");
    runtime.begin_start("unit").unwrap();
    let producer = router
        .attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("unit").unwrap(),
        })
        .unwrap();
    drop(runtime);
    assert_eq!(producer.try_publish(snapshot()), Err(PublishError::Revoked));
    assert_eq!(router.status().accepted, 0);
}
#[tokio::test]
async fn dropped_subscription_releases_only_its_owner() {
    let (router, _runtime) = router_with_source("trades");
    let request = |id: &str| SubscribeRequest {
        stream_id: "unit".into(),
        owner: DemandOwner::Direct {
            consumer_id: "unit".into(),
            subscription_id: id.into(),
        },
    };
    let a = router.subscribe(request("a")).await.unwrap();
    let b = router.subscribe(request("b")).await.unwrap();
    assert_ne!(a.id(), b.id());
    drop(a);
    assert_eq!(lock(&router.shared.context.demand).owner_count("unit"), 1);
    b.close().await.unwrap();
    assert_eq!(lock(&router.shared.context.demand).total_owners(), 0);
}
#[tokio::test]
async fn wrong_type_and_empty_batches_cannot_change_admission_sequence() {
    let (router, mut runtime) = router_with_source("depth");
    runtime.begin_start("unit").unwrap();
    let producer = router
        .attach_producer(AttachProducerRequest {
            authority: runtime.publication_authority("unit").unwrap(),
        })
        .unwrap();
    assert_eq!(
        producer.try_publish(RouterPublication::trades(vec![])),
        Err(PublishError::InvalidPublication)
    );
    assert_eq!(
        producer.try_publish(RouterPublication::Depth(Arc::from([]))),
        Err(PublishError::InvalidPublication)
    );
    assert_eq!(producer.try_publish(snapshot()).unwrap().sequence, 1);
    assert_eq!(router.status().accepted, 1);
}
