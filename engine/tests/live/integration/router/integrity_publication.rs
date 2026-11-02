use super::harness::*;
use marketforge_engine::live::{
    integrity::*,
    stream::{router::*, source::SourceTradePublication},
};
#[tokio::test]
async fn metadata_only_gap_survives_registration_and_terminal_cleanup() {
    let rig = Rig::new(
        marketforge_engine::live::stream::request::SourceChannel::Trades,
        RouterConfig::default(),
    );
    let mut sub = rig
        .router
        .subscribe_live(SubscribeRequest {
            stream_id: "test".into(),
            owner: owner("integrity"),
        })
        .await
        .unwrap();
    assert!(matches!(
        sub.recv_live().await,
        Some(RouterStreamMessage::IntegritySnapshot(_))
    ));
    let outcome = IntegrityOutcome::Trades(BoundTradeIngressOutcome::Processed(
        TradeControllerOutcome::CoverageReviewRequired {
            source_generation: 7,
            observations: vec![TradeIntegrityObservation {
                index: 0,
                kind: TradeIntegrityObservationKind::NumericIdDiscontinuity,
                previous: 10,
                current: 20,
            }],
        },
    ));
    rig.producer
        .publish_source_trades(SourceTradePublication::Integrity(&outcome))
        .unwrap();
    assert_eq!(rig.router.status().accepted, 0);
    let mut late = rig
        .router
        .subscribe_live(SubscribeRequest {
            stream_id: "test".into(),
            owner: owner("late"),
        })
        .await
        .unwrap();
    let Some(RouterStreamMessage::IntegritySnapshot(s)) = late.recv_live().await else {
        panic!()
    };
    assert_eq!(s.sources[0].observations[0].missing_records, None);
    assert_eq!(s.sources[0].coverage, "coverage_review_required");
    drop(rig.producer);
    let Some(RouterStreamMessage::IntegrityUpdate(u)) = sub.recv_live().await else {
        panic!()
    };
    assert_eq!(u.evidence.observations[0].observed, 20);
    assert!(matches!(
        sub.recv_live().await,
        Some(RouterStreamMessage::Market(RouterDelivery::ContinuityLost(
            _
        )))
    ));
    rig.router.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn subscription_cut_has_no_missing_or_duplicate_integrity_revisions() {
    let rig = Rig::new(
        marketforge_engine::live::stream::request::SourceChannel::Trades,
        RouterConfig::default(),
    );
    let producer = std::sync::Arc::new(rig.producer);
    let publisher = producer.clone();
    let task = tokio::spawn(async move {
        for i in 1..=64 {
            publisher
                .publish_trade_integrity(
                    &RouterTradeIntegrity {
                        source_generation: Some(7),
                        received_records: 0,
                        duplicate_records: 0,
                        identity_history_truncated: false,
                        coverage_history_unverified: true,
                        observations: vec![].into(),
                        baseline: TradeIntegrityBaseline {
                            numeric_trade_id: Some(i),
                            cross_sequence: None,
                        },
                    },
                    false,
                )
                .unwrap();
            tokio::task::yield_now().await;
        }
    });
    let mut sub = rig
        .router
        .subscribe_live(SubscribeRequest {
            stream_id: "test".into(),
            owner: owner("cut"),
        })
        .await
        .unwrap();
    let Some(RouterStreamMessage::IntegritySnapshot(snapshot)) = sub.recv_live().await else {
        panic!()
    };
    let mut revision = snapshot.revision;
    while revision < 64 {
        let Some(RouterStreamMessage::IntegrityUpdate(update)) =
            tokio::time::timeout(DEADLINE, sub.recv_live())
                .await
                .unwrap()
        else {
            panic!()
        };
        assert_eq!(update.revision, revision + 1);
        revision = update.revision;
    }
    task.await.unwrap();
    assert_eq!(rig.router.status().accepted, 0);
    sub.close().await.unwrap();
    drop(producer);
    rig.router.shutdown().await.unwrap();
}
#[tokio::test]
async fn replacement_snapshot_does_not_reuse_old_generation_coverage() {
    let mut rig = Rig::new(
        marketforge_engine::live::stream::request::SourceChannel::Trades,
        RouterConfig::default(),
    );
    let mut old = rig
        .router
        .subscribe_live(SubscribeRequest {
            stream_id: "test".into(),
            owner: owner("old-generation"),
        })
        .await
        .unwrap();
    let Some(RouterStreamMessage::IntegritySnapshot(initial)) = old.recv_live().await else {
        panic!()
    };
    let old_generation = initial.runtime_generation;
    rig.producer
        .publish_trade_integrity(
            &RouterTradeIntegrity {
                source_generation: Some(7),
                received_records: 1,
                duplicate_records: 0,
                identity_history_truncated: false,
                coverage_history_unverified: false,
                observations: vec![].into(),
                baseline: TradeIntegrityBaseline {
                    numeric_trade_id: Some(99),
                    cross_sequence: None,
                },
            },
            false,
        )
        .unwrap();
    let g = rig.runtime.begin_stop("test").unwrap();
    rig.runtime.mark_stopped("test", g).unwrap();
    let g = rig.runtime.begin_start("test").unwrap();
    let next = rig
        .router
        .attach_producer(AttachProducerRequest {
            authority: rig.runtime.publication_authority("test").unwrap(),
        })
        .unwrap();
    let mut sub = rig
        .router
        .subscribe_live(SubscribeRequest {
            stream_id: "test".into(),
            owner: owner("new"),
        })
        .await
        .unwrap();
    let Some(RouterStreamMessage::IntegritySnapshot(snapshot)) = sub.recv_live().await else {
        panic!()
    };
    assert_eq!(snapshot.sources[0].coverage, "unavailable");
    assert_eq!(snapshot.sources[0].numeric_trade_id, None);
    assert_eq!(snapshot.sources[0].id.runtime_generation, Some(g.get()));
    assert!(snapshot.evidence_history.is_empty());
    assert_ne!(old_generation, Some(g.get()));
    loop {
        match old.recv_live().await.unwrap() {
            RouterStreamMessage::IntegrityUpdate(update) => {
                assert_eq!(update.evidence.id.runtime_generation, old_generation)
            }
            RouterStreamMessage::IntegritySnapshot(snapshot) => {
                assert_eq!(snapshot.runtime_generation, old_generation)
            }
            RouterStreamMessage::Market(RouterDelivery::ContinuityLost(_)) => break,
            other => panic!("unexpected {other:?}"),
        }
    }

    sub.close().await.unwrap();
    drop(next);
    rig.router.shutdown().await.unwrap();
}
#[tokio::test]
async fn absent_authority_is_unavailable_instead_of_generation_zero() {
    let rig = Rig::new(
        marketforge_engine::live::stream::request::SourceChannel::Trades,
        RouterConfig::default(),
    );
    drop(rig.producer);
    let mut sub = rig
        .router
        .subscribe_live(SubscribeRequest {
            stream_id: "test".into(),
            owner: owner("unavailable"),
        })
        .await
        .unwrap();
    let Some(RouterStreamMessage::IntegritySnapshot(snapshot)) = sub.recv_live().await else {
        panic!()
    };
    assert_eq!(snapshot.runtime_generation, None);
    assert_eq!(snapshot.sources[0].id.runtime_generation, None);
    assert_eq!(snapshot.sources[0].id.revision, None);
    assert_eq!(snapshot.sources[0].received_records, None);
    assert_eq!(snapshot.sources[0].coverage, "unavailable");
    sub.close().await.unwrap();
    rig.router.shutdown().await.unwrap();
}

#[tokio::test]
async fn integrity_priority_is_bounded_and_event_evidence_is_immutable() {
    let rig = Rig::new(
        marketforge_engine::live::stream::request::SourceChannel::Trades,
        RouterConfig::default(),
    );
    let mut sub = rig
        .router
        .subscribe_live(SubscribeRequest {
            stream_id: "test".into(),
            owner: owner("fair"),
        })
        .await
        .unwrap();
    assert!(matches!(
        sub.recv_live().await,
        Some(RouterStreamMessage::IntegritySnapshot(_))
    ));
    for i in 1..=2 {
        rig.producer.try_publish(publication(i)).unwrap();
    }
    wait_dispatch(&rig.router).await;
    let meta = RouterTradeIntegrity {
        source_generation: Some(7),
        received_records: 0,
        duplicate_records: 0,
        identity_history_truncated: false,
        coverage_history_unverified: true,
        observations: vec![].into(),
        baseline: TradeIntegrityBaseline {
            numeric_trade_id: Some(999),
            cross_sequence: None,
        },
    };
    let mut market = vec![];
    let mut seen_update = false;
    // One fresh notification per serviced receive: backlog does not grow.
    for _ in 0..24 {
        rig.producer.publish_trade_integrity(&meta, false).unwrap();
        match sub.recv_live().await.unwrap() {
            RouterStreamMessage::IntegrityUpdate(u) => {
                if u.evidence.numeric_trade_id == Some(999) {
                    assert_eq!(u.evidence.after_publication_sequence, Some(2));
                    seen_update = true;
                }
            }
            RouterStreamMessage::Market(RouterDelivery::Publication {
                sequence,
                admission,
                ..
            }) => {
                assert!(seen_update);
                assert_ne!(admission.integrity(0).unwrap().numeric_trade_id, Some(999));
                market.push(sequence);
                if market.len() == 2 {
                    break;
                }
            }
            other => panic!("unexpected {other:?}"),
        }
    }
    assert_eq!(
        market,
        vec![1, 2],
        "continuous serviceable integrity must not starve queued market data"
    );
    sub.close().await.unwrap();
    rig.router.shutdown().await.unwrap();
}
