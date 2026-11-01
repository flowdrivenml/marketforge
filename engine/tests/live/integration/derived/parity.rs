use super::harness::*;
use marketforge_engine::shared::merge::{HeadQueue, MergeKey, MergePoll};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finite_router_event_time_merge_matches_shared_head_queue_and_flushes_only_explicit_eof() {
    let rig = Rig::replay(vec![
        source::definition("binance_spot", SourceChannel::Trades, "a"),
        source::definition("binance_spot", SourceChannel::Trades, "b"),
        derived(
            "m",
            DerivedOperation::Merge,
            &["a", "b"],
            &[],
            Some(MergeOrdering::EventTime),
        ),
    ]);
    let handle = rig.service.handle();
    let mut sub = subscribe(&rig.router, "m").await;
    let stream = bounded(handle.start_derived("m")).await.unwrap();
    let inputs = [vec![100, 100, 200], vec![90, 100, 300]];
    let mut queue = HeadQueue::new(2, true).unwrap();
    let mut cursors = [0usize; 2];
    for rank in 0..2 {
        queue
            .push(
                rank,
                MergeKey::new(inputs[rank][0], rank as u32, 0, 0),
                (rank, 0),
            )
            .unwrap();
    }
    let mut expected = vec![];
    loop {
        match queue.next_head().unwrap() {
            MergePoll::Ready(h) => {
                expected.push((h.key.timestamp_ns, h.event.0, h.event.1));
                let r = h.source_index;
                cursors[r] += 1;
                let ordinal = cursors[r];
                if ordinal < inputs[r].len() {
                    queue
                        .push(
                            r,
                            MergeKey::new(inputs[r][ordinal], r as u32, 0, ordinal as u64),
                            (r, ordinal),
                        )
                        .unwrap();
                } else {
                    queue.finish_source(r).unwrap();
                }
            }
            MergePoll::Exhausted => break,
            MergePoll::Pending => panic!(),
        }
    }
    for rank in 0..2 {
        for (i, t) in inputs[rank].iter().enumerate() {
            publish(
                &rig.producers[rank],
                publication(["a", "b"][rank], i as u64 + 1, *t),
            )
            .await;
        }
        publish(&rig.producers[rank], RouterPublication::ReplayComplete).await;
    }
    let mut actual = vec![];
    loop {
        let delivery = bounded(sub.recv()).await.unwrap();
        match delivery {
            RouterDelivery::Publication { publication, .. } => match publication.as_ref() {
                RouterPublication::Derived(events) => {
                    let e = &events[0];
                    actual.push((
                        e.native.timestamp_ns().unwrap(),
                        e.input_position,
                        (e.origin.publication_sequence - 1) as usize,
                    ));
                }
                RouterPublication::ReplayComplete => break,
                other => panic!("{other:?}"),
            },
            other => panic!("{other:?}; {:?}", handle.status()),
        }
    }
    assert_eq!(actual, expected);
    #[cfg(feature = "historical")]
    {
        use marketforge_engine::historical::merge::{
            KWayEventMerger, MergeEventRef, MergeEventSource, TradeEventRef,
        };
        struct Cursor {
            dataset: u64,
            events: std::collections::VecDeque<(usize, i64)>,
        }
        impl MergeEventSource for Cursor {
            fn next_event(
                &mut self,
            ) -> marketforge_engine::shared::error::Result<Option<MergeEventRef>> {
                Ok(self.events.pop_front().map(|(ordinal, t)| {
                    MergeEventRef::Trade(TradeEventRef {
                        dataset_id: self.dataset,
                        task_id: 0,
                        event_ordinal: ordinal as u64,
                        canonical_row_offset: ordinal as u64,
                        event_timestamp_ns: t,
                    })
                }))
            }
        }
        let cursors = inputs
            .iter()
            .enumerate()
            .map(|(i, events)| {
                Box::new(Cursor {
                    dataset: i as u64 + 1,
                    events: events.iter().copied().enumerate().collect(),
                }) as Box<dyn MergeEventSource>
            })
            .collect();
        let mut historical = KWayEventMerger::new(cursors).unwrap();
        let mut witness = vec![];
        while let Some(e) = historical.next_event().unwrap() {
            witness.push((
                e.event.timestamp_ns(),
                e.stream_rank as usize,
                e.event.event_ordinal() as usize,
            ));
        }
        assert_eq!(actual, witness);
    }
    assert_eq!(
        rig.producers[0].try_publish(publication("a", 9, 999)),
        Err(PublishError::Revoked)
    );
    bounded(handle.stop(&stream)).await.unwrap();
    let report = handle.take_report("m", stream.generation).unwrap();
    assert!(report.processor_retained.is_empty());
    report.discard();
    assert_eq!(handle.status().accounted_events, 0);
    rig.close().await;
}
