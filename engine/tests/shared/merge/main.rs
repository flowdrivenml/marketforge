use marketforge_engine::shared::{
    domain::canonical::{
        BookSide, DepthEventBoundary, DepthProcessingOutcome, DepthSourceEventMetadata,
        EventEnvelope, Exchange, L2LevelUpdate,
    },
    merge::*,
};
fn key(timestamp_ns: i64, stream_rank: u32, task_rank: u64, event_ordinal: u64) -> MergeKey {
    MergeKey::new(timestamp_ns, stream_rank, task_rank, event_ordinal)
}
fn ready<E>(poll: MergePoll<E>) -> SelectedHead<E> {
    match poll {
        MergePoll::Ready(event) => event,
        _ => panic!("expected a head"),
    }
}

#[test]
fn pending_is_distinct_from_exhaustion_and_blocks_unsafe_selection() {
    let mut queue = HeadQueue::new(2, true).unwrap();
    queue.push(0, key(20, 0, 0, 0), "later").unwrap();
    assert!(matches!(queue.next_head().unwrap(), MergePoll::Pending));
    queue.push(1, key(10, 1, 0, 0), "earlier").unwrap();
    assert_eq!(ready(queue.next_head().unwrap()).event, "earlier");
    assert!(matches!(queue.next_head().unwrap(), MergePoll::Pending));
    queue.finish_source(1).unwrap();
    assert_eq!(ready(queue.next_head().unwrap()).event, "later");
    assert!(matches!(queue.next_head().unwrap(), MergePoll::Pending));
    queue.finish_source(0).unwrap();
    assert!(matches!(queue.next_head().unwrap(), MergePoll::Exhausted));
    assert!(matches!(queue.next_head().unwrap(), MergePoll::Exhausted));
    assert_eq!(queue.events_emitted(), 2);
}

#[test]
fn empty_sources_and_duplicate_or_invalid_source_slots_are_rejected() {
    assert!(matches!(
        HeadQueue::<()>::new(0, true),
        Err(MergeError::EmptySources)
    ));
    let mut queue = HeadQueue::new(2, true).unwrap();
    queue.push(0, key(0, 0, 0, 0), ()).unwrap();
    assert!(matches!(
        queue.push(0, key(1, 0, 0, 1), ()),
        Err(MergeError::SourceNotAwaiting(0))
    ));
    assert!(matches!(
        queue.finish_source(0),
        Err(MergeError::SourceNotAwaiting(0))
    ));
    assert!(matches!(
        queue.push(1, key(0, 0, 0, 0), ()),
        Err(MergeError::RankMismatch(1))
    ));
    assert!(matches!(
        queue.finish_source(2),
        Err(MergeError::InvalidSource(2))
    ));
    queue.finish_source(1).unwrap();
    assert!(matches!(
        queue.push(1, key(0, 1, 0, 0), ()),
        Err(MergeError::SourceNotAwaiting(1))
    ));
}

#[test]
fn lexicographic_order_and_incremental_source_advancement_match_sorted_witness() {
    let inputs = [
        vec![key(1, 0, 0, 0), key(1, 0, 0, 1), key(2, 0, 0, 0)],
        vec![key(1, 1, 0, 9), key(1, 1, 1, 0), key(3, 1, 0, 0)],
        vec![],
    ];
    let mut expected = inputs.concat();
    expected.sort();
    let mut sources = inputs.map(|v| v.into_iter());
    let mut queue = HeadQueue::new(3, true).unwrap();
    for (index, source) in sources.iter_mut().enumerate() {
        if let Some(key) = source.next() {
            queue.push(index, key, Box::new(key)).unwrap();
        } else {
            queue.finish_source(index).unwrap();
        }
    }
    let mut actual = Vec::new();
    loop {
        match queue.next_head().unwrap() {
            MergePoll::Ready(head) => {
                actual.push(*head.event);
                if let Some(key) = sources[head.source_index].next() {
                    queue.push(head.source_index, key, Box::new(key)).unwrap();
                } else {
                    queue.finish_source(head.source_index).unwrap();
                }
            }
            MergePoll::Exhausted => break,
            MergePoll::Pending => panic!("all heads should be supplied"),
        }
    }
    assert_eq!(actual, expected);
}

#[test]
fn immediate_source_and_deferred_global_regression_checks_preserve_error_timing() {
    for immediate in [true, false] {
        let mut queue = HeadQueue::new(1, immediate).unwrap();
        queue.push(0, key(10, 0, 0, 0), ()).unwrap();
        ready(queue.next_head().unwrap());
        let pushed = queue.push(0, key(9, 0, 0, 1), ());
        if immediate {
            assert!(matches!(pushed, Err(MergeError::SourceRegression { .. })));
        } else {
            pushed.unwrap();
            assert!(matches!(
                queue.next_head(),
                Err(MergeError::GlobalRegression { .. })
            ));
        }
        assert_eq!(queue.events_emitted(), 1);
    }
}

#[test]
fn nonclone_depth_outcomes_are_atomic_and_keep_boundary_and_native_metadata() {
    let mut queue = HeadQueue::new(1, true).unwrap();
    let source = DepthSourceEventMetadata {
        source_event_ordinal: Some(42),
        event_timestamp_ns: 100,
        system_timestamp_ns: Some(101),
        sequence_start: Some(5),
        sequence_end: Some(8),
    };
    let level = L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: 100,
            system_timestamp_ns: Some(101),
            exchange: Exchange::Bybit,
            instrument_id: 7,
            symbol: "BTCUSDT".into(),
            stream_id: "depth".into(),
        },
        side: BookSide::Bid,
        price: 10.into(),
        quantity_base: Some(2.into()),
        quantity_quote: Some(20.into()),
        quantity_contracts: None,
        order_count: Some(3),
    };
    let event = DepthProcessingOutcome::initialization(vec![level.clone(), level.clone()])
        .with_source(source.clone());
    queue.push(0, key(100, 0, 7, 42), event).unwrap();
    let selected = ready(queue.next_head().unwrap());
    assert_eq!(selected.event.source, source);
    assert_eq!(selected.event.events, vec![level.clone(), level]);
    assert_eq!(selected.event.boundary, DepthEventBoundary::Initialization);
    queue.finish_source(0).unwrap();
    assert!(matches!(queue.next_head().unwrap(), MergePoll::Exhausted));
    assert_eq!(queue.events_emitted(), 1);
}

#[test]
fn all_sources_can_be_empty() {
    let mut queue = HeadQueue::<()>::new(2, true).unwrap();
    queue.finish_source(0).unwrap();
    queue.finish_source(1).unwrap();
    assert!(matches!(queue.next_head().unwrap(), MergePoll::Exhausted));
    assert_eq!(queue.events_emitted(), 0);
}

#[test]
fn ordering_uses_timestamp_then_stream_then_task_then_ordinal() {
    let ordered = [
        key(9, 9, 9, 9),
        key(10, 0, 0, 0),
        key(10, 0, 0, 1),
        key(10, 0, 1, 0),
        key(10, 1, 0, 0),
    ];
    for pair in ordered.windows(2) {
        assert!(pair[0] < pair[1]);
    }
}

#[test]
fn equal_keys_remain_distinct_events_and_source_local_ordinal_regressions_fail() {
    let mut queue = HeadQueue::new(1, true).unwrap();
    let same = key(10, 0, 7, 3);
    queue.push(0, same, "first").unwrap();
    assert_eq!(ready(queue.next_head().unwrap()).event, "first");
    queue.push(0, same, "second").unwrap();
    assert_eq!(ready(queue.next_head().unwrap()).event, "second");
    assert!(matches!(
        queue.push(0, key(10, 0, 7, 2), "regression"),
        Err(MergeError::SourceRegression { .. })
    ));
    assert_eq!(queue.events_emitted(), 2);
}
