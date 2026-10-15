#![cfg(feature = "historical")]

use marketforge_engine::historical::merge::{
    KWayEventMerger, MergeEventRef, MergeEventSource, MergeKey, TradeEventRef,
    UnifiedTimelineRecord,
};

// -----------------------------------------------------------------------------
// Synthetic event source
// -----------------------------------------------------------------------------

struct SyntheticSource {
    events: std::vec::IntoIter<TradeEventRef>,
}

impl SyntheticSource {
    fn new(events: Vec<TradeEventRef>) -> Self {
        Self {
            events: events.into_iter(),
        }
    }
}

impl MergeEventSource for SyntheticSource {
    fn next_event(&mut self) -> marketforge_engine::error::Result<Option<MergeEventRef>> {
        Ok(self.events.next().map(MergeEventRef::Trade))
    }
}

// -----------------------------------------------------------------------------
// Event fixture
// -----------------------------------------------------------------------------

fn trade(dataset_id: u64, task_id: u64, ordinal: u64, timestamp_ns: i64) -> TradeEventRef {
    TradeEventRef {
        dataset_id,
        task_id,
        event_ordinal: ordinal,
        canonical_row_offset: ordinal,
        event_timestamp_ns: timestamp_ns,
    }
}

// -----------------------------------------------------------------------------
// Merge helper
// -----------------------------------------------------------------------------

fn merge(sources: Vec<Vec<TradeEventRef>>) -> Vec<UnifiedTimelineRecord> {
    let streams = sources
        .into_iter()
        .map(|events| Box::new(SyntheticSource::new(events)) as Box<dyn MergeEventSource>)
        .collect();

    let mut merger = KWayEventMerger::new(streams).expect("construct merger");

    let mut records = Vec::new();

    while let Some(event) = merger.next_event().expect("merge event") {
        records.push(UnifiedTimelineRecord::from(&event));
    }

    records
}

// -----------------------------------------------------------------------------
// Ordering key
// -----------------------------------------------------------------------------

fn key(record: &UnifiedTimelineRecord) -> MergeKey {
    MergeKey {
        timestamp_ns: record.event_timestamp_ns,
        stream_rank: record.stream_rank,
        task_rank: record.task_id,
        event_ordinal: record.event_ordinal,
    }
}

// -----------------------------------------------------------------------------
// Equal timestamps across streams
// -----------------------------------------------------------------------------

#[test]
fn orders_equal_timestamps_by_stream_rank() {
    let records = merge(vec![
        vec![trade(111, 0, 0, 100)],
        vec![trade(109, 0, 0, 100)],
        vec![trade(115, 0, 0, 100)],
    ]);

    assert_eq!(records.len(), 3);

    assert_eq!(
        records
            .iter()
            .map(|record| record.dataset_id)
            .collect::<Vec<_>>(),
        vec![111, 109, 115],
    );

    assert!(
        records
            .windows(2)
            .all(|pair| key(&pair[0]) <= key(&pair[1]))
    );
}

// -----------------------------------------------------------------------------
// Equal timestamps across tasks
// -----------------------------------------------------------------------------

#[test]
fn orders_equal_timestamps_by_task_and_ordinal() {
    let records = merge(vec![vec![
        trade(111, 0, 0, 100),
        trade(111, 0, 1, 100),
        trade(111, 1, 0, 100),
        trade(111, 1, 1, 100),
    ]]);

    assert_eq!(records.len(), 4);

    let identities = records
        .iter()
        .map(|record| (record.task_id, record.event_ordinal))
        .collect::<Vec<_>>();

    assert_eq!(identities, vec![(0, 0), (0, 1), (1, 0), (1, 1)]);
}

// -----------------------------------------------------------------------------
// Exact partition boundaries
// -----------------------------------------------------------------------------

#[test]
fn assigns_boundary_events_to_exactly_one_partition() {
    let sources = vec![
        vec![
            trade(111, 0, 0, 99),
            trade(111, 0, 1, 100),
            trade(111, 0, 2, 199),
            trade(111, 0, 3, 200),
            trade(111, 0, 4, 299),
            trade(111, 0, 5, 300),
        ],
        vec![
            trade(109, 0, 0, 100),
            trade(109, 0, 1, 200),
            trade(109, 0, 2, 300),
        ],
    ];

    let full = merge(sources);

    let partitions = [(100_i64, 200_i64), (200, 300), (300, 400)];

    let mut partitioned = Vec::new();

    for (start, end) in partitions {
        let selected = full
            .iter()
            .copied()
            .filter(|record| record.event_timestamp_ns >= start && record.event_timestamp_ns < end)
            .collect::<Vec<_>>();

        assert!(selected.iter().all(|record| {
            record.event_timestamp_ns >= start && record.event_timestamp_ns < end
        }));

        partitioned.extend(selected);
    }

    let expected = full
        .into_iter()
        .filter(|record| record.event_timestamp_ns >= 100 && record.event_timestamp_ns < 400)
        .collect::<Vec<_>>();

    assert_eq!(partitioned, expected);

    assert_eq!(partitioned.len(), 8);
}

// -----------------------------------------------------------------------------
// Empty partitions
// -----------------------------------------------------------------------------

#[test]
fn handles_empty_partitions_without_changing_order() {
    let full = merge(vec![vec![trade(111, 0, 0, 100), trade(111, 0, 1, 400)]]);

    let partitions = [(100_i64, 200_i64), (200, 300), (300, 400), (400, 500)];

    let mut counts = Vec::new();
    let mut output = Vec::new();

    for (start, end) in partitions {
        let selected = full
            .iter()
            .copied()
            .filter(|record| record.event_timestamp_ns >= start && record.event_timestamp_ns < end)
            .collect::<Vec<_>>();

        counts.push(selected.len());
        output.extend(selected);
    }

    assert_eq!(counts, vec![1, 0, 0, 1]);
    assert_eq!(output, full);
}

// -----------------------------------------------------------------------------
// Multiple equal-timestamp boundary events
// -----------------------------------------------------------------------------

#[test]
fn preserves_tie_order_at_partition_boundaries() {
    let sources = vec![
        vec![trade(111, 0, 0, 200), trade(111, 0, 1, 200)],
        vec![trade(109, 0, 0, 200), trade(109, 0, 1, 200)],
        vec![trade(115, 0, 0, 200)],
    ];

    let full = merge(sources);

    let previous_partition = full
        .iter()
        .filter(|record| record.event_timestamp_ns < 200)
        .count();

    let next_partition = full
        .iter()
        .filter(|record| record.event_timestamp_ns >= 200 && record.event_timestamp_ns < 300)
        .copied()
        .collect::<Vec<_>>();

    assert_eq!(previous_partition, 0);
    assert_eq!(next_partition.len(), 5);

    assert_eq!(
        next_partition
            .iter()
            .map(|record| record.stream_rank)
            .collect::<Vec<_>>(),
        vec![0, 0, 1, 1, 2]
    );

    assert!(
        next_partition
            .windows(2)
            .all(|pair| key(&pair[0]) <= key(&pair[1]))
    );
}

// -----------------------------------------------------------------------------
// Reject unsorted source
// -----------------------------------------------------------------------------

#[test]
fn rejects_source_ordering_regression() {
    let streams: Vec<Box<dyn MergeEventSource>> = vec![Box::new(SyntheticSource::new(vec![
        trade(111, 0, 0, 200),
        trade(111, 0, 1, 100),
    ]))];

    let mut merger = KWayEventMerger::new(streams).unwrap();

    assert!(
        merger.next_event().is_err(),
        "unsorted source must be rejected"
    );
}
