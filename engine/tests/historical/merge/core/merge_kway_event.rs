#![cfg(feature = "historical")]

use {
    marketforge_engine::historical::merge::KWayEventMerger,
    marketforge_engine::historical::merge::MergeEventRef,
    marketforge_engine::historical::merge::MergeEventSource,
    marketforge_engine::historical::merge::TradeEventRef,
    marketforge_engine::shared::error::Result,
};

// -----------------------------------------------------------------------------
// Synthetic trade source
// -----------------------------------------------------------------------------

struct TestTradeSource {
    dataset_id: u64,
    timestamps: Vec<i64>,
    position: usize,
}

impl TestTradeSource {
    fn new(dataset_id: u64, timestamps: Vec<i64>) -> Self {
        Self {
            dataset_id,
            timestamps,
            position: 0,
        }
    }
}

impl MergeEventSource for TestTradeSource {
    fn next_event(&mut self) -> Result<Option<MergeEventRef>> {
        let Some(&timestamp) = self.timestamps.get(self.position) else {
            return Ok(None);
        };

        let ordinal = self.position as u64;

        self.position += 1;

        Ok(Some(MergeEventRef::Trade(TradeEventRef {
            dataset_id: self.dataset_id,
            task_id: 0,
            event_ordinal: ordinal,
            canonical_row_offset: ordinal,
            event_timestamp_ns: timestamp,
        })))
    }
}

// -----------------------------------------------------------------------------
// Deterministic chronological ordering
// -----------------------------------------------------------------------------

#[test]
fn merges_trade_streams_deterministically() {
    let streams: Vec<Box<dyn MergeEventSource>> = vec![
        Box::new(TestTradeSource::new(100, vec![100, 200, 200, 400])),
        Box::new(TestTradeSource::new(200, vec![100, 150, 200, 500])),
        Box::new(TestTradeSource::new(300, vec![50, 200, 300, 600])),
    ];

    let mut merger = KWayEventMerger::new(streams).expect("construct K-way event merger");

    let mut actual = Vec::new();

    while let Some(event) = merger.next_event().expect("read merged event") {
        actual.push((
            event.event.timestamp_ns(),
            event.stream_rank,
            event.event.event_ordinal(),
        ));
    }

    let expected = vec![
        (50, 2, 0),
        (100, 0, 0),
        (100, 1, 0),
        (150, 1, 1),
        (200, 0, 1),
        (200, 0, 2),
        (200, 1, 2),
        (200, 2, 1),
        (300, 2, 2),
        (400, 0, 3),
        (500, 1, 3),
        (600, 2, 3),
    ];

    assert_eq!(actual, expected);
    assert_eq!(merger.events_emitted(), 12);
}

// -----------------------------------------------------------------------------
// Reject source ordering regressions
// -----------------------------------------------------------------------------

#[test]
fn rejects_unsorted_trade_source() {
    let streams: Vec<Box<dyn MergeEventSource>> =
        vec![Box::new(TestTradeSource::new(100, vec![100, 200, 150]))];

    let mut merger = KWayEventMerger::new(streams).expect("construct K-way event merger");

    assert!(merger.next_event().unwrap().is_some());

    let result = merger.next_event();

    assert!(result.is_err(), "unsorted trade source should be rejected");
}

// -----------------------------------------------------------------------------
// Empty streams
// -----------------------------------------------------------------------------

#[test]
fn handles_empty_sources() {
    let streams: Vec<Box<dyn MergeEventSource>> = vec![
        Box::new(TestTradeSource::new(100, vec![])),
        Box::new(TestTradeSource::new(200, vec![10, 20])),
        Box::new(TestTradeSource::new(300, vec![])),
    ];

    let mut merger = KWayEventMerger::new(streams).expect("construct K-way event merger");

    let mut count = 0;

    while merger.next_event().unwrap().is_some() {
        count += 1;
    }

    assert_eq!(count, 2);
    assert_eq!(merger.events_emitted(), 2);
}
