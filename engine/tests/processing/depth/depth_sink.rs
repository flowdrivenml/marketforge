#![cfg(feature = "process")]

use marketforge_engine::{
    book::SegmentTracker,
    canonical::{BookSide, EventEnvelope, Exchange, L2LevelUpdate},
    error::{MarketForgeError, Result},
    formats::depth::{DepthEventBoundary, DepthProcessingOutcome},
    process::worker::DepthSink,
};

use marketforge_engine::process::boundary::DepthBoundaryManifest;
use rust_decimal::Decimal;
// -----------------------------------------------------------------------------
// In-memory depth sink
// -----------------------------------------------------------------------------

#[derive(Debug, Default)]
struct TestDepthSink {
    events: Vec<L2LevelUpdate>,
    segments: SegmentTracker,
    finished: bool,
    failed: bool,
}

impl DepthSink for TestDepthSink {
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()> {
        if self.finished || self.failed {
            return Err(MarketForgeError::InvalidConfiguration(
                "cannot write to finished or failed depth sink".to_owned(),
            ));
        }

        let count = outcome.events.len();

        // ---------------------------------------------------------------------
        // Validate before modifying sink state
        // ---------------------------------------------------------------------

        match outcome.boundary {
            DepthEventBoundary::Initialization => {
                if count == 0 {
                    return Err(MarketForgeError::InvalidConfiguration(
                        "empty depth initialization".to_owned(),
                    ));
                }
            }

            DepthEventBoundary::Changes => {
                if self.segments.segments().is_empty() {
                    return Err(MarketForgeError::InvalidConfiguration(
                        "depth changes before initialization".to_owned(),
                    ));
                }
            }
        }

        // Every level in one outcome must belong to the same source event.
        if let Some(first) = outcome.events.first() {
            if outcome
                .events
                .iter()
                .any(|event| event.envelope.event_timestamp_ns != first.envelope.event_timestamp_ns)
            {
                return Err(MarketForgeError::InvalidConfiguration(
                    "depth outcome contains multiple event timestamps".to_owned(),
                ));
            }
        }

        // ---------------------------------------------------------------------
        // Prepare segment changes
        // ---------------------------------------------------------------------

        // Clone first so segment-tracking failures cannot partially modify
        // the sink's existing state.
        //
        // This is acceptable for tests. The production Parquet writer should
        // avoid cloning the complete segment history for every outcome.
        let mut next_segments = self.segments.clone();

        match outcome.boundary {
            DepthEventBoundary::Initialization => {
                let timestamp = outcome.events[0].envelope.event_timestamp_ns;

                next_segments.begin_segment(timestamp, count)?;
            }

            DepthEventBoundary::Changes => {
                next_segments.record_changes(count)?;
            }
        }

        // ---------------------------------------------------------------------
        // Commit accepted outcome
        // ---------------------------------------------------------------------

        self.events.extend(outcome.events);
        self.segments = next_segments;

        Ok(())
    }
    fn write_boundary(&mut self, _boundary: DepthBoundaryManifest) -> Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        if self.failed {
            return Err(MarketForgeError::InvalidConfiguration(
                "cannot finish failed depth sink".to_owned(),
            ));
        }

        self.finished = true;

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Test helpers
// -----------------------------------------------------------------------------

fn level(timestamp_ns: i64, side: BookSide, price: i64, quantity: i64) -> L2LevelUpdate {
    let price = Decimal::new(price, 0);
    let quantity = Decimal::new(quantity, 0);

    L2LevelUpdate {
        envelope: EventEnvelope {
            event_timestamp_ns: timestamp_ns,
            system_timestamp_ns: None,
            exchange: Exchange::Bybit,
            instrument_id: 1,
            symbol: "BTCUSDT".to_owned(),
            stream_id: "bybit:BTCUSDT:depth".to_owned(),
        },

        side,
        price,

        quantity_base: Some(quantity),
        quantity_quote: Some(price * quantity),
        quantity_contracts: None,

        order_count: None,
    }
}

fn initialization(timestamp_ns: i64, levels: &[(BookSide, i64, i64)]) -> DepthProcessingOutcome {
    DepthProcessingOutcome::initialization(
        levels
            .iter()
            .map(|(side, price, quantity)| level(timestamp_ns, *side, *price, *quantity))
            .collect(),
    )
}

fn changes(timestamp_ns: i64, levels: &[(BookSide, i64, i64)]) -> DepthProcessingOutcome {
    DepthProcessingOutcome::changes(
        levels
            .iter()
            .map(|(side, price, quantity)| level(timestamp_ns, *side, *price, *quantity))
            .collect(),
    )
}

// -----------------------------------------------------------------------------
// Initial snapshot
// -----------------------------------------------------------------------------

#[test]
fn accepts_initial_snapshot() {
    let mut sink = TestDepthSink::default();

    sink.write_outcome(initialization(
        1000,
        &[
            (BookSide::Bid, 100, 10),
            (BookSide::Bid, 99, 20),
            (BookSide::Ask, 101, 15),
        ],
    ))
    .unwrap();

    assert_eq!(sink.events.len(), 3);
    assert_eq!(sink.segments.segments().len(), 1);

    let segment = &sink.segments.segments()[0];

    assert_eq!(segment.segment_id, 0);
    assert_eq!(segment.start_timestamp_ns, 1000);
    assert_eq!(segment.initial_event_offset, 0);
    assert_eq!(segment.initial_level_count, 3);

    assert_eq!(sink.segments.next_event_offset(), 3);
}

// -----------------------------------------------------------------------------
// Incremental updates
// -----------------------------------------------------------------------------

#[test]
fn accepts_incremental_changes() {
    let mut sink = TestDepthSink::default();

    sink.write_outcome(initialization(1000, &[(BookSide::Bid, 100, 10)]))
        .unwrap();

    sink.write_outcome(changes(
        1001,
        &[(BookSide::Bid, 100, 5), (BookSide::Ask, 101, 15)],
    ))
    .unwrap();

    assert_eq!(sink.events.len(), 3);
    assert_eq!(sink.segments.segments().len(), 1);
    assert_eq!(sink.segments.next_event_offset(), 3);

    assert_eq!(sink.events[1].quantity_base, Some(Decimal::new(5, 0)));
    assert_eq!(sink.events[2].side, BookSide::Ask);
}

// -----------------------------------------------------------------------------
// Recovery segment
// -----------------------------------------------------------------------------

#[test]
fn creates_recovery_segment() {
    let mut sink = TestDepthSink::default();

    sink.write_outcome(initialization(1000, &[(BookSide::Bid, 100, 10)]))
        .unwrap();

    sink.write_outcome(changes(1001, &[(BookSide::Bid, 100, 5)]))
        .unwrap();

    sink.write_outcome(initialization(
        2000,
        &[(BookSide::Bid, 100, 8), (BookSide::Ask, 101, 12)],
    ))
    .unwrap();

    assert_eq!(sink.segments.segments().len(), 2);

    let recovery = &sink.segments.segments()[1];

    assert_eq!(recovery.segment_id, 1);
    assert_eq!(recovery.start_timestamp_ns, 2000);
    assert_eq!(recovery.initial_event_offset, 2);
    assert_eq!(recovery.initial_level_count, 2);

    assert_eq!(sink.segments.next_event_offset(), 4);
}

// -----------------------------------------------------------------------------
// Zero-change source events
// -----------------------------------------------------------------------------

#[test]
fn accepts_zero_change_outcomes() {
    let mut sink = TestDepthSink::default();

    sink.write_outcome(initialization(1000, &[(BookSide::Bid, 100, 10)]))
        .unwrap();

    sink.write_outcome(DepthProcessingOutcome::changes(vec![]))
        .unwrap();

    assert_eq!(sink.events.len(), 1);
    assert_eq!(sink.segments.next_event_offset(), 1);
}

// -----------------------------------------------------------------------------
// Missing initialization
// -----------------------------------------------------------------------------

#[test]
fn rejects_changes_before_initialization() {
    let mut sink = TestDepthSink::default();

    let result = sink.write_outcome(changes(1000, &[(BookSide::Bid, 100, 10)]));

    assert!(result.is_err());
    assert!(sink.events.is_empty());
    assert!(sink.segments.segments().is_empty());
}

// -----------------------------------------------------------------------------
// Empty initialization
// -----------------------------------------------------------------------------

#[test]
fn rejects_empty_initialization() {
    let mut sink = TestDepthSink::default();

    let result = sink.write_outcome(DepthProcessingOutcome::initialization(vec![]));

    assert!(result.is_err());
    assert!(sink.events.is_empty());
    assert!(sink.segments.segments().is_empty());
}

// -----------------------------------------------------------------------------
// Event ordering
// -----------------------------------------------------------------------------

#[test]
fn preserves_event_order() {
    let mut sink = TestDepthSink::default();

    sink.write_outcome(initialization(
        1000,
        &[
            (BookSide::Bid, 100, 10),
            (BookSide::Bid, 99, 20),
            (BookSide::Ask, 101, 15),
        ],
    ))
    .unwrap();

    sink.write_outcome(changes(
        1001,
        &[(BookSide::Ask, 102, 5), (BookSide::Bid, 100, 0)],
    ))
    .unwrap();

    let prices: Vec<_> = sink.events.iter().map(|event| event.price).collect();

    assert_eq!(
        prices,
        vec![
            Decimal::new(100, 0),
            Decimal::new(99, 0),
            Decimal::new(101, 0),
            Decimal::new(102, 0),
            Decimal::new(100, 0),
        ]
    );

    assert_eq!(sink.segments.next_event_offset(), 5);
}

// -----------------------------------------------------------------------------
// Atomic rejection
// -----------------------------------------------------------------------------

#[test]
fn rejects_invalid_outcome_atomically() {
    let mut sink = TestDepthSink::default();

    sink.write_outcome(initialization(1000, &[(BookSide::Bid, 100, 10)]))
        .unwrap();

    let previous_events = sink.events.clone();
    let previous_offset = sink.segments.next_event_offset();

    // One outcome contains two different source-event timestamps.
    let invalid = DepthProcessingOutcome::changes(vec![
        level(1001, BookSide::Bid, 100, 5),
        level(1002, BookSide::Ask, 101, 10),
    ]);

    assert!(sink.write_outcome(invalid).is_err());

    assert_eq!(sink.events, previous_events);
    assert_eq!(sink.segments.next_event_offset(), previous_offset);
}

// -----------------------------------------------------------------------------
// Finish behavior
// -----------------------------------------------------------------------------

#[test]
fn rejects_writes_after_finish() {
    let mut sink = TestDepthSink::default();

    sink.write_outcome(initialization(1000, &[(BookSide::Bid, 100, 10)]))
        .unwrap();

    sink.finish().unwrap();

    assert!(
        sink.write_outcome(changes(1001, &[(BookSide::Bid, 100, 5)],))
            .is_err()
    );
}
