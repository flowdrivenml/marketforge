#![cfg(feature = "process")]

use std::path::{Path, PathBuf};

use marketforge_engine::{
    book::SegmentTracker,
    formats::depth::{DepthEventBoundary, DepthProcessingOutcome},
};
use marketforge_engine::{
    book::SequencePolicy,
    canonical::L2LevelUpdate,
    error::Result,
    job::load_processing_job,
    process::{
        load_processing_config,
        metrics::{ScopedIntegrityMetrics, TaskMetrics},
        worker::{DepthSink, process_depth_task_with_metrics},
    },
};

// -----------------------------------------------------------------------------
// Test configuration
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Counting sink
// -----------------------------------------------------------------------------

#[derive(Default)]
struct CountingDepthSink {
    levels_written: u64,
    bid_updates: u64,
    ask_updates: u64,
    deletions: u64,

    first_timestamp_ns: Option<i64>,
    last_timestamp_ns: Option<i64>,

    samples: Vec<L2LevelUpdate>,

    segments: SegmentTracker,
}

impl DepthSink for CountingDepthSink {
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()> {
        let event_count = outcome.events.len();

        // ---------------------------------------------------------------------
        // Validate outcome
        // ---------------------------------------------------------------------

        if outcome.boundary == DepthEventBoundary::Initialization && event_count == 0 {
            return Err(
                marketforge_engine::error::MarketForgeError::InvalidConfiguration(
                    "empty depth initialization".to_owned(),
                ),
            );
        }

        // ---------------------------------------------------------------------
        // Record reconstruction segment
        // ---------------------------------------------------------------------

        match outcome.boundary {
            DepthEventBoundary::Initialization => {
                let timestamp = outcome.events[0].envelope.event_timestamp_ns;

                self.segments.begin_segment(timestamp, event_count)?;
            }

            DepthEventBoundary::Changes => {
                if self.segments.segments().is_empty() {
                    return Err(
                        marketforge_engine::error::MarketForgeError::InvalidConfiguration(
                            "depth changes received before initialization".to_owned(),
                        ),
                    );
                }

                self.segments.record_changes(event_count)?;
            }
        }

        // ---------------------------------------------------------------------
        // Process canonical L2 levels
        // ---------------------------------------------------------------------

        for level in outcome.events {
            // Retain the first 20 normalized levels for inspection.
            if self.samples.len() < 20 {
                self.samples.push(level.clone());
            }

            self.levels_written += 1;

            match level.side {
                marketforge_engine::canonical::BookSide::Bid => {
                    self.bid_updates += 1;
                }

                marketforge_engine::canonical::BookSide::Ask => {
                    self.ask_updates += 1;
                }
            }

            // -------------------------------------------------------------
            // Detect level deletion
            // -------------------------------------------------------------

            let is_deletion = [
                level.quantity_base,
                level.quantity_quote,
                level.quantity_contracts,
            ]
            .into_iter()
            .flatten()
            .all(|quantity| quantity.is_zero());

            if is_deletion {
                self.deletions += 1;
            }

            // -------------------------------------------------------------
            // Timestamp tracking
            // -------------------------------------------------------------

            let timestamp = level.envelope.event_timestamp_ns;

            if self.first_timestamp_ns.is_none() {
                self.first_timestamp_ns = Some(timestamp);
            }

            self.last_timestamp_ns = Some(timestamp);
        }

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}
// -----------------------------------------------------------------------------
// Real archive integration test
// -----------------------------------------------------------------------------

#[test]
fn processes_real_bybit_depth_archive() {
    let root = project_root();

    let job_path =
        root.join("data/.jobs/process/bybit-spot-spot-BTCUSDT-l2-20260901-20260904-d105.json");

    let job = load_processing_job(&job_path).expect("load Bybit depth job");

    let task = &job.tasks[0];

    assert!(
        task.input_path.is_file(),
        "missing archive: {}",
        task.input_path.display()
    );

    let config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing config");

    let mut sink = CountingDepthSink::default();
    let mut metrics = TaskMetrics::default();

    let previous = ScopedIntegrityMetrics::default();

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — REAL BYBIT DEPTH ARCHIVE");
    println!("{}", "=".repeat(90));

    println!("Archive : {}", task.input_path.display());
    println!("Format  : {}", task.format_code.0);
    println!("Symbol  : {}", task.symbol);

    let start = std::time::Instant::now();

    let result = process_depth_task_with_metrics(
        task,
        &mut sink,
        &mut metrics,
        &config.integrity_policy,
        &previous,
        SequencePolicy::Consecutive,
    );

    let elapsed = start.elapsed().as_secs_f64();

    println!("\nRESULTS");
    println!("  Elapsed             : {elapsed:.3}s");
    println!("  Source records      : {}", metrics.counters.records_read);
    println!(
        "  Matched records     : {}",
        metrics.counters.records_matched
    );
    println!(
        "  Rejected records    : {}",
        metrics.counters.records_rejected
    );
    println!("  Canonical levels    : {}", sink.levels_written);
    println!("  Bid updates         : {}", sink.bid_updates);
    println!("  Ask updates         : {}", sink.ask_updates);
    println!("  Level deletions     : {}", sink.deletions);
    println!("  First timestamp     : {:?}", sink.first_timestamp_ns);
    println!("  Last timestamp      : {:?}", sink.last_timestamp_ns);

    println!(
        "  Throughput          : {:.0} source records/sec",
        metrics.counters.records_read as f64 / elapsed.max(f64::EPSILON)
    );

    if let Err(error) = &result {
        println!("  Processing error    : {error}");
    }

    println!(
        "  RESULT              : {}",
        if result.is_ok() { "PASS" } else { "FAIL" }
    );

    result.expect("real Bybit depth processing failed");

    assert!(metrics.counters.records_read > 0);
    assert!(sink.levels_written > 0);
    assert_eq!(metrics.counters.records_rejected, 0);

    assert_eq!(metrics.counters.events_written, sink.levels_written);

    assert_eq!(sink.bid_updates + sink.ask_updates, sink.levels_written);

    println!("\n{}", "=".repeat(110));
    println!("CANONICAL L2 LEVEL UPDATES");
    println!("{}", "=".repeat(110));

    println!(
        "{:<20} {:<6} {:<14} {:<18} {:<18} {:<18} {:<10}",
        "TIMESTAMP_NS",
        "SIDE",
        "PRICE",
        "QUANTITY_BASE",
        "QUANTITY_QUOTE",
        "QUANTITY_CONTRACTS",
        "ORDERS"
    );

    for event in &sink.samples {
        println!(
            "{:<20} {:<6} {:<14} {:<18} {:<18} {:<18} {:<10}",
            event.envelope.event_timestamp_ns,
            format!("{:?}", event.side),
            event.price,
            event
                .quantity_base
                .map_or("-".to_owned(), |value| value.to_string()),
            event
                .quantity_quote
                .map_or("-".to_owned(), |value| value.to_string()),
            event
                .quantity_contracts
                .map_or("-".to_owned(), |value| value.to_string()),
            event
                .order_count
                .map_or("-".to_owned(), |value| value.to_string()),
        );
    }
    println!("\nRECONSTRUCTION SEGMENTS");
    println!("  Segments: {}", sink.segments.segments().len());

    for segment in sink.segments.segments() {
        println!(
            "  Segment {} | timestamp={} | offset={} | initial_levels={}",
            segment.segment_id,
            segment.start_timestamp_ns,
            segment.initial_event_offset,
            segment.initial_level_count,
        );
    }

    assert!(!sink.segments.segments().is_empty());

    assert_eq!(sink.segments.next_event_offset(), sink.levels_written);
}
