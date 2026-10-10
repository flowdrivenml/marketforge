#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use arrow_array::{Array, Decimal256Array, Int64Array, StringArray, UInt64Array};

use marketforge_engine::{
    book::{BookLevel, BookStore, SequencePolicy},
    canonical::{BookSide, L2LevelUpdate},
    error::Result,
    formats::depth::{DepthEventBoundary, DepthProcessingOutcome},
    job::load_processing_job,
    process::{
        load_processing_config,
        metrics::{ScopedIntegrityMetrics, TaskMetrics},
        parquet::{ParquetDepthWriter, i256_to_decimal, read_depth_segments},
        worker::{DepthSink, process_depth_task_with_metrics},
    },
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// -----------------------------------------------------------------------------
// Temporary directory
// -----------------------------------------------------------------------------

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-depth-replay-{}-{id}",
            std::process::id()
        ));

        fs::create_dir_all(&path).unwrap();

        Self { path }
    }
}

impl Drop for TestDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Canonical level conversion
// -----------------------------------------------------------------------------

fn book_level(event: &L2LevelUpdate) -> BookLevel {
    BookLevel {
        quantity_base: event.quantity_base,
        quantity_quote: event.quantity_quote,
        quantity_contracts: event.quantity_contracts,
        order_count: event.order_count,
    }
}

// -----------------------------------------------------------------------------
// Reference sink
// -----------------------------------------------------------------------------

/// Maintains a reference book directly from canonical processor outcomes,
/// while forwarding the same outcomes to the Parquet writer.
struct ReferenceSink {
    writer: ParquetDepthWriter,
    book: BookStore,
    outcomes: u64,
}

impl ReferenceSink {
    fn new(writer: ParquetDepthWriter) -> Self {
        Self {
            writer,
            book: BookStore::new(),
            outcomes: 0,
        }
    }
}

impl DepthSink for ReferenceSink {
    fn write_outcome(&mut self, outcome: DepthProcessingOutcome) -> Result<()> {
        // Construct the reference state from the processor's canonical
        // output, independently of Parquet serialization.

        let mut bids = Vec::new();
        let mut asks = Vec::new();
        let mut changes = Vec::new();

        for event in &outcome.events {
            let level = book_level(event);

            match outcome.boundary {
                DepthEventBoundary::Initialization => match event.side {
                    BookSide::Bid => bids.push((event.price, level)),
                    BookSide::Ask => asks.push((event.price, level)),
                },

                DepthEventBoundary::Changes => {
                    changes.push((event.side, event.price, level));
                }
            }
        }

        // Write first. If Parquet rejects the outcome, do not modify
        // the reference book.
        self.writer.write_outcome(outcome)?;

        match if !bids.is_empty() || !asks.is_empty() {
            // An initialization replaces the complete reference state.
            self.book.clear();
            self.book.apply_snapshot(bids, asks)
        } else if !changes.is_empty() {
            self.book.apply_batch(changes)
        } else {
            Ok(Vec::new())
        } {
            Ok(_) => {}
            Err(error) => return Err(error),
        }

        self.outcomes += 1;

        Ok(())
    }

    fn finish(&mut self) -> Result<()> {
        self.writer.finish()
    }
}

// -----------------------------------------------------------------------------
// Parquet files
// -----------------------------------------------------------------------------

fn parquet_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|extension| extension.to_str()) == Some("parquet"))
        .collect::<Vec<_>>();

    files.sort();

    files
}

// -----------------------------------------------------------------------------
// Arrow column helpers
// -----------------------------------------------------------------------------

fn decimal_at(array: &Decimal256Array, index: usize) -> Option<rust_decimal::Decimal> {
    if array.is_null(index) {
        None
    } else {
        Some(i256_to_decimal(array.value(index)).unwrap())
    }
}

// -----------------------------------------------------------------------------
// Replay Parquet
// -----------------------------------------------------------------------------

fn replay_parquet(directory: &Path) -> (BookStore, u64) {
    let manifest = read_depth_segments(directory).unwrap();

    let files = parquet_files(directory);

    assert!(!files.is_empty());
    assert!(!manifest.segments.is_empty());

    let mut book = BookStore::new();

    let mut row_offset = 0u64;
    let mut segment_index = 0usize;

    let mut initialization_bids = Vec::new();
    let mut initialization_asks = Vec::new();

    for path in files {
        let file = fs::File::open(&path).unwrap();

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .with_batch_size(8192)
            .build()
            .unwrap();

        for batch in reader {
            let batch = batch.unwrap();

            let timestamps = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();

            let sides = batch
                .column_by_name("side")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let prices = batch
                .column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            let quantities_base = batch
                .column_by_name("quantity_base")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            let quantities_quote = batch
                .column_by_name("quantity_quote")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            let quantities_contracts = batch
                .column_by_name("quantity_contracts")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();

            let order_counts = batch
                .column_by_name("order_count")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();

            for index in 0..batch.num_rows() {
                let side = match sides.value(index) {
                    "bid" => BookSide::Bid,
                    "ask" => BookSide::Ask,
                    other => panic!("invalid depth side: {other}"),
                };

                let price = decimal_at(prices, index).unwrap();

                let level = BookLevel {
                    quantity_base: decimal_at(quantities_base, index),
                    quantity_quote: decimal_at(quantities_quote, index),
                    quantity_contracts: decimal_at(quantities_contracts, index),

                    order_count: if order_counts.is_null(index) {
                        None
                    } else {
                        Some(order_counts.value(index))
                    },
                };

                // ---------------------------------------------------------
                // Segment initialization
                // ---------------------------------------------------------

                if segment_index < manifest.segments.len() {
                    let segment = &manifest.segments[segment_index];

                    let start = segment.initial_event_offset;

                    let end = start.checked_add(segment.initial_level_count).unwrap();

                    if row_offset == start {
                        book.clear();

                        initialization_bids.clear();
                        initialization_asks.clear();
                    }

                    if row_offset >= start && row_offset < end {
                        assert_eq!(timestamps.value(index), segment.start_timestamp_ns);

                        match side {
                            BookSide::Bid => {
                                initialization_bids.push((price, level));
                            }

                            BookSide::Ask => {
                                initialization_asks.push((price, level));
                            }
                        }

                        row_offset += 1;

                        if row_offset == end {
                            book.apply_snapshot(
                                std::mem::take(&mut initialization_bids),
                                std::mem::take(&mut initialization_asks),
                            )
                            .unwrap();

                            segment_index += 1;
                        }

                        continue;
                    }
                }

                // ---------------------------------------------------------
                // Absolute level update
                // ---------------------------------------------------------

                assert!(
                    book.is_initialized(),
                    "replay update before initialization at row {row_offset}"
                );

                book.set_level(side, price, level).unwrap();

                row_offset += 1;
            }
        }
    }

    assert_eq!(row_offset, manifest.total_levels);

    assert_eq!(
        segment_index,
        manifest.segments.len(),
        "not all reconstruction segments were initialized"
    );

    assert!(book.is_initialized());

    (book, row_offset)
}

// -----------------------------------------------------------------------------
// Real archive equivalence test
// -----------------------------------------------------------------------------

#[test]
fn parquet_replay_matches_reference_book() {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/bybit-spot-spot-BTCUSDT-l2-20260901-20260904-d105.json"),
    )
    .expect("load Bybit depth job");

    let task = &job.tasks[0];

    assert!(
        task.input_path.is_file(),
        "missing source archive: {}",
        task.input_path.display()
    );

    let config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    let directory = TestDirectory::new();

    let writer = ParquetDepthWriter::new(&directory.path, config.resources.parquet.clone())
        .expect("create Parquet depth writer");

    let mut sink = ReferenceSink::new(writer);

    let mut metrics = TaskMetrics::default();

    let previous = ScopedIntegrityMetrics::default();

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — DEPTH PARQUET REPLAY EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("Archive : {}", task.input_path.display());
    println!("Output  : {}", directory.path.display());

    // -------------------------------------------------------------------------
    // Process archive
    // -------------------------------------------------------------------------

    let processing_start = Instant::now();

    process_depth_task_with_metrics(
        task,
        &mut sink,
        &mut metrics,
        &config.integrity_policy,
        &previous,
        SequencePolicy::Consecutive,
    )
    .expect("process Bybit depth archive");

    sink.finish().expect("finalize Parquet writer");

    let processing_elapsed = processing_start.elapsed().as_secs_f64();

    assert_eq!(metrics.counters.records_rejected, 0);

    // -------------------------------------------------------------------------
    // Reference state
    // -------------------------------------------------------------------------

    assert!(sink.book.is_initialized());

    let reference_bids = sink.book.bids().clone();
    let reference_asks = sink.book.asks().clone();

    let expected_rows = sink.writer.metrics().levels_written;

    println!("\nREFERENCE BOOK");
    println!("  Bids             : {}", reference_bids.len());
    println!("  Asks             : {}", reference_asks.len());
    println!("  Canonical levels : {expected_rows}");
    println!("  Processing time  : {processing_elapsed:.3}s");

    // -------------------------------------------------------------------------
    // Replay persisted Parquet
    // -------------------------------------------------------------------------

    let replay_start = Instant::now();

    let (replayed, replayed_rows) = replay_parquet(&directory.path);

    let replay_elapsed = replay_start.elapsed().as_secs_f64();

    // -------------------------------------------------------------------------
    // Exact state comparison
    // -------------------------------------------------------------------------

    assert_eq!(replayed_rows, expected_rows);

    assert_eq!(
        replayed.bids(),
        &reference_bids,
        "replayed bids differ from reference"
    );

    assert_eq!(
        replayed.asks(),
        &reference_asks,
        "replayed asks differ from reference"
    );

    // -------------------------------------------------------------------------
    // Results
    // -------------------------------------------------------------------------

    println!("\nREPLAY RESULTS");
    println!("  Rows replayed     : {replayed_rows}");
    println!("  Replayed bids     : {}", replayed.bids().len());
    println!("  Replayed asks     : {}", replayed.asks().len());
    println!("  Replay time       : {replay_elapsed:.3}s");

    println!(
        "  Replay throughput : {:.0} levels/sec",
        replayed_rows as f64 / replay_elapsed.max(f64::EPSILON)
    );

    println!("\nEQUIVALENCE");
    println!("  Bid state         : MATCH");
    println!("  Ask state         : MATCH");
    println!("  Canonical rows    : MATCH");

    println!("\nRESULT: PASS");
}
