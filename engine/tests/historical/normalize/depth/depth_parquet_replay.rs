#![cfg(feature = "historical")]

use std::{
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use arrow_array::{Array, Decimal256Array, Int64Array, StringArray, UInt64Array};
use marketforge_engine::historical::normalize::boundary::{
    BoundaryBookState, DepthBoundaryManifest, fingerprint_book,
};
use {
    marketforge_engine::historical::normalize::metrics::ScopedIntegrityMetrics,
    marketforge_engine::historical::normalize::metrics::TaskMetrics,
    marketforge_engine::historical::normalize::worker::DepthSink,
    marketforge_engine::historical::normalize::worker::process_depth_task_with_metrics,
    marketforge_engine::shared::domain::book::BookLevel,
    marketforge_engine::shared::domain::book::BookStore,
    marketforge_engine::shared::domain::canonical::BookSide,
    marketforge_engine::shared::domain::canonical::L2LevelUpdate,
    marketforge_engine::shared::error::Result,
    marketforge_engine::shared::formats::depth::DepthEventBoundary,
    marketforge_engine::shared::formats::depth::DepthProcessingOutcome,
    marketforge_engine::shared::persistence::parquet::ParquetDepthWriter,
    marketforge_engine::shared::persistence::parquet::i256_to_decimal,
    marketforge_engine::shared::persistence::parquet::read_depth_segments,
};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

// -----------------------------------------------------------------------------
// Temporary directory
// -----------------------------------------------------------------------------

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
    fn write_boundary(&mut self, boundary: DepthBoundaryManifest) -> Result<()> {
        self.writer.write_boundary(boundary)
    }
}

// -----------------------------------------------------------------------------
// Parquet files
// -----------------------------------------------------------------------------

fn parquet_files(directory: &Path) -> Vec<PathBuf> {
    let mut files = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("part-") && name.ends_with(".parquet"))
        })
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
fn verify_boundary_replay(directory: &Path, replayed_book: &BookStore, reference_book: &BookStore) {
    // -------------------------------------------------------------------------
    // Load persisted boundary metadata
    // -------------------------------------------------------------------------

    let path = directory.join("boundary.json");

    assert!(
        path.is_file(),
        "missing boundary metadata: {}",
        path.display()
    );

    let file = fs::File::open(&path).unwrap();

    let manifest: DepthBoundaryManifest =
        serde_json::from_reader(file).expect("deserialize boundary metadata");

    let final_boundary = manifest
        .final_state
        .as_ref()
        .expect("missing final boundary state");

    // -------------------------------------------------------------------------
    // Extract reconstructed book states
    // -------------------------------------------------------------------------

    let replayed_state =
        BoundaryBookState::from_book(replayed_book).expect("replayed book must be initialized");

    let reference_state =
        BoundaryBookState::from_book(reference_book).expect("reference book must be initialized");

    // -------------------------------------------------------------------------
    // Exact book-state equivalence
    // -------------------------------------------------------------------------

    assert_eq!(
        reference_state, replayed_state,
        "raw reconstruction differs from Parquet replay"
    );

    assert_eq!(
        replayed_state, final_boundary.state,
        "Parquet replay differs from persisted boundary state"
    );

    // -------------------------------------------------------------------------
    // Fingerprint equivalence
    // -------------------------------------------------------------------------

    let reference_hash = fingerprint_book(&reference_state);
    let replayed_hash = fingerprint_book(&replayed_state);

    assert_eq!(
        reference_hash, replayed_hash,
        "reference and replay fingerprints differ"
    );

    assert_eq!(
        replayed_hash, final_boundary.fingerprint,
        "replayed book fingerprint differs from boundary metadata"
    );

    // -------------------------------------------------------------------------
    // Print verification
    // -------------------------------------------------------------------------

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — BOUNDARY REPLAY EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("\nBOOK STATES");
    println!("  Reference bids     : {}", reference_state.bids.len());
    println!("  Reference asks     : {}", reference_state.asks.len());
    println!("  Replayed bids      : {}", replayed_state.bids.len());
    println!("  Replayed asks      : {}", replayed_state.asks.len());
    println!("  Boundary bids      : {}", final_boundary.state.bids.len());
    println!("  Boundary asks      : {}", final_boundary.state.asks.len());

    println!("\nFINGERPRINTS");
    println!("  Reference          : {reference_hash}");
    println!("  Replay             : {replayed_hash}");
    println!("  Boundary           : {}", final_boundary.fingerprint);

    println!("\nEQUIVALENCE");
    println!("  Raw ↔ Parquet      : MATCH");
    println!("  Parquet ↔ Boundary : MATCH");
    println!("  SHA-256            : MATCH");

    println!("\nBOUNDARY REPLAY RESULT: PASS");
}
// -----------------------------------------------------------------------------
// Real archive equivalence test
// -----------------------------------------------------------------------------

#[test]
fn parquet_replay_matches_reference_book() {
    use crate::support::{fixtures::workload, runtime::TestRun};
    use marketforge_engine::{
        historical::{
            job::{ContentType, InstrumentKind},
            normalize::resolve_sequence_policy,
        },
        shared::{domain::canonical::Exchange, historical::limits::LimitScope},
    };

    let mut batch = workload().expect("load authoritative normalization batch");

    batch
        .jobs
        .retain(|job| job.output.content_type == ContentType::Depth);

    assert_eq!(batch.jobs.len(), 1);

    let job = &mut batch.jobs[0];

    // Select a deterministic Bybit Spot depth source.
    // No instrument, symbol, date, or task ID is hardcoded.
    job.tasks.retain(|task| {
        task.exchange == Exchange::Bybit
            && task.instrument_type == InstrumentKind::Spot
            && task.scope.is_none()
            && task.symbol.is_some()
    });

    job.tasks.sort_by(|a, b| {
        a.stream_id
            .0
            .cmp(&b.stream_id.0)
            .then(a.input_path.cmp(&b.input_path))
    });

    assert!(
        !job.tasks.is_empty(),
        "authoritative batch has no Bybit Spot depth source"
    );

    job.tasks.truncate(1);

    let mut run = TestRun::new("depth/parquet_replay").expect("create isolated depth replay run");

    let config = run
        .runtime_from_general()
        .expect("load production normalization configuration");

    let resolved = run
        .prepare(&batch, &config)
        .expect("resolve selected depth source");

    assert_eq!(resolved.jobs.len(), 1);
    assert_eq!(resolved.jobs[0].tasks.len(), 1);

    let task = &resolved.jobs[0].tasks[0];

    assert!(
        task.input_path.is_file(),
        "missing source archive: {}",
        task.input_path.display()
    );

    let sequence_policy = resolve_sequence_policy(task).expect("resolve depth sequence policy");

    let _limits = LimitScope::enter(
        config.resources.normalization_limits,
        format!(
            "depth replay: task={}, source={}",
            task.task_id.0,
            task.input_path.display()
        ),
    );

    let output_path = run.path.join("depth-parquet");

    run.owned(&output_path)
        .expect("validate isolated output path");

    let writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet depth writer");

    let mut sink = ReferenceSink::new(writer);

    let mut metrics = TaskMetrics::default();

    let previous = ScopedIntegrityMetrics::default();

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — DEPTH PARQUET REPLAY EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("Archive : {}", task.input_path.display());
    println!("Output  : {}", output_path.display());

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
        sequence_policy,
    )
    .expect("process selected depth archive");

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

    let (replayed, replayed_rows) = replay_parquet(&output_path);

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

    verify_boundary_replay(&output_path, &replayed, &sink.book);

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
