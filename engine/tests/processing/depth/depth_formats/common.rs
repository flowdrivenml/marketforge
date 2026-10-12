use marketforge_engine::{
    book::SequencePolicy,
    job::load_processing_job,
    process::{
        boundary::{DepthBoundaryManifest, fingerprint_book},
        load_processing_config,
        metrics::{ScopedIntegrityMetrics, TaskMetrics},
        parquet::ParquetDepthWriter,
        worker::{DepthSink, process_depth_task_with_metrics},
    },
};
use std::collections::BTreeMap;
use std::{
    fs::{self, File},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use std::{path::Path, time::Instant};

use arrow_array::{Array, Decimal256Array, Int64Array, StringArray, UInt64Array};

use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::{
    book::{BookLevel, BookStore},
    canonical::BookSide,
    process::{
        boundary::BoundaryBookState,
        parquet::{i256_to_decimal, read_depth_segments},
    },
};

use rust_decimal::Decimal;

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

pub struct ExpectedDepthLevel {
    pub side: BookSide,
    pub price: &'static str,
    pub quantity_base: &'static str,
    pub quantity_quote: &'static str,
    pub quantity_contracts: Option<&'static str>,
    pub base_tolerance: Option<&'static str>,
}

pub struct ExpectedDepthEvent {
    pub source_ordinal: u64,
    pub side: BookSide,
    pub price: &'static str,
    pub quantity_base: Option<&'static str>,
    pub quantity_quote: Option<&'static str>,
    pub quantity_contracts: Option<&'static str>,
    pub base_tolerance: Option<&'static str>,
}

pub struct TestDirectory {
    pub path: PathBuf,
}

impl TestDirectory {
    pub fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-depth-format-{}-{id}",
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

pub fn project_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

pub struct DepthFormatResult {
    pub records_read: u64,
    pub records_processed: u64,
    pub records_rejected: u64,
    pub levels_written: u64,
    pub boundary: DepthBoundaryManifest,
    pub segment_count: usize,
}

pub fn verify_snapshot_quantities(
    boundary: &DepthBoundaryManifest,
    expected: &[ExpectedDepthLevel],
) {
    let initial = boundary
        .initial
        .as_ref()
        .expect("missing initial boundary snapshot");

    for sample in expected {
        let levels = match sample.side {
            BookSide::Bid => &initial.state.bids,
            BookSide::Ask => &initial.state.asks,
        };

        let price: Decimal = sample.price.parse().unwrap();

        let level = levels
            .iter()
            .find(|level| level.price == price)
            .unwrap_or_else(|| {
                panic!(
                    "missing snapshot level: side={:?}, price={}",
                    sample.side, sample.price
                )
            });

        let expected_base: Decimal = sample.quantity_base.parse().unwrap();

        let expected_quote: Decimal = sample.quantity_quote.parse().unwrap();

        let actual_base = level
            .quantity_base
            .expect("missing canonical base quantity");

        if let Some(tolerance) = sample.base_tolerance {
            let tolerance: Decimal = tolerance.parse().unwrap();

            assert!(
                (actual_base - expected_base).abs() <= tolerance,
                "base quantity mismatch at price {}: expected {}, actual {}",
                sample.price,
                expected_base,
                actual_base
            );
        } else {
            assert_eq!(
                actual_base, expected_base,
                "base quantity mismatch at price {}",
                sample.price
            );
        }

        assert_eq!(
            level.quantity_quote,
            Some(expected_quote),
            "quote quantity mismatch at price {}",
            sample.price
        );

        let expected_contracts = sample
            .quantity_contracts
            .map(|value| value.parse::<Decimal>().unwrap());

        assert_eq!(
            level.quantity_contracts, expected_contracts,
            "contract quantity mismatch at price {}",
            sample.price
        );

        println!(
            "  VERIFIED | {:?} | price={} | base={} | quote={} | contracts={:?}",
            sample.side, sample.price, actual_base, expected_quote, expected_contracts
        );
    }
}
pub fn run_depth_format_test(
    job_filename: &str,
    sequence_policy: SequencePolicy,
) -> DepthFormatResult {
    run_depth_format_test_with_events(job_filename, sequence_policy, &[])
}
pub fn run_depth_format_test_with_events(
    job_filename: &str,
    sequence_policy: SequencePolicy,
    expected_events: &[ExpectedDepthEvent],
) -> DepthFormatResult {
    let root = project_root();

    let job = load_processing_job(root.join("data/.jobs/process").join(job_filename))
        .expect("load depth processing job");

    let task = &job.tasks[0];

    assert!(
        task.input_path.is_file(),
        "missing source archive: {}",
        task.input_path.display()
    );

    let config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    let directory = TestDirectory::new();

    let mut writer = ParquetDepthWriter::new(&directory.path, config.resources.parquet.clone())
        .expect("create Parquet depth writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        sequence_policy,
    )
    .expect("process depth archive");

    writer.finish().expect("finalize Parquet depth writer");

    let segments = read_depth_segments(&directory.path).expect("read reconstruction segments");

    segments
        .validate()
        .expect("validate reconstruction segments");

    let segment_count = segments.segments.len();

    println!("\nRECONSTRUCTION SEGMENTS");
    println!("  Segments         : {segment_count}");
    println!("  Total levels     : {}", segments.total_levels);

    for segment in &segments.segments {
        println!(
            "  Segment {} | offset={} | initial_levels={} | timestamp={}",
            segment.segment_id,
            segment.initial_event_offset,
            segment.initial_level_count,
            segment.start_timestamp_ns,
        );
    }

    let boundary: DepthBoundaryManifest = serde_json::from_reader(
        File::open(directory.path.join("boundary.json")).expect("open boundary metadata"),
    )
    .expect("deserialize boundary metadata");

    assert_eq!(
        segments.total_levels,
        writer.metrics().levels_written,
        "segment manifest row count differs from Parquet writer"
    );

    assert_eq!(boundary.version, 1);

    if let Some(initial) = &boundary.initial {
        assert_eq!(initial.fingerprint, fingerprint_book(&initial.state));
    }

    if let Some(final_state) = &boundary.final_state {
        assert_eq!(
            final_state.fingerprint,
            fingerprint_book(&final_state.state)
        );
    }
    verify_parquet_replay(&directory.path, &boundary, writer.metrics().levels_written);
    verify_source_events(&directory.path, expected_events);
    if !expected_events.is_empty() {
        verify_source_events(&directory.path, expected_events);
    }

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — DEPTH FORMAT VALIDATION");
    println!("{}", "=".repeat(90));

    println!("Job              : {job_filename}");
    println!("Source records   : {}", metrics.counters.records_read);
    println!("Processed records: {}", metrics.counters.records_processed);
    println!("Rejected records : {}", metrics.counters.records_rejected);
    println!("Canonical levels : {}", writer.metrics().levels_written);
    println!("Continuity       : {:?}", boundary.continuity);

    if let Some(initial) = &boundary.initial {
        println!(
            "Initial book     : {} bids / {} asks",
            initial.state.bids.len(),
            initial.state.asks.len()
        );
    }

    if let Some(final_state) = &boundary.final_state {
        println!(
            "Final book       : {} bids / {} asks",
            final_state.state.bids.len(),
            final_state.state.asks.len()
        );
    }

    DepthFormatResult {
        records_read: metrics.counters.records_read,
        records_processed: metrics.counters.records_processed,
        records_rejected: metrics.counters.records_rejected,
        levels_written: writer.metrics().levels_written,
        segment_count,
        boundary,
    }
}

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

fn decimal_at(array: &Decimal256Array, index: usize) -> Option<Decimal> {
    if array.is_null(index) {
        None
    } else {
        Some(i256_to_decimal(array.value(index)).unwrap())
    }
}

fn read_source_event_offsets(directory: &Path) -> BTreeMap<u64, (u64, u64)> {
    let file = File::open(directory.join("events.parquet")).expect("open source-event index");

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("open event-index reader")
        .build()
        .expect("build event-index reader");

    let mut offsets = BTreeMap::new();

    for batch in reader {
        let batch = batch.expect("read event-index batch");

        let ordinals = batch
            .column_by_name("event_ordinal")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let row_offsets = batch
            .column_by_name("canonical_row_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let row_counts = batch
            .column_by_name("canonical_row_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for index in 0..batch.num_rows() {
            let ordinal = ordinals.value(index);

            assert!(
                offsets
                    .insert(
                        ordinal,
                        (row_offsets.value(index), row_counts.value(index),),
                    )
                    .is_none(),
                "duplicate source-event ordinal: {ordinal}"
            );
        }
    }

    offsets
}

pub fn verify_source_events(directory: &Path, expected: &[ExpectedDepthEvent]) {
    if expected.is_empty() {
        return;
    }

    let offsets = read_source_event_offsets(directory);

    // Resolve the expected global canonical row offsets.
    let mut targets = BTreeMap::<u64, &ExpectedDepthEvent>::new();

    for sample in expected {
        let &(offset, count) = offsets
            .get(&sample.source_ordinal)
            .unwrap_or_else(|| panic!("missing source-event ordinal {}", sample.source_ordinal));

        assert_eq!(
            count, 1,
            "expected exactly one canonical row for source ordinal {}",
            sample.source_ordinal
        );

        assert!(
            targets.insert(offset, sample).is_none(),
            "multiple expected events reference canonical row {offset}"
        );
    }

    let mut global_offset = 0u64;
    let mut verified = 0usize;

    for path in parquet_files(directory) {
        let file = File::open(&path).expect("open canonical Parquet file");

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .expect("open canonical Parquet reader")
            .build()
            .expect("build canonical Parquet reader");

        for batch in reader {
            let batch = batch.expect("read canonical Parquet batch");

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

            for index in 0..batch.num_rows() {
                if let Some(sample) = targets.get(&global_offset) {
                    let expected_side = match sample.side {
                        BookSide::Bid => "bid",
                        BookSide::Ask => "ask",
                    };

                    assert_eq!(
                        sides.value(index),
                        expected_side,
                        "side mismatch at source ordinal {}",
                        sample.source_ordinal
                    );

                    let expected_price: Decimal = sample.price.parse().unwrap();

                    assert_eq!(
                        decimal_at(prices, index),
                        Some(expected_price),
                        "price mismatch at source ordinal {}",
                        sample.source_ordinal
                    );

                    let expected_base = sample
                        .quantity_base
                        .map(|value| value.parse::<Decimal>().unwrap());

                    let actual_base = decimal_at(quantities_base, index);

                    if let Some(tolerance) = sample.base_tolerance {
                        let tolerance: Decimal = tolerance.parse().unwrap();

                        let actual = actual_base.expect("missing canonical base quantity");
                        let expected = expected_base.expect("missing expected base quantity");

                        assert!(
                            (actual - expected).abs() <= tolerance,
                            "base quantity mismatch at source ordinal {}: expected {}, actual {}",
                            sample.source_ordinal,
                            expected,
                            actual
                        );
                    } else {
                        assert_eq!(
                            actual_base, expected_base,
                            "base quantity mismatch at source ordinal {}",
                            sample.source_ordinal
                        );
                    }

                    let expected_quote = sample
                        .quantity_quote
                        .map(|value| value.parse::<Decimal>().unwrap());

                    assert_eq!(
                        decimal_at(quantities_quote, index),
                        expected_quote,
                        "quote quantity mismatch at source ordinal {}",
                        sample.source_ordinal
                    );

                    let expected_contracts = sample
                        .quantity_contracts
                        .map(|value| value.parse::<Decimal>().unwrap());

                    assert_eq!(
                        decimal_at(quantities_contracts, index),
                        expected_contracts,
                        "contract quantity mismatch at source ordinal {}",
                        sample.source_ordinal
                    );

                    verified += 1;
                    println!(
                        "  VERIFIED | ordinal={} | {:?} | price={} | base={:?}",
                        sample.source_ordinal, sample.side, sample.price, actual_base
                    );
                }

                global_offset += 1;
            }
        }
    }

    assert_eq!(
        verified,
        expected.len(),
        "not all expected source events were verified"
    );

    println!("\nSOURCE-TO-PARQUET NORMALIZATION: {verified} EVENTS VERIFIED");
}

pub fn replay_parquet(directory: &Path) -> (BookStore, u64) {
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

fn verify_parquet_replay(directory: &Path, boundary: &DepthBoundaryManifest, expected_rows: u64) {
    let start = Instant::now();

    let (replayed_book, replayed_rows) = replay_parquet(directory);

    let elapsed = start.elapsed().as_secs_f64();

    assert_eq!(
        replayed_rows, expected_rows,
        "Parquet replay row count mismatch"
    );

    let replayed_state =
        BoundaryBookState::from_book(&replayed_book).expect("replayed book is not initialized");

    let final_boundary = boundary
        .final_state
        .as_ref()
        .expect("missing final boundary state");

    assert_eq!(
        replayed_state, final_boundary.state,
        "Parquet replay differs from boundary.json"
    );

    let replayed_hash = fingerprint_book(&replayed_state);

    assert_eq!(
        replayed_hash, final_boundary.fingerprint,
        "Parquet replay fingerprint mismatch"
    );

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — PARQUET REPLAY EQUIVALENCE");
    println!("{}", "=".repeat(90));

    println!("Rows replayed     : {replayed_rows}");
    println!("Replay time       : {elapsed:.3}s");

    println!(
        "Replay throughput : {:.0} levels/sec",
        replayed_rows as f64 / elapsed.max(f64::EPSILON)
    );

    println!("Final bids        : {}", replayed_state.bids.len());
    println!("Final asks        : {}", replayed_state.asks.len());

    println!("\nEQUIVALENCE");
    println!("Parquet ↔ Boundary: MATCH");
    println!("SHA-256           : MATCH");

    println!("\nREPLAY RESULT: PASS");
}
