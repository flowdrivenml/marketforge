#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use marketforge_engine::process::boundary::{
    BoundaryContinuity, DepthBoundaryManifest, fingerprint_book,
};
use marketforge_engine::{
    book::SequencePolicy,
    job::load_processing_job,
    process::{
        load_processing_config,
        metrics::{ScopedIntegrityMetrics, TaskMetrics},
        parquet::{ParquetDepthWriter, read_depth_segments},
        worker::{DepthSink, process_depth_task_with_metrics},
    },
};

use arrow_array::UInt8Array;
use arrow_array::{Array, Decimal256Array, Int64Array, StringArray, UInt64Array};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use marketforge_engine::process::parquet::i256_to_decimal;

use parquet::file::reader::{FileReader, SerializedFileReader};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);

// -----------------------------------------------------------------------------
// Test directory
// -----------------------------------------------------------------------------

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-depth-real-parquet-{}-{id}",
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
// Parquet inspection
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

fn parquet_row_count(path: &Path) -> u64 {
    let file = fs::File::open(path).unwrap();
    let reader = SerializedFileReader::new(file).unwrap();

    u64::try_from(reader.metadata().file_metadata().num_rows()).unwrap()
}

// -----------------------------------------------------------------------------
// Real archive source-event index verification
// -----------------------------------------------------------------------------
//
// -----------------------------------------------------------------------------
// Inspect native source sequence continuity
// -----------------------------------------------------------------------------

fn inspect_depth_sequences(directory: &Path) {
    let path = directory.join("events.parquet");

    let file = fs::File::open(&path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .with_batch_size(8192)
        .build()
        .unwrap();

    let mut previous_sequence: Option<u64> = None;
    let mut previous_ordinal: Option<u64> = None;

    let mut consecutive = 0u64;
    let mut duplicates = 0u64;
    let mut gaps = 0u64;
    let mut backwards = 0u64;
    let mut missing = 0u64;
    let mut resets = 0u64;

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — SOURCE SEQUENCE CONTINUITY");
    println!("{}", "=".repeat(90));

    for batch in reader {
        let batch = batch.unwrap();

        let ordinals = batch
            .column_by_name("event_ordinal")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let sequences = batch
            .column_by_name("sequence_start")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let boundaries = batch
            .column_by_name("event_boundary")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        let operations = batch
            .column_by_name("source_operation")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        for index in 0..batch.num_rows() {
            let ordinal = ordinals.value(index);
            let boundary = boundaries.value(index);

            if sequences.is_null(index) {
                missing += 1;
                previous_sequence = None;
                previous_ordinal = Some(ordinal);
                continue;
            }

            let sequence = sequences.value(index);

            // Verify the final Bybit delta and authoritative snapshot.
            if ordinal == 825_282 {
                assert_eq!(
                    operations.value(index),
                    1,
                    "penultimate event must be an absolute update"
                );

                assert_eq!(boundary, 0);
                assert_eq!(sequence, 80_077_761);
            }

            if ordinal == 825_283 {
                assert_eq!(
                    operations.value(index),
                    0,
                    "final event must be an authoritative snapshot"
                );

                assert_eq!(boundary, 0);
                assert_eq!(sequence, 80_077_761);
            }

            if operations.value(index) == 0 {
                resets += 1;

                println!(
                    "  SNAPSHOT | ordinal={} | sequence={} | boundary={}",
                    ordinal, sequence, boundary,
                );

                previous_sequence = Some(sequence);
                previous_ordinal = Some(ordinal);
                continue;
            }

            if let Some(previous) = previous_sequence {
                match previous.checked_add(1) {
                    Some(expected) if sequence == expected => {
                        consecutive += 1;
                    }

                    _ if sequence == previous => {
                        duplicates += 1;

                        println!(
                            "  DUPLICATE | ordinal={} | previous={} | current={}",
                            ordinal, previous, sequence
                        );
                    }

                    Some(expected) if sequence > expected => {
                        gaps += 1;

                        println!(
                            "  GAP | ordinal={} | expected={} | received={}",
                            ordinal, expected, sequence
                        );
                    }

                    _ => {
                        backwards += 1;

                        println!(
                            "  BACKWARDS | ordinal={} | previous={} | current={}",
                            ordinal, previous, sequence
                        );
                    }
                }
            }

            previous_sequence = Some(sequence);
            previous_ordinal = Some(ordinal);
        }
    }

    println!("\nSEQUENCE SUMMARY");
    println!("  Consecutive updates : {consecutive}");
    println!("  Duplicate sequences : {duplicates}");
    println!("  Sequence gaps       : {gaps}");
    println!("  Backward sequences  : {backwards}");
    println!("  Missing sequences   : {missing}");
    println!("  Snapshot events     : {resets}");
    println!("  Last ordinal        : {previous_ordinal:?}");

    println!("\nSEQUENCE INSPECTION COMPLETE");
}

fn verify_depth_boundary(directory: &Path) {
    let path = directory.join("boundary.json");

    assert!(
        path.is_file(),
        "missing depth boundary metadata: {}",
        path.display()
    );

    let file = fs::File::open(&path).unwrap();

    let boundary: DepthBoundaryManifest = serde_json::from_reader(file).unwrap();

    assert_eq!(boundary.version, 1);
    assert_eq!(boundary.symbol, "BTCUSDT");

    assert_eq!(boundary.first_sequence, Some(79_252_480));

    assert_eq!(boundary.last_sequence, Some(80_077_761));

    assert_eq!(boundary.continuity, BoundaryContinuity::Verified);

    // Initial authoritative book.
    let initial = boundary
        .initial
        .as_ref()
        .expect("missing initial boundary state");

    assert_eq!(initial.state.bids.len(), 200);
    assert_eq!(initial.state.asks.len(), 200);

    assert_eq!(initial.fingerprint, fingerprint_book(&initial.state));

    // Final reconstructed book.
    let final_state = boundary
        .final_state
        .as_ref()
        .expect("missing final boundary state");

    assert_eq!(final_state.state.bids.len(), 200);
    assert_eq!(final_state.state.asks.len(), 200);

    assert_eq!(
        final_state.fingerprint,
        fingerprint_book(&final_state.state)
    );

    assert_eq!(initial.sequence, boundary.first_sequence);

    assert_eq!(final_state.sequence, boundary.last_sequence);

    assert_eq!(initial.coverage.depth_per_side, Some(200));
    assert_eq!(initial.coverage.bid_levels, 200);
    assert_eq!(initial.coverage.ask_levels, 200);

    assert_eq!(final_state.coverage.depth_per_side, Some(200));
    assert_eq!(final_state.coverage.bid_levels, 200);
    assert_eq!(final_state.coverage.ask_levels, 200);

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — DEPTH BOUNDARY VERIFICATION");
    println!("{}", "=".repeat(90));

    println!("\nARCHIVE BOUNDARIES");
    println!("  Exchange          : {:?}", boundary.exchange);
    println!("  Instrument        : {}", boundary.symbol);
    println!("  Stream            : {}", boundary.stream_id);
    println!("  Continuity        : {:?}", boundary.continuity);

    println!("\nINITIAL BOOK");
    println!("  Sequence          : {:?}", initial.sequence);
    println!("  Timestamp         : {}", initial.timestamp_ns);
    println!("  Bids              : {}", initial.state.bids.len());
    println!("  Asks              : {}", initial.state.asks.len());
    println!("  Fingerprint       : {}", initial.fingerprint);

    println!("\nFINAL BOOK");
    println!("  Sequence          : {:?}", final_state.sequence);
    println!("  Timestamp         : {}", final_state.timestamp_ns);
    println!("  Bids              : {}", final_state.state.bids.len());
    println!("  Asks              : {}", final_state.state.asks.len());
    println!("  Fingerprint       : {}", final_state.fingerprint);

    println!("\nBOUNDARY RESULT: PASS");
}

fn verify_depth_event_index(
    directory: &Path,
    expected_source_events: u64,
    expected_canonical_rows: u64,
) {
    let path = directory.join("events.parquet");

    assert!(
        path.is_file(),
        "missing depth source-event index: {}",
        path.display()
    );

    let file = fs::File::open(&path).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("open depth event index")
        .with_batch_size(8192)
        .build()
        .expect("build depth event-index reader");

    let mut indexed_events = 0u64;
    let mut next_canonical_offset = 0u64;

    let mut first_sequence = None;
    let mut last_sequence = None;

    let mut first_ordinal = None;
    let mut last_ordinal = None;

    let mut initialization_events = 0u64;
    let mut zero_change_events = 0u64;

    let mut samples = Vec::new();

    for batch in reader {
        let batch = batch.expect("read event-index batch");

        let ordinals = batch
            .column_by_name("event_ordinal")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let offsets = batch
            .column_by_name("canonical_row_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let counts = batch
            .column_by_name("canonical_row_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let timestamps = batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let sequences = batch
            .column_by_name("sequence_start")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        let boundaries = batch
            .column_by_name("event_boundary")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();

        for index in 0..batch.num_rows() {
            let ordinal = ordinals.value(index);
            let offset = offsets.value(index);
            let count = counts.value(index);
            let boundary = boundaries.value(index);

            // -------------------------------------------------------------
            // Validate source-event ordering
            // -------------------------------------------------------------

            if let Some(previous) = last_ordinal {
                assert!(
                    ordinal > previous,
                    "non-increasing source ordinal: previous={previous}, current={ordinal}"
                );
            }

            first_ordinal.get_or_insert(ordinal);
            last_ordinal = Some(ordinal);

            // -------------------------------------------------------------
            // Validate canonical row offsets
            // -------------------------------------------------------------

            assert_eq!(
                offset, next_canonical_offset,
                "event-index offset mismatch at source ordinal {ordinal}"
            );

            next_canonical_offset = offset
                .checked_add(count)
                .expect("canonical row offset overflow");

            // -------------------------------------------------------------
            // Sequence metadata
            // -------------------------------------------------------------

            if !sequences.is_null(index) {
                let sequence = sequences.value(index);

                first_sequence.get_or_insert(sequence);
                last_sequence = Some(sequence);
            }

            // -------------------------------------------------------------
            // Boundary classification
            // -------------------------------------------------------------

            match boundary {
                0 => {}
                1 => {
                    initialization_events += 1;

                    assert!(
                        count > 0,
                        "initialization event must contain canonical levels"
                    );
                }
                other => panic!("invalid depth event boundary: {other}"),
            }

            if count == 0 {
                zero_change_events += 1;
            }

            // -------------------------------------------------------------
            // Collect first 10 samples
            // -------------------------------------------------------------

            if samples.len() < 10 {
                samples.push((
                    ordinal,
                    offset,
                    count,
                    timestamps.value(index),
                    if sequences.is_null(index) {
                        None
                    } else {
                        Some(sequences.value(index))
                    },
                    boundary,
                ));
            }

            indexed_events += 1;
        }
    }

    // -------------------------------------------------------------------------
    // Final consistency checks
    // -------------------------------------------------------------------------

    assert_eq!(
        indexed_events, expected_source_events,
        "source-event index count mismatch"
    );

    assert_eq!(
        next_canonical_offset, expected_canonical_rows,
        "event index does not cover all canonical rows"
    );

    assert_eq!(first_ordinal, Some(1), "source ordinals must begin at 1");

    assert_eq!(
        last_ordinal,
        Some(expected_source_events),
        "last ordinal must match source-record count"
    );

    assert!(
        initialization_events > 0,
        "depth event index contains no initialization"
    );

    // -------------------------------------------------------------------------
    // Print results
    // -------------------------------------------------------------------------

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — REAL BYBIT SOURCE-EVENT INDEX");
    println!("{}", "=".repeat(90));

    println!("\nINDEX SUMMARY");

    println!("  Indexed events       : {indexed_events}");
    println!("  Canonical rows       : {next_canonical_offset}");
    println!("  Initialization events: {initialization_events}");
    println!("  Zero-change events   : {zero_change_events}");
    println!("  First ordinal        : {first_ordinal:?}");
    println!("  Last ordinal         : {last_ordinal:?}");
    println!("  First sequence       : {first_sequence:?}");
    println!("  Last sequence        : {last_sequence:?}");

    println!("\nFIRST 10 SOURCE EVENTS");

    println!(
        "{:<10} {:<12} {:<10} {:<20} {:<14} {:<10}",
        "ORDINAL", "ROW OFFSET", "ROW COUNT", "TIMESTAMP_NS", "SEQUENCE", "BOUNDARY",
    );

    for (ordinal, offset, count, timestamp, sequence, boundary) in samples {
        println!(
            "{:<10} {:<12} {:<10} {:<20} {:<14} {:<10}",
            ordinal,
            offset,
            count,
            timestamp,
            sequence
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_owned()),
            if boundary == 1 { "Snapshot" } else { "Changes" },
        );
    }

    println!("\nEVENT INDEX RESULT: PASS");
}

// -----------------------------------------------------------------------------
// Inspect canonical depth Parquet
// -----------------------------------------------------------------------------

fn inspect_depth_parquet(directory: &Path) {
    let files = parquet_files(directory);

    assert!(!files.is_empty(), "no Parquet files found");

    println!("\n{}", "=".repeat(110));
    println!("MARKETFORGE — CANONICAL DEPTH PARQUET INSPECTION");
    println!("{}", "=".repeat(110));

    // -------------------------------------------------------------------------
    // Parquet files
    // -------------------------------------------------------------------------

    println!("\nPARQUET FILES");

    for (index, path) in files.iter().enumerate() {
        let rows = parquet_row_count(path);
        let bytes = fs::metadata(path).unwrap().len();

        println!(
            "  File {:<3} | {:<24} | rows={:<12} | size={:.2} MiB",
            index,
            path.file_name().unwrap().to_string_lossy(),
            rows,
            bytes as f64 / 1_048_576.0,
        );
    }

    // -------------------------------------------------------------------------
    // Open first Parquet file
    // -------------------------------------------------------------------------

    let file = fs::File::open(&files[0]).unwrap();

    let builder = ParquetRecordBatchReaderBuilder::try_new(file).expect("open Parquet reader");

    let schema = builder.schema().clone();

    // -------------------------------------------------------------------------
    // Arrow schema
    // -------------------------------------------------------------------------

    println!("\nARROW SCHEMA");

    for (index, field) in schema.fields().iter().enumerate() {
        println!(
            "  {:<2} {:<24} {:<30} nullable={}",
            index,
            field.name(),
            format!("{:?}", field.data_type()),
            field.is_nullable(),
        );
    }

    let mut reader = builder
        .with_batch_size(1024)
        .build()
        .expect("build Parquet reader");

    let batch = reader
        .next()
        .expect("Parquet file contains a batch")
        .expect("read first batch");

    // -------------------------------------------------------------------------
    // Extract canonical columns
    // -------------------------------------------------------------------------

    let timestamps = batch
        .column_by_name("event_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let system_timestamps = batch
        .column_by_name("system_timestamp_ns")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let exchanges = batch
        .column_by_name("exchange")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    let symbols = batch
        .column_by_name("symbol")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
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

    // -------------------------------------------------------------------------
    // Print canonical levels
    // -------------------------------------------------------------------------

    println!("\nFIRST 20 CANONICAL L2 LEVELS");

    println!(
        "{:<20} {:<7} {:<10} {:<6} {:<14} {:<18} {:<18} {:<12} {:<8}",
        "TIMESTAMP_NS",
        "EXCHANGE",
        "SYMBOL",
        "SIDE",
        "PRICE",
        "QTY_BASE",
        "QTY_QUOTE",
        "QTY_CONTRACTS",
        "ORDERS",
    );

    for index in 0..batch.num_rows().min(20) {
        let price = i256_to_decimal(prices.value(index)).unwrap();

        let base = if quantities_base.is_null(index) {
            "-".to_owned()
        } else {
            i256_to_decimal(quantities_base.value(index))
                .unwrap()
                .to_string()
        };

        let quote = if quantities_quote.is_null(index) {
            "-".to_owned()
        } else {
            i256_to_decimal(quantities_quote.value(index))
                .unwrap()
                .to_string()
        };

        let contracts = if quantities_contracts.is_null(index) {
            "-".to_owned()
        } else {
            i256_to_decimal(quantities_contracts.value(index))
                .unwrap()
                .to_string()
        };

        let orders = if order_counts.is_null(index) {
            "-".to_owned()
        } else {
            order_counts.value(index).to_string()
        };

        println!(
            "{:<20} {:<7} {:<10} {:<6} {:<14} {:<18} {:<18} {:<12} {:<8}",
            timestamps.value(index),
            exchanges.value(index),
            symbols.value(index),
            sides.value(index),
            price,
            base,
            quote,
            contracts,
            orders,
        );
    }

    // -------------------------------------------------------------------------
    // System timestamp inspection
    // -------------------------------------------------------------------------

    println!("\nTIMESTAMP INSPECTION");

    for index in 0..batch.num_rows().min(5) {
        println!(
            "  Row {:<3} | event_ns={} | system_ns={}",
            index,
            timestamps.value(index),
            if system_timestamps.is_null(index) {
                "-".to_owned()
            } else {
                system_timestamps.value(index).to_string()
            },
        );
    }

    // -------------------------------------------------------------------------
    // Reconstruction metadata
    // -------------------------------------------------------------------------

    let manifest = read_depth_segments(directory).expect("read depth segment manifest");

    println!("\nRECONSTRUCTION SEGMENTS");

    println!("  Total levels : {}", manifest.total_levels);
    println!("  Segments     : {}", manifest.segments.len());

    for segment in &manifest.segments {
        println!(
            "  Segment {} | offset={} | initial_levels={} | timestamp={}",
            segment.segment_id,
            segment.initial_event_offset,
            segment.initial_level_count,
            segment.start_timestamp_ns,
        );
    }
}

// -----------------------------------------------------------------------------
// Real archive test
// -----------------------------------------------------------------------------

#[test]
fn writes_real_bybit_depth_archive_to_parquet() {
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
        .expect("load processing config");

    let directory = TestDirectory::new();

    let mut writer = ParquetDepthWriter::new(&directory.path, config.resources.parquet.clone())
        .expect("create depth Parquet writer");

    let mut metrics = TaskMetrics::default();
    let previous = ScopedIntegrityMetrics::default();

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — REAL BYBIT DEPTH → PARQUET");
    println!("{}", "=".repeat(90));

    println!("Archive : {}", task.input_path.display());
    println!("Output  : {}", directory.path.display());

    let start = Instant::now();

    process_depth_task_with_metrics(
        task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &previous,
        SequencePolicy::Consecutive,
    )
    .expect("process real depth archive");

    writer.finish().expect("finalize Parquet writer");

    let elapsed = start.elapsed().as_secs_f64();

    // -------------------------------------------------------------------------
    // Inspect output
    // -------------------------------------------------------------------------

    let files = parquet_files(&directory.path);

    assert!(!files.is_empty());

    let total_rows: u64 = files.iter().map(|path| parquet_row_count(path)).sum();

    let manifest = read_depth_segments(&directory.path).expect("load depth segment manifest");

    // -------------------------------------------------------------------------
    // Verify consistency
    // -------------------------------------------------------------------------

    assert_eq!(
        metrics.counters.events_written,
        writer.metrics().levels_written
    );

    assert_eq!(total_rows, writer.metrics().levels_written);

    assert_eq!(manifest.total_levels, total_rows);

    assert_eq!(writer.next_event_offset(), total_rows);

    assert_eq!(writer.metrics().files_written as usize, files.len());

    assert_eq!(metrics.counters.records_rejected, 0);

    assert!(!manifest.segments.is_empty());

    // -------------------------------------------------------------------------
    // Results
    // -------------------------------------------------------------------------

    let total_bytes: u64 = files
        .iter()
        .map(|path| fs::metadata(path).unwrap().len())
        .sum();
    // -------------------------------------------------------------------------
    // Inspect actual serialized canonical depth records
    // -------------------------------------------------------------------------

    inspect_depth_parquet(&directory.path);

    // -------------------------------------------------------------------------
    // Verify persisted source-event index
    // -------------------------------------------------------------------------

    assert_eq!(writer.events_indexed(), metrics.counters.records_processed,);

    verify_depth_event_index(
        &directory.path,
        metrics.counters.records_processed,
        writer.metrics().levels_written,
    );

    inspect_depth_sequences(&directory.path);
    verify_depth_boundary(&directory.path);

    println!("\nRESULTS");
    println!("  Source records     : {}", metrics.counters.records_read);
    println!(
        "  Records processed  : {}",
        metrics.counters.records_processed
    );
    println!("  Canonical levels   : {}", total_rows);
    println!("  Parquet files      : {}", files.len());
    println!(
        "  Parquet size       : {:.2} MiB",
        total_bytes as f64 / 1_048_576.0
    );
    println!("  Segments           : {}", manifest.segments.len());
    println!("  Elapsed            : {elapsed:.3}s");
    println!(
        "  Levels/sec         : {:.0}",
        total_rows as f64 / elapsed.max(f64::EPSILON)
    );

    for segment in &manifest.segments {
        println!(
            "  Segment {} | offset={} | initial_levels={} | timestamp={}",
            segment.segment_id,
            segment.initial_event_offset,
            segment.initial_level_count,
            segment.start_timestamp_ns,
        );
    }

    println!("\nRESULT: PASS");
}
