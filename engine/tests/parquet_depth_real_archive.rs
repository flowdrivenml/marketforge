#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
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
        .filter(|path| path.extension().and_then(|ext| ext.to_str()) == Some("parquet"))
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
