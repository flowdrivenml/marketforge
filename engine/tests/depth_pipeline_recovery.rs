#![cfg(feature = "process")]

use std::{
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use arrow_array::{Array, UInt64Array};
use marketforge_engine::process::config::load_processing_config;
use marketforge_engine::{
    book::SequencePolicy,
    job::{SourceContainer, load_processing_job},
    process::{
        boundary::{BoundaryContinuity, DepthBoundaryManifest},
        metrics::{ScopedIntegrityMetrics, TaskMetrics},
        parquet::ParquetDepthWriter,
        worker::{DepthSink, process_depth_task_with_metrics},
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::json;

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

// -----------------------------------------------------------------------------
// Temporary directory
// -----------------------------------------------------------------------------

struct TestDirectory {
    path: PathBuf,
}

impl TestDirectory {
    fn new() -> Self {
        let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

        let path = std::env::temp_dir().join(format!(
            "marketforge-depth-recovery-{}-{id}",
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
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .to_path_buf()
}

// -----------------------------------------------------------------------------
// Synthetic Bybit records
// -----------------------------------------------------------------------------

fn snapshot(sequence: u64, timestamp_ms: u64, price: &str) -> serde_json::Value {
    json!({
        "type": "snapshot",
        "ts": timestamp_ms,
        "cts": timestamp_ms,
        "data": {
            "s": "BTCUSDT",
            "u": sequence,
            "b": [[price, "2"]],
            "a": [["101", "3"]]
        }
    })
}

fn delta(sequence: u64, timestamp_ms: u64, price: &str) -> serde_json::Value {
    json!({
        "type": "delta",
        "ts": timestamp_ms,
        "cts": timestamp_ms,
        "data": {
            "s": "BTCUSDT",
            "u": sequence,
            "b": [[price, "5"]],
            "a": []
        }
    })
}

fn write_jsonl(path: &Path, records: &[serde_json::Value]) {
    let mut file = File::create(path).unwrap();

    for record in records {
        writeln!(file, "{}", serde_json::to_string(record).unwrap()).unwrap();
    }
}

// -----------------------------------------------------------------------------
// Read source-event ordinals
// -----------------------------------------------------------------------------

fn read_event_ordinals(directory: &Path) -> Vec<u64> {
    let file = File::open(directory.join("events.parquet")).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .unwrap()
        .build()
        .unwrap();

    let mut ordinals = Vec::new();

    for batch in reader {
        let batch = batch.unwrap();

        let column = batch
            .column_by_name("event_ordinal")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();

        for index in 0..column.len() {
            ordinals.push(column.value(index));
        }
    }

    ordinals
}

// -----------------------------------------------------------------------------
// Recovery integration test
// -----------------------------------------------------------------------------

#[test]
fn depth_pipeline_recovers_after_sequence_gap() {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/bybit-spot-spot-BTCUSDT-l2-20260901-20260904-d105.json"),
    )
    .expect("load Bybit depth job");

    let mut task = job.tasks[0].clone();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    // Allow integrity violations to be recorded without aborting.
    config.integrity_policy.enabled = false;

    let directory = TestDirectory::new();

    let input_path = directory.path.join("synthetic.jsonl");
    let output_path = directory.path.join("output");

    // -------------------------------------------------------------------------
    // Synthetic source sequence
    // -------------------------------------------------------------------------

    let records = vec![
        snapshot(100, 1000, "100"),
        delta(101, 1001, "100"),
        delta(105, 1002, "100"),
        delta(106, 1003, "100"),
        snapshot(200, 1004, "99"),
        delta(201, 1005, "99"),
    ];

    write_jsonl(&input_path, &records);

    task.input_path = input_path;
    task.source_compression = SourceContainer::Plain;
    task.archive_member = None;

    // -------------------------------------------------------------------------
    // Execute complete processing pipeline
    // -------------------------------------------------------------------------

    let mut writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet depth writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        &task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Consecutive,
    )
    .expect("process synthetic recovery archive");

    writer.finish().expect("finalize depth writer");

    // -------------------------------------------------------------------------
    // Processing counters
    // -------------------------------------------------------------------------

    assert_eq!(metrics.counters.records_read, 6);
    assert_eq!(metrics.counters.records_processed, 4);
    assert_eq!(metrics.counters.records_rejected, 2);
    assert_eq!(metrics.counters.records_rejected_unmatched, 2);

    // -------------------------------------------------------------------------
    // Event index
    // -------------------------------------------------------------------------

    let ordinals = read_event_ordinals(&output_path);

    assert_eq!(ordinals, vec![1, 2, 5, 6]);

    // -------------------------------------------------------------------------
    // Boundary manifest
    // -------------------------------------------------------------------------

    let boundary: DepthBoundaryManifest =
        serde_json::from_reader(File::open(output_path.join("boundary.json")).unwrap()).unwrap();

    assert_eq!(
        boundary.continuity,
        BoundaryContinuity::RecoveredFromSnapshot
    );

    assert_eq!(boundary.first_sequence, Some(100));
    assert_eq!(boundary.last_sequence, Some(201));

    assert!(boundary.initial.is_some());
    assert!(boundary.final_state.is_some());

    let final_state = boundary.final_state.as_ref().unwrap();

    assert_eq!(final_state.sequence, Some(201));

    assert_eq!(final_state.state.bids.len(), 1);
    assert_eq!(final_state.state.asks.len(), 1);

    // -------------------------------------------------------------------------
    // Reconstruction segments
    // -------------------------------------------------------------------------

    assert_eq!(writer.segments().len(), 2);

    // -------------------------------------------------------------------------
    // Results
    // -------------------------------------------------------------------------

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — DEPTH PIPELINE RECOVERY");
    println!("{}", "=".repeat(90));

    println!("\nPROCESSING");
    println!("  Source records       : {}", metrics.counters.records_read);
    println!(
        "  Accepted records     : {}",
        metrics.counters.records_processed
    );
    println!(
        "  Rejected records     : {}",
        metrics.counters.records_rejected
    );

    println!("\nEVENT INDEX");
    println!("  Accepted ordinals    : {ordinals:?}");

    println!("\nRECONSTRUCTION");
    println!("  Segments             : {}", writer.segments().len());
    println!("  Continuity           : {:?}", boundary.continuity);
    println!("  First sequence       : {:?}", boundary.first_sequence);
    println!("  Last sequence        : {:?}", boundary.last_sequence);

    println!("\nFINAL BOOK");
    println!("  Bids                 : {}", final_state.state.bids.len());
    println!("  Asks                 : {}", final_state.state.asks.len());
    println!("  Fingerprint          : {}", final_state.fingerprint);

    println!("\nRESULT: PASS");
}

// -----------------------------------------------------------------------------
// Unrecoverable sequence gap
// -----------------------------------------------------------------------------

#[test]
fn depth_pipeline_invalidates_final_book_after_unrecovered_gap() {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/bybit-spot-spot-BTCUSDT-l2-20260901-20260904-d105.json"),
    )
    .expect("load Bybit depth job");

    let mut task = job.tasks[0].clone();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    // Record integrity violations without aborting processing.
    config.integrity_policy.enabled = false;

    let directory = TestDirectory::new();

    let input_path = directory.path.join("synthetic.jsonl");
    let output_path = directory.path.join("output");

    // -------------------------------------------------------------------------
    // Source messages
    // -------------------------------------------------------------------------

    let records = vec![
        snapshot(100, 1000, "100"),
        delta(101, 1001, "100"),
        delta(105, 1002, "100"),
    ];

    write_jsonl(&input_path, &records);

    task.input_path = input_path;
    task.source_compression = SourceContainer::Plain;
    task.archive_member = None;

    // -------------------------------------------------------------------------
    // Process source archive
    // -------------------------------------------------------------------------

    let mut writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet depth writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        &task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Consecutive,
    )
    .expect("process synthetic archive");

    writer.finish().expect("finalize depth writer");

    // -------------------------------------------------------------------------
    // Processing counters
    // -------------------------------------------------------------------------

    assert_eq!(metrics.counters.records_read, 3);
    assert_eq!(metrics.counters.records_processed, 2);
    assert_eq!(metrics.counters.records_rejected, 1);
    assert_eq!(metrics.counters.records_rejected_unmatched, 1);

    // -------------------------------------------------------------------------
    // Source-event index
    // -------------------------------------------------------------------------

    let ordinals = read_event_ordinals(&output_path);

    assert_eq!(ordinals, vec![1, 2]);

    // -------------------------------------------------------------------------
    // Boundary metadata
    // -------------------------------------------------------------------------

    let boundary: DepthBoundaryManifest =
        serde_json::from_reader(File::open(output_path.join("boundary.json")).unwrap()).unwrap();

    assert_eq!(boundary.continuity, BoundaryContinuity::GapDetected);

    assert_eq!(boundary.first_sequence, Some(100));

    // Last successfully accepted sequence.
    assert_eq!(boundary.last_sequence, Some(101));

    // Initial authoritative state remains valid.
    assert!(boundary.initial.is_some());

    // The final book must be invalidated.
    assert!(
        boundary.final_state.is_none(),
        "unrecovered sequence gap must not publish a final book"
    );

    // -------------------------------------------------------------------------
    // Reconstruction segments
    // -------------------------------------------------------------------------

    assert_eq!(writer.segments().len(), 1);

    // -------------------------------------------------------------------------
    // Results
    // -------------------------------------------------------------------------

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — UNRECOVERABLE DEPTH SEQUENCE GAP");
    println!("{}", "=".repeat(90));

    println!("\nPROCESSING");
    println!("  Source records       : {}", metrics.counters.records_read);
    println!(
        "  Accepted records     : {}",
        metrics.counters.records_processed
    );
    println!(
        "  Rejected records     : {}",
        metrics.counters.records_rejected
    );

    println!("\nEVENT INDEX");
    println!("  Accepted ordinals    : {ordinals:?}");

    println!("\nRECONSTRUCTION");
    println!("  Segments             : {}", writer.segments().len());
    println!("  Continuity           : {:?}", boundary.continuity);
    println!("  First sequence       : {:?}", boundary.first_sequence);
    println!("  Last valid sequence  : {:?}", boundary.last_sequence);
    println!(
        "  Final book valid     : {}",
        boundary.final_state.is_some()
    );

    println!("\nRESULT: PASS");
}
