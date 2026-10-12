#![cfg(feature = "process")]
#[allow(dead_code)]
#[path = "depth_formats/common.rs"]
mod depth_format_common;

use std::{
    fs::{self, File},
    io::Write,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::process::boundary::{BoundaryBookState, fingerprint_book};
use marketforge_engine::{
    book::SequencePolicy,
    job::{SourceContainer, load_processing_job},
    process::{
        boundary::BoundaryContinuity,
        config::load_processing_config,
        metrics::{ScopedIntegrityMetrics, TaskMetrics},
        parquet::ParquetDepthWriter,
        worker::{DepthSink, process_depth_task_with_metrics},
    },
};

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

        let path =
            std::env::temp_dir().join(format!("marketforge-depth-csv-{}-{id}", std::process::id()));

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
// Gate.io spot fixture
// -----------------------------------------------------------------------------

#[test]
fn processes_gateio_spot_csv_pipeline() {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/")
            .join("gateio-spot-spot-BTC_USDT-l2-20260901-20260904-d112.json"),
    )
    .expect("load Gate.io spot job");

    let mut task = job.tasks[0].clone();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    config.integrity_policy.enabled = false;

    let directory = TestDirectory::new();

    let input_path = directory.path.join("synthetic.csv");
    let output_path = directory.path.join("output");

    // ---------------------------------------------------------
    // Synthetic Gate.io source
    // ---------------------------------------------------------

    let mut file = File::create(&input_path).unwrap();

    writeln!(file, "1000,2,set,99,10,100,0").unwrap();
    writeln!(file, "1000,1,set,101,5,100,0").unwrap();

    writeln!(file, "1000.1,2,make,99,3,101,1").unwrap();
    writeln!(file, "1000.2,1,take,101,2,102,1").unwrap();
    writeln!(file, "1000.3,2,take,99,13,103,1").unwrap();

    task.input_path = input_path;
    task.source_compression = SourceContainer::Plain;
    task.archive_member = None;

    // ---------------------------------------------------------
    // Execute complete pipeline
    // ---------------------------------------------------------

    let mut writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        &task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Ranged,
    )
    .expect("process Gate.io CSV fixture");

    writer.finish().expect("finalize Parquet writer");

    // ---------------------------------------------------------
    // Physical source-row accounting
    // ---------------------------------------------------------

    assert_eq!(metrics.counters.records_read, 5);
    assert_eq!(metrics.counters.records_processed, 5);
    assert_eq!(metrics.counters.records_rejected, 0);

    // ---------------------------------------------------------
    // Canonical levels
    // ---------------------------------------------------------

    assert_eq!(metrics.counters.events_written, 5);

    // ---------------------------------------------------------
    // Boundary metadata
    // ---------------------------------------------------------

    let boundary_path = output_path.join("boundary.json");

    let boundary: marketforge_engine::process::boundary::DepthBoundaryManifest =
        serde_json::from_slice(&fs::read(boundary_path).unwrap()).unwrap();

    assert_eq!(boundary.continuity, BoundaryContinuity::Verified,);

    assert_eq!(boundary.first_sequence, Some(100));
    assert_eq!(boundary.last_sequence, Some(103));

    println!("\nGATE.IO CSV PIPELINE RESULT: PASS");

    // -------------------------------------------------------------------------
    // Verify final reconstructed book
    // -------------------------------------------------------------------------

    let final_state = boundary
        .final_state
        .as_ref()
        .expect("missing final book state");

    assert!(
        final_state.state.bids.is_empty(),
        "expected all bid levels to be deleted"
    );

    assert_eq!(
        final_state.state.asks.len(),
        1,
        "expected exactly one ask level"
    );

    let ask = &final_state.state.asks[0];

    assert_eq!(ask.price.to_string(), "101");
    assert_eq!(ask.quantity_base.unwrap().to_string(), "3");

    println!("\nFINAL BOOK: VERIFIED");
    // -------------------------------------------------------------------------
    // Independent Parquet replay
    // -------------------------------------------------------------------------

    let (replayed_book, replayed_rows) = depth_format_common::replay_parquet(&output_path);

    assert_eq!(
        replayed_rows, 5,
        "unexpected number of replayed canonical levels"
    );

    // -------------------------------------------------------------------------
    // Reconstruct boundary state from replayed book
    // -------------------------------------------------------------------------

    let replayed_state =
        BoundaryBookState::from_book(&replayed_book).expect("replayed book is not initialized");

    let replayed_fingerprint = fingerprint_book(&replayed_state);

    let final_boundary = boundary
        .final_state
        .as_ref()
        .expect("missing final boundary state");

    // -------------------------------------------------------------------------
    // Verify exact equivalence
    // -------------------------------------------------------------------------

    assert_eq!(
        replayed_state, final_boundary.state,
        "Parquet replay differs from final boundary book"
    );

    assert_eq!(
        replayed_fingerprint, final_boundary.fingerprint,
        "Parquet replay SHA-256 mismatch"
    );

    println!("\nGATE.IO — PARQUET REPLAY EQUIVALENCE");
    println!("Rows replayed : {replayed_rows}");
    println!("Final bids    : {}", replayed_book.bids().len());
    println!("Final asks    : {}", replayed_book.asks().len());
    println!("Book state    : MATCH");
    println!("SHA-256       : MATCH");
    println!("REPLAY RESULT : PASS");
}

#[test]
fn recovers_after_malformed_gateio_snapshot() {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/gateio-spot-spot-BTC_USDT-l2-20260901-20260904-d112.json"),
    )
    .expect("load Gate.io spot job");

    let mut task = job.tasks[0].clone();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    config.integrity_policy.enabled = false;

    let directory = TestDirectory::new();

    let input_path = directory.path.join("malformed_snapshot.csv");
    let output_path = directory.path.join("output");

    let mut file = File::create(&input_path).unwrap();

    // Snapshot 1: incomplete because a malformed row interrupts it.
    writeln!(file, "1000,2,set,99,10,100,0").unwrap();
    writeln!(file, "1000,1,set,101,5,100,0").unwrap();
    writeln!(file, "malformed,row").unwrap();

    // Must be rejected because synchronization was lost.
    writeln!(file, "1000.1,2,make,99,3,101,1").unwrap();

    // Snapshot 2: authoritative recovery.
    writeln!(file, "1001,2,set,98,7,200,0").unwrap();
    writeln!(file, "1001,1,set,102,4,200,0").unwrap();

    // Valid update after recovery.
    writeln!(file, "1001.1,2,make,98,2,201,1").unwrap();

    task.input_path = input_path;
    task.source_compression = SourceContainer::Plain;
    task.archive_member = None;

    let mut writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        &task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Ranged,
    )
    .expect("process malformed Gate.io fixture");

    writer.finish().expect("finalize writer");

    // ---------------------------------------------------------
    // Physical source-record accounting
    // ---------------------------------------------------------

    assert_eq!(metrics.counters.records_read, 7);
    assert_eq!(metrics.counters.records_processed, 3);
    assert_eq!(metrics.counters.records_rejected, 4);
    assert_eq!(metrics.counters.records_rejected_unmatched, 2);

    // ---------------------------------------------------------
    // Canonical output
    // ---------------------------------------------------------

    assert_eq!(metrics.counters.events_written, 3);

    // ---------------------------------------------------------
    // Recovered boundary
    // ---------------------------------------------------------

    let boundary: marketforge_engine::process::boundary::DepthBoundaryManifest =
        serde_json::from_slice(&fs::read(output_path.join("boundary.json")).unwrap()).unwrap();

    assert_eq!(
        boundary.continuity,
        BoundaryContinuity::RecoveredFromSnapshot,
    );

    let final_state = boundary.final_state.as_ref().unwrap();

    assert_eq!(final_state.state.bids.len(), 1);
    assert_eq!(final_state.state.asks.len(), 1);

    let bid = &final_state.state.bids[0];

    assert_eq!(bid.price.to_string(), "98");
    assert_eq!(bid.quantity_base.unwrap().to_string(), "9");

    println!("\nGATE.IO MALFORMED SNAPSHOT RECOVERY: PASS");
}
#[test]
fn recovers_after_gateio_sequence_gap() {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/gateio-spot-spot-BTC_USDT-l2-20260901-20260904-d112.json"),
    )
    .expect("load Gate.io spot job");

    let mut task = job.tasks[0].clone();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    config.integrity_policy.enabled = false;

    let directory = TestDirectory::new();

    let input_path = directory.path.join("sequence_gap.csv");
    let output_path = directory.path.join("output");

    let mut file = File::create(&input_path).unwrap();

    // Initial authoritative snapshot.
    writeln!(file, "1000,2,set,99,10,100,0").unwrap();
    writeln!(file, "1000,1,set,101,5,100,0").unwrap();

    // Valid update: sequence 101.
    writeln!(file, "1000.1,2,make,99,3,101,1").unwrap();

    // Gap: expected 102, received 105.
    writeln!(file, "1000.2,1,take,101,2,105,1").unwrap();

    // Must be rejected because synchronization was lost.
    writeln!(file, "1000.3,2,make,99,2,106,1").unwrap();

    // New authoritative snapshot restores synchronization.
    writeln!(file, "1001,2,set,98,7,200,0").unwrap();
    writeln!(file, "1001,1,set,102,4,200,0").unwrap();

    // Valid ranged update covering sequences 201–203.
    writeln!(file, "1001.1,2,make,98,2,201,3").unwrap();

    // Next update must begin at 204.
    writeln!(file, "1001.2,1,take,102,1,204,1").unwrap();

    task.input_path = input_path;
    task.source_compression = SourceContainer::Plain;
    task.archive_member = None;

    let mut writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        &task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Ranged,
    )
    .expect("process sequence-gap fixture");

    writer.finish().expect("finalize writer");

    // ---------------------------------------------------------
    // Physical source-record accounting
    // ---------------------------------------------------------

    assert_eq!(metrics.counters.records_read, 9);
    assert_eq!(metrics.counters.records_processed, 7);
    assert_eq!(metrics.counters.records_rejected, 2);

    // ---------------------------------------------------------
    // Canonical output
    // ---------------------------------------------------------

    assert_eq!(metrics.counters.events_written, 7);

    // ---------------------------------------------------------
    // Boundary continuity
    // ---------------------------------------------------------

    let boundary: marketforge_engine::process::boundary::DepthBoundaryManifest =
        serde_json::from_slice(&fs::read(output_path.join("boundary.json")).unwrap()).unwrap();

    assert_eq!(
        boundary.continuity,
        BoundaryContinuity::RecoveredFromSnapshot,
    );

    assert_eq!(boundary.last_sequence, Some(204));

    // ---------------------------------------------------------
    // Final reconstructed book
    // ---------------------------------------------------------

    let final_state = boundary.final_state.as_ref().unwrap();

    assert_eq!(final_state.state.bids.len(), 1);
    assert_eq!(final_state.state.asks.len(), 1);

    let bid = &final_state.state.bids[0];
    let ask = &final_state.state.asks[0];

    assert_eq!(bid.price.to_string(), "98");
    assert_eq!(bid.quantity_base.unwrap().to_string(), "9");

    assert_eq!(ask.price.to_string(), "102");
    assert_eq!(ask.quantity_base.unwrap().to_string(), "3");

    println!("\nGATE.IO SEQUENCE GAP RECOVERY: PASS");
}

#[test]
fn flushes_gateio_snapshot_at_eof() {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/gateio-spot-spot-BTC_USDT-l2-20260901-20260904-d112.json"),
    )
    .expect("load Gate.io spot job");

    let mut task = job.tasks[0].clone();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    config.integrity_policy.enabled = false;

    let directory = TestDirectory::new();

    let input_path = directory.path.join("snapshot_eof.csv");
    let output_path = directory.path.join("output");

    let mut file = File::create(&input_path).unwrap();

    // Complete snapshot without subsequent updates.
    writeln!(file, "1000,2,set,99,10,100,0").unwrap();
    writeln!(file, "1000,1,set,101,5,100,0").unwrap();

    task.input_path = input_path;
    task.source_compression = SourceContainer::Plain;
    task.archive_member = None;

    let mut writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        &task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Ranged,
    )
    .expect("process snapshot-only CSV");

    writer.finish().expect("finalize writer");

    assert_eq!(metrics.counters.records_read, 2);
    assert_eq!(metrics.counters.records_processed, 2);
    assert_eq!(metrics.counters.records_rejected, 0);
    assert_eq!(metrics.counters.events_written, 2);

    let boundary: marketforge_engine::process::boundary::DepthBoundaryManifest =
        serde_json::from_slice(&fs::read(output_path.join("boundary.json")).unwrap()).unwrap();

    assert_eq!(boundary.continuity, BoundaryContinuity::Verified,);

    let final_state = boundary.final_state.as_ref().unwrap();

    assert_eq!(final_state.state.bids.len(), 1);
    assert_eq!(final_state.state.asks.len(), 1);

    println!("\nGATE.IO SNAPSHOT EOF FLUSH: PASS");
}

#[test]
fn rejects_incomplete_initial_snapshot_at_eof() {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/gateio-spot-spot-BTC_USDT-l2-20260901-20260904-d112.json"),
    )
    .expect("load Gate.io spot job");

    let mut task = job.tasks[0].clone();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    config.integrity_policy.enabled = false;

    let directory = TestDirectory::new();

    let input_path = directory.path.join("incomplete_initial.csv");
    let output_path = directory.path.join("output");

    let mut file = File::create(&input_path).unwrap();

    // Snapshot contains bids but no asks.
    writeln!(file, "1000,2,set,99,10,100,0").unwrap();
    writeln!(file, "1000,2,set,98,5,100,0").unwrap();

    task.input_path = input_path;
    task.source_compression = SourceContainer::Plain;
    task.archive_member = None;

    let mut writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        &task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Ranged,
    )
    .expect("process incomplete initial snapshot");

    writer.finish().expect("finalize writer");

    // All physical rows must be rejected.
    assert_eq!(metrics.counters.records_read, 2);
    assert_eq!(metrics.counters.records_processed, 0);
    assert_eq!(metrics.counters.records_rejected, 2);
    assert_eq!(metrics.counters.records_matched, 2);
    assert_eq!(metrics.counters.records_rejected_unmatched, 0);
    assert_eq!(metrics.counters.events_written, 0);

    let boundary: marketforge_engine::process::boundary::DepthBoundaryManifest =
        serde_json::from_slice(&fs::read(output_path.join("boundary.json")).unwrap()).unwrap();

    assert_eq!(boundary.continuity, BoundaryContinuity::GapDetected,);

    assert!(boundary.initial.is_none());
    assert!(boundary.final_state.is_none());

    println!("\nGATE.IO INCOMPLETE INITIAL SNAPSHOT: PASS");
}

#[test]
fn rejects_incomplete_replacement_snapshot_at_eof() {
    let root = project_root();

    let job = load_processing_job(
        root.join("data/.jobs/process/gateio-spot-spot-BTC_USDT-l2-20260901-20260904-d112.json"),
    )
    .expect("load Gate.io spot job");

    let mut task = job.tasks[0].clone();

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    config.integrity_policy.enabled = false;

    let directory = TestDirectory::new();

    let input_path = directory.path.join("incomplete_replacement.csv");
    let output_path = directory.path.join("output");

    let mut file = File::create(&input_path).unwrap();

    // Valid initial snapshot.
    writeln!(file, "1000,2,set,99,10,100,0").unwrap();
    writeln!(file, "1000,1,set,101,5,100,0").unwrap();

    // Valid relative update.
    writeln!(file, "1000.1,2,make,99,3,101,1").unwrap();

    // Incomplete replacement snapshot at EOF.
    writeln!(file, "1001,2,set,98,7,200,0").unwrap();
    writeln!(file, "1001,2,set,97,4,200,0").unwrap();

    task.input_path = input_path;
    task.source_compression = SourceContainer::Plain;
    task.archive_member = None;

    let mut writer = ParquetDepthWriter::new(&output_path, config.resources.parquet.clone())
        .expect("create Parquet writer");

    let mut metrics = TaskMetrics::default();

    process_depth_task_with_metrics(
        &task,
        &mut writer,
        &mut metrics,
        &config.integrity_policy,
        &ScopedIntegrityMetrics::default(),
        SequencePolicy::Ranged,
    )
    .expect("process incomplete replacement snapshot");

    writer.finish().expect("finalize writer");

    assert_eq!(metrics.counters.records_read, 5);
    assert_eq!(metrics.counters.records_processed, 3);
    assert_eq!(metrics.counters.records_rejected, 2);
    assert_eq!(metrics.counters.events_written, 3);

    let boundary: marketforge_engine::process::boundary::DepthBoundaryManifest =
        serde_json::from_slice(&fs::read(output_path.join("boundary.json")).unwrap()).unwrap();

    // The previous book must not be advertised as a valid final state.
    assert_eq!(boundary.continuity, BoundaryContinuity::GapDetected,);

    assert!(boundary.initial.is_some());
    assert!(boundary.final_state.is_none());

    println!("\nGATE.IO INCOMPLETE REPLACEMENT SNAPSHOT: PASS");
}
