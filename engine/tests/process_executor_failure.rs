#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    job::load_processing_job,
    process::{
        ProcessingStatus, execute_processing_job,
        failure::{PROCESSING_FAILURE_FILENAME, ProcessingFailureReport},
        load_processing_config,
    },
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const JOB_FILE: &str = "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json";

// -----------------------------------------------------------------------------
// Test environment
// -----------------------------------------------------------------------------

fn project_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root")
        .to_path_buf()
}

fn test_directory() -> PathBuf {
    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

    let directory = std::env::temp_dir().join(format!(
        "marketforge-executor-failure-{}-{id}",
        std::process::id(),
    ));

    fs::create_dir_all(&directory).expect("create test directory");

    directory
}

// -----------------------------------------------------------------------------
// Executor failure integration test
// -----------------------------------------------------------------------------

#[test]
fn failed_processing_persists_failure_report() {
    let root = project_root();

    // -------------------------------------------------------------------------
    // Load existing processing job
    // -------------------------------------------------------------------------

    let mut job = load_processing_job(root.join("data/.jobs/process").join(JOB_FILE))
        .expect("load processing job");

    let mut config = load_processing_config(root.join("data/.jobs/processing.json"), &root)
        .expect("load processing configuration");

    assert!(
        job.tasks.len() >= 2,
        "test requires at least two processing tasks"
    );

    // -------------------------------------------------------------------------
    // Prepare isolated test directories
    // -------------------------------------------------------------------------

    let directory = test_directory();

    let staging = directory.join("staging");
    let dataset = directory.join("dataset");

    job.output.staging_path = staging.clone();
    job.output.dataset_path = dataset.clone();

    // -------------------------------------------------------------------------
    // Inject source failure
    // -------------------------------------------------------------------------

    // Task 1 processes its existing OKX archive successfully.
    // Task 2 intentionally references a nonexistent archive.
    job.tasks[1].input_path = directory.join("missing-source.zip");

    // Keep Parquet targets small for fast testing.
    config.resources.parquet.row_group_target_bytes = 16 * 1024;
    config.resources.parquet.file_target_bytes = 64 * 1024;

    // -------------------------------------------------------------------------
    // Execute processing job
    // -------------------------------------------------------------------------

    println!("\n{}", "=".repeat(90));
    println!("MARKETFORGE — FAILED EXECUTOR TEST");
    println!("{}", "=".repeat(90));

    println!("Job file     : {}", JOB_FILE);
    println!("Tasks        : {}", job.tasks.len());
    println!("Staging      : {}", staging.display());
    println!("Final output : {}", dataset.display());
    println!("Missing source: {}", job.tasks[1].input_path.display());

    let result =
        execute_processing_job(&job, &config).expect("executor should return structured failure");

    // -------------------------------------------------------------------------
    // Verify processing failure
    // -------------------------------------------------------------------------

    assert_eq!(
        result.status,
        ProcessingStatus::Failed,
        "expected processing failure"
    );

    assert!(
        staging.is_dir(),
        "staging directory should survive processing failure"
    );

    assert!(!dataset.exists(), "failed dataset must never be committed");

    // -------------------------------------------------------------------------
    // Verify failure report exists
    // -------------------------------------------------------------------------

    let failure_path = staging.join(PROCESSING_FAILURE_FILENAME);

    assert!(failure_path.is_file(), "failure.json must exist in staging");

    let report: ProcessingFailureReport =
        serde_json::from_slice(&fs::read(&failure_path).expect("read failure report"))
            .expect("deserialize failure report");

    // -------------------------------------------------------------------------
    // Verify failure metadata
    // -------------------------------------------------------------------------

    assert_eq!(report.job_id, job.job_id.0);
    assert_eq!(report.dataset_id, job.dataset_id.0);

    assert_eq!(report.failed_task_id, Some(job.tasks[1].task_id.0),);

    assert!(
        report.error_message.contains("missing-source.zip"),
        "failure report should identify the missing source"
    );

    // -------------------------------------------------------------------------
    // Verify partial processing metrics
    // -------------------------------------------------------------------------

    // Both the completed first task and failed second task
    // must appear in the processing report.
    assert_eq!(
        report.processing_metrics.tasks.len(),
        3,
        "failure report must preserve all attempted tasks"
    );

    assert!(
        report.processing_metrics.counters.records_read > 0,
        "completed first task should contribute records"
    );

    assert_eq!(report.processing_metrics.counters.tasks_completed, 2);

    assert_eq!(report.processing_metrics.counters.tasks_failed, 1);

    // The second task failed before source records were decoded.
    assert_eq!(report.processing_metrics.tasks[1].counters.records_read, 0);

    // -------------------------------------------------------------------------
    // Verify integrity evaluation
    // -------------------------------------------------------------------------

    // Final integrity evaluation should not exist because
    // processing stopped before all tasks completed.
    assert!(report.integrity_evaluation.is_none());

    // -------------------------------------------------------------------------
    // Verify failure report serialization
    // -------------------------------------------------------------------------

    let serialized = serde_json::to_string_pretty(&report).expect("serialize failure report");

    let restored: ProcessingFailureReport =
        serde_json::from_str(&serialized).expect("deserialize failure report");

    assert_eq!(restored, report);

    // -------------------------------------------------------------------------
    // Display results
    // -------------------------------------------------------------------------

    println!("\nRESULTS");

    println!("  Status           : {:?}", result.status);
    println!("  Failed task      : {:?}", report.failed_task_id);

    println!(
        "  Records read     : {}",
        report.processing_metrics.counters.records_read
    );

    println!(
        "  Records matched  : {}",
        report.processing_metrics.counters.records_matched
    );

    println!(
        "  Records rejected : {}",
        report.processing_metrics.counters.records_rejected
    );

    println!(
        "  Events written   : {}",
        report.processing_metrics.counters.events_written
    );

    println!(
        "  Tasks completed  : {}",
        report.processing_metrics.counters.tasks_completed
    );

    println!(
        "  Tasks failed     : {}",
        report.processing_metrics.counters.tasks_failed
    );

    println!("  Failure reason   : {}", report.error_message);
    println!("  Failure report   : {}", failure_path.display());

    println!("\nRESULT: PASS");

    // -------------------------------------------------------------------------
    // Cleanup
    // -------------------------------------------------------------------------

    // fs::remove_dir_all(directory).expect("remove test directory");
}
