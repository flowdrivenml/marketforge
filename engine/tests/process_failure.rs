#![cfg(feature = "process")]

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use marketforge_engine::{
    job::load_processing_job,
    process::{
        ProcessingErrorCode,
        failure::{
            PROCESSING_FAILURE_FILENAME, PROCESSING_FAILURE_VERSION, ProcessingFailureReport,
        },
        metrics::{IntegrityEvaluation, IntegrityStatus, ProcessingMetricsReport},
    },
};

static NEXT_TEST_ID: AtomicU64 = AtomicU64::new(0);

const JOB_FILE: &str = "okx-future-linear-BTC-USD_UM-261225-trade-20260901-20260904-d120.json";

fn test_directory() -> PathBuf {
    let id = NEXT_TEST_ID.fetch_add(1, Ordering::Relaxed);

    let directory = std::env::temp_dir().join(format!(
        "marketforge-failure-test-{}-{id}",
        std::process::id(),
    ));

    fs::create_dir_all(&directory).expect("create test directory");

    directory
}

fn load_job() -> marketforge_engine::job::ProcessingJob {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("project root");

    load_processing_job(root.join("data/.jobs/process").join(JOB_FILE))
        .expect("load processing job")
}

#[test]
fn serializes_failure_report() {
    let directory = test_directory();

    let job = load_job();

    let mut metrics = ProcessingMetricsReport::default();

    metrics.counters.records_read = 100;
    metrics.counters.records_matched = 50;
    metrics.counters.records_rejected = 2;
    metrics.counters.events_normalized = 48;
    metrics.counters.events_written = 48;
    metrics.counters.tasks_failed = 1;

    let mut evaluation = IntegrityEvaluation::default();
    evaluation.status = IntegrityStatus::Failed;

    let report = ProcessingFailureReport::new(
        &job,
        Some(job.tasks[0].task_id.0),
        ProcessingErrorCode::ProcessingFailure,
        "integrity policy threshold exceeded",
        metrics,
        Some(evaluation),
    );

    let path = report.write_to(&directory).expect("write failure report");

    assert_eq!(path, directory.join(PROCESSING_FAILURE_FILENAME),);

    let contents = fs::read_to_string(&path).expect("read failure report");

    let restored: ProcessingFailureReport =
        serde_json::from_str(&contents).expect("deserialize failure report");

    assert_eq!(restored, report);

    assert_eq!(restored.protocol_version, PROCESSING_FAILURE_VERSION,);

    assert_eq!(restored.processing_metrics.counters.records_read, 100);
    assert_eq!(restored.processing_metrics.counters.records_rejected, 2);

    assert_eq!(
        restored.integrity_evaluation.unwrap().status,
        IntegrityStatus::Failed,
    );

    println!("\nMARKETFORGE — FAILURE REPORT");
    println!("Job ID       : {}", report.job_id);
    println!("Dataset ID   : {}", report.dataset_id);
    println!("Failed task  : {:?}", report.failed_task_id);
    println!("Error        : {}", report.error_message);
    println!(
        "Records read : {}",
        report.processing_metrics.counters.records_read
    );
    println!("Report       : {}", path.display());

    fs::remove_dir_all(directory).expect("remove test directory");
}

#[test]
fn rejects_duplicate_failure_report() {
    let directory = test_directory();

    let job = load_job();

    let report = ProcessingFailureReport::new(
        &job,
        None,
        ProcessingErrorCode::ProcessingFailure,
        "synthetic failure",
        ProcessingMetricsReport::default(),
        None,
    );

    report.write_to(&directory).unwrap();

    assert!(report.write_to(&directory).is_err());

    fs::remove_dir_all(directory).unwrap();
}

#[test]
fn rejects_missing_staging_directory() {
    let directory = test_directory();

    let missing = directory.join("missing");

    let job = load_job();

    let report = ProcessingFailureReport::new(
        &job,
        None,
        ProcessingErrorCode::ProcessingFailure,
        "synthetic failure",
        ProcessingMetricsReport::default(),
        None,
    );

    assert!(report.write_to(&missing).is_err());

    fs::remove_dir_all(directory).unwrap();
}
