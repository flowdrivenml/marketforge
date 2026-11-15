#![cfg(feature = "historical")]

use std::fs;

use {
    crate::support::migration::template as load_processing_job,
    marketforge_engine::historical::normalize::ProcessingErrorCode,
    marketforge_engine::historical::normalize::failure::PROCESSING_FAILURE_FILENAME,
    marketforge_engine::historical::normalize::failure::PROCESSING_FAILURE_VERSION,
    marketforge_engine::historical::normalize::failure::ProcessingFailureReport,
    marketforge_engine::historical::normalize::metrics::IntegrityEvaluation,
    marketforge_engine::historical::normalize::metrics::IntegrityStatus,
    marketforge_engine::historical::normalize::metrics::ProcessingMetricsReport,
};

const JOB_FILE: &str = "okx.future.linear.trades:BTC-USD_UM-261225";

fn test_directory() -> crate::support::migration::PersistentDirectory {
    crate::support::migration::PersistentDirectory::new()
}

fn load_job() -> marketforge_engine::historical::job::ProcessingJob {
    load_processing_job(JOB_FILE).expect("load processing job")
}

#[test]
fn serializes_failure_report() {
    let directory = test_directory();

    let job = load_job();

    let mut metrics = ProcessingMetricsReport::default();

    metrics.counters.records_read = 100;
    metrics.counters.records_matched = 50;
    metrics.counters.records_skipped_instrument = 50;
    metrics.counters.records_processed = 48;
    metrics.counters.records_rejected = 2;
    metrics.counters.events_normalized = 48;
    metrics.counters.events_written = 48;
    metrics.counters.tasks_failed = 1;

    let evaluation = IntegrityEvaluation {
        status: IntegrityStatus::Failed,
        ..Default::default()
    };

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
}
