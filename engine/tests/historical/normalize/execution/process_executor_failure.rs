#![cfg(feature = "historical")]

use std::fs;

use crate::support::runtime::TestRun;

use marketforge_engine::historical::normalize::{
    ProcessingStatus, execute_processing_job,
    failure::{PROCESSING_FAILURE_FILENAME, ProcessingFailureReport},
};

#[test]
fn failed_processing_persists_failure_report() {
    let mut run =
        TestRun::new("execution/executor_preflight_failure").expect("create isolated test run");

    let config = run
        .runtime_from_general()
        .expect("load production normalization configuration");

    // Select two real sources dynamically from normalize.json.
    let jobs = super::process_session::selected_session_jobs(&mut run, &config);

    assert_eq!(jobs.len(), 2);
    assert_eq!(jobs[0].tasks.len(), 1);
    assert_eq!(jobs[1].tasks.len(), 1);

    // Select the same authoritative source tasks, then resolve
    // them together to construct a complete stream registry.
    let selected_ids: std::collections::BTreeSet<u64> = jobs
        .iter()
        .flat_map(|job| job.tasks.iter().map(|task| task.task_id.0))
        .collect();

    assert_eq!(selected_ids.len(), 2);

    let mut batch =
        crate::support::fixtures::workload().expect("load authoritative normalization batch");

    batch.jobs.retain(|job| {
        job.output.content_type == marketforge_engine::historical::job::ContentType::Trades
    });

    assert_eq!(batch.jobs.len(), 1);

    batch.jobs[0]
        .tasks
        .retain(|task| selected_ids.contains(&task.task_id.0));

    assert_eq!(batch.jobs[0].tasks.len(), 2);

    let resolved = run
        .prepare(&batch, &config)
        .expect("resolve both source tasks into one job");

    assert_eq!(resolved.jobs.len(), 1);

    let mut job = resolved.jobs.into_iter().next().unwrap();

    assert_eq!(job.tasks.len(), 2);
    assert_ne!(job.tasks[0].task_id, job.tasks[1].task_id);

    for task in &job.tasks {
        assert!(task.input_path.is_file());
    }

    // Inject a missing source into the second task.
    let missing = run.path.join("missing-source.zip");
    assert!(!missing.exists());

    job.tasks[1].input_path = missing;

    let staging = job.output.staging_path.clone();
    let dataset = job.output.dataset_path.clone();

    let result =
        execute_processing_job(&job, &config).expect("executor should return structured failure");

    assert_eq!(result.status, ProcessingStatus::Failed);

    assert!(staging.is_dir());
    assert!(!dataset.exists());

    let failure_path = staging.join(PROCESSING_FAILURE_FILENAME);

    assert!(
        failure_path.is_file(),
        "missing failure report: {}",
        failure_path.display()
    );

    let report: ProcessingFailureReport =
        serde_json::from_slice(&fs::read(&failure_path).expect("read failure report"))
            .expect("deserialize failure report");

    assert_eq!(report.job_id, job.job_id.0);
    assert_eq!(report.dataset_id, job.dataset_id.0);
    assert_eq!(report.failed_task_id, Some(job.tasks[1].task_id.0));

    assert!(
        report.error_message.contains("missing-source.zip"),
        "failure must identify the missing source: {}",
        report.error_message
    );

    // Preflight rejection must happen before processing any records.
    assert!(report.processing_metrics.tasks.is_empty());

    let counters = &report.processing_metrics.counters;

    assert_eq!(counters.records_read, 0);
    assert_eq!(counters.records_matched, 0);
    assert_eq!(counters.records_rejected, 0);
    assert_eq!(counters.events_written, 0);
    assert_eq!(counters.tasks_completed, 0);
    assert_eq!(counters.tasks_failed, 0);

    assert!(report.integrity_evaluation.is_none());

    // Failure reports must survive serialization unchanged.
    let serialized = serde_json::to_string_pretty(&report).unwrap();

    let restored: ProcessingFailureReport = serde_json::from_str(&serialized).unwrap();

    assert_eq!(restored, report);

    println!("Job:            {}", job.job_id.0);
    println!("Failed task:    {:?}", report.failed_task_id);
    println!("Records read:   {}", counters.records_read);
    println!("Failure report: {}", failure_path.display());
    println!("PREFLIGHT FAILURE REPORTING: PASS");
}
