//! Opt-in complete prepared trade workload. Sources and counts are dynamic.
use crate::support::{assertions, fixtures::workload, runtime::TestRun};
use marketforge_engine::historical::{
    job::ContentType,
    normalize::{ProcessingStatus, session::execute_processing_session},
};

#[test]
fn processes_all_trade_archives() {
    let mut run =
        TestRun::new("execution/full_trade_correctness").expect("owned full-workload run");
    let mut batch = workload().expect("authoritative prepared batch");
    batch
        .jobs
        .retain(|job| job.output.content_type == ContentType::Trades);
    assert!(!batch.jobs.is_empty(), "prepared batch has no trade jobs");
    let config = run
        .runtime_from_general()
        .expect("production runtime configuration");
    let resolved = run
        .prepare(&batch, &config)
        .expect("production batch resolution");
    let expected_tasks = resolved
        .jobs
        .iter()
        .map(|job| job.tasks.len())
        .sum::<usize>();
    for job in &resolved.jobs {
        for task in &job.tasks {
            assert!(
                task.input_path.is_file(),
                "missing prepared source: {}",
                task.input_path.display()
            );
        }
    }
    let session = execute_processing_session(&resolved.jobs, &config)
        .expect("execute complete trade workload");
    assert_eq!(session.total_tasks, expected_tasks);
    assert_eq!(session.jobs.len(), resolved.jobs.len());
    let mut records_read = 0;
    let mut events_written = 0;
    for (job, result) in batch.jobs.iter().zip(&session.jobs) {
        assert_eq!(
            result.status,
            ProcessingStatus::Complete,
            "job {}: {:?}",
            job.job_id.0,
            result.failure
        );
        let manifest =
            assertions::committed(&run, job, result).expect("verified complete canonical dataset");
        manifest
            .processing_metrics
            .counters
            .validate()
            .expect("complete record accounting");
        records_read += manifest.processing_metrics.counters.records_read;
        events_written += manifest.events_written;
    }
    assert!(records_read > 0);
    assert!(events_written > 0);
    run.metadata["results"] = serde_json::json!({"jobs":session.jobs.len(),"tasks":expected_tasks,"records_read":records_read,"events_written":events_written});
    run.finish(&Ok(()))
        .expect("retain complete workload report");
}
