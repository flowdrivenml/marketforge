#![cfg(feature = "historical")]

use std::{fs, io::Write};

use crate::support::{
    normalization_fixture::{canonical_normalization_batch, select_normalization_sample},
    runtime::TestRun,
};
use marketforge_engine::{
    historical::job::{ContentType, InstrumentKind},
    shared::domain::canonical::Exchange,
};

use marketforge_engine::historical::normalize::{
    ProcessingStatus, execute_processing_job,
    failure::{PROCESSING_FAILURE_FILENAME, ProcessingFailureReport},
};

#[test]
fn failed_processing_persists_failure_report() {
    let mut run =
        TestRun::new("execution/executor_preflight_failure").expect("create isolated test run");

    let config = run.runtime(Default::default()).unwrap();
    let workload = canonical_normalization_batch().unwrap();
    let select = |symbol: &str| {
        select_normalization_sample(&workload, symbol, ContentType::Trades, 1, |task| {
            task.exchange == Exchange::Bybit
                && task.instrument_type == InstrumentKind::Spot
                && task.symbol.as_deref() == Some(symbol)
        })
        .unwrap()
    };
    let mut batch = select("BTCUSDT");
    batch.jobs[0]
        .tasks
        .extend(select("ETHBTC").jobs.remove(0).tasks);
    for task in &mut batch.jobs[0].tasks {
        let path = run.path.join(format!("{}.csv.gz", task.task_id.0));
        let mut gzip = flate2::write::GzEncoder::new(
            fs::File::create(&path).unwrap(),
            flate2::Compression::default(),
        );
        gzip.write_all(b"id,timestamp,price,volume,side,rpi\n7,1250,200,3,buy,0\n")
            .unwrap();
        gzip.finish().unwrap();
        task.input_path = path;
    }
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
    let [task_report] = report.processing_metrics.tasks.as_slice() else {
        panic!("expected one diagnostic for the missing source");
    };
    assert_eq!(task_report.task_id, job.tasks[1].task_id.0);
    assert_eq!(task_report.stream_id, job.tasks[1].stream_id.0);
    assert_eq!(task_report.counters, Default::default());
    let failure = task_report
        .failure
        .as_ref()
        .expect("missing source diagnostic");
    assert_eq!(failure.typed_cause, "source_open");
    assert_eq!(
        failure.physical_source,
        job.tasks[1].input_path.to_str().unwrap()
    );
    assert!(!failure.stopped_by_another_failure);

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
    run.finish(&Ok(())).unwrap();
}
