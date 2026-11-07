use super::{migration::template, runtime::TestRun};
use marketforge_engine::{
    historical::{
        job::*,
        normalize::{manifest::DatasetManifest, session::execute_processing_session, *},
    },
    shared::io::container::SourceContainer,
};
use serde_json::json;
fn job(run: &TestRun) -> (ProcessingJob, ProcessingConfig) {
    let config = run.runtime(Default::default()).unwrap();
    let mut job = template("binance-spot-spot-BTCUSDT-trade-synthetic.json").unwrap();
    job.output.staging_path = run.path.join("staging");
    job.output.dataset_path = run.path.join("dataset");
    for (i, task) in job.tasks.iter_mut().enumerate() {
        let timestamp = 1_000_000 + i as i64 * 10;
        task.input_path = run.path.join(format!("source-{i}.csv"));
        std::fs::write(
            &task.input_path,
            format!("{},100.5,0.2,20.1,{timestamp},false,true\n", i + 1),
        )
        .unwrap();
        task.source_compression = SourceContainer::Plain;
        task.raw_schema["marketforge_source"]["source_time_bounds"] =
            json!({"start_timestamp_ns":timestamp*1000,"end_timestamp_ns":(timestamp+9)*1000});
    }
    // The prepared list is deliberately reverse chronological.
    job.tasks.reverse();
    (job, config)
}
fn manifest(job: &ProcessingJob) -> DatasetManifest {
    serde_json::from_slice(&std::fs::read(job.output.dataset_path.join("manifest.json")).unwrap())
        .unwrap()
}
#[test]
fn shared_trade_writer_spans_values_and_downstream_reader() {
    let run = TestRun::new("trades/stream_shared").unwrap();
    let (job, config) = job(&run);
    let r = execute_processing_job(&job, &config).unwrap();
    assert_eq!(r.status, ProcessingStatus::Complete, "{:?}", r.failure);
    let m = manifest(&job);
    assert_eq!(m.files.len(), 1);
    assert_eq!(m.events_written, 3);
    assert_eq!(m.streams.len(), 1);
    assert_eq!(
        m.streams[0]
            .sources
            .iter()
            .map(|s| (s.task_id, s.start_row, s.end_row))
            .collect::<Vec<_>>(),
        vec![(1, 0, 1), (2, 1, 2), (3, 2, 3)]
    );
    assert!(
        m.processing_metrics
            .tasks
            .iter()
            .all(|t| t.counters.events_written == 1)
    );
    use arrow_array::{Int64Array, StringArray};
    let batch = ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(job.output.dataset_path.join(&m.files[0].path)).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap()
    .next()
    .unwrap()
    .unwrap();
    assert_eq!(
        batch
            .column_by_name("event_timestamp_ns")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1_000_000_000, 1_000_010_000, 1_000_020_000]
    );
    assert_eq!(
        batch
            .column_by_name("trade_id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0),
        "1"
    );
    let input = marketforge_engine::historical::merge::ResolvedMergeInput {
        dataset_id: job.dataset_id.0,
        dataset_path: job.output.dataset_path.clone(),
        manifest: m,
        canonical_files: vec![],
        auxiliary_files: vec![],
    };
    let inventory = marketforge_engine::historical::merge::build_trade_inventory(&[input]).unwrap();
    let mut stream =
        marketforge_engine::historical::merge::TradeStream::new(job.dataset_id.0, inventory)
            .unwrap();
    let mut ids = Vec::new();
    while let Some(e) = stream.next_event().unwrap() {
        ids.push((e.task_id, e.canonical_row_offset));
    }
    assert_eq!(ids, vec![(1, 0), (2, 0), (3, 0)]);
}
#[test]
fn trade_rotation_and_worker_determinism() {
    for workers in [1, 3] {
        let run = TestRun::new("trades/stream_rotation").unwrap();
        let (job, mut config) = job(&run);
        config.resources.workers = workers;
        config.resources.parquet.row_group_target_bytes = 1;
        config.resources.parquet.file_target_bytes = 1;
        let r = execute_processing_job(&job, &config).unwrap();
        assert_eq!(r.status, ProcessingStatus::Complete, "{:?}", r.failure);
        let m = manifest(&job);
        assert_eq!(m.files.len(), 3);
        assert!(m.files.iter().all(|f| f.rows == 1));
        assert_eq!(m.streams[0].sources[0].task_id, 1);
    }
}
#[test]
fn observed_overlap_cannot_commit_and_other_job_survives() {
    let run = TestRun::new("trades/stream_failure").unwrap();
    let (mut bad, config) = job(&run);
    let mut good = bad.clone();
    good.job_id = JobId(9002);
    good.dataset_id = DatasetId(9002);
    good.output.staging_path = run.path.join("good-staging");
    good.output.dataset_path = run.path.join("good-dataset");
    for task in &mut bad.tasks {
        task.raw_schema["marketforge_source"]["source_time_bounds"] = json!({"start_timestamp_ns":0,"end_timestamp_ns":2_000_000_000i64,"partition":task.task_id.0});
    }
    std::fs::write(
        &bad.tasks[1].input_path,
        "2,100.5,0.2,20.1,999999,false,true\n",
    )
    .unwrap();
    // Good job uses independent sources.
    good.tasks.truncate(1);
    let result = execute_processing_session(&[bad.clone(), good.clone()], &config).unwrap();
    assert_eq!(result.jobs[0].status, ProcessingStatus::Failed);
    assert_eq!(result.jobs[1].status, ProcessingStatus::Complete);
    assert!(!bad.output.dataset_path.exists());
    assert!(result.jobs[0].failure_report_path.is_some());
}
#[test]
fn resource_admission_rejects_stream_without_deadlock() {
    let run = TestRun::new("resources/stream_limit").unwrap();
    let (job, mut config) = job(&run);
    config.resources.memory_budget_bytes = 1;
    let r = execute_processing_job(&job, &config).unwrap();
    assert_eq!(r.status, ProcessingStatus::Failed);
    assert!(!job.output.dataset_path.exists());
}
#[test]
fn unsafe_logical_ids_are_preserved_but_paths_are_encoded() {
    let run = TestRun::new("trades/unsafe_stream").unwrap();
    let (mut job, config) = job(&run);
    for task in &mut job.tasks {
        task.stream_id = StreamId("../../outside".into());
    }
    job.streams[0].stream_id = StreamId("../../outside".into());
    let r = execute_processing_job(&job, &config).unwrap();
    assert_eq!(r.status, ProcessingStatus::Complete);
    let m = manifest(&job);
    assert_eq!(m.streams[0].stream_id, "../../outside");
    assert!(m.files.iter().all(|f| !f.path.contains("..")));
}
#[test]
fn declared_overlap_and_missing_order_fail_without_commit() {
    for missing in [false, true] {
        let run = TestRun::new("trades/declared_overlap").unwrap();
        let (mut job, config) = job(&run);
        for t in &mut job.tasks {
            t.raw_schema["marketforge_source"]["source_time_bounds"] = if missing {
                serde_json::Value::Null
            } else {
                json!({"start_timestamp_ns":0,"end_timestamp_ns":2_000_000_000i64})
            };
        }
        let r = execute_processing_job(&job, &config).unwrap();
        assert_eq!(r.status, ProcessingStatus::Failed);
        assert!(!job.output.dataset_path.exists());
    }
}
