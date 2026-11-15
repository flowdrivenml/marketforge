use crate::case_run::CaseRun as TestRun;
use crate::support::migration::template;
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
    let mut job = template("binance.spot.spot.trades:BTCUSDT").unwrap();
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
fn observed_chronology_retains_unique_sources_and_other_job_survives() {
    let run = TestRun::new("trades/stream_failure").unwrap();
    let (bad, config) = job(&run);
    let mut good = bad.clone();
    good.job_id = JobId(9002);
    good.dataset_id = DatasetId(9002);
    good.output.staging_path = run.path.join("good-staging");
    good.output.dataset_path = run.path.join("good-dataset");
    std::fs::write(
        &bad.tasks[1].input_path,
        "2,100.5,0.2,20.1,999999,false,true\n",
    )
    .unwrap();
    // Good job uses independent sources.
    good.tasks.truncate(1);
    let result = execute_processing_session(&[bad.clone(), good.clone()], &config).unwrap();
    assert_eq!(result.jobs[0].status, ProcessingStatus::Complete);
    assert_eq!(result.jobs[1].status, ProcessingStatus::Complete);
    assert!(bad.output.dataset_path.exists());
    assert!(result.jobs[0].failure_report_path.is_none());
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
fn prepared_fixture_omits_external_coverage() {
    let value: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/normalize.json")).unwrap();
    for job in value["jobs"].as_array().unwrap() {
        for task in job["tasks"].as_array().unwrap() {
            assert!(task.get("source_time_bounds").is_none());
        }
    }
}
#[test]
fn production_trade_execution_preserves_degraded_task_local_order() {
    let run = TestRun::new("execution/degraded_trade_order").unwrap();
    let (mut job, mut config) = job(&run);
    job.tasks.truncate(1);
    let task = &mut job.tasks[0];
    std::fs::write(&task.input_path,"1,100,1,100,1000000,false,true\n2,100,1,100,999999,false,true\n3,100,1,100,1000001,false,true\n").unwrap();
    config.integrity_policy.timestamp_regression.action = IntegrityAction::Degrade;
    config.integrity_policy.timestamp_regression.max_count = Some(10);
    config.integrity_policy.timestamp_regression.max_rate = None;
    let r = execute_processing_job(&job, &config).unwrap();
    assert_eq!(r.status, ProcessingStatus::Complete, "{:?}", r.failure);
    let m = manifest(&job);
    assert_eq!(
        m.processing_metrics.integrity.counters.timestamp_regression,
        1
    );
    assert_eq!(m.integrity_status, metrics::IntegrityStatus::Degraded);
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
            .downcast_ref::<arrow_array::Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[1_000_000_000, 999_999_000, 1_000_001_000]
    );
}
#[test]
fn resource_preflight_isolates_unadmittable_job() {
    let run = TestRun::new("resources/session_admission").unwrap();
    let (bad, mut config) = job(&run);
    let mut good = bad.clone();
    good.tasks.truncate(1);
    good.job_id = JobId(9003);
    good.dataset_id = DatasetId(9003);
    good.output.staging_path = run.path.join("good-stage");
    good.output.dataset_path = run.path.join("good-data");
    // Multi-source reconciliation needs its workspace in addition to retained
    // reports. One direct source fits; the full-budget workspace cannot fit.
    config.resources.normalization_limits.reconciliation_bytes =
        config.resources.memory_budget_bytes;
    let r = execute_processing_session(&[bad.clone(), good.clone()], &config).unwrap();
    assert_eq!(r.jobs[0].status, ProcessingStatus::Failed);
    assert_eq!(r.jobs[1].status, ProcessingStatus::Complete);
    assert!(!bad.output.dataset_path.exists());
    assert!(good.output.dataset_path.exists());
}
#[test]
fn committed_provenance_supports_merge_and_conversion_without_private_jobs() {
    use marketforge_engine::{
        historical::{convert, merge},
        shared::domain::instrument::InstrumentCatalog,
    };
    let run = TestRun::new("execution/downstream_provenance").unwrap();
    let (job, config) = job(&run);
    assert_eq!(
        execute_processing_job(&job, &config).unwrap().status,
        ProcessingStatus::Complete
    );
    let m = manifest(&job);
    let input = merge::ResolvedMergeInput {
        dataset_id: job.dataset_id.0,
        dataset_path: job.output.dataset_path.clone(),
        manifest: m,
        canonical_files: vec![],
        auxiliary_files: vec![],
    };
    let streams = merge::resolve_unified_merge_streams(&[input], &run.path).unwrap();
    assert_eq!(streams[0].instrument_id, job.tasks[0].instrument_id);
    let mut manifest:merge::PhysicalMergeManifest=serde_json::from_value(json!({"protocol_version":1,"physical_schema_version":1,"job_id":1,"mode":"trades","input_datasets":[{"dataset_id":job.dataset_id.0,"dataset_path":job.output.dataset_path,"manifest_version":3}],"time_range":{"start_ns":0,"end_ns":2_000_000_000i64},"streams":streams,"events_written":0,"trade_events":0,"depth_events":0,"canonical_rows":0,"files_written":0,"row_groups":0,"start_timestamp_ns":null,"end_timestamp_ns":null,"files":[],"boundary_links":[],"skipped_initializations":[],"checkpoints":[],"initial_reconstruction":[]})).unwrap();
    let catalog_path = run.path.join("instruments.json");
    std::fs::write(&catalog_path,serde_json::to_vec(&json!({"version":1,"instruments":[{"exchange":"binance","symbol":"BTCUSDT","instrument_kind":"spot","contract_kind":null,"base_currency":"BTC","quote_currency":"USDT"}]})).unwrap()).unwrap();
    let catalog = InstrumentCatalog::load(&catalog_path).unwrap();
    let convert:ConvertJob=serde_json::from_value(json!({"job_id":2,"operation":"convert","input_merge":run.path.join("merge"),"conversion":{"target_quote":"USDT","price_source":"last_trade","max_staleness_ns":1000000},"resources":{"workers":1,"memory_limit_mb":512,"max_open_files":32},"output":{"dataset_path":run.path.join("converted")}})).unwrap();
    let resolved =
        convert::resolve_conversion_catalog(&convert, &manifest, &catalog, &run.path).unwrap();
    assert_eq!(resolved.sources.len(), 1);
    assert_eq!(resolved.sources[0].base_currency.as_str(), "BTC");
    // A modified committed interpretation is never silently trusted.
    let tampered = run.path.join("tampered-copy");
    std::fs::create_dir(&tampered).unwrap();
    std::fs::copy(
        job.output.dataset_path.join("processing-job.sha256"),
        tampered.join("processing-job.sha256"),
    )
    .unwrap();
    std::fs::write(tampered.join("processing-job.json"), "{}").unwrap();
    manifest.input_datasets[0].dataset_path = tampered.display().to_string();
    assert!(load_committed_processing_job(&tampered).is_err());
    assert!(convert::resolve_conversion_catalog(&convert, &manifest, &catalog, &run.path).is_err());
}
#[test]
fn manifest_stream_paths_and_duplicate_ownership_are_rejected() {
    let run = TestRun::new("execution/manifest_validation").unwrap();
    let (job, config) = job(&run);
    assert_eq!(
        execute_processing_job(&job, &config).unwrap().status,
        ProcessingStatus::Complete
    );
    let m = manifest(&job);
    m.validate_streams().unwrap();
    let mut unsafe_path = m.clone();
    unsafe_path.streams[0].directory = "../outside".into();
    assert!(unsafe_path.validate_streams().is_err());
    let mut duplicate = m.clone();
    duplicate.streams.push(duplicate.streams[0].clone());
    assert!(duplicate.validate_streams().is_err());
    let mut missing = m.clone();
    missing.streams.clear();
    assert!(missing.validate_streams().is_err());
    let mut gap = m;
    gap.streams[0].sources[1].start_row += 1;
    assert!(gap.validate_streams().is_err());
}
#[test]
fn physical_reader_resolves_source_rows_inside_shared_trade_part() {
    use marketforge_engine::historical::merge::{
        self, CanonicalParquetReader, CanonicalReferenceResolver, CanonicalRowGroupCache,
        CanonicalTaskKey, UnifiedTimelineRecord,
    };
    let run = TestRun::new("execution/physical_source_slice").unwrap();
    let (job, config) = job(&run);
    assert_eq!(
        execute_processing_job(&job, &config).unwrap().status,
        ProcessingStatus::Complete
    );
    let m = manifest(&job);
    let input = merge::ResolvedMergeInput {
        dataset_id: job.dataset_id.0,
        dataset_path: job.output.dataset_path,
        manifest: m,
        canonical_files: vec![],
        auxiliary_files: vec![],
    };
    let tasks = merge::build_trade_inventory(&[input]).unwrap();
    let mut reader = CanonicalReferenceResolver::new(
        CanonicalParquetReader::new(4).unwrap(),
        CanonicalRowGroupCache::new(1_048_576).unwrap(),
    );
    for task in &tasks {
        reader
            .register_source_task(
                CanonicalTaskKey {
                    dataset_id: task.dataset_id,
                    task_id: task.task_id,
                    event_kind: 0,
                },
                task.files
                    .iter()
                    .map(|f| (f.path.clone(), f.file_start_row, f.rows))
                    .collect(),
            )
            .unwrap();
    }
    let task = &tasks[1];
    let result = reader
        .resolve(UnifiedTimelineRecord {
            event_timestamp_ns: 1_000_010_000,
            stream_rank: 0,
            event_kind: 0,
            dataset_id: task.dataset_id,
            task_id: task.task_id,
            event_ordinal: 0,
            canonical_row_offset: 0,
            canonical_row_count: 1,
            event_boundary: None,
            source_operation: None,
        })
        .unwrap();
    assert_eq!(result.canonical_rows(), 1);
    assert_eq!(
        result.canonical_batches[0]
            .column_by_name("trade_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::StringArray>()
            .unwrap()
            .value(0),
        "2"
    );
}
#[test]
fn captured_request_priority_preserves_equal_timestamp_order() {
    let run = TestRun::new("trades/equal_partition_order").unwrap();
    let (mut job, config) = job(&run);
    for task in &mut job.tasks {
        std::fs::write(
            &task.input_path,
            format!("{},100,1,100,1000000,false,true\n", task.task_id.0),
        )
        .unwrap();
    }
    assert_eq!(
        execute_processing_job(&job, &config).unwrap().status,
        ProcessingStatus::Complete
    );
    let m = manifest(&job);
    assert_eq!(
        m.streams[0]
            .sources
            .iter()
            .map(|s| s.task_id)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    use marketforge_engine::historical::merge;
    let input = merge::ResolvedMergeInput {
        dataset_id: job.dataset_id.0,
        dataset_path: job.output.dataset_path.clone(),
        manifest: m,
        canonical_files: vec![],
        auxiliary_files: vec![],
    };
    let tasks = merge::build_trade_inventory(&[input]).unwrap();
    let mut stream = merge::TradeStream::new(job.dataset_id.0, tasks).unwrap();
    let mut ids = Vec::new();
    while let Some(e) = stream.next_event().unwrap() {
        ids.push(e.task_id);
    }
    assert_eq!(ids, vec![1, 2, 3]);
}
