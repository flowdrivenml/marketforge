use super::{migration::template, runtime::TestRun};
use marketforge_engine::{
    historical::{
        job::*,
        merge::{self, DepthEventCursor, ResolvedMergeInput},
        normalize::{manifest::DatasetManifest, *},
    },
    shared::{
        io::container::SourceContainer,
        persistence::parquet::{
            i256_to_decimal, read_depth_segments, read_depth_stream_boundaries,
        },
    },
};
use serde_json::json;
fn fixture(run: &TestRun) -> (ProcessingJob, ProcessingConfig) {
    let mut job = template("bybit-spot-spot-BTCUSDT-l2-synthetic.json").unwrap();
    let config = run.runtime(Default::default()).unwrap();
    job.output.staging_path = run.path.join("staging");
    job.output.dataset_path = run.path.join("dataset");
    for (i, task) in job.tasks.iter_mut().enumerate() {
        let time = 1000 + i as i64 * 100;
        let q = i + 2;
        task.input_path = run.path.join(format!("depth-{i}.data"));
        task.source_compression = SourceContainer::Plain;
        let messages = [
            json!({"type":"snapshot","ts":time,"cts":time,"data":{"s":"BTCUSDT","u":10,"seq":1,"b":[["99",q.to_string()]],"a":[["101","5"]]}}),
            json!({"type":"delta","ts":time+1,"cts":time+1,"data":{"s":"BTCUSDT","u":11,"seq":2,"b":[["99",q.to_string()]],"a":[]}}),
            json!({"type":"delta","ts":time+2,"cts":time+2,"data":{"s":"BTCUSDT","u":12,"seq":3,"b":[["99",(q+1).to_string()]],"a":[]}}),
            json!({"type":"delta","ts":time+3,"cts":time+3,"data":{"s":"BTCUSDT","u":13,"seq":4,"b":[],"a":[["101","0"]]}}),
        ];
        std::fs::write(
            &task.input_path,
            messages
                .iter()
                .map(|m| format!("{m}\n"))
                .collect::<String>(),
        )
        .unwrap();
        task.raw_schema["marketforge_source"]["source_time_bounds"] =
            json!({"start_timestamp_ns":time*1_000_000,"end_timestamp_ns":(time+99)*1_000_000});
    }
    job.tasks.reverse();
    (job, config)
}
fn manifest(job: &ProcessingJob) -> DatasetManifest {
    serde_json::from_slice(&std::fs::read(job.output.dataset_path.join("manifest.json")).unwrap())
        .unwrap()
}
fn inventory(job: &ProcessingJob, m: DatasetManifest) -> Vec<merge::DepthTaskInventory> {
    merge::build_depth_inventory(&[ResolvedMergeInput {
        dataset_id: job.dataset_id.0,
        dataset_path: job.output.dataset_path.clone(),
        manifest: m,
        canonical_files: vec![],
        auxiliary_files: vec![],
    }])
    .unwrap()
}
#[test]
fn depth_shared_index_local_ordinals_zero_changes_and_replay() {
    use arrow_array::{Decimal256Array, StringArray};
    for workers in [1, 3] {
        let run = TestRun::new("depth/consolidation").unwrap();
        let (job, mut config) = fixture(&run);
        config.resources.workers = workers;
        let result = execute_processing_job(&job, &config).unwrap();
        assert_eq!(
            result.status,
            ProcessingStatus::Complete,
            "{:?}",
            result.failure
        );
        let m = manifest(&job);
        assert_eq!(
            (m.events_written, m.files_written, m.streams.len()),
            (12, 1, 1)
        );
        assert_eq!(m.auxiliary_files.len(), 3);
        let directory = job.output.dataset_path.join(&m.streams[0].directory);
        let boundaries = read_depth_stream_boundaries(&directory).unwrap();
        assert_eq!(boundaries.version, 2);
        let segments = read_depth_segments(&directory).unwrap();
        assert_eq!(
            segments
                .segments
                .iter()
                .map(|s| s.initial_event_offset)
                .collect::<Vec<_>>(),
            vec![0, 4, 8]
        );
        for (i, source) in boundaries.sources.iter().enumerate() {
            assert_eq!(
                (
                    source.task_id,
                    source.start_row,
                    source.end_row,
                    source.start_event,
                    source.end_event
                ),
                (
                    i as u64 + 1,
                    i as u64 * 4,
                    i as u64 * 4 + 4,
                    i as u64 * 4,
                    i as u64 * 4 + 4
                )
            );
            assert_eq!(
                source.boundary.initial.as_ref().unwrap().state.asks.len(),
                1
            );
            assert_eq!(
                source
                    .boundary
                    .final_state
                    .as_ref()
                    .unwrap()
                    .state
                    .asks
                    .len(),
                0
            );
        }
        let tasks = inventory(&job, m.clone());
        assert_eq!(tasks.len(), 3);
        for task in &tasks {
            let v = merge::validate_depth_events(task).unwrap();
            assert_eq!(
                (
                    v.events,
                    v.canonical_rows,
                    v.zero_row_events,
                    v.initialization_events
                ),
                (4, 4, 1, 1)
            );
            let mut cursor = DepthEventCursor::for_task(task, None).unwrap();
            let mut events = Vec::new();
            while let Some(e) = cursor.next_event().unwrap() {
                events.push((
                    e.event_ordinal,
                    e.canonical_row_offset,
                    e.canonical_row_count,
                ));
            }
            assert_eq!(events, vec![(1, 0, 2), (2, 2, 0), (3, 2, 1), (4, 3, 1)]);
        }
        // Replay persisted canonical rows independently; each source initialization
        // clears the preceding source's state at its explicit segment boundary.
        let reader = ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(job.output.dataset_path.join(&m.files[0].path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        let mut book = std::collections::BTreeMap::new();
        let mut row = 0u64;
        for batch in reader {
            let batch = batch.unwrap();
            for n in 0..batch.num_rows() {
                if row % 4 == 0 {
                    book.clear();
                }
                let text = |name: &str| {
                    batch
                        .column_by_name(name)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(n)
                };
                let decimal = |name: &str| {
                    i256_to_decimal(
                        batch
                            .column_by_name(name)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<Decimal256Array>()
                            .unwrap()
                            .value(n),
                    )
                    .unwrap()
                };
                let side = text("side").to_owned();
                let price = decimal("price");
                let quantity = decimal("quantity_base");
                if quantity.is_zero() {
                    book.remove(&(side, price));
                } else {
                    book.insert((side, price), quantity);
                }
                let q = (row / 4 + 2) as i64;
                if row % 4 == 1 {
                    assert_eq!(book.get(&("bid".into(), 99.into())), Some(&q.into()));
                    assert_eq!(book.get(&("ask".into(), 101.into())), Some(&5.into()));
                }
                if row % 4 == 2 {
                    assert_eq!(book.get(&("bid".into(), 99.into())), Some(&(q + 1).into()));
                }
                if row % 4 == 3 {
                    assert_eq!(book.len(), 1);
                    assert_eq!(book.get(&("bid".into(), 99.into())), Some(&(q + 1).into()));
                }
                row += 1;
            }
        }
        assert_eq!(row, 12);
    }
}
#[test]
fn depth_rollover_does_not_create_segments_or_drop_zero_events() {
    let run = TestRun::new("depth/rotation").unwrap();
    let (job, mut config) = fixture(&run);
    config.resources.parquet.row_group_target_bytes = 1;
    config.resources.parquet.file_target_bytes = 1;
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let m = manifest(&job);
    assert_eq!(m.files_written, 9);
    assert!(m.files.iter().all(|f| f.rows > 0));
    let tasks = inventory(&job, m);
    assert!(
        tasks
            .iter()
            .all(|t| merge::validate_depth_events(t).unwrap().zero_row_events == 1)
    );
}
#[test]
fn second_depth_source_cannot_borrow_previous_book() {
    let run = TestRun::new("depth/no_continuation").unwrap();
    let (mut job, config) = fixture(&run);
    // Remove the second source's snapshot; independent initialization must fail.
    let task = job.tasks.iter_mut().find(|t| t.task_id.0 == 2).unwrap();
    let bytes = std::fs::read_to_string(&task.input_path).unwrap();
    std::fs::write(
        &task.input_path,
        bytes.lines().skip(1).collect::<Vec<_>>().join("\n") + "\n",
    )
    .unwrap();
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(result.status, ProcessingStatus::Failed);
    assert!(!job.output.dataset_path.exists());
    assert!(result.failure_report_path.is_some());
}
#[test]
fn depth_recovery_segments_are_rebased_across_sources() {
    let run = TestRun::new("depth/recovery").unwrap();
    let (job, mut config) = fixture(&run);
    config.integrity_policy.enabled = false;
    let task = job.tasks.iter().find(|t| t.task_id.0 == 2).unwrap();
    let snapshot = |u, ts, p: &str| json!({"type":"snapshot","ts":ts,"cts":ts,"data":{"s":"BTCUSDT","u":u,"b":[[p,"2"]],"a":[["101","5"]]}});
    let delta = |u, ts, p: &str| json!({"type":"delta","ts":ts,"cts":ts,"data":{"s":"BTCUSDT","u":u,"b":[[p,"3"]],"a":[]}});
    let events = [
        snapshot(100, 1100, "99"),
        delta(101, 1101, "99"),
        delta(105, 1102, "99"),
        delta(106, 1103, "99"),
        snapshot(200, 1104, "98"),
        delta(201, 1105, "98"),
    ];
    std::fs::write(
        &task.input_path,
        events.iter().map(|e| format!("{e}\n")).collect::<String>(),
    )
    .unwrap();
    let r = execute_processing_job(&job, &config).unwrap();
    assert_eq!(r.status, ProcessingStatus::Complete, "{:?}", r.failure);
    let m = manifest(&job);
    let tasks = inventory(&job, m.clone());
    let recovered = tasks.iter().find(|t| t.task_id == 2).unwrap();
    let v = merge::validate_depth_events(recovered).unwrap();
    assert_eq!(v.initialization_events, 2);
    assert_eq!(v.events, 4);
    assert_eq!(
        recovered
            .segments
            .segments
            .iter()
            .map(|s| s.initial_event_offset)
            .collect::<Vec<_>>(),
        vec![0, 3]
    );
    let global =
        read_depth_segments(&job.output.dataset_path.join(&m.streams[0].directory)).unwrap();
    assert_eq!(
        global
            .segments
            .iter()
            .map(|s| s.initial_event_offset)
            .collect::<Vec<_>>(),
        vec![0, 4, 7, 10]
    );
    let boundaries =
        read_depth_stream_boundaries(&job.output.dataset_path.join(&m.streams[0].directory))
            .unwrap();
    let final_book = &boundaries.sources[1]
        .boundary
        .final_state
        .as_ref()
        .unwrap()
        .state;
    assert_eq!(final_book.bids[0].price, 98.into());
    assert_eq!(final_book.bids[0].quantity_base, Some(3.into()));
}
#[test]
fn depth_writer_failure_after_sources_remains_failed() {
    use marketforge_engine::{
        historical::normalize::{
            metrics::TaskMetrics, resolve_sequence_policy, worker::process_depth_task_with_metrics,
        },
        shared::persistence::parquet::ParquetDepthWriter,
    };
    let run = TestRun::new("depth/writer_failure").unwrap();
    let (job, config) = fixture(&run);
    let directory = run.path.join("partial-stream");
    let mut writer = ParquetDepthWriter::new(&directory, config.resources.parquet.clone()).unwrap();
    writer.enable_sources().unwrap();
    for task in job.tasks.iter().rev().take(2) {
        writer.begin_source(task.task_id.0).unwrap();
        process_depth_task_with_metrics(
            task,
            &mut writer,
            &mut TaskMetrics::default(),
            &config.integrity_policy,
            &Default::default(),
            resolve_sequence_policy(task).unwrap(),
        )
        .unwrap();
    }
    std::fs::create_dir(directory.join("part-000000.parquet")).unwrap();
    assert!(writer.finish().is_err());
    assert!(writer.is_failed());
    assert!(!writer.is_finished());
    assert!(!job.output.dataset_path.exists());
}
#[test]
fn downstream_materialization_registers_shared_files_and_source_metadata() {
    use merge::{
        DepthMetadataResolver, MaterializationBatchLimits, MaterializationPartitionInput,
        MaterializationPartitionWorker, MaterializationWorkerLimits, UnifiedMergeMode,
        UnifiedTimelineRecord,
    };
    let run = TestRun::new("depth/downstream").unwrap();
    let (job, config) = fixture(&run);
    assert_eq!(
        execute_processing_job(&job, &config).unwrap().status,
        ProcessingStatus::Complete
    );
    let tasks = inventory(&job, manifest(&job));
    let parquet = MergeParquetConfig {
        row_group_target_mb: 1,
        file_target_mb: 2,
        compression_level: 3,
    };
    let input = MaterializationPartitionInput {
        partition_id: 0,
        start_ns: 0,
        end_ns: 2_000_000_000,
        timeline_files: &[],
        trade_inventory: &[],
        depth_inventory: &tasks,
        streams: &[],
        mode: UnifiedMergeMode::Depth,
        parquet: &parquet,
        limits: MaterializationWorkerLimits {
            batch_limits: MaterializationBatchLimits::new(4, 100).unwrap(),
            reader_batch_size: 4,
            max_open_cursors: 8,
        },
        staging: &run.path,
    };
    let worker = MaterializationPartitionWorker::new(&input).unwrap();
    assert_eq!(worker.registered_depth_tasks(), 3);
    let mut metadata = DepthMetadataResolver::new();
    for task in &tasks {
        metadata
            .register_inventory_range(task, 0, 2_000_000_000)
            .unwrap();
        let timestamp = 1_000_000_000 + (task.task_id - 1) as i64 * 100_000_000;
        let reference = UnifiedTimelineRecord {
            event_timestamp_ns: timestamp,
            stream_rank: 0,
            event_kind: 1,
            dataset_id: task.dataset_id,
            task_id: task.task_id,
            event_ordinal: 1,
            canonical_row_offset: 0,
            canonical_row_count: 2,
            event_boundary: Some(1),
            source_operation: Some(0),
        };
        let result = metadata.resolve(&reference).unwrap();
        assert_eq!(result.sequence_start, Some(10));
    }
}
