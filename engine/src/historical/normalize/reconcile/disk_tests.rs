//! Tiny production-executor regressions. No archive fixture is read: only its
//! configuration metadata selects existing profiles; source bytes are synthetic.
use crate::historical::{
    config::{
        FormatCatalog,
        normalization_fixture::{canonical_normalization_batch, select_normalization_sample},
        selection_test_support as support,
    },
    job::ContentType,
    normalize::*,
};
use crate::shared::{
    io::container::SourceContainer,
    persistence::parquet::{read_depth_segments, read_depth_stream_boundaries},
};
use serde_json::json;
pub(super) fn setup(
    depth: bool,
    root: &std::path::Path,
) -> (crate::historical::job::ProcessingJob, ProcessingConfig) {
    let (kind, stream) = if depth {
        (ContentType::Depth, "bybit.spot.spot.BTCUSDT.depth")
    } else {
        (ContentType::Trades, "bybit.spot.spot.BTCUSDT.trades")
    };
    let b = select_normalization_sample(
        &canonical_normalization_batch().unwrap(),
        stream,
        kind,
        2,
        |task| task.stream_id.0 == stream,
    )
    .unwrap();
    let mut j = b.jobs[0].clone();
    j.output.dataset_path = root.join("dataset");
    j.output.staging_path = root.join("staging");
    let mut config = support::runtime();
    config.resources.scratch_path = root.join("scratch");
    config.resources.normalization_limits.reconciliation_bytes = 4 << 20;
    config.resources.parquet.row_group_target_bytes = 1024;
    config.resources.parquet.file_target_bytes = 8192;
    let mut job = b
        .resolve_job(
            &j,
            &FormatCatalog::load(support::root().join("data/.config/historical")).unwrap(),
            &config,
            &support::root(),
        )
        .unwrap();
    for (i, t) in job.tasks.iter_mut().enumerate() {
        t.input_path = root.join(format!("synthetic-{i}"));
        t.source_compression = SourceContainer::Plain;
    }
    (job, config)
}
pub(super) fn manifest(job: &crate::historical::job::ProcessingJob) -> manifest::DatasetManifest {
    serde_json::from_slice(&std::fs::read(job.output.dataset_path.join("manifest.json")).unwrap())
        .unwrap()
}
#[test]
fn trades_verified_duplicates_publish_exact_rows_and_suppression() {
    let root = tempfile::tempdir().unwrap();
    let (mut job, config) = setup(false, root.path());
    // Synthetic global identities exercise strict precedence, not BYBIT-T1 local IDs.
    for task in &mut job.tasks {
        task.raw_schema["marketforge_trade_identity"] = json!({"scope":"global"});
    }
    let same = "00000000-0000-0000-0000-000000000001";
    let tail = "00000000-0000-0000-0000-000000000002";
    std::fs::write(
        &job.tasks[0].input_path,
        format!("id,timestamp,price,volume,side,rpi\n{same},3000,200,3,buy,0\n"),
    )
    .unwrap();
    std::fs::write(
        &job.tasks[1].input_path,
        format!(
            "id,timestamp,price,volume,side,rpi\n{same},3000,200,3,buy,0\n{tail},4000,201,3,buy,0\n"
        ),
    )
    .unwrap();
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let m = manifest(&job);
    assert_eq!(m.events_written, 2);
    assert_eq!(m.streams.len(), 1);
    assert_eq!(m.processing_metrics.counters.rows_suppressed, 1);
    assert_eq!(m.processing_metrics.counters.rows_unpublished, 0);
    assert_eq!(
        m.processing_metrics
            .tasks
            .iter()
            .map(|t| t.source.as_ref().unwrap().duplicates)
            .sum::<u64>(),
        1
    );
    assert_eq!(
        m.streams[0]
            .sources
            .iter()
            .map(|s| s.row_count().unwrap())
            .collect::<Vec<_>>(),
        [1, 1]
    );
    assert!(!job.output.staging_path.exists());
    assert_eq!(
        std::fs::read_dir(&config.resources.scratch_path)
            .unwrap()
            .count(),
        0
    );
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow_array::{Int64Array, StringArray};
    let mut rows = Vec::new();
    for f in &m.files {
        let r = ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(job.output.dataset_path.join(&f.path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        for batch in r {
            let batch = batch.unwrap();
            let t = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let id = batch
                .column_by_name("trade_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((t.value(i), id.value(i).to_owned()));
            }
        }
    }
    assert_eq!(
        rows,
        [(3_000_000_000, same.into()), (4_000_000_000, tail.into())]
    );
}
#[test]
fn depth_replay_keeps_snapshots_zero_change_events_boundaries_and_segments() {
    let root = tempfile::tempdir().unwrap();
    let (job, config) = setup(true, root.path());
    // Priority is the reverse of chronology. Each source independently recovers.
    for (i, task) in job.tasks.iter().enumerate() {
        let ts = if i == 0 { 2000 } else { 1000 };
        let records = [
            json!({"type":"snapshot","ts":ts,"cts":ts,"data":{"s":"BTCUSDT","u":10,"seq":1,"b":[["99","2"]],"a":[["101","5"]]}}),
            json!({"type":"delta","ts":ts+1,"cts":ts+1,"data":{"s":"BTCUSDT","u":11,"seq":2,"b":[["99","2"]],"a":[]}}),
            json!({"type":"delta","ts":ts+2,"cts":ts+2,"data":{"s":"BTCUSDT","u":12,"seq":3,"b":[["99","3"]],"a":[]}}),
        ];
        std::fs::write(
            &task.input_path,
            records.iter().map(|v| format!("{v}\n")).collect::<String>(),
        )
        .unwrap();
    }
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let m = manifest(&job);
    assert_eq!(m.events_written, 6);
    assert_eq!(m.processing_metrics.counters.records_processed, 6);
    assert_eq!(m.processing_metrics.counters.records_rejected, 0);
    let directory = job.output.dataset_path.join(&m.streams[0].directory);
    let segments = read_depth_segments(&directory).unwrap();
    assert_eq!(
        segments
            .segments
            .iter()
            .map(|s| s.initial_event_offset)
            .collect::<Vec<_>>(),
        [0, 3]
    );
    let boundaries = read_depth_stream_boundaries(&directory).unwrap();
    assert_eq!(boundaries.sources.len(), 2);
    assert_eq!(boundaries.sources[0].task_id, job.tasks[1].task_id.0);
    for source in &boundaries.sources {
        assert_eq!(source.end_row - source.start_row, 3);
        assert_eq!(source.end_event - source.start_event, 3);
        assert_eq!(
            source
                .boundary
                .initial
                .as_ref()
                .unwrap()
                .state
                .level_count(),
            2
        );
        assert!(source.boundary.final_state.is_some());
    }
    use crate::shared::domain::book::boundary::fingerprint_book;
    for source in &boundaries.sources {
        for state in [&source.boundary.initial, &source.boundary.final_state] {
            let state = state.as_ref().unwrap();
            assert_eq!(state.fingerprint, fingerprint_book(&state.state));
        }
    }
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow_array::UInt64Array;
    let reader = ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(directory.join("events.parquet")).unwrap(),
    )
    .unwrap()
    .build()
    .unwrap();
    let mut indexed = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let column = |name| {
            batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
        };
        for i in 0..batch.num_rows() {
            indexed.push((
                column("event_ordinal").value(i),
                column("canonical_row_offset").value(i),
                column("canonical_row_count").value(i),
            ));
        }
    }
    assert_eq!(
        indexed,
        [
            (1, 0, 2),
            (2, 2, 0),
            (3, 2, 1),
            (1, 3, 2),
            (2, 5, 0),
            (3, 5, 1)
        ]
    );
    assert!(!job.output.staging_path.exists());
    assert_eq!(
        std::fs::read_dir(&config.resources.scratch_path)
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn scratch_exhaustion_does_not_publish_or_lose_source_failure() {
    let root = tempfile::tempdir().unwrap();
    let (job, mut config) = setup(false, root.path());
    config.resources.scratch_budget_bytes = 30;
    for t in &job.tasks {
        std::fs::write(&t.input_path,"id,timestamp,price,volume,side,rpi\n00000000-0000-0000-0000-000000000001,1000,200,3,buy,0\n").unwrap();
    }
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(result.status, ProcessingStatus::Failed);
    assert!(!job.output.dataset_path.exists());
    let report: failure::ProcessingFailureReport = serde_json::from_slice(
        &std::fs::read(result.failure_report_path.as_ref().unwrap()).unwrap(),
    )
    .unwrap();
    assert_eq!(report.failed_task_id, Some(job.tasks[0].task_id.0));
    let failure = result.failure.unwrap();
    assert_eq!(failure.code, ProcessingErrorCode::ScratchBudgetExceeded);
    assert!(failure.message.contains("scratch bytes"));
    assert!(failure.message.contains("reconciliation scratch write"));
    assert_eq!(
        std::fs::read_dir(&config.resources.scratch_path)
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn writer_failure_keeps_definitive_suppression_and_unpublished_survivors() {
    let root = tempfile::tempdir().unwrap();
    let (mut job, config) = setup(false, root.path());
    // Synthetic global identities exercise strict precedence, not BYBIT-T1 local IDs.
    for task in &mut job.tasks {
        task.raw_schema["marketforge_trade_identity"] = json!({"scope":"global"});
    }
    for task in &job.tasks {
        std::fs::write(&task.input_path,"id,timestamp,price,volume,side,rpi\n00000000-0000-0000-0000-000000000001,1000,200,3,buy,0\n").unwrap();
    }
    std::fs::create_dir_all(&job.output.staging_path).unwrap();
    std::fs::write(
        job.output.staging_path.join("streams"),
        b"injected writer path failure",
    )
    .unwrap();
    let units = streams::plan(&job).unwrap();
    let executions =
        streams::execute(&units[0], &config, &progress::ProgressReporter::disabled()).unwrap();
    let mut context = super::super::executor::ExecutionContext::default();
    let error = super::super::executor::finalize_processing_job(
        &job,
        &config,
        executions.into_iter().map(|(_, e)| e).collect(),
        &mut context,
    )
    .unwrap_err();
    assert!(matches!(
        error,
        crate::shared::error::MarketForgeError::StreamFailure { .. }
    ));
    assert_eq!(context.failed_task_id, None);
    assert_eq!(context.report.tasks.len(), 2);
    let counters = &context.report.counters;
    assert_eq!(counters.tasks_completed, 0);
    assert_eq!(counters.tasks_failed, 1);
    assert_eq!(
        (
            counters.events_normalized,
            counters.rows_suppressed,
            counters.rows_unpublished,
            counters.events_written
        ),
        (2, 1, 1, 0)
    );
    counters.validate().unwrap();
    assert!(!job.output.dataset_path.exists());
    drop(context);
    assert_eq!(
        std::fs::read_dir(&config.resources.scratch_path)
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn depth_recovery_after_gap_replays_authoritative_snapshot_segments() {
    let root = tempfile::tempdir().unwrap();
    let (mut job, mut config) = setup(true, root.path());
    // Isolated policy case; production runtime/profile policies are unchanged.
    config.integrity_policy.sequence_gap.action =
        crate::shared::historical::IntegrityAction::Degrade;
    config.integrity_policy.sequence_gap.max_count = Some(1);
    job.integrity_policy = config.integrity_policy.clone();
    let snapshot = |ts, u, qty| json!({"type":"snapshot","ts":ts,"cts":ts,"data":{"s":"BTCUSDT","u":u,"seq":u,"b":[["99",qty]],"a":[["101","5"]]}});
    let gap = json!({"type":"delta","ts":1001,"cts":1001,"data":{"s":"BTCUSDT","u":12,"seq":12,"b":[["99","7"]],"a":[]}});
    let messages = [
        vec![snapshot(1000, 10, "2"), gap, snapshot(1002, 20, "3")],
        vec![snapshot(2000, 30, "4")],
    ];
    for (task, rows) in job.tasks.iter().zip(messages) {
        std::fs::write(
            &task.input_path,
            rows.iter().map(|r| format!("{r}\n")).collect::<String>(),
        )
        .unwrap();
    }
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let m = manifest(&job);
    assert_eq!(m.events_written, 6);
    assert_eq!(m.processing_metrics.counters.records_rejected, 1);
    let directory = job.output.dataset_path.join(&m.streams[0].directory);
    let boundaries = read_depth_stream_boundaries(&directory).unwrap();
    assert_eq!(
        boundaries.sources[0].boundary.continuity,
        crate::shared::domain::book::boundary::BoundaryContinuity::RecoveredFromSnapshot
    );
    let segments = read_depth_segments(&directory).unwrap();
    assert_eq!(
        segments
            .segments
            .iter()
            .map(|s| s.initial_event_offset)
            .collect::<Vec<_>>(),
        [0, 2, 4]
    );
}

#[test]
fn depth_missing_initial_snapshot_and_ambiguous_overlap_never_publish() {
    for missing_snapshot in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (job, config) = setup(true, root.path());
        for (index, task) in job.tasks.iter().enumerate() {
            let msg = json!({"type":if missing_snapshot{"delta"}else{"snapshot"},"ts":1000,"cts":1000,"data":{"s":"BTCUSDT","u":10,"seq":10,"b":[["99",if index==0 {"2"} else {"3"}]],"a":[["101","5"]]}});
            std::fs::write(&task.input_path, format!("{msg}\n")).unwrap();
        }
        let result = execute_processing_job(&job, &config).unwrap();
        assert_eq!(result.status, ProcessingStatus::Failed);
        assert!(!job.output.dataset_path.exists());
        let message = result.failure.unwrap().message;
        assert!(
            message.to_ascii_lowercase().contains(if missing_snapshot {
                "snapshot"
            } else {
                "overlap"
            }),
            "{message}"
        );
        assert_eq!(
            std::fs::read_dir(&config.resources.scratch_path)
                .unwrap()
                .count(),
            0
        );
    }
}

#[test]
fn bybit_profile_source_local_ids_survive_disjoint_sources() {
    let root = tempfile::tempdir().unwrap();
    let (job, config) = setup(false, root.path());
    for (i, task) in job.tasks.iter().enumerate() {
        assert_eq!(
            task.raw_schema["marketforge_trade_identity"]["scope"],
            "source_local"
        );
        std::fs::write(
            &task.input_path,
            format!(
                "id,timestamp,price,volume,side,rpi\n1,{},200,3,buy,0\n",
                1000 + i * 1000
            ),
        )
        .unwrap();
    }
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let m = manifest(&job);
    assert_eq!(m.events_written, 2);
    assert_eq!(m.processing_metrics.counters.rows_suppressed, 0);
    assert_eq!(
        std::fs::read_dir(&config.resources.scratch_path)
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn identical_sequenced_boundary_snapshots_suppress_zero_change_checkpoint_and_replay_books() {
    use crate::shared::persistence::parquet::i256_to_decimal;
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow_array::{Decimal256Array, StringArray};
    use rust_decimal::Decimal;
    use std::collections::BTreeMap;
    let artifact =
        support::root().join("data/tests/historical/normalize/completion/bybit-boundary-unit");
    std::fs::create_dir_all(&artifact).unwrap();
    let root = tempfile::tempdir_in(artifact).unwrap();
    let (job, config) = setup(true, root.path());
    let record = |kind: &str, timestamp, u, bid: &str, ask: &str| json!({"type":kind,"ts":timestamp,"cts":timestamp,"data":{"s":"BTCUSDT","u":u,"seq":u,"b":[["99",bid]],"a":[["101",ask]]}});
    let first = record("snapshot", 1000, 10, "1", "1");
    let delta = record("delta", 2000, 11, "2", "1");
    let snapshot = record("snapshot", 2000, 11, "2", "1");
    let next = record("delta", 3000, 12, "2", "2");
    std::fs::write(
        &job.tasks[0].input_path,
        format!("{first}\n{delta}\n{snapshot}\n"),
    )
    .unwrap();
    // Same decoded JSON, deliberately different object field order.
    let reordered = "{\"ts\":2000,\"type\":\"snapshot\",\"data\":{\"u\":11,\"b\":[[\"99\",\"2\"]],\"seq\":11,\"s\":\"BTCUSDT\",\"a\":[[\"101\",\"1\"]]},\"cts\":2000}";
    std::fs::write(&job.tasks[1].input_path, format!("{reordered}\n{next}\n")).unwrap();
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let m = manifest(&job);
    assert_eq!(m.processing_metrics.counters.records_processed, 5);
    assert_eq!(
        m.processing_metrics
            .tasks
            .iter()
            .filter_map(|t| t.accepted_depth_events)
            .sum::<u64>(),
        5
    );
    assert_eq!(
        m.processing_metrics
            .tasks
            .iter()
            .filter_map(|t| t.source.as_ref())
            .map(|s| s.duplicates)
            .sum::<u64>(),
        1
    );
    let decision = m
        .processing_metrics
        .tasks
        .iter()
        .find(|t| t.task_id == job.tasks[0].task_id.0)
        .unwrap()
        .source
        .as_ref()
        .unwrap();
    assert_eq!(
        decision.boundary_duplicate_of_task,
        Some(job.tasks[1].task_id.0)
    );
    assert_eq!(decision.boundary_duplicate_sequence, Some(11));
    assert_eq!(
        decision
            .boundary_duplicate_source_sha256
            .as_ref()
            .unwrap()
            .len(),
        64
    );
    let directory = job.output.dataset_path.join(&m.streams[0].directory);
    let boundaries = read_depth_stream_boundaries(&directory).unwrap();
    assert_eq!(
        boundaries
            .sources
            .iter()
            .map(|s| s.end_event - s.start_event)
            .collect::<Vec<_>>(),
        [2, 2]
    );
    assert_eq!(
        boundaries.sources[0]
            .boundary
            .final_state
            .as_ref()
            .unwrap()
            .state,
        boundaries.sources[1]
            .boundary
            .initial
            .as_ref()
            .unwrap()
            .state
    );
    assert_eq!(m.events_written, 6);
    let segments = read_depth_segments(&directory).unwrap();
    let starts = segments
        .segments
        .iter()
        .map(|s| s.initial_event_offset)
        .collect::<std::collections::BTreeSet<_>>();
    let mut offset = 0u64;
    let mut book = BTreeMap::<(String, Decimal), Decimal>::new();
    for file in &m.files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(job.output.dataset_path.join(&file.path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            let side = batch
                .column_by_name("side")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let prices = batch
                .column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();
            let quantities = batch
                .column_by_name("quantity_base")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                if starts.contains(&offset) {
                    book.clear();
                }
                let key = (
                    side.value(row).to_owned(),
                    i256_to_decimal(prices.value(row)).unwrap(),
                );
                let quantity = i256_to_decimal(quantities.value(row)).unwrap();
                if quantity.is_zero() {
                    book.remove(&key);
                } else {
                    book.insert(key, quantity);
                }
                offset += 1;
            }
        }
    }
    assert_eq!(book.len(), 2);
    assert_eq!(
        book.values().copied().collect::<Vec<_>>(),
        [Decimal::from(2), Decimal::from(2)]
    );
    assert_eq!(
        boundaries.sources[1]
            .boundary
            .final_state
            .as_ref()
            .unwrap()
            .state
            .bids[0]
            .quantity_base,
        Some(Decimal::from(2))
    );
    assert_eq!(
        boundaries.sources[1]
            .boundary
            .final_state
            .as_ref()
            .unwrap()
            .state
            .asks[0]
            .quantity_base,
        Some(Decimal::from(2))
    );
}

#[test]
fn equal_depth_sequence_ids_do_not_deduplicate_delta_against_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let (job, config) = setup(true, root.path());
    let snapshot = json!({"type":"snapshot","ts":1000,"cts":1000,"data":{"s":"BTCUSDT","u":10,"seq":10,"b":[["99","1"]],"a":[["101","1"]]}});
    let delta = json!({"type":"delta","ts":2000,"cts":2000,"data":{"s":"BTCUSDT","u":11,"seq":11,"b":[["99","2"]],"a":[]}});
    let other = json!({"type":"snapshot","ts":2000,"cts":2000,"data":{"s":"BTCUSDT","u":11,"seq":11,"b":[["99","2"]],"a":[["101","1"]]}});
    std::fs::write(&job.tasks[0].input_path, format!("{snapshot}\n{delta}\n")).unwrap();
    std::fs::write(&job.tasks[1].input_path, format!("{other}\n")).unwrap();
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(result.status, ProcessingStatus::Failed);
    assert!(!job.output.dataset_path.exists());
}
