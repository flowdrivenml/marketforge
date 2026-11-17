//! Positive production-executor policy tests. All payloads are synthetic and
//! scoped to completion artifacts; production thresholds are not modified.
use super::disk_tests::{manifest, setup};
use crate::{
    historical::{
        job::*,
        normalize::{metrics::*, *},
    },
    shared::{
        domain::canonical::Exchange, historical::policies::*, io::container::SourceContainer,
    },
};
use serde_json::{Value, json};
fn directory() -> tempfile::TempDir {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .join("data/tests/historical/normalize/completion/policies/execution");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}
fn case(depth: bool, count: usize) -> (tempfile::TempDir, ProcessingJob, ProcessingConfig) {
    let root = directory();
    let (mut job, mut config) = setup(depth, root.path());
    while job.tasks.len() < count {
        let mut task = job.tasks[0].clone();
        task.task_id.0 = 1000 + job.tasks.len() as u64;
        task.input_path = root.path().join(format!("source-{}", job.tasks.len()));
        job.tasks.push(task);
    }
    job.tasks.truncate(count);
    for (i, task) in job.tasks.iter_mut().enumerate() {
        task.raw_schema["marketforge_source"]["source_priority"] = json!(i);
        if !depth {
            task.raw_schema["marketforge_trade_identity"] = json!({"scope":"global"});
        }
    }
    config.integrity_policy.timestamp_regression.action = IntegrityAction::Degrade;
    config.integrity_policy.timestamp_regression.max_count = Some(100);
    job.integrity_policy = config.integrity_policy.clone();
    (root, job, config)
}
fn trades(job: &ProcessingJob, source: usize, rows: &[(u64, i64, i64)]) {
    let mut text = "id,timestamp,price,volume,side,rpi\n".to_owned();
    for (id, ts, price) in rows {
        text += &format!("{id},{ts},{price},1,buy,0\n");
    }
    std::fs::write(&job.tasks[source].input_path, text).unwrap();
}
fn depth(job: &ProcessingJob, source: usize, rows: &[Value]) {
    std::fs::write(
        &job.tasks[source].input_path,
        rows.iter().map(|r| format!("{r}\n")).collect::<String>(),
    )
    .unwrap();
}
fn snapshot(ts: i64, id: u64, quantity: &str) -> Value {
    json!({"type":"snapshot","ts":ts,"cts":ts,"data":{"s":"BTCUSDT","u":id,"seq":id,"b":[["99",quantity]],"a":[["101","5"]]}})
}
fn delta(ts: i64, id: u64, quantity: &str) -> Value {
    json!({"type":"delta","ts":ts,"cts":ts,"data":{"s":"BTCUSDT","u":id,"seq":id,"b":[["99",quantity]],"a":[]}})
}
fn run(job: &ProcessingJob, config: &ProcessingConfig) -> ProcessingResult {
    execute_processing_job(job, config).unwrap()
}
fn complete(job: &ProcessingJob, result: &ProcessingResult) {
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    assert!(job.output.dataset_path.join("manifest.json").is_file());
}
fn failed(job: &ProcessingJob, result: &ProcessingResult) {
    assert_eq!(
        result.status,
        ProcessingStatus::Failed,
        "{:?}",
        result.failure
    );
    assert!(!job.output.dataset_path.exists());
}
fn times(job: &ProcessingJob) -> Vec<i64> {
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow_array::Int64Array;
    let mut times = Vec::new();
    for file in manifest(job).files {
        for batch in ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(job.output.dataset_path.join(file.path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        {
            let batch = batch.unwrap();
            let column = batch
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                times.push(column.value(row));
            }
        }
    }
    times
}
#[test]
fn trade_preserve_reorder_and_discard_execute_and_publish_distinct_semantics() {
    for policy in [
        TradeTimestamp::Preserve,
        TradeTimestamp::Reorder,
        TradeTimestamp::Discard,
    ] {
        let (_root, job, mut config) = case(false, 1);
        trades(&job, 0, &[(1, 1000, 200), (2, 999, 201), (3, 1001, 202)]);
        config.timestamp_regression.trades = TradeTimestampConfig {
            policy,
            max_regression_ms: 1,
            max_reorder_window_ms: 1,
            max_discarded_events: 1,
            ..Default::default()
        };
        let result = run(&job, &config);
        complete(&job, &result);
        let m = manifest(&job);
        assert_eq!(m.integrity_status, IntegrityStatus::Degraded);
        let expected = match policy {
            TradeTimestamp::Preserve => vec![1_000_000_000, 999_000_000, 1_001_000_000],
            TradeTimestamp::Reorder => vec![999_000_000, 1_000_000_000, 1_001_000_000],
            _ => vec![1_000_000_000, 1_001_000_000],
        };
        assert_eq!(times(&job), expected);
        let quality = m.processing_quality.as_ref().unwrap();
        assert_eq!(
            quality.timestamp_ordering,
            if policy == TradeTimestamp::Preserve {
                "native_source_order"
            } else {
                "event_timestamp_ns"
            }
        );
        assert_eq!(
            quality.known_unique_event_exclusions,
            u64::from(policy == TradeTimestamp::Discard)
        );
        assert_eq!(quality.loss_rate_denominator, 3);
        let source = m.processing_metrics.tasks[0].source.as_ref().unwrap();
        assert_eq!(source.processing.raw_timestamp_regressions, 1);
        let positions = std::fs::read_to_string(
            job.output
                .dataset_path
                .join(&m.streams[0].directory)
                .join("sources.jsonl"),
        )
        .unwrap();
        let ordinals = positions
            .lines()
            .map(|s| {
                serde_json::from_str::<Value>(s).unwrap()["source_record"]
                    .as_u64()
                    .unwrap()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            ordinals,
            match policy {
                TradeTimestamp::Preserve => vec![1, 2, 3],
                TradeTimestamp::Reorder => vec![2, 1, 3],
                _ => vec![1, 3],
            }
        );
        assert_eq!(m.protocol_version, 5);
    }
}
#[test]
fn strict_tolerance_cumulative_bounds_and_fatal_integrity_remain_independent() {
    for mode in 0..4 {
        let (_root, job, mut config) = case(false, 1);
        trades(&job, 0, &[(1, 1000, 200), (2, 999, 201), (3, 998, 202)]);
        config.timestamp_regression.trades = TradeTimestampConfig {
            policy: if mode == 0 {
                TradeTimestamp::Strict
            } else {
                TradeTimestamp::Reorder
            },
            max_regression_ms: 1,
            max_reorder_window_ms: 1,
            ..Default::default()
        };
        if mode == 2 {
            config.integrity_policy.timestamp_regression.action = IntegrityAction::Fail;
        }
        if mode == 3 {
            config.integrity_policy.enabled = false;
        }
        failed(&job, &run(&job, &config));
    }
    let (_root, job, mut config) = case(false, 1);
    trades(&job, 0, &[(1, 1000, 200), (2, 1000, 201)]);
    config.timestamp_regression.trades.max_regression_ms = 1;
    complete(&job, &run(&job, &config));
}
#[test]
fn verified_duplicate_regression_retains_raw_evidence_without_a_logical_ordering_failure() {
    let (_root, job, config) = case(false, 1);
    trades(&job, 0, &[(1, 1000, 200), (2, 1001, 201), (1, 1000, 200)]);
    complete(&job, &run(&job, &config));
    let m = manifest(&job);
    assert_eq!(m.events_written, 2);
    let source = m.processing_metrics.tasks[0].source.as_ref().unwrap();
    assert_eq!(source.duplicates, 1);
    assert_eq!(source.processing.raw_timestamp_regressions, 1);
    assert_eq!(source.processing.unresolved_timestamp_regressions, 0);
}
#[test]
fn equal_timestamp_distinct_nonconsecutive_exact_numeric_ids_are_lossless() {
    let (_root, mut job, config) = case(false, 2);
    trades(&job, 0, &[(100, 1000, 200), (101, 1000, 201)]);
    trades(&job, 1, &[(102, 1000, 202), (109, 1000, 203)]);
    job.tasks.reverse();
    complete(&job, &run(&job, &config));
    let m = manifest(&job);
    assert_eq!(m.events_written, 4);
    assert_eq!(m.processing_metrics.counters.rows_suppressed, 0);
    assert_eq!(
        super::super::policies::numeric_id_cmp(
            "99999999999999999999999999999",
            "100000000000000000000000000000"
        ),
        Some(std::cmp::Ordering::Less)
    );
    assert_eq!(
        super::super::policies::numeric_id_cmp("10", "9"),
        Some(std::cmp::Ordering::Greater)
    );
}
#[test]
fn earlier_later_prefer_coverage_not_task_order_and_never_drop_distinct_reliable_ids() {
    for selection in [Selection::Earlier, Selection::Later] {
        let (_root, mut job, mut config) = case(false, 3);
        trades(&job, 0, &[(10, 1000, 200), (20, 2000, 201)]);
        trades(&job, 1, &[(20, 2000, 202), (30, 3000, 203)]);
        trades(&job, 2, &[(20, 2000, 204), (40, 4000, 205)]);
        config.reconciliation.trades.overlap_policy = TradeOverlap::PreferSource;
        config.reconciliation.trades.prefer_source = PreferSource {
            selection,
            on_unique_data_loss: LossAction::MarkDegraded,
            max_excluded_events: 2,
            ..Default::default()
        };
        job.tasks.reverse();
        config.resources.workers = 3;
        complete(&job, &run(&job, &config));
        let m = manifest(&job);
        assert_eq!(m.events_written, 4);
        assert_eq!(
            m.processing_quality
                .as_ref()
                .unwrap()
                .unverified_observation_exclusions,
            2
        );
        assert_eq!(m.integrity_status, IntegrityStatus::Degraded);
        let survivor = m
            .processing_metrics
            .tasks
            .iter()
            .find(|t| t.source.as_ref().unwrap().conflicts == 0 && t.counters.events_written > 1)
            .unwrap();
        // Equal starts of B/C use stable source identity, independently of task order.
        let expected = if selection == Selection::Earlier {
            job.tasks[2].task_id.0
        } else {
            job.tasks
                .iter()
                .filter(|t| t.task_id.0 != job.tasks[2].task_id.0)
                .min_by_key(|t| t.input_path.clone())
                .unwrap()
                .task_id
                .0
        };
        assert_eq!(survivor.task_id, expected);
    }
}
#[test]
fn unverified_nested_and_chained_overlaps_are_bounded_by_count_rate_and_contiguous_span() {
    for failure in 0..4 {
        let (_root, mut job, mut config) = case(false, 2);
        for task in &mut job.tasks {
            task.raw_schema["marketforge_trade_identity"] = json!({"scope":"unreliable"});
        }
        trades(&job, 0, &[(1, 1000, 200), (2, 2000, 201), (5, 5000, 202)]);
        trades(&job, 1, &[(6, 2000, 203), (7, 3000, 204), (8, 4000, 205)]);
        config.reconciliation.trades.overlap_policy = TradeOverlap::PreferSource;
        config.reconciliation.trades.prefer_source = PreferSource {
            on_unique_data_loss: LossAction::MarkDegraded,
            max_excluded_events: 3,
            max_excluded_interval_ms: Some(2000),
            ..Default::default()
        };
        if failure == 1 {
            config
                .reconciliation
                .trades
                .prefer_source
                .max_excluded_events = 2;
        }
        if failure == 2 {
            config
                .reconciliation
                .trades
                .prefer_source
                .max_excluded_interval_ms = Some(1999);
        }
        if failure == 3 {
            config.reconciliation.trades.prefer_source.max_excluded_rate = Some(0.49);
        }
        let result = run(&job, &config);
        if failure == 0 {
            complete(&job, &result);
            assert_eq!(
                manifest(&job)
                    .processing_quality
                    .unwrap()
                    .unverified_observation_exclusions,
                3
            );
        } else {
            failed(&job, &result);
        }
    }
}
#[test]
fn default_preference_tables_do_not_authorize_conflict_loss() {
    for policy in [TradeOverlap::Strict, TradeOverlap::PreferSource] {
        let (_root, job, mut config) = case(false, 2);
        trades(&job, 0, &[(1, 1000, 200)]);
        trades(&job, 1, &[(1, 1000, 201)]);
        config.reconciliation.trades.overlap_policy = policy;
        if policy == TradeOverlap::Strict {
            config.reconciliation.trades.prefer_source = PreferSource {
                on_unique_data_loss: LossAction::MarkDegraded,
                max_excluded_events: 100,
                ..Default::default()
            };
        }
        failed(&job, &run(&job, &config));
    }
}
#[test]
fn changing_duplicate_depth_snapshot_is_represented_once_with_valid_books() {
    let (_root, job, config) = case(true, 2);
    let boundary = snapshot(2000, 11, "3");
    depth(
        &job,
        0,
        &[
            snapshot(1000, 10, "1"),
            delta(1500, 11, "2"),
            boundary.clone(),
        ],
    );
    depth(&job, 1, &[boundary, delta(3000, 12, "4")]);
    complete(&job, &run(&job, &config));
    let m = manifest(&job);
    assert_eq!(m.processing_metrics.counters.rows_suppressed, 1);
    assert_eq!(
        m.processing_metrics
            .tasks
            .iter()
            .map(|t| t.source.as_ref().unwrap().duplicates)
            .sum::<u64>(),
        1
    );
    let bounds = crate::shared::persistence::parquet::read_depth_stream_boundaries(
        &job.output.dataset_path.join(&m.streams[0].directory),
    )
    .unwrap();
    assert_eq!(
        bounds.sources[0]
            .boundary
            .final_state
            .as_ref()
            .unwrap()
            .state
            .bids[0]
            .quantity_base,
        Some(2.into())
    );
    assert_eq!(
        bounds.sources[1]
            .boundary
            .initial
            .as_ref()
            .unwrap()
            .state
            .bids[0]
            .quantity_base,
        Some(3.into())
    );
    assert_eq!(
        bounds.sources[1]
            .boundary
            .final_state
            .as_ref()
            .unwrap()
            .state
            .bids[0]
            .quantity_base,
        Some(4.into())
    );
}
#[test]
fn snapshot_recovery_retains_overlapping_independent_histories_without_loss() {
    let (_root, job, mut config) = case(true, 2);
    depth(&job, 0, &[snapshot(1000, 1, "1"), delta(3000, 2, "2")]);
    depth(&job, 1, &[snapshot(2000, 10, "3"), delta(4000, 11, "4")]);
    config.reconciliation.depth.overlap_policy = DepthOverlap::SnapshotRecovery;
    complete(&job, &run(&job, &config));
    let m = manifest(&job);
    assert_eq!(
        m.processing_quality
            .as_ref()
            .unwrap()
            .unverified_observation_exclusions,
        0
    );
    assert!(!m.processing_quality.as_ref().unwrap().continuity_proven);
    assert_eq!(
        m.processing_quality.as_ref().unwrap().timestamp_ordering,
        "native_source_order"
    );
    assert_eq!(
        m.processing_metrics
            .tasks
            .iter()
            .map(|t| t
                .source
                .as_ref()
                .unwrap()
                .processing
                .published_logical_events)
            .sum::<u64>(),
        4
    );
}
#[test]
fn depth_preference_switches_at_snapshots_and_reenters_only_at_valid_boundaries() {
    for selection in [Selection::Earlier, Selection::Later] {
        let (_root, job, mut config) = case(true, 2);
        depth(
            &job,
            0,
            &[
                snapshot(1000, 1, "1"),
                snapshot(2000, 2, "2"),
                snapshot(3000, 3, "3"),
                snapshot(4000, 4, "4"),
                snapshot(5000, 5, "5"),
            ],
        );
        depth(
            &job,
            1,
            &[
                snapshot(2000, 10, "6"),
                snapshot(3000, 11, "7"),
                snapshot(4000, 12, "8"),
            ],
        );
        config.reconciliation.depth.overlap_policy = DepthOverlap::PreferSource;
        config.reconciliation.depth.prefer_source = DepthPreferSource {
            selection,
            on_unique_data_loss: LossAction::MarkDegraded,
            max_excluded_events: 3,
            ..Default::default()
        };
        complete(&job, &run(&job, &config));
        let m = manifest(&job);
        assert_eq!(
            m.processing_quality
                .as_ref()
                .unwrap()
                .unverified_observation_exclusions,
            3
        );
        let bounds = crate::shared::persistence::parquet::read_depth_stream_boundaries(
            &job.output.dataset_path.join(&m.streams[0].directory),
        )
        .unwrap();
        if selection == Selection::Later {
            assert_eq!(bounds.version, 3);
            assert_eq!(
                bounds
                    .sources
                    .iter()
                    .filter(|s| s.end_event > s.start_event)
                    .map(|s| s.task_id)
                    .collect::<Vec<_>>(),
                [
                    job.tasks[0].task_id.0,
                    job.tasks[1].task_id.0,
                    job.tasks[0].task_id.0
                ]
            );
        }
    }
}
#[test]
fn depth_source_boundary_accepts_proven_state_and_sequence_and_rejects_unsafe_switch() {
    for safe in [true, false] {
        let (_root, job, mut config) = case(true, 2);
        depth(
            &job,
            0,
            &[
                snapshot(1000, 1, "1"),
                snapshot(2000, 2, "2"),
                snapshot(3000, 3, "3"),
                delta(5000, 4, if safe { "5" } else { "6" }),
            ],
        );
        depth(
            &job,
            1,
            &[
                snapshot(2000, 1, "1"),
                snapshot(3000, 3, if safe { "3" } else { "4" }),
                snapshot(4000, 3, if safe { "3" } else { "4" }),
            ],
        );
        config.reconciliation.depth.overlap_policy = DepthOverlap::PreferSource;
        config.reconciliation.depth.prefer_source = DepthPreferSource {
            selection: Selection::Later,
            switch_boundary: SwitchBoundary::SourceBoundary,
            on_unique_data_loss: LossAction::MarkDegraded,
            max_excluded_events: 3,
            ..Default::default()
        };
        let result = run(&job, &config);
        if safe {
            complete(&job, &result);
        } else {
            failed(&job, &result);
        }
    }
}
#[test]
fn incremental_depth_timestamp_preserve_and_snapshot_recovery_keep_books_valid() {
    for policy in [DepthTimestamp::Preserve, DepthTimestamp::SnapshotRecovery] {
        let (_root, job, mut config) = case(true, 1);
        depth(
            &job,
            0,
            &[
                snapshot(1000, 1, "1"),
                delta(999, 2, "2"),
                snapshot(1001, 3, "3"),
                delta(1002, 4, "4"),
            ],
        );
        config.timestamp_regression.depth = DepthTimestampConfig {
            policy,
            max_regression_ms: 1,
            max_discarded_events: 1,
            ..Default::default()
        };
        complete(&job, &run(&job, &config));
        let m = manifest(&job);
        let source = m.processing_metrics.tasks[0].source.as_ref().unwrap();
        assert_eq!(
            source.processing.timestamp_exclusions,
            u64::from(policy == DepthTimestamp::SnapshotRecovery)
        );
        let bounds = crate::shared::persistence::parquet::read_depth_stream_boundaries(
            &job.output.dataset_path.join(&m.streams[0].directory),
        )
        .unwrap();
        assert_eq!(
            bounds.sources[0]
                .boundary
                .final_state
                .as_ref()
                .unwrap()
                .state
                .bids[0]
                .quantity_base,
            Some(4.into())
        );
    }
}
#[test]
fn recovery_without_snapshot_or_without_positive_allowance_never_publishes() {
    for allowance in [0, 1] {
        let (_root, job, mut config) = case(true, 1);
        depth(&job, 0, &[snapshot(1000, 1, "1"), delta(999, 2, "2")]);
        config.timestamp_regression.depth = DepthTimestampConfig {
            policy: DepthTimestamp::SnapshotRecovery,
            max_regression_ms: 1,
            max_discarded_events: allowance,
            ..Default::default()
        };
        failed(&job, &run(&job, &config));
    }
}
#[test]
fn actual_gate_snapshot_profile_preserves_one_millisecond_regression_without_sequence_fields() {
    let root = directory();
    let b = crate::historical::config::read_prepared_batch(
        "engine/tests/historical/fixtures/normalize.json",
        super::super::super::config::selection_test_support::root(),
    )
    .unwrap();
    let mut job = b
        .jobs
        .iter()
        .find(|j| j.output.content_type == ContentType::Depth)
        .unwrap()
        .clone();
    job.tasks.retain(|t| {
        t.exchange == Exchange::GateIo
            && t.market_category == crate::historical::config::MarketCategory::Inverse
            && t.source_options
                .as_ref()
                .is_some_and(|o| o.feed == crate::historical::config::Feed::Snapshot)
    });
    job.tasks.truncate(1);
    assert_eq!(job.tasks.len(), 1);
    let mut config = super::super::super::config::selection_test_support::runtime();
    config.resources.scratch_path = root.path().join("scratch");
    config.resources.normalization_limits.reconciliation_bytes = 4 << 20;
    config.integrity_policy.timestamp_regression.action = IntegrityAction::Degrade;
    config.integrity_policy.timestamp_regression.max_count = Some(10);
    config.timestamp_regression.depth = DepthTimestampConfig {
        policy: DepthTimestamp::Preserve,
        max_regression_ms: 1,
        ..Default::default()
    };
    job.output.staging_path = root.path().join("stage");
    job.output.dataset_path = root.path().join("dataset");
    let mut resolved = b
        .resolve_job(
            &job,
            &crate::historical::config::FormatCatalog::load(
                super::super::super::config::selection_test_support::root()
                    .join("data/.config/historical"),
            )
            .unwrap(),
            &config,
            &super::super::super::config::selection_test_support::root(),
        )
        .unwrap();
    resolved.tasks[0].input_path = root.path().join("snapshot.jsonl");
    resolved.tasks[0].source_compression = SourceContainer::Plain;
    assert!(resolved.tasks[0].stream_id.0.ends_with(".snapshot"));
    std::fs::write(&resolved.tasks[0].input_path,"{\"update\":\"1.000\",\"bids\":[{\"p\":\"99\",\"s\":\"1\"}],\"asks\":[{\"p\":\"101\",\"s\":\"1\"}]}\n{\"update\":\"0.999\",\"bids\":[{\"p\":\"99\",\"s\":\"2\"}],\"asks\":[{\"p\":\"101\",\"s\":\"2\"}]}\n").unwrap();
    complete(&resolved, &run(&resolved, &config));
    let m = manifest(&resolved);
    assert_eq!(
        m.processing_quality.unwrap().timestamp_ordering,
        "native_source_order"
    );
    let bounds = crate::shared::persistence::parquet::read_depth_stream_boundaries(
        &resolved.output.dataset_path.join(&m.streams[0].directory),
    )
    .unwrap();
    assert_eq!(bounds.sources[0].boundary.last_sequence, None);
}

#[test]
fn job_wide_loss_across_independent_streams_is_enforced_before_commit() {
    for (limit, floor, rate, success) in [
        (2, 1000, Some(0.0), true),
        (1, 1000, Some(0.0), false),
        (2, 0, Some(0.32), false),
        (2, 0, Some(1.0 / 3.0 + 0.0001), true),
    ] {
        let (_root, mut job, mut config) = case(false, 2);
        job.tasks[1].stream_id.0.push_str(".other");
        job.streams.push(StreamConfig {
            stream_id: job.tasks[1].stream_id.clone(),
            stream_rank: job.streams.iter().map(|s| s.stream_rank).max().unwrap() + 1,
        });
        for i in 0..2 {
            trades(&job, i, &[(1, 1000, 200), (2, 999, 201), (3, 1001, 202)]);
        }
        config.timestamp_regression.trades = TradeTimestampConfig {
            policy: TradeTimestamp::Discard,
            max_regression_ms: 1,
            max_discarded_events: limit,
            max_discarded_rate: rate,
            minimum_samples: floor,
            ..Default::default()
        };
        config.resources.workers = 2;
        let result = run(&job, &config);
        if success {
            complete(&job, &result);
            assert_eq!(
                manifest(&job)
                    .processing_quality
                    .unwrap()
                    .known_unique_event_exclusions,
                2
            );
        } else {
            failed(&job, &result);
            assert_eq!(result.events_written, 0);
            let report: Value = serde_json::from_reader(
                std::fs::File::open(result.failure_report_path.unwrap()).unwrap(),
            )
            .unwrap();
            assert_eq!(
                report["processing_metrics"]["counters"]["events_written"],
                0
            );
        }
    }
}
#[test]
fn native_policy_output_and_attribution_are_worker_and_task_order_independent() {
    let (_root, mut job, mut config) = case(false, 3);
    for i in 0..3 {
        trades(
            &job,
            i,
            &[
                (i as u64 * 3 + 1, 1000 + i as i64 * 10, 200),
                (i as u64 * 3 + 2, 999 + i as i64 * 10, 201),
            ],
        );
    }
    config.timestamp_regression.trades = TradeTimestampConfig {
        policy: TradeTimestamp::Reorder,
        max_regression_ms: 1,
        max_reorder_window_ms: 1,
        ..Default::default()
    };
    config.resources.workers = 1;
    complete(&job, &run(&job, &config));
    let first = manifest(&job);
    let first_times = times(&job);
    let first_positions = std::fs::read(
        job.output
            .dataset_path
            .join(&first.streams[0].directory)
            .join("sources.jsonl"),
    )
    .unwrap();
    job.output.dataset_path.set_file_name("second-dataset");
    job.output.staging_path.set_file_name("second-staging");
    job.tasks.reverse();
    config.resources.workers = 3;
    complete(&job, &run(&job, &config));
    let second = manifest(&job);
    assert_eq!(times(&job), first_times);
    assert_eq!(
        std::fs::read(
            job.output
                .dataset_path
                .join(&second.streams[0].directory)
                .join("sources.jsonl")
        )
        .unwrap(),
        first_positions
    );
    assert_eq!(first.streams, second.streams);
}
#[test]
fn recovery_snapshot_below_high_water_restores_full_state_once() {
    let (_root, job, mut config) = case(true, 1);
    depth(
        &job,
        0,
        &[
            snapshot(1000, 1, "1"),
            delta(999, 2, "2"),
            snapshot(999, 3, "3"),
            delta(1000, 4, "4"),
        ],
    );
    config.timestamp_regression.depth = DepthTimestampConfig {
        policy: DepthTimestamp::SnapshotRecovery,
        max_regression_ms: 1,
        max_discarded_events: 1,
        ..Default::default()
    };
    complete(&job, &run(&job, &config));
    let m = manifest(&job);
    let source = m.processing_metrics.tasks[0].source.as_ref().unwrap();
    assert_eq!(source.processing.timestamp_exclusions, 1);
    let bounds = crate::shared::persistence::parquet::read_depth_stream_boundaries(
        &job.output.dataset_path.join(&m.streams[0].directory),
    )
    .unwrap();
    assert_eq!(
        bounds.sources[0]
            .boundary
            .final_state
            .as_ref()
            .unwrap()
            .state
            .bids[0]
            .quantity_base,
        Some(4.into())
    );
}

fn replay_bids(job: &ProcessingJob) -> Vec<rust_decimal::Decimal> {
    use crate::shared::persistence::parquet::i256_to_decimal;
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow_array::{Decimal256Array, StringArray, UInt8Array, UInt64Array};
    let m = manifest(job);
    let mut levels = Vec::new();
    for part in &m.files {
        for batch in ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(job.output.dataset_path.join(&part.path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        {
            let b = batch.unwrap();
            let side = b
                .column_by_name("side")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let price = b
                .column_by_name("price")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();
            let qty = b
                .column_by_name("quantity_base")
                .unwrap()
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap();
            for r in 0..b.num_rows() {
                levels.push((
                    side.value(r).to_owned(),
                    i256_to_decimal(price.value(r)).unwrap(),
                    i256_to_decimal(qty.value(r)).unwrap(),
                ));
            }
        }
    }
    let mut book = std::collections::BTreeMap::new();
    let mut result = Vec::new();
    let mut offset = 0;
    for batch in ParquetRecordBatchReaderBuilder::try_new(
        std::fs::File::open(
            job.output
                .dataset_path
                .join(&m.streams[0].directory)
                .join("events.parquet"),
        )
        .unwrap(),
    )
    .unwrap()
    .build()
    .unwrap()
    {
        let b = batch.unwrap();
        let starts = b
            .column_by_name("canonical_row_offset")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let counts = b
            .column_by_name("canonical_row_count")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        let boundaries = b
            .column_by_name("event_boundary")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt8Array>()
            .unwrap();
        for r in 0..b.num_rows() {
            assert_eq!(starts.value(r), offset);
            if boundaries.value(r) == 1 {
                book.clear();
            }
            for (side, price, qty) in &levels[offset as usize..(offset + counts.value(r)) as usize]
            {
                if qty.is_zero() {
                    book.remove(&(side.clone(), *price));
                } else {
                    book.insert((side.clone(), *price), *qty);
                }
            }
            offset += counts.value(r);
            assert_eq!(book.len(), 2);
            result.push(book[&("bid".into(), 99.into())]);
            assert_eq!(book[&("ask".into(), 101.into())], 5.into());
        }
    }
    assert_eq!(offset as usize, levels.len());
    result
}
#[test]
fn published_depth_events_replay_complete_states_with_native_regression_and_recovery() {
    for (policy, expected) in [
        (DepthTimestamp::Preserve, vec![1, 2, 3, 4]),
        (DepthTimestamp::SnapshotRecovery, vec![1, 3, 4]),
    ] {
        let (_root, job, mut config) = case(true, 1);
        depth(
            &job,
            0,
            &[
                snapshot(1000, 1, "1"),
                delta(999, 2, "2"),
                snapshot(999, 3, "3"),
                delta(1000, 4, "4"),
            ],
        );
        config.timestamp_regression.depth = DepthTimestampConfig {
            policy,
            max_regression_ms: 1,
            max_discarded_events: 1,
            ..Default::default()
        };
        complete(&job, &run(&job, &config));
        assert_eq!(
            replay_bids(&job),
            expected.into_iter().map(Into::into).collect::<Vec<_>>()
        );
    }
}
#[test]
fn single_snapshot_sources_with_reordered_json_keys_deduplicate_without_losing_initialization() {
    let (_root, job, config) = case(true, 2);
    let same = snapshot(1000, 1, "1");
    depth(&job, 0, &[same.clone()]);
    let reordered = format!(
        "{{\"data\":{},\"cts\":1000,\"ts\":1000,\"type\":\"snapshot\"}}\n",
        same["data"]
    );
    std::fs::write(&job.tasks[1].input_path, reordered).unwrap();
    complete(&job, &run(&job, &config));
    assert_eq!(replay_bids(&job), vec![1.into()]);
    assert_eq!(
        manifest(&job)
            .processing_metrics
            .tasks
            .iter()
            .map(|t| t.source.as_ref().unwrap().duplicates)
            .sum::<u64>(),
        1
    );
}

#[test]
fn reorder_scratch_pressure_retains_typed_task_failure_and_releases_reservations() {
    let (_root, job, mut config) = case(false, 2);
    for i in 0..2 {
        trades(&job, i, &[(1, 1000, 200), (2, 999, 201)]);
    }
    config.timestamp_regression.trades = TradeTimestampConfig {
        policy: TradeTimestamp::Reorder,
        max_regression_ms: 1,
        max_reorder_window_ms: 1,
        ..Default::default()
    };
    config.resources.scratch_budget_bytes = 30;
    let result = run(&job, &config);
    failed(&job, &result);
    assert_eq!(result.events_written, 0);
    let report: crate::historical::normalize::failure::ProcessingFailureReport =
        serde_json::from_reader(std::fs::File::open(result.failure_report_path.unwrap()).unwrap())
            .unwrap();
    let actual = report
        .processing_metrics
        .tasks
        .iter()
        .filter_map(|t| t.failure.as_ref())
        .find(|f| f.typed_cause == "resource_limit")
        .unwrap();
    assert_eq!(actual.job_id, job.job_id.0);
    assert_eq!(actual.task_id, job.tasks[0].task_id.0);
    assert_eq!(
        actual.physical_source,
        job.tasks[0].input_path.to_string_lossy()
    );
    assert!(
        actual.details.as_ref().unwrap()["requested"]
            .as_u64()
            .unwrap()
            > 30
    );
    assert!(
        report
            .processing_metrics
            .tasks
            .iter()
            .filter_map(|t| t.failure.as_ref())
            .any(|f| f.stopped_by_another_failure)
    );
    assert_eq!(
        std::fs::read_dir(&config.resources.scratch_path)
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn loss_spans_follow_native_source_order_instead_of_identity_sort_order() {
    let (_root, job, mut config) = case(false, 2);
    trades(&job, 0, &[(1, 500, 200), (10, 1000, 201), (30, 3000, 203)]);
    trades(
        &job,
        1,
        &[(10, 1000, 202), (20, 2000, 204), (30, 3000, 205)],
    );
    config.reconciliation.trades.overlap_policy = TradeOverlap::PreferSource;
    config.reconciliation.trades.prefer_source = PreferSource {
        on_unique_data_loss: LossAction::MarkDegraded,
        max_excluded_events: 2,
        max_excluded_interval_ms: Some(0),
        ..Default::default()
    };
    complete(&job, &run(&job, &config));
    let m = manifest(&job);
    assert_eq!(m.events_written, 4);
    assert_eq!(
        m.processing_metrics.tasks[1]
            .source
            .as_ref()
            .unwrap()
            .processing
            .max_overlap_span_ns,
        0
    );
}

#[test]
fn three_nested_depth_sources_have_deterministic_snapshot_ownership_and_full_replay() {
    for selection in [Selection::Earlier, Selection::Later] {
        let (_root, mut job, mut config) = case(true, 3);
        for (i, start, end, offset) in [(0, 1, 7, 0), (1, 2, 6, 20), (2, 3, 5, 30)] {
            let rows = (start..=end)
                .map(|n| {
                    snapshot(
                        n * 1000,
                        n as u64 + offset,
                        &(n + offset as i64).to_string(),
                    )
                })
                .collect::<Vec<_>>();
            depth(&job, i, &rows);
        }
        config.reconciliation.depth.overlap_policy = DepthOverlap::PreferSource;
        config.reconciliation.depth.prefer_source = DepthPreferSource {
            selection,
            on_unique_data_loss: LossAction::MarkDegraded,
            max_excluded_events: 8,
            ..Default::default()
        };
        job.tasks.reverse();
        config.resources.workers = 3;
        complete(&job, &run(&job, &config));
        let expected = if selection == Selection::Earlier {
            vec![1, 2, 3, 4, 5, 6, 7]
        } else {
            vec![1, 22, 33, 34, 35, 26, 7]
        };
        assert_eq!(
            replay_bids(&job),
            expected.into_iter().map(Into::into).collect::<Vec<_>>()
        );
        assert_eq!(
            manifest(&job)
                .processing_quality
                .unwrap()
                .unverified_observation_exclusions,
            8
        );
    }
}
#[test]
fn incomplete_trade_position_provenance_is_rejected_by_manifest_validation() {
    let (_root, job, mut config) = case(false, 1);
    trades(&job, 0, &[(1, 1000, 200), (2, 999, 201)]);
    config.timestamp_regression.trades = TradeTimestampConfig {
        policy: TradeTimestamp::Preserve,
        max_regression_ms: 1,
        ..Default::default()
    };
    complete(&job, &run(&job, &config));
    let m = manifest(&job);
    std::fs::write(
        job.output
            .dataset_path
            .join(&m.streams[0].directory)
            .join("sources.jsonl"),
        "{\"canonical_row\":0,\"task_id\":1",
    )
    .unwrap();
    let error = crate::historical::normalize::manifest::DatasetManifest::from_processing_job(
        &job,
        &job.output.dataset_path,
        m.processing_metrics,
        m.integrity_evaluation,
    )
    .unwrap_err();
    assert!(error.to_string().contains("source-position validation"));
}

#[test]
fn native_order_dataset_requires_a_segment_aware_consumer() {
    let (root, job, mut config) = case(true, 1);
    depth(&job, 0, &[snapshot(1000, 1, "1"), delta(999, 2, "2")]);
    config.timestamp_regression.depth = DepthTimestampConfig {
        policy: DepthTimestamp::Preserve,
        max_regression_ms: 1,
        ..Default::default()
    };
    complete(&job, &run(&job, &config));
    assert_eq!(replay_bids(&job), vec![1.into(), 2.into()]);
    let merge = MergeJob {
        job_id: 1,
        operation: MergeOperation::Merge,
        inputs: vec![job.dataset_id.0],
        input_paths: std::collections::HashMap::from([(
            job.dataset_id.0,
            job.output.dataset_path.clone(),
        )]),
        time_range: MergeTimeRange {
            start_ns: 0,
            end_ns: 2_000_000_000,
        },
        resources: MergeResources {
            workers: 1,
            memory_limit_mb: 1024,
            max_open_files: 64,
        },
        partitioning: MergePartitioning {
            partitions_per_worker: 1,
        },
        parquet: MergeParquetConfig {
            row_group_target_mb: 1,
            file_target_mb: 4,
            compression_level: 3,
        },
        output: MergeOutput {
            dataset_path: root.path().join("merge"),
        },
    };
    let error = crate::historical::merge::resolve_merge_inputs(&merge, root.path()).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("event-index/segment-aware reader"),
        "{error}"
    );
}
