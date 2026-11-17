//! Actual BITGET-T1 ZIP/CSV profile -> worker -> external reconciliation ->
//! committed Parquet. Native IDs exceed 2^53, as in the inspected archives.
use crate::{
    historical::{
        config::{FormatCatalog, read_prepared_batch, selection_test_support as support},
        job::{ContentType, InstrumentKind, ProcessingJob},
        normalize::{manifest::DatasetManifest, metrics::*, *},
    },
    shared::historical::policies::*,
};
use serde_json::Value;
use std::io::Write;
const BASE: u64 = 1498674744255070200;
fn case(spot: bool, count: usize) -> (tempfile::TempDir, ProcessingJob, ProcessingConfig) {
    let parent = support::root()
        .join("data/tests/historical/normalize/completion/bitget-reconciliation/unit");
    std::fs::create_dir_all(&parent).unwrap();
    let dir = tempfile::tempdir_in(parent).unwrap();
    let b = read_prepared_batch(
        "engine/tests/historical/fixtures/normalize_bitget.json",
        &support::root(),
    )
    .unwrap();
    let mut prepared = b
        .jobs
        .iter()
        .find(|j| j.output.content_type == ContentType::Trades)
        .unwrap()
        .clone();
    prepared.tasks.retain(|t| {
        t.instrument_type
            == if spot {
                InstrumentKind::Spot
            } else {
                InstrumentKind::Perpetual
            }
    });
    prepared.tasks.truncate(count);
    assert_eq!(prepared.tasks.len(), count);
    let mut config = support::runtime();
    config.resources.normalization_limits.reconciliation_bytes = 4 << 20;
    config.resources.scratch_path = dir.path().join("scratch");
    config.resources.parquet.row_group_target_bytes = 1024;
    config.resources.parquet.file_target_bytes = 8192;
    config.reconciliation.trades.overlap_policy = TradeOverlap::Strict;
    config.timestamp_regression.trades = Default::default();
    let mut job = b
        .resolve_job(
            &prepared,
            &FormatCatalog::load(support::root().join("data/.config/historical")).unwrap(),
            &config,
            &support::root(),
        )
        .unwrap();
    for (i, t) in job.tasks.iter_mut().enumerate() {
        t.input_path = dir.path().join(format!("source-{i}.zip"));
    }
    job.output.dataset_path = dir.path().join("dataset");
    job.output.staging_path = dir.path().join("staging");
    (dir, job, config)
}
fn source(job: &ProcessingJob, index: usize, rows: &[(Option<u64>, i64, i64)]) {
    let mut zip = zip::ZipWriter::new(std::fs::File::create(&job.tasks[index].input_path).unwrap());
    zip.start_file(
        "BTCUSDT_UMCBL_chunk.csv",
        zip::write::SimpleFileOptions::default(),
    )
    .unwrap();
    zip.write_all(b"trade_id,timestamp,price,side,volume(quote),size(base)\n")
        .unwrap();
    for (id, ts, price) in rows {
        writeln!(
            zip,
            "{},{ts},{price},buy,{price},1",
            id.map(|i| (BASE + i).to_string()).unwrap_or_default()
        )
        .unwrap();
    }
    zip.finish().unwrap();
}
fn run(job: &ProcessingJob, config: &ProcessingConfig) -> ProcessingResult {
    execute_processing_job(job, config).unwrap()
}
fn manifest(job: &ProcessingJob) -> DatasetManifest {
    serde_json::from_reader(
        std::fs::File::open(job.output.dataset_path.join("manifest.json")).unwrap(),
    )
    .unwrap()
}
fn events(job: &ProcessingJob) -> Vec<(u64, i64)> {
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use arrow_array::{Int64Array, StringArray};
    let mut result = Vec::new();
    for file in manifest(job).files {
        for batch in ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(job.output.dataset_path.join(file.path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
        {
            let b = batch.unwrap();
            let ids = b
                .column_by_name("trade_id")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let ts = b
                .column_by_name("event_timestamp_ns")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..b.num_rows() {
                result.push((ids.value(row).parse::<u64>().unwrap() - BASE, ts.value(row)));
            }
        }
    }
    result
}
fn complete(job: &ProcessingJob, result: &ProcessingResult, expected: &[u64], duplicates: u64) {
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let m = manifest(job);
    assert_eq!(
        events(job).iter().map(|r| r.0).collect::<Vec<_>>(),
        expected
    );
    assert_eq!(m.integrity_status, IntegrityStatus::Clean);
    assert_eq!(
        m.processing_metrics
            .tasks
            .iter()
            .map(|t| t.source.as_ref().unwrap().duplicates)
            .sum::<u64>(),
        duplicates
    );
    for t in &m.processing_metrics.tasks {
        let s = t.source.as_ref().unwrap();
        assert!(s.native_identity_order_validated);
        assert_eq!(s.processing.known_unique_event_exclusions, 0);
        assert_eq!(s.processing.unverified_observation_exclusions, 0);
    }
    assert_eq!(m.events_written, expected.len() as u64);
}
#[test]
fn bitget_equal_time_distinct_native_ids_are_lossless_for_both_feeds_and_policies() {
    for spot in [false, true] {
        for prefer in [false, true] {
            let (_dir, job, mut config) = case(spot, 2);
            source(&job, 0, &[(Some(8), 1000, 80339), (Some(9), 1000, 80339)]);
            source(&job, 1, &[(Some(10), 1000, 80339), (Some(14), 1000, 80339)]);
            if prefer {
                config.reconciliation.trades.overlap_policy = TradeOverlap::PreferSource;
                config.reconciliation.trades.prefer_source = PreferSource {
                    on_unique_data_loss: LossAction::MarkDegraded,
                    max_excluded_events: 1,
                    ..Default::default()
                };
            }
            complete(&job, &run(&job, &config), &[8, 9, 10, 14], 0);
            assert!(events(&job).iter().all(|r| r.1 == 1_000_000_000));
        }
    }
}
#[test]
fn bitget_partial_contained_equal_start_and_nonadjacent_duplicates_preserve_union() {
    for spot in [false, true] {
        for selection in [Selection::Earlier, Selection::Later] {
            for overlap in [TradeOverlap::Strict, TradeOverlap::PreferSource] {
                let (_dir, job, mut config) = case(spot, 3);
                source(
                    &job,
                    0,
                    &[
                        (Some(1), 1000, 100),
                        (Some(3), 3000, 100),
                        (Some(5), 5000, 100),
                    ],
                );
                source(
                    &job,
                    1,
                    &[
                        (Some(2), 2000, 100),
                        (Some(3), 3000, 100),
                        (Some(4), 4000, 100),
                    ],
                );
                source(
                    &job,
                    2,
                    &[
                        (Some(1), 1000, 100),
                        (Some(4), 4000, 100),
                        (Some(6), 6000, 100),
                    ],
                );
                config.reconciliation.trades.overlap_policy = overlap;
                config.reconciliation.trades.prefer_source = PreferSource {
                    selection,
                    ..Default::default()
                };
                complete(&job, &run(&job, &config), &[1, 2, 3, 4, 5, 6], 3);
            }
        }
    }
}
#[test]
fn bitget_duplicate_boundary_is_verified_even_under_strict() {
    let (_dir, job, config) = case(false, 2);
    source(&job, 0, &[(Some(1), 1000, 100), (Some(2), 1000, 100)]);
    source(&job, 1, &[(Some(2), 1000, 100), (Some(3), 1000, 100)]);
    complete(&job, &run(&job, &config), &[1, 2, 3], 1);
}
#[test]
fn bitget_missing_ids_conflicts_and_resets_never_become_preference_loss() {
    for scenario in 0..5 {
        for prefer in [false, true] {
            let (_dir, mut job, mut config) = case(false, 2);
            source(&job, 0, &[(Some(10), 1000, 100), (Some(20), 2000, 100)]);
            match scenario {
                0 => source(&job, 1, &[(None, 2000, 100), (Some(30), 3000, 100)]),
                1 => source(&job, 1, &[(Some(20), 2000, 101), (Some(30), 3000, 100)]),
                2 => source(&job, 1, &[(Some(1), 3000, 100), (Some(2), 4000, 100)]),
                3 => source(&job, 1, &[(Some(30), 3000, 100), (Some(25), 4000, 100)]),
                _ => {
                    source(&job, 1, &[(Some(30), 3000, 100)]);
                    job.tasks[1].raw_schema["marketforge_trade_identity"]["reset_domain"] =
                        serde_json::json!("unrelated-session");
                }
            }
            if prefer {
                config.reconciliation.trades.overlap_policy = TradeOverlap::PreferSource;
                config.reconciliation.trades.prefer_source = PreferSource {
                    on_unique_data_loss: LossAction::MarkDegraded,
                    max_excluded_events: 1000,
                    ..Default::default()
                };
            }
            let result = run(&job, &config);
            assert_eq!(
                result.status,
                ProcessingStatus::Failed,
                "scenario={scenario}"
            );
            assert!(!job.output.dataset_path.exists());
            assert_eq!(result.events_written, 0);
            if scenario < 4 {
                let report: Value = serde_json::from_reader(
                    std::fs::File::open(result.failure_report_path.unwrap()).unwrap(),
                )
                .unwrap();
                let task = report["processing_metrics"]["tasks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|t| {
                        t["failure"]["typed_cause"]
                            == if scenario == 1 {
                                "trade_reconciliation"
                            } else {
                                "trade_identity"
                            }
                    })
                    .unwrap();
                let context = &task["failure"]["details"]["reconciliation"];
                assert_eq!(context["canonical_stream_id"], job.tasks[0].stream_id.0);
                assert_eq!(
                    context["applied_exclusion_limit"],
                    config
                        .reconciliation
                        .trades
                        .prefer_source
                        .max_excluded_events
                );
                assert_eq!(context["unverified_excluded_count"], "0");
                assert!(!context["sampled_sources"].as_array().unwrap().is_empty());
            }
        }
    }
}
#[test]
fn bitget_worker_and_task_order_do_not_change_canonical_union() {
    let (_dir, mut job, mut config) = case(false, 3);
    source(&job, 0, &[(Some(1), 1000, 100), (Some(3), 3000, 100)]);
    source(&job, 1, &[(Some(2), 2000, 100), (Some(3), 3000, 100)]);
    source(&job, 2, &[(Some(3), 3000, 100), (Some(4), 4000, 100)]);
    config.resources.workers = 1;
    complete(&job, &run(&job, &config), &[1, 2, 3, 4], 2);
    let first = manifest(&job);
    let rows = events(&job);
    job.output.dataset_path.set_file_name("second-dataset");
    job.output.staging_path.set_file_name("second-staging");
    job.tasks.reverse();
    config.resources.workers = 3;
    complete(&job, &run(&job, &config), &[1, 2, 3, 4], 2);
    assert_eq!(events(&job), rows);
    assert_eq!(manifest(&job).streams, first.streams);
}

#[test]
fn bitget_original_ambiguity_loss_failure_has_bounded_actionable_context() {
    let (_dir, mut job, mut config) = case(false, 2);
    for task in &mut job.tasks {
        task.raw_schema["marketforge_trade_identity"] = serde_json::json!({"scope":"unreliable"});
    }
    source(&job, 0, &[(Some(1), 1000, 100)]);
    source(&job, 1, &[(Some(2), 1000, 100), (Some(3), 1000, 100)]);
    config.reconciliation.trades.overlap_policy = TradeOverlap::PreferSource;
    config.reconciliation.trades.prefer_source = PreferSource {
        on_unique_data_loss: LossAction::MarkDegraded,
        max_excluded_events: 1,
        ..Default::default()
    };
    let result = run(&job, &config);
    assert_eq!(result.status, ProcessingStatus::Failed);
    assert!(!job.output.dataset_path.exists());
    let report: Value =
        serde_json::from_reader(std::fs::File::open(result.failure_report_path.unwrap()).unwrap())
            .unwrap();
    let task = report["processing_metrics"]["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["failure"]["typed_cause"] == "trade_reconciliation")
        .unwrap();
    let context = &task["failure"]["details"]["reconciliation"];
    assert_eq!(
        context["canonical_stream_id"],
        "bitget.linear.perpetual.BTCUSDT.trades"
    );
    assert_eq!(context["unverified_excluded_count"], "2");
    assert_eq!(context["known_unique_excluded_count"], "0");
    assert_eq!(context["verified_duplicate_count"], "0");
    assert_eq!(context["applied_exclusion_limit"], 1);
    assert_eq!(context["source_count"], 2);
    let sources = context["sampled_sources"].as_array().unwrap();
    assert_eq!(sources.len(), 2);
    assert!(
        sources.iter().all(
            |s| s["overlap_with_focus_ns"] == serde_json::json!([1_000_000_000, 1_000_000_000])
        )
    );
    assert!(
        sources
            .iter()
            .all(|s| s["identity_contract"]["scope"] == "unreliable")
    );
    assert!(
        sources
            .iter()
            .any(|s| s["physical_source"] == job.tasks[1].input_path.to_string_lossy().as_ref())
    );
}

#[test]
fn bitget_timestamp_discard_cannot_remove_evidence_of_an_invalid_identity_run() {
    let (_dir, job, mut config) = case(false, 1);
    source(&job, 0, &[(Some(10), 1000, 100), (Some(20), 999, 100)]);
    config.timestamp_regression.trades = TradeTimestampConfig {
        policy: TradeTimestamp::Discard,
        max_regression_ms: 1,
        max_discarded_events: 1,
        ..Default::default()
    };
    config.integrity_policy.timestamp_regression.action =
        crate::shared::historical::IntegrityAction::Degrade;
    config.integrity_policy.timestamp_regression.max_count = Some(1);
    let result = run(&job, &config);
    assert_eq!(result.status, ProcessingStatus::Failed);
    assert!(!job.output.dataset_path.exists());
    assert!(
        result
            .failure
            .unwrap()
            .message
            .contains("chronological source union")
    );
}
