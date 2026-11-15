//! Small production-executor checks, never real archive data.
use super::disk_tests::{manifest, setup};
use crate::{
    historical::{job::ProcessingJob, normalize::*},
    shared::{
        domain::canonical::{EventEnvelope, Trade, TradeSide},
        historical::{
            IntegrityAction,
            config::{DiagnosticsMode, ScratchCodec, ScratchFormat},
        },
        persistence::parquet::TradeBatchBuilder,
    },
};
use serde_json::{Value, json};
use std::{fs, path::Path};
const HEADER: &str = "id,timestamp,price,volume,side,rpi\n";
fn write(job: &ProcessingJob, source: usize, rows: &str) {
    fs::write(&job.tasks[source].input_path, format!("{HEADER}{rows}")).unwrap();
}
fn diagnostic_report(root: &Path) -> Value {
    let path = fs::read_dir(root)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.file_name().to_string_lossy().starts_with("performance-"))
        .unwrap()
        .path()
        .join("performance.json");
    serde_json::from_slice(&fs::read(path).unwrap()).unwrap()
}
fn path_report(report: &Value) -> &Value {
    report["resources"]
        .as_object()
        .unwrap()
        .values()
        .find(|v| v.get("reason").is_some())
        .unwrap()
}
fn assert_failed(job: &ProcessingJob, result: &ProcessingResult) {
    assert_eq!(result.status, ProcessingStatus::Failed, "{result:?}");
    assert!(!job.output.dataset_path.exists());
    assert!(Path::new(result.failure_report_path.as_ref().unwrap()).is_file());
}
fn expected(job: &ProcessingJob, time: i64, id: &str, price: i64) -> Trade {
    let t = &job.tasks[0];
    Trade {
        envelope: EventEnvelope {
            event_timestamp_ns: time * 1_000_000,
            system_timestamp_ns: None,
            exchange: t.exchange,
            instrument_id: t.instrument_id,
            symbol: t.symbol.clone(),
            stream_id: t.stream_id.0.clone(),
        },
        trade_id: Some(id.into()),
        sequence: None,
        side: TradeSide::Buy,
        price: price.into(),
        quantity_base: Some(3.into()),
        quantity_quote: Some((price * 3).into()),
        quantity_contracts: None,
        is_rpi: Some(false),
        trade_iv: None,
        mark_iv: None,
        index_price: None,
        mark_price: None,
    }
}
fn assert_rows(job: &ProcessingJob, expected_rows: Vec<Trade>) {
    use ::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let m = manifest(job);
    let mut builder = TradeBatchBuilder::new();
    for row in expected_rows {
        builder.push(row);
    }
    let expected = builder.finish().unwrap().unwrap();
    let mut offset = 0;
    for file in &m.files {
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            fs::File::open(job.output.dataset_path.join(&file.path)).unwrap(),
        )
        .unwrap()
        .build()
        .unwrap();
        for batch in reader {
            let batch = batch.unwrap();
            assert_eq!(batch, expected.slice(offset, batch.num_rows()));
            offset += batch.num_rows();
        }
    }
    assert_eq!(offset, expected.num_rows());
}
#[test]
fn three_source_local_archives_reverse_order_keep_every_complete_trade_and_owner_without_sorts() {
    for memory in [2 << 20, 4 << 20] {
        for codec in [ScratchCodec::Zlib, ScratchCodec::Uncompressed] {
            for format in [ScratchFormat::Json, ScratchFormat::TypedTrades] {
                let root = tempfile::tempdir().unwrap();
                let (mut job, mut config) = setup(false, root.path());
                let mut third = job.tasks[0].clone();
                third.task_id.0 = job.tasks.iter().map(|t| t.task_id.0).max().unwrap() + 1;
                third.input_path = root.path().join("third-source");
                third.raw_schema["marketforge_source"]["task_id"] = json!(third.task_id.0);
                job.tasks.push(third);
                for (i, task) in job.tasks.iter_mut().enumerate() {
                    task.raw_schema["marketforge_source"]["source_priority"] = json!(i);
                }
                write(
                    &job,
                    0,
                    "1,1000,101,3,buy,0\n1,1001,102,3,buy,0\n2,1001,103,3,buy,0\n",
                );
                write(&job, 1, "1,2000,201,3,buy,0\n2,2002,202,3,buy,0\n");
                write(&job, 2, "1,3000,301,3,buy,0\n2,3001,302,3,buy,0\n");
                let owners: Vec<_> = job.tasks.iter().map(|t| t.task_id.0).collect();
                job.tasks.reverse();
                config.resources.normalization_limits.reconciliation_bytes = memory;
                config.scratch_codec = codec;
                config.scratch_format = format;
                config.resources.parquet.row_group_target_bytes = 128;
                config.resources.parquet.file_target_bytes = 512;
                config.diagnostics.mode = DiagnosticsMode::Basic;
                let result = execute_processing_job(&job, &config).unwrap();
                assert_eq!(
                    result.status,
                    ProcessingStatus::Complete,
                    "{:?}",
                    result.failure
                );
                assert_rows(
                    &job,
                    [
                        (1000, "1", 101),
                        (1001, "1", 102),
                        (1001, "2", 103),
                        (2000, "1", 201),
                        (2002, "2", 202),
                        (3000, "1", 301),
                        (3001, "2", 302),
                    ]
                    .into_iter()
                    .map(|(t, id, p)| expected(&job, t, id, p))
                    .collect(),
                );
                let m = manifest(&job);
                assert_eq!(m.events_written, 7);
                assert_eq!(m.processing_metrics.counters.records_read, 7);
                assert_eq!(m.processing_metrics.counters.rows_suppressed, 0);
                assert_eq!(
                    m.streams[0]
                        .sources
                        .iter()
                        .map(|s| (s.task_id, s.start_row, s.end_row))
                        .collect::<Vec<_>>(),
                    vec![(owners[0], 0, 3), (owners[1], 3, 5), (owners[2], 5, 7)]
                );
                let report = diagnostic_report(root.path());
                let decision = path_report(&report);
                assert_eq!(decision["trade_execution_path"], "chronological_append");
                let resources = report["resources"].as_object().unwrap();
                let spool = resources
                    .values()
                    .find(|v| v.get("counters").is_some())
                    .unwrap();
                assert_eq!(spool["counters"]["runs_generated"], 0);
                assert_eq!(spool["counters"]["merge_passes"], 0);
                assert_eq!(spool["counters"]["frames_written"], 7);
                assert_eq!(spool["counters"]["frames_read"], 7);
                assert!(spool["accounted_memory_peak"].as_u64().unwrap() <= memory);
                assert_eq!(spool["scratch_current"], 0);
                let perf = resources
                    .values()
                    .find(|v| v.get("parquet_row_groups").is_some())
                    .unwrap();
                assert_eq!(perf["canonical_rows_written"], 7);
                assert_eq!(perf["parquet_row_groups"], 7);
                assert_eq!(report["status"], "committed");
                assert_eq!(
                    fs::read_dir(&config.resources.scratch_path)
                        .unwrap()
                        .count(),
                    0
                );
            }
        }
    }
}
#[test]
fn local_equal_partial_and_complete_overlap_and_unreliable_ids_fail_with_strict_path() {
    for (a, b) in [
        ("1,1000,200,3,buy,0\n", "1,1000,201,3,buy,0\n"),
        (
            "1,1000,200,3,buy,0\n2,3000,202,3,buy,0\n",
            "1,2000,201,3,buy,0\n2,4000,203,3,buy,0\n",
        ),
        (
            "1,1000,200,3,buy,0\n2,2000,202,3,buy,0\n",
            "1,2000,201,3,buy,0\n2,3000,203,3,buy,0\n",
        ),
    ] {
        for scope in ["source_local", "unreliable"] {
            let root = tempfile::tempdir().unwrap();
            let (mut job, mut config) = setup(false, root.path());
            for task in &mut job.tasks {
                task.raw_schema["marketforge_trade_identity"] = json!({"scope":scope});
            }
            config.diagnostics.mode = DiagnosticsMode::Basic;
            write(&job, 0, a);
            write(&job, 1, b);
            let result = execute_processing_job(&job, &config).unwrap();
            assert_failed(&job, &result);
            assert!(
                result
                    .failure
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("ambiguous trade overlap")
            );
            let report = diagnostic_report(root.path());
            assert_eq!(
                path_report(&report)["trade_execution_path"],
                "identity_reconciliation"
            );
            assert_eq!(report["status"], "incomplete_or_failed");
        }
    }
}
#[test]
fn global_ids_still_reconcile_even_across_disjoint_timestamps_and_scoped_contract_requires_domain()
{
    for contract in [
        json!({"scope":"global"}),
        json!({"scope":"instrument_scoped","reset_domain":"instrument_lifetime"}),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (mut job, mut config) = setup(false, root.path());
        for task in &mut job.tasks {
            task.raw_schema["marketforge_trade_identity"] = contract.clone();
        }
        config.diagnostics.mode = DiagnosticsMode::Basic;
        write(&job, 0, "1,3000,200,3,buy,0\n");
        write(&job, 1, "1,1000,199,3,buy,0\n");
        let result = execute_processing_job(&job, &config).unwrap();
        assert_eq!(
            result.status,
            ProcessingStatus::Complete,
            "{:?}",
            result.failure
        );
        assert_rows(&job, vec![expected(&job, 3000, "1", 200)]);
        assert_eq!(
            manifest(&job).processing_metrics.counters.rows_suppressed,
            1
        );
        assert_eq!(
            path_report(&diagnostic_report(root.path()))["trade_execution_path"],
            "identity_reconciliation"
        );
    }
}
#[test]
fn degraded_source_regression_cannot_be_repaired_by_chronological_append() {
    let root = tempfile::tempdir().unwrap();
    let (job, mut config) = setup(false, root.path());
    config.integrity_policy.timestamp_regression.action = IntegrityAction::Degrade;
    write(&job, 0, "1,2000,201,3,buy,0\n2,1000,200,3,buy,0\n");
    write(&job, 1, "1,3000,202,3,buy,0\n");
    let result = execute_processing_job(&job, &config).unwrap();
    assert_failed(&job, &result);
    assert!(
        result
            .failure
            .as_ref()
            .unwrap()
            .message
            .contains("regression")
    );
}
#[test]
fn source_failure_and_memory_exhaustion_do_not_commit_or_leak_scratch() {
    for memory_failure in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let (job, mut config) = setup(false, root.path());
        write(&job, 0, "1,1000,200,3,buy,0\n");
        if !memory_failure {
            fs::write(&job.tasks[1].input_path, "wrong,header\n1,2\n").unwrap();
        }
        if memory_failure {
            write(&job, 1, "1,2000,201,3,buy,0\n");
            config.resources.normalization_limits.reconciliation_bytes = 128;
        } // cannot admit even one codec
        let result = execute_processing_job(&job, &config).unwrap();
        assert_failed(&job, &result);
        if !memory_failure {
            let failure: Value = serde_json::from_slice(
                &fs::read(result.failure_report_path.as_ref().unwrap()).unwrap(),
            )
            .unwrap();
            assert_eq!(
                failure["processing_metrics"]["counters"]["events_normalized"],
                1
            );
            assert_eq!(
                failure["processing_metrics"]["counters"]["rows_unpublished"],
                1
            );
        }
        if config.resources.scratch_path.exists() {
            assert_eq!(
                fs::read_dir(&config.resources.scratch_path)
                    .unwrap()
                    .count(),
                0
            );
        }
    }
}
#[test]
fn rejected_rows_obey_policy_and_do_not_disable_source_local_append() {
    let root = tempfile::tempdir().unwrap();
    let (job, mut config) = setup(false, root.path());
    config.integrity_policy.enabled = true;
    config.integrity_policy.invalid_record = crate::shared::historical::IntegrityRule {
        action: IntegrityAction::Degrade,
        max_count: Some(1),
        max_rate: None,
        minimum_samples: 0,
        windows: Default::default(),
    };
    write(&job, 0, "1,1000,200,3,buy,0\n2,1500,-1,3,buy,0\n");
    write(&job, 1, "1,2000,201,3,buy,0\n");
    let result = execute_processing_job(&job, &config).unwrap();
    assert_eq!(
        result.status,
        ProcessingStatus::Complete,
        "{:?}",
        result.failure
    );
    let m = manifest(&job);
    assert_eq!(m.processing_metrics.counters.records_rejected, 1);
    assert_eq!(m.events_written, 2);
    assert_rows(
        &job,
        vec![
            expected(&job, 1000, "1", 200),
            expected(&job, 2000, "1", 201),
        ],
    );
}

#[test]
fn missing_ids_overlap_and_local_uuid_ids_are_not_promoted_by_shape() {
    for (id, scope) in [
        ("", "global"),
        ("00000000-0000-0000-0000-000000000001", "source_local"),
    ] {
        let root = tempfile::tempdir().unwrap();
        let (mut job, config) = setup(false, root.path());
        for task in &mut job.tasks {
            task.raw_schema["marketforge_trade_identity"] = json!({"scope":scope});
        }
        write(&job, 0, &format!("{id},1000,200,3,buy,0\n"));
        write(&job, 1, &format!("{id},1000,201,3,buy,0\n"));
        let result = execute_processing_job(&job, &config).unwrap();
        assert_failed(&job, &result);
        assert!(
            result
                .failure
                .as_ref()
                .unwrap()
                .message
                .contains("ambiguous trade overlap")
        );
    }
}
#[test]
fn final_integrity_rejection_is_not_bypassed_and_empty_accepted_sources_validate() {
    for permitted in [0, 1] {
        let root = tempfile::tempdir().unwrap();
        let (job, mut config) = setup(false, root.path());
        config.integrity_policy.enabled = true;
        config.integrity_policy.invalid_record = crate::shared::historical::IntegrityRule {
            action: IntegrityAction::Degrade,
            max_count: Some(permitted),
            max_rate: None,
            minimum_samples: 0,
            windows: Default::default(),
        };
        write(&job, 0, "1,1000,-1,3,buy,0\n");
        write(&job, 1, "1,2000,201,3,buy,0\n");
        let result = execute_processing_job(&job, &config).unwrap();
        if permitted == 0 {
            assert_failed(&job, &result);
            assert!(
                result
                    .failure
                    .as_ref()
                    .unwrap()
                    .message
                    .contains("final integrity policy failed")
            );
        } else {
            assert_eq!(
                result.status,
                ProcessingStatus::Complete,
                "{:?}",
                result.failure
            );
            assert_rows(&job, vec![expected(&job, 2000, "1", 201)]);
            let m = manifest(&job);
            assert_eq!(m.processing_metrics.counters.records_rejected, 1);
            assert_eq!(m.processing_metrics.counters.records_read, 2);
            assert_eq!(
                m.streams[0]
                    .sources
                    .iter()
                    .map(|s| s.row_count().unwrap())
                    .sum::<u64>(),
                1
            );
        }
    }
}
#[test]
fn chronological_append_writer_failure_retains_all_unsuppressed_rows_and_cleans_scratch() {
    let root = tempfile::tempdir().unwrap();
    let (job, config) = setup(false, root.path());
    write(&job, 0, "1,1000,200,3,buy,0\n");
    write(&job, 1, "1,2000,201,3,buy,0\n");
    fs::create_dir_all(&job.output.staging_path).unwrap();
    fs::write(job.output.staging_path.join("streams"), b"writer failure").unwrap();
    let units = super::super::streams::plan(&job).unwrap();
    let executions =
        super::super::streams::execute(&units[0], &config, &progress::ProgressReporter::disabled())
            .unwrap();
    let mut context = super::super::executor::ExecutionContext::default();
    assert!(
        super::super::executor::finalize_processing_job(
            &job,
            &config,
            executions.into_iter().map(|(_, e)| e).collect(),
            &mut context
        )
        .is_err()
    );
    assert_eq!(context.report.counters.events_normalized, 2);
    assert_eq!(context.report.counters.rows_suppressed, 0);
    assert_eq!(context.report.counters.rows_unpublished, 2);
    assert_eq!(context.report.counters.events_written, 0);
    assert_eq!(
        fs::read_dir(&config.resources.scratch_path)
            .unwrap()
            .count(),
        0
    );
    assert!(!job.output.dataset_path.exists());
}
#[test]
fn append_cursor_preserves_physical_ordinals_and_validates_completion_before_success() {
    use super::{Quota, external::SortedRows, spool::Workspace};
    let root = tempfile::tempdir().unwrap();
    let w = Workspace::new(root.path(), 1 << 20, Quota::new(2 << 20)).unwrap();
    let mut inputs = Vec::new();
    for source in 0..2 {
        let spool = w.create().unwrap();
        let mut writer = spool.writer().unwrap();
        for ordinal in [2, 7] {
            let mut entry = super::tests::event(
                source,
                source as u64,
                100 + source as i64 * 100,
                Some("1"),
                1,
            );
            entry.ordinal = ordinal;
            writer.push_trade(&entry).unwrap();
        }
        writer.finish().unwrap();
        inputs.push((source, spool));
    }
    let mut rows = SortedRows::Append(inputs).rows().unwrap();
    let mut actual = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        actual.push((row.value.source, row.value.ordinal));
    }
    rows.finish().unwrap();
    assert_eq!(actual, [(0, 2), (0, 7), (1, 2), (1, 7)]);
    assert_eq!(w.peaks().2, 1);
    assert_eq!(w.quota().used(), 0);
    let spool = w.create().unwrap();
    let mut writer = spool.writer().unwrap();
    let e = super::tests::event(0, 0, 1, Some("1"), 1);
    writer.push_trade(&e).unwrap();
    drop(writer); // deliberately no completion marker
    let mut rows = SortedRows::Append(vec![(0, spool)]).rows().unwrap();
    let result = (|| -> crate::shared::error::Result<()> {
        while rows.next()?.is_some() {}
        Ok(())
    })();
    assert!(result.is_err());
    assert!(rows.finish().is_err());
}

#[test]
fn contradictory_source_contracts_cannot_split_a_compatible_stream_to_bypass_reconciliation() {
    let root = tempfile::tempdir().unwrap();
    let (mut job, config) = setup(false, root.path());
    job.tasks[1].raw_schema["marketforge_trade_identity"] = json!({"scope":"global"});
    write(&job, 0, "1,1000,200,3,buy,0\n");
    write(&job, 1, "1,1000,201,3,buy,0\n");
    let result = execute_processing_job(&job, &config).unwrap();
    assert_failed(&job, &result);
    assert!(
        result
            .failure
            .as_ref()
            .unwrap()
            .message
            .contains("inconsistent native trade identity contracts")
    );
}
