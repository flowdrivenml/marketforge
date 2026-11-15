//! Synthetic admission/component checks. Never invoke normalization execution.
use super::*;
use crate::{
    historical::{
        job::*,
        normalize::{metrics::*, task::TaskExecution},
    },
    shared::{
        domain::{
            book::{BookLevel, BookStore},
            canonical::BookSide,
        },
        error::MarketForgeError,
        formats::depth::{DepthSnapshotAssembler, ExtractedLevel},
        historical::limits::*,
    },
};
use rust_decimal::Decimal;
use serde_json::json;
use std::{io::Write, sync::Arc};
fn task(path: std::path::PathBuf) -> WorkTask {
    serde_json::from_value(json!({
        "task_id":1,"stream_id":"synthetic","input_path":path,"format_code":"synthetic",
        "exchange":"bybit","instrument_id":1,"symbol":"TEST",
        "raw_schema":{"fields":[{"name":"timestamp","position":0}]},
        "instrument":{"instrument_kind":"spot","tick_size":"1"},
        "normalizations":[{"timestamp_encoding":"seconds","quantity_encoding":"base","target_schema":"depth","rules":{}}],
        "source_ordering":"source_ordered","source_compression":"plain","archive_member":null
    })).unwrap()
}
#[test]
fn streaming_size_and_classification_are_shared() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    let mut task = task(file.path().to_path_buf());
    let parquet = ParquetResourceConfig {
        row_group_target_bytes: 1024,
        file_target_bytes: 4096,
    };
    let first = estimate_task_resources(&task, &parquet).unwrap();
    file.write_all(&[0; 128]).unwrap();
    assert_eq!(first, estimate_task_resources(&task, &parquet).unwrap());
    assert!(first.open_files > 0);
    for value in [json!(0), json!("1"), json!(-1), json!(null)] {
        task.raw_schema["workbook_sheets"] = value;
        assert!(source_format(&task).is_err());
        assert!(estimate_task_resources(&task, &parquet).is_err());
    }
    task.raw_schema["workbook_sheets"] = json!(1);
    assert_eq!(source_format(&task).unwrap(), SourceFormat::Xlsx);
    assert_eq!(
        estimate_breakdown(&task, &parquet, &Default::default())
            .unwrap()
            .format,
        SourceFormat::Xlsx
    );
}
#[test]
fn arithmetic_and_configuration_fail_explicitly() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let task = task(file.path().to_path_buf());
    let parquet = ParquetResourceConfig {
        row_group_target_bytes: u64::MAX,
        file_target_bytes: u64::MAX,
    };
    assert!(estimate_task_resources(&task, &parquet).is_err());
    let mut limits = NormalizationLimits::default();
    limits.record_bytes = 0;
    assert!(limits.validate().is_err());
    limits.record_bytes = u64::MAX;
    assert!(limits.validate().is_err());
    let old = json!({"workers":1,"memory_budget_bytes":1024,"scratch_path":"unused","scratch_budget_bytes":1,"parquet":{"row_group_target_bytes":1,"file_target_bytes":1}});
    let resources: ResourceConfig = serde_json::from_value(old).unwrap();
    assert_eq!(resources.normalization_limits, Default::default());
    assert!(
        serde_json::to_value(resources).unwrap()["normalization_limits"]["record_bytes"]
            .as_u64()
            .is_some()
    );
}
#[test]
fn depth_growth_fails_before_mutation() {
    let mut limits = NormalizationLimits::default();
    limits.depth_levels = 2;
    let _scope = LimitScope::enter(limits, "task=7, source=synthetic".into());
    let level = BookLevel {
        quantity_base: Some(Decimal::ONE),
        quantity_quote: None,
        quantity_contracts: None,
        order_count: None,
    };
    let mut book = BookStore::new();
    book.apply_snapshot(
        vec![(Decimal::ONE, level.clone())],
        vec![(Decimal::from(2), level.clone())],
    )
    .unwrap();
    let error = book
        .apply_batch(vec![(BookSide::Bid, Decimal::from(3), level)])
        .unwrap_err();
    assert!(
        matches!(error, MarketForgeError::ResourceLimit(ref e) if e.stage == "atomic book update" && e.scope.contains("task=7"))
    );
    assert_eq!(book.bids().len() + book.asks().len(), 2);
    let mut assembler = DepthSnapshotAssembler::new();
    let level = ExtractedLevel {
        price: Decimal::ONE,
        quantity: Decimal::ONE,
        order_count: None,
    };
    assembler
        .push(1, None, BookSide::Bid, level.clone())
        .unwrap();
    assembler
        .push(1, None, BookSide::Ask, level.clone())
        .unwrap();
    assert!(matches!(
        assembler.push(1, None, BookSide::Bid, level),
        Err(MarketForgeError::ResourceLimit(_))
    ));
    let snapshot = assembler.finish().unwrap().unwrap();
    assert_eq!(snapshot.bids.len() + snapshot.asks.len(), 2);
}
#[test]
fn retained_results_and_file_capacity_have_owned_lifetimes() {
    let budget = Arc::new(ResourceBudget::with_file_limit(100, 1, 3).unwrap());
    let retained = budget
        .acquire_owned(ResourceRequirements {
            memory_bytes: 20,
            scratch_bytes: 0,
            open_files: 1,
        })
        .unwrap();
    {
        let _execution = budget
            .acquire(ResourceRequirements {
                memory_bytes: 80,
                scratch_bytes: 0,
                open_files: 2,
            })
            .unwrap();
        assert_eq!(budget.usage().unwrap().open_files, 3);
        assert!(matches!(
            budget.acquire(ResourceRequirements {
                memory_bytes: 0,
                scratch_bytes: 0,
                open_files: 4
            }),
            Err(MarketForgeError::ResourceLimit(_))
        ));
        // Mock writer finalization is inside the execution reservation.
        assert_eq!(budget.usage().unwrap().memory_bytes, 100);
    }
    assert_eq!(budget.usage().unwrap().memory_bytes, 20);
    drop(retained);
    assert_eq!(budget.usage().unwrap().memory_bytes, 0);
}
#[test]
fn bounded_metrics_preserve_existing_entries() {
    let mut limits = NormalizationLimits::default();
    limits.metric_entries = 3;
    let _scope = LimitScope::enter(limits, "synthetic".into());
    let scope = IntegrityScope {
        task_id: 1,
        stream_id: "s".into(),
        source_file: "f".into(),
    };
    let mut metrics = ScopedIntegrityMetrics::default();
    metrics
        .record(&scope, IntegrityCategory::InvalidRecord, false, Some(0))
        .unwrap();
    let original = metrics.clone();
    assert!(matches!(
        metrics.record(
            &scope,
            IntegrityCategory::InvalidRecord,
            false,
            Some(HOUR_NS)
        ),
        Err(MarketForgeError::ResourceLimit(_))
    ));
    assert_eq!(original, metrics);
}
#[test]
fn fatal_mock_source_stops_submission_without_losing_previous_metrics() {
    let mut submitted = Vec::new();
    let mut executions = Vec::new();
    for index in 0..3 {
        if crate::historical::normalize::streams::stopped(&executions) {
            continue;
        }
        submitted.push(index);
        let mut metrics = TaskMetrics::default();
        metrics.counters.records_read = 1;
        executions.push((
            index,
            TaskExecution {
                task_id: index as u64,
                output_paths: vec![],
                metrics,
                error: if index == 1 {
                    Some(error("memory", 1, 2, "mock source").into())
                } else {
                    None
                },
            },
        ));
    }
    assert_eq!(submitted, [0, 1]);
    assert_eq!(executions[0].1.metrics.counters.records_read, 1);
    assert!(executions[0].1.error.is_none());
    assert!(matches!(
        executions[1].1.error,
        Some(MarketForgeError::ResourceLimit(_))
    ));
}

#[test]
fn mocked_budgeted_work_preserves_other_results_on_impossible_admission() {
    let budget = ResourceBudget::with_file_limit(8, 1, 2).unwrap();
    let results = crate::historical::normalize::scheduler::execute_parallel_budgeted(
        vec![1u64, 9, 2],
        2,
        &budget,
        |value| {
            Ok(ResourceRequirements {
                memory_bytes: *value,
                scratch_bytes: 0,
                open_files: 1,
            })
        },
        Ok,
    )
    .unwrap();
    assert_eq!(*results[0].as_ref().unwrap(), 1);
    assert!(matches!(
        results[1],
        Err(MarketForgeError::ResourceLimit(_))
    ));
    assert_eq!(*results[2].as_ref().unwrap(), 2);
    assert_eq!(budget.usage().unwrap().open_files, 0);
}
#[test]
fn sequential_sources_share_writer_memory_in_stream_estimate() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let task = task(file.path().into());
    let mut wire: toml::Value = toml::from_str(include_str!(
        "../../../shared/historical/config/fixtures/runtime.toml"
    ))
    .unwrap();
    wire.as_table_mut().unwrap().remove("schema_version");
    let config: crate::historical::normalize::ProcessingConfig = wire.try_into().unwrap();
    // Construct a job solely for estimates; no source reader or writer is opened.
    let job: ProcessingJob = serde_json::from_value(json!({
        "protocol_version":1,"job_id":1,"dataset_id":1,"operation":"process","tasks":[task],
        "streams":[],"ordering":{"primary":"event_timestamp_ns","tie_break":"stream_rank"},
        "integrity_policy":config.integrity_policy,"resources":config.resources,
        "output":{"dataset_path":"unused-final","staging_path":"unused-stage","content_type":"depth"}
    })).unwrap();
    let first = crate::historical::normalize::streams::StreamUnit {
        job: &job,
        tasks: vec![(0, &job.tasks[0])],
        key: "synthetic".into(),
    };
    let second = crate::historical::normalize::streams::StreamUnit {
        job: &job,
        tasks: vec![(0, &job.tasks[0]), (1, &job.tasks[0])],
        key: "synthetic".into(),
    };
    let a = crate::historical::normalize::streams::estimate(&first, &config).unwrap();
    let b = crate::historical::normalize::streams::estimate(&second, &config).unwrap();
    assert_eq!(a.open_files, 5);
    assert_eq!(b.open_files, config.merge_fan_in as u64 + 6);
    assert_eq!(
        b.memory_bytes - a.memory_bytes,
        (std::mem::size_of::<crate::historical::normalize::streams::SourceSpan>()
            + std::mem::size_of::<(usize, TaskExecution)>()) as u64
            + config.resources.normalization_limits.reconciliation_bytes
    );
}

#[test]
fn finite_file_capacity_rejects_concurrent_growth_without_waiting() {
    use std::{sync::mpsc, time::Duration};
    let budget = Arc::new(ResourceBudget::with_file_limit(100, 1, 1).unwrap());
    let requirements = ResourceRequirements {
        memory_bytes: 1,
        scratch_bytes: 0,
        open_files: 1,
    };
    let first = budget.acquire(requirements).unwrap();
    let (send, receive) = mpsc::channel();
    let other = budget.clone();
    let handle = std::thread::spawn(move || {
        send.send(other.try_acquire(requirements).unwrap().is_none())
            .unwrap();
    });
    assert!(
        receive
            .recv_timeout(Duration::from_secs(2))
            .expect("nonblocking capacity check must return")
    );
    handle.join().unwrap();
    assert_eq!(budget.usage().unwrap().open_files, 1);
    drop(first);
    assert!(budget.try_acquire(requirements).unwrap().is_some());
}
#[test]
fn mocked_finalization_preserves_resource_error_metrics_and_skipped_cardinality() {
    use crate::historical::normalize::executor::{ExecutionContext, finalize_processing_job};
    let file = tempfile::NamedTempFile::new().unwrap();
    let first = task(file.path().into());
    let mut second = first.clone();
    second.task_id = TaskId(2);
    let mut third = first.clone();
    third.task_id = TaskId(3);
    let mut wire: toml::Value = toml::from_str(include_str!(
        "../../../shared/historical/config/fixtures/runtime.toml"
    ))
    .unwrap();
    wire.as_table_mut().unwrap().remove("schema_version");
    let config: crate::historical::normalize::ProcessingConfig = wire.try_into().unwrap();
    let job: ProcessingJob = serde_json::from_value(json!({
        "protocol_version":1,"job_id":1,"dataset_id":1,"operation":"process","tasks":[first,second,third],
        "streams":[],"ordering":{"primary":"event_timestamp_ns","tie_break":"stream_rank"},
        "integrity_policy":config.integrity_policy,"resources":config.resources,
        "output":{"dataset_path":"unused-final","staging_path":"unused-stage","content_type":"depth"}
    })).unwrap();
    let mut executions = Vec::new();
    for (index, task) in job.tasks.iter().enumerate() {
        let mut metrics = TaskMetrics::default();
        if index < 2 {
            metrics.counters.records_read = 1;
        }
        let error = match index {
            0 => None,
            1 => Some(error("book levels", 2, 3, "mock source").into()),
            _ => Some(MarketForgeError::InvalidConfiguration(
                "source not attempted: preceding stream failure".into(),
            )),
        };
        executions.push(TaskExecution {
            task_id: task.task_id.0,
            output_paths: vec![],
            metrics,
            error,
        });
    }
    let mut context = ExecutionContext::default();
    let result = finalize_processing_job(&job, &config, executions, &mut context);
    assert!(
        matches!(result, Err(MarketForgeError::ResourceLimit(ref e)) if e.stage == "mock source")
    );
    assert_eq!(context.failed_task_id, Some(2));
    assert_eq!(context.report.tasks.len(), 3);
    assert_eq!(context.report.counters.records_read, 2);
    assert_eq!(context.report.tasks[2].counters.records_read, 0);
    // An honestly stream-scoped writer error must not invent a responsible task.
    let executions = job
        .tasks
        .iter()
        .enumerate()
        .map(|(i, task)| TaskExecution {
            task_id: task.task_id.0,
            output_paths: vec![],
            metrics: Default::default(),
            error: if i == 2 {
                Some(MarketForgeError::StreamFailure {
                    stream: "synthetic".into(),
                    stage: "writer finalization",
                    source: Box::new(error("memory", 1, 2, "footer").into()),
                })
            } else {
                None
            },
        })
        .collect();
    let mut context = ExecutionContext::default();
    assert!(matches!(
        finalize_processing_job(&job, &config, executions, &mut context),
        Err(MarketForgeError::StreamFailure { .. })
    ));
    assert_eq!(context.failed_task_id, None);
}

#[test]
fn retained_admission_rejects_before_starting_work() {
    let file = tempfile::NamedTempFile::new().unwrap();
    let task = task(file.path().into());
    let mut wire: toml::Value = toml::from_str(include_str!(
        "../../../shared/historical/config/fixtures/runtime.toml"
    ))
    .unwrap();
    wire.as_table_mut().unwrap().remove("schema_version");
    let mut config: crate::historical::normalize::ProcessingConfig = wire.try_into().unwrap();
    let execution = estimate_task_resources_with_limits(
        &task,
        &config.resources.parquet,
        &config.resources.normalization_limits,
    )
    .unwrap()
    .memory_bytes;
    let retained = retained_task_memory(&task, &config.resources.normalization_limits).unwrap();
    config.resources.memory_budget_bytes = execution + retained - 1;
    assert!(execution < config.resources.memory_budget_bytes);
    // Metadata inspection/admission only: no execution callback or writer exists.
    assert!(
        matches!(reserve_isolated(&task, &config), Err(MarketForgeError::ResourceLimit(ref e)) if e.resource == "memory bytes" && e.requested == execution + retained)
    );
}
#[test]
fn scheduler_holds_stream_capacity_during_mock_finalization() {
    let budget = ResourceBudget::with_file_limit(100, 1, 2).unwrap();
    let requirements = ResourceRequirements {
        memory_bytes: 70,
        scratch_bytes: 0,
        open_files: 2,
    };
    let result = crate::historical::normalize::scheduler::execute_parallel_budgeted(
        vec![()],
        1,
        &budget,
        |_| Ok(requirements),
        |_| {
            // Two sequential mock sources use the same writer reservation.
            for _ in 0..2 {
                assert_eq!(budget.usage().unwrap().memory_bytes, 70);
            }
            // Mock writer finish and cleanup still own both handles.
            assert_eq!(budget.usage().unwrap().open_files, 2);
            Ok(())
        },
    )
    .unwrap();
    assert!(result[0].is_ok());
    assert_eq!(budget.usage().unwrap().memory_bytes, 0);
    assert_eq!(budget.usage().unwrap().open_files, 0);
}
