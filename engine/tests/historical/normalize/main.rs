#[path = "../support/mod.rs"]
mod support;
// Grouped Historical normalize tests. Execute using the bounded Historical verifier.

#[path = "dataset_manifest.rs"]
mod dataset_manifest;
#[path = "depth/depth_boundary.rs"]
mod depth_boundary;
#[path = "depth/depth_boundary_recovery.rs"]
mod depth_boundary_recovery;
#[path = "depth/depth_csv_pipeline.rs"]
mod depth_csv_pipeline;
#[path = "depth/depth_sequence_ranges.rs"]
mod depth_sequence_ranges;
#[path = "depth/depth_sink.rs"]
mod depth_sink;
#[path = "depth/depth_xlsx_pipeline.rs"]
mod depth_xlsx_pipeline;

#[path = "resources/integrity_policy.rs"]
mod integrity_policy;
#[path = "execution/process_failure.rs"]
mod process_failure;
#[path = "execution/process_metrics.rs"]
mod process_metrics;
#[path = "execution/process_scheduler.rs"]
mod process_scheduler;
#[path = "execution/process_session_validation.rs"]
mod process_session_validation;

#[path = "execution/processing_config.rs"]
mod processing_config;
#[path = "execution/processing_metrics_aggregation.rs"]
mod processing_metrics_aggregation;
#[path = "resources/resource_file_budget.rs"]
mod resource_file_budget;

#[path = "trades/trade_integrity_enforcement.rs"]
mod trade_integrity_enforcement;

#[path = "depth/depth_pipeline_recovery.rs"]
mod depth_pipeline_recovery;

mod exchanges;
mod profiles;

#[cfg(feature = "historical-full-tests")]
#[path = "depth/depth_archive_continuity.rs"]
mod depth_archive_continuity;

#[cfg(feature = "historical-full-tests")]
#[path = "depth/depth_parquet_replay.rs"]
mod depth_parquet_replay;

#[cfg(feature = "historical-full-tests")]
#[path = "depth/depth_real_archive.rs"]
mod depth_real_archive;

#[cfg(feature = "historical-full-tests")]
#[path = "parquet/parquet_depth_real_archive.rs"]
mod parquet_depth_real_archive;

#[cfg(feature = "historical-full-tests")]
#[path = "parquet/parquet_discovery.rs"]
mod parquet_discovery;

#[cfg(feature = "historical-full-tests")]
#[path = "execution/process_executor.rs"]
mod process_executor;

#[cfg(feature = "historical-full-tests")]
#[path = "execution/process_executor_failure.rs"]
mod process_executor_failure;

#[cfg(feature = "historical-full-tests")]
#[path = "execution/process_parallel_consistency.rs"]
mod process_parallel_consistency;

#[cfg(feature = "historical-full-tests")]
#[path = "execution/process_session.rs"]
mod process_session;

#[cfg(feature = "historical-full-tests")]
#[path = "execution/process_session_finalization.rs"]
mod process_session_finalization;

#[cfg(feature = "historical-full-tests")]
#[path = "execution/process_task.rs"]
mod process_task;

#[cfg(feature = "historical-full-tests")]
#[path = "trades/trade_all_archives.rs"]
mod trade_all_archives;

#[cfg(feature = "historical-full-tests")]
#[path = "trades/trade_worker.rs"]
mod trade_worker;

#[cfg(feature = "historical-full-tests")]
#[path = "trades/trade_worker_metrics.rs"]
mod trade_worker_metrics;

#[cfg(feature = "historical-real-tests")]
#[path = "trades/trade_real_archives.rs"]
mod trade_real_archives;

mod configuration;
mod equivalence;
#[path = "trades/trade_worker_metrics_bounded.rs"]
mod trade_worker_metrics_bounded;

mod cli_contract;
