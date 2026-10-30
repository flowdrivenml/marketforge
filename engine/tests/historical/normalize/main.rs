//! Grouped Historical normalize tests. Execute using the bounded Historical verifier.
mod configuration;
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
mod equivalence;
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
#[path = "execution/process_task.rs"]
mod process_task;
#[path = "execution/processing_config.rs"]
mod processing_config;
#[path = "execution/processing_metrics_aggregation.rs"]
mod processing_metrics_aggregation;
#[path = "resources/resource_file_budget.rs"]
mod resource_file_budget;
#[path = "trades/trade_formats.rs"]
mod trade_formats;
#[path = "trades/trade_real_archives.rs"]
mod trade_real_archives;

#[path = "trades/trade_worker_metrics_bounded.rs"]
mod trade_worker_metrics;

#[path = "trades/trade_integrity_enforcement.rs"]
mod trade_integrity_enforcement;
