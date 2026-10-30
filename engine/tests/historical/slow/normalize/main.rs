//! Grouped Historical normalize tests. Opt-in only; do not execute during ordinary verification.
#[path = "../../normalize/depth/depth_archive_continuity.rs"]
mod depth_archive_continuity;
#[path = "../../normalize/depth/depth_formats.rs"]
mod depth_formats;
#[path = "../../normalize/depth/depth_parquet_replay.rs"]
mod depth_parquet_replay;
#[path = "../../normalize/depth/depth_pipeline_recovery.rs"]
mod depth_pipeline_recovery;
#[path = "../../normalize/depth/depth_real_archive.rs"]
mod depth_real_archive;
#[path = "../../normalize/parquet/parquet_depth_real_archive.rs"]
mod parquet_depth_real_archive;
#[path = "../../normalize/parquet/parquet_discovery.rs"]
mod parquet_discovery;
#[path = "../../normalize/execution/process_executor.rs"]
mod process_executor;
#[path = "../../normalize/execution/process_executor_failure.rs"]
mod process_executor_failure;
#[path = "../../normalize/execution/process_parallel_consistency.rs"]
mod process_parallel_consistency;
#[path = "../../normalize/execution/process_session.rs"]
mod process_session;
#[path = "../../normalize/execution/process_session_finalization.rs"]
mod process_session_finalization;
#[path = "../../normalize/trades/trade_all_archives.rs"]
mod trade_all_archives;
#[path = "../../normalize/trades/trade_worker.rs"]
mod trade_worker;
#[path = "../../normalize/trades/trade_worker_metrics.rs"]
mod trade_worker_metrics;
