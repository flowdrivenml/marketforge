//! historical_merge: bounded per-case execution.
#[path = "materialize/cache.rs"]
mod materialize_cache;
#[path = "materialize/cached_reader.rs"]
mod materialize_cached_reader;
#[path = "materialize/checkpoints.rs"]
mod materialize_checkpoints;
#[path = "materialize/combined_builder.rs"]
mod materialize_combined_builder;
#[path = "materialize/depth_builder.rs"]
mod materialize_depth_builder;
#[path = "materialize/depth_metadata.rs"]
mod materialize_depth_metadata;
#[path = "materialize/manifest.rs"]
mod materialize_manifest;
#[path = "materialize/partition.rs"]
mod materialize_partition;
#[path = "materialize/planner.rs"]
mod materialize_planner;
#[path = "materialize/publish.rs"]
mod materialize_publish;
#[path = "materialize/reader.rs"]
mod materialize_reader;
#[path = "materialize/reconstruction.rs"]
mod materialize_reconstruction;
#[path = "materialize/resolver.rs"]
mod materialize_resolver;
#[path = "materialize/resources.rs"]
mod materialize_resources;
#[path = "materialize/row_groups.rs"]
mod materialize_row_groups;
#[path = "materialize/timeline_reader.rs"]
mod materialize_timeline_reader;
#[path = "materialize/trade_builder.rs"]
mod materialize_trade_builder;
#[path = "materialize/window.rs"]
mod materialize_window;
#[path = "materialize/writer.rs"]
mod materialize_writer;
#[path = "depth/boundary_summary.rs"]
mod merge_boundary_summary;
#[path = "execution/merge_commit.rs"]
mod merge_commit;
#[path = "core/merge_job.rs"]
mod merge_job;
#[path = "core/merge_kway.rs"]
mod merge_kway;
#[path = "core/merge_kway_event.rs"]
mod merge_kway_event;
#[path = "execution/merge_resource_estimation.rs"]
mod merge_resource_estimation;
#[path = "execution/merge_timeline_writer.rs"]
mod merge_timeline_writer;
#[path = "partition/merge_unified_boundary_stress.rs"]
mod merge_unified_boundary_stress;
#[path = "output/merge_unified_file_rotation.rs"]
mod merge_unified_file_rotation;
#[path = "execution/merge_unified_manifest.rs"]
mod merge_unified_manifest;

#[path = "combined/merge_unified_timeline_bounded.rs"]
mod merge_unified_timeline;

#[path = "trades/merge_trade_stream_bounded.rs"]
mod merge_trade_stream;

#[path = "execution/merge_unified_failure.rs"]
mod merge_unified_failure;

#[path = "core/core_regression.rs"]
mod core_regression;
