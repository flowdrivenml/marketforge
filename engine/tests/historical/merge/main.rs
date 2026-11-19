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

#[path = "core/core_regression.rs"]
mod core_regression;

#[path = "support/job.rs"]
mod merge_test_job;

#[path = "depth/merge_checkpoint.rs"]
mod merge_checkpoint;
#[path = "combined/merge_combined_real.rs"]
mod merge_combined_real;
#[path = "depth/merge_depth_cursor.rs"]
mod merge_depth_cursor;
#[path = "depth/merge_depth_cursor_range.rs"]
mod merge_depth_cursor_range;
#[path = "depth/merge_depth_stream.rs"]
mod merge_depth_stream;
#[path = "depth/merge_event_validation.rs"]
mod merge_event_validation;
#[path = "execution/merge_executor.rs"]
mod merge_executor;
#[path = "depth/merge_indexed_cursor.rs"]
mod merge_indexed_cursor;
#[path = "depth/merge_inventory.rs"]
mod merge_inventory;
#[path = "execution/merge_manifest_builder.rs"]
mod merge_manifest_builder;
#[path = "execution/merge_output_validation.rs"]
mod merge_output_validation;
#[path = "execution/merge_parallel.rs"]
mod merge_parallel;
#[path = "execution/merge_parallel_commit.rs"]
mod merge_parallel_commit;
#[path = "partition/merge_partition_coverage.rs"]
mod merge_partition_coverage;
#[path = "partition/merge_partition_equivalence.rs"]
mod merge_partition_equivalence;
#[path = "partition/merge_partition_executor.rs"]
mod merge_partition_executor;
#[path = "partition/merge_partition_input.rs"]
mod merge_partition_input;
#[path = "execution/merge_resolver.rs"]
mod merge_resolver;
#[path = "trades/merge_trade_cursor.rs"]
mod merge_trade_cursor;
#[path = "trades/merge_trade_cursor_indexed.rs"]
mod merge_trade_cursor_indexed;
#[path = "trades/merge_trade_kway_real.rs"]
mod merge_trade_kway_real;
#[path = "trades/merge_trade_stream.rs"]
mod merge_trade_stream;
#[path = "execution/merge_unified_commit.rs"]
mod merge_unified_commit;
#[path = "combined/merge_unified_timeline.rs"]
mod merge_unified_depth_timeline;
#[path = "execution/merge_unified_manifest_builder.rs"]
mod merge_unified_manifest_builder;
#[path = "execution/merge_unified_output_validation.rs"]
mod merge_unified_output_validation;
#[path = "partition/merge_unified_parallel_equivalence.rs"]
mod merge_unified_parallel_equivalence;
#[path = "partition/merge_unified_partition_executor.rs"]
mod merge_unified_partition_executor;
#[path = "partition/merge_unified_partition_input.rs"]
mod merge_unified_partition_input;
#[path = "output/merge_unified_production_rotation.rs"]
mod merge_unified_production_rotation;
#[path = "execution/merge_unified_reference_integrity.rs"]
mod merge_unified_reference_integrity;
#[path = "execution/merge_unified_stream_resolver.rs"]
mod merge_unified_stream_resolver;
#[path = "combined/merge_unified_writer.rs"]
mod merge_unified_writer;

#[path = "execution/bounded_recorded.rs"]
mod bounded_recorded;

#[path = "core/merge_kway.rs"]
mod merge_kway;

#[path = "trades/merge_trade_stream_bounded.rs"]
mod merge_trade_stream_bounded;

#[path = "execution/merge_unified_failure.rs"]
mod merge_unified_failure;

#[path = "support/datasets.rs"]
mod recorded_datasets;

#[path = "support/depth_index.rs"]
mod depth_index;

#[path = "support/publication.rs"]
mod publication;

#[path = "support/recorded_references.rs"]
mod recorded_references;

#[path = "benchmarks/merge_cursor_benchmark.rs"]
mod merge_cursor_benchmark;
