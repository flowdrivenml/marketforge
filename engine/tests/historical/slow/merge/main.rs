//! historical_slow_merge: opt-in compilation only during this milestone.
#[path = "../../merge/depth/merge_checkpoint.rs"]
mod merge_checkpoint;
#[path = "../../merge/combined/merge_combined_real.rs"]
mod merge_combined_real;
#[path = "../../merge/benchmarks/merge_cursor_benchmark.rs"]
mod merge_cursor_benchmark;
#[path = "../../merge/depth/merge_depth_cursor.rs"]
mod merge_depth_cursor;
#[path = "../../merge/depth/merge_depth_cursor_range.rs"]
mod merge_depth_cursor_range;
#[path = "../../merge/depth/merge_depth_stream.rs"]
mod merge_depth_stream;
#[path = "../../merge/depth/merge_event_validation.rs"]
mod merge_event_validation;
#[path = "../../merge/execution/merge_executor.rs"]
mod merge_executor;
#[path = "../../merge/partition/merge_full_equivalence.rs"]
mod merge_full_equivalence;
#[path = "../../merge/depth/merge_indexed_cursor.rs"]
mod merge_indexed_cursor;
#[path = "../../merge/depth/merge_inventory.rs"]
mod merge_inventory;
#[path = "../../merge/execution/merge_manifest_builder.rs"]
mod merge_manifest_builder;
#[path = "../../merge/execution/merge_output_validation.rs"]
mod merge_output_validation;
#[path = "../../merge/execution/merge_parallel.rs"]
mod merge_parallel;
#[path = "../../merge/execution/merge_parallel_commit.rs"]
mod merge_parallel_commit;
#[path = "../../merge/partition/merge_partition_coverage.rs"]
mod merge_partition_coverage;
#[path = "../../merge/partition/merge_partition_equivalence.rs"]
mod merge_partition_equivalence;
#[path = "../../merge/partition/merge_partition_executor.rs"]
mod merge_partition_executor;
#[path = "../../merge/partition/merge_partition_input.rs"]
mod merge_partition_input;
#[path = "../../merge/execution/merge_resolver.rs"]
mod merge_resolver;
#[path = "../../merge/trades/merge_trade_cursor.rs"]
mod merge_trade_cursor;
#[path = "../../merge/trades/merge_trade_cursor_indexed.rs"]
mod merge_trade_cursor_indexed;
#[path = "../../merge/trades/merge_trade_kway_real.rs"]
mod merge_trade_kway_real;
#[path = "../../merge/trades/merge_trade_stream.rs"]
mod merge_trade_stream;
#[path = "../../merge/execution/merge_unified_commit.rs"]
mod merge_unified_commit;
#[path = "../../merge/execution/merge_unified_manifest_builder.rs"]
mod merge_unified_manifest_builder;
#[path = "../../merge/execution/merge_unified_output_validation.rs"]
mod merge_unified_output_validation;
#[path = "../../merge/partition/merge_unified_parallel_equivalence.rs"]
mod merge_unified_parallel_equivalence;
#[path = "../../merge/partition/merge_unified_partition_executor.rs"]
mod merge_unified_partition_executor;
#[path = "../../merge/partition/merge_unified_partition_input.rs"]
mod merge_unified_partition_input;
#[path = "../../merge/output/merge_unified_production_rotation.rs"]
mod merge_unified_production_rotation;
#[path = "../../merge/execution/merge_unified_reference_integrity.rs"]
mod merge_unified_reference_integrity;
#[path = "../../merge/execution/merge_unified_stream_resolver.rs"]
mod merge_unified_stream_resolver;
#[path = "../../merge/combined/merge_unified_timeline.rs"]
mod merge_unified_timeline;
#[path = "../../merge/combined/merge_unified_writer.rs"]
mod merge_unified_writer;
