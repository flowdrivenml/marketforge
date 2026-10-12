#[path = "depth/boundary.rs"]
mod boundary;
#[path = "depth/checkpoint.rs"]
mod checkpoint;
#[path = "execution/commit.rs"]
mod commit;
#[path = "depth/depth_cursor.rs"]
mod depth_cursor;
#[path = "depth/depth_stream.rs"]
mod depth_stream;
#[path = "core/event.rs"]
mod event;
#[path = "core/event_source.rs"]
mod event_source;
#[path = "depth/event_validation.rs"]
mod event_validation;
#[path = "execution/executor.rs"]
mod executor;
#[path = "depth/inventory.rs"]
mod inventory;
#[path = "core/kway.rs"]
mod kway;
#[path = "core/kway_event.rs"]
mod kway_event;
#[path = "output/manifest.rs"]
mod manifest;
#[path = "output/manifest_builder.rs"]
mod manifest_builder;
#[path = "core/ordering.rs"]
mod ordering;
#[path = "output/output_validation.rs"]
mod output_validation;
#[path = "execution/parallel.rs"]
mod parallel;
#[path = "execution/parallel_executor.rs"]
mod parallel_executor;
#[path = "partition/partition.rs"]
mod partition;
#[path = "partition/partition_executor.rs"]
mod partition_executor;
#[path = "partition/partition_input.rs"]
mod partition_input;
#[path = "depth/partition_stream.rs"]
mod partition_stream;
#[path = "execution/resolver.rs"]
mod resolver;
#[path = "execution/resources.rs"]
mod resources;
#[path = "core/source.rs"]
mod source;
#[path = "output/timeline.rs"]
mod timeline;
#[path = "trades/trade_cursor.rs"]
mod trade_cursor;
#[path = "trades/trade_inventory.rs"]
mod trade_inventory;
#[path = "trades/trade_stream.rs"]
mod trade_stream;
#[path = "execution/unified_executor.rs"]
mod unified_executor;
#[path = "output/unified_manifest.rs"]
mod unified_manifest;
#[path = "output/unified_manifest_builder.rs"]
mod unified_manifest_builder;
#[path = "output/unified_output_validation.rs"]
mod unified_output_validation;
#[path = "execution/unified_parallel.rs"]
mod unified_parallel;
#[path = "partition/unified_partition_executor.rs"]
mod unified_partition_executor;
#[path = "partition/unified_partition_input.rs"]
mod unified_partition_input;
#[path = "output/unified_reference_validation.rs"]
mod unified_reference_validation;
#[path = "execution/unified_stream_resolver.rs"]
mod unified_stream_resolver;
#[path = "output/unified_timeline.rs"]
mod unified_timeline;
#[path = "output/unified_writer.rs"]
mod unified_writer;
#[path = "output/writer.rs"]
mod writer;

pub use boundary::{DepthBoundaryLink, MergeBoundaryRelation, reconcile_depth_boundaries};
pub use checkpoint::{
    CheckpointAvailability, InitialMergeCheckpoint, MergeCheckpoint, build_merge_checkpoints,
    resolve_initial_checkpoints, select_merge_checkpoints,
};
pub use commit::MergeCommitPaths;
pub use depth_cursor::DepthEventCursor;
pub use depth_stream::{DepthEventRef, DepthStream};
pub use event::{MergeEventKind, MergeEventRef, MergedEvent};
pub use event_source::MergeEventSource;
pub use event_validation::{DepthEventValidation, validate_depth_events};
pub use executor::{MergeExecutionResult, execute_merge_job};
pub use inventory::{DepthTaskInventory, build_depth_inventory};
pub use kway::{KWayDepthMerger, MergedDepthEvent};
pub use kway_event::KWayEventMerger;
pub use manifest::{
    MERGE_MANIFEST_FILENAME, MERGE_MANIFEST_VERSION, MergeBoundaryRecord, MergeInputDataset,
    MergeManifest, MergeSkippedInitialization, MergeStream, MergeTimelineFile, build_merge_streams,
    write_merge_manifest,
};
pub use manifest_builder::build_parallel_merge_manifest;
pub use ordering::MergeKey;
pub use output_validation::{ValidatedMergeOutput, validate_merge_output};
pub use parallel::{execute_merge_partitions_parallel, validate_partition_results};
pub use parallel_executor::{ParallelMergeExecutionResult, execute_parallel_merge_job};
pub use partition::{MergePartition, MergePartitionPlan, plan_merge_partitions};
pub use partition_executor::{MergePartitionResult, execute_merge_partition};
pub use partition_input::{PartitionInputPlan, PartitionTaskInput, plan_partition_inputs};
pub use partition_stream::PartitionDepthStream;
pub use resolver::{ResolvedMergeInput, resolve_merge_inputs};
pub use resources::{estimate_merge_partition_resources, estimate_merge_stream_resources};
pub use source::DepthEventSource;
pub use timeline::{TimelineBatchBuilder, TimelineRecord, timeline_schema};
pub use trade_cursor::{IndexedTrade, TradeCursor};
pub use trade_inventory::{TradeFileInventory, TradeTaskInventory, build_trade_inventory};
pub use trade_stream::{TradeEventRef, TradeStream};
pub use unified_executor::{UnifiedMergeExecutionResult, execute_unified_merge_job};
pub use unified_manifest::{
    UNIFIED_MERGE_MANIFEST_VERSION, UnifiedMergeManifest, UnifiedMergeMode, UnifiedMergeStream,
    UnifiedMergeStreamKind,
};
pub use unified_manifest_builder::build_unified_merge_manifest;
pub use unified_output_validation::{ValidatedUnifiedMergeOutput, validate_unified_merge_output};
pub use unified_parallel::{
    execute_unified_partitions_parallel, validate_unified_partition_results,
};
pub use unified_partition_executor::{UnifiedPartitionResult, execute_unified_partition};
pub use unified_partition_input::{
    MergeTaskInterval, UnifiedPartitionInputPlan, depth_task_intervals,
    plan_unified_partition_inputs, trade_task_intervals,
};
pub use unified_reference_validation::{CanonicalTaskBounds, UnifiedReferenceValidator};
pub use unified_stream_resolver::resolve_unified_merge_streams;
pub use unified_timeline::{
    UNIFIED_TIMELINE_VERSION, UnifiedTimelineBatchBuilder, UnifiedTimelineRecord,
    unified_timeline_schema,
};
pub use unified_writer::{UnifiedTimelineWriter, UnifiedTimelineWriterMetrics};
pub use writer::{TimelineWriter, TimelineWriterMetrics};
