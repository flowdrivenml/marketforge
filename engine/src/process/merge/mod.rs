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
#[path = "materialize/cache.rs"]
mod materialize_cache;
#[path = "materialize/checkpoints.rs"]
mod materialize_checkpoints;
#[path = "materialize/combined_builder.rs"]
mod materialize_combined_builder;
#[path = "materialize/decoder.rs"]
mod materialize_decoder;
#[path = "materialize/depth_builder.rs"]
mod materialize_depth_builder;
#[path = "materialize/depth_metadata.rs"]
mod materialize_depth_metadata;
#[path = "materialize/global_validate.rs"]
mod materialize_global_validate;
#[path = "materialize/inventory.rs"]
mod materialize_inventory;
#[path = "materialize/manifest.rs"]
mod materialize_manifest;
#[path = "materialize/parallel.rs"]
mod materialize_parallel;
#[path = "materialize/partition.rs"]
mod materialize_partition;
#[path = "materialize/plan.rs"]
mod materialize_plan;
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
#[path = "materialize/schema.rs"]
mod materialize_schema;
#[path = "materialize/timeline_reader.rs"]
mod materialize_timeline_reader;
#[path = "materialize/trade_builder.rs"]
mod materialize_trade_builder;
#[path = "materialize/validate.rs"]
mod materialize_validate;
#[path = "materialize/window.rs"]
mod materialize_window;
#[path = "materialize/writer.rs"]
mod materialize_writer;
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

pub use materialize_cache::{CanonicalCacheMetrics, CanonicalRowGroupCache};
pub use materialize_checkpoints::{PhysicalMergeCheckpoint, build_physical_checkpoints};
pub use materialize_combined_builder::{PhysicalCombinedBatchBuilder, PhysicalCombinedEvent};
pub use materialize_decoder::GroupedCanonicalDecoder;
pub use materialize_depth_builder::{PhysicalDepthBatchBuilder, PhysicalDepthEvent};
pub use materialize_depth_metadata::{DepthMetadataResolver, ResolvedDepthMetadata};
pub use materialize_global_validate::{ValidatedPhysicalDataset, validate_physical_dataset};
pub use materialize_inventory::{CanonicalFileRange, CanonicalFileSlice, CanonicalRowIndex};
pub use materialize_manifest::{
    PHYSICAL_MERGE_MANIFEST_VERSION, PHYSICAL_MERGE_SCHEMA_VERSION, PhysicalMergeManifest,
    write_physical_merge_manifest,
};
pub use materialize_parallel::{MaterializationPartitionPlan, execute_parallel_materialization};
pub use materialize_partition::{
    MaterializationPartitionInput, MaterializationPartitionResult, MaterializationPartitionWorker,
};
pub use materialize_plan::build_materialization_plans;
pub use materialize_planner::{
    CanonicalReadPlanner, GroupedReadPlan, PlannedCanonicalRead, PlannedRowGroupKey,
};
pub use materialize_publish::prepare_physical_publication;
pub use materialize_reader::CanonicalParquetReader;
pub use materialize_reconstruction::{
    PhysicalInitialReconstruction, PhysicalReconstructionAvailability,
    resolve_physical_initial_reconstruction,
};
pub use materialize_resolver::{CanonicalReferenceResolver, CanonicalTaskKey, ResolvedMarketEvent};
pub use materialize_resources::{
    MaterializationResourcePlan, MaterializationWorkerLimits, estimate_materialization_resources,
};
pub use materialize_row_groups::{
    CanonicalRowGroup, CanonicalRowGroupIndex, CanonicalRowGroupSlice,
};
pub use materialize_schema::{
    physical_combined_schema, physical_depth_schema, physical_trade_schema,
};
pub use materialize_timeline_reader::MaterializationTimelineReader;
pub use materialize_trade_builder::PhysicalTradeBatchBuilder;
pub use materialize_validate::{
    PhysicalFileValidation, PhysicalPartitionValidation, validate_physical_partition,
};
pub use materialize_window::{
    MaterializationBatchLimits, MaterializationWindow, MaterializationWindowBuilder,
};
pub use materialize_writer::{PhysicalParquetWriter, PhysicalWriterMetrics};
