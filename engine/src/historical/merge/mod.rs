mod core;
mod depth;
mod execution;
mod materialize;
mod output;
mod partition;
mod progress;
mod trades;

pub use core::event::{MergeEventKind, MergeEventRef, MergedEvent};
pub use core::event_source::MergeEventSource;
pub use core::kway::{KWayDepthMerger, MergedDepthEvent};
pub use core::kway_event::KWayEventMerger;
pub use core::ordering::MergeKey;
pub use core::source::DepthEventSource;
pub use depth::boundary::{DepthBoundaryLink, MergeBoundaryRelation, reconcile_depth_boundaries};
pub use depth::checkpoint::{
    CheckpointAvailability, InitialMergeCheckpoint, MergeCheckpoint, build_merge_checkpoints,
    resolve_initial_checkpoints, select_merge_checkpoints,
};
pub use depth::cursor::DepthEventCursor;
pub use depth::event_validation::{DepthEventValidation, validate_depth_events};
pub use depth::inventory::{DepthTaskInventory, build_depth_inventory};
pub use depth::partition_stream::PartitionDepthStream;
pub use depth::stream::{DepthEventRef, DepthStream};
pub use execution::parallel::executor::{ParallelMergeExecutionResult, execute_parallel_merge_job};
pub use execution::parallel::parallel::{
    execute_merge_partitions_parallel, validate_partition_results,
};
pub use execution::shared::commit::MergeCommitPaths;
pub use execution::shared::executor::{MergeExecutionResult, execute_merge_job};
pub use execution::shared::resolver::{ResolvedMergeInput, resolve_merge_inputs};
pub use execution::shared::resources::{
    estimate_merge_partition_resources, estimate_merge_stream_resources,
};
pub use execution::unified::executor::{
    UnifiedMergeExecutionResult, execute_unified_merge_job, execute_unified_merge_job_with_progress,
};
pub use execution::unified::parallel::{
    execute_unified_partitions_parallel, execute_unified_partitions_parallel_with_progress,
    validate_unified_partition_results,
};
pub use execution::unified::stream_resolver::resolve_unified_merge_streams;
pub use output::manifests::manifest::{
    MERGE_MANIFEST_FILENAME, MERGE_MANIFEST_VERSION, MergeBoundaryRecord, MergeInputDataset,
    MergeManifest, MergeSkippedInitialization, MergeStream, MergeTimelineFile, build_merge_streams,
    write_merge_manifest,
};
pub use output::manifests::manifest_builder::build_parallel_merge_manifest;
pub use output::manifests::unified_manifest::{
    UNIFIED_MERGE_MANIFEST_VERSION, UnifiedMergeManifest, UnifiedMergeMode, UnifiedMergeStream,
    UnifiedMergeStreamKind,
};
pub use output::manifests::unified_manifest_builder::build_unified_merge_manifest;
pub use output::validation::output_validation::{ValidatedMergeOutput, validate_merge_output};
pub use output::validation::unified_output_validation::{
    ValidatedUnifiedMergeOutput, validate_unified_merge_output,
};
pub use output::validation::unified_reference_validation::{
    CanonicalTaskBounds, UnifiedReferenceValidator,
};
pub use output::writers::timeline::{TimelineBatchBuilder, TimelineRecord, timeline_schema};
pub use output::writers::unified_timeline::{
    UNIFIED_TIMELINE_VERSION, UnifiedTimelineBatchBuilder, UnifiedTimelineRecord,
    unified_timeline_schema,
};
pub use output::writers::unified_writer::{UnifiedTimelineWriter, UnifiedTimelineWriterMetrics};
pub use output::writers::writer::{TimelineWriter, TimelineWriterMetrics};
pub use partition::shared::partition_executor::{MergePartitionResult, execute_merge_partition};
pub use partition::shared::partition_input::{
    PartitionInputPlan, PartitionTaskInput, plan_partition_inputs,
};
pub use partition::unified::partition_executor::{
    UnifiedPartitionResult, execute_unified_partition, execute_unified_partition_with_progress,
};
pub use partition::unified::partition_input::{
    MergeTaskInterval, UnifiedPartitionInputPlan, depth_task_intervals,
    plan_unified_partition_inputs, trade_task_intervals,
};
pub use partition::{MergePartition, MergePartitionPlan, plan_merge_partitions};
pub use trades::cursor::{IndexedTrade, TradeCursor};
pub use trades::inventory::{TradeFileInventory, TradeTaskInventory, build_trade_inventory};
pub use trades::stream::{TradeEventRef, TradeStream};

pub use depth::boundary_summary::{MergeBoundarySnapshot, MergeBoundarySummary};
pub use materialize::builders::combined_builder::{
    PhysicalCombinedBatchBuilder, PhysicalCombinedEvent,
};
pub use materialize::builders::depth_builder::{PhysicalDepthBatchBuilder, PhysicalDepthEvent};
pub use materialize::builders::depth_metadata::{DepthMetadataResolver, ResolvedDepthMetadata};
pub use materialize::builders::schema::{
    physical_combined_schema, physical_depth_schema, physical_trade_schema,
};
pub use materialize::builders::trade_builder::PhysicalTradeBatchBuilder;
pub use materialize::builders::writer::{PhysicalParquetWriter, PhysicalWriterMetrics};
pub use materialize::planning::partition::{
    MaterializationPartitionInput, MaterializationPartitionResult, MaterializationPartitionWorker,
};
pub use materialize::planning::plan::build_materialization_plans;
pub use materialize::planning::planner::{
    CanonicalReadPlanner, GroupedReadPlan, PlannedCanonicalRead, PlannedRowGroupKey,
};
pub use materialize::planning::resources::{
    MaterializationResourcePlan, MaterializationWorkerLimits, estimate_materialization_resources,
};
pub use materialize::planning::row_groups::{
    CanonicalRowGroup, CanonicalRowGroupIndex, CanonicalRowGroupSlice,
};
pub use materialize::planning::window::{
    MaterializationBatchLimits, MaterializationWindow, MaterializationWindowBuilder,
};
pub use materialize::publication::manifest::{
    PHYSICAL_MERGE_MANIFEST_VERSION, PHYSICAL_MERGE_SCHEMA_VERSION, PhysicalMergeManifest,
    write_physical_merge_manifest,
};
pub use materialize::publication::publish::prepare_physical_publication;
pub use materialize::readers::cache::{CanonicalCacheMetrics, CanonicalRowGroupCache};
pub use materialize::readers::decoder::GroupedCanonicalDecoder;
pub use materialize::readers::inventory::{
    CanonicalFileRange, CanonicalFileSlice, CanonicalRowIndex,
};
pub use materialize::readers::reader::{CanonicalForwardCursor, CanonicalParquetReader};
pub use materialize::readers::timeline_reader::MaterializationTimelineReader;
pub use materialize::shared::parallel::{
    MaterializationPartitionPlan, execute_parallel_materialization,
    execute_parallel_materialization_with_progress,
};
pub use materialize::shared::resolver::{
    CanonicalReferenceResolver, CanonicalTaskKey, ResolvedMarketEvent,
};
pub use materialize::validation::checkpoints::{
    PhysicalMergeCheckpoint, build_physical_checkpoints,
};
pub use materialize::validation::global_validate::{
    ValidatedPhysicalDataset, validate_physical_dataset, validate_physical_dataset_with_progress,
};
pub use materialize::validation::reconstruction::{
    PhysicalInitialReconstruction, PhysicalReconstructionAvailability,
    resolve_physical_initial_reconstruction,
};
pub use materialize::validation::validate::{
    PhysicalFileValidation, PhysicalPartitionValidation, validate_physical_partition,
    validate_physical_partition_with_progress,
};
pub use progress::{
    MergePhase, MergeProgressEvent, MergeProgressReporter, MergeProgressState, MergeWorkerProgress,
    PartitionKey, PartitionProgress, PartitionProgressCounters,
};
