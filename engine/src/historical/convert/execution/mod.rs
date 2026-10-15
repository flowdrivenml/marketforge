#[path = "shared/checkpoints.rs"]
mod checkpoints;
#[path = "combined/combined_alignment.rs"]
mod combined_alignment;
#[path = "combined/combined_checkpoints.rs"]
mod combined_checkpoints;
#[path = "combined/combined_conversion.rs"]
mod combined_conversion;
#[path = "combined/combined_materialize.rs"]
mod combined_materialize;
#[path = "combined/combined_parallel.rs"]
mod combined_parallel;
#[path = "combined/combined_references.rs"]
mod combined_references;
#[path = "combined/combined_worker.rs"]
mod combined_worker;
#[path = "trades/conversion.rs"]
mod conversion;
#[path = "depth/depth_alignment.rs"]
mod depth_alignment;
#[path = "depth/depth_checkpoints.rs"]
mod depth_checkpoints;
#[path = "depth/depth_conversion.rs"]
mod depth_conversion;
#[path = "depth/depth_materialize.rs"]
mod depth_materialize;
#[path = "depth/depth_parallel.rs"]
mod depth_parallel;
#[path = "depth/depth_worker.rs"]
mod depth_worker;
#[path = "trades/materialize.rs"]
mod materialize;
#[path = "trades/parallel.rs"]
mod parallel;
#[path = "shared/partition.rs"]
mod partition;
#[path = "shared/partition_reader.rs"]
mod partition_reader;
#[path = "shared/prepass.rs"]
mod prepass;
#[path = "shared/reader.rs"]
mod reader;
#[path = "trades/worker.rs"]
mod worker;

pub use checkpoints::{ConversionCheckpointBuilder, ConversionReferenceCheckpoint};
pub use combined_alignment::CombinedReferenceAlignment;
pub use combined_checkpoints::{
    CombinedCheckpointAccumulator, CombinedReferenceCheckpoint, CombinedReferencePrepass,
    build_combined_reference_checkpoints, build_combined_reference_checkpoints_full_reader,
    build_combined_reference_checkpoints_with_progress,
};
pub use combined_conversion::{
    CombinedConversionKernel, CombinedConversionMetrics, CombinedConversionRow,
};
pub use combined_materialize::{
    PhysicalCombinedConversionMetrics, materialize_combined_conversion,
};
pub use combined_parallel::{
    ParallelCombinedConversionResult, execute_parallel_combined_conversion,
    execute_parallel_combined_conversion_with_progress,
};
pub use combined_references::CombinedReferenceSelection;
pub use combined_worker::{
    CombinedPartitionResult, execute_combined_partition, execute_combined_partition_with_progress,
};
pub use conversion::TradeConversionKernel;
pub use depth_alignment::DepthConversionAlignment;
pub use depth_checkpoints::{
    DepthReferenceCheckpoint, DepthReferencePrepass, build_depth_reference_checkpoints,
};
pub use depth_conversion::{DepthConversionKernel, DepthConversionMetrics};
pub use depth_materialize::{PhysicalDepthConversionMetrics, materialize_depth_conversion};
pub use depth_parallel::{
    ParallelDepthConversionResult, execute_parallel_depth_conversion,
    execute_parallel_depth_conversion_with_progress,
};
pub use depth_worker::{
    DepthPartitionResult, execute_depth_partition, execute_depth_partition_with_progress,
};
pub use materialize::{PhysicalConversionMetrics, materialize_trade_conversion};
pub use parallel::{
    ParallelConversionResult, execute_parallel_conversion,
    execute_parallel_conversion_with_progress,
};
pub use partition::{
    ConversionPartition, ConversionPartitionPlan, ConversionRowGroup,
    build_conversion_partition_plan, build_conversion_partition_plan_with_count,
};
pub use partition_reader::ConversionPartitionReader;
pub use prepass::{ConversionPrepassResult, build_conversion_checkpoints};
pub use reader::{CONVERSION_READ_BATCH_SIZE, PhysicalConversionReader};
pub use worker::{
    ConversionPartitionResult, execute_conversion_partition,
    execute_conversion_partition_with_progress,
};
