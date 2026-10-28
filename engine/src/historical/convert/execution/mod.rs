mod combined;
mod depth;
pub(in crate::historical::convert) mod executor;
mod shared;
mod trades;

pub use combined::alignment::CombinedReferenceAlignment;
pub use combined::checkpoints::{
    CombinedCheckpointAccumulator, CombinedReferenceCheckpoint, CombinedReferencePrepass,
    build_combined_reference_checkpoints, build_combined_reference_checkpoints_full_reader,
    build_combined_reference_checkpoints_with_progress,
};
pub use combined::conversion::{
    CombinedConversionKernel, CombinedConversionMetrics, CombinedConversionRow,
};
pub use combined::materialize::{
    PhysicalCombinedConversionMetrics, materialize_combined_conversion,
};
pub use combined::parallel::{
    ParallelCombinedConversionResult, execute_parallel_combined_conversion,
    execute_parallel_combined_conversion_with_progress,
};
pub use combined::references::CombinedReferenceSelection;
pub use combined::worker::{
    CombinedPartitionResult, execute_combined_partition, execute_combined_partition_with_progress,
};
pub use depth::alignment::DepthConversionAlignment;
pub use depth::checkpoints::{
    DepthReferenceCheckpoint, DepthReferencePrepass, build_depth_reference_checkpoints,
};
pub use depth::conversion::{DepthConversionKernel, DepthConversionMetrics};
pub use depth::materialize::{PhysicalDepthConversionMetrics, materialize_depth_conversion};
pub use depth::parallel::{
    ParallelDepthConversionResult, execute_parallel_depth_conversion,
    execute_parallel_depth_conversion_with_progress,
};
pub use depth::worker::{
    DepthPartitionResult, execute_depth_partition, execute_depth_partition_with_progress,
};
pub use shared::checkpoints::{ConversionCheckpointBuilder, ConversionReferenceCheckpoint};
pub use shared::partition::{
    ConversionPartition, ConversionPartitionPlan, ConversionRowGroup,
    build_conversion_partition_plan, build_conversion_partition_plan_with_count,
};
pub use shared::partition_reader::ConversionPartitionReader;
pub use shared::prepass::{ConversionPrepassResult, build_conversion_checkpoints};
pub use shared::reader::{CONVERSION_READ_BATCH_SIZE, PhysicalConversionReader};
pub use trades::conversion::TradeConversionKernel;
pub use trades::materialize::{PhysicalConversionMetrics, materialize_trade_conversion};
pub use trades::parallel::{
    ParallelConversionResult, execute_parallel_conversion,
    execute_parallel_conversion_with_progress,
};
pub use trades::worker::{
    ConversionPartitionResult, execute_conversion_partition,
    execute_conversion_partition_with_progress,
};
