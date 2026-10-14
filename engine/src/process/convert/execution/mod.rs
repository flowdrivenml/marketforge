mod checkpoints;
mod combined_alignment;
mod combined_checkpoints;
mod combined_conversion;
mod combined_materialize;
mod combined_parallel;
mod combined_references;
mod combined_worker;
mod conversion;
mod depth_alignment;
mod depth_checkpoints;
mod depth_conversion;
mod depth_materialize;
mod depth_parallel;
mod depth_worker;
mod materialize;
mod parallel;
mod partition;
mod partition_reader;
mod prepass;
mod reader;
mod worker;

pub use checkpoints::{ConversionCheckpointBuilder, ConversionReferenceCheckpoint};
pub use combined_alignment::CombinedReferenceAlignment;
pub use combined_checkpoints::{
    CombinedCheckpointAccumulator, CombinedReferenceCheckpoint, CombinedReferencePrepass,
    build_combined_reference_checkpoints,
};
pub use combined_conversion::{
    CombinedConversionKernel, CombinedConversionMetrics, CombinedConversionRow,
};
pub use combined_materialize::{
    PhysicalCombinedConversionMetrics, materialize_combined_conversion,
};
pub use combined_parallel::{
    ParallelCombinedConversionResult, execute_parallel_combined_conversion,
};
pub use combined_references::CombinedReferenceSelection;
pub use combined_worker::{CombinedPartitionResult, execute_combined_partition};
pub use conversion::TradeConversionKernel;
pub use depth_alignment::DepthConversionAlignment;
pub use depth_checkpoints::{
    DepthReferenceCheckpoint, DepthReferencePrepass, build_depth_reference_checkpoints,
};
pub use depth_conversion::{DepthConversionKernel, DepthConversionMetrics};
pub use depth_materialize::{PhysicalDepthConversionMetrics, materialize_depth_conversion};
pub use depth_parallel::{ParallelDepthConversionResult, execute_parallel_depth_conversion};
pub use depth_worker::{DepthPartitionResult, execute_depth_partition};
pub use materialize::{PhysicalConversionMetrics, materialize_trade_conversion};
pub use parallel::{ParallelConversionResult, execute_parallel_conversion};
pub use partition::{
    ConversionPartition, ConversionPartitionPlan, ConversionRowGroup,
    build_conversion_partition_plan,
};
pub use partition_reader::ConversionPartitionReader;
pub use prepass::{ConversionPrepassResult, build_conversion_checkpoints};
pub use reader::{CONVERSION_READ_BATCH_SIZE, PhysicalConversionReader};
pub use worker::{ConversionPartitionResult, execute_conversion_partition};
