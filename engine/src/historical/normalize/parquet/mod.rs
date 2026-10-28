//! Historical facade and sink adapters for canonical Parquet output.
mod sinks;
pub use crate::shared::persistence::parquet::{
    DECIMAL_PRECISION, DECIMAL_SCALE, DepthBatchBuilder, DepthEventBatchBuilder,
    DepthSegmentManifest, IndexedDepthEvent, ParquetDepthWriter, ParquetDepthWriterMetrics,
    ParquetTradeWriter, ParquetWriterMetrics, TradeBatchBuilder, decimal_to_i256,
    depth_event_schema, depth_schema, i256_to_decimal, read_depth_segments, trade_schema,
    write_depth_segments,
};
