// Re-export shared schema and decimal primitives from the storage module.
// Local modules below depend on these shared types.
pub use crate::storage::parquet::decimal::{decimal_to_i256, i256_to_decimal};
pub use crate::storage::parquet::schema::depth_event_schema;
pub use crate::storage::parquet::schema::depth_schema;
pub use crate::storage::parquet::schema::{DECIMAL_PRECISION, DECIMAL_SCALE, trade_schema};

mod decimal;
mod depth;
mod depth_events;
mod depth_writer;
mod schema;
mod segments;
mod trades;
mod writer;

pub use depth::DepthBatchBuilder;
pub use depth_events::{DepthEventBatchBuilder, IndexedDepthEvent};
pub use depth_writer::{ParquetDepthWriter, ParquetDepthWriterMetrics};
pub use segments::{DepthSegmentManifest, read_depth_segments, write_depth_segments};
pub use trades::TradeBatchBuilder;
pub use writer::{ParquetTradeWriter, ParquetWriterMetrics};
