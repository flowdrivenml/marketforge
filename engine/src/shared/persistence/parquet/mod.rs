pub mod decimal;
pub mod schema;
pub mod trades;

pub use decimal::{decimal_to_i256, i256_to_decimal};
pub use schema::depth_event_schema;
pub use schema::depth_schema;
pub use schema::{DECIMAL_PRECISION, DECIMAL_SCALE, trade_schema};
pub use trades::TradeBatchBuilder;

mod depth;
mod depth_events;
mod depth_writer;
mod segments;
mod trade_writer;
pub use depth::DepthBatchBuilder;
pub use depth_events::{DepthEventBatchBuilder, IndexedDepthEvent};
pub use depth_writer::{ParquetDepthWriter, ParquetDepthWriterMetrics};
pub use segments::{DepthSegmentManifest, read_depth_segments, write_depth_segments};
pub use trade_writer::{ParquetTradeWriter, ParquetWriterMetrics};
