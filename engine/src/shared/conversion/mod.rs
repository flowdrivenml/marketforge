//! Exact, transport-independent last-trade currency conversion.
pub mod decimal;
pub mod depth;
pub mod evaluation;
pub mod graph;
pub mod planner;
pub mod reference;
pub mod trades;

pub use decimal::{CONVERSION_DECIMAL_PRECISION, CONVERSION_DECIMAL_SCALE, ConversionDecimal};
pub use depth::{ConvertedDepthLevel, convert_depth_level};
pub use graph::{ConversionDirection, ConversionEdge, ConversionGraph, ConversionPath};
pub use trades::{ConvertedTradeValues, convert_trade_values};
