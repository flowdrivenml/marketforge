#[path = "planning/catalog.rs"]
mod catalog;
#[path = "shared/decimal.rs"]
mod decimal;
#[path = "depth/depth.rs"]
mod depth;
pub mod execution;
#[path = "execution/executor.rs"]
mod executor;
#[path = "planning/graph.rs"]
mod graph;
pub mod output;
#[path = "planning/planner.rs"]
mod planner;
pub mod progress;
pub mod reference;
#[path = "trades/trade.rs"]
mod trade;

pub use catalog::{
    ResolvedConversionCatalog, ResolvedConversionInstrument, resolve_conversion_catalog,
};
pub use decimal::{CONVERSION_DECIMAL_PRECISION, CONVERSION_DECIMAL_SCALE, ConversionDecimal};
pub use depth::{ConvertedDepthLevel, convert_depth_level};
pub use executor::{
    ConversionExecutionStats, ConversionStreamStats, execute_conversion_sequential,
};
pub use graph::{ConversionDirection, ConversionEdge, ConversionGraph, ConversionPath};
pub use planner::{ConversionPlan, DEFAULT_MAX_PATHS, PlannedConversionStream, plan_conversion};
pub use trade::{ConvertedTradeValues, convert_trade_values};
