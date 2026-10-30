//! Grouped Historical convert tests. Execute using the bounded Historical verifier.
#[path = "output/atomic_publication.rs"]
mod convert_atomic_publication;
#[path = "planning/catalog.rs"]
mod convert_catalog;
#[path = "execution/checkpoints.rs"]
mod convert_checkpoints;
#[path = "combined/combined_checkpoint_ordering.rs"]
mod convert_combined_checkpoint_ordering;
#[path = "combined/combined_conversion.rs"]
mod convert_combined_conversion;
#[path = "combined/combined_schema.rs"]
mod convert_combined_schema;
#[path = "planning/decimal.rs"]
mod convert_decimal;
#[path = "depth/depth_arithmetic.rs"]
mod convert_depth_arithmetic;
#[path = "depth/depth_builder.rs"]
mod convert_depth_builder;
#[path = "depth/depth_planner.rs"]
mod convert_depth_planner;
#[path = "depth/depth_schema.rs"]
mod convert_depth_schema;
#[path = "planning/graph.rs"]
mod convert_graph;
#[path = "planning/job.rs"]
mod convert_job;
#[path = "output/output_schema.rs"]
mod convert_output_schema;
#[path = "output/output_trade.rs"]
mod convert_output_trade;
#[path = "output/output_writer.rs"]
mod convert_output_writer;
#[path = "planning/planner.rs"]
mod convert_planner;
#[path = "output/receipt.rs"]
mod convert_receipt;
#[path = "reference/reference_evaluation.rs"]
mod convert_reference_evaluation;
#[path = "reference/reference_selection.rs"]
mod convert_reference_selection;
#[path = "reference/reference_state.rs"]
mod convert_reference_state;
#[path = "trades/trade.rs"]
mod convert_trade;

#[path = "combined/combined_alignment.rs"]
mod convert_combined_alignment;

#[path = "reference/reference_reader_bounded.rs"]
mod convert_reference_reader;

#[path = "execution/partitions.rs"]
mod convert_partitions;

#[path = "execution/execution_reader_bounded.rs"]
mod convert_execution_reader;

#[path = "execution/core_regression.rs"]
mod core_regression;
