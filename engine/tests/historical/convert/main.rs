//! Grouped Historical convert tests. Execute using the bounded Historical verifier.
#[path = "output/atomic_publication.rs"]
mod convert_atomic_publication;
#[path = "execution/checkpoints.rs"]
mod convert_checkpoints;
#[path = "combined/combined_checkpoint_ordering.rs"]
mod convert_combined_checkpoint_ordering;
#[path = "combined/combined_schema.rs"]
mod convert_combined_schema;
#[path = "planning/decimal.rs"]
mod convert_decimal;
#[path = "depth/depth_arithmetic.rs"]
mod convert_depth_arithmetic;
#[path = "depth/depth_builder.rs"]
mod convert_depth_builder;
#[path = "depth/depth_schema.rs"]
mod convert_depth_schema;
#[path = "planning/job.rs"]
mod convert_job;
#[path = "output/output_schema.rs"]
mod convert_output_schema;
#[path = "output/output_trade.rs"]
mod convert_output_trade;
#[path = "output/output_writer.rs"]
mod convert_output_writer;
#[path = "output/receipt.rs"]
mod convert_receipt;
#[path = "reference/reference_evaluation.rs"]
mod convert_reference_evaluation;
#[path = "reference/reference_state.rs"]
mod convert_reference_state;
#[path = "trades/trade.rs"]
mod convert_trade;

#[path = "combined/combined_alignment.rs"]
mod convert_combined_alignment;

#[path = "execution/partitions.rs"]
mod convert_partitions;

#[path = "execution/core_regression.rs"]
mod core_regression;

#[path = "support/arrow_comparison.rs"]
mod arrow_comparison;
#[path = "../support/conversion_fixture.rs"]
mod conversion_fixture;
#[path = "support/market.rs"]
mod market;
#[path = "execution/portable.rs"]
mod portable;

#[path = "combined/combined_validation.rs"]
mod convert_combined_validation;

#[path = "combined/combined_publication.rs"]
mod convert_combined_publication;
#[path = "reference/reference_seek.rs"]
mod convert_reference_seek;

#[path = "planning/catalog.rs"]
mod convert_catalog;

#[path = "planning/graph.rs"]
mod convert_graph;

#[path = "planning/planner.rs"]
mod convert_planner;

#[path = "reference/external_reference.rs"]
mod convert_external_reference;

#[path = "output/output_manifest.rs"]
mod convert_output_manifest;

#[path = "depth/depth_validation.rs"]
mod convert_depth_validation;

#[path = "support/depth_fixture.rs"]
mod depth_fixture;
