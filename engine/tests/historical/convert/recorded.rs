//! Grouped Historical convert tests. Opt-in only; do not execute during ordinary verification.
#[path = "combined/combined_alignment_external.rs"]
mod convert_combined_alignment;
#[path = "combined/combined_checkpoints.rs"]
mod convert_combined_checkpoints;
#[path = "combined/combined_depth_adapter.rs"]
mod convert_combined_depth_adapter;
#[path = "combined/combined_equivalence.rs"]
mod convert_combined_equivalence;
#[path = "combined/combined_materialize.rs"]
mod convert_combined_materialize;
#[path = "combined/combined_parallel.rs"]
mod convert_combined_parallel;
#[path = "combined/combined_publication.rs"]
mod convert_combined_publication;
#[path = "combined/combined_references.rs"]
mod convert_combined_references;
#[path = "combined/combined_restoration.rs"]
mod convert_combined_restoration;
#[path = "combined/combined_validation.rs"]
mod convert_combined_validation;
#[path = "combined/combined_worker.rs"]
mod convert_combined_worker;
#[path = "combined/combined_writer.rs"]
mod convert_combined_writer;
#[path = "depth/depth_alignment.rs"]
mod convert_depth_alignment;
#[path = "depth/depth_checkpoints.rs"]
mod convert_depth_checkpoints;
#[path = "depth/depth_equivalence.rs"]
mod convert_depth_equivalence;
#[path = "depth/depth_materialize.rs"]
mod convert_depth_materialize;
#[path = "depth/depth_parallel.rs"]
mod convert_depth_parallel;
#[path = "depth/depth_preservation.rs"]
mod convert_depth_preservation;
#[path = "depth/depth_publication.rs"]
mod convert_depth_publication;
#[path = "depth/depth_validation.rs"]
mod convert_depth_validation;
#[path = "execution/execution_reader.rs"]
mod convert_execution_reader;
#[path = "execution/executor.rs"]
mod convert_executor;
#[path = "reference/external_reference.rs"]
mod convert_external_reference;
#[path = "execution/materialize.rs"]
mod convert_materialize;
#[path = "execution/materialize_multihop.rs"]
mod convert_materialize_multihop;
#[path = "output/output_manifest.rs"]
mod convert_output_manifest;
#[path = "output/output_validation.rs"]
mod convert_output_validation;
#[path = "execution/parallel.rs"]
mod convert_parallel;
#[path = "execution/parallel_publication.rs"]
mod convert_parallel_publication;
#[path = "execution/parallel_validation.rs"]
mod convert_parallel_validation;
#[path = "execution/partition_reader.rs"]
mod convert_partition_reader;
#[path = "trades/persistent_fixture.rs"]
mod convert_persistent_fixture;
#[path = "execution/prepass.rs"]
mod convert_prepass;
#[path = "output/publication.rs"]
mod convert_publication;
#[path = "reference/reference_reader.rs"]
mod convert_reference_reader;
#[path = "reference/reference_seek.rs"]
mod convert_reference_seek;
#[path = "output/source_preservation.rs"]
mod convert_source_preservation;
#[path = "trades/validate_values.rs"]
mod convert_validate_values;
#[path = "execution/worker.rs"]
mod convert_worker;
