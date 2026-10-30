//! Grouped Historical convert tests. Opt-in only; do not execute during ordinary verification.
#[path = "../../convert/combined/combined_alignment_external.rs"]
mod convert_combined_alignment;
#[path = "../../convert/combined/combined_checkpoints.rs"]
mod convert_combined_checkpoints;
#[path = "../../convert/combined/combined_depth_adapter.rs"]
mod convert_combined_depth_adapter;
#[path = "../../convert/combined/combined_equivalence.rs"]
mod convert_combined_equivalence;
#[path = "../../convert/combined/combined_materialize.rs"]
mod convert_combined_materialize;
#[path = "../../convert/combined/combined_parallel.rs"]
mod convert_combined_parallel;
#[path = "../../convert/combined/combined_publication.rs"]
mod convert_combined_publication;
#[path = "../../convert/combined/combined_references.rs"]
mod convert_combined_references;
#[path = "../../convert/combined/combined_restoration.rs"]
mod convert_combined_restoration;
#[path = "../../convert/combined/combined_validation.rs"]
mod convert_combined_validation;
#[path = "../../convert/combined/combined_worker.rs"]
mod convert_combined_worker;
#[path = "../../convert/combined/combined_writer.rs"]
mod convert_combined_writer;
#[path = "../../convert/depth/depth_alignment.rs"]
mod convert_depth_alignment;
#[path = "../../convert/depth/depth_checkpoints.rs"]
mod convert_depth_checkpoints;
#[path = "../../convert/depth/depth_equivalence.rs"]
mod convert_depth_equivalence;
#[path = "../../convert/depth/depth_materialize.rs"]
mod convert_depth_materialize;
#[path = "../../convert/depth/depth_parallel.rs"]
mod convert_depth_parallel;
#[path = "../../convert/depth/depth_preservation.rs"]
mod convert_depth_preservation;
#[path = "../../convert/depth/depth_publication.rs"]
mod convert_depth_publication;
#[path = "../../convert/depth/depth_validation.rs"]
mod convert_depth_validation;
#[path = "../../convert/execution/execution_reader.rs"]
mod convert_execution_reader;
#[path = "../../convert/execution/executor.rs"]
mod convert_executor;
#[path = "../../convert/reference/external_reference.rs"]
mod convert_external_reference;
#[path = "../../convert/execution/materialize.rs"]
mod convert_materialize;
#[path = "../../convert/execution/materialize_multihop.rs"]
mod convert_materialize_multihop;
#[path = "../../convert/output/output_manifest.rs"]
mod convert_output_manifest;
#[path = "../../convert/output/output_validation.rs"]
mod convert_output_validation;
#[path = "../../convert/execution/parallel.rs"]
mod convert_parallel;
#[path = "../../convert/execution/parallel_publication.rs"]
mod convert_parallel_publication;
#[path = "../../convert/execution/parallel_validation.rs"]
mod convert_parallel_validation;
#[path = "../../convert/execution/partition_reader.rs"]
mod convert_partition_reader;
#[path = "../../convert/trades/persistent_fixture.rs"]
mod convert_persistent_fixture;
#[path = "../../convert/execution/prepass.rs"]
mod convert_prepass;
#[path = "../../convert/output/publication.rs"]
mod convert_publication;
#[path = "../../convert/reference/reference_reader.rs"]
mod convert_reference_reader;
#[path = "../../convert/reference/reference_seek.rs"]
mod convert_reference_seek;
#[path = "../../convert/output/source_preservation.rs"]
mod convert_source_preservation;
#[path = "../../convert/trades/validate_values.rs"]
mod convert_validate_values;
#[path = "../../convert/execution/worker.rs"]
mod convert_worker;
