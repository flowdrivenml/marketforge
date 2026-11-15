> Historical implementation report: the later [disk-backed refactor](normalization-disk-reconciliation.md)
> supersedes whole-group buffering and unused reconciliation scratch described below.

# Source reconciliation and instrument selection: change report

See [the accepted contract and resource model](normalization-source-selection.md)
for the Python handoff, safe-emission rules, authority requirements and limitations.
This report covers the second change; [resource-safety work](normalization-resource-safety.md)
remains in the checkout. Existing user and concurrent changes were preserved.

## Implemented changes and code references

- `config/batch.rs`: strict prepared schema 4, retired bounds rejected, validated
  tagged selection, original priority captured before deterministic expansion,
  offline family authority, deferred candidate specifications, bounded expansion.
- `config/job.rs`: standalone protocol-3 priority rebuilt from request array;
  legacy descriptive prepared provenance cannot activate multi-instrument routing.
- `normalize/streams.rs::plan/estimate/execute`: no external bounds/partition,
  compatible family grouping, one shared reconciliation quota and sequential
  writer capacity. Existing single-concrete-source streaming path retained.
- `normalize/reconcile.rs::reconcile_trades/depth_source_order`: observed chronology,
  instrument-scoped cross-source identity winners selected before output,
  unique overlapping trades retained; unsafe depth overlap rejected.
- `normalize/reconciled.rs::execute`: charged deferred events, physical-source
  equivalence, atomic depth outcomes, ordered canonical submission, accurate row
  ownership, fatal stop, unpublished metrics and typed not-attempted work.
- `normalize/family.rs::route`: one forward mixed-source decode, exact native
  filtering before full metadata lookup, independent concrete queues, changing
  populations, missing authority/unsupported/unsatisfied diagnostics.
- `normalize/worker/decoded.rs`, trade/depth workers and sinks: one-shot routed
  records with original ordinals; independent interpretation/state preserved.
- `normalize/metrics/*`, `executor.rs`, `session.rs`: source observations and
  default-zero accounting additions; typed `NotAttempted` cannot replace the
  original definitive failure. No successful commit with unpublished work.
- `normalize/manifest/{models,builder,validation}.rs`: old contiguous manifest3
  remains valid; manifest4 explicitly represents interleaved ownership,
  suppression and unencountered family candidates without empty output streams.

Confirmed defects addressed: external bounds previously governed grouped source
ordering, source priority was not carried independently through expansion, and
family expansion could repeatedly decode sources without an explicit population
selection contract. The active strict contract, execution grouping and routing now
share the new model. No price/quantity/contract/time conversion, raw profile,
canonical Parquet schema or live processing algorithm was changed.

## Resource and failure boundaries

The only new runtime field is `resources.normalization_limits.reconciliation_bytes`:
67,108,864 bytes per grouped/family unit by explicit default, validated, charged
before protected growth and reserved once in stream admission. Old runtime
schema1 documents default it; effective-config provenance serializes it. Existing
runtime filenames/settings remain authoritative. The ignored local
`data/.config/historical/general.toml` explicitly contains the same value; it is
not force-added. Deployments can omit the field and receive the explicit default.

Decoded routing queues, normalized events, identity/ownership allowances and
boundary storage remain charged through the unit. Existing record, depth,
metric-entry, workbook and finite-handle limits remain active. Readers are
sequential and concrete writers open/finalize sequentially. Retained results
remain covered by session reservations. No scratch consumer/spilling was added.
Admission estimates and conservative application charges are not measured RSS or
a hard process-memory ceiling; independent processes do not share a pool.

Mid-source failure preserves available physical/routed/unprocessed diagnostics.
Canonical writer/reconciliation failure stops subsequent submissions; pending
sources are not fabricated successes. Original definitive errors survive typed
skips and secondary finalization checks. Existing failure persistence/staging and
unrelated definitive job results remain intact. No incomplete dataset is committed.

## Contract and fixture compatibility

Prepared batch schema4 is deliberately distinct from standalone job3, runtime1,
CLI results and manifests. Batch3/retired bounds are rejected, not silently ignored.
Concrete omitted selection stays single-symbol; family omission and ambiguous
symbol-plus-all are rejected. Python code was not changed.

Fixture changes only: schema4, 103 bounds fields removed, explicit all on 21
family tasks. An independent structural comparison with the initial checkout
confirmed all 357 source paths/order/identities, requested interval, destinations,
namespace and catalog unchanged. All three requested OKX family scopes lack
concrete authority (21 tasks total); no real specifications were invented.

The new versioned example replaces `normalize-batch-v3.example.json` with
`normalize-batch-v4.example.json`. `.gitignore` adds one exact exception so that
new public example is visible despite the generic JSON ignore rule. Runtime TOML
ignore behavior is unchanged.

## Remaining limitations

- Grouped execution buffers a bounded whole group rather than implementing a
  scalable multi-cursor merge. Large groups fail resource admission/execution;
  no arbitrary-order/unlimited-overlap claim is made.
- Native persistent trade identity is supported only in documented established
  domains. Ambiguous IDs, regressions and unsafe preferred-record fallback fail.
- Partial/equal-time independent depth overlap fails; physical redundant sources
  and disjoint independently recoverable histories are supported.
- Family routing supports existing OKX native identity grammar, mixed CSV trades
  and JSONL depth. Arbitrary member enumeration and XLSX mixed identity are not
  implemented. Finite member maps cannot silently redefine all.
- Exact include requires supplied candidate authority at resolution. Unknown
  unselected records need no contract lookup; encountered selected unknowns fail.
- Manifest4 is intentionally rejected by the unchanged merge resolver (which
  accepts 2/3). Downstream merge support is a separate compatibility task.
- Workbook dependency allocation risks documented in the resource-safety report
  remain; this task does not claim allocator-wide enforcement.

## Modified files in this change

Implementation: `engine/src/historical/config/{batch.rs,batch/tests.rs,batch/selection_tests.rs,job.rs,mod.rs}`;
`engine/src/bin/marketforge-normalize.rs`;
`engine/src/historical/normalize/{streams.rs,streams_depth.rs,reconcile.rs,reconcile_tests.rs,reconciled.rs,family.rs,family_tests.rs,executor.rs,session.rs,mod.rs}`;
`normalize/worker/{decoded.rs,trades.rs,depth.rs,integrity.rs,sink.rs,mod.rs}`;
`normalize/metrics/{counters.rs,report.rs,mod.rs}`;
`normalize/manifest/{builder.rs,models.rs,validation.rs}`;
`normalize/resources/{estimate.rs,mod.rs,safety_tests.rs}`;
`engine/src/shared/historical/limits.rs`;
`engine/src/shared/error/model.rs`;
`engine/src/shared/formats/trades/{identity.rs,mod.rs}`.
Paths beginning `normalize/` are under `engine/src/historical/`.

Fixture/helper compatibility: `engine/tests/historical/fixtures/normalize.json`;
`engine/tests/historical/support/{migration.rs,profile_cases.rs,streams.rs,stream_depth.rs}`;
`engine/tests/historical/normalize/{cli_contract.rs,integrity_accounting.rs}`;
`engine/tests/historical/normalize/execution/{process_metrics.rs,processing_metrics_aggregation.rs}`.
The CLI/integrity pipeline helpers only lose obsolete field assignments; they were
not executed. No benchmark implementation changes were needed.

Documentation: `.gitignore`; `docs/historical/normalization-source-selection.md`;
this report; versioned batch example replacement;
`normalization-{runtime,tests,benchmarks,resource-safety}.md` and
`normalize-stream-output.md`. Benchmark documentation changes only update contract
version references, not methodology. Prior resource-safety and pre-existing
Cargo/agent changes remain uncommitted and are not attributed to this change.

## Verification

All 62 selected tests passed. Each selected test was inspected; tests use parsing,
small synthetic records/configuration, pure reconciliation, mocked execution,
isolated readers/decoders or sequence/accounting components. Exact commands:

```sh
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::config::batch::selection_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconcile::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::family::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconciled::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::worker::decoded::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconciled::failure_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::resources::safety_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib resource_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::config::batch::tests::fixture_preserves_large_ids_shared_ranks_and_authority -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::config::batch::tests::invalid_batches_fail_structural_validation -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::config::batch::tests::unknown_nested_fields_and_invalid_wire_values_fail -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::config::batch::tests::resolution_rejects_conflicts_missing_profiles_and_unmapped_family -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::config::batch::tests::family_expansion_uses_each_concrete_metadata_and_preserves_source_identity -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::manifest::validation::reconciliation_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::streams::tests::grouping_ignores_task_paths_but_separates_concrete_semantics -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --test book_sequence detects_sequence_gap -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --test book_sequence invalidation_requires_new_snapshot -- --exact
```

Individual test names:

```text
historical::config::batch::selection_tests::single_symbol_omission_is_compatible_and_all_is_not_ambiguous
historical::config::batch::selection_tests::strict_selection_modes_and_retired_fields
historical::config::batch::selection_tests::active_fixture_contract_has_no_bounds_and_explicit_family_intent
historical::config::batch::selection_tests::explicit_concrete_ranks_are_preserved_when_family_expands
historical::config::batch::selection_tests::family_reservation_counts_one_shared_buffer_and_one_active_writer
historical::config::batch::selection_tests::family_expansion_cannot_allocate_an_unadmitted_fanout
historical::config::batch::selection_tests::expansion_preserves_original_priority_and_is_deterministic
historical::config::batch::selection_tests::metadata_is_deferred_for_unencountered_family_candidates
historical::normalize::reconcile::tests::depth_recovery_never_fabricates_synchronization
historical::normalize::reconcile::tests::depth_uses_observed_chronology_and_rejects_ambiguous_equal_time_books
historical::normalize::reconcile::tests::quota_rejects_before_growth_and_overflow_is_explicit
historical::normalize::reconcile::tests::identities_are_instrument_exchange_and_stream_scoped_and_within_source_is_unchanged
historical::normalize::reconcile::tests::missing_id_overlap_and_unknown_reset_domains_fail_explicitly
historical::normalize::reconcile::tests::observed_first_last_and_min_max_are_not_invented
historical::normalize::reconcile::tests::priority_wins_before_emission_even_if_loser_has_an_earlier_timestamp
historical::normalize::reconcile::tests::overlap_keeps_unique_records_equal_timestamp_ids_and_later_tail
historical::normalize::reconcile::tests::mocked_jobs_preserve_definitive_results_after_another_reconciliation_fails
historical::normalize::reconcile::tests::scheduling_and_completion_order_do_not_change_winners
historical::normalize::family::tests::csv_routing_uses_the_existing_authoritative_identity_column_precedence
historical::normalize::family::tests::degradable_trade_csv_parse_errors_remain_bounded_file_diagnostics
historical::normalize::family::tests::include_filters_unknown_unselected_symbols_before_contract_lookup
historical::normalize::family::tests::changing_monthly_population_is_routed_once_with_per_source_absence
historical::normalize::family::tests::redundant_equivalent_family_selection_reads_first_only
historical::normalize::family::tests::jsonl_depth_routes_interleaved_instruments_without_a_shared_timestamp_domain
historical::normalize::family::tests::one_mixed_decode_and_bounded_queues_replace_per_symbol_archive_reads
historical::normalize::family::tests::never_observed_include_is_unsatisfied_but_absence_in_one_source_is_allowed
historical::normalize::family::tests::all_never_hides_encountered_missing_or_unsupported_metadata
historical::normalize::reconciled::tests::snapshot_outcomes_are_atomic_and_checked_before_buffer_growth
historical::normalize::reconciled::tests::source_equivalence_includes_selection_member_and_interpretation
historical::normalize::worker::decoded::tests::one_shot_inputs_preserve_physical_ordinals_without_reopening
historical::normalize::reconciled::failure_tests::earlier_skipped_member_does_not_replace_responsible_resource_failure
historical::normalize::resources::safety_tests::fatal_mock_source_stops_submission_without_losing_previous_metrics
historical::normalize::resources::safety_tests::bounded_metrics_preserve_existing_entries
historical::normalize::resources::safety_tests::depth_growth_fails_before_mutation
historical::normalize::resources::safety_tests::retained_results_and_file_capacity_have_owned_lifetimes
historical::normalize::resources::safety_tests::scheduler_holds_stream_capacity_during_mock_finalization
historical::normalize::resources::safety_tests::mocked_budgeted_work_preserves_other_results_on_impossible_admission
historical::normalize::resources::safety_tests::arithmetic_and_configuration_fail_explicitly
historical::normalize::resources::safety_tests::finite_file_capacity_rejects_concurrent_growth_without_waiting
historical::normalize::resources::safety_tests::streaming_size_and_classification_are_shared
historical::normalize::resources::safety_tests::retained_admission_rejects_before_starting_work
historical::normalize::resources::safety_tests::sequential_sources_share_writer_memory_in_stream_estimate
historical::normalize::resources::safety_tests::mocked_finalization_preserves_resource_error_metrics_and_skipped_cardinality
shared::persistence::parquet::footprint::resource_tests::footer_growth_checks_predicted_groups_before_writing
shared::io::source::jsonl::resource_tests::jsonl_limit_is_checked_before_buffer_growth
shared::io::decode::csv::csv_boundary_resource_tests::csv_payload_limit_ignores_blank_lines_and_crlf
shared::io::decode::csv::resource_tests::csv_scanner_handles_chunked_fields_and_escaped_quotes
shared::io::decode::csv::resource_tests::csv_limit_preserves_quoted_multiline_and_exact_boundaries
shared::io::decode::xlsx::observation_resource_tests::resource_failure_never_becomes_malformed_row_observation
shared::io::decode::xlsx::resource_tests::expanded_xml_and_materialized_rows_are_bounded
historical::normalize::manifest::validation::reconciliation_tests::suppression_and_unprocessed_rows_are_not_parse_failures
historical::normalize::manifest::validation::reconciliation_tests::interleaved_ownership_requires_version_four_and_complete_disjoint_coverage
historical::normalize::manifest::validation::reconciliation_tests::unobserved_family_candidates_need_no_empty_output_stream
historical::normalize::manifest::validation::reconciliation_tests::legacy_contiguous_spans_still_validate_without_new_fields
historical::config::batch::tests::fixture_preserves_large_ids_shared_ranks_and_authority
historical::config::batch::tests::invalid_batches_fail_structural_validation
historical::config::batch::tests::unknown_nested_fields_and_invalid_wire_values_fail
historical::config::batch::tests::resolution_rejects_conflicts_missing_profiles_and_unmapped_family
historical::config::batch::tests::family_expansion_uses_each_concrete_metadata_and_preserves_source_identity
historical::normalize::streams::tests::grouping_ignores_task_paths_but_separates_concrete_semantics
detects_sequence_gap
invalidation_requires_new_snapshot
```

Compilation passed:

```sh
cargo check --manifest-path engine/Cargo.toml --features "historical,monitor-tui"
cargo check --manifest-path engine/Cargo.toml --lib
cargo build --manifest-path engine/Cargo.toml --release --features "historical,monitor-tui" --bin marketforge-normalize
```

The isolated sequence test target also compiled unrelated binaries and emitted
five existing dead-code warnings in `marketforge-convert.rs`; that file was not
changed. The three requested compilation checks completed without warnings.
Targeted rustfmt (`--edition 2024 --config skip_children=true --check` on the
explicit edited Rust-file list) and `git diff --check -- engine/src engine/tests/historical docs/historical`
passed. Minified pre-existing migration/profile helpers received only their
necessary field/version edits, without broad reformatting.

Intentionally skipped: all normalization/CLI workloads and trade/depth pipelines,
real archives, the 357-source fixture as execution, full/unfiltered suites,
PostgreSQL, downloads, benchmarks and deleted fixtures. No end-to-end correctness
or performance claim is made. No binary was launched against a job.
