# Disk-backed historical source reconciliation

> Current behavior: the explicit BYBIT-T1 source-local contract now enables verified chronological append for strictly disjoint accepted intervals. The unsupported numeric-ID rejection discussed below describes the earlier implementation. See [source-local trade append](source-local-trade-append.md).


Grouped/family normalization now retains bounded active reconciliation state and
spools complete canonical records to owned scratch files. A single concrete
source still uses its existing direct worker-to-Parquet streaming path. Prepared
schema4, standalone job3, source priority, selection, profiles and canonical
schemas are unchanged. No external coverage fields or pointer file is introduced.

## Execution

`reconciled.rs::execute` calls the existing workers with spooling sinks. Each
source is normalized forward once. Observations, rejected-record policies and
physical/source ordinals are collected as before. Worker failure stops further
normalization submissions. No archive-sized canonical vector remains in this
production path.

Trades use `reconcile/external.rs::trades`:

1. Canonical source spools become bounded identity-sorted runs. Each run uses a
   bounded leased chunk and in-place unstable sorting with a complete deterministic
   key. Run inventories are pass/count integers and deterministic owned filenames,
   not a vector containing every run path.
2. Fixed two-input multipass merging produces identity/precedence order. Keys
   retain exchange, concrete stream, instrument and native ID scope, followed by
   original priority, source index and source ordinal.
3. One authoritative first record is retained for the current identity group.
   Repeated records from that source are retained, including different payloads,
   exactly as in the reference. Lower-priority sources are suppressed and compared
   against that first record for duplicate/conflict counters. Missing IDs are not
   identities. Existing Binance/OKX and validated Bybit-UUID reliability rules are
   unchanged. Unsupported cross-source collisions fail even outside overlapping
   timestamp windows; unreliable/missing IDs in overlapping sources fail.
4. Survivors go to another spool. This is the bounded suppression representation:
   records discarded here cannot reappear. There is no in-memory suppression set
   or complete identity map.
5. Survivors become chronological sorted runs and undergo another bounded
   two-input multipass merge. Canonical publication reads that final spool directly
   into the existing `ParquetTradeWriter`, ordered by event timestamp, priority,
   source index and ordinal.

This uses a second external chronological sort rather than maintaining one cursor
per original source plus an external ordinal-suppression index. It adds scratch
I/O but keeps the resident algorithm and handle count small. Chronological merges
operate on sorted runs, not an unbounded fan-in over all archives. Sources with
regressions are rejected before either sort can conceal malformed chronology.
Identity winners are complete before any canonical trade publication, including
conflicts whose losing record has an earlier timestamp.

`reconcile_trades()` remains available under `cfg(test)` as the small-data
reference oracle; its existing behavioral tests remain. Production does not call
that archive-sized reference implementation.

Depth uses the same spool infrastructure for complete `DepthProcessingOutcome`
values and separate bounded source boundary records. After all sources normalize,
`depth_source_order()` and `validate_depth_recovery()` retain their conservative
checks. Overlapping independent books are not made reconcilable by disk storage.
Validated, disjoint source histories replay one event at a time through existing
`begin_source`, `write_outcome`, `write_boundary` and `finish` calls. Snapshots,
zero-change events, operations, sequence metadata, ordinals, initialization
segments, boundary states/fingerprints and event-index offsets are preserved.
No depth-level identity sort or cross-book splice was added.

## Family routing

`family.rs::route` still performs one physical CSV/JSONL decode per compatible
family source/member. Exact selection precedes full selected-instrument metadata
lookup. Selected rows go to per-concrete scratch descriptors rather than vectors.
CSV byte fields are lossless, including non-UTF8 values; JSON values retain the
existing arbitrary-precision representation. `worker/decoded.rs` exposes one-shot
incremental iterators consumed by the existing workers, retaining physical record
ordinals. Each concrete instrument keeps independent normalization/book state.

Routing opens only the active append handle. Each append is an independent zlib
member, so it does not retain a compressor or writer handle per symbol. This is
correct but introduces per-record compression/open overhead; performance was not
benchmarked. Consumed raw routing spools are deleted on iterator release,
including early worker failure. Empty candidates remain diagnostic candidates,
not emitted instrument streams. Existing missing-authority and unsupported-family
rules remain; no fixture metadata was fabricated.

## Temporary format and ownership

`reconcile/spool.rs` uses deterministic zlib-compressed framed JSON with magic
`MFSP0001`, checked little-endian record lengths, per-record FNV checksums and a
required record-count EOF footer. Zlib also validates member checksums. Canonical
decimals serialize as strings without precision loss; optional fields, source
index, priority and ordinal round-trip. Temporary files are not a public/resumable
format and are never reused across processes.

Zlib has a fixed header: unlike optional gzip names/comments, corruption cannot
trigger an unbounded header allocation before the application frame guard.
Readers check declared frame length before allocating, require exact reads,
validate checksums/footer/trailing data, and become terminal after errors. Tests
truncate each of the final eight compressed bytes independently. Serialization
first counts/checks length without a payload buffer, then serializes incrementally
through the compressor; there is no full-file or full-archive serialization.

Each execution creates an exclusive `normalize-spool-PID-counter` directory under
the configured scratch root. Only owned files/directories are removed. Consumed
merge inputs are deleted after their replacement is finalized; disk charges are
released after successful deletion. RAII cleanup removes remaining owned files
on ordinary success/failure before job finalization/publication. Cleanup errors go
to stderr without replacing the original error; failed deletions retain charges
while the pool is alive. Abrupt process termination cannot run RAII: abandoned
scratch directories require explicit operator cleanup and are not automatically
deleted by later runs.

## Resource controls

No runtime field, schema version or default was added or raised. The authoritative
file remains `data/.config/historical/general.toml` (ignored deployment file).
Effective configuration continues recording these existing fields:

| Field | Default | Scope and enforcement |
|---|---:|---|
| `normalization_limits.reconciliation_bytes` | 67,108,864 bytes | Resident reconciliation accounting per grouped/family execution unit; added once to stream memory admission |
| `scratch_budget_bytes` | 1,073,741,824 bytes in current deployment | Concurrent owned compressed scratch bytes per canonical scratch root in this process; checked atomically before every filesystem write |
| `normalization_limits.open_files` | 64 handles | Existing finite engine admission, including session headroom; grouped/family units now reserve eight participating handles |
| `normalization_limits.metric_entries` | 4,096 entries | Existing bounded source/metric inventories and canonical row-range attribution |

`Quota::reserve` returns reusable memory leases. Codec/cursor/chunk/event leases
release when their allocations are released. Source-equivalence keys and retained
row ownership remain cumulative retained charges. A scratch handle leases 384 KiB
before constructing a codec: installed miniz/flate2 buffers are fixed capacities,
including dictionary/hash/LZ/Huffman/output buffers; readers add an 8 KiB input
buffer. At most three scratch handles are open in one workspace (two merge inputs
and one output). Admission's eight-handle envelope includes source/Parquet and
auxiliary writer headroom. Sources/writers are sequential; more sources or runs
add merge passes rather than simultaneous handles.

Encoded frame length is bounded by `reconciliation_bytes / 128` (524,288 bytes at
the default). This is an additional explicit per-spooled-record/event constraint,
not an archive-size limit. Oversized events fail atomically before frame output;
existing raw record, workbook and depth-level limits also remain. Generic frame
reads lease `64 * encoded_length + 1,024` bytes before allocation for the encoded
payload and decoded structures. After canonical trade decode frees its JSON
payload, the retained lease shrinks to a footprint derived from fixed struct size
and actual string capacities, with vector/allocator allowance. Chunks flush at
one third of the resident quota, leaving room for codec buffers and transient
frame decoding; vector capacity is freed when a chunk is flushed. In-place sorting
does not allocate a second chunk. Very small budgets fail promptly if even fixed
codec/cursor state cannot fit.

Scratch writes use a process-shared pool keyed by canonical root, protected by a
mutex across capacity checking and the actual write. Short writes charge only
stored bytes. No worker waits for scratch growth while retaining other resources:
exhaustion returns a typed limit immediately. Concurrent units share capacity;
independent processes do not automatically share the pool. Scratch accounting
counts participating logical file bytes, not filesystem blocks, inode overhead,
unowned files, abandoned prior-process files, or machine-wide free space. The
shared `ResourceBudget`/`ScratchWorkspace` interfaces remain compatible. Source
estimates have zero static scratch reservation; grouped execution instead enforces
actual growth in the specialized scratch pool. Canonical staging is never scratch.

These are admission estimates and explicit application accounting, not a hard RSS
ceiling. Existing source decoder, reconstruction and Parquet footprint allowances
remain separately reserved. Library/allocator overhead, OS caches and diagnostic
serialization are not measured by the reconciliation quota. Per-task retained
metrics/session finalization reservations remain active. Memory depends on bounded
active buffers and admitted source metadata, not total historical record count;
scratch capacity and attribution limits can still reject large work explicitly.

## Metrics, failures and publication

Canonical row ranges grow incrementally during replay. Consecutive rows owned by
one source extend its last range; new ranges are checked against `metric_entries`
and resident capacity before growth. Existing manifest3/4 representation and
publication checks remain unchanged. Suppression counters are finalized after
identity resolution and before canonical replay, so later writer errors retain
known suppression decisions. Unwritten survivors remain `rows_unpublished`.

Mid-source spool failures retain the responsible task and available observations.
Stream-wide sort/read/reconciliation failures report stream scope. The existing
public `scratch_budget_exceeded` code now classifies typed scratch failures,
including wrapped stream errors; other resource categories remain typed internally
and use the existing processing-failure response. No extra reporting interface was
introduced. stdout remains machine-readable; accounted peak diagnostics use stderr.

Failure stops later canonical submissions, retains partial metrics and skipped
work, and prevents dataset commit. Failure reports/staging are preserved. The
existing session collection retains unrelated definitive job results. Scratch
cleanup happens before manifest/commit, and a later manifest/commit failure cannot
leave an active reconciliation workspace behind.

## Compatibility and limitations

No Python, live processing, merge/conversion algorithm, normalization profile,
raw schema, canonical Parquet schema or authoritative workload fixture was changed.
`DepthProcessingOutcome` only gains lossless serde derives for internal spooling;
its other callers retain existing behavior. Single-source normalization remains
streaming. Earlier manifest4 downstream merge restrictions remain unchanged.

Unsupported depth overlap, unreliable trade domains, rejected preferred identities,
unknown selected family metadata and unsupported member/identity formats still
fail explicitly. Source metadata and row-range limits remain finite. No arbitrary
order repair, automatic lower-priority fallback, process isolation, shared
cross-process scheduler, or unlimited dataset-size guarantee is claimed.

## Verification and acceptance status

The bounded disk-backed implementation is connected to production execution.
**The requested real trade success criterion remains unmet:** the archives reuse
numeric IDs outside the unchanged reliable Bybit identity domain. All other
selected checks below pass; no integrity guarantee was relaxed to force success.

The hardened real run normalized **3,822,020** trades with no rejected records,
validated all three sources as chronological, completed external identity sorting,
and then rejected a cross-source identity collision before canonical publication.
Observed per-source normalized counts were 1,034,450 (task76), 1,074,049 (task77)
and 1,713,521 (task78). These are observed processing counts, not output rows.
The report retains all those rows as unpublished. No committed dataset or
successful manifest exists; owned scratch is empty after failure. Staging and the
existing `failure.json` remain.

A bounded first-three-record inspection of the intended Bybit spot trade sources
confirmed the native CSV header is `id,timestamp,price,volume,side,rpi` and IDs
`1`, `2`, `3` recur across all three sources. In particular, native ID `1` has
millisecond timestamps 1793059200377, 1793145600070 and 1793232000117, respectively.
No UUID identity column exists in those CSV headers. This observation does not
establish a reset domain: calendar/source boundaries remain insufficient under
the requested rules. The unchanged reference oracle independently rejects the
same disjoint-source numeric-ID pattern, now covered by an explicit regression.
Making this workload publish successfully requires authoritative identity-domain
support/data or a separate change to the required identity contract. Neither was
invented in this refactor; the real test assertions and fixture are unchanged.

The real run exposed a completion-counter defect: worker completion could remain
counted after downstream stream failure. It was fixed afterward and verified with
synthetic failure tests. Failure now clears affected completion counts, marks a
definitive failed execution, distinguishes not-attempted canonical work, and
preserves already completed other concrete streams. Error details now include a
bounded native-ID sample and both known source/task identities/timestamps, with
honest stream scope rather than an invented single failing task. The captured
real failure artifact is preserved as originally emitted, not rewritten.

### Resource evidence

| Evidence | Accounted resident peak | Scratch bytes | Scratch handles | Meaning |
|---|---:|---:|---:|---|
| Hardened real trade run | 22,764,522 bytes | 346,264,991 bytes | 3 | Exact maxima of application charges / actual participating compressed writes through identity reconciliation, before the required semantic rejection |
| 6,000-record repeated-identity synthetic case, 2 MiB budget | 1,208,332 bytes | bounded by 8 MiB synthetic scratch capacity | 3 | Multipass identity and chronological merge complete; one identity group is processed incrementally |
| 40,000-record generic spool test, 2 MiB budget | below 2 MiB | below 1 MiB test disk capacity | bounded | Input serialized bytes exceed the resident budget; incremental write/read and lease release asserted |
| 5,000-record family routing, 1 MiB budget | below 1 MiB | enforced by unchanged runtime scratch capacity | bounded | The old cumulative decoded-queue allowance would be exceeded; selected records remain disk-backed |

The real process's largest sampled Linux `VmHWM` was **18,680 KiB**, sampled every
five seconds. This is OS process information, not the reconciliation quota, and
sampling does not establish its final whole-run peak. Accounted resident values
are conservative charges, not measured allocator usage. Scratch figures count
logical participating compressed file bytes, not allocated filesystem blocks.
The real failure occurs before chronological publication, so its numbers are not
claimed to be a successful full-dataset finalization peak. Defaults remained
64 MiB reconciliation and 1 GiB scratch throughout.

### Real depth regression

`writes_real_bybit_depth_archive_to_parquet` passed, retaining the prior observed
824,909 source messages, 11,041,886 canonical L2 rows, 18 Parquet files, one
reconstruction segment, 400-level initial snapshot, Verified continuity, and zero
sequence gaps/duplicates/backward sequences. Existing source-event index coverage,
row offsets, ordinals and initial/final fingerprint checks passed. This is the
existing single-source streaming regression. New multisource disk replay is
verified by synthetic production-executor tests covering atomic initialization,
zero-change events, reverse request priority, disjoint chronology, source-local
ordinals, recovery segments after an explicitly allowed synthetic gap, missing
snapshots and unsafe overlaps. No real multisource depth success is claimed.

### Commands and results

All **67 component tests** and **three focused integration regressions** passed,
as did the named real depth test (71 distinct passing tests). Selected tests were
inspected before execution. Exact focused commands:


```sh
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconcile::spool::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconcile::external::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconcile::disk_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconcile::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::family::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconciled::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::reconciled::failure_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::resources::safety_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::config::batch::selection_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::manifest::validation::reconciliation_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::worker::decoded::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_normalize resource_file_budget::reserves_and_releases_open_files -- --exact --nocapture --test-threads=1
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_normalize resource_file_budget::rejects_requirements_exceeding_capacity -- --exact --nocapture --test-threads=1
cargo test --manifest-path engine/Cargo.toml --features historical --test historical_normalize process_scheduler::preserves_individual_task_errors -- --exact --nocapture --test-threads=1
```

Real commands:

```sh
# Requested default-profile command: earlier development prototype interrupted
# after the hardened release run established the definitive identity rejection.
cargo test --manifest-path engine/Cargo.toml --features "historical,historical-real-tests" --test historical_normalize process_executor::processes_multiple_trade_tasks_into_staging -- --exact --nocapture --test-threads=1

# Hardened implementation: completed, FAILED for the required identity-domain
# rejection described above; no memory/scratch budget failure.
cargo test --manifest-path engine/Cargo.toml --release --features "historical,historical-real-tests" --test historical_normalize process_executor::processes_multiple_trade_tasks_into_staging -- --exact --nocapture --test-threads=1

# Existing real depth regression: PASSED.
cargo test --manifest-path engine/Cargo.toml --features "historical,historical-real-tests" --test historical_normalize parquet_depth_real_archive::writes_real_bybit_depth_archive_to_parquet -- --exact --nocapture --test-threads=1
```

The interrupted prototype used an earlier conservative generic trade lease and
gzip spool format. Its only owned temporary spool directory was explicitly
removed after process exit; staging/test metadata and an interruption note were
retained. It is not counted as a passing test. The completed hardened real run
used zlib and the shrinking canonical trade leases. Later counter/error-detail
cleanup was validated synthetically; the known semantic rejection was not hidden
by rerunning or weakening the real workload. Test-run metadata remains `running`
when the existing integration test panics before `run.finish`; the preserved
failure report and captured test exit are definitive.

Compilation passed after the final implementation changes, without new warnings:

```sh
cargo check --manifest-path engine/Cargo.toml --features "historical,monitor-tui"
cargo check --manifest-path engine/Cargo.toml --lib
cargo build --manifest-path engine/Cargo.toml --release --features "historical,monitor-tui" --bin marketforge-normalize
```

Real/integration target compilation emitted existing unused-import/dead-code
warnings in test helpers and five dead-code warnings in `marketforge-convert.rs`;
those unrelated modules were not changed. The release binary was compiled, not
launched against a CLI job. No full suites, benchmarks, PostgreSQL, downloads,
restored retained-case archives, or 357-source fixture execution ran.

Targeted rustfmt (`--edition 2024 --config skip_children=true --check` on the Rust
files listed below) and `git diff --check -- engine/src docs/historical` passed.

Individual focused passing test names (the real depth test is listed separately):

```text
historical::normalize::reconcile::spool::tests::scratch_handles_and_reader_length_checks_fail_promptly
historical::normalize::reconcile::spool::tests::consuming_iterator_closes_before_releasing_disk_and_cleans_early_exit
historical::normalize::reconcile::spool::tests::concurrent_workspace_disk_capacity_and_owned_cleanup
historical::normalize::reconcile::spool::tests::oversized_corrupted_and_truncated_frames_fail
historical::normalize::reconcile::spool::tests::canonical_trade_roundtrip_preserves_every_optional_field_and_decimal_scale
historical::normalize::reconcile::spool::tests::incremental_roundtrip_larger_than_memory_and_reservations_release
historical::normalize::reconcile::external::tests::source_local_numeric_bybit_ids_fail_the_unchanged_reference_too
historical::normalize::reconcile::external::tests::unique_events_inside_overlap_and_later_tail_survive_exactly
historical::normalize::reconcile::external::tests::identity_domain_and_missing_id_failures_match_reference
historical::normalize::reconcile::external::tests::precedence_conflicts_equal_times_and_same_source_duplicates_match_oracle
historical::normalize::reconcile::external::tests::frequent_identity_multipass_and_small_resident_budget
historical::normalize::reconcile::disk_tests::writer_failure_keeps_definitive_suppression_and_unpublished_survivors
historical::normalize::reconcile::disk_tests::scratch_exhaustion_does_not_publish_or_lose_source_failure
historical::normalize::reconcile::disk_tests::bybit_numeric_identity_reuse_across_disjoint_sources_fails_without_guessing_a_domain
historical::normalize::reconcile::disk_tests::depth_recovery_after_gap_replays_authoritative_snapshot_segments
historical::normalize::reconcile::disk_tests::trades_priority_before_chronology_publish_exact_rows_and_suppression
historical::normalize::reconcile::disk_tests::depth_replay_keeps_snapshots_zero_change_events_boundaries_and_segments
historical::normalize::reconcile::disk_tests::depth_missing_initial_snapshot_and_ambiguous_overlap_never_publish
historical::normalize::reconcile::tests::depth_recovery_never_fabricates_synchronization
historical::normalize::reconcile::tests::quota_rejects_before_growth_and_overflow_is_explicit
historical::normalize::reconcile::tests::depth_uses_observed_chronology_and_rejects_ambiguous_equal_time_books
historical::normalize::reconcile::tests::observed_first_last_and_min_max_are_not_invented
historical::normalize::reconcile::tests::identities_are_instrument_exchange_and_stream_scoped_and_within_source_is_unchanged
historical::normalize::reconcile::tests::priority_wins_before_emission_even_if_loser_has_an_earlier_timestamp
historical::normalize::reconcile::tests::overlap_keeps_unique_records_equal_timestamp_ids_and_later_tail
historical::normalize::reconcile::tests::missing_id_overlap_and_unknown_reset_domains_fail_explicitly
historical::normalize::reconcile::tests::mocked_jobs_preserve_definitive_results_after_another_reconciliation_fails
historical::normalize::reconcile::tests::scheduling_and_completion_order_do_not_change_winners
historical::normalize::family::tests::include_filters_unknown_unselected_symbols_before_contract_lookup
historical::normalize::family::tests::redundant_equivalent_family_selection_reads_first_only
historical::normalize::family::tests::jsonl_depth_routes_interleaved_instruments_without_a_shared_timestamp_domain
historical::normalize::family::tests::one_mixed_decode_and_bounded_queues_replace_per_symbol_archive_reads
historical::normalize::family::tests::changing_monthly_population_is_routed_once_with_per_source_absence
historical::normalize::family::tests::degradable_trade_csv_parse_errors_remain_bounded_file_diagnostics
historical::normalize::family::tests::csv_routing_uses_the_existing_authoritative_identity_column_precedence
historical::normalize::family::tests::never_observed_include_is_unsatisfied_but_absence_in_one_source_is_allowed
historical::normalize::family::tests::all_never_hides_encountered_missing_or_unsupported_metadata
historical::normalize::family::tests::routed_population_larger_than_old_queue_budget_stays_disk_backed
historical::normalize::reconciled::tests::snapshot_outcomes_are_atomic_and_checked_before_buffer_growth
historical::normalize::reconciled::tests::source_equivalence_includes_selection_member_and_interpretation
historical::normalize::reconciled::failure_tests::earlier_skipped_member_does_not_replace_responsible_resource_failure
historical::normalize::resources::safety_tests::fatal_mock_source_stops_submission_without_losing_previous_metrics
historical::normalize::resources::safety_tests::bounded_metrics_preserve_existing_entries
historical::normalize::resources::safety_tests::depth_growth_fails_before_mutation
historical::normalize::resources::safety_tests::retained_results_and_file_capacity_have_owned_lifetimes
historical::normalize::resources::safety_tests::finite_file_capacity_rejects_concurrent_growth_without_waiting
historical::normalize::resources::safety_tests::streaming_size_and_classification_are_shared
historical::normalize::resources::safety_tests::arithmetic_and_configuration_fail_explicitly
historical::normalize::resources::safety_tests::retained_admission_rejects_before_starting_work
historical::normalize::resources::safety_tests::mocked_finalization_preserves_resource_error_metrics_and_skipped_cardinality
historical::normalize::resources::safety_tests::mocked_budgeted_work_preserves_other_results_on_impossible_admission
historical::normalize::resources::safety_tests::scheduler_holds_stream_capacity_during_mock_finalization
historical::normalize::resources::safety_tests::sequential_sources_share_writer_memory_in_stream_estimate
historical::config::batch::selection_tests::single_symbol_omission_is_compatible_and_all_is_not_ambiguous
historical::config::batch::selection_tests::strict_selection_modes_and_retired_fields
historical::config::batch::selection_tests::active_fixture_contract_has_no_bounds_and_explicit_family_intent
historical::config::batch::selection_tests::family_expansion_cannot_allocate_an_unadmitted_fanout
historical::config::batch::selection_tests::explicit_concrete_ranks_are_preserved_when_family_expands
historical::config::batch::selection_tests::family_reservation_counts_one_shared_buffer_and_one_active_writer
historical::config::batch::selection_tests::metadata_is_deferred_for_unencountered_family_candidates
historical::config::batch::selection_tests::expansion_preserves_original_priority_and_is_deterministic
historical::normalize::manifest::validation::reconciliation_tests::suppression_and_unprocessed_rows_are_not_parse_failures
historical::normalize::manifest::validation::reconciliation_tests::unobserved_family_candidates_need_no_empty_output_stream
historical::normalize::manifest::validation::reconciliation_tests::interleaved_ownership_requires_version_four_and_complete_disjoint_coverage
historical::normalize::manifest::validation::reconciliation_tests::legacy_contiguous_spans_still_validate_without_new_fields
historical::normalize::worker::decoded::tests::one_shot_inputs_preserve_physical_ordinals_without_reopening
historical::normalize::worker::decoded::tests::scratch_io_errors_retain_their_engine_category_through_csv
resource_file_budget::reserves_and_releases_open_files
resource_file_budget::rejects_requirements_exceeding_capacity
process_scheduler::preserves_individual_task_errors
```

## Files changed by this refactor

Created:

- `engine/src/historical/normalize/reconcile/spool.rs`
- `engine/src/historical/normalize/reconcile/external.rs`
- `engine/src/historical/normalize/reconcile/disk_tests.rs`
- `docs/historical/normalization-disk-reconciliation.md`

Modified:

- `engine/src/historical/normalize/reconcile.rs`: reusable leases, reference oracle
  retained for tests, serializable canonical trade spool entries, submodules.
- `engine/src/historical/normalize/reconciled.rs`: spooling sinks, external trade
  phases, depth replay, incremental attribution, failure/status accounting.
- `engine/src/historical/normalize/family.rs`, `worker/decoded.rs`: disk routing and
  incremental consumed inputs with close-before-unlink cleanup.
- `engine/src/historical/normalize/worker/{trades.rs,depth.rs,integrity.rs}`:
  incremental routed-record iteration and original scratch-error preservation.
- `engine/src/historical/normalize/streams.rs`: eight-handle grouped reservation;
  resident reconciliation allowance and original single-source streaming retained.
- `engine/src/historical/normalize/executor.rs`: existing scratch-budget public
  code applied to typed scratch errors, including stream wrappers.
- `engine/src/shared/domain/canonical/depth.rs`: serde derives only; shared/live
  depth algorithms and canonical meaning unchanged.
- `engine/src/historical/normalize/{reconcile_tests.rs,family_tests.rs}`:
  reference helper visibility and storage-aware assertions preserving their
  original behavioral expectations; added bounded routing regression.
- `engine/src/historical/normalize/resources/safety_tests.rs` and
  `engine/src/historical/config/batch/selection_tests.rs`: expected grouped handle
  requirement updated to eight for real spool/merge handles.
- `docs/historical/normalization-source-selection.md`: current execution/resource
  model; earlier resource/source-selection reports receive supersession links.
- Ignored local `data/.config/historical/general.toml`: explanatory comment only;
  every numeric resource/deployment value preserved.

Existing uncommitted/untracked user, agent, fixture and prior-task changes were
preserved. No dependency, benchmark implementation, raw archive, authoritative
fixture, profile, schema, Python, merge/conversion or live algorithm change was
needed. No commit or staging operation was performed.
