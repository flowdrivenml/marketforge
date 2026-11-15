# Normalization resource accounting

The authoritative CLI default remains `data/.config/historical/general.toml`
(`marketforge-normalize`'s `--config` argument). Existing worker, memory, scratch
and Parquet deployment values are unchanged. This checkout's `general.toml` is
ignored by `.gitignore:121`; its local limit table was updated without force-adding
or staging the user's configuration. Rust defaults and this document preserve the
same values in reviewable source. Runtime schema version remains 1.
Older version-1 TOML and legacy embedded resource documents acquire the explicit
Rust defaults below through serde defaults. Unknown fields still fail validation.
Protocol v3 inputs still contain neither resources nor integrity policy. The
existing resolved `processing-job.json` and SHA-256 provenance include the new
runtime limits; previously published provenance is not rewritten.

## Limits

All fields below are in `[resources.normalization_limits]`. All are positive
integers and must be safely addressable (`<= usize::MAX / 128`). Overflow in
footprint calculations is an error. Defaults are conservative application limits,
not benchmark measurements or performance tuning.

| Field | Default | Unit and scope | Enforcement |
|---|---:|---|---|
| `workbook_bytes` | 8,388,608 | bytes of the selected XLSX source/member | Incremental `read_bounded` in `process_xlsx_depth`; probe one additional byte at the exact boundary, never append it |
| `workbook_xml_bytes` | 16,777,216 | expanded bytes per worksheet/shared-string XML member; also cumulative worksheet cell text | Check each append during XML input; check cumulative materialized cell text before storing/cloning cells |
| `workbook_rows` | 25,000 | worksheet rows and decoded records; shared-string entries at most three times this | Check before row/record/string vector insertion; worksheet cells have the existing three-column shape |
| `record_bytes` | 1,048,576 | CSV raw payload bytes (excluding record terminators/blank lines), JSONL line bytes (including terminators); decoded XLSX cell/string text | CSV parser scanner before collecting parser input; JSONL before append; XLSX before accumulating cell text |
| `depth_levels` | 25,000 | combined bid+ask book or pending scalar snapshot; also extracted levels per array | Snapshot assembler before push, extraction before capacity allocation, snapshot book before maps, atomic batch before mutation, individual book insert before growth |
| `metric_entries` | 4,096 | sum of file/hourly/daily keys per task; canonical files per stream; row groups per open Parquet file/event index; reconstruction segments per stream | Before metric-map insertion, file creation, and Parquet `write` that could add footer entries |
| `reconciliation_bytes` | 67,108,864 | charged bytes per grouped/family execution unit | Before decoded source queues, event/identity buffers and ownership growth; reserved once per unit |
| `open_files` | 64 | handles of participating normalization operations in one session | Finite `ResourceBudget::with_file_limit`; eight handles reserved for session operations |

The 8 MiB workbook / 16 MiB XML / 25,000-row limits give the eager workbook
path a finite planning envelope alongside the existing 1 GiB deployment budget.
The 1 MiB record bound accommodates substantial multiline/array records while
keeping per-record expansion finite. A 25,000-level book limit leaves headroom
above the Bitget profiles' declared 500 levels per side without changing that
declaration. The metadata-entry quota bounds long-lived inventories independently
of record count; 64 handles leaves room above the current single worker's five
execution handles plus eight session handles. These are policy defaults, not
claims that every valid source will fit; adjust limits deliberately with admission
estimates, rather than increasing the overall memory budget to hide growth.

These limits feed estimates; they do not replace the overall memory budget.
Increasing a limit can make admission fail. Reaching a limit fails execution; no
book truncation, skipped levels, snapshot-depth reinterpretation, or record
truncation implements these constraints. Integrity policy cannot downgrade a
`ResourceLimit` into a recoverable malformed record.

Normalization establishes thread-local RAII limit scopes because shared book,
decoder and writer implementations also serve other workloads. Scopes are
restored after worker/stream execution. Unscoped shared callers retain their
existing behavior. Normalization workers called directly receive default limits;
config-aware task and stream entry points use the provided runtime limits.

## Reservation lifetimes

`resources::estimate_breakdown` exposes format, active-source bytes, writer bytes,
finalization bytes, open files and scratch. Accessibility checks still call
`fs::metadata`; its length no longer determines streaming memory admission.
`source_format` is shared by estimation and depth dispatch and rejects invalid
`workbook_sheets` values instead of treating mere key presence as XLSX.

An active streaming source allows for 1 MiB of input/decompression buffers,
64 times the bounded raw record size for decoded/transformation storage, and
32 MiB library/allocator safety allowance. Depth adds an allowance derived from
maximum book levels and concrete symbol/stream identity lengths, covering old/new
books, extracted/normalized levels, diffs, emitted outcomes and boundary captures.
These multipliers and node allowances are heuristics, not measured peaks.

XLSX adds the workbook byte bound, twelve times the expanded XML bound and
512 bytes per allowed row. The workbook, XML, shared strings, rows and decoded
records have overlapping lifetimes; records remain sorted by
`(timestamp, source_ordinal)`. Individual decoded JSON structures are additionally
constrained by bounded cell text and extracted-array counts.

The Parquet footprint calculation lives next to the owning writers in
`shared/persistence/parquet/footprint.rs`. It allows four times the row-group
memory target for retained native vector capacity, Arrow construction, encoding
and compression; footer allowance is 16 KiB per bounded metadata entry. Depth
adds complete-outcome overshoot (up to twice the book limit), three copies of the
segment inventory bounded by both the shared byte allowance and the metadata-entry quota, and 16 MiB for event-index batches/encoding.
This is an admission heuristic. The compressed output file target is not treated
as allocated memory. Writer behavior and output schemas remain unchanged.

A stream reserves one writer plus the **maximum** active-source requirement,
concrete source-span/result structure sizes, and an 8 MiB stream-finalization
allowance. Sequential archives do not multiply the writer/source working set.
The scheduler's reservation surrounds the entire stream call, including writer
finish, metadata persistence and cleanup.

Before workers start, a session reserves the worst permitted retained metrics
for all admitted tasks. Per-entry accounting includes statistics, scope/path
strings, and map/node overhead; existing bounded diagnostics use their declared
count/message-size constants. Six copies allow task reports, aggregate maps,
atomic merge copies, manifest/report ownership and serialization temporaries.
Depth adds 4 KiB per bounded level per source for finalization rereading initial
and terminal boundary artifacts alongside decoded boundary structures. Prepared raw-schema/normalization values are walked without serializing another
copy; their capacities, strings and container/node allowances are included for
three retained/provenance copies. These are allowances rather than live allocator
measurements.

Owned reservations remain in `ProcessingSessionTasks` after worker completion.
The job/session executors keep them through aggregation, integrity evaluation,
manifest/failure serialization and publication. Finalization separately reserves
16 MiB and bounded footer metadata, plus twelve times the segment allowance for depth (pretty JSON input, decoded vectors and initialization tuples), after workers stop.
Admission checks retained memory plus the greater of execution/finalization
requirements. No worker waits for retained-result growth while holding capacity:
metrics are capped, and the retained maximum is admitted upfront. Impossible
requests fail before entering a budget wait. A job whose cumulative retained
requirements cannot fit is rejected during preflight, without dropping other
eligible jobs' results.

## Handles and scratch

Execution requirements are three handles for trades and five for depth. This
covers the active source/archive reader, canonical writer, depth event index,
source-boundary writer and transient metadata output. Sequential sources do not
sum their handles. Eight session handles provide headroom for metadata,
provenance, failure/manifest persistence, logging and finalization. Admission
rejects requests that cannot fit alongside that headroom. No OS descriptor limit
is queried or raised. This is participating-engine accounting, not a machine-wide
file-descriptor ceiling; unrelated operations and independent processes do not
share the pool.

Active normalization uses callback-based plain/gzip/ZIP/TAR readers and in-memory
decoders. They do not create temporary files. Scratch reservation therefore
remains zero; existing scratch fields and shared APIs are retained. Scratch
settings do not constrain canonical Parquet staging output. Buffered compatibility
source helpers outside normalization remain unchanged; no spill or external sort
has been introduced.

## Failures and publication

`ResourceLimitError` carries resource, permitted/requested amount, stage and
available execution scope. It survives CSV/JSONL I/O wrappers and XLSX decoding.
Known source scopes include stream, task and input path. Stream setup and writer
finalization use `StreamFailure` with an honest stream/stage and nested original
error. Admission failures for a stream aggregate have no invented task ID.
The existing public `ProcessingFailure` error code is reused.

After the first source/writer error, a stream does not submit subsequent sources.
Earlier and available partial metrics remain. Later tasks have zero counters,
an explicit "not attempted" error and skipped progress status, preserving the
finalizer's identity/cardinality contract. Writer finalization/publication is
skipped for a failed source stream. Finalization errors also prevent dataset
commit. Recoverable staging artifacts remain. Failure persistence errors append
diagnostics without replacing the original processing error. Failure and
successful manifest persistence stream JSON through a buffered writer; integrity
map serialization borrows entries instead of cloning entire maps.

## Limits of the guarantees

Estimated/reserved memory, checked application growth and process RSS are
separate quantities. No estimate is a measured peak or a hard process-memory
ceiling. Allocator rounding, third-party codecs, Arrow/Parquet internals and
preexisting job/configuration objects remain outside exact allocation control.
Independent API invocations/processes do not automatically share a budget.

In zip 9.0.3, `ZipArchive::new` -> `with_config` -> `get_metadata` ->
`read_central_header` allocates `Vec::with_capacity(file_capacity)` and parses
central-directory metadata **before** application member/row hooks. Its `Config`
only exposes archive-offset selection. The XLSX input byte bound reduces exposure
but is not a proof against malicious metadata or codec allocation. The same
metadata issue exists for outer ZIP source readers; their filesystem length is
not a memory reservation. No claim of a complete adversarial ZIP memory bound is
made.

quick-xml `read_event_into`, `Event::xml_content` and entity unescaping can allocate
a token/text value before the cell-text hook. They operate on already bounded XML
members, so the XML bound applies, but the smaller per-cell limit is not a
pre-allocation cap on those dependency temporaries. `serde_json::from_str` for a
cell likewise allocates before the level-count hook, within bounded cell text.
Sorting/library scratch, compression windows and encoder memory use heuristic
allowances rather than allocator-level enforcement.

The old standalone `execute_trade_task` APIs accept policy and Parquet settings
rather than a runtime `ProcessingConfig`. Outside a config-aware admitted scope,
they now load the same default `data/.config/historical/general.toml` relative to
`.` as the CLI, retain their explicit Parquet writer settings, and reserve memory
and handles through writer finalization. Missing runtime configuration fails
explicitly. Embedders using a different root/config must use the config-aware
`execute_task` APIs; the legacy signatures cannot convey an alternate root. This
is a deliberate compatibility constraint rather than an unlimited fallback.
Low-level worker APIs with caller-owned sinks enforce default application limits
when unscoped, but leave admission ownership to their embedding caller.

## Verification scope

Only inspected synthetic unit/component tests and compilation are permitted for
this change. No normalization CLI job, historical archive workload, full test
suite, PostgreSQL test, deleted fixture recovery or benchmark was executed.
The accompanying change report records exact commands, test names and results.

## Change report and focused verification

Confirmed defects were archive-length streaming admission, XLSX dispatch/estimate
divergence, unbounded workbook/XML/record/depth/metric growth, unlimited handle
admission, results outliving execution reservations, post-failure stream submission,
and aggregate-admission attribution to the first task. The functions described
above now connect estimates and checks to the real worker/writer/session paths.
No profile, raw-schema definition, canonical schema, job wire contract, merge or
conversion algorithm, Python orchestration, PostgreSQL schema or benchmark code
was changed.

The following compilation commands passed (the release binary was not launched):

```sh
cargo check --manifest-path engine/Cargo.toml --features 'historical,monitor-tui'
cargo check --manifest-path engine/Cargo.toml --lib
cargo build --manifest-path engine/Cargo.toml --release --features 'historical,monitor-tui' --bin marketforge-normalize
```

Each selected test was inspected before execution. Commands below run only the
named component groups or exact cases; none invokes normalization execution on
market data. All final selected cases passed. An intermediate new test setup
incorrectly passed a versioned TOML document directly to `ProcessingConfig`; the
synthetic fixture adapter was corrected and the group passed on rerun.

```sh
cargo test --manifest-path engine/Cargo.toml --features historical --lib resource_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::resources::safety_tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib shared::io::bounded::tests -- --nocapture
cargo test --manifest-path engine/Cargo.toml --features historical --lib shared::historical::config::tests::normalization_limits_default_validate_and_serialize -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib shared::historical::config::tests::runtime_toml_loads_and_resolves_relative_scratch -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::config::job::protocol_tests::v3_example_deserializes_resolves_and_uses_global_runtime -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::config::job::protocol_tests::legacy_v1_v2_validate_embedded_fields_but_global_settings_win -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::metrics::windows::tests::scoped_integrity_json_roundtrip -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --lib historical::normalize::executor::failure_path_tests::unavailable_failure_report_preserves_definitive_failure -- --exact
cargo test --manifest-path engine/Cargo.toml --features historical --test depth_xlsx_decoder decodes_and_sorts_xlsx_snapshots -- --exact
```

Exact test names (28 distinct cases):

- `shared::persistence::parquet::footprint::resource_tests::footer_growth_checks_predicted_groups_before_writing`
- `shared::io::source::jsonl::resource_tests::jsonl_limit_is_checked_before_buffer_growth`
- `shared::io::decode::csv::csv_boundary_resource_tests::csv_payload_limit_ignores_blank_lines_and_crlf`
- `shared::io::decode::csv::resource_tests::csv_scanner_handles_chunked_fields_and_escaped_quotes`
- `shared::io::decode::csv::resource_tests::csv_limit_preserves_quoted_multiline_and_exact_boundaries`
- `shared::io::decode::xlsx::observation_resource_tests::resource_failure_never_becomes_malformed_row_observation`
- `shared::io::decode::xlsx::resource_tests::expanded_xml_and_materialized_rows_are_bounded`
- `historical::normalize::resources::safety_tests::depth_growth_fails_before_mutation`
- `historical::normalize::resources::safety_tests::bounded_metrics_preserve_existing_entries`
- `historical::normalize::resources::safety_tests::fatal_mock_source_stops_submission_without_losing_previous_metrics`
- `historical::normalize::resources::safety_tests::arithmetic_and_configuration_fail_explicitly`
- `historical::normalize::resources::safety_tests::retained_results_and_file_capacity_have_owned_lifetimes`
- `historical::normalize::resources::safety_tests::finite_file_capacity_rejects_concurrent_growth_without_waiting`
- `historical::normalize::resources::safety_tests::scheduler_holds_stream_capacity_during_mock_finalization`
- `historical::normalize::resources::safety_tests::mocked_budgeted_work_preserves_other_results_on_impossible_admission`
- `historical::normalize::resources::safety_tests::streaming_size_and_classification_are_shared`
- `historical::normalize::resources::safety_tests::retained_admission_rejects_before_starting_work`
- `historical::normalize::resources::safety_tests::sequential_sources_share_writer_memory_in_stream_estimate`
- `historical::normalize::resources::safety_tests::mocked_finalization_preserves_resource_error_metrics_and_skipped_cardinality`
- `shared::io::bounded::tests::boundary_probe_preserves_io_errors`
- `shared::io::bounded::tests::bounded_reads_distinguish_eof_and_boundary`
- `shared::historical::config::tests::normalization_limits_default_validate_and_serialize`
- `shared::historical::config::tests::runtime_toml_loads_and_resolves_relative_scratch`
- `historical::config::job::protocol_tests::v3_example_deserializes_resolves_and_uses_global_runtime`
- `historical::config::job::protocol_tests::legacy_v1_v2_validate_embedded_fields_but_global_settings_win`
- `historical::normalize::metrics::windows::tests::scoped_integrity_json_roundtrip`
- `historical::normalize::executor::failure_path_tests::unavailable_failure_report_preserves_definitive_failure`
- `decodes_and_sorts_xlsx_snapshots`

Targeted formatting used `rustfmt --edition 2024 --config skip_children=true`
and the same invocation with `--check` on the Rust files listed below.
`git diff --check -- engine/src data/.config/historical/general.toml docs/historical/normalization-resource-safety.md`
passed. Including `engine/Cargo.toml` in that whitespace check reports trailing
whitespace in existing user-edited dependency declarations; those unrelated
lines were preserved. The isolated XLSX integration-test target also compiled
conversion binaries and reported existing unused-function warnings there; no
conversion code was edited.

Shared callers: live depth integrity uses the shared `BookStore`; normalization
checks are inactive outside normalization scopes, including the extra atomic
capacity preflight. Shared decoder/book/writer contract tests remain unscoped
unless explicitly testing these limits. The shared budget's legacy constructor
continues to leave descriptor accounting disabled for unrelated callers.
Normalization explicitly uses its finite constructor. No benchmark compatibility
edits were needed.

Dependency/compatibility adjustments: added a direct optional `csv-core` dependency
and its historical feature activation so the limiting scanner uses the same CSV
grammar as `csv`. The lockfile adds that already-installed direct dependency.
Legacy trade entry points now require the default runtime TOML when called outside
a configured admission scope, as described above.

Modified/new files belonging to this change:

- `data/.config/historical/general.toml`
- `docs/historical/normalization-resource-safety.md`
- `engine/Cargo.lock`
- `engine/Cargo.toml`
- `engine/src/historical/normalize/executor.rs`
- `engine/src/historical/normalize/failure.rs`
- `engine/src/historical/normalize/manifest/persistence.rs`
- `engine/src/historical/normalize/metrics/windows.rs`
- `engine/src/historical/normalize/resources/classification.rs`
- `engine/src/historical/normalize/resources/estimate.rs`
- `engine/src/historical/normalize/resources/mod.rs`
- `engine/src/historical/normalize/resources/safety_tests.rs`
- `engine/src/historical/normalize/session.rs`
- `engine/src/historical/normalize/streams.rs`
- `engine/src/historical/normalize/streams_depth.rs`
- `engine/src/historical/normalize/task.rs`
- `engine/src/historical/normalize/worker/depth.rs`
- `engine/src/historical/normalize/worker/integrity.rs`
- `engine/src/historical/normalize/worker/mod.rs`
- `engine/src/historical/normalize/worker/trades.rs`
- `engine/src/shared/domain/book/batch.rs`
- `engine/src/shared/domain/book/snapshot.rs`
- `engine/src/shared/domain/book/store.rs`
- `engine/src/shared/error/mod.rs`
- `engine/src/shared/error/model.rs`
- `engine/src/shared/formats/depth/assembler.rs`
- `engine/src/shared/formats/depth/transforms.rs`
- `engine/src/shared/historical/budget.rs`
- `engine/src/shared/historical/config/tests.rs`
- `engine/src/shared/historical/limits.rs`
- `engine/src/shared/historical/mod.rs`
- `engine/src/shared/historical/resources.rs`
- `engine/src/shared/historical/runtime_validation.rs`
- `engine/src/shared/io/bounded.rs`
- `engine/src/shared/io/decode/csv.rs`
- `engine/src/shared/io/decode/xlsx.rs`
- `engine/src/shared/io/mod.rs`
- `engine/src/shared/io/source/jsonl.rs`
- `engine/src/shared/persistence/parquet/depth_writer.rs`
- `engine/src/shared/persistence/parquet/footprint.rs`
- `engine/src/shared/persistence/parquet/mod.rs`
- `engine/src/shared/persistence/parquet/trade_writer.rs`

The initial user changes in `.agents/`, the untracked `.agents/issues`, the
normalization JSON fixture and the existing manifest changes were preserved.
No full suite, end-to-end normalization, trade/depth pipeline, real archive,
PostgreSQL test, benchmark, dataset download, deleted fixture restoration,
destructive cleanup or broad repository formatting was run. These component
checks do not establish end-to-end normalization correctness, measured peak
RSS, a hard process-memory ceiling or any performance improvement.

Source reconciliation extends this model with a shared bounded group buffer and
sequential concrete writer routing. See [the current source/selection contract](normalization-source-selection.md)
for the schema-4 input, manifest-4 output and additional resource/accounting rules.
The verification record above describes the earlier resource-safety change.


Current disk-backed reconciliation supersedes this report's earlier unused-scratch
and cumulative-buffer description for grouped/family executions. See
[the disk reconciliation contract](normalization-disk-reconciliation.md).
Single concrete sources still stream without reconciliation scratch. This report's
original verification history is retained.
