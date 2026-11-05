# Historical normalization runtime and Protocol v3

## Configuration ownership

```text
data/.config/historical/
├── normalize.toml
├── profiles/
│   ├── trades/
│   └── depth/
└── raw_schemas/
    ├── trades/
    └── depth/
```

`normalize.toml` controls execution: workers, memory and scratch budgets, scratch
path, Parquet targets, and integrity policies. Profiles define market-data
interpretation and source ordering. Raw schemas describe physical archive/record
structure. Protocol v3 job JSON selects inputs, instrument metadata, profiles,
streams, ordering, and output destinations. The retained PostgreSQL exports and
provenance reports are historical documentation, never runtime dependencies.

The former global `data/.jobs/processing.json` is absent in this checkout. No
historical deployment limits or policies could be recovered as authoritative.
The new file therefore explicitly documents starting defaults: one worker,
1 GiB memory, 1 GiB scratch, `data/.work/normalize` scratch path, 64 MiB Parquet
row groups, and 256 MiB Parquet file targets. Integrity is enabled; all seven
categories use `action = "fail"`, `max_count = 0`. These are conservative starting
settings, not an exact migration or workload sizing recommendation. Existing
legacy jobs were not treated as the missing global configuration.

The TOML envelope requires `schema_version = 1`, `resources`, and
`integrity_policy`. There is no fallback to JSON or automatic missing-file
creation. Unknown keys are rejected throughout resources, Parquet, and integrity
policy tables. Resource fields must be present, numerical limits positive,
scratch path nonempty, and row-group target no larger than file target. An
existing scratch path must be a directory. Existing integrity validation is
shared with legacy-job validation: rates must be finite and in [0,1], and degrade
rules need a count, rate, or window threshold. Policy categories remain required;
existing optional defaults for `enabled`, thresholds, and windows remain intact.
Errors include the configuration filename and relevant field/table.

Optional integrity rule fields retain their existing meanings: `max_count`
(unsigned integer), `max_rate` (float), `minimum_samples` (unsigned integer,
default 0), and `windows.daily` / `windows.hourly` tables, each requiring
`max_rate` and `minimum_samples`. Actions are `fail` or `degrade`. Enforcement
mathematics and behavior when integrity is disabled are unchanged.

## Invocation and paths

```sh
.bin/marketforge-normalize \
  --project-root /path/to/marketforge \
  --config data/.config/historical/normalize.toml \
  --job data/.jobs/normalize/1001.json
```

`--config` is optional and defaults to the path above. `--project-root` defaults
to `.` and must identify an existing directory; use an explicit absolute root
when launching from an arbitrary working directory. `--jobs-dir DIRECTORY`
replaces `--job FILE` for a sorted, non-recursive list of `.json` job files.
Exactly one input mode is required. An empty job directory fails. Existing
`--output-root DIRECTORY` and feature-gated `--tui` behavior remain supported.

Configuration, job/job-directory, input, scratch, staging, final dataset, and
output-redirection paths resolve relative to the canonical project root, never
the job's directory or the launcher's working directory. Absolute paths remain
absolute without prefixing. Loading checks documents and metadata; it does not
open inputs or create scratch/staging/output directories. Source accessibility
and publication existence checks remain in execution.

One catalog is loaded explicitly from
`PROJECT_ROOT/data/.config/historical/{profiles,raw_schemas}` and reused for all
jobs in the invocation. Runtime configuration may live elsewhere via `--config`;
that does not change catalog selection. The CLI does not use environment-based
catalog overrides, Cargo's build-time manifest directory, process-wide environment
mutation, or CWD changes. Copying the release executable into `.bin/` does not
change this resolution.

## Protocol v3 orchestration contract

The complete structural schema is [normalize-job-v3.schema.json](normalize-job-v3.schema.json).
The complete [example job](normalize-job-v3.example.json) uses the supported
`bybit.spot.spot.bybit-t1.v1` profile. A Rust unit test deserializes that exact
file and resolves it using a relocated temporary catalog, without any source
archive. It is documentation, not a runnable historical fixture.

All top-level fields are required:

| Field | Wire value |
|---|---|
| `protocol_version` | Integer `3` |
| `job_id`, `dataset_id` | Unsigned 64-bit integers |
| `operation` | `"normalize"` |
| `tasks` | Nonempty array of profile tasks |
| `streams` | Nonempty array of stream definitions |
| `ordering` | `{"primary":"event_timestamp_ns","tie_break":"stream_rank"}` |
| `output` | Content type and staging/final destination paths |

Each task requires `task_id` (unsigned 64-bit), `stream_id`, `input_path`,
`format_profile`, `exchange`, `instrument_id` (positive signed 64-bit), `symbol`
(nonblank), and `instrument`. The stream ID is an opaque string used for exact
reference matching, not parsed as a profile identity. Tasks within a job must
have unique IDs. Stream definitions require `stream_id` and `stream_rank`
(unsigned 32-bit); IDs and ranks must each be unique, and every task must reference
a declared stream. Rank 0 is valid.

Optional task fields, omitted or null:

- `profile_fingerprint`: exact SHA-256 of the current compiled profile. Omission
  resolves the current catalog; a supplied stale value fails. Production
  orchestration can pin this for reproducibility.
- `archive_member`: archive member selector retained by the existing readers.
- `source_compression`: `plain`, `gzip`, `zip`, or `tar_gzip`. If omitted, the
  profile must permit exactly one container; otherwise selection is required.
  A supplied container must be allowed by that profile.

Exchanges are `bybit`, `binance`, `okx`, `bitget`, and `gateio`. A profile ID is
`exchange.market_category.instrument_type.format_code_lowercase.v1` (including
both `spot.spot` dimensions). Selection must match declared exchange/instrument
metadata. Unknown and unsupported profiles fail; no option/inverse support was
added.

`instrument` requires `instrument_kind` (`spot`, `perpetual`, `future`, `option`)
and a positive decimal `tick_size`. Decimal strings are recommended to preserve
precision; the existing Rust decimal deserializer also accepts JSON numbers.
Optional fields are `contract_kind` (`linear`, `inverse`, or null),
`contract_value` (decimal or null), and `contract_value_asset` (string or null).
They retain the existing consistency checks. Contract-quantity profiles require
authoritative positive contract value and a nonblank denomination asset; Python
must obtain these from instrument metadata, never illustrative historical notes.

`output` requires `content_type` (`trades` or `depth`), `staging_path`, and
`dataset_path`. Task normalizations must match the output type. Combined output
is not supported. Paths must be nonempty and staging/final paths distinct.
Existing final/staging directories are not overwritten during execution; dataset
publication remains the existing same-filesystem atomic rename.

Unknown v3 fields are rejected at every object boundary, including nested
instrument/output/ordering/stream objects. In particular `resources` and
`integrity_policy` are errors, not ignored settings. The JSON schema describes
structure; Rust additionally validates catalog references, fingerprints,
instrument consistency, normalization targets, stream references, and paths.

## Internal conversion and legacy compatibility

Protocol v3 has a dedicated wire type and operation enum. Conversion maps
`normalize` to the existing internal `ProcessingOperation::Process`, resolves
profile rules/schema/fingerprints, and supplies the loaded global resources and
integrity policy to `ProcessingJob`. The internal protocol remains version 1.
Neither execution-report nor manifest versions changed; merge/conversion
operation contracts are unchanged.

Protocol v1 inline jobs and v2 profile jobs keep their existing JSON shape and
`operation: "process"`. Their resource and integrity fields remain required and
are validated before being replaced with global settings. They cannot override
`normalize.toml`. Existing profile compatibility, unsupported-profile rejection,
and v1/v2 provenance/fingerprint checks remain active. Original job documents are
not rewritten.

Rust callers should use `load_normalization_job(path, catalog, runtime, root)` for
the explicit v1/v2/v3 boundary. Older context-free `load_document`,
`load_document_with_catalog`, and `load_processing_job` remain legacy v1/v2
loaders; v3 reports that explicit runtime configuration is required. They do not
fabricate defaults. Executing an internal job always requires `ProcessingConfig`.

Both single-job and session scheduling reserve budgets and choose workers from
`ProcessingConfig`; workers, Parquet writers, immediate integrity checks, and
final policy evaluation use the same configuration. Entry points validate both
resources and policy. The resolved `processing-job.json` snapshot continues to
contain the exact authoritative execution settings, with its existing checksum;
that internal provenance artifact is intentionally different from the slim wire
request. Runtime settings do not enter profile fingerprints.

Current normalization resource estimates reserve zero scratch bytes, and the
existing `ScratchWorkspace` helper is not instantiated by these paths. The global
scratch budget still feeds the scheduler; the configured path is resolved and
recorded. This migration introduces no scratch allocation or scheduling redesign.

## Results and failures

Stdout remains JSON: one result object for `--job`, or an array of result objects
for `--jobs-dir`. A result contains:

```json
{
  "protocol_version": 1,
  "job_id": 1001,
  "dataset_id": 501,
  "status": "complete",
  "manifest_path": "/project/data/normalized/501/manifest.json",
  "events_written": 0,
  "files_written": 0,
  "start_timestamp_ns": null,
  "end_timestamp_ns": null,
  "failure": null
}
```

This is a result-shape illustration; no job was executed. The manifest filename
remains `manifest.json` (manifest protocol version 2). `status` is `complete` or `failed`. Failure objects
contain `code` and `message`; codes remain `invalid_job`, `missing_source`,
`corrupt_source`, `decompression_failure`, `scratch_budget_exceeded`,
`processing_failure`, or `output_failure`. Counts are unsigned 64-bit and optional
timestamps signed 64-bit nanoseconds. Diagnostics/progress remain on stderr/TUI.

CLI/configuration/job-loading errors return nonzero with a diagnostic; no result
JSON is guaranteed on those errors. The existing executor can return a failed
result as successful Rust return data; therefore orchestration must inspect
`status` and `failure` even when the process exits zero. Publication, failure
reports, metrics, Parquet schemas, and normalization algorithms are unchanged.

## Verification scope

Only configuration/protocol/path tests and builds are used for this migration.
No engine execution, real archives, end-to-end tests, databases, benchmarks,
removed fixtures, or published datasets are required. Passing these checks does
not verify runtime normalization correctness. Python orchestration changes are
still a separate task; it must adopt this contract and stop emitting job-level
resources/policies for v3.

### Implementation and checks

Created `data/.config/historical/normalize.toml`, the v3 schema/example, runtime
configuration tests, protocol tests, and `job/runtime_validation.rs`. Updated
`marketforge-normalize.rs`, historical `config/{job,mod}.rs`,
`job/{mod,resources,validate}.rs`, `normalize/{config,executor,mod,session}.rs`,
this documentation, the catalog documentation link, and narrow `.gitignore`
exceptions for the new configuration and contract artifacts. No files were
removed by this migration; pre-existing deletions were preserved.

Executed only these filtered test groups:

```sh
cargo test --manifest-path engine/Cargo.toml --features "historical,monitor-tui" --lib historical::normalize::config::tests
cargo test --manifest-path engine/Cargo.toml --features "historical,monitor-tui" --lib historical::config::job::protocol_tests
cargo test --manifest-path engine/Cargo.toml --features "historical,monitor-tui" --bin marketforge-normalize runtime_cli_tests
cargo test --manifest-path engine/Cargo.toml --features "historical,monitor-tui" --lib historical::config::catalog::tests
cargo test --manifest-path engine/Cargo.toml --features "historical,monitor-tui" --lib historical::config::job::catalog_fingerprint_tests
```

All 32 focused tests passed (4 runtime, 5 wire/legacy, 1 CLI/directory, 21 catalog,
1 job provenance). The catalog audit again reports zero unapproved differences;
all profile/raw-schema bytes match the preceding reconciliation baseline.
Targeted `rustfmt --check` and `git diff --check` passed. Both requested builds
passed:

```sh
cargo check --manifest-path engine/Cargo.toml --features "historical,monitor-tui"
cargo build --manifest-path engine/Cargo.toml --release --features "historical,monitor-tui" --bin marketforge-normalize
```

A default-feature `cargo check --lib` also passed, confirming the extracted
validators do not introduce a historical-feature dependency into the job types.
Full suites, missing-fixture tests, normalization pipelines, and benchmark
executables were intentionally skipped.
