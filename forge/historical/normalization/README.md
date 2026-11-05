# Historical normalization preparation

Forge prepares JSON only: a versioned Forge batch containing Rust protocol-v3 jobs. It never launches the Rust engine or writes
job, dataset, lineage or pending records. Testing IDs are collision checked but
remain unreserved.

```sh
mf normalize new engine_test --from-acquisition engine_test
mf normalize validate engine_test
mf normalize prepare engine_test --testing --dry-run
mf normalize prepare engine_test --testing
# Create, open in the configured editor, validate, and prepare in one command:
mf normalize prepare another_name --testing --from-acquisition engine_test
```

`new` creates an acquisition-shaped TOML in
`data/.config/historical/normalization/`, opens it through the shared editor helper,
and validates after a successful editor exit. It refuses to overwrite or shadow an
existing legacy request. A source recipe is copied intact, including scope and feed
options. Without a source, the acquisition template clearly asks for editing.

Recipes use acquisition's strict `version`, `[interval]`, and `[[requests]]` models.
Only completed local archives in the configured raw root are selected, using the
full exchange/market/instrument-type/scope/data-type/feed identity and half-open
interval overlap. Dates select whole archives; the report records requested dates,
actual full bounds, and nominal coverage gaps. Missing coverage or mappings makes
the batch partial and returns a nonzero CLI status while preserving successful jobs.

One embedded job is produced per engine output content type (`trades` or `depth`),
combining all resolved instruments. Depth feed variants get distinct stream IDs
with a `.snapshot` or `.incremental` suffix where registered. Repeated
archive/member selections are deduplicated; daily archives become multiple tasks
with stable sequential task IDs. `order_book_l2` becomes engine `depth`. Profiles,
container signatures and single regular ZIP/TAR members are resolved conservatively
and passed through the existing manual-input validator. Multiple compatible profiles,
multiple members, missing authoritative instrument specifications, or unsupported
family-to-member mappings are blockers. Archives are inspected without extraction.
Rust remains the only format compiler/interpreter.

The latest published turnover snapshot supplies ordering evidence. Ranked streams
come first by stored position, ties and unranked streams by native stream ID. Ranks
are then dense `1..N` inside each job; a single stream gets rank `1`. No snapshot is
required. The selected snapshot ID is frozen in the batch diagnostics.

Dry-run and publication use the same planner. Dry-run creates no run or output.
Publication atomically exposes exactly one file:
`data/runs/<run_id>/normalize-batch.json`. Its Forge `schema_version = 1`,
`kind = "normalization_batch"`, `run_id`, `status`, and `jobs` array describe a
preparation run. `jobs` contains complete unchanged Rust v3 objects, one per
resolved content type. `job_summaries`, `requests`, coverage gaps, blockers,
whole-archive intervals and ranking evidence are in the same file. Successfully
resolved tasks remain included when other inputs are blocked; status is partial.
No per-job files or separate report are written.

The wrapper is a Forge artifact and cannot be passed directly to Rust `--job`.
Rust accepts an individual embedded v3 object. Engine dispatch is deferred, and
preparation does not print an executable invocation for the wrapper. Engine
readiness is reported separately. Output paths default to unused sibling staging
and final directories in `data/test/normalized/`. Each embedded job gets distinct
collision-checked job/dataset IDs; registered IDs, legacy jobs, embedded jobs in
existing batches and dataset/staging directories are checked. IDs are not reserved.

## Legacy manual requests

Existing `data/.config/historical/normalization_jobs/NAME.toml` requests remain
readable, as do manual requests in the new library. They retain explicit job/dataset
IDs, input selections, optional output paths, and stream ranks. Single manual
streams still default to rank zero; recipe-generated jobs use rank one. Legacy
jobs continue to be written atomically without replacement to
`data/.jobs/normalize/JOB.json`. Their preparation supports `--dry-run` too.

All resolution uses the guarded testing connection and a repeatable-read, read-only
transaction. Prepared run jobs are also visible to storage's concrete dependency
checks. Runtime resources and integrity policy remain in `historical/normalize.toml`.

## Verified wire boundary

`contract.py` isolates serialization, based on current Rust sources:

- `engine/src/historical/config/job.rs`: protocol 3, operation `normalize`,
  profile tasks, optional fingerprint, no runtime resources/integrity fields.
- `engine/src/historical/job/{validate,stream,ordering}.rs`: unique task/stream
  IDs and ranks, valid task references, event timestamp ordering with stream rank.
- `engine/src/shared/{domain/instrument/spec,io/container}.rs`: decimal specs and
  container names `plain`, `gzip`, `zip`, `tar_gzip`.
- `engine/src/bin/marketforge-normalize.rs`: `--project-root`, `--config`, `--job`.

There is no incompatible v3 field in the requested contract. Rust accepts zero
for its unsigned job/dataset/task IDs; preparation deliberately requires positive
IDs. Fingerprints are serialized as null, which Rust supports; Python does not
reproduce Rust's compiled-profile fingerprint or normalization interpreter.
Only profile identity, market/content compatibility, container permissions and
required metadata are checked here. Rust remains authoritative for compilation,
format processing and runtime validation.

`data/.config/historical/normalize.toml` remains authoritative for engine resources
and integrity policy. Preparation does not parse or duplicate these settings.
The result reports whether `.bin/marketforge-normalize` is executable. Legacy
manual preparation retains its shell-quoted single-job invocation. Batch dispatch,
real engine integration and database-backed normalization lifecycle remain deferred.
