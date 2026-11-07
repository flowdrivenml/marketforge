# Normalization CLI contract

Use exactly one of `--batch`, `--job`, or `--jobs-dir` with
`marketforge-normalize`. `--batch` consumes the production prepared schema 3
contract, including its persisted offline instrument authority. `--job` keeps
legacy resolved/profile job compatibility; directory jobs are sorted by path.
All execution uses shared runtime configuration and production profile resolution.
Paths are absolute or relative to `--project-root`. Runtime defaults to
`data/.config/historical/general.toml`. `--output-root` redirects staging/final
paths for every mode, refuses an existing root, and leaves source paths intact.

Normal execution writes exactly one JSON document to stdout. Diagnostics go to
stderr. `--help`/`--version` are human-readable. When compiled with `monitor-tui`,
explicit `--tui` selects interactive terminal rendering on stdout; its final
JSON is printed after restoration. Do not select TUI for machine consumption.

The response has independent `schema_version: 1`, `kind: normalization_result`,
nullable batch `run_id`, `status: complete|failed`, `jobs`, `summary`, and nullable
`error` (`code`, `message`). Legacy modes have null run identity. Per-job fields:
`job_id`, `dataset_id`, `status`, `manifest_path`, `failure_report_path`,
`events_written`, `files_written`, `start_timestamp_ns`, `end_timestamp_ns`,
`failure` (existing `code` and `message`). Identifiers remain exact integers;
consumers must avoid floating-point parsing.

`summary` contains `jobs_total`, `jobs_complete`, `jobs_failed`,
`jobs_not_reported`. Their sum is consistent; unknown results are never invented.
A session is complete only when every requested job committed. A degraded,
integrity-approved committed dataset is complete, with manifest integrity
recorded separately. Any failed/unreported job or initialization error causes
nonzero exit. Definitive job results remain available even on nonzero exit.
Argument errors are nonzero with Clap diagnostics; they need not emit JSON.

A successful job has an existing committed manifest, no failure, and no failure
report. A failed job has no success manifest and a structured failure; its report
path is populated only if persistence succeeded. Report persistence failure is
included in the failure message. Early initialization failures can have no job
results/reports. Earlier definitive session results survive later fatal errors.

Manifest version 2 and failure-report version 1 are preserved. Canonical Parquet
inventories report relative paths, finalized footer row counts, actual file
sizes and timestamp statistics. Artifact discovery rejects symlinks. Depth
requires events.parquet, segments.json and boundary.json per task; reconstruction
row totals must match canonical rows. Existing staging/rename/collision checks
remain. Atomic rename is not a claim of stronger crash durability.

`processing-job.json` records effective runtime/policy, profiles/fingerprints,
concrete instruments and original source/stream/rank provenance.
`processing-job.sha256` hashes its exact bytes, not archives or Parquet contents.
This snapshot plus manifest source-task inventory and processing task metrics
supplies stream/source relationships; no duplicate stream registry is added.
Resolved feed variants use the same resolver when reloading provenance.

Parquet targets retain existing behavior: row-group triggers estimate buffered
records; trade file rotation observes bytes written after row-group flush;
depth file rotation accumulates Arrow batch memory size. These are targets,
not persisted-size limits. Inventory size_bytes is always actual filesystem
size. No target, compression or rotation algorithm changes are made here.

Forge must parse the envelope, inspect process status and every job status,
register only committed manifests, and retain failure diagnostics. PostgreSQL
registration remains exclusively Python's responsibility. Rust does not require
Python or PostgreSQL. Benchmark/storage consolidation work is separate.
