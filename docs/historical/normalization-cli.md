# Normalization CLI contract

Use exactly one of `--batch`, `--job`, or `--jobs-dir` with
`marketforge-normalize`. `--batch` consumes the production prepared schema 4
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

Strict production output uses manifest 3/4; activated processing policies use manifest 5. The timestamp-first merge reader admits only manifest 2/3 and rejects newer ownership/native-order formats explicitly. Failure reports remain version 1. Canonical Parquet inventories
report relative paths, finalized footer row counts, actual file sizes and
timestamp statistics. Discovery rejects symlinks. Depth requires one event
index, segment inventory and boundary2 source inventory per stream. Persisted
event/row spans and reconstruction initializations are validated before commit.
See [stream output](normalize-stream-output.md) for source ranges and versions.
Existing staging/rename/collision checks remain; atomic rename is not a stronger
crash-durability claim.

`processing-job.json` records effective runtime/policy, profiles/fingerprints,
concrete instruments and original source/stream/rank provenance.
`processing-job.sha256` hashes its exact bytes, not archives or Parquet contents.
The manifest stream inventory supplies half-open task row spans; the snapshot
retains exact source and profile identities. Depth boundary2 adds event ranges.
Resolved feed variants use the same resolver when reloading provenance.

Parquet targets retain existing behavior: row-group triggers estimate buffered
records; trade file rotation observes bytes written after row-group flush;
depth file rotation accumulates Arrow batch memory size. These are targets,
not persisted-size limits. Inventory size_bytes is always actual filesystem
size. Source completion does not close shared writers. Depth flushes row groups at
configured batch boundaries and bounds event-index and segment buffering.

Forge must parse the envelope, inspect process status and every job status,
register only committed manifests, and retain failure diagnostics. PostgreSQL
registration remains exclusively Python's responsibility. Rust does not require
Python or PostgreSQL. Actual benchmark execution remains separately authorized.

Preparation discovery is reported in the TUI before normalization begins. Discovery, decoding, finalization and joins run on its worker. Final stdout remains the version-1 machine response after terminal restoration. Failure reports retain one bounded structured failure per affected task; committed counters are zero for failed jobs even if staging contains rows.

Preparation failures can additionally carry optional structured `error.context` in the existing version-1 CLI response (job/task, source/member where known, stream, stage, typed cause and relevant limits). This does not fabricate definitive processing-job results for jobs that never reached execution. Per-task execution errors remain in the streamed failure report.
