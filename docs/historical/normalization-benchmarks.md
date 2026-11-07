# Historical normalization benchmark infrastructure

Real benchmark execution requires separate user authorization. The refactor was
verified using mocks, synthetic statistics and nonexecuting plans only.

The existing binary is retained: `marketforge-normalize-benchmark`.
`--input` is required and accepts a prepared schema-3 batch anywhere on the
filesystem. Relative input paths use the invocation directory; source paths use
the production batch/project-root contract. There is no automatic exchange,
filename, duration or size exclusion. Unresolved family specifications fail
production resolution; the runner does not fabricate instrument metadata.

Nonexecuting example:

```sh
marketforge-normalize-benchmark --input /path/to/prepared.json \
  --workers 1,2,4 --repetitions 3 --warmups 1 --scheduler global --dry-run
```

Dry-run prints a versioned plan without creating output directories, reading
archive payloads, warming up, generating Parquet or producing measurements.
`--scheduler per-job` executes jobs sequentially through the production executor;
`global` uses the production session. Each measured trial includes the complete
selected workload. A fresh child process runs each trial, including warmups.
Worker order reverses on alternating rounds; warmups precede measured rounds.
No cold-cache claim is made. Plans reject invalid/duplicate/zero worker counts,
zero repetitions, invalid resource overrides and output collisions. Zero warmups
is valid. Plan descriptors are bounded to 256 MiB and 100000 trials.

Runtime defaults to `data/.config/historical/general.toml`. `--config`,
`--project-root`, `--memory-budget`, `--scratch-budget`, `--row-group-target` and
`--file-target` use the shared loader/validator and in-memory overrides. Integrity
policies are preserved. Every trial persists exact effective configuration.

Runs use exclusive directories under
`data/benchmarks/historical/normalize/runs/<time-pid-counter>/`.
`--output-root` selects an exact new run directory; existing directories are
rejected. Previous runs are never cleared. Artifacts include run.json,
environment.json, workload.json, results.jsonl, and trials/<trial-id>/ with
result.json, request.json, effective-config.toml, execution.json,
parquet-files.json, stdout.log, stderr.log, preserved metadata, datasets and
scratch as applicable. A dry-run plan's unallocated destination is not a run ID.

Run/trial schemas are independently versioned at 1. Trial states are planned,
running, complete, failed or interrupted. Final JSON uses atomic replacement;
JSONL appends and syncs each definitive result. A process interruption leaves
running/unfinished state and prior finalized trials intact. Automatic resume is
not implemented. Failed or unverified trials have no successful throughput.

Measurements implemented for future authorized execution:

| Field | Unit / boundary |
|---|---|
| execution_wall_seconds | Monotonic complete production workload calls inside child; excludes startup and harness verification |
| trial_wall_seconds | Parent monotonic execution, verification, metadata extraction and retention; excludes final result persistence |
| verification_wall_seconds | Parent output validation only |
| per_job_wall_seconds | Production call per sequential job; null for global scheduling |
| source_records_per_second | Committed manifest source-record counters / execution wall |
| canonical_rows_per_second | Committed canonical trade/L2 rows / execution wall |
| input_bytes_per_second | Distinct declared archive file volume / execution wall; not physical I/O |
| output_bytes_per_second | Actual canonical Parquet bytes / execution wall |
| process_cpu_*_seconds | Linux/macOS getrusage SELF deltas around execution, all process threads; small counter-reading overhead included |
| peak_rss_bytes | Fresh child's lifetime high-water mark through execution; Linux KiB converted to bytes, macOS bytes |
| logical_read/written_bytes | Linux /proc/self/io rchar/wchar deltas; includes counter-read overhead |
| physical_read/written_bytes | Linux kernel read_bytes/write_bytes storage accounting; not raw device/durable I/O; null on macOS |

Configured budgets remain separate from RSS. Peak internal accounted memory and
uncompressed input bytes are unavailable/null. Detailed engine phase timings
are not collected. No zero substitutes unavailable observations. Per-file
Parquet observations preserve actual sizes, decoded rows, row groups, column
compressed/uncompressed metadata and timestamp bounds. Aggregated statistics
and comparisons belong to the next task.

Verification compares requested identities, task inventories, statuses,
manifests, exact sizes/footer rows/timestamps, actual decoded canonical schemas
and rows, checksummed resolved configuration, depth events/segments/boundaries,
source-event ordinals and row offsets. Typed, ordered per-stream canonical hashes
ignore Parquet physical layout. Separate depth event/reconstruction hashes
preserve zero-row events and initialization metadata. These consistency checks
complement independent profile oracles; they do not establish economic semantics
for every real source.

`--retention metadata` (default) removes only verified successful datasets owned
by that newly created trial, after copying manifests, resolved jobs/checksums,
auxiliary metadata and available failures. Results explicitly mark datasets not
retained. Failed outputs remain. `full` retains everything. Ownership markers,
exact assigned destinations and symlink/traversal checks protect cleanup.

Build-time metadata records actual rustc/profile/target/features. Environment
metadata records Git SHA/branch/dirty status, executable/Cargo.lock checksums,
available CPU/core/memory/OS details. Optional frequency/power/storage details
are null. No arbitrary environment variables or credentials are collected.
Normalization identity is the production library linked into the child binary.
Workload provenance includes original input/checksum and prepared document,
resolved jobs/profile hashes, identities and archive paths/sizes/mtime. Source
payload hashes are not collected automatically; equivalence based on metadata
is explicitly weaker. Metadata is checked before and after each trial. No source
archive is modified or decoded merely for planning.

Linux compilation and synthetic infrastructure tests passed. macOS probes are
implemented behind platform conditions but have not been runtime-tested here.
Actual throughput, memory, I/O, scaling, real fragmentation and measurement
overhead remain unmeasured. No performance improvement is claimed.

## Statistical summaries and comparisons

These commands analyze persisted metadata and never execute normalization:

```sh
marketforge-normalize-benchmark summarize --run /path/to/exported-run
marketforge-normalize-benchmark compare --baseline /path/to/baseline \
  --candidate /path/to/candidate
```

`summary.json` (schema 1) is the authoritative aggregate; `summary.md` renders the
same values. Regeneration changes only those derived files. Comparisons use
exclusive directories under `data/benchmarks/historical/normalize/comparisons/`,
with `comparison.json`/`comparison.md` (independent schema 1). `--output-root`
selects an exact new comparison directory. Source workload files and generated
datasets need not remain available; analysis uses persisted provenance/results.

Groups preserve workload, scheduler, workers, budgets, Parquet targets, full
integrity policy and measurement source/definition. Assigned scratch paths are
normalized for grouping; exact original paths remain in trial records. Warmups
are excluded from statistics and accounted separately. Missing, running,
interrupted and failed states remain distinct. Failed required warmups or
repetitions prevent a group appearing complete. Interrupted runs retain valid
completed observations. Identical duplicate finalized records are counted once;
conflicting duplicates and corrupt/truncated JSON fail with artifact/trial
context. Atomic per-trial results can recover a missing JSONL append. Corrupt
JSONL is never silently discarded.

Statistics use finite nonnegative observations, arithmetic mean, ordinary median,
Welford sample standard deviation (`n-1`), nearest-rank P95 (`ceil(.95*n)`) and CV
(`SD/abs(mean)`). SD/CV are null with fewer than two samples; CV is null for zero
mean. Missing metrics remain unavailable, not zero. Positive execution duration
is required. IDs retain integer representation; statistical measurements use
floating-point values with no input rounding. File-size metrics are computed
per trial, then summarized across trials; full per-file observations remain
available. The median of trial file-size medians is not a pooled file median.

Session rates use complete-workload counts divided by execution wall time.
Pooled rates separately use summed work divided by summed time. Individual job
rates are never averaged into session throughput. Optional additive content-type
and stream work-volume counters preserve source and canonical rows separately;
older reports expose missing detail. Mixed trade/depth rates describe that
selected workload, not universal normalization efficiency.

Scaling defaults to a valid one-worker baseline. `summarize --baseline-workers N`
selects another explicitly recorded baseline. Speedup is baseline median time /
candidate median time; relative efficiency divides speedup by the worker-count
ratio. Missing/failed/incomplete baselines produce null ratios with reasons.

Comparisons classify `comparable`, `comparable_with_caveats`, or `incomparable`.
Software revision changes are intentional by default. Use
`compare --intentional workers,memory-budget,parquet` (comma-separated) to declare
other variables; supported values also include scratch-budget, scheduler,
hardware, build, integrity-policy, profiles and cache. These flags do not waive
source-corpus or canonical correctness checks. Unmatched groups are identified;
only compatible groups are paired. Hardware changes require an explicit
cross-machine variable; dirty state, ordinary caches, optional missing metrics
and weaker source identity produce caveats.

Verified source hashes allow path relocation. When common full hashes are
unavailable, matching basename/size/mtime provides explicitly weaker evidence;
missing essential identity or confirmed hash mismatches invalidate comparisons.
Canonical record/depth metadata fingerprints support equivalent physical layouts.
Structural row counts alone never establish semantic equivalence; such reports
are diagnostic and do not claim an accepted optimization.

For eligible metrics, differences are candidate minus baseline; percent changes
use baseline as denominator. Duration speedup is baseline/candidate; other ratios
are candidate/baseline. Zero baselines yield null percent/ratio. CPU percentages
can exceed100. Memory/I/O/bytes/file counts are objective-dependent trade-offs.
Execution/source/canonical rate interpretations show sample count and variability.
Changes within observed CV or single-sample cases have no established conclusion;
this heuristic is not a significance test. No significance or causal claims are
made, and production settings are never tuned automatically.

Synthetic report fixtures are explicitly labeled as test data. Real performance
benchmarks were not executed during implementation.

Depth proof lookup normalizes allocated job/dataset identities to their source
units before comparing hashes; changing an allocation alone does not establish a
semantic mismatch. Missing required depth proof remains an evidence limitation.
