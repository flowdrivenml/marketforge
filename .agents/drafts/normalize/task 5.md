## MarketForge — Benchmark Specification Amendments

**Status:** Approved changes to the historical normalization benchmarking refactor.

**Purpose:** Update benchmark input handling, workload selection, and implementation verification requirements.

These instructions supersede any conflicting requirements in the previous benchmarking specification.

Preserve the previously agreed benchmark directory structure, measurement definitions, statistical reporting, comparison system, and artifact-retention policies.

## 1. Benchmark Input Must Be Explicit and Location-Independent

The benchmark runner must accept an explicit path to a JSON workload document.

Use:

```bash
marketforge-benchmark normalize \
  --input /path/to/jobs.json \
  --workers 1,4,8,16 \
  --repetitions 5 \
  --warmups 1
```

**The input document may be located anywhere on the filesystem.**

Examples of valid locations:

```text
engine/tests/historical/fixtures/jobs.json

data/runs/normalize-abc123/normalize-batch.json

/home/user/benchmark-corpora/large-trades.json

/mnt/market-data/experiments/normalization-batch.json
```

Do not require benchmark input documents to reside inside the MarketForge repository.

Do not hardcode:

```text
data/.jobs/process/
engine/tests/historical/fixtures/jobs.json
data/runs/
```

as mandatory input locations.

The existing `engine/tests/historical/fixtures/jobs.json` may be used for testing benchmark infrastructure, but it must not be required for normal benchmark execution.

### Input Contract

The benchmark runner should accept the authoritative schema-version-3 prepared normalization batch.

```json
{
  "schema_version": 3,
  "kind": "normalization_batch",
  "status": "prepared",
  "jobs": []
}
```

The example above is schematic; actual valid documents must contain all required batch fields and valid jobs.

Use the production batch parser, validation, and profile-resolution infrastructure.

Do not implement a benchmark-specific duplicate JSON schema.

Do not automatically discover arbitrary processing jobs from directories unless an explicitly supported input mode requests that behavior.

Do not silently convert unsupported JSON documents into benchmark workloads.

If the supplied input is incompatible with the production protocol, fail with an actionable validation error.

### Path Resolution

Support both absolute and relative input paths.

Absolute paths must remain absolute.

Relative input paths should be resolved consistently against the process invocation directory, then converted into an absolute path before execution.

After loading the input, resolve source archive paths according to the authoritative batch-loading contract.

Do not assume archive paths are relative to the benchmark output directory.

Keep source archives read-only.

Do not modify the original input document.

### Input Provenance

Every benchmark run must record:

- Original input path.
- Resolved absolute input path.
- Input document checksum.
- Input schema version.
- Selected job IDs.
- Selected task IDs.
- Source archive identities.
- Resolved profile IDs and fingerprints.
- Effective runtime configuration.

This information must be preserved in the benchmark's workload metadata.

The goal is to make it possible to reproduce an experiment even when the original workload document was stored outside the repository.

## 2. Workload Selection Is Explicit

The previous benchmark implementation excluded certain Gate.io archives automatically because they represented large monthly datasets.

This exclusion was intentional during development to avoid distorting worker-scaling measurements.

However, the exclusion must not be hardcoded into the new benchmark implementation.

### Required Behavior

**The supplied workload determines what is benchmarked.**

If the input contains only Binance and Bybit tasks, benchmark those tasks.

If the input contains Gate.io tasks, include them unless an explicit selection option excludes them.

If the input contains a month-long archive, do not silently replace it with a shorter archive.

If the input contains multiple exchanges, preserve their actual identities.

Do not automatically exclude jobs based on:

- Exchange name.
- Archive duration.
- Source filename.
- File size.
- Instrument type.
- Data type.
- Historical development conventions.

### Optional Selection Filters

The benchmark CLI may support explicit filters such as:

```text
--exchange
--exclude-exchange
--content-type
--profile
--stream
```

Implement only filters that are genuinely useful and can be validated reliably.

The user must be able to control which workloads are included.

For example:

```bash
marketforge-benchmark normalize \
  --input /data/experiments/jobs.json \
  --exclude-exchange gateio \
  --workers 1,4,8
```

This is illustrative CLI syntax.

Do not assume the option already exists.

The purpose is to make workload exclusion explicit rather than embed exchange-specific exclusions in source code.

### Preferred Workflow

Initially, prioritize the simplest approach:

**Prepare a benchmark input document containing exactly the desired jobs.**

This avoids introducing an unnecessarily complicated workload-filtering language.

More advanced selection features can be added when there is a demonstrated need.

Do not invent a new benchmark workload format merely to support selection.

### Workload Comparability

For performance comparisons, the selected workload must remain identical between baseline and candidate runs unless workload size or composition is the intentional experimental variable.

Record the exact selected jobs and archives.

Do not compare a benchmark containing a month-long Gate.io archive against one containing only daily archives and report the result as an implementation regression.

The comparison system must detect workload differences.

## 3. Benchmark Output Directory

Preserve the previously agreed structure:

```text
data/benchmarks/
└── historical/
    └── normalize/
        ├── runs/
        │   ├── <benchmark-run-id>/
        │   └── <benchmark-run-id>/
        └── comparisons/
            └── <comparison-id>/
```

Benchmark results should use this location by default.

The input document's location must not determine the benchmark output directory.

Input and output paths are independent.

An explicit output-root override may be supported when useful, while retaining the same internal directory structure.

Do not overwrite earlier benchmark runs.

Preserve the agreed metadata and full-retention modes.

All actual performance measurements must come from executions that genuinely occurred.

Do not create fabricated benchmark results.

## 4. Codex Must Not Execute Benchmarks

**Critical requirement: During implementation, do not run actual performance benchmarks.**

The purpose of this Codex task is to implement and verify the benchmarking infrastructure.

It is not to collect performance measurements.

Do not run:

```bash
marketforge-benchmark normalize \
  --input <real-workload> \
  --workers 1,4,8,16
```

Do not execute full historical normalization workloads merely to verify benchmark functionality.

Do not automatically start benchmark experiments after compilation.

Do not process large market-data archives.

Do not run worker-scaling experiments.

Do not perform warmup executions against real datasets.

Do not generate large Parquet datasets.

Do not perform disk I/O stress tests.

Do not run benchmarks in the background.

### Authorized Verification

Codex may perform the following:

**Compilation**

Verify that the benchmark runner and related infrastructure compile successfully.

**Formatting and linting**

Run appropriate Rust formatting and lint checks.

**Unit tests**

Run lightweight, deterministic tests for:

- CLI argument parsing.
- Input-path validation.
- JSON workload parsing.
- Workload selection.
- Configuration loading.
- Run and trial identity generation.
- Benchmark report serialization.
- Statistical calculations.
- Speedup calculations.
- Comparison compatibility checks.
- Result accounting.
- Retention-policy validation.

**Synthetic infrastructure tests**

Use small in-memory fixtures, mocked execution results, and temporary metadata files.

Do not execute actual normalization workloads as part of these tests.

**CLI verification**

Verify help output and argument validation.

**Dry-run verification**

An optional `--dry-run` or equivalent planning mode is recommended.

It should:

1. Load and validate the supplied input.
2. Resolve the selected workload.
3. Validate applicable configuration.
4. Construct the benchmark execution plan.
5. Display the planned jobs, tasks, worker counts, repetitions, and estimated execution count.
6. Exit without invoking normalization or producing benchmark datasets.

Dry-run must never execute measured trials.

Dry-run must never be recorded as a completed benchmark.

### Compilation Versus Runtime Verification

Compilation and lightweight tests demonstrate that the benchmarking infrastructure is structurally valid.

They do not prove that the complete benchmark execution path works with real market data.

Codex must report that distinction.

Do not claim real-data benchmark execution was verified when no real workload was executed.

## 5. Defer Performance Measurements

The benchmark implementation must support the measurement capabilities described in the previous specification.

However, Codex must not populate performance reports with invented observations.

If a metric requires actual runtime execution, verify its implementation through lightweight tests or mocked metric providers where possible.

For metrics requiring operating-system support, verify the API integration without running a real benchmark.

Examples include:

```text
peak_rss_bytes
process_cpu_user_seconds
process_cpu_system_seconds
process_read_bytes
process_written_bytes
execution_wall_seconds
canonical_rows_per_second
```

Where appropriate, use controlled synthetic values to test calculations.

Do not present those values as measured MarketForge performance.

The first real benchmark execution will be performed separately after the implementation has been reviewed.

## 6. Update Benchmark Acceptance Criteria

Replace any previous requirement stating that Codex must execute a representative real-data benchmark.

The updated acceptance criteria are:

- Benchmark input is supplied explicitly through `--input`.
- Input documents may reside anywhere on the filesystem.
- The authoritative normalization batch protocol is reused.
- No legacy processing-job directory is required.
- Gate.io is not excluded automatically.
- No exchange-specific workload exclusions are hardcoded.
- Explicit workload selection is deterministic.
- Selected workload identities are preserved in benchmark metadata.
- Runtime configuration comes from the shared historical configuration infrastructure.
- The persistent benchmark output structure is implemented.
- Benchmark reports support the previously agreed measurements.
- Statistical aggregation and comparison infrastructure are implemented.
- Lightweight infrastructure tests pass.
- CLI argument validation works.
- Compilation and relevant lint checks pass.
- No real performance benchmark is executed by Codex.
- No large historical workload is processed during implementation.
- No fabricated benchmark results are generated.
- No production normalization behavior is modified.

## 7. Final Implementation Report

Codex must report:

### Implemented

Describe the benchmark runner, supported CLI options, input loading, workload selection, metrics, reporting, and comparison infrastructure.

### Verified

List compilation checks, linting, unit tests, and synthetic infrastructure tests actually executed.

### Not Executed

Explicitly state:

**Real-data normalization benchmarks were not executed, as instructed.**

### Remaining Runtime Verification

Identify anything that still requires a real benchmark execution, such as:

- Actual throughput measurements.
- Worker-scaling behavior.
- Peak memory measurements under load.
- Physical disk I/O measurements.
- End-to-end execution timing accuracy.
- Real-data output correctness.
- Performance comparison validity under realistic workloads.

These are deferred verification items, not automatically implementation defects.

## Final Instruction

Implement and validate the benchmark infrastructure without conducting benchmark experiments.

The benchmark runner must be ready to accept an explicitly provided normalization workload from any filesystem location.

The user will decide which archives, exchanges, instruments, and time periods to benchmark.

**Do not execute actual benchmarks until explicitly instructed.**