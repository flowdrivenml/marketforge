# Task 08 — Historical Normalization Benchmark Infrastructure

**Project:** MarketForge  
**Component:** Rust Historical Normalization Engine  
**Priority:** High / Performance Infrastructure  
**Status:** Approved for implementation  
**Dependencies:** Tasks 01–07  
**Next task:** Benchmark Statistics, Worker Scaling, and Run Comparisons

## 1. Objective

Replace the outdated historical normalization benchmark implementation with a reproducible, configurable, and persistent benchmarking system.

The new infrastructure must support:

- Explicit selection of normalization workloads.
- Input documents located anywhere on the filesystem.
- Execution through the production normalization engine.
- Configurable worker counts and repetitions.
- Per-job and global scheduling comparisons.
- Warmup executions.
- Detailed performance measurements.
- Hardware and software provenance.
- Runtime configuration provenance.
- Source archive and profile identities.
- Persistent results under `data/benchmarks/`.
- Optional retention of generated datasets.
- Correctness verification of benchmark outputs.
- Safe execution planning without running normalization.

The benchmark runner must generate enough information to support meaningful comparisons between software versions, hardware environments, resource configurations, and execution strategies.

**The primary objective is reliable performance evidence, not merely reporting records per second.**

### Critical Restriction

**Do not execute actual benchmarks during this implementation task.**

Codex must implement the infrastructure, compile it, and verify its functionality through lightweight tests and mocked execution results.

The user will explicitly authorize real benchmark execution later.

Complete only Task 08.

---

## 2. Architectural Context

Tasks 01–07 established:

- Shared historical runtime configuration.
- Production schema-version-3 batch loading.
- Unified normalization profiles.
- Shared testing infrastructure.
- Integrity and source-error correctness.
- Reliable dataset manifests.
- Standardized normalization CLI results.
- Consistent execution-status reporting.

The benchmark runner must reuse these components.

### Intended Architecture

```text
User
 │
 ▼
Benchmark CLI
 │
 ├── Input workload
 ├── Worker configurations
 ├── Repetitions
 ├── Warmups
 ├── Scheduler mode
 └── Retention policy
 │
 ▼
Benchmark Planner
 │
 ├── Validate workload
 ├── Resolve profiles
 ├── Load runtime configuration
 ├── Prepare trial configurations
 ├── Capture environment
 └── Generate execution plan
 │
 ▼
Benchmark Runner
 │
 ├── Execute production normalization
 ├── Collect measurements
 ├── Verify committed datasets
 ├── Preserve artifacts
 └── Persist trial results
 │
 ▼
data/benchmarks/historical/normalize/
 │
 ├── Run metadata
 ├── Environment metadata
 ├── Workload provenance
 ├── Effective configuration
 ├── Individual trial results
 ├── Execution diagnostics
 └── Dataset metadata
```

The benchmark runner orchestrates measurements.

The normalization engine performs actual processing.

**Do not create benchmark-specific normalization algorithms.**

---

# 3. Inspect Existing Benchmark Infrastructure

Before modifying code, inspect the current historical normalization benchmark implementation.

The previous implementation supported:

```text
--dataset trades|depth|all
--scheduler per-job|global
--workers 1,4,8,16
--repetitions 3
--output <path>
```

It also contained:

- Per-job benchmark results.
- Global-session benchmark results.
- Worker-scaling calculations.
- Basic throughput measurements.
- Dataset manifest verification.
- Automatic cleanup of successful datasets.
- JSON report generation.

Preserve useful functionality.

### Known Outdated Behavior

The previous implementation:

- Loaded legacy processing jobs.
- Used an obsolete runtime configuration path.
- Discovered jobs from a hardcoded directory.
- Excluded Gate.io automatically.
- Excluded some workloads using filename conventions.
- Deleted successful datasets.
- Recorded insufficient environment provenance.
- Recorded limited CPU, memory, and I/O statistics.
- Did not persist results after each trial.
- Did not adequately distinguish measurement boundaries.
- Did not support reproducible cross-version comparisons.

Replace these behaviors where necessary.

### Initial Audit

Inspect:

```text
engine/benches/
engine/src/bin/
engine/src/historical/normalize/
engine/src/shared/
engine/Cargo.toml
```

Use actual repository paths.

Determine:

1. Where the current benchmark runner lives.
2. Which measurement helpers can be reused.
3. Which manifest-validation helpers already exist.
4. How the normalization CLI exposes execution results.
5. How global sessions are executed.
6. Which performance counters are available.
7. How runtime resource overrides are applied.
8. Whether process-level CPU and memory measurements already exist.
9. How existing benchmark targets are compiled.
10. Whether any current benchmark utilities are used elsewhere.

Do not retain obsolete architecture merely because it compiles.

Do not discard useful existing benchmark validation logic.

---

# 4. Benchmark CLI

Provide a dedicated benchmarking entry point following existing MarketForge command conventions.

The preferred interface is:

```bash
marketforge-benchmark normalize \
  --input /path/to/normalize.json \
  --workers 1,2,4,8,16 \
  --repetitions 5 \
  --warmups 1 \
  --scheduler global \
  --retention metadata
```

This is the desired interface. Adapt executable naming to established project conventions where necessary.

### Required Options

| Option | Responsibility |
|---|---|
| `--input` | Path to a prepared normalization batch |
| `--workers` | Worker counts to evaluate |
| `--repetitions` | Number of measured repetitions |
| `--warmups` | Number of unmeasured warmup executions |
| `--scheduler` | Execution scheduling mode |
| `--retention` | Generated dataset retention policy |
| `--dry-run` | Validate and display the execution plan without processing |

Support an explicit output-root override when useful.

Optional resource overrides may include:

```text
--memory-budget
--scratch-budget
--row-group-target
--file-target
```

Use naming consistent with the existing CLI conventions.

Do not introduce a large number of speculative options.

### CLI Validation

Reject:

- Missing input.
- Invalid input paths.
- Empty worker lists.
- Zero workers.
- Duplicate worker counts.
- Zero measured repetitions.
- Unsupported scheduler modes.
- Unsupported retention modes.
- Invalid resource overrides.
- Output-directory collisions.

Zero warmups must be valid.

Worker counts must remain explicitly configurable.

Do not assume that all machines support the same number of useful workers.

---

# 5. Explicit Workload Input

**The benchmark input must be supplied explicitly using `--input`.**

The file may reside anywhere on the filesystem.

Examples:

```text
engine/tests/historical/fixtures/normalize.json

data/runs/normalize-abc123/normalize-batch.json

/home/user/benchmarks/trades.json

/mnt/market-data/experiments/depth.json
```

Do not require input documents to reside inside the repository.

Do not hardcode:

```text
data/.jobs/process/
engine/tests/historical/fixtures/normalize.json
data/runs/
```

as mandatory locations.

### Authoritative Input Contract

Use the production schema-version-3 normalization batch:

```text
schema_version = 3
kind = normalization_batch
status = prepared
```

Load it through the production loader established by Task 02.

Do not introduce a benchmark-specific job schema.

Do not reconstruct normalization rules manually.

Do not duplicate profile resolution.

### Path Resolution

Support absolute and relative input paths.

Resolve relative CLI input paths consistently against the invocation directory.

Then use the production batch contract to resolve source paths and related metadata.

Do not assume input archives are relative to the benchmark output directory.

Preserve original source archives.

Do not modify the input document.

### Empty Workloads

Reject batches that do not contain executable jobs or tasks.

A benchmark with zero actual workload must not be reported as successful.

---

# 6. Workload Selection

The supplied input determines which jobs are benchmarked.

For the initial implementation, prioritize:

**A prepared input containing exactly the intended benchmark workload.**

For example, if the user wants only Binance and Bybit trades, they can supply a batch containing those tasks.

Do not automatically discover additional archives.

Do not replace source files.

### No Hardcoded Exclusions

The previous benchmark implementation excluded Gate.io archives because some represented month-long workloads.

That was an intentional practical decision, but it must not remain hardcoded.

Remove exclusions based on:

- Exchange name.
- Archive duration.
- Source filename.
- File size.
- Instrument.
- Market category.
- Historical development conventions.

If the provided workload contains a month-long Gate.io archive, include it.

If the user wants to exclude that archive, the supplied workload should omit it.

### Optional Filters

Additional selection options may be supported when straightforward:

```text
--exchange
--exclude-exchange
--content-type
--profile
--stream
```

Do not implement a complex query language.

If filters are supported, record the actual selected jobs and tasks.

Never silently exclude selected work.

### Workload Identity

Preserve the exact selected workload so future comparisons can establish whether two experiments processed equivalent input data.

Do not describe a selected subset as a complete historical workload.

---

# 7. Runtime Configuration

Use:

```text
data/.config/historical/general.toml
```

Load configuration through the shared historical runtime loader established by Task 01.

### Resource Overrides

Benchmark-specific resource settings must be applied in memory.

For example:

```text
workers = 8
memory_budget_bytes = 17179869184
row_group_target_bytes = 67108864
file_target_bytes = 268435456
```

These values are illustrative.

Do not change the production TOML file.

Do not introduce a separate benchmark runtime configuration parser.

### Effective Configuration

Record the exact effective configuration used by each trial.

This must include:

- Worker count.
- Memory budget.
- Scratch budget.
- Scratch path.
- Parquet row-group target.
- Parquet file target.
- Integrity policy.
- Any relevant operation-specific settings.

Do not record only the original TOML if trial overrides change its values.

### Integrity Policies

Do not disable integrity checking to improve benchmark throughput.

Do not relax thresholds automatically.

A performance comparison is meaningful only when the effective correctness requirements are understood.

If two benchmark runs use different integrity policies, record the difference and make it available to the comparison system.

---

# 8. Persistent Benchmark Directory Structure

Store all benchmark results under:

```text
data/benchmarks/
```

Use:

```text
data/benchmarks/
└── historical/
    ├── normalize/
    │   ├── runs/
    │   │   ├── <benchmark-run-id>/
    │   │   └── <benchmark-run-id>/
    │   │
    │   └── comparisons/
    │       └── <comparison-id>/
    │
    ├── merge/
    └── convert/
```

Only normalization benchmarking is in scope.

The merge and conversion directories are reserved for future work.

Do not implement those benchmark runners.

## 8.1 Benchmark Run Layout

Each benchmark invocation creates a unique directory:

```text
data/benchmarks/historical/normalize/runs/<benchmark-run-id>/
```

Suggested structure:

```text
<benchmark-run-id>/
│
├── run.json
├── environment.json
├── workload.json
├── effective-config.toml
├── results.jsonl
│
└── trials/
    │
    ├── <trial-id>/
    │   ├── result.json
    │   ├── execution.json
    │   ├── parquet-files.json
    │   ├── stdout.log
    │   ├── stderr.log
    │   │
    │   ├── artifacts/
    │   │   ├── manifests/
    │   │   ├── resolved-jobs/
    │   │   └── failures/
    │   │
    │   ├── datasets/
    │   ├── staging/
    │   └── scratch/
    │
    └── <trial-id>/
        └── ...
```

This is a proposed structure.

Adapt it where necessary to avoid unnecessary duplication.

Do not create empty directories without a purpose.

### Reports Reserved for Task 09

Task 09 will add:

```text
summary.json
summary.md
```

and:

```text
comparisons/<comparison-id>/
├── comparison.json
└── comparison.md
```

Task 08 must preserve enough raw data to generate these reports later.

It does not need to implement the full statistical comparison system.

---

# 9. Run and Trial Identity

A benchmark run represents one experiment definition.

A trial represents one execution of the selected workload under one specific measured configuration.

### Run Identity

Every invocation receives a unique benchmark run ID.

Do not overwrite previous runs.

Use exclusive directory creation.

Do not rely only on a timestamp or process ID for uniqueness.

### Trial Identity

A trial must identify:

```text
benchmark run
scheduler mode
worker count
repetition
warmup/measured classification
```

Additional resource variations must also be recorded.

Do not use worker count alone as the trial identity.

### Example

Conceptually:

```text
Benchmark run A
    │
    ├── workers=1, warmup=1
    ├── workers=1, repetition=1
    ├── workers=1, repetition=2
    │
    ├── workers=4, warmup=1
    ├── workers=4, repetition=1
    ├── workers=4, repetition=2
    │
    └── ...
```

The execution order may differ.

Record the actual order.

---

# 10. Execution Modes

Preserve support for the existing scheduling modes where they remain valid.

```text
per-job
global
```

These are different execution strategies.

Do not combine their measurements without identifying the strategy.

## 10.1 Per-Job Scheduling

Jobs execute independently through the production normalization interface.

For a workload containing several jobs, preserve the existing sequential per-job benchmark behavior unless the actual implementation establishes another explicit contract.

Measure:

- Individual job execution duration.
- Complete workload/session duration.
- Job-level success or failure.
- Job-level output statistics.

Do not report a sequence of separately executed jobs as a globally scheduled session.

## 10.2 Global Scheduling

Execute the selected jobs through the production global-session mechanism.

Measure:

- Complete session execution duration.
- Total source records.
- Total canonical output.
- Total output files.
- Individual job outcomes.

Do not invent individual job durations if the executor does not expose them.

Use `null` or an explicitly unavailable metric.

### Important Measurement Boundary

**One measured trial must represent the complete selected workload under the chosen scheduler mode.**

This keeps the trial-level comparison meaningful.

For example:

```text
Workload:
    5 jobs
    25 tasks

Trial A:
    scheduler = per-job
    workers = 8

Trial B:
    scheduler = global
    workers = 8
```

Both trials must process the same selected workload.

Only the scheduling strategy differs.

### Process Isolation

Prefer independent process-level execution for measured trials where practical.

This supports cleaner CPU and memory measurement.

However, preserve the production scheduling semantics.

Do not introduce additional per-job parallelism that changes what `per-job` means.

Document whether startup and initialization overhead are included in each timing metric.

---

# 11. Benchmark Execution Planning

Before running any normalization, construct a complete execution plan.

The planner must determine:

- Selected jobs.
- Selected tasks.
- Resolved profiles.
- Scheduler mode.
- Worker configurations.
- Warmup count.
- Repetition count.
- Trial execution order.
- Output destinations.
- Effective resource settings.
- Retention policy.

### Example Plan

```text
Input: /data/experiments/trades.json

Jobs: 5
Tasks: 25

Scheduler: global

Workers:
    1
    2
    4
    8

Warmups: 1
Repetitions: 5

Warmup trials: 4
Measured trials: 20

Retention: metadata
```

These values are illustrative.

### Deterministic Trial Ordering

Use a deterministic execution order.

The previous benchmark reversed worker order on alternating repetitions to reduce systematic ordering bias.

Preserve or improve that approach.

Do not always execute worker configurations in a manner that systematically favors the final configuration through cache warming.

If randomized ordering is introduced, record its seed.

Do not introduce complicated experiment scheduling unnecessarily.

---

# 12. Dry-Run Mode

Implement:

```bash
marketforge-benchmark normalize \
  --input /path/to/normalize.json \
  --workers 1,4,8 \
  --repetitions 5 \
  --warmups 1 \
  --dry-run
```

### Required Behavior

Dry-run must:

1. Parse the CLI arguments.
2. Load the requested batch.
3. Validate the workload.
4. Resolve normalization profiles.
5. Load and validate runtime configuration.
6. Calculate the planned trial count.
7. Determine the planned scheduler configurations.
8. Display the execution plan.
9. Exit without executing normalization.

### Restrictions

Dry-run must not:

- Execute normalization tasks.
- Generate canonical Parquet.
- Create committed datasets.
- Run warmups.
- Collect fabricated performance measurements.
- Mark benchmark trials complete.
- Modify source archives.

If dry-run writes a plan file, it must be clearly identified as planning metadata, not a completed benchmark result.

### Source Availability

Separate workload parsing from expensive source processing.

Where practical, dry-run may validate source existence and metadata without decoding complete archives.

Do not require full archive decompression merely to display the plan.

---

# 13. Performance Measurement Contract

The benchmark runner must collect detailed measurements.

Use explicit units.

Do not store an ambiguous field such as:

```text
throughput = 123456
```

without defining its meaning.

Prefer:

```text
source_records_per_second
canonical_rows_per_second
```

The measurements below define the intended infrastructure.

If a metric is unavailable on a platform, record that fact rather than fabricating a value.

---

## 13.1 Execution Time

At minimum, record:

```text
execution_wall_seconds
trial_wall_seconds
```

### Execution Wall Time

Time spent executing the selected normalization workload under the defined production execution boundary.

### Trial Wall Time

Complete measured trial duration, potentially including process startup, result collection, and verification.

These are different metrics.

Do not use them interchangeably.

### Phase Timings

Where reliable engine instrumentation exists, collect:

```text
source_read_seconds
decode_seconds
normalization_seconds
parquet_write_seconds
finalization_seconds
manifest_seconds
commit_seconds
```

These are optional until accurately measurable.

Do not invent phase durations.

Do not estimate one phase by subtracting unrelated counters.

Concurrent phases may overlap.

Do not require their sum to equal total wall time.

### Measurement Source

Use monotonic clocks for elapsed duration.

Record wall-clock timestamps separately for provenance.

Do not calculate execution duration by subtracting calendar timestamps when a monotonic timer is available.

---

## 13.2 Throughput

Record:

```text
source_records_per_second
canonical_rows_per_second
```

Where meaningful, also calculate:

```text
input_bytes_per_second
output_bytes_per_second
```

### Source Records

Use actual source-record processing counters.

Do not confuse source records with emitted canonical rows.

### Canonical Rows

Use actual committed output counts.

For trades:

```text
canonical rows = normalized trade rows
```

For depth:

```text
canonical rows = emitted L2 level-update rows
```

One source depth event may produce many canonical rows or no changed rows.

### Throughput Calculation

For successful trials:

```text
throughput = completed_work / execution_wall_seconds
```

Do not use partial output from a failed trial to report successful throughput.

Do not silently replace unavailable timing values with zero.

---

## 13.3 Input Volume

Record useful input-volume statistics:

```text
source_archive_count
source_file_bytes
source_records_read
```

Where the distinction is available:

```text
compressed_input_bytes
uncompressed_input_bytes
```

Do not claim archive file size equals uncompressed source size.

Do not claim archive size equals physical disk I/O.

If uncompressed size is unavailable, use `null`.

---

## 13.4 CPU Measurements

Collect supported process-level CPU measurements.

Preferred fields:

```text
process_cpu_user_seconds
process_cpu_system_seconds
process_cpu_total_seconds
```

CPU utilization may also be calculated when the measurement boundary is documented.

### Requirements

- Use the appropriate operating-system facilities.
- Distinguish CPU time from wall time.
- Support multithreaded processes.
- Preserve platform differences.
- Avoid expensive sampling where unnecessary.

CPU utilization may exceed 100% when expressed relative to one logical CPU.

Do not clamp valid multithreaded utilization to 100%.

### Platform Support

The project targets Linux and macOS.

Provide measurements supported by the host platform.

Unavailable measurements must be represented explicitly.

Do not require root privileges.

Do not introduce a mandatory external profiler.

---

## 13.5 Memory Measurements

Record:

```text
peak_rss_bytes
```

where supported.

Also record existing engine resource-accounting statistics where available.

For example:

```text
configured_memory_budget_bytes
peak_accounted_memory_bytes
```

Use actual available engine fields.

Do not invent internal memory counters.

### Important Distinction

These are not equivalent:

```text
Configured memory budget.
Scheduler-estimated memory.
Reserved/accounted memory.
Actual process RSS.
```

Keep them separate.

### Process Isolation

Avoid reading a process-lifetime memory high-water mark across several unrelated trials and reporting it as a fresh peak for each trial.

Prefer separate measured processes or another demonstrably correct measurement mechanism.

For per-job sessions involving multiple processes, define whether reported peak memory represents the maximum individual process RSS or another measured aggregate.

Do not sum independent peaks and call the result simultaneous peak memory.

---

## 13.6 I/O Measurements

Record process-level I/O counters where supported.

Preferred measurements include:

```text
process_read_bytes
process_written_bytes
```

Where available, distinguish:

```text
logical_read_bytes
logical_written_bytes
physical_read_bytes
physical_written_bytes
```

### Important Distinction

These are different quantities:

```text
Source archive size.
Bytes returned by read operations.
Bytes processed from filesystem cache.
Actual physical device I/O.
```

Do not label one as another.

### Linux and macOS

Use platform-specific measurement implementations only where their semantics are reliable.

Do not introduce Linux-only assumptions into common benchmark code.

Unavailable metrics must be nullable or explicitly marked unsupported.

### Measurement Overhead

Do not sample unnecessarily at very high frequency.

Prefer low-overhead counters and bounded sampling.

Record important measurement limitations.

---

# 14. Parquet Output Statistics

The benchmark must collect detailed information about generated Parquet output.

This is particularly important because excessive file fragmentation is a suspected performance bottleneck.

### Required Statistics

Record:

```text
parquet_file_count
parquet_total_bytes
parquet_total_rows
parquet_row_group_count
```

Also calculate or preserve the information needed to calculate:

```text
parquet_min_file_bytes
parquet_max_file_bytes
parquet_mean_file_bytes
parquet_median_file_bytes
files_per_million_canonical_rows
```

Statistical aggregation can be finalized in Task 09.

Task 08 must preserve the per-file observations.

### Per-File Inventory

Create:

```text
parquet-files.json
```

For each file, record:

```text
relative path
size_bytes
rows
row_groups
start_timestamp_ns
end_timestamp_ns
```

Include compressed/uncompressed Parquet metadata where reliably available.

### Source of Truth

Use committed manifests and actual Parquet metadata.

Do not estimate file counts from task counts.

Do not assume every task produces one file.

Do not assume every stream produces one file.

### File Fragmentation

Preserve file-size distributions so future comparisons can establish whether a storage refactor reduced fragmentation.

Do not classify every small file as an error.

The final file in a valid output sequence may legitimately be smaller than the configured target.

Do not change Parquet writers during this task.

---

# 15. Environment Provenance

Every benchmark run must preserve enough environment information to support later comparisons.

Write:

```text
environment.json
```

### Software Metadata

Record:

```text
Git commit SHA.
Git branch.
Working-tree dirty status.
Rust compiler version.
Target architecture.
Build profile.
Enabled Cargo features.
Benchmark executable identity.
Normalization executable identity.
```

Where practical, include:

```text
Cargo.lock checksum.
Executable checksum.
```

### Dirty Working Tree

A benchmark may run with uncommitted changes.

Record that fact.

Do not describe a dirty working-tree benchmark as a clean release build.

If the exact source state cannot be reconstructed from commit identity alone, preserve the available limitation.

Do not automatically commit repository changes.

### Hardware Metadata

Record available information about:

```text
CPU model.
Logical CPU count.
Physical CPU count.
Total system memory.
Operating system.
Kernel version.
CPU architecture.
```

Where supported, also record:

```text
CPU frequency information.
CPU power profile.
Filesystem type.
Storage device type.
```

Do not require privileged access.

Do not fail the benchmark solely because optional hardware metadata is unavailable.

### Relevant Environment

Record relevant execution conditions.

Examples:

```text
Benchmark start timestamp.
Execution host platform.
Configured worker count.
Runtime resource limits.
Build profile.
Scheduler mode.
```

Do not dump all environment variables.

Avoid collecting passwords, tokens, database credentials, or unrelated personal information.

Only collect explicitly relevant, nonsecret metadata.

---

# 16. Workload Provenance

Write:

```text
workload.json
```

This document describes exactly what was selected for benchmarking.

### Required Metadata

Record:

```text
Original input path.
Resolved absolute input path.
Input JSON checksum.
Input schema version.
Selected job IDs.
Selected dataset IDs.
Selected task IDs.
Selected stream identities.
Content types.
Resolved profile IDs.
Profile fingerprints.
Source archive paths.
Source archive sizes.
```

Where applicable, preserve:

```text
Source archive members.
Instrument identities.
Source periods.
Source feed types.
```

### Source Checksums

Where practical, record full-file source checksums.

However, do not calculate expensive checksums inside every measured trial.

Calculate or reuse them outside the measured normalization interval.

For very large archives, checksum calculation may be separately controlled.

If only path, size, and modification time were verified, record that weaker identity evidence accurately.

Do not claim full source equivalence without adequate evidence.

### Preserve Original Input

Do not modify the original prepared batch.

The benchmark runner may create isolated execution copies or resolved specifications inside its run directory.

These must remain attributable to the original workload.

---

# 17. Trial Execution Model

Each trial represents one execution of the selected workload under a particular configuration.

### Required Trial Metadata

Record:

```text
trial_id
benchmark_run_id
scheduler
workers
repetition
warmup
effective_configuration
execution_status
start_timestamp
end_timestamp
```

Also preserve:

```text
execution result
performance measurements
output metadata
correctness verification
failure information
```

### Trial States

Use:

```text
planned
running
complete
failed
interrupted
```

These represent benchmark trial lifecycle states.

Do not confuse them with the normalization engine's execution status.

### Important Distinction

A benchmark trial is complete only after the expected normalization execution and required output verification succeed.

An existing output directory is not evidence that a trial completed.

An interrupted trial must not be marked successful.

---

# 18. Warmups and Repetitions

Support configurable warmups and measured repetitions.

### Warmups

Warmups are not included in the primary measured performance sample.

Record their existence and execution status.

Do not present warmup timings as measured repetitions.

Warmups may affect filesystem caches and runtime initialization.

Preserve this fact in benchmark metadata.

### Repetitions

Each measured repetition must have its own persisted result.

Do not save only aggregated averages.

### Execution Order

Preserve the actual order of trials.

Where worker-count order is alternated or randomized to reduce systematic cache bias, record the strategy.

Do not fabricate cold-cache conditions.

### Cache Policy

Record whether the benchmark used:

```text
ordinary operating-system cache behavior
```

or another explicitly controlled policy.

Do not claim cold-cache measurements unless the environment actually establishes them.

Do not require root privileges to clear filesystem caches.

---

# 19. Correctness Verification

**A trial must not be considered a successful benchmark merely because normalization returned without an exception.**

Use the production execution result and committed dataset metadata.

### Required Verification

For every requested job, verify:

- Execution completed successfully.
- A committed dataset exists.
- Manifest exists.
- Manifest job ID matches.
- Manifest dataset ID matches.
- Required source tasks are represented.
- Referenced files exist.
- File sizes match metadata.
- Row counts are consistent.
- Parquet files are readable.
- Required depth auxiliary artifacts exist.
- Integrity status is acceptable under the effective policy.

Reuse existing verification infrastructure where possible.

### Semantic Correctness

Internal consistency is not necessarily semantic correctness.

Preserve available canonical-output checks or deterministic semantic fingerprints where applicable.

Do not use raw Parquet byte equality as the only correctness criterion.

Different physical Parquet layouts may contain equivalent canonical records.

For depth, correctness must preserve source-event order and reconstruction semantics.

### Avoid Expensive Verification Inside Timed Execution

Separate engine execution time from benchmark-harness verification time.

If semantic verification requires rereading output, measure and report that cost separately.

Do not silently include verification overhead in the normalization throughput metric.

### Failed Trials

If normalization or verification fails:

- Mark the trial failed.
- Preserve available diagnostics.
- Preserve the failure reason.
- Do not report successful throughput.
- Do not silently exclude the trial.
- Do not replace the workload.

Partial output may remain available for diagnosis.

Do not represent partial output as a committed successful dataset.

---

# 20. Persistent Trial Results

The previous benchmark implementation wrote its main report only after the experiment finished.

Replace this with incremental persistence.

Every trial must persist its result immediately after completion or failure.

### Required Files

Maintain:

```text
results.jsonl
```

and per-trial:

```text
result.json
```

### JSONL Record

Each completed trial contributes a structured machine-readable record.

Include its identity, status, and available measurements.

Use one consistent versioned serialization model.

### Per-Trial Result

The per-trial result must provide sufficient information to inspect that execution independently.

Include:

```text
trial identity
effective configuration
execution outcome
timing
throughput
CPU metrics
memory metrics
I/O metrics
Parquet statistics
correctness verification
artifact references
failure information
```

Unavailable optional metrics must be represented explicitly.

### Atomic Persistence

Where practical, write finalized JSON documents through atomic replacement.

Do not leave a partially written document that appears valid and complete.

### Interrupted Runs

If the benchmark process stops unexpectedly:

- Preserve previously completed trials.
- Preserve available failed-trial diagnostics.
- Leave unfinished trials identifiable.
- Do not report the entire run as complete.

Task 09 may generate summaries from partially completed runs while clearly identifying their status.

### No Automatic Resume Requirement

Do not implement complicated resume behavior unless the existing infrastructure already supports it.

Persistent partial results are required.

Automatic continuation is optional and outside the core scope.

---

# 21. Dataset Retention Policy

All benchmark metadata and measurements must remain persistent.

Generated canonical datasets may be retained or removed according to an explicit retention policy.

Support:

```text
metadata
full
```

## 21.1 Metadata Retention

Preserve:

- Run metadata.
- Environment metadata.
- Workload provenance.
- Effective configuration.
- Trial results.
- Execution results.
- Dataset manifest metadata.
- Resolved-job provenance.
- Parquet file inventories.
- Correctness verification results.
- Failure diagnostics.
- Logs.

Successful generated datasets may be removed after verification and metadata extraction.

### Required Safety

Do not remove datasets before verification.

Do not delete the only copy of required diagnostic metadata.

Record whether each generated dataset was retained.

For example:

```json
{
  "dataset_retained": false
}
```

Do not leave metadata suggesting that deleted dataset files still exist.

## 21.2 Full Retention

Preserve complete generated datasets.

This includes:

```text
Canonical Parquet files.
Dataset manifests.
Resolved jobs.
Depth event indexes.
Depth reconstruction segments.
Boundary metadata.
Available failure artifacts.
```

Record:

```json
{
  "dataset_retained": true
}
```

### Storage Cost

Full retention across many worker configurations and repetitions can consume substantial disk space.

Do not make full retention mandatory.

Use `metadata` as the default unless an established project convention requires otherwise.

### Safe Cleanup Ownership

Cleanup may remove only generated datasets owned by the current benchmark trial.

Never remove:

- Original source archives.
- Prepared workload input.
- Production datasets.
- Previous benchmark runs.
- Generated test artifacts under `data/tests/`.
- Another trial's datasets.

Do not clear the benchmark output root before execution.

Do not overwrite previous benchmark history.

---

# 22. Logs and Diagnostics

Preserve useful execution diagnostics.

Per trial, record:

```text
stdout.log
stderr.log
```

where applicable.

The production normalization CLI must continue providing machine-readable execution results.

Do not mix benchmark progress messages into captured normalization JSON.

### Terminal Output

The benchmark CLI should display concise progress:

```text
Benchmark run: <id>

Input: <path>
Jobs: <count>
Tasks: <count>

Scheduler: global
Workers: [1, 2, 4, 8]
Warmups: 1
Repetitions: 5

Current trial: workers=4, repetition=2

Completed: <count>
Failed: <count>

Results: data/benchmarks/historical/normalize/runs/<id>/
```

The example is illustrative.

Do not print enormous per-record logs.

Preserve enough information to identify the active trial and locate its artifacts.

---

# 23. Benchmark Metadata Schemas

Define versioned serialization structures for benchmark metadata.

These are benchmark-specific reports, not changes to the normalization batch or dataset manifest protocols.

## 23.1 Run Metadata

Write:

```text
run.json
```

Illustrative structure:

```json
{
  "schema_version": 1,
  "kind": "normalization_benchmark_run",
  "run_id": "benchmark-example",
  "status": "running",
  "input_path": "/data/experiments/normalize.json",
  "scheduler": "global",
  "workers": [1, 2, 4, 8],
  "warmups": 1,
  "repetitions": 5,
  "retention": "metadata"
}
```

The actual schema should also preserve relevant creation timestamps and trial accounting.

Do not claim the run is complete until all required trials have a definitive result.

## 23.2 Environment Metadata

Write:

```text
environment.json
```

Include software, hardware, platform, and relevant runtime information.

Use explicit units and nullable unavailable fields.

## 23.3 Workload Metadata

Write:

```text
workload.json
```

Preserve the selected workload and source provenance.

Do not replace the production batch input with this metadata document.

## 23.4 Trial Result

Write:

```text
trials/<trial-id>/result.json
```

Include the trial's execution status, measurements, verification result, and artifact references.

Use a stable versioned schema.

### Schema Discipline

- Use meaningful field names.
- Use explicit units.
- Preserve exact integer identifiers.
- Avoid untyped arbitrary JSON blobs where a stable structure is appropriate.
- Reject unsupported versions.
- Avoid silently changing serialized meanings.
- Do not confuse missing metrics with zero-valued measurements.

Do not overengineer schema migration for a brand-new benchmark report format.

---

# 24. Statistical Processing Boundary

Task 08 must preserve individual observations.

It may calculate simple immediate progress totals.

However, full statistical summaries belong to Task 09.

### Task 08 Responsibilities

Implement:

- Reliable trial result collection.
- Individual measurement persistence.
- Run/trial accounting.
- Basic result validation.
- Basic completion/failure counts.
- Sufficient metadata for later grouping.

### Task 09 Responsibilities

Implement:

- Mean and median calculations.
- Standard deviation.
- Percentiles.
- Coefficient of variation.
- Worker speedup.
- Parallel efficiency.
- Statistical summary reports.
- Baseline/candidate comparisons.
- Compatibility classifications.
- Comparison reports.

Do not implement the entire Task 09 comparison system here.

The Task 08 data model must support those calculations without rerunning benchmarks.

---

# 25. Lightweight Benchmark Infrastructure Tests

Implement focused tests that do not execute actual benchmark workloads.

### CLI Tests

Verify:

- Required argument validation.
- Absolute input paths.
- Relative input paths.
- Worker-count parsing.
- Duplicate worker rejection.
- Invalid repetition counts.
- Zero warmups.
- Scheduler selection.
- Retention selection.
- Output-root validation.

### Planning Tests

Verify:

- Correct workload selection.
- Correct job and task accounting.
- Correct trial count.
- Correct warmup count.
- Deterministic trial ordering.
- Correct effective configuration.
- No execution during dry-run.

### Metadata Tests

Verify:

- Run metadata serialization.
- Environment metadata serialization.
- Workload provenance serialization.
- Trial result serialization.
- Unsupported schema-version handling.
- Exact integer preservation.
- Nullable metrics.
- Unique run identities.

### Measurement Tests

Use mocked or synthetic measurements.

Verify:

- Wall-time unit handling.
- Throughput calculations.
- CPU time accounting.
- Memory measurement representation.
- I/O counter interpretation.
- Parquet file inventory aggregation.
- Unavailable metric handling.

Do not generate fabricated production benchmark observations.

### Persistence Tests

Verify:

- New run directories are unique.
- Existing runs are never overwritten.
- Trial results persist.
- Failed trials retain diagnostics.
- Interrupted trials remain identifiable.
- Metadata retention preserves required reports.
- Full retention preserves generated dataset references.
- Cleanup cannot escape the current trial's owned directory.

### Mocked Execution

Where useful, introduce a small execution abstraction that allows tests to supply predetermined normalization results.

Keep it minimal.

Do not build an elaborate plugin framework.

Tests must be able to verify orchestration behavior without processing real market-data archives.

---

# 26. Verification Restrictions

**Codex must not execute real performance benchmarks.**

This restriction applies to Task 08 and remains in effect for Task 09.

Do not execute:

```bash
marketforge-benchmark normalize \
  --input <real-workload> \
  --workers 1,4,8,16
```

Do not run:

- Real benchmark trials.
- Worker-scaling experiments.
- Full historical normalization workloads.
- Large archive-processing workloads.
- Real-data warmups.
- Disk I/O stress tests.
- Performance profiling sessions.
- Benchmarks in the background.

Do not generate large Parquet datasets merely to test benchmark infrastructure.

### Authorized Verification

Codex may run:

- Rust formatting checks.
- Rust compilation checks.
- Relevant lint checks.
- Lightweight unit tests.
- Synthetic measurement tests.
- Mocked execution tests.
- CLI help output.
- CLI argument validation.
- Dry-run planning tests.
- Serialization tests.
- Filesystem-isolation tests.

### Important Distinction

A successful compilation proves that the code compiles.

A successful mocked trial proves that the runner can handle the mocked scenario.

A successful dry-run proves that the planner can construct a valid execution plan.

None of these proves actual normalization throughput, memory usage, or performance under real workloads.

Report this limitation explicitly.

---

# 27. Verification Commands

Use the repository's actual Cargo structure.

### Formatting

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

### Compilation

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Compile the relevant benchmark executable or target.

Run relevant lightweight benchmark infrastructure tests.

Verify help output.

Verify that invalid arguments fail clearly.

Where a suitable small valid batch fixture exists, verify `--dry-run` without invoking normalization.

### Do Not Execute Actual Trials

Do not test the runner by processing the real workload.

Do not claim end-to-end performance functionality was measured.

---

# 28. Repository Hygiene

Ignore generated benchmark outputs.

Add an appropriate rule if not already covered:

```gitignore
/data/benchmarks/
```

Do not ignore benchmark source code.

Do not ignore benchmark documentation.

Do not commit generated trial measurements or large datasets.

Do not modify unrelated `.gitignore` rules.

### Working-Tree Safety

Inspect the working tree before modifying files.

Preserve unrelated user changes.

Do not reset the repository.

Do not overwrite existing benchmark runs.

Do not modify production configuration values.

Do not modify source archives.

Do not create commits or branches unless requested.

---

# 29. Implementation Sequence

## Phase A — Audit

Inspect the existing benchmark implementation, normalization interfaces, runtime configuration, and available measurement infrastructure.

Identify reusable components and obsolete dependencies.

## Phase B — CLI and Planning

Implement the new CLI input handling.

Support explicit `--input`, worker configurations, repetitions, warmups, scheduler selection, retention, and dry-run.

Use the production batch loader.

## Phase C — Storage and Identity

Implement persistent benchmark run directories.

Implement run and trial identities.

Define versioned metadata structures.

## Phase D — Workload and Environment Provenance

Capture software, hardware, runtime, profile, and source workload metadata.

Separate expensive provenance collection from timed execution.

## Phase E — Execution Infrastructure

Integrate the benchmark runner with production normalization execution.

Preserve per-job and global scheduling behavior.

Use isolated trial destinations.

Do not execute real trials during implementation.

## Phase F — Measurement Infrastructure

Implement wall-time, throughput, CPU, memory, I/O, and Parquet metadata collection where supported.

Use explicit metric definitions.

Handle unavailable measurements honestly.

## Phase G — Correctness and Retention

Integrate dataset verification.

Implement metadata and full-retention modes.

Preserve failure diagnostics.

## Phase H — Persistence

Persist individual trial results incrementally.

Support interrupted-run visibility.

Do not implement automatic resume unless already available.

## Phase I — Verification

Run formatting, compilation, and lightweight infrastructure tests.

Verify dry-run behavior.

Do not execute actual benchmarks.

Stop after completing Task 08.

---

# 30. Scope Restrictions

## Authorized

This task authorizes:

- Refactoring the normalization benchmark runner.
- Explicit workload input paths.
- Production batch loading.
- Runtime configuration integration.
- Worker-count configuration.
- Scheduler selection.
- Warmups and repetitions.
- Dry-run planning.
- Persistent benchmark directories.
- Run and trial identity management.
- Environment provenance.
- Workload provenance.
- Effective configuration capture.
- Performance measurement infrastructure.
- Per-file Parquet statistics.
- Benchmark correctness verification.
- Incremental trial result persistence.
- Metadata and full-retention modes.
- Lightweight infrastructure tests.
- Minimal benchmark documentation.

## Not Authorized

Do not implement:

- Full statistical summary reports.
- Baseline/candidate comparison reports.
- Statistical significance analysis.
- New normalization algorithms.
- New normalization profiles.
- A new batch input schema.
- Stream-oriented Parquet consolidation.
- Parquet writer redesign.
- Depth reconstruction changes.
- Merge benchmarking.
- Conversion benchmarking.
- PostgreSQL benchmark registration.
- Python Forge orchestration changes.
- A network benchmarking service.
- A TUI redesign.

### Important Boundary

Task 09 will implement statistical summaries and run comparisons using the persisted measurements created here.

Task 10 will begin the stream-oriented output refactor.

Do not implement either task now.

---

# 31. Required Implementation Report

When Task 08 is complete, provide the following.

## Benchmark Architecture

Describe:

- Benchmark runner location.
- Final executable name.
- CLI options.
- Production input loading.
- Scheduler modes.
- Trial execution model.
- Dry-run behavior.

## Storage

Describe:

- Final benchmark directory structure.
- Run ID generation.
- Trial ID generation.
- Persistent metadata.
- Trial result persistence.
- Retention behavior.

## Measurements

List each implemented metric.

For every metric, identify:

- Unit.
- Measurement source.
- Timing boundary where relevant.
- Supported platforms.
- Known limitations.

Do not claim a metric is measured if only its schema field exists.

## Provenance

Describe:

- Git and build identity.
- Hardware identity.
- Effective configuration.
- Workload identity.
- Source archive verification.

## Verification

Report:

- Formatting checks.
- Compilation checks.
- Unit tests executed.
- Mocked execution tests.
- Dry-run tests.
- Tests passed.
- Tests failed.
- Tests blocked by prerequisites.

## Not Executed

Explicitly state:

**Real-data normalization benchmarks were not executed, as instructed.**

## Remaining Runtime Verification

Identify what still requires an explicitly authorized real benchmark execution.

Examples:

- Actual source throughput.
- Worker-scaling behavior.
- Process memory under load.
- CPU utilization under load.
- Physical disk I/O.
- Parquet fragmentation under realistic workloads.
- Measurement overhead.
- Real-data correctness.
- Performance stability across repetitions.

Do not fabricate these measurements.

## Deferred Work

Identify functionality belonging to Task 09 or later tasks.

Do not implement it.

---

# 32. Acceptance Criteria

Task 08 is complete when:

- The benchmark runner uses the production normalization engine.
- Input is supplied explicitly using `--input`.
- Input documents may reside anywhere on the filesystem.
- The authoritative schema-version-3 batch is reused.
- Legacy processing-job directories are no longer required.
- The shared historical runtime configuration is reused.
- Runtime overrides do not modify production configuration.
- No exchange or archive is excluded automatically.
- Gate.io is not hardcoded out.
- Per-job scheduling remains supported.
- Global scheduling remains supported.
- Worker counts are configurable.
- Warmups and repetitions are configurable.
- Dry-run produces an execution plan without processing data.
- Every benchmark run has a unique identity.
- Every trial has a unique identity.
- Previous benchmark runs are never overwritten.
- Results are stored under `data/benchmarks/historical/normalize/`.
- Software and build provenance are captured.
- Hardware and platform provenance are captured where available.
- Source workload identities are preserved.
- Effective runtime configuration is preserved.
- Execution-time measurements have documented boundaries.
- Source-record and canonical-row throughput remain distinct.
- CPU measurements are supported where available.
- Memory measurements are supported where available.
- I/O measurements are supported where available.
- Parquet file and row-group statistics are collected.
- Unavailable measurements are represented honestly.
- Successful benchmark trials require correctness verification.
- Failed trials remain visible.
- Trial results are persisted incrementally.
- Interrupted runs do not appear complete.
- Metadata retention preserves necessary reports.
- Full retention preserves generated datasets.
- Cleanup cannot affect production data or previous benchmark runs.
- Lightweight infrastructure tests pass.
- Relevant Rust compilation checks pass.
- No actual performance benchmarks are executed by Codex.
- No fabricated benchmark results are generated.
- Production normalization behavior remains unchanged.

---

## 33. Stop Condition

**Stop after Task 08 is complete.**

Do not automatically proceed to Task 09.

Do not execute real benchmarks.

Do not implement the Parquet I/O refactor.

The next assignment is:

**Task 09 — Benchmark Statistics, Worker Scaling, and Run Comparisons.**

Task 09 will consume the persistent trial measurements implemented here and provide:

- Statistical summaries.
- Worker-scaling analysis.
- Speedup and parallel efficiency.
- Baseline/candidate comparison.
- Workload compatibility checks.
- Performance regression reporting.
- Human-readable Markdown reports.

It must not require rerunning benchmark workloads merely to compare existing results.

---

**Final objective:** Establish a reproducible normalization benchmarking infrastructure that accepts explicitly selected workloads, records trustworthy performance and environment information, preserves its execution history, and provides the evidence required for later performance comparisons—without running benchmarks during implementation.