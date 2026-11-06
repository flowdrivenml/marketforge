# Task 09 — Benchmark Statistics, Worker Scaling, and Run Comparisons

**Project:** MarketForge  
**Component:** Rust Historical Normalization Benchmarking  
**Priority:** High / Performance Analysis  
**Status:** Approved for implementation  
**Dependencies:** Tasks 01–08  
**Next task:** Stream-Oriented Trade Output and Shared Execution Foundation

## 1. Objective

Complete MarketForge's historical normalization benchmarking system by implementing statistical analysis, worker-scaling evaluation, and reproducible comparisons between benchmark runs.

Task 08 established the infrastructure for:

- Executing controlled normalization benchmarks.
- Collecting individual trial measurements.
- Recording workload identities.
- Capturing hardware and software provenance.
- Recording effective runtime configurations.
- Measuring CPU, memory, I/O, and Parquet output characteristics.
- Verifying dataset correctness.
- Persisting benchmark results.

Task 09 must build on this infrastructure.

**The objective is to transform individual benchmark measurements into reliable evidence about performance, scalability, resource utilization, and the effects of software changes.**

The system must support comparisons such as:

- Before versus after a normalization optimization.
- Different worker counts.
- Different memory budgets.
- Different Parquet sizing configurations.
- Different scheduling strategies.
- Different Git revisions.
- Different machines, with appropriate comparability warnings.
- Different output layouts that preserve equivalent canonical data.

### Critical Restriction

**Do not execute actual performance benchmarks during this task.**

Codex must implement and verify statistical analysis using persisted or synthetic benchmark results.

Real benchmark execution will be authorized separately.

Complete only Task 09.

---

# 2. Architectural Context

The intended workflow is:

```text
Task 08 — Benchmark Execution
             │
             ▼
      Persistent Trial Results
             │
             ├── Execution duration
             ├── Source throughput
             ├── Canonical throughput
             ├── CPU measurements
             ├── Memory measurements
             ├── I/O measurements
             ├── Parquet statistics
             ├── Correctness verification
             ├── Runtime configuration
             ├── Software provenance
             └── Hardware provenance
             │
             ▼
Task 09 — Statistical Analysis
             │
             ├── Validate measurements
             ├── Group comparable trials
             ├── Calculate statistics
             ├── Evaluate worker scaling
             └── Generate summaries
             │
             ▼
      Benchmark Run Summary
             │
             ▼
     Baseline / Candidate Comparison
             │
             ├── Validate comparability
             ├── Compare performance
             ├── Compare resource usage
             ├── Compare Parquet characteristics
             ├── Verify correctness evidence
             └── Identify relevant differences
             │
             ▼
        Comparison Reports
```

**All analysis must operate on persisted benchmark measurements.**

Do not require normalization to execute again when generating a summary or comparing existing runs.

Do not introduce another benchmark execution implementation.

---

# 3. Inspect Task 08 Before Implementation

Before modifying code, inspect the benchmarking infrastructure produced by Task 08.

Relevant locations include:

```text
engine/benches/
engine/src/bin/
engine/src/shared/
engine/src/historical/normalize/

data/benchmarks/historical/normalize/
```

Use the actual repository structure.

Identify the implementations responsible for:

- Run metadata.
- Trial metadata.
- Execution measurements.
- Environment metadata.
- Workload provenance.
- Effective configuration.
- Trial persistence.
- Result serialization.
- Correctness verification.
- Dataset retention.
- Run and trial status.

### Reuse Existing Types

Reuse the existing benchmark result structures where appropriate.

Do not create a second independent representation of the same trial measurements.

Do not duplicate the benchmark metadata parser.

Do not reimplement normalization execution.

Do not introduce an unnecessary statistics framework when straightforward Rust functions are sufficient.

### Important

Task 08 may have produced structures that differ from the examples in its specification.

Inspect the actual implementation before extending it.

Preserve existing versioned contracts where possible.

If essential metadata is missing, extend the smallest appropriate existing structure and document the change.

---

# 4. Benchmark Directory Structure

Preserve the established directory layout:

```text
data/benchmarks/
└── historical/
    ├── normalize/
    │   ├── runs/
    │   │   ├── <benchmark-run-id-A>/
    │   │   └── <benchmark-run-id-B>/
    │   │
    │   └── comparisons/
    │       ├── <comparison-id-A>/
    │       └── <comparison-id-B>/
    │
    ├── merge/
    └── convert/
```

Task 09 operates on:

```text
data/benchmarks/historical/normalize/
```

Do not implement merge or conversion benchmarking.

## 4.1 Per-Run Statistical Reports

Each benchmark run should contain:

```text
<benchmark-run-id>/
├── run.json
├── environment.json
├── workload.json
├── effective-config.toml
├── results.jsonl
├── summary.json
├── summary.md
└── trials/
    └── ...
```

The statistical summary is derived from persisted trial results.

`summary.json` is the authoritative machine-readable aggregate report.

`summary.md` is a human-readable representation of the same results.

Do not maintain separate competing summary calculations for JSON and Markdown output.

## 4.2 Comparison Reports

Store comparisons under:

```text
data/benchmarks/historical/normalize/comparisons/<comparison-id>/
```

Each comparison contains:

```text
<comparison-id>/
├── comparison.json
└── comparison.md
```

The comparison must identify the exact benchmark runs being compared.

Do not overwrite previous comparisons automatically.

### Path Independence

Support reading benchmark runs using explicit filesystem paths.

Do not require comparison inputs to be located inside the default benchmark directory.

The default directory remains the preferred location for generated results.

The user must be able to compare previously exported or relocated benchmark runs.

Resolve artifact references according to the established Task 08 contract.

---

# 5. Statistical Analysis Input

The statistical analyzer consumes completed measured trials.

Relevant trial information includes:

```text
trial_id
run_id
scheduler
workers
repetition
warmup
status
effective_configuration
execution_wall_seconds
source_records_per_second
canonical_rows_per_second
peak_rss_bytes
CPU measurements
I/O measurements
Parquet measurements
correctness verification
```

Use the actual Task 08 serialization models.

### Trial Eligibility

A trial is eligible for successful performance statistics only when:

1. It is a measured repetition, not a warmup.
2. Execution completed successfully.
3. Required correctness verification passed.
4. The metric being analyzed is available and valid.
5. The trial belongs to the intended experimental group.

A failed trial must remain visible in the report.

It must not contribute partial processing measurements to successful throughput statistics.

### Warmups

Exclude warmups from primary statistical calculations.

Preserve:

```text
warmups_planned
warmups_completed
warmups_failed
```

or equivalent existing metadata.

Do not silently treat failed warmups as successful measured repetitions.

If a failed required warmup prevents a configuration from reaching its intended measurement state, report the configuration as incomplete or failed.

### Missing Trials

A planned trial without a definitive result must not be treated as a successful observation.

Preserve explicit accounting for:

```text
planned
completed
failed
interrupted
missing
```

Use existing lifecycle states where available.

Do not fabricate observations to satisfy the expected repetition count.

---

# 6. Statistical Grouping

Before calculating statistics, group measurements by comparable experimental configuration.

A group must preserve relevant dimensions such as:

```text
Workload identity.
Scheduler mode.
Worker count.
Effective memory budget.
Scratch budget.
Parquet settings.
Integrity policy.
Measurement definition.
```

Different configurations must not be combined into one undifferentiated average.

### Example

Suppose one benchmark run contains:

```text
workers = 1
workers = 2
workers = 4
workers = 8
```

with five measured repetitions each.

The analyzer should produce four independent groups:

```text
Group A: workers=1, repetitions=5
Group B: workers=2, repetitions=5
Group C: workers=4, repetitions=5
Group D: workers=8, repetitions=5
```

Do not average all twenty measurements and report one normalization throughput result.

### Scheduler Separation

Keep:

```text
scheduler = per-job
```

separate from:

```text
scheduler = global
```

unless explicitly performing a scheduler-mode comparison.

Do not assume these scheduling modes have identical timing characteristics.

### Per-Job Versus Session Measurements

Use complete workload/session measurements for primary scaling comparisons.

For per-job scheduling, distinguish individual job durations from the complete sequential session duration.

For global scheduling, do not fabricate individual job durations that were never measured.

Do not compare an individual job duration against a complete multi-job session duration.

---

# 7. Required Statistical Measures

For each eligible measurement group, calculate:

| Statistic | Meaning |
|---|---|
| Sample count | Number of valid measured observations |
| Minimum | Smallest observed value |
| Maximum | Largest observed value |
| Mean | Arithmetic average |
| Median | Central observed tendency |
| Standard deviation | Variation between observations |
| P95 | 95th-percentile value |
| Coefficient of variation | Relative variability |

Use consistent and documented definitions.

## 7.1 Mean

For observations:

```text
x1, x2, ..., xn
```

calculate:

```text
mean = sum(xi) / n
```

Do not include missing or failed measurements.

## 7.2 Median

Sort observations numerically.

For an odd number of observations, select the middle value.

For an even number, calculate the average of the two middle values.

The median should be the primary statistic for ordinary benchmark comparisons.

## 7.3 Standard Deviation

Use the sample standard deviation for repeated benchmark observations:

```text
s = sqrt(sum((xi - mean)^2) / (n - 1))
```

For fewer than two observations, report standard deviation as unavailable.

Do not silently substitute zero.

Use a numerically stable implementation.

## 7.4 P95

Implement a consistent percentile calculation.

Use the nearest-rank definition unless the existing project establishes another explicit percentile convention:

```text
rank = ceil(0.95 * n)
```

with one-based indexing into the sorted observations.

Document the selected definition in the report contract.

Do not mix percentile algorithms across reports.

## 7.5 Coefficient of Variation

Calculate:

```text
CV = standard_deviation / abs(mean)
```

Only calculate this when:

- At least two valid observations exist.
- The mean is nonzero.
- The metric's scale and interpretation make the ratio meaningful.

Report unavailable values as `null`.

Do not fabricate zero variability.

## 7.6 Numeric Validation

Reject or explicitly exclude invalid numeric observations.

Examples:

```text
NaN
Infinity
Negative execution duration
Negative memory usage
Negative byte counts
```

Zero is valid for some counters but invalid as a denominator for throughput or speedup calculations.

Do not silently replace invalid measurements with arbitrary values.

Preserve the relevant trial ID in diagnostic errors.

---

# 8. Primary Performance Statistics

Calculate statistical summaries for the following measurements where available.

## 8.1 Execution Time

```text
execution_wall_seconds
```

This is the primary performance metric.

Report:

```text
minimum
maximum
mean
median
standard_deviation
p95
coefficient_of_variation
```

Do not mix engine execution time with benchmark-harness verification time.

## 8.2 Source Throughput

```text
source_records_per_second
```

This measures the rate at which source records are processed.

Preserve the distinction between source records and canonical rows.

## 8.3 Canonical Throughput

```text
canonical_rows_per_second
```

This measures canonical output production.

For depth, canonical level rows are not interchangeable with source depth messages.

## 8.4 Input and Output Throughput

Where measured:

```text
input_bytes_per_second
output_bytes_per_second
```

Do not compare compressed archive bytes to decoded input bytes as though they were the same measurement.

## 8.5 CPU

Summarize available CPU measurements:

```text
process_cpu_user_seconds
process_cpu_system_seconds
process_cpu_total_seconds
```

Where valid, summarize process CPU utilization according to the documented measurement boundary.

Do not clamp multithreaded utilization to 100%.

## 8.6 Memory

Summarize:

```text
peak_rss_bytes
```

and applicable engine-accounted memory metrics.

Do not confuse:

```text
Configured memory budget.
Estimated memory use.
Accounted memory use.
Actual process RSS.
```

These represent different quantities.

## 8.7 I/O

Summarize available logical and physical read/write measurements.

Preserve measurement-source metadata.

Do not compare device-level physical I/O with process-level logical I/O under the same field name.

---

# 9. Aggregate Throughput Correctly

A major requirement is to avoid misleading throughput aggregation.

### Incorrect Calculation

Do not calculate complete-workload throughput by simply averaging individual job throughputs:

```text
average(job_1_records_per_second,
        job_2_records_per_second)
```

This ignores differences in job size and execution duration.

### Correct Session Throughput

For a completed workload/session:

```text
session_throughput =
    total_source_records / session_execution_wall_seconds
```

For canonical output:

```text
canonical_throughput =
    total_canonical_rows / session_execution_wall_seconds
```

### Multiple Repetitions

Preserve each repetition's throughput as an individual observation.

For a useful additional aggregate, calculate:

```text
pooled_throughput =
    sum(records_across_trials) /
    sum(execution_seconds_across_trials)
```

Label this separately from:

```text
mean_trial_throughput
median_trial_throughput
```

These quantities are not mathematically identical.

Do not silently use one as the other.

### Mixed Workloads

A mixed trade/depth workload may contain substantially different work per source record.

Report its throughput as aggregate workload throughput, not as a universally comparable measure of normalization efficiency.

Preserve per-content-type and per-stream information where the underlying measurements support it.

Do not create meaningless cross-workload throughput rankings.

---

# 10. Worker-Scaling Analysis

Implement worker-scaling analysis for comparable benchmark configurations.

The primary objective is to measure how execution duration and throughput change with worker count.

### Baseline

Prefer one worker as the scaling baseline.

For example:

```text
workers = 1
workers = 2
workers = 4
workers = 8
workers = 16
```

If the one-worker configuration is absent, do not silently invent one.

A different explicitly selected baseline may be supported.

Record the baseline selection.

## 10.1 Speedup

Calculate:

```text
speedup =
    baseline_median_execution_seconds /
    candidate_median_execution_seconds
```

Example:

```text
1 worker median: 100 seconds
4 workers median: 30 seconds

speedup = 100 / 30 = 3.33x
```

Only calculate speedup when the workloads and measurement definitions are comparable.

## 10.2 Parallel Efficiency

For a one-worker baseline:

```text
parallel_efficiency =
    speedup / worker_count
```

Example:

```text
workers = 4
speedup = 3.33

parallel_efficiency = 0.8325
```

Display efficiency as a percentage where appropriate:

```text
83.25%
```

For a non-one-worker baseline, use the appropriate relative worker-count ratio and label the result as relative efficiency.

Do not apply the one-worker formula unchanged.

## 10.3 Throughput Scaling

Also report:

```text
source throughput ratio
canonical throughput ratio
```

Example:

```text
1 worker: 1,000,000 records/s
4 workers: 3,000,000 records/s

throughput ratio = 3.0x
```

Do not assume throughput ratio always equals duration-based speedup if work quantities or measurement boundaries differ.

## 10.4 Scaling Report

For every worker configuration, present:

```text
workers
sample count
median execution time
execution variability
median source throughput
median canonical throughput
speedup
parallel efficiency
peak memory
CPU utilization where available
```

### Missing Baseline

If a valid baseline is unavailable:

- Preserve the raw statistics.
- Report speedup as unavailable.
- Report efficiency as unavailable.
- Explain the missing baseline.

Do not display:

```text
0.00x
```

as though it represented a measured speedup.

---

# 11. Parquet Output Analysis

Parquet output characteristics are a major reason for implementing this benchmarking system.

Future MarketForge changes will consolidate compatible source archives into fewer, larger output files.

The comparison system must be able to evaluate the results without assuming that fewer files automatically means better performance.

### Required Statistics

Analyze:

```text
parquet_file_count
parquet_total_bytes
parquet_total_rows
parquet_row_group_count
parquet_min_file_bytes
parquet_max_file_bytes
parquet_mean_file_bytes
parquet_median_file_bytes
files_per_million_canonical_rows
```

Use the measurements persisted by Task 08.

### Fragmentation Metric

Calculate:

```text
files_per_million_canonical_rows =
    parquet_file_count * 1,000,000 /
    parquet_total_rows
```

Only calculate it when the denominator is positive.

Do not report a meaningful fragmentation ratio for an empty dataset.

### File-Size Distribution

Preserve information about the distribution of actual Parquet file sizes.

A large number of very small files may be relevant evidence of output fragmentation.

However, do not classify small files as incorrect merely because they are below the configured file target.

The final part of a stream may legitimately be smaller.

### Repeated Trials

If a benchmark repetition produces a different number of files, preserve that observation.

Do not silently assume file counts must be identical.

Report variability where it occurs.

### Future Consolidation Comparisons

The analysis must support comparisons such as:

```text
Before:
    1,200 Parquet files
    20 GiB total output
    120 seconds execution

After:
    80 Parquet files
    19 GiB total output
    75 seconds execution
```

These are illustrative values.

The report should calculate:

- File-count reduction.
- Output-size change.
- Execution-time change.
- Throughput change.
- Relevant CPU and memory changes.
- Relevant I/O changes.

It must not automatically conclude that reduced file count caused the runtime improvement.

Causal explanations require supporting evidence.

---

# 12. Human-Readable Benchmark Summary

Generate:

```text
summary.md
```

from the authoritative:

```text
summary.json
```

The Markdown report should be suitable for inspection inside the repository or a Markdown viewer.

Use concise explanatory sections and readable tables.

## 12.1 Required Sections

Include:

- Benchmark identification.
- Execution status.
- Software version.
- Hardware summary.
- Workload summary.
- Effective runtime configuration.
- Scheduler mode.
- Worker configurations.
- Repetition accounting.
- Execution-time statistics.
- Throughput statistics.
- Worker scaling.
- CPU and memory results.
- I/O results.
- Parquet output statistics.
- Correctness verification.
- Measurement limitations.

### Example Table

```text
| Workers | Median Time | Records/s | Speedup | Efficiency |
|---------|-------------|-----------|---------|------------|
| 1       | 100.0s      | 1.0M      | 1.00x   | 100%       |
| 2       | 55.0s       | 1.8M      | 1.82x   | 91%        |
| 4       | 30.0s       | 3.3M      | 3.33x   | 83%        |
| 8       | 22.0s       | 4.5M      | 4.55x   | 57%        |
```

These values are illustrative.

Do not generate fabricated performance tables during implementation.

### Incomplete Runs

If the benchmark run contains missing or failed trials, display that status prominently.

Do not label an incomplete experiment as fully successful.

---

# 13. Baseline and Candidate Comparison

Implement comparison between two existing benchmark runs.

### Intended CLI

Use an interface conceptually equivalent to:

```bash
marketforge-benchmark compare \
  --baseline /path/to/baseline-run \
  --candidate /path/to/candidate-run
```

This is the preferred behavior.

Adapt command naming to the established Task 08 CLI structure.

The inputs may be explicit paths or supported run IDs.

Do not require the original benchmark source input to remain in the same location if the persisted run contains sufficient provenance.

### Comparison Semantics

The baseline represents the reference implementation or configuration.

The candidate represents the implementation or configuration being evaluated.

Do not assume the candidate is better.

The comparison must objectively report differences.

### Directionality

For execution time:

```text
lower is generally better
```

For throughput:

```text
higher is generally better
```

For memory:

```text
lower may be preferable, depending on the objective
```

For Parquet file count:

```text
fewer files may reduce fragmentation
```

For output size:

```text
smaller is not automatically better if correctness or encoding differs
```

Do not reduce multidimensional performance to an unexplained single score.

---

# 14. Comparison Compatibility Validation

**This is a critical requirement.**

Two benchmark runs must not automatically be considered comparable merely because both contain execution-duration measurements.

Before calculating optimization conclusions, inspect the relevant provenance.

### Required Dimensions

Compare:

| Dimension | Required consideration |
|---|---|
| Input workload | Same intended jobs, tasks, and source corpus |
| Source identities | Equivalent archive bytes or adequately verified provenance |
| Profiles | Same effective interpretation, unless intentionally changed |
| Canonical correctness | Equivalent required semantic output |
| Hardware | Same machine or explicitly identified hardware differences |
| Build configuration | Comparable optimization profile and features |
| Scheduler | Same mode unless scheduler comparison is intentional |
| Runtime resources | Same settings except intentional variables |
| Measurement definitions | Equivalent timing and metric semantics |
| Cache conditions | Comparable or explicitly qualified |
| Integrity policy | Equivalent validation requirements |
| Software revision | Difference identified and documented |

### Important Distinction

Some differences are intentional.

For example:

**Software optimization comparison**

```text
Baseline Git revision: A
Candidate Git revision: B

Same:
    workload
    hardware
    scheduler
    runtime resources
    integrity policy
```

This is a meaningful comparison when correctness and measurement boundaries also match.

**Worker-scaling comparison**

```text
Baseline workers: 1
Candidate workers: 8

Same:
    workload
    software
    hardware
    scheduler
    other runtime settings
```

The worker count is the intentional variable.

**Parquet configuration comparison**

```text
Baseline file target: 256 MiB
Candidate file target: 512 MiB
```

This may be meaningful when the remaining relevant conditions match.

### Do Not Reject Every Difference

The comparison system must distinguish intentional experimental variables from accidental differences.

Do not require byte-for-byte equality of all runtime configuration documents when the experiment intentionally changes one setting.

Do not automatically treat different Git commits as incompatible.

Software changes are often the purpose of benchmarking.

### Workload Relocation

Two workloads may use different absolute filesystem paths while referring to identical source bytes.

Do not reject them solely because their paths differ.

Use source identities, checksums, task semantics, and profile provenance.

When evidence of identity is insufficient, report the limitation.

---

# 15. Comparison Classification

Assign every comparison one of three classifications:

```text
comparable
comparable_with_caveats
incomparable
```

## 15.1 Comparable

Use when:

- Workload identity is adequately established.
- Required correctness evidence passes.
- Measurement definitions match.
- Relevant hardware and environment conditions are compatible.
- All nonintentional experimental dimensions are sufficiently controlled.

## 15.2 Comparable With Caveats

Use when the comparison remains useful but has identified limitations.

Examples:

- Different CPU power profiles.
- Different hardware with an explicitly cross-machine objective.
- Unverified filesystem-cache equivalence.
- Missing optional resource measurements.
- Dirty working-tree provenance that limits exact reproducibility.
- Source identity based on weaker metadata rather than verified hashes.

Explain each caveat.

Do not silently classify such results as fully controlled.

## 15.3 Incomparable

Use when a material difference prevents a meaningful performance conclusion.

Examples:

- Different source corpora in a supposed software-regression comparison.
- Different measurement boundaries.
- Invalid required correctness evidence.
- Incompatible source interpretations.
- Missing essential execution measurements.
- Unresolved differences in required integrity policy.
- Unverifiable workload identity when equivalence is required.

Do not produce authoritative speedup or regression conclusions from incomparable runs.

Diagnostic side-by-side values may still be displayed when clearly labeled as noncomparable.

### Required Report

Every comparison must explain:

```text
Classification.
Reasons.
Intentional variables.
Uncontrolled variables.
Missing evidence.
Metrics eligible for comparison.
Metrics that cannot be compared.
```

---

# 16. Correctness Before Performance

A candidate implementation must not be declared a successful optimization if it produces semantically incorrect data.

### Required Verification

Use the correctness evidence persisted by Task 08.

Where available, verify:

- Canonical record counts.
- Canonical schema compatibility.
- Source-task coverage.
- Instrument identity.
- Canonical value equivalence.
- Relevant ordering semantics.
- Integrity outcomes.
- Depth event indexing.
- Depth reconstruction semantics.

### Canonical Equivalence

Different physical Parquet files can contain equivalent canonical data.

For example:

```text
Baseline:
    100 files

Candidate:
    10 files
```

This difference alone must not make the results incompatible.

Compare logical output semantics where the persisted verification evidence supports doing so.

### Source-to-Output Identity

Preserve enough information to distinguish:

```text
Same source workload.
Same canonical records.
Different physical storage layout.
```

from:

```text
Same source workload.
Different canonical interpretation.
```

The second case may indicate a correctness regression.

### Evidence Limitations

A successful manifest verification does not automatically prove complete semantic equivalence.

If the persisted results contain only structural verification, report that limitation.

Do not invent semantic fingerprints after generated datasets have been deleted.

Do not reconstruct unavailable correctness evidence from aggregate row counts alone.

### Invalid Correctness

If a candidate fails required correctness verification:

- Report the mismatch.
- Preserve available performance measurements as diagnostics.
- Mark optimization conclusions invalid.
- Do not report the candidate as an accepted performance improvement.

---

# 17. Performance Difference Calculations

For comparable baseline and candidate measurements, calculate absolute and relative differences.

## 17.1 Absolute Difference

For a metric:

```text
difference = candidate - baseline
```

For execution duration:

```text
baseline = 100 seconds
candidate = 80 seconds

difference = -20 seconds
```

## 17.2 Percentage Change

Calculate:

```text
percentage_change =
    (candidate - baseline) / baseline * 100
```

Only when the baseline is nonzero and the ratio is meaningful.

For duration:

```text
(80 - 100) / 100 * 100 = -20%
```

This means execution duration decreased by 20%.

## 17.3 Speedup

For duration-based speedup:

```text
speedup = baseline_duration / candidate_duration
```

Example:

```text
100 / 80 = 1.25x
```

### Important

A 20% reduction in execution time corresponds to a 1.25x speedup.

Do not incorrectly report it as a 20% throughput improvement.

### Metric Direction

Label whether a change represents:

```text
improvement
regression
trade-off
no established conclusion
```

Only when the direction and comparison validity support that interpretation.

Do not present tiny noisy changes as definitive improvements.

---

# 18. Resource Trade-Off Analysis

Optimization decisions should consider several dimensions.

For example:

```text
Candidate:
    30% faster
    80% more memory
    90% fewer Parquet files
```

This is a meaningful trade-off.

Do not automatically accept or reject it.

The report should present the measurements.

### Memory

Compare peak RSS when both measurements use compatible definitions.

Do not compare measured RSS against an estimated resource budget as though they were the same metric.

### CPU

Compare CPU time separately from wall time.

An implementation may reduce wall time while consuming more total CPU.

This is not necessarily a contradiction.

### I/O

Compare logical and physical I/O separately.

Do not claim that reduced source read bytes prove reduced physical disk I/O unless the measurement supports that conclusion.

### Output Fragmentation

Compare:

```text
Parquet file count.
Total Parquet bytes.
Median file size.
Row-group count.
Files per million canonical rows.
```

### Summary

Highlight significant measured trade-offs.

Do not generate unsupported explanations about bottlenecks.

---

# 19. Statistical Confidence and Noise

Benchmark measurements can vary because of:

- Filesystem caching.
- CPU frequency changes.
- Thermal conditions.
- Background processes.
- Disk contention.
- Scheduler variation.
- Memory pressure.

The reporting system must expose measurement variability.

### Minimum Requirements

Use:

```text
Median.
Standard deviation.
Coefficient of variation.
Sample count.
```

where meaningful.

Do not claim a performance difference is statistically significant merely because the candidate median is lower.

### Optional Confidence Intervals

Confidence intervals may be implemented if the methodology is straightforward, documented, and supported by adequate observations.

Do not introduce an unnecessarily complicated statistical package.

Do not require confidence intervals to produce ordinary benchmark summaries.

### Small Samples

If a configuration contains only one valid observation:

- Report the observation.
- Report sample count `1`.
- Do not fabricate standard deviation.
- Do not claim strong statistical confidence.

### Regression Interpretation

A small apparent regression within the observed variability should be reported cautiously.

For example:

```text
Candidate median duration: 1.5% higher.
Observed trial variability: 4%.
```

This is evidence of a measured difference, but not sufficient evidence of a reliable performance regression.

Do not invent an automatic statistical-significance conclusion.

---

# 20. Comparison Report Structure

Generate:

```text
comparison.json
comparison.md
```

## 20.1 Machine-Readable Comparison

The JSON must contain:

```text
schema_version
comparison_id
baseline_run_id
candidate_run_id
comparison_status
comparability
comparability_reasons
intentional_variables
configuration_differences
workload_verification
correctness_verification
metric_comparisons
measurement_limitations
```

Use typed structures.

Preserve existing versioning conventions from Task 08.

### Metric Comparison

For each supported metric, preserve:

```text
metric name
unit
baseline summary
candidate summary
absolute difference
percentage difference
ratio or speedup where applicable
availability
comparability status
```

Do not force every metric to have a ratio when its mathematical meaning does not support one.

Do not calculate differences from unavailable values.

### Report Versioning

The comparison JSON should have its own schema version.

Do not reuse the normalization batch version.

Do not reuse the dataset manifest version.

---

# 21. Human-Readable Comparison Report

Generate:

```text
comparison.md
```

Use documentation-ready Markdown.

### Required Sections

#### Comparison Identity

Include:

```text
Comparison ID.
Baseline run.
Candidate run.
Creation timestamp.
```

#### Comparability

Include the comparison classification and its reasons.

#### Software and Environment

Show relevant software, hardware, and configuration differences.

#### Workload

Show the selected source corpus and whether workload equivalence was established.

#### Correctness

Show whether the candidate's output correctness evidence is compatible with the baseline.

#### Performance

Compare:

```text
Median execution time.
Source throughput.
Canonical throughput.
Speedup.
```

#### Resource Usage

Compare:

```text
Peak memory.
CPU time.
I/O measurements.
```

where available.

#### Parquet Output

Compare:

```text
File counts.
Output bytes.
File-size distribution.
Row-group counts.
```

#### Statistical Variability

Show sample counts and relevant variability.

#### Conclusions

Summarize supported findings without inventing explanations.

### Example Comparison Table

```text
| Metric             | Baseline | Candidate | Change  |
|--------------------|----------|-----------|---------|
| Median duration    | 100.0s   | 75.0s     | -25.0%  |
| Source records/s   | 1.0M     | 1.33M     | +33.0%  |
| Peak RSS           | 8.0 GiB  | 10.0 GiB  | +25.0%  |
| Parquet files      | 1,200    | 80        | -93.3%  |
| Parquet bytes      | 20 GiB   | 19 GiB    | -5.0%   |
```

These values are illustrative.

Do not populate reports with fabricated measurements during implementation.

---

# 22. CLI Interface

Extend the Task 08 benchmark CLI.

Support a summary command conceptually equivalent to:

```bash
marketforge-benchmark summarize \
  --run /path/to/benchmark-run
```

Support comparison:

```bash
marketforge-benchmark compare \
  --baseline /path/to/baseline-run \
  --candidate /path/to/candidate-run
```

Use naming consistent with the existing CLI implementation.

### Summary Behavior

The summary command must:

1. Load persisted run metadata.
2. Load individual trial results.
3. Validate the measurements.
4. Identify incomplete or failed trials.
5. Group eligible observations.
6. Calculate statistics.
7. Calculate worker-scaling metrics.
8. Write `summary.json`.
9. Write `summary.md`.

It must not execute normalization.

### Comparison Behavior

The comparison command must:

1. Load baseline metadata and summaries.
2. Load candidate metadata and summaries.
3. Validate or regenerate derived summaries when appropriate.
4. Validate workload and environment comparability.
5. Validate available correctness evidence.
6. Compare eligible metrics.
7. Write comparison reports.
8. Print a concise terminal summary.

It must not execute normalization.

### Output Safety

Summary regeneration may replace derived summary files within the selected run, but must not modify:

```text
run.json
environment.json
workload.json
results.jsonl
trials/
```

Comparison generation must create a unique comparison directory.

Do not overwrite previous comparisons.

---

# 23. Deterministic Report Generation

Statistical summaries must be reproducible from the same persisted observations.

### Requirements

- Deterministic grouping.
- Deterministic sorting.
- Stable metric names.
- Stable units.
- Stable report formatting.
- Stable statistical definitions.
- Consistent null handling.

Do not depend on filesystem directory iteration order.

Do not depend on hash-map iteration order when serializing report tables.

### Floating-Point Representation

Use adequate precision for statistical values.

Avoid unnecessary rounding in machine-readable JSON.

Formatting and rounding belong primarily to human-readable Markdown output.

For example:

```text
Machine-readable:
    1.234567891

Human-readable:
    1.235
```

Do not round inputs before calculating statistics.

### Idempotency

Running the summary command twice against unchanged trial data must produce equivalent statistical values.

Creation timestamps or similar presentation metadata may differ only where explicitly documented.

Do not duplicate trial records during regeneration.

---

# 24. Persisted Result Recovery

Task 08 persists results incrementally.

Task 09 must be able to analyze runs that were interrupted.

### Required Behavior

- Load completed trial results.
- Identify failed trials.
- Identify interrupted trials.
- Identify missing planned trials.
- Report incomplete repetition groups.
- Preserve valid completed observations.
- Avoid fabricating missing results.

### Incomplete Run Summary

An incomplete benchmark may still have a useful diagnostic summary.

However, label it clearly:

```text
status = incomplete
```

or use the appropriate established benchmark-report status.

Do not treat it as a complete controlled experiment.

### Duplicate Trial Records

Inspect the Task 08 JSONL persistence behavior.

If a trial appears more than once because of recovery or repeated writes, apply the established unambiguous identity/version policy.

Reject conflicting duplicate finalized results.

Do not silently count the same trial twice.

### Corrupt Report Files

Invalid or truncated JSON must not be interpreted as successful measurements.

Report the specific affected artifact and trial.

Do not silently ignore corrupt records unless the established persistence contract explicitly supports recovery from a truncated final append.

---

# 25. Testing Strategy

All tests for this task must use lightweight synthetic or persisted fixture measurements.

**Do not execute the normalization engine to generate performance observations.**

Create deterministic fixtures that exercise the statistical functions and report generation.

## 25.1 Statistical Tests

Verify:

- Mean.
- Median.
- Minimum.
- Maximum.
- Sample standard deviation.
- P95.
- Coefficient of variation.
- Empty sample handling.
- Single-observation handling.
- Invalid numeric input.
- Missing values.

Use known expected results.

Do not generate expected values using the same function being tested.

## 25.2 Throughput Tests

Verify:

- Correct session throughput.
- Correct canonical throughput.
- Pooled throughput.
- Mean trial throughput.
- Median trial throughput.
- No accidental averaging of unequal job throughputs.
- Invalid zero-duration handling.

## 25.3 Worker-Scaling Tests

Verify:

- One-worker baseline.
- Two-worker scaling.
- Four-worker scaling.
- Missing baseline.
- Failed baseline.
- Equal-duration configurations.
- Slower candidate.
- Non-one-worker relative baseline where supported.
- Parallel efficiency calculation.

## 25.4 Comparability Tests

Verify:

- Identical workloads.
- Different absolute paths with identical verified source content.
- Different source corpora.
- Different integrity policies.
- Different scheduler modes.
- Different worker counts.
- Different Git revisions.
- Different hardware.
- Different timing definitions.
- Missing optional metrics.
- Missing essential metrics.
- Failed correctness verification.
- Insufficient source identity evidence.

Every test must assert the appropriate compatibility classification.

## 25.5 Comparison Mathematics

Verify:

- Absolute differences.
- Percentage changes.
- Duration speedup.
- Throughput ratios.
- Memory changes.
- File-count reduction.
- Output-size changes.
- Zero baseline handling.
- Negative performance changes.
- Unavailable metric handling.

## 25.6 Report Serialization

Verify:

- Valid `summary.json`.
- Valid `comparison.json`.
- Schema version.
- Required fields.
- Deterministic ordering.
- Exact run identities.
- Nullable metrics.
- No NaN or Infinity serialization.

## 25.7 Markdown Reports

Verify that generated Markdown contains the required sections and tables.

Do not require pixel-perfect formatting.

Check meaningful content and correct values.

## 25.8 Interrupted Runs

Verify that an incomplete run:

- Preserves completed trials.
- Identifies missing trials.
- Identifies failed trials.
- Does not claim full completion.
- Produces correct available statistics.
- Does not fabricate absent measurements.

## 25.9 Filesystem Safety

Verify:

- Summary generation does not modify original trial results.
- Existing comparisons are not overwritten.
- Unique comparison directories are created.
- Explicit external run paths are accepted.
- Invalid paths fail safely.
- Report generation cannot escape assigned output directories.

---

# 26. Verification Requirements

Run formatting, compilation, and lightweight tests.

### Formatting

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

### Compilation

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Also compile the relevant benchmark executable or target.

### Unit Tests

Run focused tests for:

```text
Statistics.
Throughput aggregation.
Worker scaling.
Comparison validation.
Report serialization.
Markdown generation.
Interrupted-run analysis.
Filesystem safety.
```

### CLI Tests

Verify help output and argument validation.

Use synthetic persisted benchmark runs to verify summary and comparison commands.

Do not execute normalization to create those fixtures.

### Benchmark Restriction

Do not run:

```bash
marketforge-benchmark normalize \
  --input <real-workload> \
  --workers 1,4,8
```

Do not run real worker-scaling experiments.

Do not generate large Parquet datasets.

Do not perform CPU, memory, or disk stress tests.

Do not run benchmarks in the background.

---

# 27. Performance Analysis Restrictions

The comparison system must not invent explanations for observed results.

For example:

```text
Candidate produced 90% fewer Parquet files.
Candidate completed 25% faster.
```

These observations do not establish that fewer files caused the speedup.

The report may identify a plausible relationship, but must distinguish inference from measured fact.

Likewise:

```text
CPU utilization decreased.
```

does not independently prove an I/O bottleneck.

Avoid unsupported causal conclusions.

### No Automatic Optimization Decisions

Do not automatically alter:

```text
workers
memory budgets
Parquet targets
scheduler policies
```

based on the benchmark summary.

This system analyzes results.

It does not tune the production engine.

Do not implement an automatic optimizer.

---

# 28. Scope Restrictions

## Authorized

This task authorizes:

- Statistical aggregation.
- Deterministic measurement grouping.
- Mean and median calculations.
- Standard deviation and percentiles.
- Coefficient of variation.
- Source and canonical throughput summaries.
- Worker-scaling analysis.
- Speedup and parallel efficiency.
- Parquet file-distribution analysis.
- Benchmark run summaries.
- Baseline/candidate comparisons.
- Workload compatibility validation.
- Correctness compatibility checks.
- Resource trade-off reporting.
- Machine-readable JSON reports.
- Human-readable Markdown reports.
- Summary and comparison CLI commands.
- Interrupted-run analysis.
- Lightweight statistical and report tests.
- Minimal benchmark documentation.

## Not Authorized

Do not implement:

- New benchmark execution strategies.
- New normalization algorithms.
- New source-format support.
- A new batch protocol.
- A new runtime configuration architecture.
- New Parquet writers.
- Stream-oriented output consolidation.
- New depth reconstruction behavior.
- New merge or conversion functionality.
- PostgreSQL benchmark registration.
- Python Forge integration.
- Automatic performance tuning.
- A network benchmarking API.
- A TUI redesign.

### Important Boundary

Task 08 collects and persists measurements.

Task 09 analyzes and compares those measurements.

Do not duplicate Task 08's measurement collection or execution orchestration.

The next tasks will modify normalization output architecture.

Those changes must remain separate from statistical reporting.

---

# 29. Working-Tree Safety

Inspect the working tree before modifying files.

Preserve unrelated user changes.

Do not reset the repository.

Do not overwrite benchmark trial measurements.

Do not modify existing source archives.

Do not modify production resource configuration.

Do not delete previous benchmark runs.

Do not modify real benchmark results merely to make comparisons succeed.

Do not create commits or branches unless requested.

Prefer small, reviewable changes.

---

# 30. Implementation Sequence

## Phase A — Audit

Inspect the Task 08 benchmark result models, persistence format, and CLI.

Identify reusable structures.

## Phase B — Statistics

Implement deterministic statistical functions and validation.

Test them using known values.

## Phase C — Grouping

Implement grouping by workload and effective configuration.

Separate successful measured trials from warmups, failures, and interruptions.

## Phase D — Worker Scaling

Implement baseline selection, speedup, throughput scaling, and parallel efficiency.

Handle missing or invalid baselines.

## Phase E — Run Summaries

Generate versioned `summary.json` and `summary.md`.

Keep original trial measurements unchanged.

## Phase F — Comparability

Implement workload, configuration, environment, and correctness compatibility validation.

Classify comparisons and report reasons.

## Phase G — Comparison Reports

Implement baseline/candidate metric comparisons and generate:

```text
comparison.json
comparison.md
```

## Phase H — CLI Integration

Add summary and comparison commands.

Ensure neither command executes normalization.

## Phase I — Verification

Run formatting, compilation, unit tests, and synthetic report-generation tests.

Do not execute benchmarks.

Stop after completing Task 09.

---

# 31. Required Implementation Report

When Task 09 is complete, provide the following.

## Statistical Infrastructure

Describe:

- Statistical functions.
- Grouping rules.
- Percentile definition.
- Standard deviation convention.
- Missing-value handling.

## Worker Scaling

Describe:

- Baseline selection.
- Speedup calculation.
- Parallel efficiency.
- Throughput scaling.
- Missing baseline behavior.

## Comparison System

Describe:

- Comparison inputs.
- Compatibility dimensions.
- Classification logic.
- Supported experimental variables.
- Correctness validation.
- Unavailable metric handling.

## Reports

Describe:

- `summary.json`.
- `summary.md`.
- `comparison.json`.
- `comparison.md`.
- Schema versions.
- CLI commands.

## Verification

Report:

- Formatting checks.
- Compilation checks.
- Statistical unit tests.
- Synthetic summary tests.
- Synthetic comparison tests.
- CLI tests.
- Tests passed.
- Tests failed.
- Tests blocked by prerequisites.

## Not Executed

Explicitly state:

**Real-data normalization benchmarks were not executed, as instructed.**

## Remaining Limitations

Identify:

- Metrics not supported by the current environment.
- Comparison limitations.
- Correctness-evidence limitations.
- Statistical limitations.
- Features deferred to later tasks.

Do not invent performance conclusions.

---

# 32. Acceptance Criteria

Task 09 is complete when:

- Existing persisted benchmark results can be loaded without executing normalization.
- Measured trials are distinguishable from warmups.
- Failed and interrupted trials remain visible.
- Statistical grouping preserves relevant configuration dimensions.
- Mean, median, minimum, and maximum are calculated correctly.
- Sample standard deviation is calculated correctly.
- P95 is calculated using a documented convention.
- Coefficient of variation is calculated correctly where meaningful.
- Missing measurements are handled explicitly.
- Invalid numeric measurements are rejected.
- Session throughput is aggregated correctly.
- Individual job throughputs are not averaged incorrectly.
- Worker-scaling summaries are available.
- Duration-based speedup is calculated correctly.
- Parallel efficiency is calculated correctly.
- Missing baselines do not produce fabricated speedups.
- Parquet file-count and file-size statistics are available.
- Per-run `summary.json` and `summary.md` are generated.
- Baseline/candidate comparisons can be created from existing runs.
- Comparisons validate workload identity.
- Comparisons validate measurement compatibility.
- Intentional experimental variables are distinguished from uncontrolled differences.
- Correctness evidence is evaluated before optimization conclusions.
- Physically different but semantically equivalent Parquet layouts can be compared.
- Incomparable runs are identified clearly.
- Percentage changes and performance ratios are calculated correctly.
- CPU, memory, and I/O trade-offs are reported where supported.
- Statistical variability and sample counts are visible.
- No unsupported claims of statistical significance are generated.
- Comparison reports are stored under `data/benchmarks/historical/normalize/comparisons/`.
- Previous benchmark history is never overwritten.
- Summary regeneration does not modify original trial data.
- Deterministic report generation is supported.
- Incomplete runs do not appear fully successful.
- Lightweight statistical and report tests pass.
- No actual performance benchmarks are executed.
- No fabricated benchmark observations are generated.
- Production normalization behavior remains unchanged.

---

## 33. Stop Condition

**Stop after Task 09 is complete.**

Do not automatically proceed to Task 10.

Do not execute normalization benchmarks.

Do not modify the production Parquet writers.

The next assignment is:

**Task 10 — Stream-Oriented Trade Output and Shared Execution Foundation.**

Task 10 will begin the normalization I/O optimization by separating source-task boundaries from Parquet file boundaries.

The benchmark infrastructure from Tasks 08 and 09 will provide the means to evaluate that optimization later.

Actual performance measurements must remain separately authorized.

---

**Final objective:** Complete a reproducible, statistically meaningful benchmarking analysis system that compares MarketForge normalization implementations, identifies performance and resource trade-offs, validates experiment comparability, and preserves trustworthy historical evidence without requiring repeated benchmark execution.