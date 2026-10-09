**Objective:** Detect, measure, and report data-quality problems across historical market data without silently producing unreliable datasets.

Integrity evaluation must consider **error counts, relative frequency, temporal concentration, and severity**.

A fixed number of errors is insufficient because trading activity varies significantly between exchanges, instruments, and time periods.

### Quick Navigation

- [Core Principles](#core-principles)
- [Integrity Categories](#integrity-categories)
- [Evaluation Levels](#evaluation-levels)
- [Specialized Depth Metrics](#specialized-depth-metrics)
- [Integrity Policy](#integrity-policy)
- [Metrics and Reporting](#metrics-and-reporting)
- [Implementation Architecture](#implementation-architecture)
- [Implementation Order](#implementation-order)

## Core Principles

Integrity measurement and policy enforcement are separate responsibilities.

```text
Source Data
    ↓
Decode / Normalize / Validate
    ↓
Detect Integrity Violation
    ↓
Record Metrics and Diagnostics
    ↓
Evaluate Integrity Policy
    │
    ├── Accept → Continue
    ├── Degrade → Record violation and continue
    └── Fail → Terminate processing
```

Requirements:

- Preserve accurate error counts and rates.
- Detect localized corruption hidden by dataset-level averages.
- Evaluate integrity per instrument and stream where applicable.
- Use category-specific denominators.
- Preserve bounded diagnostic examples.
- Never silently discard invalid records.
- Distinguish ordinary instrument filtering from integrity failures.
- Never publish a failed dataset as complete.
- Preserve integrity results for downstream applications.

## Integrity Categories

Reuse the existing `IntegrityPolicy` categories.

| Category | Description |
|---|---|
| `parse_failure` | Malformed or undecodable source record |
| `invalid_record` | Record violates canonical validation |
| `sequence_gap` | Missing or inconsistent sequence continuity |
| `timestamp_regression` | Unexpected backward timestamp movement |
| `missing_snapshot` | Incremental depth without required book initialization |
| `invalid_book` | Invalid or inconsistent order-book state |
| `transformation_failure` | Failed normalization or quantity conversion |

Fatal source failures, including missing archives, corrupt containers, and decompression failures, must terminate processing independently of degradable record-level policies.

## Evaluation Levels

Integrity is evaluated at four complementary levels.

| Scope | Measurements | Purpose |
|---|---|---|
| Global | Count + rate | Overall dataset quality |
| Source file | Count + rate | Identify problematic archives |
| UTC day | Count + rate | Detect bad trading days |
| UTC hour | Count + rate | Detect concentrated corruption |

Metrics should retain instrument and stream identity where relevant.

### Error Rates

For each category:

\[
\text{Error Rate} =
\frac{\text{Violation Count}}{\text{Eligible Observations}}
\]

The denominator depends on the integrity category.

Examples:

- Invalid records → eligible source records.
- Parse failures → attempted source records.
- Timestamp regressions → eligible timestamp transitions.
- Sequence gaps → eligible sequence transitions.

Do not use total dataset records as a universal denominator.

### Temporal Windows

Use fixed UTC boundaries:

```text
Daily  → 00:00:00–24:00:00 UTC
Hourly → HH:00:00–HH+1:00:00 UTC
```

Windows use half-open intervals:

```text
start_timestamp_ns <= timestamp < end_timestamp_ns
```

Malformed records without recoverable event timestamps must be tracked separately by source file and record position.

A low global error rate must not conceal severe corruption within individual windows.

### Minimum Sample Size

Rate-based enforcement must define minimum eligible observations.

Small samples should not trigger misleading percentage-based failures.

Absolute limits and fatal conditions remain enforceable regardless of sample size.

## Specialized Depth Metrics

Some depth integrity problems require additional measurements beyond incident counts.

### Sequence Gaps

Track:

```text
sequence_gap_incidents
estimated_missing_sequence_units
```

One missing sequence number and a gap spanning thousands of sequence numbers must not be treated as equivalent.

Estimate missing units only when native sequence semantics support that calculation.

### Invalid Book State

Track:

```text
invalid_book_incidents
invalid_book_duration_ns
missing_snapshot_incidents
```

Book validity follows:

```text
Uninitialized
    ↓ Snapshot
Valid
    ↓ Integrity failure
Invalid
    ↓ Authoritative snapshot
Valid
```

Measure invalid-state duration when reliable event timestamps and recovery boundaries are available.

Continuity rules remain exchange- and format-specific.

## Integrity Policy

The authoritative integrity policy will be stored alongside global resources in:

```text
data/.jobs/processing.json
```

Existing `ProcessingJob.integrity_policy` fields remain for protocol compatibility but will not control execution.

### Policy Rules

Each integrity category may define:

```text
action
max_count
max_rate
minimum_samples
daily limits
hourly limits
```

Example conceptual configuration:

```json
{
  "integrity_policy": {
    "profile": "standard",

    "invalid_record": {
      "action": "degrade",
      "max_count": 10000,
      "max_rate": 0.001,
      "minimum_samples": 1000,
      "windows": {
        "daily": {
          "max_rate": 0.005
        },
        "hourly": {
          "max_rate": 0.02
        }
      }
    },

    "sequence_gap": {
      "action": "fail",
      "max_count": 0
    }
  }
}
```

**Note:** This is a proposed schema. Rust models and validation must be extended before using it.

### Enforcement Semantics

- `Fail`: terminate when the configured violation condition is met.
- `Degrade`: tolerate violations within configured limits and mark the resulting dataset degraded.
- Threshold exceeded: terminate processing.
- Fatal source failure: always terminate.

For `Degrade`, `max_count` represents the maximum tolerated count; exceeding it fails processing.

Define whether rate thresholds are evaluated continuously or at completed-window boundaries before implementation.

The `standard` and `strict` profiles must have explicit semantics rather than functioning as decorative labels.

## Metrics and Reporting

Metrics are collected once and shared across the processing system.

### Persistent Metrics

Store in each completed dataset's `manifest.json`:

- Global integrity counters and rates.
- Per-category statistics.
- Per-file integrity summaries.
- Daily and hourly violations.
- Worst affected windows.
- Specialized depth metrics.
- Bounded diagnostic examples.
- Effective integrity policy.
- Final integrity status.

Avoid storing every clean time window or every individual error in the manifest.

Detailed statistics may be stored separately if required.

### Failed Processing

```text
Successful / Degraded Job
    → Dataset manifest
    → Integrity summary

Failed Job
    → Failed-run report
    → Integrity counters
    → Diagnostic examples
    → No committed dataset
```

Failed-run diagnostics must survive unsuccessful execution.

### Live Monitoring

The Rust TUI will display:

```text
INTEGRITY — BTCUSDT

Overall error rate      0.015%
Worst hourly rate       3.000%
Affected UTC windows    1
Invalid records         150
Sequence gaps           0

Worst window:
2026-09-01 02:00–03:00 UTC
150 invalid / 5,000 records

Status: DEGRADED
```

The TUI must display integrity errors alongside processing progress and resource consumption.

Rendering must never block processing workers.

## Implementation Architecture

```text
engine/src/
├── job/
│   └── integrity.rs          # Policy definitions
│
├── process/
│   ├── config.rs             # Global policy loading
│   ├── metrics/
│   │   ├── counters.rs       # Processing statistics
│   │   ├── integrity.rs      # Violations and windows
│   │   ├── report.rs         # Serializable reports
│   │   └── mod.rs
│   ├── worker/               # Detection and enforcement
│   ├── manifest/             # Persistent results
│   └── executor.rs           # Aggregation
│
└── validate/                 # Canonical validation
```

Responsibility boundary:

```text
Validator → Detection
Metrics   → Measurement
Policy    → Decisions
Worker    → Enforcement
Executor  → Aggregation
Manifest  → Persistence
Rust TUI  → Presentation
```

## Implementation Order

1. Extend global processing configuration with integrity policy.
2. Define category-specific counters, denominators, and time windows.
3. Implement bounded diagnostic storage.
4. Implement count, rate, and window-based policy evaluation.
5. Integrate integrity accounting into the trade worker.
6. Aggregate per-task and per-job metrics.
7. Persist integrity summaries in manifests and failed-run reports.
8. Reuse the infrastructure for depth processing and BookStore validation.
9. Expose live metrics through the Rust TUI.

### Required Tests

Verify:

- Absolute count thresholds.
- Relative error rates.
- Minimum sample sizes.
- Hourly and daily boundary handling.
- Localized corruption despite low global error rates.
- Multiple instruments and streams.
- Records without recoverable timestamps.
- Sequence-gap severity.
- Invalid-book duration and recovery.
- Bounded diagnostic memory.
- Correct degraded/failed decisions.
- No dataset publication after fatal failure.

**Final decision:** MarketForge integrity must be evaluated using absolute counts, relative rates, temporal concentration, and category-specific severity—not a single global error threshold.