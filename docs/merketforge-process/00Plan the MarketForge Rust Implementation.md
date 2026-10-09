# MarketForge — Rust Processing Roadmap

Historical market microstructure processing engine.

**Architecture:** Python handles acquisition, metadata, planning, and orchestration. Rust handles decoding, normalization, validation, Parquet serialization, execution, and dataset publication.

## Quick Navigation

- [Current Status](#current-status)
- [Implementation Priorities](#implementation-priorities)
- [Phase 1 — Metrics and Integrity](#phase-1--metrics-and-integrity)
- [Phase 2 — Historical Depth Processing](#phase-2--historical-depth-processing)
- [Phase 3 — BookStore](#phase-3--bookstore)
- [Phase 4 — Resource Management](#phase-4--resource-management)
- [Phase 5 — Reliability and Hardening](#phase-5--reliability-and-hardening)
- [Phase 6 — CLI and Runtime Integration](#phase-6--cli-and-runtime-integration)
- [Deferred Work](#deferred-work)
- [Testing Strategy](#testing-strategy)

## Current Status

### Completed

**Historical trade processing is functional end to end.**

- Python-generated processing job loading and structural validation.
- Source readers: plain, GZIP, ZIP, TAR.GZ.
- CSV decoding, including headerless formats.
- Nine trade formats across Binance, Bybit, OKX, Bitget, and Gate.io.
- Spot, linear/inverse perpetual, and dated futures normalization.
- Exact timestamp, price, and quantity normalization.
- Instrument identity and family filtering.
- Canonical trade validation.
- Arrow RecordBatch construction.
- Exact `Decimal256(76, 28)` serialization.
- ZSTD-compressed Parquet writer.
- Multi-task trade processing.
- Dataset manifests and Parquet metadata verification.
- Staging and transactional dataset publication.
- Global processing configuration.

**Verified:**

| Test                           | Result                 |
| ------------------------------ | ---------------------- |
| Trade configurations           | 18/18 passed           |
| Complete-archive normalization | 37,330,201 trades      |
| Multi-task executor            | 3 OKX archives         |
| Instrument filtering           | 22,511 records skipped |
| Committed Parquet dataset      | 162 trades             |
| Manifest verification          | Passed                 |
| Decimal256 round trips         | Passed                 |

### Current Limitations

- Processing is sequential.
- Global resource budgets are configured but not fully enforced.
- Integrity policies are not yet enforced by workers.
- Processing metrics are limited.
- Depth processing is incomplete.
- Output ordering is not globally guaranteed.
- Merge execution is not implemented.
- Python CLI integration remains incomplete.

## Implementation Priorities

| Order | Component | Status |
|---|---|---|
| 1 | Processing metrics | **Next** |
| 2 | Integrity accounting and policy enforcement | Pending |
| 3 | Historical depth normalization | Pending |
| 4 | Depth Arrow and Parquet | Pending |
| 5 | Depth worker and executor integration | Pending |
| 6 | BookStore and continuity validation | Pending |
| 7 | Parallel execution and resource enforcement | Pending |
| 8 | Failure recovery and hardening | Pending |
| 9 | Python CLI and live monitoring | Pending |
| Later | Temporary runs and chronological ordering | Deferred |
| Later | Dataset merging | Deferred |
| Later | Combined trades + depth | Deferred |
| Later | Live WebSocket processing | Deferred |

## Phase 1 — Metrics and Integrity

**Priority: Immediate**

### Processing Metrics

Implement shared metrics infrastructure:

```text
engine/src/process/metrics/
├── mod.rs
├── counters.rs
├── integrity.rs
└── report.rs
```

Track:

- Records read, matched, filtered, rejected, and written.
- Processing throughput and elapsed time.
- Completed and active tasks.
- Input/output bytes.
- Active workers and streams.
- Memory and CPU usage.
- Scratch storage usage.
- Parquet files and row groups.

Metrics must be reusable by trade and depth workers.

### Integrity Accounting

Use the existing `job/integrity.rs` policy definitions.

Track:

```text
parse_failure
invalid_record
sequence_gap
timestamp_regression
missing_snapshot
invalid_book
transformation_failure
```

For each category, maintain:

- Total occurrence count.
- Bounded diagnostic examples.
- Source task and record location.
- Error description.
- Applied integrity decision.

Never accumulate unlimited error messages in memory.

Instrument filtering is normal processing behavior, not an integrity failure.

### Integrity Policy Enforcement

Separate responsibilities:

```text
Validator → Detect
Metrics   → Record
Policy    → Decide
Worker    → Enforce
Manifest  → Persist
CLI       → Display
```

Support:

- `Fail`: terminate processing.
- `Degrade`: tolerate violations within configured limits.
- Threshold exceeded: terminate processing.
- Fatal source corruption: always fail.

A degraded dataset must never be reported as clean.

Persist integrity summaries and bounded diagnostics in the dataset manifest.

**Done when:** Both successful and failed processing jobs produce accurate, structured diagnostics, and integrity policies are enforced consistently.

## Phase 2 — Historical Depth Processing

**Priority: After metrics and integrity**

### Depth Decoding

Implement exchange-specific physical decoding as required:

- CSV and JSONL.
- XLSX where required.
- Nested bid/ask structures.
- Snapshot and incremental update records.
- Native sequence metadata.

Use existing source readers and canonical types.

### Snapshot Normalization

Start with a simple independent-snapshot format.

Pipeline:

```text
Raw Depth
    ↓
Decode
    ↓
Normalize
    ↓
L2Snapshot
    ↓
Validate
```

Preserve:

- Event timestamp and instrument identity.
- Bid and ask levels.
- Exact prices and quantities.
- Order counts when available.
- Native sequence metadata.

### Incremental Updates

Implement canonical `L2Update` normalization.

```text
L2Update
├── sequence metadata
└── changes[]
    ├── side
    ├── action
    ├── price
    ├── quantity
    └── order_count
```

One native update must remain **one atomic canonical event**, even when it modifies multiple price levels.

Implement format-specific transformations for Bybit, OKX, and other supported depth sources.

### Depth Parquet

Extend the existing Arrow/Parquet infrastructure.

Requirements:

- Nested depth structures.
- Decimal256 values.
- Nullable sequence metadata.
- Atomic update preservation.
- Exact canonical round trips.
- Bounded serialization.

### Depth Worker

Connect depth normalization to the existing executor.

Reuse:

```text
SourceReader
    ↓
DepthProcessor
    ↓
CanonicalValidator
    ↓
IntegrityMetrics
    ↓
DepthSink
    ↓
Parquet
    ↓
Manifest
    ↓
Commit
```

**Done when:** Representative real depth archives produce verified, committed canonical Parquet datasets.

## Phase 3 — BookStore

Implement stateful order-book reconstruction.

```text
engine/src/book/
```

### Book State

Maintain independent books identified by:

```text
instrument_id + stream_id
```

Support:

- Snapshot replacement.
- Atomic incremental updates.
- Bid/ask insertion and deletion.
- Ordered price levels.
- Best bid, best ask, and midpoint.
- Book validity state.

Never use floating-point prices as ordered keys.

### Continuity Validation

Implement source-specific sequence rules.

Possible models:

```text
PreviousSequence
ContiguousRange
Monotonic
None
```

Only apply rules supported by actual exchange semantics.

Book lifecycle:

```text
Uninitialized
    ↓ Snapshot
Valid
    ↓ Continuity failure
Invalid
    ↓ New authoritative snapshot
Valid
```

Continuity state must survive source-file and batch boundaries.

Report violations through the shared integrity system.

**Done when:** Canonical depth streams can reconstruct valid books and detect continuity failures.

## Phase 4 — Resource Management

Use the authoritative global configuration:

```text
data/.jobs/processing.json
```

Existing job manifests retain their `resources` fields for compatibility, but Rust execution ignores them.

### Parallel Execution

Implement bounded concurrent `WorkTask` execution.

- Fixed worker pool.
- Configurable concurrency.
- Independent task processing.
- Deterministic results.
- No uncontrolled thread creation.

Worker count must not change canonical semantics.

### Resource Enforcement

Enforce:

```text
workers
memory_budget_bytes
scratch_budget_bytes
parquet.row_group_target_bytes
parquet.file_target_bytes
```

Track actual resource consumption.

Handle resource exhaustion through structured failures rather than uncontrolled memory or disk usage.

**Important:** Global configuration alone does not coordinate resources across multiple Rust processes. Cross-job allocation belongs to the future Python scheduler.

**Done when:** Processing respects configured resource limits and produces equivalent results across worker counts.

## Phase 5 — Reliability and Hardening

### Source Integrity

Validate:

- Missing or unreadable archives.
- Corrupt ZIP/GZIP/TAR.GZ containers.
- Missing archive members.
- Invalid source structures.
- Truncated input.
- Transformation failures.

Fatal source failures must never publish incomplete datasets.

### Dataset Publication

Strengthen the existing commit implementation:

- Validate Parquet metadata and schema.
- Verify manifest consistency.
- Reject conflicting dataset destinations.
- Handle interrupted processing.
- Detect abandoned staging directories.
- Prevent concurrent publication conflicts.
- Handle cross-filesystem staging and output.
- Preserve failed-run diagnostics.

### Processing Verification

Run representative real jobs across supported exchanges and markets.

Verify:

- Canonical values.
- Record accounting.
- Integrity decisions.
- Depth atomicity.
- Book reconstruction.
- Manifest consistency.
- Parquet readability.
- Failure behavior.

**Done when:** Processing failures are controlled, observable, and cannot silently produce invalid completed datasets.

## Phase 6 — CLI and Runtime Integration

### Python ↔ Rust Execution

Connect the existing Python processing CLI to `marketforge-process`.

```text
Python CLI
    ↓
Processing Job
    ↓
Rust Executor
    ↓
ProcessingResult
    ↓
Python Catalog Update
```

Rust stdout remains reserved for machine-readable results.

Progress and diagnostics use stderr or a structured event channel.

### Live CLI Monitoring

**The CLI must display processing progress, resource consumption, and integrity errors together in real time.**

Example:

```text
MARKETFORGE — PROCESSING

Dataset: 120
Status: RUNNING

PROGRESS
  Tasks          2 / 6
  Records        8,420,000
  Throughput     185,000 records/s
  Parquet files  4

RESOURCES
  CPU            78%
  Memory         2.1 / 4.0 GiB
  Workers        4 / 4
  Scratch        1.3 / 20.0 GiB

INTEGRITY
  Parse failures         0
  Invalid records        3
  Sequence gaps          1
  Timestamp regressions  0
  Missing snapshots      0
  Invalid books          0
  Transformation failures 0

RECENT ERRORS
  Task 1203 | Record 45281 | Invalid price
  Task 1204 | Sequence gap detected

Status: DEGRADED
```

Display requirements:

- Continuously update progress and resource metrics.
- Show integrity counters and recent errors live.
- Distinguish warnings from fatal failures.
- Keep diagnostic history bounded.
- Support non-interactive execution.
- Never block processing workers on terminal rendering.

Monitoring modes:

```text
none
simple
json
tui
```

The TUI is optional. All modes consume the same underlying metrics.

### Python TODO

- CLI commands to inspect and modify global processing resources.
- Python orchestration using the global configuration.
- Resource-aware scheduling across concurrent Rust processes.
- Dataset lifecycle updates from `ProcessingResult`.
- Live progress, resource, and integrity display.

**Done when:** MarketForge can launch processing jobs through Python and display live execution health without interfering with processing.

## Deferred Work

### Temporary Runs and Chronological Ordering

Implement later:

- Arrow IPC temporary runs.
- External timestamp sorting.
- Deterministic equal-timestamp ordering.
- K-way chronological merging.
- Stream-rank ordering.

Preserve authoritative source ordering where required.

### Dataset Merging

Deferred until historical processing is complete.

Responsibilities:

- Canonical Parquet readers.
- Time-range clipping.
- Trade-only merging.
- Depth-only merging.
- Combined trades and depth.
- Cross-exchange merging.
- Deterministic chronological output.

Merge operations must preserve native canonical values.

### Combined Processing

Combine canonical trades and depth into a replayable chronological event stream.

BookStore updates only from depth events.

### Live Processing

Future implementation:

- WebSocket acquisition.
- Real-time normalization.
- Order-book reconstruction.
- Disconnection recovery.
- Downstream ML integration.

### Advanced Optimizations

Defer until justified by profiling:

- Adaptive buffering.
- Specialized decoding.
- Run caching.
- Checkpointing.
- Hierarchical merging.
- Distributed processing.

## Testing Strategy

Implement tests alongside each component.

**Fast tests:**

```bash
cargo test --manifest-path engine/Cargo.toml \
  --features process --lib
```

**Compilation:**

```bash
cargo check --manifest-path engine/Cargo.toml \
  --features process
```

**Focused integration tests:**

```bash
cargo test --manifest-path engine/Cargo.toml \
  --features process \
  --test process_executor
```

Complete-archive tests should run only when required.

Use small deterministic fixtures for ordinary development and real archives for milestone verification.

Required invariants:

- Exact canonical numeric values.
- Lossless Arrow/Parquet round trips.
- Atomic L2 updates.
- Accurate integrity accounting.
- Consistent results across worker counts.
- No publication after fatal failure.
- Manifest and physical dataset consistency.
- Monitoring never changes processing results.

## Immediate Next Step

**Implement Phase 1 — Processing Metrics and Integrity.**

Start with:

```text
engine/src/process/metrics/
├── mod.rs
├── counters.rs
├── integrity.rs
└── report.rs
```

First establish shared counters, integrity categories, bounded diagnostic examples, and serializable reports.

Then integrate policy enforcement into the existing trade worker before extending the same infrastructure to depth processing.