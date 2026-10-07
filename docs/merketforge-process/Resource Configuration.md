MarketForge exposes a small set of machine-level resource controls and derives lower-level processing limits automatically.

## Quick Navigation

- [Primary Configuration](#primary-configuration)
- [Workers](#workers)
- [Memory Budget](#memory-budget)
- [BookStore Memory](#bookstore-memory)
- [Scratch Storage](#scratch-storage)
- [Merge Memory](#merge-memory)
- [Parquet Targets](#parquet-targets)
- [Automatic Planning](#automatic-planning)
- [Core Rules](#core-rules)

## Primary Configuration

Normal users should only need:

```text
workers
memory_budget
scratch_budget
scratch_path
```

Example:

```text
workers        = auto
memory_budget  = 8 GiB
scratch_budget = 100 GiB
scratch_path   = data/.work
```

Internal settings such as worker batch sizes and merge-reader buffers are derived automatically.

## Workers

Worker count is constrained by CPU and memory:

\[
W_{\text{effective}}
=
\min(
W_{\text{requested}},
W_{\text{CPU}},
W_{\text{memory}}
)
\]

`workers = auto` should be the default.

Each worker processes one `WorkTask` sequentially while multiple workers execute concurrently.

## Memory Budget

`memory_budget` defines the approximate total processing-memory budget.

```text
memory_budget
    ↓
ResourcePlanner
    ↓
worker buffers
merge readers
BookStore
Arrow output
Parquet writer
safety headroom
```

Individual processing components do not independently choose arbitrary memory limits.

Worker batch targets are derived from the memory available to active workers and leave headroom for:

```text
decompression
parsing
normalization
sorting
Arrow allocations
temporary allocations
```

Batch boundaries are memory-driven rather than fixed row counts.

## BookStore Memory

Book state is persistent while depth streams are active and therefore requires separate monitoring.

```text
BookStore
    ↓
active books
    ↓
estimated memory usage
```

The monitor should expose:

```text
active books
estimated book memory
```

If active books cannot fit safely within the processing budget, the initial implementation should fail clearly rather than introduce disk-backed book state.

## Scratch Storage

Temporary Arrow runs use a configurable scratch location:

```text
scratch_path
```

Example:

```text
/mnt/nvme/marketforge
```

This allows raw data, temporary processing, and final datasets to use different storage devices.

`scratch_budget` is a hard safety limit:

```text
temporary runs
    ↓
track scratch usage
    ↓
approaching hard limit
    ↓
stop safely / fail job
```

MarketForge must not fill the filesystem uncontrollably.

Scratch usage is visible in the processing monitor.

## Merge Memory

The merger receives a global reader-memory budget rather than a fixed buffer per stream.

Conceptually:

\[
B_{\text{stream}}
\approx
\frac{M_{\text{merge}}}
{N_{\text{active streams}}}
\]

subject to practical minimum and maximum buffer sizes.

Therefore:

```text
few streams
    → larger reader batches

many streams
    → smaller reader batches
```

This allows large merges without requiring memory proportional to a fixed large buffer for every stream.

## Parquet Targets

Storage tuning uses approximate targets such as:

```text
row_group_target
file_target
```

Initial values may be approximately:

```text
Parquet row group   ~128 MiB
Parquet file        ~512 MiB
```

These are performance targets, not correctness requirements.

Advanced configuration may expose:

```json
{
  "parquet": {
    "row_group_target_mib": 128,
    "file_target_mib": 512
  }
}
```

Normal users should not need to modify them.

## Automatic Planning

Python resolves user configuration and machine resources before launching Rust:

```text
CLI arguments
    +
MarketForge defaults
    +
available CPU
    +
available memory
    +
available scratch storage
    +
number of streams
    ↓
ResourcePlanner
    ↓
ResolvedResourceConfig
    ↓
ProcessingJob
```

Example user overrides:

```bash
marketforge process ... \
    --workers 12 \
    --memory 16G \
    --scratch /mnt/nvme/marketforge \
    --scratch-budget 500G
```

Python converts friendly values such as `16G` into resolved numeric values before passing the job to Rust.

The effective plan should be visible before and during processing:

```text
Resource Plan
────────────────────────
CPU threads         16
Workers              8

Memory budget       8.0 GiB
Worker allocation   4.0 GiB
Merge readers       1.0 GiB
Output buffers      1.0 GiB
Reserved/headroom   2.0 GiB

Scratch path        /data/.work
Scratch available   482 GiB

Streams             37
Depth books         18
```

## Core Rules

- expose machine-level budgets rather than dozens of low-level tuning parameters
- `workers = auto` is the normal default
- worker count respects both CPU and memory constraints
- one central resource planner derives component budgets
- batch sizing is memory-driven
- BookStore memory is monitored separately
- merge buffers adapt to the number of active streams
- scratch storage has an explicit hard safety budget
- scratch location is configurable for fast dedicated storage
- Parquet row-group and file sizes are approximate performance targets
- resource decisions are included in `ProcessingJob`
- resolved configuration is exposed through monitoring and preserved for reproducibility
- resource exhaustion should cause controlled degradation or failure, never uncontrolled OOM or filesystem exhaustion

> Expose a few machine-level budgets; derive low-level processing limits automatically.