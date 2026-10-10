## Quick Navigation

- [Objective](#objective)
- [Algorithm](#algorithm)
- [Parallel Execution](#parallel-execution)
- [Adaptive Partitioning](#adaptive-partitioning)
- [Performance Optimizations](#performance-optimizations)
- [Output](#output)
- [Design Decisions](#design-decisions)

## Objective

Merge multiple validated, chronologically ordered canonical datasets into one deterministic event stream.

Stage 1 resolves same-instrument archive continuity. Stage 2 performs optional cross-instrument conversion. Stage 3 combines the resulting streams chronologically.

**Prioritize parallel execution, bounded memory consumption, and minimal redundant I/O.**

## Algorithm

Use **K-way merging with a min-heap**, implemented using Rust's `BinaryHeap<Reverse<MergeKey>>`.

Each input stream is already chronologically ordered, so no additional sorting is required.

```text
BTCUSDT ─────┐
ETHUSDT ─────┤
SOLUSDT ─────┤
...          ├──► K-way Merge ──► Ordered Events
Instrument N ┘
```

Complexity:

\[
O(N\log k)
\]

Where:

- \(N\) = total source events.
- \(k\) = number of input streams.

Memory consumption is bounded by active stream cursors and Arrow buffers.

Use a deterministic ordering key:

```rust
(
    event_timestamp_ns,
    stream_rank,
    source_event_ordinal,
)
```

Atomic depth-event groups must remain indivisible.

## Parallel Execution

Divide the requested time range into independent chronological partitions.

Each worker performs a standard K-way merge over its assigned interval.

Example: 15 instruments, one day of data, eight workers.

```text
                  15 CANONICAL DATASETS
                           │
                           ▼
                    TIME PARTITIONER
                           │
          ┌────────────────┼────────────────┐
          ▼                ▼                ▼
       Worker 1         Worker 2         Worker N
       00–03h           03–06h           ...
          │                │                │
          ▼                ▼                ▼
       K-way Merge      K-way Merge      K-way Merge
          │                │                │
          ▼                ▼                ▼
       Part 000         Part 001         Part N
          │                │                │
          └────────────────┼────────────────┘
                           ▼
                     ORDERED MANIFEST
```

Workers operate independently on read-only input datasets.

Use half-open intervals:

\[
[t_{\text{start}},t_{\text{end}})
\]

Events exactly at a partition boundary belong to the later partition.

No final event-level merge is necessary because output partitions are already chronologically ordered.

## Adaptive Partitioning

Prefer **event-count-balanced time partitions** over fixed-duration intervals.

Market activity varies throughout the day, so equal time intervals may contain substantially different workloads.

Use available metadata to estimate event density:

- Parquet file timestamp ranges.
- Row-group timestamp statistics.
- Row counts.
- Source-event indexes.

Choose partition boundaries that approximately balance the number of events processed by each worker.

```text
Worker 1 → 00:00–01:30
Worker 2 → 01:30–04:00
Worker 3 → 04:00–07:00
...
```

These boundaries are illustrative.

The scheduler may create more partitions than workers to improve load balancing.

## Performance Optimizations

### Metadata-Based Pruning

Use file and row-group timestamp statistics to skip irrelevant input regions.

Workers should not scan complete daily files when processing only a small time interval.

### Streaming Arrow Batches

Read Parquet through bounded Arrow batches.

Maintain one cursor per active input stream.

Avoid loading complete datasets into memory.

### Atomic Depth Events

Preserve source-event boundaries using a compact event index.

One depth message may contain multiple canonical L2 rows.

The merger must make one ordering decision per atomic source event rather than independently interleaving its levels.

### Logical Concatenation

When timestamp ranges do not overlap, prefer referencing existing Parquet files without rewriting them.

Use K-way merging only where chronological interleaving is necessary.

### Parallel Resource Management

Control:

- Number of active workers.
- Concurrent Parquet readers.
- Arrow buffer memory.
- Output writer memory.
- Disk I/O contention.

Benchmark worker counts rather than assuming maximum CPU utilization provides maximum throughput.

## Output

Each worker produces an independently ordered Parquet partition.

```text
Merged Dataset
├── part-000000.parquet
├── part-000001.parquet
├── part-000002.parquet
├── ...
└── manifest.json
```

The manifest records:

- Partition ordering.
- Timestamp ranges.
- Row and event counts.
- Source stream identities.
- Reconstruction dependencies.
- Integrity and provenance metadata.

The output must support deterministic chronological iteration.

For depth streams, preserve reconstruction segments and their required initialization states. A time partition does not automatically establish an independently reconstructable order book.

## Design Decisions

- Use K-way merging with Rust's `BinaryHeap`.
- Reuse the same merge algorithm for sequential and parallel execution.
- Parallelize across independent time partitions.
- Prefer adaptive event-count-balanced partitioning.
- Use half-open timestamp intervals.
- Preserve deterministic ordering for equal timestamps.
- Never split atomic depth events.
- Use Parquet metadata for file and row-group pruning.
- Maintain bounded memory usage.
- Avoid sorting already-ordered canonical streams.
- Prefer logical concatenation for non-overlapping ranges.
- Produce chronologically ordered Parquet partitions.
- Combine partitions through an ordered dataset manifest.
- Validate parallel output against a sequential reference implementation.

## Implementation Strategy

**Phase 1 — Sequential Reference**

Implement and validate a single-worker K-way merge.

**Phase 2 — Parallel Execution**

Reuse the same merge function across independent time partitions.

**Phase 3 — Adaptive Partitioning**

Balance partitions using estimated event counts and timestamp distributions.

**Phase 4 — Performance Optimization**

Benchmark throughput, memory usage, Parquet I/O, worker counts, and optional alternative merge structures.

**Stage 3 uses adaptive time partitioning and parallel K-way merging to combine already-ordered canonical streams efficiently, without unnecessary sorting or additional final merging.**