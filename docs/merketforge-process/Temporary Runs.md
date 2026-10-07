Temporary runs provide a bounded-memory bridge between parallel worker processing and the final chronological merge.

## Quick Navigation

- [Purpose](#purpose)
- [Run Format](#run-format)
- [Run Metadata](#run-metadata)
- [Memory and Flushing](#memory-and-flushing)
- [Ordering](#ordering)
- [Scratch Storage](#scratch-storage)
- [Future Optimization](#future-optimization)

## Purpose

A worker converts bounded canonical batches into temporary ordered runs:

```text
Raw Source
    ↓
Worker
    ↓
Canonical Events
    ↓
Bounded RAM Batch
    ↓
Sort if Required
    ↓
Temporary Run
```

Large inputs therefore become:

```text
run-000000.arrow
run-000001.arrow
run-000002.arrow
...
```

Temporary runs are internal processing artifacts.

```text
Temporary Run ≠ Parquet Part
```

Many runs may eventually produce only a few final Parquet files.

## Run Format

Initial temporary storage uses **Arrow IPC**.

```text
Canonical Events
    ↓
Arrow RecordBatch
    ↓
Arrow IPC Run
```

Runs preserve the canonical event model:

```text
Trade
L2Snapshot
L2Update
```

including nested atomic:

```text
L2Update
└── changes[]
```

The merger later reads runs in bounded Arrow batches.

## Run Metadata

Each run has a small `RunDescriptor`:

```text
path
task_id
stream_id
run_index

first_timestamp_ns
last_timestamp_ns

event_count
size_bytes
ordering
```

Workers return descriptors rather than returning the actual event data:

```text
TaskResult
└── runs[]
    ├── RunDescriptor
    ├── RunDescriptor
    └── ...
```

## Memory and Flushing

Run boundaries are primarily memory-driven rather than row-count-driven.

```text
worker memory budget
        ↓
bounded canonical buffer
        ↓
flush threshold reached
        ↓
write run
        ↓
reuse memory
```

The flush target must leave headroom for:

```text
decompression
parsing
sorting
Arrow builders
temporary allocations
```

A fixed number of rows should not determine run size because event sizes can differ significantly.

## Ordering

Every run must contain events in a valid ordering defined by the source-format policy.

For naturally ordered sources:

```text
run-000
T0 → T1

run-001
T1 → T2

run-002
T2 → T3
```

For sortable unordered data, independently sorted runs may overlap:

```text
run-000
10:00 → 11:00

run-001
09:30 → 12:00
```

These become independent ordered inputs to the later k-way merge.

Stateful depth must preserve authoritative source/sequence ordering and must never be arbitrarily timestamp-sorted.

```text
format semantics
    ↓
ordering policy
    ↓
temporary run
```

Runs preserve valid ordering; they do not determine market-ordering semantics.

## Scratch Storage

Temporary data is isolated by job and task:

```text
data/.work/
└── {job_id}/
    ├── task-000000/
    │   ├── run-000000.arrow
    │   ├── run-000001.arrow
    │   └── ...
    │
    └── task-000001/
        ├── run-000000.arrow
        └── ...
```

Lifecycle:

```text
processing
    ↓
temporary runs
    ↓
global merge
    ↓
final Parquet
    ↓
manifest published
    ↓
delete job scratch
```

During development, failed-job scratch data may be retained for debugging.

## Future Optimization

The initial implementation may always stage worker output through temporary runs:

```text
Worker
    ↓
Arrow Runs
    ↓
Merge
    ↓
Parquet
```

This keeps v1 simple and deterministic.

Later, already ordered/simple inputs may bypass temporary storage:

```text
Worker
    ↓
Direct Canonical Stream
    ↓
Merge / Writer
```

The temporary-run layer should therefore remain replaceable.

Possible future optimizations include:

```text
direct streaming
adaptive run sizing
different intermediate encoding
compressed runs
memory-backed runs
hierarchical spilling
```

These optimizations must not affect parsers, normalization, canonical schemas, or ordering semantics.

> Temporary runs are disposable, ordered canonical spill storage used to bound memory between worker processing and global merging.