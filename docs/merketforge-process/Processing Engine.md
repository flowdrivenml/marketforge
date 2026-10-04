`marketforge-process` is a bounded streaming engine. Python decides **what** to process; Rust executes it efficiently.

## Quick Navigation

- [Pipeline](#pipeline)
- [Shared Event Model](#shared-event-model)
- [Sorting and Merging](#sorting-and-merging)
- [Memory](#memory)
- [Output](#output)
- [Python Boundary](#python-boundary)

## Pipeline

```text
ProcessingJob
    ↓
Open / Decompress
    ↓
Parse
    ↓
Normalize
    ↓
Validate
    ↓
Canonical Event Stream
    ↓
Sort if required
    ↓
Merge / Combine
    ↓
Arrow Batches
    ↓
Parquet Parts
    ↓
manifest.json
```

The complete dataset is never required in memory.

## Shared Event Model

All processing converges to:

```rust
CanonicalEvent {
    Trade,
    L2Snapshot,
    L2Update,
}
```

Historical parsers are exchange/format-specific. After normalization, the rest of the engine operates only on canonical events.

Shared with the future live engine:

```text
canonical types
normalization
book reconstruction
canonical validation
```

## Sorting and Merging

Each input becomes an independently chronological stream.

Already ordered sources are streamed directly. Unordered sources require bounded/external sorting.

Multiple streams use a **k-way merge**:

```text
Trades ───────┐
Depth ────────┤
Bybit ────────┤
Binance ──────┼→ min-heap → chronological stream
OKX ──────────┘
```

For \(N\) events across \(k\) streams:

\[
O(N\log k)
\]

The same mechanism handles:

```text
trades
depth
trades + depth
cross-exchange datasets
```

## Memory

Memory and storage boundaries are independent:

```text
bounded input
    ↓
bounded normalization/sort buffers
    ↓
Arrow batch
    ↓
Parquet writer
```

Processing batches do **not** become individual Parquet files.

## Output

```text
data/datasets/{dataset_id}/
├── manifest.json
├── part-00000.parquet
├── part-00001.parquet
└── ...
```

Parquet parts are:

```text
chronological
contiguous in time
size-bounded
composed of multiple row groups
```

`manifest.json` records file ranges, sizes, rows, and ordering.

Optional numeraire conversion or derived transformations happen **after canonical normalization** and never modify native canonical values.

## Python Boundary

Python owns:

```text
catalog
job construction
input selection
instrument metadata
dataset registration
dataset lineage
CLI
```

Rust receives a `ProcessingJob`, writes the dataset, and returns a structured `ProcessingResult`.

```text
Python
    ↓ ProcessingJob
Rust
    ↓ ProcessingResult
Python
    ↓ catalog.datasets / dataset_inputs
```

Rust does not need direct PostgreSQL access.

> Core abstraction: a bounded, chronological `CanonicalEvent` stream.