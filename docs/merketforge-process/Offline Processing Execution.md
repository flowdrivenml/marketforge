`marketforge-process` uses a bounded streaming architecture designed to handle datasets much larger than available RAM.

The processor separates parallel preparation, temporary storage, chronological merging, optional stateful processing, and final Parquet output.

## Quick Navigation

- [Execution Overview](#execution-overview)
- [Parallel Work](#parallel-work)
- [Bounded Processing and Temporary Runs](#bounded-processing-and-temporary-runs)
- [Chronological Merge](#chronological-merge)
- [Stateful Book Processing](#stateful-book-processing)
- [Parquet Output](#parquet-output)
- [Dataset Completion](#dataset-completion)
- [Resource Model](#resource-model)
- [Core Rules](#core-rules)

## Execution Overview

The complete processing pipeline is:

```text
Raw Inputs
    ↓
Parallel Workers
    ↓
Decompress / Parse / Normalize / Validate
    ↓
Bounded Temporary Runs
    ↓
Chronological K-Way Merge
    ↓
Optional Stateful Processing
    ↓
Arrow Batches
    ↓
Parquet Row Groups
    ↓
Size-Bounded Parquet Parts
    ↓
manifest.json
```

The complete dataset is never required in memory.

## Parallel Work

Independent input preparation is parallelized through one fixed worker pool.

```text
Task 0 ─┐
Task 1 ─┤
Task 2 ─┼→ Worker Pool
Task 3 ─┤
Task 4 ─┘
```

A task may represent:

```text
one source archive
one instrument/file
one exchange/file
one independent processing unit
```

Each worker executes the complete preparation pipeline:

```text
Open
  ↓
Decompress
  ↓
Parse
  ↓
Normalize
  ↓
Validate
  ↓
Prepared Canonical Data
```

Workers do not determine final event ordering.

Parallelism is primarily across independent:

```text
files
instruments
exchanges
datasets
```

A fixed worker pool prevents nested exchange × instrument × file parallelism from creating excessive threads.

## Bounded Processing and Temporary Runs

Workers process input using bounded memory.

```text
source stream
    ↓
parse
    ↓
normalize
    ↓
bounded RAM batch
    ↓
sort if required
    ↓
temporary run
```

A large input may therefore produce:

```text
data/.work/{job_id}/
├── task-0000/
│   ├── run-00000.arrow
│   ├── run-00001.arrow
│   └── run-00002.arrow
└── task-0001/
    ├── run-00000.arrow
    └── run-00001.arrow
```

Temporary runs are internal processing artifacts, not final datasets.

Arrow IPC is suitable for temporary canonical runs because they are short-lived intermediate data and do not require final analytical-storage compression.

If an input is already ordered and does not require staging, MarketForge may use a direct path:

```text
Raw
 ↓
Parse
 ↓
Normalize
 ↓
Validate
 ↓
Parquet
```

Temporary runs are primarily required for:

```text
large inputs
sorting
parallel preparation
multi-input merging
bounded intermediate storage
```

## Chronological Merge

Each prepared input becomes an ordered event stream.

Multiple streams are combined using a k-way merge:

```text
Stream A ─┐
Stream B ─┤
Stream C ─┼→ Min-Heap → Chronological Stream
Stream D ─┘
```

For \(N\) events across \(k\) streams:

\[
O(N \log k)
\]

The merge keeps only the current event or event group from each active stream, plus bounded reader buffers.

Physical I/O remains batched:

```text
Parquet / Arrow
      ↓
RecordBatch
      ↓
cursor
      ↓
next event group
```

When a batch is exhausted:

```text
load next batch
    ↓
reset cursor
    ↓
continue merge
```

This allows very large numbers of streams to be merged without loading complete datasets into memory.

### Combined Trades and Depth

Trades and depth use the same merge mechanism:

```text
Trades ───────┐
Depth ────────┼→ chronological combined stream
Other Input ──┘
```

Result:

```text
L2Snapshot
L2Update
Trade
L2Update
Trade
L2Update
...
```

### Source Event Groups

One native book message may normalize into multiple `L2Update` rows.

These mutations remain one logical source event:

```text
Source Event
├── L2Update
├── L2Update
└── L2Update
```

The group must not be interleaved with unrelated events.

Final ordering is deterministic and must never depend on:

```text
worker completion order
thread scheduling
temporary-file creation order
```

## Stateful Book Processing

Some processing operations require current order-book state.

For each required book:

```text
L2Snapshot
    ↓
initialize Book

L2Update
    ↓
mutate Book
```

Independent books maintain independent state:

```text
Book[exchange, instrument]
Book[exchange, instrument]
Book[exchange, instrument]
...
```

Updates for one book must remain sequential.

Stateful processing is required for operations such as:

```text
current BBO
mid price
book validation
relative-depth transformation
cross-market reference prices
depth aggregation
checkpoint generation
```

It is not mandatory for every job.

For example:

```text
trades only
    → no book required

canonical depth conversion
    → may not require reconstruction

relative-depth transformation
    → book required

cross-market depth aggregation
    → books required
```

Canonical normalization remains separate from derived stateful transformations.

## Parquet Output

The final chronological stream is written using bounded Arrow batches:

```text
Chronological Events
        ↓
Arrow Batch Builder
        ↓
RecordBatch
        ↓
Parquet Writer
```

Multiple batches form Parquet row groups:

```text
Arrow Batch ─┐
Arrow Batch ─┼→ Row Group
Arrow Batch ─┘
```

Multiple row groups form one Parquet part:

```text
part-00000.parquet
├── Row Group 0
├── Row Group 1
├── Row Group 2
└── Row Group 3
```

Parts rotate when they reach an approximate target physical size.

Typical initial targets:

```text
Arrow batch      bounded by memory
Row group        ~64–128 MiB
Parquet part     ~256–512 MiB
```

These are performance settings and should be benchmarked.

Processing batches, row groups, and Parquet files are independent boundaries.

All output remains chronological:

\[
\max(T_i)
\le
\min(T_{i+1})
\]

for consecutive Parquet parts.

## Dataset Completion

When a Parquet part closes, Rust records:

```text
path
start_timestamp_ns
end_timestamp_ns
row_count
size_bytes
```

After all parts complete:

```text
part-00000.parquet ✓
part-00001.parquet ✓
part-00002.parquet ✓
        ↓
manifest.json.tmp
        ↓
atomic rename
        ↓
manifest.json
```

Final layout:

```text
data/datasets/{dataset_id}/
├── manifest.json
├── part-00000.parquet
├── part-00001.parquet
└── part-00002.parquet
```

The final manifest marks physical dataset completion.

If processing fails before publication:

```text
manifest.json
```

does not exist and the partial output is not considered a completed MarketForge dataset.

Rust returns a small structured `ProcessingResult` to Python.

Python then updates:

```text
catalog.datasets
catalog.dataset_inputs
```

Rust does not require direct PostgreSQL access.

## Resource Model

CPU, memory, temporary storage, and final storage are controlled independently.

Conceptually:

\[
W
=
\min
\left(
W_{\text{CPU}},
W_{\text{memory}}
\right)
\]

where \(W\) is the number of active workers.

Memory is divided across:

```text
worker buffers
active input batches
temporary-run readers
book state
Arrow output batches
Parquet writer
safety headroom
```

More streams therefore require smaller active buffers or more memory.

Smaller buffers trade memory usage for additional I/O overhead.

For extremely large merges, MarketForge may use hierarchical merging:

```text
many streams
    ↓
intermediate merged runs
    ↓
smaller number of streams
    ↓
final merge
```

This allows processing to degrade toward additional disk I/O rather than failing because the complete workload does not fit in RAM.

## Core Rules

- independent input preparation is parallel
- one fixed worker pool controls concurrency
- input size must not determine required RAM
- workers operate on bounded batches
- temporary runs provide spill storage when required
- already ordered simple inputs may bypass temporary runs
- physical reads and writes are batched
- chronological merging operates logically at event/source-event granularity
- one native source event remains atomic after normalization
- worker execution order never determines market-event order
- stateful updates for one book remain sequential
- book reconstruction is enabled only when required
- canonical normalization is separate from derived stateful transformations
- Arrow batches, Parquet row groups, and Parquet files are independent boundaries
- final Parquet parts are chronological and size-bounded
- the final manifest is written only after successful dataset completion
- large workloads should become slower through bounded I/O/spilling rather than require the complete dataset in memory

### Book-State Rule

If a processing job contains depth, MarketForge **always maintains current order-book state**.

```text
Trades only
    → stateless processing

Depth present
    → BookStore enabled
```

For each depth stream:

```text
L2Snapshot → initialize / replace book
L2Update   → mutate current book
```

`BookStore` maintains one independent book per exchange/instrument/depth stream.

The maintained state provides a shared foundation for:

```text
BBO
mid price
book validation
relative-depth transformation
cross-market reference prices
depth aggregation
checkpoint generation
```

These operations consume the same `BookState`; they do not implement separate reconstruction logic.

Canonical events are still written normally. Maintaining book state does not require emitting a complete snapshot after every update.