
I am preparing to implement `marketforge-process`, the Rust historical-data processing engine for MarketForge.

I have already spent considerable time defining the architecture. I am providing the design documents and current repository tree.

Your task is to produce a **dependency-aware implementation roadmap**, not production code.

I will implement the engine incrementally, one step at a time. Your plan should let me later say:

> Implement S07.

and work on that step without redesigning the surrounding architecture.

Do not implement files, rewrite the repository, or begin executing the roadmap.

## `marketforge-process` Implementation Order

Short implementation sequence for the initial Rust offline engine. The architecture already defines `ProcessingJob → workers → temporary runs → merge → Parquet → manifest`. :chatgpt-content-reference{index="0"}

### Phase 1 — Rust Foundation

- Cargo features: `process`, `live`, optional `monitor-tui`
- `marketforge-process` binary
- shared error types
- canonical Rust types:
  - `Trade`
  - `L2Level`
  - `L2LevelUpdate`
  - `L2Snapshot`
  - atomic `L2Update`
  - `CanonicalEvent`
- numeric types and timestamp conventions

### Phase 2 — Processing Protocol

- `ProcessingJob`
- `WorkTask`
- stream configuration and `stream_rank`
- resource configuration
- integrity policy
- output configuration
- `ProcessingResult`
- JSON loading and job validation
- hand-written `engine/fixtures/jobs/*.json`

Python job generation comes later; Rust fixtures are used while the protocol evolves. :chatgpt-content-reference{index="1"}

### Phase 3 — Shared Raw Processing

- source/file reader
- GZIP
- ZIP
- TAR.GZ
- CSV decoder
- JSONL decoder
- shared timestamp/decimal/quantity normalization
- canonical validation
- format-specific validation interface

### Phase 4 — First Real Format

Implement one simple trade format first:

```text
Raw trade fixture
    ↓
Open / Decompress
    ↓
CSV
    ↓
Parameterized Trade Processor
    ↓
Normalize
    ↓
Validate
    ↓
Canonical Trade
```

Use the existing historical raw fixtures where suitable; the repository already contains raw fixtures for multiple exchanges and market types. :chatgpt-content-reference{index="2"}

### Phase 5 — Sequential Worker

Implement:

```text
WorkTask
    ↓
SequentialTaskRunner
    ↓
bounded canonical batch
    ↓
sort if required
    ↓
TaskResult
```

Keep individual pipeline stages separate so execution can be optimized later.

### Phase 6 — Temporary Runs

- Arrow schema for `CanonicalEvent`
- Arrow batch builder
- Arrow IPC writer
- Arrow IPC reader
- `RunDescriptor`
- memory-driven flushing
- scratch layout and accounting

```text
data/.work/{job_id}/{task_id}/run-*.arrow
```

Temporary runs are bounded intermediate storage, not final dataset files. :chatgpt-content-reference{index="3"}

### Phase 7 — Merge Engine

- ordered run cursor
- k-way min-heap merger
- `event_timestamp_ns` ordering
- `stream_rank` tie breaking
- overlapping-run support
- deterministic output independent of worker execution order

### Phase 8 — Parquet Output

- combined event-envelope Arrow schema
- Trade payload
- L2Snapshot payload
- L2Update payload with nested `changes[]`
- bounded RecordBatch writing
- Parquet row groups
- size-based Parquet part rotation
- Parquet round-trip tests

Each Parquet row represents one atomic canonical event. :chatgpt-content-reference{index="4"}

### Phase 9 — Dataset Lifecycle

- staging under `.work`
- final output directory
- `manifest.json`
- file ranges / rows / sizes
- integrity summary
- atomic publication
- cleanup on success
- structured failure result

```text
.work/{id}/output/
    ↓
complete + manifest
    ↓
datasets/{id}/
```

Incomplete output must never appear as a completed dataset. :chatgpt-content-reference{index="5"}

### Phase 10 — L2 Processing

- parameterized/custom depth processors
- snapshot assembly
- atomic update grouping
- sequence mapping
- format-specific continuity validation
- L2 fixture tests

### Phase 11 — BookStore

- `BookKey`
- `BookState`
- `BookStatus`
- bids / asks
- snapshot replacement
- atomic update application
- BBO
- midpoint
- invalidation/recovery

Every processing job containing depth maintains book state. :chatgpt-content-reference{index="6"}

### Phase 12 — Combined Trades + Depth

```text
Trade stream ───┐
                ├→ deterministic merge → BookStore → Parquet
Depth stream ───┘
```

Verify that the final dataset can be replayed chronologically while maintaining the current book.

### Phase 13 — Parallel Workers

- Rayon fixed worker pool
- multiple independent `WorkTask`s
- cancellation on fatal failure
- deterministic collection of `TaskResult`s

Verify:

```text
workers = 1
    ==
workers = N
```

for canonical output and integrity results.

### Phase 14 — Resource Enforcement

- worker memory budgets
- bounded batch targets
- merge-reader budget
- scratch hard limit
- BookStore memory accounting
- controlled resource failure

### Phase 15 — Remaining Formats

Add formats incrementally:

```text
Bybit
Binance
OKX
Bitget
Gate.io
```

Prefer parameterized processors when cleanly expressible; use custom processors for exceptional semantics.

### Phase 16 — Hardening

- corruption tests
- malformed-record policies
- sequence-gap policies
- timestamp-regression tests
- scratch exhaustion
- execution-equivalence tests
- full end-to-end fixture jobs

### Phase 17 — Monitoring and Performance

Only after correctness:

- processing metrics
- integrity metrics
- simple progress output
- optional TUI
- benchmarks
- profiling
- optimize demonstrated bottlenecks

### Phase 18 — Python Integration

After `ProcessingJob` / `ProcessingResult` stabilize:

```text
Python Planner
    ↓
JobStore
    ↓
data/jobs/{id}.json
    ↓
Rust Runner
    ↓
ProcessingResult
    ↓
PostgreSQL update
```

The current repository already separates the Python application and Rust `engine/`, so Rust tests should remain under `engine/tests/` while existing Python tests stay under top-level `tests/`. :chatgpt-content-reference{index="7"} :chatgpt-content-reference{index="8"}

> **Implementation strategy:** build one complete trade-only vertical slice first, then L2/BookStore, then combined processing, then parallelism and optimization.

## Quick Navigation

- [Inputs and Evidence](#inputs-and-evidence)
- [Scope and Fixed Decisions](#scope-and-fixed-decisions)
- [Consistency Check](#consistency-check)
- [Implementation Strategy](#implementation-strategy)
- [Required Roadmap Format](#required-roadmap-format)
- [Testing and Completion](#testing-and-completion)
- [Final Summary](#final-summary)

## Inputs and Evidence

### Design Documents

`repomix-output(20261005-114137).xml` contains the processing design notes.

Read its internal `<file path="...">` entries. Despite the generic Repomix introduction, this attachment is a design-document bundle, not a complete implementation snapshot.

Relevant documents include:

```text
BookStore.md
Combined Parquet Event Model.md
Dataset Lifecycle.md
Offline Merging Logic.md
Offline Processing Execution.md
Parameterized Raw Processors.md
Processed Dataset Storage.md
Processing Configuration and Ordering.md
Processing Engine.md
Processing Error Policy.md
Processing Monitor.md
Python ↔ Rust Processing Protocol 1.md
Raw Format Processing Architecture.md
Replaceable Worker Execution.md
Resource Configuration.md
Rust Engine Architecture and Coding Style.md
Rust Processing Tests.md
Temporary Runs.md
Worker Pipeline.md
```

### Repository Tree

`Pasted text(20261005-114343).txt` contains the current repository structure.

Use it to understand existing locations, not to infer unseen implementations.

Important observations to account for:

- The Rust directory is `engine/`; the tree lists `engine/cargo.toml` and `engine/src/`. Verify the manifest filename and contents before relying on a build configuration.
- Python tests already live under top-level `tests/`.
- Existing historical fixtures are listed under `tests/fixtures/raw/`, while the design proposes Rust-owned fixture directories under `engine/fixtures/`.
- Python already has directories such as `src/marketforge/engine/`, `processing/`, `storage/`, `catalog/`, and `database/`. Their presence does not establish what is implemented.
- The canonical schema document and detailed historical-format documents are listed in the tree, but their complete contents are not included in this design bundle.

Do not invent source code, fixture filenames, field mappings, dependencies, or completed functionality.

When a required implementation detail is unavailable, identify the exact missing material and the step that needs it. Continue planning unaffected work rather than blocking the entire roadmap.

Use document filenames and headings when referring to design decisions.

## Scope and Fixed Decisions

Preserve the following architecture. Do not replace it with a different processing framework merely because another design is possible.

| Area | Established direction |
|---|---|
| Rust packaging | One crate under `engine/`, with `marketforge-process` and a future `marketforge-live` binary |
| Dependency isolation | Explicit Cargo features, optional heavy dependencies, and a dependency-light shared core |
| Current implementation scope | Offline processing; do not implement live acquisition or networking |
| Python boundary | Python plans and registers work; Rust executes processing without PostgreSQL access |
| Development protocol | Start with Rust job models and hand-written JSON job fixtures; build Python job generation after the protocol stabilizes |
| Worker execution | One worker processes one `WorkTask` sequentially; initial parallelism is between tasks |
| Replaceability | Keep pipeline stages separate so the task execution strategy can change later |
| Raw processing | Shared I/O and decoders, parameterized common formats, custom processors for exceptional semantics |
| Canonical events | `Trade`, `L2Snapshot`, and atomic `L2Update` containing `changes: Vec<L2LevelUpdate>` |
| Validation | Shared canonical validation plus format-specific validation |
| Book state | Every job containing depth maintains independent `BookStore` state per depth stream |
| Intermediate storage | Bounded batches and Arrow IPC temporary runs |
| Merging | Deterministic, batch-read, cursor-driven k-way merging |
| Output | One chronological combined stream using an envelope and typed nullable payloads |
| Final storage | Dataset-ID directories, size-based chronological Parquet parts, and a final manifest; no mandatory daily folders |
| Reliability | Fatal source/archive failures; configurable integrity policy; no generic duplicate detection or deduplication |
| Lifecycle | Private staging, publication only after completion, persistent job specifications, and full retries in v1 |
| Monitoring | Shared processing/integrity metrics; optional dashboard implemented after the core works |

The worker pipeline remains:

```text
WorkTask
    ↓
Open Source
    ↓
Stream Decompress / Unpack
    ↓
Parse Raw Records
    ↓
Normalize
    ↓
Validate
    ↓
Bounded RAM Batch
    ↓
Sort if Required
    ↓
Temporary Arrow Run
    ↓
Repeat Until EOF
    ↓
TaskResult
```

Workers prepare inputs. The dataset-level pipeline owns cross-input merging, ordered book replay, requested transformations, final Parquet writing, and manifest publication.

### Ordering

Preserve authoritative source order within each stream.

Across eligible ordered streams, use:

```text
event_timestamp_ns
    ↓
stable stream_rank
```

Do not compare unrelated exchange sequences globally. Worker completion order must never determine output order.

An `L2Update` remains one atomic event regardless of the number of entries in `changes[]`.

### Scope Boundaries

Canonical processing and chronological merging are the first priority.

Numeraire conversion, relative-depth transformations, and aggregation remain separate derived operations. Plan them after the core pipeline rather than making them prerequisites for basic processing.

Options may retain their canonical representation and available metadata, but advanced option transformations and economic option-depth aggregation are outside the current scope.

## Consistency Check

Start with a **brief implementation-readiness review**, not another architecture essay.

Some documents contain older wording. Identify meaningful conflicts explicitly rather than silently choosing one or implementing both.

In particular, inspect:

```text
optional BookStore
    vs
depth present → always maintain BookStore

one L2Update per level
    vs
one atomic L2Update containing changes[]

duplicate counters/handling
    vs
no generic duplicate detection or deduplication

writing partial output into final dataset directories
    vs
private staging and final publication
```

Use the explicit fixed decisions in this prompt as the intended target and state which older passages need updating.

Also identify unresolved details that could block a concrete implementation step. Examples worth checking include:

```text
numeric representation, precision, rounding, and overflow

exact Arrow types and nullability

source order versus timestamp regressions

stable ordering across task, batch, and run boundaries

continuity validation across multiple files of one stream

job ID, dataset ID, and retry identity

relative-path resolution in job fixtures

scratch storage on a different filesystem from final output

handling one unusually large atomic event within memory limits

reading a depth time range that begins after its initial snapshot
```

Do not silently settle these as though the documents already define the answers.

For each substantive issue, state:

```text
source document / section
what is unclear or conflicting
which implementation step depends on it
smallest recommended clarification
whether other work can proceed
```

Distinguish established decisions from your recommendations.

Do not let a deferred optimization block the first working implementation.

## Implementation Strategy

Prefer **small vertical slices** over building every module before anything runs.

### First Working Slice

Aim for an early path approximately like:

```text
Hand-written ProcessingJob JSON
    ↓
One small historical trade fixture
    ↓
Shared source/decompression
    ↓
CSV decoding
    ↓
Parameterized format specification
    ↓
Canonical Trade normalization
    ↓
Validation
    ↓
Bounded sequential worker
    ↓
Arrow IPC temporary run
    ↓
Read temporary run
    ↓
Parquet output
    ↓
Manifest and publication
    ↓
Read back and verify expected canonical events
```

A Bybit Spot Trades fixture is a candidate, not an assumption about available bytes or field mappings. Select the first format using actual supplied format documentation and fixture contents when available.

Introduce the combined event-envelope contract early enough that the first trade output does not become a throwaway storage format.

It is acceptable to implement only the trade payload initially, provided unsupported event types are rejected explicitly and that limitation is clear.

### Expand Incrementally

After the first slice works, expand toward:

```text
second trade format to verify reuse
L2 snapshots and atomic updates
BookStore and continuity validation
trades + depth combined output
multiple input streams and overlapping runs
multi-worker execution
resource-budget enforcement
additional historical formats
dataset-to-dataset merging
metrics and optional monitoring
derived transformations
Python integration after protocol stabilization
```

Adjust this order where dependencies require it, and explain the adjustment.

Correctness checks, bounded buffering, and safe output handling should grow with the relevant implementation—not be postponed as a final cleanup phase.

### Keep the First Version Simple

Use a working sequential baseline before adding multi-worker execution.

Keep `SequentialTaskRunner` separate from format semantics, but do not build a general task-graph framework.

Initially staging through Arrow runs is acceptable. Direct streaming, resumable execution, cached tasks, specialized merge algorithms, and custom high-performance book structures can remain later optimizations.

Do not create empty modules or traits merely to reproduce every box in a diagram.

## Required Roadmap Format

Organize the roadmap into phases with stable step identifiers:

```text
S01
S02
S03
...
```

Each step should be small enough to implement, compile, and test independently.

Use this template:

### SXX — Step Name

**Objective:** What capability this step adds and why it belongs here.

**Depends on:** Explicit earlier step IDs.

**Files and modules:** Exact paths to create or modify. Clearly distinguish existing inspected files, files known only from the tree, and proposed files.

**Types and contracts:** Important structs, enums, functions, or interfaces. Specify ownership and stage boundaries where relevant, without writing the implementation.

**Dependencies and features:** Crates or Cargo changes needed now. Do not add dependencies for functionality scheduled much later. Do not assert current versions without verification.

**Tests:** Concrete cases, expected outcomes, and where the tests belong.

**Verification:** Commands to compile or test the completed step. State the working directory and required features consistently.

**Done when:** An observable acceptance condition—not merely “module created.”

**Not included yet:** What remains intentionally deferred.

Do not use giant steps such as “implement the processing engine” or “support all exchanges.”

Do not label placeholder implementations as completed functionality. A scaffold may compile, but it must not claim that processing or publication succeeded.

## Testing and Completion

Tests accompany each step rather than appearing only in the final phase.

Keep these boundaries:

```text
engine/src/** inline tests
    → local unit tests

engine/tests/
    → Rust component and integration tests

engine/fixtures/
    → proposed Rust job/expected/curated fixture data

engine/benches/
    → Rust performance benchmarks

top-level tests/
    → existing Python tests and later Python-to-Rust integration
```

The repository already lists raw fixtures under top-level `tests/fixtures/raw/`. Propose an explicit reuse or fixture-location strategy. Do not blindly duplicate large archives or reorganize Python tests.

Ensure nested Rust test modules are actually connected to test entry points and feature-gated configurations.

The roadmap must cover:

```text
real raw fixture → independently checked canonical values

Trade / L2Snapshot / L2Update Arrow and Parquet round trips

atomic L2Update.changes[] preservation

source continuity across file and run boundaries

equal-timestamp ordering

overlapping runs from the same source

workers=1 versus workers=N

small versus larger successful memory budgets

different temporary-run boundaries

archive corruption discovered before and during processing

missing snapshot and invalid BookStore behavior

policy-controlled record failures

scratch-budget exhaustion and cancellation

no publication of partial datasets

manifest/file consistency
```

Compare decoded logical output, ordering, book states, and integrity results. Do not require identical elapsed times, monitoring samples, or incidental file bytes.

Expected market values must not be generated solely by the same normalization code being tested.

All ordinary Rust processing tests should run locally without exchange requests, Python orchestration, or a PostgreSQL server.

## Final Summary

End with a compact summary containing:

- The complete ordered step index and critical dependency path.
- Milestone IDs for the first runnable binary, first valid published Parquet dataset, first depth dataset, first combined dataset, and first multi-worker equivalent execution.
- Remaining decision gates and the last step before each must be resolved.
- Features intentionally postponed beyond the core engine.
- The conditions under which the Python planner and runner can begin implementation.

Finish by identifying the first implementation step and any exact additional file contents needed for it.

Do not begin implementing that step.

The goal is a practical construction plan: **small tested changes, early runnable output, preserved market semantics, and no unnecessary redesign.**