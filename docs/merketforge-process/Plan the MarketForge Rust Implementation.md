`marketforge-process` is the Rust historical-data execution engine for MarketForge.

Python is already the control plane. It discovers raw archives, resolves catalog metadata and normalization rules, registers datasets, assigns stream ordering, and generates complete process and merge job JSON files.

Rust receives those frozen job specifications and performs the heavy data work:

```text
Python
    ↓
ProcessingJob JSON
    ↓
marketforge-process
    ↓
canonical processing / merging
    ↓
Parquet + manifest
    ↓
ProcessingResult
    ↓
Python
```

The implementation goal is therefore no longer to design the Python ↔ Rust protocol from scratch. The immediate goal is to make the Rust engine execute the real jobs already produced by Python.

## Quick Navigation

- [Current State](#current-state)
- [Implementation Principles](#implementation-principles)
- [Target Processing Pipeline](#target-processing-pipeline)
- [Phase 1 — Protocol Alignment and Execution](#phase-1--protocol-alignment-and-execution)
- [Phase 2 — First Trade Vertical Slice](#phase-2--first-trade-vertical-slice)
- [Phase 3 — Canonical Arrow and Temporary Runs](#phase-3--canonical-arrow-and-temporary-runs)
- [Phase 4 — Chronological Merge and Parquet](#phase-4--chronological-merge-and-parquet)
- [Phase 5 — Dataset Publication](#phase-5--dataset-publication)
- [Phase 6 — Trade Format Coverage](#phase-6--trade-format-coverage)
- [Phase 7 — L2 Processing](#phase-7--l2-processing)
- [Phase 8 — BookStore and Combined Processing](#phase-8--bookstore-and-combined-processing)
- [Phase 9 — Dataset-to-Dataset Merge](#phase-9--dataset-to-dataset-merge)
- [Phase 10 — Parallelism and Resource Enforcement](#phase-10--parallelism-and-resource-enforcement)
- [Phase 11 — Integrity and Hardening](#phase-11--integrity-and-hardening)
- [Phase 12 — Runtime Integration and Performance](#phase-12--runtime-integration-and-performance)
- [Implementation Step Index](#implementation-step-index)
- [Milestones](#milestones)
- [Deferred Work](#deferred-work)

## Current State

### Python

Python orchestration is already implemented far enough to generate real execution manifests.

Current development jobs include:

```text
36 process jobs
+
representative merge jobs
```

covering combinations of:

```text
Bybit
Binance
OKX
Bitget
Gate.io

spot
perpetual
future

linear
inverse

trade
L2
```

The generated jobs are now the primary protocol fixtures for Rust development.

Hand-written speculative job JSON is no longer the main development strategy.

### Rust

The Rust engine already contains substantial foundation code.

Implemented foundations include:

```text
canonical/
    Trade
    L2Level
    L2LevelUpdate
    L2Snapshot
    L2Update
    CanonicalEvent
    EventEnvelope
    sequence metadata
    exact Decimal values

job/
    ProcessingJob
    WorkTask
    InstrumentSpec
    stream configuration
    ordering
    resources
    integrity policy
    output configuration
    ProcessingResult
    JSON loading
    structural validation

source/
    plain
    GZIP
    ZIP
    TAR.GZ

decode/
    CSV

normalize/
    timestamps
    decimals
    trade side

formats/
    generic CSV trade processing
```

The major execution modules remain largely unimplemented:

```text
process/worker/
process/runs/
process/merge/
process/parquet/
process/manifest/
process/metrics/

book/
validate/
```

The current `marketforge-process` binary loads and validates a job but does not yet execute the processing pipeline.

## Implementation Principles

The responsibility boundary is:

```text
Python
    → planning
    → catalog access
    → dataset registration
    → job construction

Rust
    → execution
    → raw parsing
    → normalization
    → validation
    → temporary runs
    → chronological merging
    → BookStore
    → Parquet
    → manifest
```

Rust does not access PostgreSQL.

The core abstraction is:

```text
bounded chronological CanonicalEvent stream
```

Canonical events remain:

```text
Trade

L2Snapshot

L2Update
└── changes[]
```

One native L2 update remains one atomic canonical event.

### Ordering

Within a source stream, authoritative source ordering must be preserved according to the source format.

Across independent streams, deterministic ordering is:

```text
event_timestamp_ns
    ↓
stream_rank
```

Worker completion order, thread scheduling, temporary-run creation order, and filesystem ordering must never determine canonical output.

### Merge Semantics

MarketForge merge is a lossless chronological operation.

Any supported canonical datasets may coexist in one merged timeline:

```text
spot + perpetual
spot + future
perpetual + future
linear + inverse
BTC + ETH
USDT + USDC
trade + L2
cross-exchange
```

MarketForge does not modify native canonical prices to make instruments economically equivalent.

Relationships such as:

```text
spread
basis
relative return
price divergence
synthetic price
cross-asset response
```

belong to downstream feature generation.

The merge engine therefore performs:

```text
canonical streams
        ↓
event_timestamp_ns
        ↓
stream_rank
        ↓
one deterministic chronological stream
```

## Target Processing Pipeline

Raw processing:

```text
ProcessingJob
    ↓
Validate / Preflight
    ↓
WorkTask
    ↓
Open Source
    ↓
Decompress / Unpack
    ↓
Decode Raw Records
    ↓
Format Processing
    ↓
Normalize
    ↓
Canonical Validate
    ↓
Bounded Event Buffer
    ↓
Sort if permitted / required
    ↓
Arrow IPC Temporary Runs
    ↓
K-Way Chronological Merge
    ↓
BookStore if depth is present
    ↓
Arrow RecordBatches
    ↓
Parquet Parts
    ↓
manifest.json
    ↓
Atomic Publication
    ↓
ProcessingResult
```

Dataset merge uses a shorter path:

```text
ProcessingJob(operation=merge)
    ↓
Canonical Dataset Inputs
    ↓
Parquet Readers
    ↓
Canonical Event Cursors
    ↓
K-Way Chronological Merge
    ↓
BookStore if depth is present
    ↓
Parquet Parts
    ↓
manifest.json
    ↓
Atomic Publication
```

Raw processing and dataset merging converge at the canonical event-stream boundary.

---

	## Phase 1 — Protocol Alignment and Execution

### S01 — Align Rust With Current Python Process Jobs

**Objective:** Make Rust deserialize and validate the actual process manifests currently generated by Python.

**Depends on:** Existing Rust foundation.

Compare Rust job structures against the real generated process JSONs.

Support the complete task representation required by those manifests, including:

```text
raw schema
instrument specification
source ordering
source container
archive member

normalizations[]
    target_schema
    timestamp_encoding
    quantity_encoding
    normalization rules
```

Depth tasks may contain multiple normalization targets:

```text
l2_snapshot
l2_update
```

Rust must not assume one normalization configuration per `WorkTask`.

**Tests:**

Load every current process-job JSON.

Expected:

```text
36 / 36 deserialize
36 / 36 validate structurally
```

No raw processing occurs yet.

**Done when:** Every current Python-generated process manifest can be loaded by Rust without protocol translation or hand editing.

**Not included yet:** Actual archive processing.

### S02 — Model Process and Merge Inputs Explicitly

**Objective:** Represent the two execution operations without forcing merge jobs into raw `WorkTask` semantics.

**Depends on:** S01.

The common job envelope contains:

```text
protocol_version
job_id
dataset_id
streams
ordering
integrity_policy
resources
output
```

Operation-specific inputs are:

```text
Process
    → raw WorkTask[]

Merge
    → canonical dataset inputs[]
```

Validation becomes operation-aware:

```text
process
    → requires tasks

merge
    → requires merge_inputs
```

A merge job must not fail simply because it has no raw processing tasks.

**Tests:**

```text
valid process job        → accepted
valid merge job          → accepted
process without tasks    → rejected
merge without inputs     → rejected
```

**Done when:** All current process and merge manifests deserialize and pass the appropriate structural validation.

**Not included yet:** Merge execution.

### S03 — Execution Shell and Workspace Lifecycle

**Objective:** Replace the current placeholder success path with a real execution boundary.

**Depends on:** S02.

Implement:

```text
load job
    ↓
validate
    ↓
preflight
    ↓
prepare .work/{dataset_id}/
    ↓
dispatch Process | Merge
    ↓
ProcessingResult
```

The binary remains thin.

Processing logic belongs in the library.

Unsupported or unfinished execution paths must return explicit failures rather than:

```text
status = complete
events_written = 0
files_written = 0
```

**Done when:** Running `marketforge-process` enters a real executor and cannot report a false successful dataset.

**Not included yet:** Complete processing pipeline.

### S04 — Canonical Validation

**Objective:** Establish the correctness boundary every format must pass before data reaches temporary storage.

**Depends on:** S03.

Implement shared validation for:

```text
Trade
L2Level
L2LevelUpdate
L2Snapshot
L2Update
CanonicalEvent
```

Validate universal invariants such as:

```text
price > 0
valid quantity representation
valid timestamps
valid set/delete representation
valid bid/ask levels
valid sequence structure where universally applicable
```

Format-specific continuity remains separate.

**Done when:** Invalid canonical events cannot enter the processing pipeline.

**Not included yet:** Exchange-specific sequence continuity.

---

## Phase 2 — First Trade Vertical Slice

### S05 — First Real Trade Processor

**Objective:** Process one real archive referenced by an existing Python-generated job into canonical trades.

**Depends on:** S04.

Prefer a simple CSV trade source already represented by the current archive corpus.

Pipeline:

```text
real archive
    ↓
source reader
    ↓
CSV decoder
    ↓
format specification
    ↓
trade processor
    ↓
canonical Trade
    ↓
canonical validation
```

Initially an in-memory event collection is acceptable for a small fixture.

**Tests:**

Verify independently selected source rows against expected canonical values:

```text
timestamp
side
price
quantity
trade ID
instrument identity
```

Expected values must not be generated solely by the same normalization code being tested.

**Done when:** A real historical archive produces independently verified canonical `Trade` events.

**Not included yet:** Arrow, temporary runs, or Parquet.

### S06 — Complete Quantity Normalization

**Objective:** Correctly represent spot and derivative quantity semantics.

**Depends on:** S05.

Support:

```text
base quantity
quote quantity
contract quantity
```

using:

```text
quantity_encoding
price
contract_kind
contract_value
contract_value_asset
```

Cover:

```text
spot
linear perpetual
inverse perpetual
linear future
inverse future
```

No exchange-specific contract economics should be hardcoded when `InstrumentSpec` already provides them.

**Tests:**

Include at least:

```text
spot base quantity
linear contract quantity
inverse contract quantity
dated future
```

**Done when:** Canonical quantities preserve the economics described by the job's instrument metadata.

---

## Phase 3 — Canonical Arrow and Temporary Runs

### S07 — Canonical Arrow Schema

**Objective:** Define one stable Arrow representation shared by temporary runs and final Parquet.

**Depends on:** S06.

Use one event envelope:

```text
event_timestamp_ns
system_timestamp_ns
exchange
instrument_id
symbol
stream_id
event_type
```

with nullable typed payloads:

```text
trade
l2_snapshot
l2_update
```

Exactly one payload is populated according to `event_type`.

The schema should reserve the final L2 representation even if only trades are executable initially.

**Done when:** The Arrow schema can represent every canonical event type without introducing a future incompatible trade-only format.

### S08 — Canonical Arrow Conversion

**Objective:** Convert canonical Rust events to and from Arrow batches.

**Depends on:** S07.

Implement:

```text
CanonicalEvent
    ↓
Arrow builders
    ↓
RecordBatch
```

and the inverse reader path.

Initial tests focus on `Trade`.

Later L2 support uses the same schema.

**Done when:**

```text
CanonicalEvent
→ Arrow
→ CanonicalEvent
```

preserves the logical trade event exactly.

### S09 — Temporary Arrow Runs

**Objective:** Introduce bounded intermediate storage.

**Depends on:** S08.

Implement:

```text
RunDescriptor
Arrow IPC writer
Arrow IPC reader
run indexing
scratch layout
timestamp ranges
event counts
size accounting
```

Layout:

```text
.work/{job_id}/
└── task-{task_id}/
    ├── run-000000.arrow
    ├── run-000001.arrow
    └── ...
```

`RunDescriptor` contains at least:

```text
path
task_id
stream_id
run_index
first_timestamp_ns
last_timestamp_ns
event_count
size_bytes
```

**Done when:** Canonical trades can be written to a temporary run and read back without semantic loss.

### S10 — SequentialTaskRunner

**Objective:** Compose the processing stages into one bounded task executor.

**Depends on:** S09.

Pipeline:

```text
WorkTask
    ↓
SourceReader
    ↓
Decoder
    ↓
FormatProcessor
    ↓
Normalizer
    ↓
CanonicalValidator
    ↓
Bounded Buffer
    ↓
Sort if required
    ↓
RunWriter
    ↓
TaskResult
```

`TaskResult` returns metadata and run descriptors, not complete event vectors.

Start with:

```text
workers = 1
```

**Done when:** One real trade `WorkTask` can produce one or more ordered temporary Arrow runs.

---

## Phase 4 — Chronological Merge and Parquet

### S11 — Deterministic K-Way Run Merge

**Objective:** Implement the central chronological merge primitive.

**Depends on:** S10.

Implement:

```text
RunCursor
MinHeap
KWayMerger
```

Global ordering:

```text
event_timestamp_ns
    ↓
stream_rank
```

Worker completion order, run creation order, and filesystem ordering must never affect canonical output.

Tests cover:

```text
single stream
multiple streams
equal timestamps
stream-rank tie breaking
empty stream
overlapping runs
multiple runs from one task
atomic events
```

**Done when:** Arbitrary ordered temporary runs produce one deterministic canonical event stream.

### S12 — Parquet Dataset Writer

**Objective:** Write the merged canonical stream into bounded chronological Parquet parts.

**Depends on:** S11.

Implement:

```text
CanonicalEvent stream
    ↓
Arrow RecordBatch
    ↓
Parquet row groups
    ↓
size-bounded Parquet parts
```

Track per part:

```text
path
start_timestamp_ns
end_timestamp_ns
row_count
size_bytes
```

Processing batches, row groups, and Parquet files remain independent boundaries.

**Tests:**

```text
CanonicalEvent
→ Arrow
→ Parquet
→ Arrow
→ CanonicalEvent
```

must preserve logical event values and ordering.

**Done when:** A real trade process job produces readable canonical Parquet with correct ordering.

---

## Phase 5 — Dataset Publication

### S13 — Manifest and Atomic Publication

**Objective:** Turn staged Parquet output into a physically complete MarketForge dataset.

**Depends on:** S12.

Implement:

```text
.work/{dataset_id}/output/
    ↓
complete Parquet parts
    ↓
manifest.json
    ↓
atomic publication
    ↓
configured dataset_path
```

Manifest records:

```text
dataset ID
schema/protocol information
content type
time range
ordering
stream ranks
files
rows
sizes
integrity summary
effective processing configuration
```

Return a real `ProcessingResult` containing:

```text
manifest_path
events_written
files_written
start_timestamp_ns
end_timestamp_ns
status
failure
```

Partial output must never be exposed as a completed dataset.

**Done when:** One Python-generated trade processing job produces a fully published Parquet dataset and manifest.

This is the first complete vertical slice.

---

## Phase 6 — Trade Format Coverage

### S14 — Expand Trade Formats

**Objective:** Apply the proven execution pipeline to the remaining trade formats.

**Depends on:** S13.

Add formats incrementally based on actual generated jobs and archives.

Prioritize semantic coverage:

```text
spot
linear perpetual
inverse perpetual
linear future
inverse future
```

across the available exchanges.

Prefer parameterized processors when the format fits cleanly.

Use explicit format implementations when semantics become awkward to express generically.

Every supported format requires:

```text
real raw fixture
→ independently verified canonical values
```

The current processing corpus should be used to exercise actual combinations rather than creating artificial format permutations.

**Done when:** The representative trade jobs from the current processing corpus execute successfully.

---

## Phase 7 — L2 Processing

### S15 — Depth Decoding Infrastructure

**Objective:** Add physical decoding required by L2 formats.

**Depends on:** S14.

Implement reusable mechanics as required by actual formats:

```text
JSONL decoding
XLSX decoding
nested bid/ask parsing
depth level parsing
action mapping
sequence mapping
```

Dependencies should be added only when the first selected format requires them.

**Done when:** Raw depth records can be decoded into typed intermediate structures.

### S16 — Snapshot-Only L2 Vertical Slice

**Objective:** Prove canonical snapshots and nested Arrow/Parquet depth storage using the simplest available depth model.

**Depends on:** S15.

The inspected Bitget L2 format is suitable because each raw row represents an independent full book snapshot:

```text
timestamp
asks
bids
```

Pipeline:

```text
ZIP
    ↓
XLSX
    ↓
row
    ↓
parse asks / bids
    ↓
L2Snapshot
    ↓
canonical validation
    ↓
Arrow
    ↓
Parquet
```

No delta reconstruction or sequence continuity is required.

Sorting by timestamp is permitted because the source consists of independent snapshots and the raw workbook order is not chronological.

**Done when:** Real historical L2 snapshots survive complete raw → canonical → Arrow → Parquet → canonical round trip.

### S17 — Snapshot + Incremental L2 Formats

**Objective:** Support sources containing snapshots and incremental book updates.

**Depends on:**### S17 — Snapshot + Incremental L2 Formats

**Objective:** Support depth formats containing authoritative snapshots followed by incremental updates.

**Depends on:** S16.

Implement normalization into the existing canonical model:

```text
L2Snapshot
    ↓
authoritative complete book state

L2Update
└── changes[]
    ├── L2LevelUpdate
    ├── L2LevelUpdate
    └── ...
```

One native source update remains one atomic canonical event regardless of how many levels it modifies.

Support:

```text
bid / ask
set / delete
price
quantity
order_count when available

sequence_first
sequence_last
sequence_previous
cross_sequence
```

For absolute-level update formats:

```text
quantity = 0
    → delete level

quantity > 0
    → set resulting absolute level
```

Do not flatten one native update into independent canonical events.

Implement the major depth formats incrementally:

```text
Bybit
    → snapshot + delta events
    → sequence metadata
    → absolute-level updates

OKX
    → periodic snapshots + updates
    → JSONL
    → TAR.GZ
    → no universal sequence assumption
```

Format-specific semantics remain explicit.

**Tests:**

Verify:

```text
raw snapshot
    → one L2Snapshot

raw update containing N changes
    → one L2Update
        └── N changes
```

Also verify:

```text
bid set
ask set
bid delete
ask delete
order_count when present
sequence mapping when present
```

**Done when:** Bybit and OKX depth archives produce correct atomic canonical `L2Snapshot` and `L2Update` events.

**Not included yet:** Stateful book reconstruction and continuity enforcement.

---

### S18 — L2 Arrow and Parquet Round Trips

**Objective:** Complete the canonical storage representation for depth.

**Depends on:** S17.

Extend the Arrow conversion from S08 to fully support:

```text
L2Snapshot
├── sequence metadata
├── bids[]
└── asks[]

L2Update
├── sequence metadata
└── changes[]
```

Each snapshot level contains:

```text
price
quantity_base
quantity_quote
quantity_contracts
order_count
```

Each update change contains:

```text
side
action
price
quantity_base
quantity_quote
quantity_contracts
order_count
```

The nested structure must remain intact through both Arrow IPC temporary runs and final Parquet.

**Tests:**

```text
L2Snapshot
→ Arrow
→ L2Snapshot

L2Update
→ Arrow
→ L2Update

L2Snapshot
→ Parquet
→ L2Snapshot

L2Update
→ Parquet
→ L2Update
```

Explicitly verify that:

```text
L2Update.changes[]
```

remains one atomic event and is never split across rows.

**Done when:** All three canonical event variants survive Arrow and Parquet round trips without semantic loss.

---

## Phase 8 — BookStore and Combined Processing

### S19 — BookStore Core

**Objective:** Maintain current order-book state for every depth stream.

**Depends on:** S18.

Implement:

```text
BookStore
    ↓
BookKey
    ↓
BookState
```

Conceptually:

```text
BookKey
├── instrument_id
└── stream_id
```

Each book maintains:

```text
bids
asks
status
sequence state
```

Book lifecycle:

```text
Uninitialized
    ↓ authoritative snapshot
Valid
    ↓ continuity / validity failure
Invalid
    ↓ authoritative snapshot
Valid
```

Implement:

```text
snapshot replacement
atomic update application

set level
delete level

best_bid()
best_ask()
mid()

ordered bids
ordered asks
```

Initial storage should favor simple ordered structures.

Do not use floating-point prices as ordered keys.

Canonical events remain unchanged when passed through `BookStore`.

**Tests:**

```text
snapshot
    ↓
expected complete book

snapshot
    ↓
update
    ↓
expected modified book

snapshot
    ↓
update with multiple changes
    ↓
all changes visible atomically

new snapshot
    ↓
old state completely replaced
```

**Done when:** Canonical depth streams can be replayed deterministically into independently maintained books.

---

### S20 — Format-Specific Continuity Validation

**Objective:** Validate source-specific sequence and continuity semantics without inventing universal exchange rules.

**Depends on:** S19.

Separate:

```text
Canonical Validation
        +
Format Continuity Validation
```

Canonical validation asks:

> Is this a valid MarketForge event?

Format validation asks:

> Is this event consistent with the native source stream?

Possible reusable continuity models may include:

```text
PreviousSequence
ContiguousRange
Monotonic
None
```

but only where actual source semantics justify them.

For example:

```text
sequence gap
    ↓
record integrity issue
    ↓
BookState::Invalid
```

Recovery:

```text
BookState::Invalid
    ↓
authoritative snapshot
    ↓
BookState::Valid
```

Continuity state must persist across:

```text
record boundaries
batch boundaries
temporary-run boundaries
source files belonging to the same stream
```

Do not compare unrelated exchange sequences globally.

**Tests:**

```text
valid continuity
sequence gap
previous-sequence mismatch
cross-file continuity
recovery through snapshot
format with no sequence information
```

**Done when:** Supported sequence-bearing formats detect real continuity failures without applying incorrect generic sequence rules.

---

### S21 — Combined Trades + Depth Processing

**Objective:** Produce the primary replayable MarketForge representation: one chronological stream containing trades and depth.

**Depends on:** S19, S20.

Pipeline:

```text
Trade tasks ─────┐
                 │
Depth tasks ─────┼→ temporary runs
                 │
Other tasks ─────┘
                      ↓
               K-Way Merge
                      ↓
                 BookStore
                      ↓
                  Parquet
```

Example output:

```text
09:00:00.000100  L2Snapshot
09:00:00.000130  L2Update
09:00:00.000170  Trade
09:00:00.000190  L2Update
09:00:00.000240  Trade
```

The output must remain directly replayable:

```text
event
    ↓
update market state if depth
    ↓
consume trade / state
    ↓
next event
```

Trade events do not mutate book state.

Depth events update only their own:

```text
instrument_id + stream_id
```

book.

**Tests:**

Use a real process job containing both trade and L2 streams.

Verify:

```text
global chronological ordering
stream identity
trade preservation
L2 preservation
correct final BookState
equal-timestamp stream_rank behavior
```

**Done when:** One real combined process job produces a chronological trade + depth Parquet dataset that can be replayed while maintaining correct books.

---

## Phase 9 — Dataset-to-Dataset Merge

### S22 — Canonical Dataset Reader

**Objective:** Read completed MarketForge Parquet datasets back into bounded canonical event streams.

**Depends on:** S13, S18.

Dataset merging must not pass through raw exchange processors.

Implement:

```text
dataset input
    ↓
manifest.json
    ↓
select overlapping Parquet parts
    ↓
Parquet RecordBatch reader
    ↓
CanonicalEvent cursor
```

The reader must preserve:

```text
event_timestamp_ns
event type
exchange
instrument_id
symbol
stream_id
canonical payload
```

Input reading remains bounded.

The manifest should be used to avoid opening irrelevant Parquet parts when the merge job specifies a narrower time range.

**Tests:**

Read datasets containing:

```text
trade
L2
combined
```

and compare decoded logical events with those originally written.

**Done when:** A completed MarketForge dataset can be consumed as a bounded ordered `CanonicalEvent` stream.

---

### S23 — Dataset-to-Dataset Chronological Merge

**Objective:** Execute the merge jobs already generated by Python.

**Depends on:** S22, S11.

Reuse the same k-way merge engine used for temporary runs.

Do not implement a second chronological merge algorithm.

Pipeline:

```text
Dataset A ─┐
Dataset B ─┤
Dataset C ─┼→ CanonicalEvent cursors
Dataset D ─┘
                    ↓
              K-Way Merge
                    ↓
                BookStore
             if depth exists
                    ↓
                 Parquet
                    ↓
               manifest.json
```

Supported combinations include:

```text
trade + trade
L2 + L2
trade + L2
combined + trade
combined + L2

spot + perpetual
spot + future
perpetual + future

linear + inverse

same exchange
cross-exchange

same underlying
different underlying

same quote
different quote
```

No price conversion occurs.

No quantity conversion occurs during merge.

No economic compatibility restriction is applied by Rust.

Canonical source values remain unchanged.

**Tests:**

Use the representative merge jobs already generated by Python:

```text
trade + L2
cross-exchange full microstructure
spot + perpetual + future
linear + inverse
large mixed chronological merge
```

Verify the fundamental invariant:

```text
output event count
=
sum of selected input events within merge range
```

and:

```text
all output events globally ordered
all canonical values unchanged
all stream identities preserved
```

**Done when:** The current representative merge manifests produce valid merged Parquet datasets.

---

## Phase 10 — Parallelism and Resource Enforcement

### S24 — Parallel WorkTask Execution

**Objective:** Execute independent raw-processing tasks concurrently without changing semantics.

**Depends on:** S21.

Introduce a fixed worker pool.

Conceptually:

```text
Task 0 ─→ Worker
Task 1 ─→ Worker
Task 2 ─→ Worker
Task 3 ─→ Worker
```

Each worker still processes one `WorkTask` sequentially:

```text
source
→ decode
→ normalize
→ validate
→ bounded buffer
→ run
```

Parallelism is between tasks, not inside the semantics of one task.

Worker output remains:

```text
TaskResult
└── RunDescriptor[]
```

The global merger remains responsible for final ordering.

**Tests:**

Run the same job using:

```text
workers = 1
workers = 2
workers = N
```

Verify:

```text
same canonical events
same ordering
same BookStore result
same integrity result
```

Physical file bytes do not need to be identical.

**Done when:** Worker count affects performance but not canonical semantics.

---

### S25 — Memory Budget Enforcement

**Objective:** Make processing memory bounded by the resolved job configuration.

**Depends on:** S24.

Use:

```text
memory_budget_bytes
```

to derive bounded allocations for:

```text
worker event buffers
Arrow builders
merge readers
output batches
Parquet writer
BookStore headroom
```

Run flushing must be memory-driven rather than based on a fixed row count.

Test the same logical input under:

```text
larger memory
    → fewer runs

smaller memory
    → more runs
```

and require:

```text
Output_large
=
Output_small
```

semantically.

One unusually large atomic event must never be split merely to satisfy a buffer target.

If a single valid event cannot be processed safely within the configured limits, fail explicitly.

**Done when:** Processing memory remains bounded while run boundaries do not affect canonical output.

---

### S26 — Scratch Budget Enforcement

**Objective:** Prevent temporary processing from filling scratch storage uncontrollably.

**Depends on:** S25.

Track:

```text
temporary Arrow run bytes
other job scratch bytes
```

against:

```text
scratch_budget_bytes
```

On exhaustion:

```text
scratch budget exceeded
    ↓
cancel processing
    ↓
structured failure
    ↓
no final dataset publication
```

Scratch location may be on a different filesystem from final output.

Atomic publication must therefore not assume that a cross-filesystem directory rename is always available.

The publication strategy must explicitly handle this case.

**Tests:**

```text
sufficient scratch
    → success

tiny scratch budget
    → controlled failure

failure
    → no completed dataset
```

**Done when:** Scratch exhaustion causes a deterministic controlled failure rather than uncontrolled filesystem consumption.

---

### S27 — Merge Reader Resource Budget

**Objective:** Keep large multi-stream merges bounded.

**Depends on:** S25.

The merger receives a global reader-memory budget.

Conceptually:

```text
merge memory
    ↓
active streams
    ↓
bounded reader batches per stream
```

More active streams should reduce per-stream buffering rather than linearly increasing total memory.

For extremely large future merges, hierarchical merging may later be added:

```text
many streams
    ↓
intermediate merged runs
    ↓
smaller stream set
    ↓
final merge
```

Hierarchical merging is not required initially unless real workloads demonstrate the need.

**Done when:** Increasing stream count does not imply unbounded merge-reader memory.

---

## Phase 11 — Integrity and Hardening

### S28 — Integrity Accounting and Policy

**Objective:** Connect real processing failures and data-quality problems to the job's integrity policy.

**Depends on:** S21, S24.

Track conditions such as:

```text
parse failure
invalid canonical record
sequence gap
timestamp regression
missing snapshot
invalid book
transformation failure
```

Distinguish:

```text
fatal source failure
```

from:

```text
data integrity problem
```

Fatal source conditions always fail:

```text
missing required archive
unreadable archive
corrupt archive
missing required member
decompression failure
unsupported source structure
```

Policy-controlled integrity conditions resolve to:

```text
degraded
```

or:

```text
failed
```

according to the effective job policy.

MarketForge performs no generic duplicate detection or automatic deduplication.

Identical-looking events are not assumed to be duplicates.

**Done when:** Known integrity problems are recorded and cannot silently produce a dataset marked clean.

---

### S29 — Source Preflight and Corruption Handling

**Objective:** Detect cheap structural failures before expensive processing while still detecting corruption encountered during streaming.

**Depends on:** S28.

Preflight should check where practical:

```text
file exists
file readable
container recognizable
required archive member available
basic source structure valid
```

without fully decompressing large archives.

Full worker execution still detects:

```text
truncated GZIP
corrupt ZIP
corrupt TAR.GZ
invalid records
streaming decompression failures
```

**Tests:**

```text
missing archive
corrupt ZIP
truncated GZIP
missing member
invalid JSONL
invalid XLSX
```

Expected:

```text
non-zero execution result
structured failure
no published dataset
```

**Done when:** Required-source corruption cannot be mistaken for a degraded but complete dataset.

---

### S30 — Ordering and Boundary Hardening

**Objective:** Prove ordering correctness across all execution boundaries.

**Depends on:** S28.

Test explicitly:

```text
equal timestamps
stream_rank tie breaking

task boundaries
batch boundaries
run boundaries
file boundaries
Parquet row-group boundaries
Parquet part boundaries

overlapping sorted runs
multiple files belonging to one stream
timestamp regressions
source-ordered depth
timestamp-sortable independent snapshots
```

Authoritative source order must not be destroyed merely to make timestamps monotonic.

Timestamp sorting is permitted only where the format semantics explicitly allow it.

**Done when:** Ordering semantics are invariant across physical processing boundaries.

---

### S31 — End-to-End Processing Corpus

**Objective:** Run the representative Python-generated processing corpus through the complete Rust engine.

**Depends on:** S29, S30.

Use the current process jobs as integration coverage.

Verify representative cases across:

```text
Bybit
Binance
OKX
Bitget
Gate.io

spot
perpetual
future

linear
inverse

trade
L2
combined
```

The purpose is not merely:

```text
job exited 0
```

Verify:

```text
expected event counts
expected event samples
chronological ordering
canonical schema
quantity semantics
BookStore state
integrity status
manifest consistency
Parquet readability
scratch cleanup
```

**Done when:** The current process-job corpus exercises all supported raw-format families successfully or fails only for explicitly unsupported formats.

---

### S32 — End-to-End Merge Corpus

**Objective:** Validate the complete canonical dataset merge path.

**Depends on:** S31, S23.

Execute the representative merge jobs.

Verify:

```text
trade + L2
cross-exchange
spot + contracts
perpetual + future
linear + inverse
different underlying assets
different quote assets
large mixed stream count
```

For every merge:

```text
canonical values before
=
canonical values after
```

apart from their new physical placement in the merged dataset.

Verify:

```text
event count conservation
time-range clipping
stream identity preservation
global ordering
BookStore replay where depth exists
manifest lineage consistency
```

**Done when:** The representative merge corpus produces deterministic lossless chronological datasets.

---

## Phase 12 — Runtime Integration and Performance

### S33 — Python Rust Runner

**Objective:** Connect the already implemented Python planner to the finished Rust executor.

**Depends on:** S31, S32.

Python already creates:

```text
process job JSON
merge job JSON
dataset rows
dataset lineage
```

The remaining runtime integration is:

```text
Python
    ↓
mark dataset processing
    ↓
launch marketforge-process --job ...
    ↓
read ProcessingResult
    ↓
complete / failed
    ↓
update catalog
```

Rust stdout should contain machine-readable final result data.

Diagnostics and progress belong on stderr.

Python must not parse human-readable log text to determine success.

**Done when:**

```text
marketforge process ...
```

and:

```text
marketforge merge ...
```

can optionally execute Rust and update the dataset lifecycle correctly.

---

### S34 — Processing and Integrity Metrics

**Objective:** Add shared observability without coupling monitoring to processing semantics.

**Depends on:** S33.

Track processing metrics such as:

```text
records processed
records/sec
input bytes
output bytes
active workers
completed tasks
active streams
active books
temporary runs
scratch bytes
Parquet parts
```

Track integrity metrics such as:

```text
parse failures
invalid records
sequence gaps
timestamp regressions
missing snapshots
book errors
```

Processing threads must not block on rendering.

**Done when:** Processing state can be observed through shared metrics without changing canonical output.

---

### S35 — Progress Output and Optional TUI

**Objective:** Add human-facing monitoring only after the processing engine is correct.

**Depends on:** S34.

Possible### S35 — Progress Output and Optional TUI

**Objective:** Add human-facing monitoring only after the processing engine is correct.

**Depends on:** S34.

Support monitoring modes such as:

```text
none
simple
json
tui
```

The same underlying metrics state should feed every renderer:

```text
ProcessingMetrics
IntegrityMetrics
        │
        ├── simple progress
        ├── structured JSON progress
        └── optional terminal dashboard
```

The TUI remains optional and must not be required for:

```text
scripts
CI
Python execution
non-interactive processing
```

Structured JSON progress is particularly useful when Python launches Rust.

Monitoring must never affect:

```text
event ordering
canonical values
BookStore state
integrity decisions
worker scheduling semantics
```

**Done when:** Processing can expose useful progress without coupling rendering to execution.

---

### S36 — Benchmarks and Profiling

**Objective:** Optimize demonstrated bottlenecks only after correctness and execution invariance are established.

**Depends on:** S31, S32.

Benchmark major stages independently:

```text
decompression
CSV decoding
JSONL decoding
XLSX decoding

normalization
decimal parsing
quantity conversion

Arrow construction
Arrow IPC writing / reading

sorting
k-way merging

BookStore mutation

Parquet encoding
Parquet writing
```

Also benchmark representative complete jobs:

```text
trade-only
depth-only
combined trade + depth
cross-dataset merge
```

Measure:

```text
records/sec
input MB/sec
output MB/sec
CPU utilization
peak memory
scratch I/O
final Parquet size
```

Optimization should follow:

```text
correct implementation
    ↓
benchmark / profile
    ↓
identify bottleneck
    ↓
optimize isolated component
    ↓
execution-equivalence tests
```

Potential later optimizations include:

```text
direct streaming for already ordered inputs
adaptive run sizing
faster CSV paths
specialized depth structures
hierarchical merging
reduced allocations
alternative temporary-run encoding
```

None should alter canonical semantics.

**Done when:** Performance characteristics of representative workloads are measured and the primary bottlenecks are known.

---

## Implementation Step Index

The complete implementation order is:

```text
S01  Align Rust with current Python process jobs
S02  Model process and merge inputs explicitly
S03  Execution shell and workspace lifecycle
S04  Canonical validation

S05  First real trade processor
S06  Complete quantity normalization

S07  Canonical Arrow schema
S08  Canonical Arrow conversion
S09  Temporary Arrow runs
S10  SequentialTaskRunner

S11  Deterministic k-way run merge
S12  Parquet dataset writer
S13  Manifest and atomic publication

S14  Expand trade formats

S15  Depth decoding infrastructure
S16  Snapshot-only L2 vertical slice
S17  Snapshot + incremental L2 formats
S18  L2 Arrow and Parquet round trips

S19  BookStore core
S20  Format-specific continuity validation
S21  Combined trades + depth processing

S22  Canonical dataset reader
S23  Dataset-to-dataset chronological merge

S24  Parallel WorkTask execution
S25  Memory budget enforcement
S26  Scratch budget enforcement
S27  Merge reader resource budget

S28  Integrity accounting and policy
S29  Source preflight and corruption handling
S30  Ordering and boundary hardening
S31  End-to-end processing corpus
S32  End-to-end merge corpus

S33  Python Rust runner
S34  Processing and integrity metrics
S35  Progress output and optional TUI
S36  Benchmarks and profiling
```

## Critical Dependency Path

The shortest path to a real processed MarketForge dataset is:

```text
S01
 ↓
S02
 ↓
S03
 ↓
S04
 ↓
S05
 ↓
S06
 ↓
S07
 ↓
S08
 ↓
S09
 ↓
S10
 ↓
S11
 ↓
S12
 ↓
S13
```

This produces the first complete trade dataset.

Depth then extends that pipeline:

```text
S13
 ↓
S14
 ↓
S15
 ↓
S16
 ↓
S17
 ↓
S18
 ↓
S19
 ↓
S20
 ↓
S21
```

Dataset merging builds on the already stable canonical storage and merge infrastructure:

```text
S13 + S18
      ↓
     S22
      ↓
S11 → S23
```

Parallel execution comes only after sequential semantics are proven:

```text
S21
 ↓
S24
 ↓
S25
 ↓
S26
 ↓
S27
```

Full runtime integration follows correctness testing:

```text
S28
 ↓
S29
 ↓
S30
 ↓
S31
 ↓
S32
 ↓
S33
```

Monitoring and optimization remain last:

```text
S33
 ↓
S34
 ↓
S35
 ↓
S36
```

## Milestones

### M1 — Real Rust Job Contract

Reached at:

```text
S02
```

At this point Rust understands both real Python-generated operations:

```text
process
merge
```

No hand-written translation layer is required.

### M2 — First Real Canonical Trade

Reached at:

```text
S05
```

A real historical exchange archive produces independently verified canonical trade events.

### M3 — First Temporary Canonical Run

Reached at:

```text
S10
```

A real `WorkTask` produces bounded ordered Arrow IPC runs.

### M4 — First Valid Published Parquet Dataset

Reached at:

```text
S13
```

The complete path works:

```text
Python-generated ProcessingJob
    ↓
real raw archive
    ↓
Rust
    ↓
canonical events
    ↓
temporary runs
    ↓
chronological merge
    ↓
Parquet
    ↓
manifest
    ↓
published dataset
```

This is the first major production-shaped milestone.

### M5 — Trade Format Coverage

Reached at:

```text
S14
```

Representative:

```text
spot
linear perpetual
inverse perpetual
linear future
inverse future
```

trade jobs execute through the same engine.

### M6 — First Valid Depth Dataset

Reached at:

```text
S16
```

A real depth archive produces canonical L2 snapshots and valid nested Parquet.

### M7 — Full Incremental Depth

Reached at:

```text
S20
```

Snapshots, atomic updates, BookStore reconstruction, and format-specific continuity validation work together.

### M8 — First Combined Trade + Depth Dataset

Reached at:

```text
S21
```

MarketForge produces the primary downstream representation:

```text
Trade
L2Snapshot
L2Update
Trade
L2Update
...
```

in one chronological replayable dataset.

### M9 — First Canonical Dataset Merge

Reached at:

```text
S23
```

Existing MarketForge datasets can be merged losslessly without returning to raw exchange archives.

### M10 — Multi-Worker Equivalent Execution

Reached at:

```text
S24
```

The invariant is proven:

```text
workers = 1
    ==
workers = N
```

for:

```text
canonical events
ordering
BookStore state
integrity result
```

### M11 — Resource-Bounded Engine

Reached at:

```text
S27
```

CPU, RAM, scratch storage, and merge-reader resources are bounded by the execution configuration.

### M12 — Hardened Processing Engine

Reached at:

```text
S32
```

The representative process and merge corpora pass end-to-end correctness and failure testing.

### M13 — Full Python → Rust Runtime

Reached at:

```text
S33
```

The complete application path becomes:

```text
marketforge process / merge
        ↓
Python planning
        ↓
dataset registration
        ↓
job JSON
        ↓
marketforge-process
        ↓
ProcessingResult
        ↓
catalog status update
```

## Testing Strategy

Tests are implemented with the corresponding functionality rather than postponed until the end.

### Rust Unit Tests

Location:

```text
engine/src/**
```

Cover isolated components such as:

```text
timestamp conversion
decimal parsing
quantity conversion
side mapping
action mapping
canonical validation
ordering keys
BookStore operations
RunDescriptor behavior
```

### Rust Integration Tests

Location:

```text
engine/tests/
```

Suggested organization:

```text
engine/tests/
├── protocol.rs
├── formats/
├── worker.rs
├── runs.rs
├── merge.rs
├── parquet.rs
├── book.rs
├── resources.rs
├── failures.rs
└── end_to_end.rs
```

### Existing Raw Data

The existing MarketForge raw archives should be reused where practical.

Do not duplicate large archives merely to create a separate Rust fixture tree.

Small curated fixtures may be created when they provide:

```text
fast deterministic tests
precise expected values
failure injection
edge-case coverage
```

### Protocol Fixtures

The Python-generated jobs are now important integration fixtures:

```text
data/.jobs/process/
data/.jobs/merge/
```

They represent the actual control-plane contract Rust is expected to execute.

Ordinary Rust unit tests should still remain runnable without:

```text
PostgreSQL
network access
Python execution
exchange APIs
```

Small stable job fixtures may therefore be copied or generated specifically for isolated Rust tests once the protocol stabilizes.

### Required Execution Invariants

The roadmap must prove:

```text
real raw fixture
→ independently checked canonical values

Trade
→ Arrow
→ Parquet
→ Trade

L2Snapshot
→ Arrow
→ Parquet
→ L2Snapshot

L2Update
→ Arrow
→ Parquet
→ L2Update

atomic L2Update.changes[] preserved

source continuity across files

equal-timestamp ordering deterministic

overlapping runs merge correctly

workers=1
==
workers=N

small memory
==
large memory

different run boundaries
==
same canonical output

corrupt source
→ no published dataset

missing snapshot
→ invalid BookState according to policy

scratch exhaustion
→ controlled failure

manifest
↔
physical Parquet files consistent
```

Logical output is compared.

Tests should not require identical:

```text
elapsed time
worker scheduling
monitor samples
temporary filenames
compressed Parquet bytes
```

when those details do not affect dataset semantics.

## Decision Gates

Some details should be finalized immediately before the step that actually requires them.

### Before S07 — Arrow Physical Types

Finalize:

```text
Decimal Arrow representation
decimal precision / scale
sequence integer types
trade ID representation
nested list / struct nullability
event_type representation
```

This decision must be stable before Arrow IPC and Parquet become persistent formats.

### Before S16 — Internal Book Price Representation

Finalize the exact BookStore key representation.

Do not use:

```text
f64
```

as an ordered price key.

Candidates include:

```text
integer ticks
exact Decimal wrapper
```

The representation must support exact ordering and efficient lookup.

### Before S20 — Per-Format Continuity Rules

For every sequence-bearing L2 format, document the actual native rule before implementing its validator.

Do not infer a universal rule from one exchange.

### Before S26 — Cross-Filesystem Publication

Determine how publication behaves when:

```text
scratch_path
```

and:

```text
final dataset path
```

are on different filesystems.

A simple atomic directory rename cannot be assumed in that case.

### Before S33 — ProcessingResult Protocol Freeze

The Rust result contract should be considered stable before Python begins depending on exact result fields and error structures.

## Deferred Work

The following are intentionally outside the core implementation path.

### Economic Transformations

Do not block canonical processing on:

```text
common-numeraire conversion
basis calculation
spread calculation
relative-depth transformation
synthetic prices
liquidity aggregation
cross-market derived features
```

Canonical merge preserves native observations.

Derived economic relationships belong after the canonical event layer.

### Advanced Options Processing

Option metadata and canonical extensibility may remain.

Advanced option-specific functionality is postponed, including:

```text
volatility surfaces
strike normalization
expiry normalization
IV-space depth aggregation
cross-strike liquidity aggregation
```

### Live Processing

Do not implement:

```text
WebSocket feeds
network recovery
live subscriptions
async transport
```

during the historical processing implementation.

The shared canonical and BookStore layers should remain reusable by the future live engine.

### Advanced Execution Optimizations

Initially defer:

```text
direct raw → Parquet bypass
run caching
partial retries
resumable tasks
checkpointed merge
disk-backed BookStore
hierarchical merge
specialized custom order-book structures
adaptive compression
distributed processing
```

The first engine should favor:

```text
simple
bounded
deterministic
testable
```

execution.

## Final Architecture

The resulting Rust architecture should converge on:

```text
                         ProcessingJob
                              │
              ┌───────────────┴───────────────┐
              │                               │
           Process                           Merge
              │                               │
              ▼                               ▼
        Raw WorkTasks                 Canonical Datasets
              │                               │
              ▼                               ▼
       Parallel Workers                Parquet Readers
              │                               │
              ▼                               │
     Canonical Arrow Runs                     │
              │                               │
              └───────────────┬───────────────┘
                              │
                              ▼
                       K-Way Merger
                              │
                              ▼
                    Chronological Events
                              │
                              ▼
                         BookStore
                     if depth is present
                              │
                              ▼
                       Parquet Writer
                              │
                              ▼
                         Manifest
                              │
                              ▼
                     Atomic Publication
                              │
                              ▼
                     ProcessingResult
```

The central architectural property is:

> Raw processing and dataset merging are different input paths that converge on the same canonical chronological event-stream infrastructure.

## Immediate Next Step

The next implementation step is:

```text
S01 — Align Rust With Current Python Process Jobs
```

Do not implement new processing infrastructure before this contract check.

The immediate acceptance test is:

```text
all current process JSONs
        +
all current merge JSONs
        ↓
Rust deserialization
        ↓
operation-aware validation
        ↓
PASS
```

Once S01 and S02 are complete, the Python ↔ Rust execution contract reflects the system that now actually exists rather than the earlier speculative design.

From there, implementation proceeds vertically:

```text
real job
→ real archive
→ real canonical event
→ real Arrow run
→ real merge
→ real Parquet
→ real manifest
```

rather than building disconnected infrastructure first.