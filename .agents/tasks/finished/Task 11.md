# Task 11 — Stream-Oriented Depth Output and Reconstruction Metadata

**Project:** MarketForge  
**Component:** Rust Historical Normalization Engine  
**Priority:** Critical / Storage Architecture and Data Correctness  
**Status:** Approved for implementation  
**Dependencies:** Tasks 01–10  
**Next task:** Final Historical Normalization Integration Verification

## 1. Objective

Extend the stream-oriented normalization architecture established in Task 10 to support **shared, long-lived Parquet writers for compatible depth source tasks**.

Task 10 separated source-task ownership from output-writer ownership and implemented stream-oriented trade persistence.

This task must introduce the equivalent architecture for canonical L2 depth data while preserving all information required for accurate order-book reconstruction.

Depth consolidation is substantially more complex than trade consolidation because depth records are stateful.

The engine must preserve:

- Source-event identities.
- Source-event ordering.
- Snapshot initialization.
- Replacement snapshots.
- Absolute and relative updates where supported.
- Sequence continuity.
- Sequence gaps and recovery.
- Reconstruction segments.
- Source-specific reconstruction boundaries.
- Canonical row offsets.
- Source-event indexes.
- Accepted events producing zero canonical changes.
- Task-level metrics and integrity observations.
- Correct order-book replay.

**Primary objective: Multiple compatible depth archives should contribute to shared, size-targeted Parquet output without changing their market meaning or reconstruction semantics.**

### Fundamental Principle

**Physical output-file boundaries must not determine logical order-book boundaries.**

Likewise:

**Combining physical output files does not automatically authorize combining reconstruction state across source archives.**

This task changes storage ownership and metadata representation.

It must not silently change how source depth events are interpreted.

Complete only Task 11.

---

## 2. Architectural Context

Tasks 01–10 established:

- Shared historical runtime configuration.
- Production schema-version-3 batch loading.
- Unified normalization profiles.
- Reproducible test infrastructure.
- Persistent normalization test artifacts.
- Correct integrity observation accounting.
- Safe source-error handling.
- Timestamp-ordering enforcement.
- Stable CLI execution results.
- Dataset manifests and provenance.
- Benchmark infrastructure.
- Stream-oriented trade execution and persistence.

Task 11 extends the shared execution foundation to depth.

### Previous Depth Architecture

Conceptually:

```text
Normalization Job
        │
        ├── Depth Task A
        │      │
        │      ├── Read source A
        │      ├── Reconstruct book A
        │      ├── Write canonical depth
        │      ├── Write events.parquet
        │      ├── Write segments.json
        │      ├── Write boundary.json
        │      └── Finalize writer A
        │
        ├── Depth Task B
        │      │
        │      ├── Read source B
        │      ├── Reconstruct book B
        │      ├── Write canonical depth
        │      ├── Write events.parquet
        │      ├── Write segments.json
        │      ├── Write boundary.json
        │      └── Finalize writer B
        │
        └── ...
```

Each task owns its physical output files and reconstruction metadata.

This makes it straightforward to associate output with a source task, but it can produce excessive file fragmentation.

### Target Depth Architecture

```text
Normalization Job
        │
        ▼
   Validated Depth Tasks
        │
        ▼
     Stream Grouping
        │
        ├── Stream A: BTCUSDT depth
        │       │
        │       ├── Source task A1
        │       │      └── Independent reconstruction
        │       │
        │       ├── Source task A2
        │       │      └── Independent reconstruction
        │       │
        │       ├── Source task A3
        │       │      └── Independent reconstruction
        │       │
        │       ▼
        │   Stream-Owned Depth Writer
        │       │
        │       ├── part-000000.parquet
        │       ├── part-000001.parquet
        │       ├── part-000002.parquet
        │       │
        │       ├── Source-event index
        │       ├── Reconstruction segments
        │       └── Source-boundary metadata
        │
        └── Stream B: ETHUSDT depth
                │
                ▼
           Independent Stream Writer
```

Each source task remains independently identifiable.

Each source's reconstruction history must remain interpretable.

The shared writer owns physical persistence.

---

# 3. Inspect Existing Depth Infrastructure

Before modifying code, inspect the current implementation.

Relevant locations may include:

```text
engine/src/historical/normalize/
├── worker/
│   ├── depth.rs
│   └── sink.rs
├── task.rs
├── session.rs
├── executor.rs
├── resources/
└── manifest/

engine/src/shared/
├── formats/depth/
├── persistence/parquet/
├── domain/
└── validation/
```

Also inspect:

```text
engine/src/historical/merge/
engine/src/historical/convert/

engine/tests/historical/
```

Use actual repository paths.

### Required Inspection

Identify how the current implementation handles:

- `DepthProcessor` construction.
- Source-event decoding.
- Source-event ordinals.
- `DepthProcessingOutcome`.
- `DepthSink`.
- `ParquetDepthWriter`.
- `DepthBoundaryTracker`.
- `DepthBoundaryManifest`.
- `DepthSegmentManifest`.
- `IndexedDepthEvent`.
- `events.parquet`.
- `segments.json`.
- `boundary.json`.
- Parquet file rotation.
- Source-level task metrics.
- Writer metrics.
- Job finalization.
- Manifest discovery.
- Depth replay.

These names reflect previously reviewed code.

Use the current implementations and existing abstractions where possible.

### Review Task 10

Inspect the stream execution foundation introduced by Task 10.

Reuse:

- Stream grouping.
- Resource-aware scheduling.
- Source ordering.
- Stream writer ownership.
- Task-to-stream attribution.
- Job finalization.
- Output discovery.
- Manifest handling.

Do not introduce a second independent stream scheduler specifically for depth.

### Important

Before changing persistence, determine which assumptions about task-owned depth files are embedded in:

```text
Event indexing.
Segment offsets.
Boundary manifests.
Dataset discovery.
Replay consumers.
Merge.
Conversion.
```

The change is incomplete if the new files can be written but no longer interpreted correctly.

---

# 4. Fundamental Ownership Separation

Depth processing must distinguish three lifetimes.

## 4.1 Source Task Lifetime

A source task owns:

```text
Source archive/member.
Source profile.
Source-specific decoder.
Source-event ordinals.
Task-level metrics.
Source integrity observations.
Source-specific reconstruction context.
Source-specific boundary information.
```

## 4.2 Reconstruction Lifetime

A reconstruction context owns:

```text
Current order book.
Sequence state.
Synchronization state.
Initialization state.
Recovery state.
Source reconstruction segments.
```

This state does not automatically survive source-archive boundaries.

## 4.3 Output Writer Lifetime

A stream writer owns:

```text
Canonical depth Parquet output.
Output-part rotation.
Stream-global canonical row offsets.
Output event indexing.
Source-to-output attribution.
Stream-level persistence metadata.
Final writer accounting.
```

### Critical Requirement

**Writer lifetime and reconstruction lifetime must be independently controlled.**

A shared writer may remain open across three archives while each archive retains its own independently initialized reconstruction context.

Do not assume:

```text
Shared writer = shared order book
```

That assumption is incorrect.

---

# 5. Preserve Independent Source Reconstruction

For the first implementation, preserve the existing source-task reconstruction semantics.

### Example

```text
Archive A
    │
    ├── Snapshot A
    ├── Updates A
    └── Final book A

Archive B
    │
    ├── Snapshot B
    ├── Updates B
    └── Final book B
```

After consolidation:

```text
Shared Parquet output
    │
    ├── Archive A canonical events
    │
    └── Archive B canonical events
```

The physical records may share Parquet parts.

However, Archive B must still retain its own initialization boundary.

### Required Behavior

- Preserve source initialization requirements.
- Preserve replacement snapshots.
- Preserve source-local sequence semantics.
- Preserve synchronization invalidation.
- Preserve gap detection.
- Preserve recovery requirements.
- Preserve source boundary information.

Do not automatically initialize Archive B using Archive A's final order book.

### Delta-Only Archives

Suppose Archive B begins with incremental updates but no authoritative snapshot.

Do not silently accept it merely because Archive A ended with a valid book.

Cross-archive continuation requires an explicit verified policy.

Such a policy would need to establish:

```text
Compatible concrete instrument.
Compatible source feed.
Compatible profile semantics.
Verified source ordering.
Verified sequence continuity.
Valid reconstruction state transfer.
```

**Do not implement general cross-archive book-state continuation during Task 11.**

Preserve the existing independent-source behavior.

If a source cannot reconstruct correctly under its supported contract, retain its existing failure or degradation outcome.

---

# 6. Stream Grouping and Compatibility

Reuse the stream identity infrastructure established by Task 10.

Only compatible depth tasks may share physical output.

### Required Identity Dimensions

A grouping identity must account for:

```text
Job/dataset identity.
Exchange.
Instrument type.
Market category.
Concrete instrument identity.
Canonical content type.
Relevant feed semantics.
Compatible reconstruction interpretation.
```

Do not group using symbol alone.

Do not combine unrelated exchange feeds.

Do not combine spot and perpetual instruments.

Do not combine different jobs.

### Feed Compatibility

Some source formats distinguish:

```text
Snapshot feed.
Incremental feed.
Combined depth feed.
```

These distinctions may be represented by source options or profile configuration.

Do not remove a feed distinction merely to force two sources into the same stream.

### Required Rule

**Two depth tasks may share physical output only when their interpreted canonical events can coexist under an explicitly supported stream and reconstruction contract.**

If compatibility cannot be established, keep their outputs separate.

Do not silently discard source-specific semantics.

### Family-Scoped Archives

Reuse Task 10's concrete instrument-resolution infrastructure.

If one family-scoped archive contains several instruments, route each concrete instrument to the correct stream.

Do not fabricate instrument metadata.

Do not mix reconstruction state between instruments.

If bounded multi-stream routing is unsupported for a particular source, report the limitation explicitly rather than misrouting data.

---

# 7. Source Ordering

Depth output must preserve valid source-event ordering.

### Important Distinction

The following are not equivalent:

```text
Source record order.
Exchange sequence order.
Event timestamp order.
Canonical output order.
Physical Parquet file order.
```

Depth reconstruction may depend on source or exchange sequence order.

### Required Behavior

Reuse the ordering infrastructure established by Tasks 05 and 10.

Do not introduce another competing ordering implementation.

For each compatible stream group:

- Establish the supported source order.
- Validate applicable timestamp requirements.
- Preserve applicable source sequence requirements.
- Preserve deterministic tie behavior.
- Reject unsupported overlaps or continuity assumptions.

### Stateful Updates

Do not sort incremental depth updates solely by event timestamp.

Example:

```text
Source order:

Sequence 100 → timestamp 103
Sequence 101 → timestamp 102
```

Reordering these events could corrupt reconstruction.

Apply the declared source and timestamp policies independently.

Do not silently transform them into:

```text
Sequence 101
Sequence 100
```

### Sortable Sources

If a profile explicitly supports sorting independently sortable snapshots or other source records before stateful interpretation, preserve that existing supported behavior.

Do not classify valid raw disorder as a regression merely because sorting is part of the source contract.

Do not introduce new sorting semantics under this task.

---

# 8. Source-Event Identity

This is a critical part of the refactor.

The existing depth implementation may assign source-event ordinals independently within each task.

Consequently, two tasks may both contain:

```text
source_event_ordinal = 1
```

That is legitimate.

### Required Identity

Preserve source-local identity using the applicable components:

```text
job_id
task_id
archive member
source_record_ordinal
```

Use the existing source identity and event-index types where possible.

### Example

```text
Task 10:
    source event 1
    source event 2
    source event 3

Task 11:
    source event 1
    source event 2
    source event 3
```

These events must remain distinguishable after consolidation.

### Do Not Replace Source Identity

Do not replace source-local ordinals with a stream-global ordinal and discard their original values.

The global ordinal identifies output ordering.

The source-local ordinal identifies the original source event.

Both are useful and represent different concepts.

### Archive Members

A task may contain multiple archive members where supported.

Preserve member identity when necessary to avoid ambiguous source ordinals.

Do not assume that a task ID alone uniquely identifies every physical source record.

---

# 9. Stream-Global Output Identity

Introduce or reuse a deterministic stream-global output identity.

Conceptually:

```text
Stream output event ordinal.
Stream-global canonical row offset.
```

These must remain distinct.

### Example

Suppose:

```text
Task A:
    3 accepted source events
    100 canonical level rows

Task B:
    2 accepted source events
    50 canonical level rows
```

The combined stream contains:

```text
5 accepted source events
150 canonical level rows
```

But the two counts are not interchangeable.

### Source Event Index

Conceptually:

```text
Global event 0 → Task A, local event 1
Global event 1 → Task A, local event 2
Global event 2 → Task A, local event 3
Global event 3 → Task B, local event 1
Global event 4 → Task B, local event 2
```

### Canonical Row Offsets

Conceptually:

```text
Task A:
    canonical rows [0, 100)

Task B:
    canonical rows [100, 150)
```

Use half-open ranges where appropriate.

### Critical Distinction

**A source-event ordinal is not necessarily a canonical row offset.**

The existing code may use names such as `event_offset` for canonical level-row positions.

Inspect their actual semantics before changing them.

Do not rename or reinterpret counters blindly.

### Required Invariants

For each accepted source event:

- Preserve source identity.
- Preserve output event identity.
- Preserve applicable sequence metadata.
- Preserve its canonical row span.
- Preserve reconstruction-boundary classification.

No two accepted output events may collide in the stream-global event index.

Canonical row ranges must be consistent and nonoverlapping.

---

# 10. Accepted Events Producing Zero Canonical Rows

A valid depth event may produce no changes to the current order book.

For example:

```text
Current bid:
    100.0 → 5 BTC

Source update:
    100.0 → 5 BTC
```

The resulting canonical level updates may be empty.

However, the source event still occurred.

### Required Behavior

Preserve its accepted source-event identity.

Preserve applicable:

```text
Timestamp.
Sequence.
Source ordinal.
Source operation.
Boundary classification.
```

The event index must represent the event even if:

```text
canonical_row_count = 0
```

### Example

```text
Event 10:
    canonical rows [100, 105)

Event 11:
    canonical rows [105, 105)

Event 12:
    canonical rows [105, 108)
```

Event 11 is legitimate.

Its zero-length canonical row span must not cause the event to disappear.

### Important

Do not assume:

```text
accepted depth events = canonical level rows
```

Do not use the number of canonical rows to generate source-event ordinals.

Do not drop events because their row count is zero.

Preserve their role in sequence continuity and reconstruction metadata.

---

# 11. Source-Event Index Consolidation

The existing depth output includes:

```text
events.parquet
```

This file represents source-event indexing and related canonical output positions.

Inspect the actual schema and current reader assumptions.

### Required Changes

Adapt the event-index representation so that it supports multiple source tasks contributing to one shared physical depth stream.

The index must retain:

- Source-task identity.
- Archive-member identity where applicable.
- Source-local event ordinal.
- Stream-global output event order.
- Canonical row offsets.
- Canonical row counts.
- Event timestamps.
- Source sequence metadata.
- Source operation.
- Reconstruction-boundary information.

Reuse existing fields where possible.

Add only essential fields required for unambiguous consolidated output.

### Index Consistency

Verify:

```text
Indexed source events
    =
Accepted source events represented by the stream.
```

Verify:

```text
Final canonical row offset
    =
Total persisted canonical level rows.
```

Do not count source events as canonical levels.

### Part Boundaries

A source event may be associated with rows near a Parquet part boundary.

The event index must identify the correct logical row span independently of physical part numbering.

Use the established output file inventory and row ordering.

Do not assume every source event fits inside one row group unless the writer explicitly guarantees that property.

---

# 12. Reconstruction Segment Consolidation

The existing engine writes:

```text
segments.json
```

This describes reconstruction segments and their canonical output offsets.

The consolidated stream may contain multiple independently reconstructed sources.

### Required Behavior

Preserve every relevant reconstruction segment.

Do not collapse segments solely because they belong to the same concrete instrument.

### Example

```text
Archive A
    │
    ├── Initialization A
    ├── Updates A
    ├── Gap
    ├── Recovery snapshot A2
    └── Updates A2

Archive B
    │
    ├── Initialization B
    └── Updates B
```

The consolidated output must preserve the corresponding logical reconstruction boundaries.

### Offset Rebasing

If Task A produces:

```text
Local canonical rows [0, 100)
```

and Task B produces:

```text
Local canonical rows [0, 50)
```

the consolidated stream may use:

```text
Task A → global rows [0, 100)
Task B → global rows [100, 150)
```

Every applicable reconstruction segment belonging to Task B must be rebased consistently.

### Required Invariants

- Segment offsets refer to correct canonical rows.
- Segment ranges remain valid.
- No overlapping or missing canonical ranges are introduced.
- Source identities remain available.
- Initialization boundaries remain explicit.
- Gap and recovery boundaries remain explicit.
- Source-independent reconstruction remains possible.

### Important

Do not reinterpret one source's final segment as automatically continuous with the next source's first segment.

Physical adjacency does not establish order-book continuity.

---

# 13. Source Boundary Metadata

The existing engine writes:

```text
boundary.json
```

for individual depth tasks.

This metadata may contain information about:

- Initial book state.
- Final book state.
- Sequence continuity.
- Source boundaries.
- Reconstruction status.

A shared stream writer cannot simply write every task's boundary to the same file path.

### Required Design

Separate source completion from stream finalization.

Conceptually:

```text
Source task completes
        │
        ▼
Source Completion Result
        │
        ├── Task identity
        ├── Source identity
        ├── Source metrics
        ├── Initial boundary
        ├── Final boundary
        ├── Sequence information
        └── Reconstruction status
        │
        ▼
Stream Metadata Collector
        │
        ▼
Consolidated Reconstruction Metadata
```

### Source Boundary Requirements

Preserve enough information to reconstruct and inspect every contributing source independently.

Do not retain only:

```text
First source initial book.
Last source final book.
```

That would discard intermediate archive-boundary evidence.

### Stream-Level Continuity

Do not report verified continuity merely because the final source ended in a healthy state.

A stream-level continuity verdict must account for all relevant source segments and cross-source relationships.

If cross-source continuity was not verified, represent that distinction accurately.

### Storage Efficiency

Prefer a consolidated indexed metadata representation where practical.

Do not create a new collection of tiny JSON files for every source task merely to recreate the original fragmentation problem.

However, correctness and compatibility take priority over minimizing metadata-file count.

Do not remove required source-level evidence to save a small amount of storage.

---

# 14. Long-Lived Depth Writer

Refactor the depth persistence path so a compatible stream can own one writer across several source tasks.

### Existing Sink Interface

Inspect:

```rust
DepthSink
```

and:

```rust
DepthProcessingOutcome
```

The existing sink receives complete source-event outcomes rather than only isolated canonical level updates.

Preserve this useful abstraction.

### Desired Lifecycle

```text
Create depth stream context
          │
          ▼
Create stream writer
          │
          ▼
Process source task A
          │
          ├── Normalize events
          ├── Write outcomes
          ├── Preserve source boundaries
          └── Finalize task metrics
          │
          ▼
Process source task B
          │
          ├── Independent reconstruction
          ├── Write outcomes
          ├── Preserve source boundaries
          └── Finalize task metrics
          │
          ▼
Finalize stream writer
          │
          ├── Flush canonical Parquet
          ├── Flush source-event index
          ├── Finalize reconstruction segments
          ├── Persist boundary metadata
          └── Validate aggregate accounting
```

### Required Separation

Finishing a source task must not automatically finalize the shared Parquet writer.

However, source-task completion must still finalize its:

- Source metrics.
- Source-local reconstruction state.
- Source boundary report.
- Source-integrity reporting.

The stream writer remains available for the next compatible source.

---

# 15. Parquet File Rotation

Use the shared historical runtime configuration:

```text
data/.config/historical/general.toml
```

Relevant settings include:

```toml
[resources.parquet]
row_group_target_bytes = 67108864
file_target_bytes = 268435456
```

Preserve the actual configured values.

### Required Behavior

Parquet rotation must be independent of source-task boundaries.

For example:

```text
Archive A → 20 MiB canonical output
Archive B → 30 MiB canonical output
Archive C → 40 MiB canonical output
```

With a sufficiently generous file target, the archives should be able to contribute to one physical output part.

### Distinguish Events

These are different operations:

```text
Source task completed.
Source event completed.
Row group flushed.
Parquet part rotated.
Stream writer finalized.
Reconstruction segment completed.
Dataset committed.
```

Do not conflate them.

### Rollover Safety

A Parquet part rollover must not:

- Reset the order book.
- Reset source sequence state.
- Invent a snapshot.
- Invent a reconstruction boundary.
- Change source-event ordinals.
- Change source-event interpretation.
- Drop pending event-index entries.
- Reset canonical row offsets.

### Complete Source Events

Prefer rotating between complete `DepthProcessingOutcome` events.

Do not split a logical source event across physical output parts without an explicitly supported event-index and replay representation.

### Large Events

A single source event may exceed the configured file-size target.

Because file size is a target rather than necessarily a strict maximum, a controlled overrun may be acceptable.

However, resource budgets must still be respected.

If the event cannot be processed within supported memory limits, return an actionable resource error.

Do not allocate unbounded memory merely to keep an event physically contiguous.

### Final Part

Do not create empty trailing Parquet files.

The final part may legitimately be smaller than the configured target.

---

# 16. Stream-Oriented Depth Output Layout

Task 10 introduced stream-oriented output for trades.

Extend the same layout convention to depth.

### Preferred Layout

```text
dataset/
├── manifest.json
├── processing-job.json
├── processing-job.sha256
│
└── streams/
    ├── <depth-stream-A>/
    │   └── depth/
    │       ├── part-000000.parquet
    │       ├── part-000001.parquet
    │       ├── events.parquet
    │       ├── segments.json
    │       └── boundary.json
    │
    └── <depth-stream-B>/
        └── depth/
            ├── part-000000.parquet
            ├── events.parquet
            ├── segments.json
            └── boundary.json
```

This is conceptual.

The final representation must support multiple source boundaries within one stream.

If the existing `segments.json` or `boundary.json` schemas cannot represent that safely, extend their metadata contracts appropriately.

### Required Properties

- Deterministic stream directory identity.
- Deterministic Parquet part numbering.
- Valid relative manifest paths.
- No stream-directory collisions.
- Correct source-to-stream attribution.
- Correct reconstruction metadata.
- No task-directory assumptions in new output discovery.

Do not create a second unrelated output-layout convention.

Reuse Task 10's filesystem-safe stream identity mechanism.

---

# 17. Metadata Contract Evolution

The existing depth metadata formats were designed around task-owned output.

Consolidation may require incompatible changes.

### Required Approach

Inspect the existing serialized contracts.

Identify which fields can be preserved.

Identify which fields require extension.

Do not silently reinterpret a field's meaning.

### Versioning

If a serialized metadata contract changes incompatibly:

- Introduce an appropriate new version.
- Preserve exact field meanings.
- Update serializers.
- Update readers.
- Update validators.
- Update tests.
- Document compatibility.

Do not use the normalization batch's schema version as the version for depth reconstruction metadata.

Keep independent artifact versions where appropriate.

### Avoid Duplicate Sources of Truth

Do not maintain separate conflicting representations of:

```text
Canonical row offsets.
Source-event ordinals.
Source-task identities.
Reconstruction segments.
Source boundaries.
```

Choose the appropriate authoritative owner for each piece of metadata.

Reference it consistently.

---

# 18. Manifest Integration

The dataset manifest remains the authoritative description of the committed dataset.

Task 10 introduced stream-aware trade output.

Task 11 must extend that support to depth.

### Required Manifest Information

Preserve:

- Dataset identity.
- Job identity.
- Content type.
- Canonical row count.
- Canonical file count.
- Timestamp boundaries.
- Contributing source tasks.
- Stream identities.
- Canonical Parquet file inventory.
- Auxiliary reconstruction artifacts.
- Integrity evaluation.
- Processing metrics.

### New Output Discovery

The engine must discover depth output under the stream-oriented layout.

Do not assume all depth output lives under:

```text
tasks/task-<id>/depth/
```

### Auxiliary Files

Ensure the manifest identifies the required consolidated:

```text
Source-event index.
Reconstruction segment metadata.
Boundary metadata.
```

### Correctness Invariants

For a committed depth dataset:

```text
Manifest canonical rows
    =
Total persisted canonical level rows.
```

```text
Manifest canonical files
    =
Actual distinct canonical Parquet parts.
```

```text
Stream canonical rows
    =
Sum of applicable contributing source-task rows.
```

```text
Event-index final canonical offset
    =
Total indexed canonical rows.
```

Do not count the same shared file once for every contributing source task.

Do not confuse auxiliary `events.parquet` with canonical level-update Parquet parts.

---

# 19. Task, Stream, and Job Metrics

A shared depth writer accumulates output across several source tasks.

Its lifetime counters are not task-specific counters.

### Example

```text
Task A:
    10 source events
    100 canonical levels

Task B:
    20 source events
    250 canonical levels

Task C:
    15 source events
    150 canonical levels
```

The stream totals are:

```text
Accepted source events: 45
Canonical level rows: 500
```

Each task must preserve its own contribution.

### Required Distinctions

Maintain separate accounting for:

```text
Physical source records.
Matched source records.
Rejected source records.
Accepted depth source events.
Canonical level rows.
Physical Parquet files.
Reconstruction segments.
```

Do not combine these counters merely because they are all related to depth.

### Writer Metrics

Use task-specific deltas or explicit sink acknowledgments.

Do not compare each task's output count with the writer's lifetime cumulative count.

### File Counts

A physical output file may contain records from multiple tasks.

Count it once at the appropriate stream or dataset level.

### Integrity Metrics

Preserve the corrected integrity observation accounting from Task 04.

Do not duplicate observations when combining source tasks into a stream.

Preserve source-local integrity scopes and diagnostics.

### Aggregation

Use checked arithmetic.

Ensure final aggregate metrics agree with persisted output.

Worker completion order must not change the logical totals.

---

# 20. Resource Management

Depth reconstruction may consume more memory than trade normalization.

The shared writer also remains alive across source-task boundaries.

Task 10 introduced resource ownership for long-lived stream writers.

Extend it correctly to depth.

### Relevant Allocations

Account for applicable:

```text
Source read buffers.
Decompression buffers.
Source decoder state.
Order-book state.
Sequence state.
Canonical outcome buffers.
Arrow record batches.
Parquet writer buffers.
Compression workspace.
Source-event index buffers.
Reconstruction metadata.
Boundary book states.
```

### Global Budget

The configured memory allowance remains a global execution resource budget.

Do not allocate the full budget independently to every active stream.

### Writer Lifetime

Reservations associated with live stream writers must remain active until the corresponding resources are released.

Do not release writer memory reservations merely because one source task completed.

### Reconstruction State Lifetime

Independent source reconstruction state should be released when that source completes and its required metadata has been transferred or persisted safely.

Do not retain entire completed order books indefinitely unless their boundary information requires it.

### Metadata Growth

A stream may contain many source events and reconstruction segments.

Do not accumulate unbounded event-index or source-metadata buffers.

Use bounded flushing or streaming persistence where appropriate.

Preserve exact metadata semantics.

### Large Snapshots

A snapshot may contain many levels.

Ensure that processing remains subject to the configured resource budget.

Do not silently bypass memory limits for a large source event.

### No Decode-Ahead

Do not implement speculative source decoding or asynchronous prefetch queues.

The intended initial architecture remains:

```text
Parallel independent streams.
Sequential source processing within streams.
Long-lived stream writers.
Bounded memory.
```

---

# 21. Failure Handling and Dataset Commit

Preserve the existing job-level staging and commit behavior.

### Source Failure

Suppose:

```text
Task A succeeds.
Task B succeeds.
Task C fails.
```

All three tasks belong to one shared depth stream.

The affected job must fail.

Do not publish Task A and Task B's partial output as a complete dataset.

### Writer Failure

If the stream writer fails during:

```text
Canonical Parquet writing.
Part rotation.
Event-index writing.
Segment persistence.
Boundary persistence.
Finalization.
```

the affected job must fail.

### Metadata Failure

Missing or inconsistent reconstruction metadata must prevent successful dataset commit.

Do not publish a dataset that contains canonical depth rows but lacks the metadata required to interpret them under the declared contract.

### Existing Dataset Safety

Never overwrite an existing committed dataset.

Preserve staging isolation.

Preserve same-filesystem atomic rename behavior where established.

Do not silently replace rename with recursive copying.

### Failure Diagnostics

Preserve available:

- Failed task identity.
- Source identity.
- Stream identity.
- Source-event locator.
- Integrity diagnostics.
- Partial processing metrics.
- Writer failure information.
- Reconstruction metadata errors.

### Independent Jobs

A failed depth job must not corrupt unrelated jobs.

Preserve the existing multi-job failure boundaries.

---

# 22. Downstream Reader and Replay Compatibility

This is a mandatory part of Task 11.

Changing depth persistence is incomplete until the relevant readers understand the new representation.

Inspect:

```text
Depth replay.
Depth event-index readers.
Reconstruction segment readers.
Boundary metadata readers.
Historical merge.
Historical conversion.
Dataset inspection.
```

Use the actual repository structure.

### Required Reader Behavior

Readers must be able to:

- Discover stream-oriented depth output.
- Read canonical depth parts in correct order.
- Resolve source-event indexes.
- Interpret source-task identities.
- Interpret stream-global canonical offsets.
- Identify reconstruction segments.
- Identify source initialization boundaries.
- Preserve gap and recovery semantics.
- Reconstruct the correct book states.

### Legacy Compatibility

Preserve support for existing committed datasets where their manifest and metadata versions remain officially supported.

Do not silently break previously produced task-oriented depth datasets.

Use version-aware readers where necessary.

Do not implement a large historical dataset migration tool.

### Merge Compatibility

The existing merge component has its own continuity and reconciliation responsibilities.

Do not replace them.

Adapt its input discovery and metadata interpretation where required.

### Conversion Compatibility

Update conversion input discovery where necessary.

Do not rewrite conversion algorithms.

### Replay Safety

Do not infer continuous reconstruction state merely because records appear consecutively in one Parquet file.

The reader must respect explicit segment and source boundaries.

---

# 23. Reconstruction Correctness Verification

A valid depth dataset must be reconstructable.

The testing strategy must verify more than file counts.

### Required Verification

Use independently reviewed source expectations where available.

Verify representative:

```text
Initial book states.
Snapshot replacement states.
Incremental updates.
Level insertions.
Quantity changes.
Level deletions.
Sequence gaps.
Recovery snapshots.
Final states.
```

### Intermediate States

Do not verify only the final order book.

A final book may match even when intermediate events were processed incorrectly.

Verify selected intermediate states.

### Source Boundaries

At each relevant source boundary, verify that:

- The preceding source ends with its recorded state.
- The next source begins according to its supported initialization requirements.
- No unsupported state continuation occurs.
- Boundary metadata remains attributable to the correct source.

### Persistent Output Verification

Read the actual persisted Parquet data and reconstruction metadata.

Do not validate only in-memory writer state.

### Physical Versus Semantic Equality

Different Parquet part layouts may contain equivalent canonical depth events.

Do not require byte-identical Parquet output.

Compare:

- Canonical values.
- Source-event ordering.
- Event-to-row mappings.
- Reconstruction boundaries.
- Relevant book states.
- Integrity outcomes.
- Source provenance.

---

# 24. Required Depth Consolidation Tests

Use the shared testing infrastructure established by Tasks 03 and 06.

Prefer small deterministic synthetic sources.

Artifact-producing tests must preserve their output under:

```text
data/tests/historical/normalize/
```

## Test A — Multiple Compatible Archives

Prepare several compatible depth source tasks belonging to one concrete stream.

Expected:

- One logical output stream.
- Shared writer lifetime.
- No forced writer closure at source EOF.
- Correct canonical output.
- Correct per-source metrics.
- Preserved reconstruction boundaries.

## Test B — Independent Source Initialization

Use two archives that each begin with an authoritative snapshot.

Expected:

- Both initializations remain represented.
- The second source does not inherit the first source's book state.
- Both source histories can be reconstructed correctly.

## Test C — Delta-Only Second Archive

Use a second archive lacking required initialization.

Expected:

- Existing independent reconstruction rules remain enforced.
- No automatic borrowing of the previous archive's final state.
- No false continuity claim.

## Test D — Parquet Rollover

Use a sufficiently small file target to produce several output parts.

Expected:

- Correct physical part ordering.
- Correct canonical row offsets.
- Correct source-event indexes.
- No fabricated reconstruction boundaries.
- No empty trailing part.

## Test E — Source Event Near Rollover

Place an event near the configured rollover threshold.

Expected:

- Complete event semantics.
- Correct event-to-row mapping.
- No lost or duplicated canonical levels.
- Correct file inventory.

## Test F — Repeated Source-Local Ordinals

Use two source tasks whose event ordinals both start at one.

Expected:

- No identity collisions.
- Source-local ordinals preserved.
- Stream-global output order unambiguous.

## Test G — Zero-Change Event

Use an accepted depth update producing zero canonical level changes.

Expected:

- Source event remains indexed.
- Canonical row span may be empty.
- Sequence and timestamp metadata remain available.
- No artificial canonical level row is generated.

## Test H — Multiple Reconstruction Segments

Use a source containing:

```text
Initialization.
Updates.
Gap.
Recovery snapshot.
Further updates.
```

Expected:

- Correct reconstruction segments.
- Correct rebased offsets.
- Preserved gap and recovery information.
- Correct replay.

## Test I — Source Boundary Metadata

Use several independently reconstructed sources.

Expected:

- Every source retains initial/final boundary evidence.
- Source identities remain available.
- Aggregate metadata does not discard intermediate boundaries.

## Test J — Distinct Instruments

Use two instruments in one job.

Expected:

- Separate stream writers.
- Separate reconstruction state.
- No cross-instrument contamination.

## Test K — Distinct Market Categories

Use the same displayed symbol across different market categories.

Expected:

- Separate stream identities.
- Correct profile and instrument metadata.
- Independent book reconstruction.

## Test L — Independent Jobs

Use two jobs containing compatible depth instruments.

Expected:

- Separate datasets.
- Separate writer lifetimes.
- Separate failure boundaries.

## Test M — Snapshot and Incremental Feeds

Use supported source-feed distinctions.

Expected:

- Feed semantics preserved.
- No unsupported automatic reconciliation.
- Correct reconstruction boundaries.
- Explicit rejection of incompatible combinations.

## Test N — Writer Failure

Inject a controlled output failure after earlier source tasks have contributed data.

Expected:

- Failed job result.
- No committed-success dataset.
- Preserved diagnostics.
- No corruption of existing datasets.

## Test O — Resource Limits

Use constrained synthetic configurations.

Expected:

- Bounded writer memory.
- Bounded metadata buffering.
- Correct resource release.
- No deadlock.
- Actionable resource failure where necessary.

## Test P — Worker Determinism

Execute a small equivalent workload with different supported worker counts.

Expected:

- Equivalent canonical depth semantics.
- Equivalent source-event ordering.
- Equivalent reconstruction boundaries.
- Equivalent replayed states.
- Equivalent applicable integrity outcomes.

## Test Q — Legacy Reader Compatibility

Use small supported legacy depth-output fixtures.

Expected:

- Existing supported manifests remain readable.
- Existing source-event indexes remain interpretable.
- Existing reconstruction metadata remains valid.

## Test R — Downstream Consumer Discovery

Verify that existing supported merge, conversion, and replay consumers can discover and interpret stream-oriented depth output.

Use focused integration tests.

Do not run large unrelated workloads.

---

# 25. Independent Semantic Assertions

Do not use the normalizer's own output as the sole authority for expected book states.

For representative tests, establish independently reviewed expected values.

Examples include:

```text
Expected initialization levels.
Expected quantity at a selected price.
Expected deletion.
Expected state after an update.
Expected state after recovery.
```

Use exact decimal comparisons where appropriate.

Do not silently loosen assertions.

Do not regenerate expected values automatically from a failing implementation.

### Structural Tests Versus Semantic Tests

Keep the distinction explicit.

A structural test may verify:

```text
All event offsets are within range.
```

A semantic test may verify:

```text
The event at a known source locator removes the expected ask level.
```

Both are useful.

Neither replaces the other.

---

# 26. Persistent Test Artifacts

All artifact-producing normalization integration tests must retain output under:

```text
data/tests/historical/normalize/
```

Suggested organization:

```text
data/tests/historical/normalize/
├── depth/
│   ├── consolidation/
│   ├── event_index/
│   ├── segments/
│   ├── boundaries/
│   └── replay/
│
├── execution/
├── parquet/
└── resources/
```

Create only directories that are actually needed.

Every execution must receive a unique run directory.

### Preserve

- Canonical Parquet files.
- Source-event indexes.
- Reconstruction segments.
- Source boundaries.
- Dataset manifests.
- Resolved jobs.
- Source/profile provenance.
- Task and stream metrics.
- Integrity diagnostics.
- Failure reports.
- Incomplete staging artifacts where available.

Do not automatically delete these artifacts.

Do not write to production dataset paths.

Do not modify source archives.

Pure unit tests may use disposable temporary resources.

---

# 27. Benchmark Restriction

Tasks 08 and 09 established benchmarking infrastructure.

**Do not execute actual performance benchmarks during Task 11.**

### Authorized

Run:

- Formatting checks.
- Compilation checks.
- Small synthetic normalization tests.
- Focused depth replay tests.
- Event-index tests.
- Segment-offset tests.
- Resource-budget tests.
- Failure-injection tests.
- Relevant existing correctness tests.

### Prohibited

Do not run:

- Full historical benchmark workloads.
- Worker-scaling benchmarks.
- Large market-data archives for performance measurement.
- Month-long Gate.io archives for benchmarking.
- Disk I/O stress tests.
- Performance profiling sessions.

Do not claim the new architecture is faster without measurements.

Reduced output-file count is a structural result.

Throughput improvement remains an empirical question.

---

# 28. Deferred Optimizations

Do not introduce additional optimization work beyond stream-owned depth persistence.

Specifically, defer:

```text
Asynchronous decode-ahead.
Speculative archive prefetch.
Cross-archive order-book continuation.
General source-overlap reconciliation.
Automatic duplicate elimination.
Full-stream in-memory sorting.
Unbounded event buffering.
Automatic performance tuning.
```

These require separate correctness and performance justification.

The intended execution model remains:

```text
Parallel independent streams.
Sequential source processing within streams.
Independent source reconstruction.
Long-lived stream writers.
Bounded memory.
```

---

# 29. Implementation Sequence

## Phase A — Audit

Inspect current depth processing, source-event indexing, reconstruction segments, boundary metadata, writer ownership, and replay consumers.

Identify task-owned assumptions.

## Phase B — Source and Output Identity

Define how source-local ordinals, stream-global event ordinals, and canonical row offsets coexist.

Preserve source provenance.

## Phase C — Reconstruction Boundaries

Separate source completion from stream writer finalization.

Design the consolidated boundary and segment metadata representation.

## Phase D — Stream-Owned Writer

Extend Task 10's stream execution infrastructure to depth.

Keep the writer alive across compatible source tasks.

Preserve independent reconstruction state.

## Phase E — Event Indexing

Implement consolidated source-event indexing.

Preserve zero-change events.

Ensure row offsets are correct across sources and Parquet parts.

## Phase F — Segment Rebasing

Rebase reconstruction segments into the stream-global canonical row space.

Preserve source and recovery boundaries.

## Phase G — Parquet Persistence

Implement stream-oriented depth output and size-targeted part rotation.

Preserve complete source-event semantics.

## Phase H — Resource Accounting

Correct resource ownership for long-lived depth writers and metadata.

Preserve global budgets.

## Phase I — Manifest Integration

Update output discovery, manifests, auxiliary-file inventories, and source-to-output attribution.

Version incompatible metadata changes.

## Phase J — Downstream Compatibility

Update relevant replay, merge, conversion, and dataset readers.

Preserve supported legacy formats.

## Phase K — Verification

Run focused correctness, persistence, replay, failure, and resource tests.

Do not execute benchmarks.

Stop after Task 11.

---

# 30. Scope Restrictions

## Authorized

This task authorizes:

- Stream-oriented depth output.
- Long-lived depth writers.
- Reuse of Task 10's stream execution foundation.
- Independent source reconstruction.
- Source-completion metadata.
- Stream-global event identities.
- Source-local ordinal preservation.
- Canonical row-offset rebasing.
- Consolidated event indexing.
- Reconstruction-segment rebasing.
- Consolidated boundary metadata.
- Zero-change event preservation.
- Parquet rotation across source boundaries.
- Stream and task metric corrections.
- Resource ownership corrections.
- Manifest and auxiliary-file updates.
- Depth replay compatibility.
- Necessary downstream consumer compatibility.
- Focused correctness tests.
- Minimal documentation updates.

## Not Authorized

Do not implement:

- New depth normalization algorithms.
- New source profiles.
- New source-format support.
- Cross-archive book-state continuation.
- General cross-archive merging.
- Automatic deduplication.
- Timestamp rewriting.
- Unapproved source sorting.
- New integrity-policy semantics.
- New batch JSON protocols.
- A new runtime configuration framework.
- Asynchronous decode-ahead.
- Unbounded buffering.
- New benchmark infrastructure.
- PostgreSQL registration.
- Python Forge changes.
- Axum API changes.
- TUI redesign.

### Important Boundary

This task consolidates physical depth storage while preserving the established logical reconstruction semantics.

Do not interpret the new physical layout as authorization to change how market events are understood.

---

# 31. Working-Tree Safety

Inspect the working tree before editing.

Preserve unrelated user changes.

Do not reset the repository.

Do not overwrite committed datasets.

Do not modify original source archives.

Do not change production runtime resource values.

Do not delete existing reconstruction metadata without replacing its required semantics.

Do not create commits or branches unless requested.

Prefer small, reviewable changes.

Build on Task 10's current implementation rather than introducing competing infrastructure.

---

# 32. Required Implementation Report

When Task 11 is complete, provide the following.

## Architecture

Describe:

- Final depth stream execution model.
- Writer ownership.
- Reconstruction lifetime.
- Source ordering.
- Source completion.
- Stream finalization.

## Output Format

Describe:

- Final directory structure.
- Parquet part rotation.
- Source-event index layout.
- Reconstruction segment layout.
- Boundary metadata layout.

## Identity and Offsets

Explain:

- Source-local event identity.
- Stream-global event identity.
- Canonical row-offset semantics.
- Event-to-row mappings.
- Segment rebasing.
- Zero-change event handling.

## Reconstruction Correctness

Explain how the implementation preserves:

- Independent source initialization.
- Snapshots.
- Updates.
- Gaps.
- Recovery.
- Source boundaries.
- Intermediate replay states.
- Final book states.

## Provenance

Describe:

- Task identity.
- Archive/member identity.
- Stream identity.
- Profile fingerprints.
- Source-to-output attribution.

## Metrics

Explain:

- Source record accounting.
- Accepted source-event accounting.
- Canonical level-row accounting.
- Writer metrics.
- Physical file counts.
- Resource reservations.

## Compatibility

Describe:

- Manifest changes.
- Auxiliary metadata version changes.
- Replay reader changes.
- Merge/convert discovery changes.
- Legacy dataset compatibility.
- Unsupported source combinations.

## Failure Safety

Describe:

- Source failure propagation.
- Writer failure propagation.
- Metadata validation.
- Dataset commit behavior.
- Independent job failure boundaries.

## Verification

List:

- Formatting checks.
- Compilation checks.
- Tests executed.
- Tests passed.
- Tests failed.
- Tests blocked by prerequisites.

Do not claim a test passed unless it was executed successfully.

## Benchmark Restriction

Explicitly state:

**Real-data performance benchmarks were not executed, as instructed.**

## Remaining Work

Identify any issues requiring final integration verification under Task 12.

Do not implement unrelated future optimizations.

---

# 33. Acceptance Criteria

Task 11 is complete when:

- Compatible depth source tasks can share a long-lived Parquet writer.
- Writer lifetime is independent of source-task lifetime.
- Reconstruction lifetime is explicitly independent of writer lifetime.
- Completing a source does not automatically close the stream writer.
- Compatible archives can contribute to shared Parquet parts.
- Parquet rotation follows supported file-size targets.
- No empty trailing parts are produced.
- Different concrete instruments remain separate.
- Different exchanges and market categories remain separate.
- Different jobs remain independent.
- Source-feed compatibility is validated.
- Family-scoped sources do not lose concrete instrument identity.
- Source-event ordering remains valid.
- Stateful depth updates are not reordered to satisfy timestamps.
- Independent source initialization is preserved.
- Cross-archive book-state continuation is not introduced implicitly.
- Source-local event ordinals remain available.
- Repeated local ordinals cannot collide in consolidated metadata.
- Stream-global event ordering is unambiguous.
- Canonical row offsets are correct.
- Source-event indexes remain complete.
- Accepted zero-change events remain represented.
- Source events retain their correct canonical row spans.
- Reconstruction segment offsets are rebased correctly.
- All required initialization boundaries are preserved.
- Gap and recovery segments are preserved.
- Every source retains necessary boundary evidence.
- Stream-level continuity claims reflect actually verified continuity.
- Parquet file boundaries do not create logical reconstruction boundaries.
- Physical files are counted exactly once.
- Task-level output metrics remain accurate.
- Stream and job totals match persisted canonical output.
- Integrity observations are not duplicated.
- Resource reservations reflect actual writer lifetimes.
- Event-index and metadata buffering remain bounded.
- Failed sources or writers cannot produce committed-success datasets.
- Existing committed datasets are never overwritten.
- Manifests correctly describe stream-oriented depth output.
- Auxiliary-file discovery remains correct.
- Supported legacy depth datasets remain readable.
- Relevant replay, merge, and conversion consumers understand the new output.
- Independently expected intermediate book states are preserved.
- Canonical depth schemas remain unchanged.
- Existing integrity-policy semantics remain unchanged.
- Persistent test artifacts are retained.
- Focused correctness and replay tests pass.
- No actual performance benchmarks are executed.
- No unsupported performance claims are made.

---

## 34. Stop Condition

**Stop after Task 11 is complete.**

Do not automatically proceed to Task 12.

Do not implement asynchronous decode-ahead.

Do not introduce cross-archive reconstruction-state continuation.

Do not run performance benchmarks.

The next assignment is:

**Task 12 — Final Historical Normalization Integration Verification.**

Task 12 will verify the combined implementation across:

- Shared runtime configuration.
- Production batch loading.
- Unified profiles.
- Trade and depth correctness.
- Stream-oriented persistence.
- Source-event provenance.
- Reconstruction and replay.
- Manifests.
- CLI execution results.
- Failure handling.
- Resource accounting.
- Downstream consumers.
- Persistent testing infrastructure.
- Benchmark planning and reporting.

Task 12 is an integration-verification assignment, not another architectural redesign.

---

**Final objective:** Implement stream-oriented depth persistence that reduces unnecessary output fragmentation while preserving exact source-event provenance, reconstruction boundaries, canonical row mappings, replay correctness, resource safety, and transactional dataset guarantees.