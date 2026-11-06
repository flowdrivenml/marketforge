# Task 10 — Stream-Oriented Trade Output and Shared Execution Foundation

**Project:** MarketForge  
**Component:** Rust Historical Normalization Engine  
**Priority:** High / Storage Architecture and Performance  
**Status:** Approved for implementation  
**Dependencies:** Tasks 01–09  
**Next task:** Stream-Oriented Depth Output and Reconstruction Metadata

## 1. Objective

Refactor the historical normalization engine so that **compatible source tasks belonging to the same concrete trade stream can write into shared, long-lived Parquet writers**.

Currently, normalization output is primarily organized around individual source tasks.

A task creates its own Parquet writer, processes its source, and finalizes its output when the task completes.

Consequently, multiple small source archives can produce many small Parquet files even when they belong to the same instrument and could have contributed to larger shared output files.

The new architecture must separate:

**Source-task ownership** — Which source archive is processed, how it is interpreted, and how its execution is reported.

**Stream-output ownership** — Which canonical stream receives the resulting records, how Parquet files are rotated, and when the writer is finalized.

The primary objective is:

**Multiple compatible trade archives within one normalization job should contribute to the same stream-owned Parquet output, with file rotation controlled by output-size targets rather than source-task completion.**

This task also establishes the shared execution foundation needed for the subsequent depth-output refactor.

### Critical Requirements

- Preserve the authoritative schema-version-3 normalization batch.
- Preserve production profile resolution.
- Preserve canonical trade schemas.
- Preserve task identities and provenance.
- Preserve job and dataset boundaries.
- Preserve source ordering guarantees.
- Preserve existing integrity behavior.
- Preserve bounded resource management.
- Preserve transactional dataset commits.
- Support single-job and multi-job execution.
- Update affected manifests, output discovery, and downstream consumers.
- Establish reusable execution infrastructure for future depth consolidation.

### Scope

**Implement trade-output consolidation now.**

Do not implement depth-output consolidation during this task.

Depth consolidation requires additional reconstruction metadata and event-index handling. That work belongs to Task 11.

Do not execute performance benchmarks.

Complete only Task 10.

---

## 2. Architectural Context

The previous tasks established:

- Shared historical runtime configuration.
- Production normalization batch loading.
- Unified TOML normalization profiles.
- Reproducible normalization tests.
- Integrity accounting and source-error correctness.
- Reliable CLI execution results.
- Dataset manifests and provenance.
- Benchmark execution infrastructure.
- Benchmark statistics and comparison tooling.

This task changes how normalization output is owned and persisted.

### Existing Execution Model

Conceptually:

```text
Normalization Job
        │
        ├── Task A
        │     ├── Read archive A
        │     ├── Normalize trades
        │     ├── Create writer A
        │     └── Finalize writer A
        │
        ├── Task B
        │     ├── Read archive B
        │     ├── Normalize trades
        │     ├── Create writer B
        │     └── Finalize writer B
        │
        └── Task C
              ├── Read archive C
              ├── Normalize trades
              ├── Create writer C
              └── Finalize writer C
```

Even if all three tasks contain compatible BTCUSDT trades, they can produce separate Parquet files.

### Target Execution Model

```text
Normalization Job
        │
        ▼
   Validated Tasks
        │
        ▼
  Stream Grouping
        │
        ├── Stream A: BTCUSDT trades
        │       │
        │       ├── Read archive A
        │       ├── Normalize task A
        │       │
        │       ├── Read archive B
        │       ├── Normalize task B
        │       │
        │       ├── Read archive C
        │       ├── Normalize task C
        │       │
        │       ▼
        │   Shared Trade Writer
        │       ├── part-000000.parquet
        │       ├── part-000001.parquet
        │       └── part-000002.parquet
        │
        └── Stream B: ETHUSDT trades
                │
                ▼
           Separate Writer
                ├── part-000000.parquet
                └── part-000001.parquet
```

Source-task boundaries remain meaningful for:

- Task metrics.
- Source identity.
- Profile provenance.
- Integrity diagnostics.
- Source-record accounting.
- Source-to-output relationships.
- Failure attribution.

However, completing a source task must no longer automatically finalize its stream's Parquet writer.

---

# 3. Inspect the Existing Implementation

Before modifying code, inspect the actual normalization execution and persistence infrastructure.

Relevant areas may include:

```text
engine/src/historical/normalize/
├── task.rs
├── session.rs
├── executor.rs
├── scheduler.rs
├── resources/
├── worker/
│   ├── trades.rs
│   ├── depth.rs
│   └── sink.rs
└── manifest/
```

Also inspect:

```text
engine/src/shared/persistence/parquet/
engine/src/shared/formats/
engine/src/shared/domain/
engine/src/shared/validation/

engine/src/historical/merge/
engine/src/historical/convert/

engine/tests/historical/
```

Use actual repository paths.

### Investigate Existing Writer Ownership

Determine:

- Where trade writers are constructed.
- Which object owns each writer.
- When the writer is finalized.
- How file rotation occurs.
- How row groups are flushed.
- How writer metrics are collected.
- How output directories are selected.
- How output files are discovered.
- How manifests are generated.
- How processing tasks are scheduled.
- How resource reservations are acquired and released.
- How source-task metrics are associated with writer output.

### Existing Abstractions

The previous review identified useful interfaces such as:

```rust
TradeSink
DepthSink
ParquetTradeWriter
ParquetDepthWriter
```

Inspect their current implementations.

The trade worker already accepts a generic sink in the reviewed implementation.

Prefer reusing this separation rather than rewriting trade normalization.

### Important

Do not assume the repository is unchanged from the previous code review.

Tasks 01–09 may have modified interfaces.

Adapt the implementation to the current architecture.

Preserve existing useful abstractions.

---

# 4. Fundamental Ownership Change

The central architectural change is:

**A source task does not own the lifetime of its output Parquet writer.**

Instead, a stream execution context owns the writer.

### Source Task Responsibilities

A task remains responsible for:

```text
Source identity.
Source archive/member.
Profile resolution.
Instrument interpretation.
Record decoding.
Trade normalization.
Task metrics.
Integrity observations.
Source-level diagnostics.
```

### Stream Execution Responsibilities

A stream execution context owns:

```text
Concrete stream identity.
Compatible source-task ordering.
Long-lived Parquet writer.
Output file rotation.
Stream-level output counters.
Source-to-output attribution.
Stream-level finalization.
Resource reservations.
```

### Proposed Structure

Conceptually:

```rust
struct StreamExecutionContext {
    // Concrete stream identity.
    // Ordered source-task references.
    // Long-lived writer.
    // Stream output accounting.
    // Source-to-output spans.
    // Resource ownership.
}
```

The exact Rust structure and naming are implementation decisions.

Do not introduce unnecessary generic types or traits.

The goal is a simple, explicit ownership model.

### Writer Lifecycle

The intended lifecycle is:

```text
Create stream context
        │
        ▼
Create trade writer
        │
        ▼
Process task A
        │
        ▼
Finalize task A metrics
        │
        ▼
Process task B
        │
        ▼
Finalize task B metrics
        │
        ▼
Process task C
        │
        ▼
Finalize task C metrics
        │
        ▼
Finalize shared writer
        │
        ▼
Validate stream output
        │
        ▼
Return stream execution result
```

Completing task A must not force writer finalization.

Completing task B must not force writer finalization.

The writer is finalized when its stream execution completes or when the existing error-handling rules require termination.

---

# 5. Deterministic Stream Identity

Tasks may share a writer only when their resulting canonical records belong to the same compatible concrete stream.

Do not group tasks using symbol alone.

### Required Identity Dimensions

The grouping identity must account for:

```text
Normalization job/dataset.
Exchange.
Instrument type.
Market category.
Concrete instrument identity.
Canonical content type.
Relevant source-feed semantics.
```

Use the existing typed domain identities where possible.

Do not create multiple competing definitions of instrument identity.

### Example: Compatible Tasks

```text
Job 100
    │
    ├── Binance BTCUSDT spot trades — October 1
    ├── Binance BTCUSDT spot trades — October 2
    └── Binance BTCUSDT spot trades — October 3
```

If their source ranges and normalization semantics are compatible, these tasks may share one output stream.

### Example: Incompatible Tasks

```text
Binance BTCUSDT spot trades
Bybit BTCUSDT spot trades
```

These must remain separate streams.

Likewise:

```text
Binance BTCUSDT spot trades
Binance BTCUSDT perpetual trades
```

must remain separate.

### Different Jobs

Two tasks belonging to different normalization jobs must not share a writer.

Even if the underlying concrete instrument is identical:

```text
Job A → BTCUSDT trades
Job B → BTCUSDT trades
```

they retain separate:

```text
Dataset identities.
Output destinations.
Failure boundaries.
Manifests.
Writers.
```

### Profile Compatibility

Different source profiles may produce the same canonical stream when their resolved semantics are compatible.

However, do not assume compatibility merely because two profiles output canonical trade records.

Preserve and validate:

- Instrument metadata.
- Quantity interpretation.
- Contract specifications.
- Source-feed meaning.
- Ordering semantics.
- Profile provenance.

If the same instrument's contract metadata changes between source periods, do not silently reuse the first task's metadata.

Require an explicit supported compatibility decision.

---

# 6. Stream IDs and Filesystem Identity

Preserve existing logical `stream_id` values.

Do not replace them with arbitrary output-directory identifiers.

### Required Distinction

Maintain separate concepts for:

```text
Logical stream identity.
Internal typed grouping identity.
Filesystem-safe stream directory key.
```

These may be related, but they are not necessarily interchangeable.

### Filesystem Safety

Source-provided stream identifiers must not be used directly as unsafe filesystem paths.

A stream ID must not be able to introduce:

```text
../
absolute paths
path traversal
directory collisions
```

Use a deterministic filesystem-safe representation.

Where necessary, use a stable encoded or hashed key.

Do not rely on a lossy string replacement that can silently map different stream identities to the same directory.

### Determinism

The same compatible stream identity should resolve to the same logical output grouping independently of:

- Worker completion order.
- Hash-map iteration order.
- Input directory iteration order.
- Machine-specific absolute source paths.

Keep the mapping reproducible.

---

# 7. Family-Scoped Source Tasks

Some prepared source tasks are family-scoped.

A family-scoped archive may contain records for several concrete instruments.

Do not assume:

```text
one source task = one concrete instrument
```

### Required Investigation

Inspect the family-scoped resolution infrastructure established by Task 02.

Determine how the production engine identifies the concrete instrument for each source record or archive member.

### Required Behavior

When a family-scoped source contains several instruments:

```text
Family Archive
      │
      ├── Instrument A
      ├── Instrument B
      └── Instrument C
```

records must be routed only to their correct compatible stream contexts.

Do not assign every record to the first instrument found.

Do not fabricate instrument metadata.

Do not combine instruments merely because they share base and quote assets.

### Source Reading

Where the existing reader supports it, prefer processing the source once and routing accepted records to the appropriate stream writer.

Avoid repeatedly opening and decompressing the same large archive merely to extract each instrument if a bounded single-pass route is practical.

However, do not implement a generalized asynchronous routing framework solely for this case.

Use the smallest reliable solution compatible with the existing production resolver and memory limits.

### Unsupported Cases

If an existing family-scoped source cannot be safely routed into concrete streams, report the exact unsupported condition.

Do not silently place it in an incorrect stream.

Do not silently discard its records.

Do not claim full workload compatibility for a case that remains unsupported.

Preserve existing supported family-scoped behavior.

---

# 8. Source Ordering Within Streams

Shared output writers require explicit source ordering.

The engine must not append source archives merely in the order they happen to finish.

### Incorrect Ordering Sources

Do not infer chronology from:

```text
Task IDs.
Job IDs.
Array position alone.
Download IDs.
Full source paths.
Filesystem directory order.
Worker completion order.
```

These values do not establish chronological source order.

### Required Approach

Inspect the production batch and profile metadata.

Use trustworthy information such as:

- Declared source periods.
- Validated archive conventions.
- Existing source-order metadata.
- Supported source inspection.
- Verified event-time boundaries.

Establish a deterministic processing plan for each stream.

### Ordering Plan

Conceptually:

```text
Stream BTCUSDT trades
    │
    ├── Source A: validated earlier range
    ├── Source B: validated next range
    └── Source C: validated final range
```

The engine must verify that the sources are compatible with sequential append under the supported ordering contract.

Do not treat a filename-derived date as proof that every record inside the archive belongs to that date.

### Task 05 Integration

Reuse the timestamp-ordering validation established by Task 05.

Do not implement a competing timestamp-regression detector.

Task 05 determines whether individual source processing respects the applicable ordering contract.

Task 10 must also ensure that **adjacent source ranges can be safely appended into one canonical stream**.

These are complementary checks.

---

# 9. Cross-Archive Ordering and Overlap

Persistent writers must not conceal ordering violations between archives.

### Sequential Append

Simple append is appropriate when:

1. Source tasks belong to the same compatible concrete stream.
2. Each source satisfies its applicable internal ordering contract.
3. The source ranges have a verified compatible ordering.
4. Their boundary relationship permits deterministic append.
5. No unsupported overlap or reconciliation is required.

### Example

```text
Archive A:
    100
    101
    102

Archive B:
    103
    104
    105
```

These can be appended directly under a nondecreasing timestamp contract.

### Equal Timestamps

Equal timestamps are not automatically invalid.

For example:

```text
Archive A:
    100
    101

Archive B:
    101
    102
```

This may be valid if the source ordering and tie-breaking contract permit it.

Preserve deterministic source order.

Do not assume `stream_rank` distinguishes events within the same stream.

If a supported sequence or source-event identity provides a stronger ordering rule, use it where applicable.

### Unsupported Overlap

Example:

```text
Archive A:
    100
    105

Archive B:
    103
    108
```

Do not blindly append these archives and claim globally chronological output.

Do not silently:

- Drop records.
- Deduplicate records.
- Rewrite timestamps.
- Truncate overlapping ranges.
- Sort entire archives together.
- Reconcile events using an invented policy.

If the existing merge subsystem provides an explicitly suitable reconciliation path, its contracts may be reused where appropriate.

Otherwise, fail the affected stream/job with an actionable error.

Include the relevant source identities and observed boundaries.

### Important

**This task must not replace the existing merge subsystem.**

Normalization consolidation is not a general cross-archive reconciliation algorithm.

The goal is to avoid unnecessary output fragmentation when sequential append is already semantically valid.

---

# 10. Execution and Scheduling

The existing normalization engine supports global task scheduling and resource-aware execution.

The new output ownership model must integrate with that infrastructure.

### Initial Concurrency Model

Prefer:

**Parallel execution across independent stream groups.**

**Sequential source processing within one stream group.**

Conceptually:

```text
Global Scheduler
      │
      ├── Stream A worker
      │      ├── Task A1
      │      ├── Task A2
      │      └── Task A3
      │
      ├── Stream B worker
      │      ├── Task B1
      │      └── Task B2
      │
      └── Stream C worker
             ├── Task C1
             └── Task C2
```

This is intentionally simpler than an asynchronous producer-consumer architecture.

### Required Behavior

- Preserve configured worker limits.
- Preserve bounded resource admission.
- Preserve independent job failure boundaries.
- Preserve task-specific metrics.
- Preserve task-level diagnostics.
- Preserve job-level aggregation.
- Preserve deterministic output ordering.

Do not process same-stream source tasks concurrently and then append their results in completion order.

Do not buffer an entire stream's canonical rows merely to reorder completed tasks later.

### Single-Job and Multi-Job Execution

Both execution entry points must use the same underlying stream-aware execution implementation.

Do not implement consolidation only for:

```text
execute_processing_job
```

while leaving:

```text
execute_processing_session
```

on the old task-owned writer path.

Use a shared execution foundation.

### Depth Compatibility

Depth may temporarily continue using its existing task-oriented execution path.

This is acceptable during Task 10.

However, the shared execution design must not make depth execution incorrect or prevent Task 11 from introducing stream-owned depth writers later.

Do not force depth through the new trade writer implementation.

---

# 11. Shared Execution Foundation

Introduce the minimum reusable infrastructure required for stream-oriented execution.

Conceptually:

```text
Validated Job
      │
      ▼
Stream Execution Planner
      │
      ▼
Stream Execution Units
      │
      ▼
Resource-Aware Scheduler
      │
      ▼
Source Task Processing
      │
      ▼
Stream-Owned Output
      │
      ▼
Stream Results
      │
      ▼
Job Finalization
```

### Shared Responsibilities

Reusable infrastructure may include:

- Stream grouping.
- Stream execution planning.
- Deterministic task ordering.
- Source identity tracking.
- Stream-level resource ownership.
- Stream-level execution status.
- Source-to-output attribution.
- Final output accounting.

### Trade-Specific Responsibilities

Keep trade-specific functionality separate:

- Trade normalization.
- Trade sink adaptation.
- Trade Parquet writer.
- Trade canonical schema.
- Trade-specific output verification.

### Future Depth Responsibilities

Task 11 will introduce:

- Depth stream-owned writers.
- Depth source-event identity mapping.
- Reconstruction segment rebasing.
- Source-boundary metadata.
- Zero-change event indexing.
- Depth replay compatibility.

Do not implement these depth-specific changes now.

### Avoid Overengineering

Do not create a large generic framework requiring every processing operation to implement many traits.

Use concrete types and small shared abstractions where possible.

The intended result is a clean execution ownership model, not an entirely new engine architecture.

---

# 12. Long-Lived Trade Writers

Refactor the trade-output path so a writer remains available across compatible source tasks.

### Existing Sink Infrastructure

Inspect the existing generic `TradeSink` interface.

Reuse it where appropriate.

A trade worker should be able to normalize a source task and write accepted canonical trades into a sink owned by the enclosing stream execution.

### Required Separation

Refactor task processing so that:

```text
Process source task
```

does not necessarily imply:

```text
Create and finalize Parquet writer
```

### Desired Execution

```text
Stream Writer
     │
     ├── Task A
     │     └── Canonical trade records
     │
     ├── Task B
     │     └── Canonical trade records
     │
     └── Task C
           └── Canonical trade records
     │
     ▼
Finalize Writer
```

The stream writer owns the complete physical output.

Each task remains responsible for its own source and normalization metrics.

### Required Writer Behavior

- Create the writer once per active stream execution.
- Keep it open across compatible source-task boundaries.
- Continue filling the current output part where allowed.
- Flush row groups according to configured resource targets.
- Rotate output parts according to configured file-size targets.
- Finalize the writer only at stream completion or required termination.
- Preserve canonical record order.
- Preserve output errors.
- Avoid empty trailing files.

### Critical Distinction

These are separate operations:

```text
Source task completed.
Row group flushed.
Parquet part rotated.
Stream writer finalized.
Dataset committed.
```

Do not conflate them.

A source task completing must not automatically trigger the other four.

---

# 13. Parquet Row Groups and File Rotation

Use the shared runtime configuration:

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

### Row-Group Target

The row-group target controls buffering and flushing behavior according to the supported Parquet writer contract.

### File Target

The file target controls when an output part should be rotated.

It is a target, not necessarily an exact maximum persisted-file size.

### Required Behavior

A task boundary must not force file rotation.

For example:

```text
Archive A produces 20 MiB
Archive B produces 30 MiB
Archive C produces 40 MiB

File target = 256 MiB
```

Where the writer's actual size measurement permits it, these sources should contribute to one output part rather than three separate task-owned files.

### File Rollover

When the supported file target is reached:

```text
part-000000.parquet
        │
        ▼
Close completed part
        │
        ▼
part-000001.parquet
```

Preserve deterministic part numbering.

Do not create an empty output part after the final record.

### Partial Final Parts

The final part may be smaller than the configured target.

Do not pad files or add meaningless records to reach the target.

### File-Size Measurement

Inspect the actual trade writer before changing its rollover logic.

Determine whether it uses:

- Estimated in-memory size.
- Encoded bytes.
- Persisted bytes.
- Another documented measurement.

Do not redesign the entire Parquet sizing system under this task.

Make only changes required for correct long-lived trade-writer behavior.

Leave broader Parquet sizing optimization for separately measured work.

---

# 14. Preserve Task-Level Metrics

Task metrics remain essential even when several tasks write into one shared output.

The previous implementation could compare task metrics with the lifetime metrics of a task-owned writer.

That assumption no longer holds.

### Example

```text
Shared Writer
    │
    ├── Task A writes 100 trades
    ├── Task B writes 200 trades
    └── Task C writes 300 trades
```

The writer reports:

```text
600 total canonical trade rows
```

But Task B must report:

```text
200 canonical trade rows
```

not:

```text
300 cumulative writer rows
```

### Required Correction

Use task-specific output deltas or explicit sink acknowledgments.

Conceptually:

```text
task_rows_written =
    writer_rows_after_task
  - writer_rows_before_task
```

This is illustrative.

Use the safest approach supported by the existing sink API.

### Required Metrics

Preserve:

```text
records_read
records_matched
records_processed
records_rejected
events_normalized
events_written
integrity observations
task completion/failure
timestamp boundaries
```

Do not change these counters merely to simplify writer ownership.

### Accounting Invariants

For applicable disjoint task contributions:

```text
stream_rows_written =
    sum(task_rows_written)
```

For a completed job:

```text
job_rows_written =
    sum(stream_rows_written)
```

The final persisted Parquet row count must agree with the corresponding aggregate.

Use checked arithmetic.

### File Counts

A shared Parquet file may contain records from several source tasks.

Therefore, physical file counts no longer belong exclusively to individual tasks.

Do not assign one shared file to every contributing task and then sum those counts as though they were distinct files.

Keep physical file counts at the appropriate stream/job/dataset level.

If task-level file attribution is needed, represent contributing output spans rather than duplicating file ownership.

---

# 15. Source-to-Output Provenance

This is a mandatory part of the refactor.

When multiple tasks contribute to one Parquet file, the engine must retain enough information to determine which source tasks contributed which canonical records.

### Example

```text
Stream BTCUSDT trades
    │
    ├── Task A → output rows 0–999
    ├── Task B → output rows 1000–2499
    └── Task C → output rows 2500–3999
```

These row ranges are illustrative.

### Required Information

Preserve:

```text
Job identity.
Dataset identity.
Concrete stream identity.
Task identity.
Source archive identity.
Source archive member where applicable.
Resolved profile identity.
Profile fingerprint.
Canonical output contribution.
Output ordering position.
```

### Source Spans

For sequentially processed tasks, source-to-output attribution can often be represented as contiguous row ranges.

Prefer compact span metadata over duplicating source provenance into every canonical row.

A span may conceptually contain:

```text
task_id
stream_id
start_row_offset
end_row_offset
```

Use a clearly documented offset convention, preferably half-open ranges:

```text
[start, end)
```

The exact persisted representation should follow the existing manifest and provenance architecture.

### Multi-File Contributions

One source task may contribute records to several Parquet parts.

One Parquet part may contain records from several source tasks.

The metadata model must support both cases.

Do not assume:

```text
one task = one file
```

or:

```text
one file = one task
```

### Family-Scoped Sources

Where one source task contributes to several concrete streams, preserve its contribution to each stream separately.

Do not lose concrete instrument attribution.

### Compact Metadata

Avoid creating enormous per-record provenance metadata unless the existing canonical contract requires it.

Prefer bounded stream/task spans and existing resolved-job provenance.

Do not introduce one additional small file per source task merely to recreate the original file-fragmentation problem.

---

# 16. Output Directory Structure

The previous layout was task-oriented.

For example:

```text
dataset/
├── manifest.json
├── processing-job.json
├── processing-job.sha256
└── tasks/
    ├── task-000001/
    │   └── trades/
    │       └── part-000000.parquet
    └── task-000002/
        └── trades/
            └── part-000000.parquet
```

The new trade output should be stream-oriented.

### Preferred Conceptual Layout

```text
dataset/
├── manifest.json
├── processing-job.json
├── processing-job.sha256
│
└── streams/
    ├── <stream-A-key>/
    │   └── trades/
    │       ├── part-000000.parquet
    │       └── part-000001.parquet
    │
    └── <stream-B-key>/
        └── trades/
            └── part-000000.parquet
```

This layout is a recommendation, not permission to ignore existing consumers.

### Required Investigation

Before finalizing the layout, inspect:

- Manifest discovery.
- Output validation.
- Existing dataset readers.
- Merge input discovery.
- Conversion input discovery.
- Any source-to-task path assumptions.
- Existing dataset-inspection utilities.

### Compatibility

Prefer the simplest stream-oriented layout that can be supported consistently by production consumers.

Do not keep two competing authoritative output layouts merely for convenience.

If a versioned manifest change is required, implement it explicitly.

Do not change canonical trade columns merely to represent the new directory structure.

---

# 17. Manifest and Output Discovery

The existing dataset manifest is the authoritative description of committed output.

Task 07 established its reliable external contract.

Task 10 must update it where required to describe shared stream-owned output.

### Required Changes

The manifest must correctly identify:

- Actual Parquet files.
- Concrete stream ownership.
- Canonical row counts.
- File sizes.
- Timestamp ranges.
- Source-task contributions.
- Source-to-output relationships.
- Relevant profile provenance.
- Integrity status.
- Processing metrics.

### File Inventory

The manifest must no longer assume that all canonical files live under:

```text
tasks/task-<id>/
```

Update discovery accordingly.

### Stream Inventory

If Task 07 already introduced a stream inventory, extend or reuse it.

Otherwise, introduce the smallest metadata extension needed to represent grouped trade output.

Do not duplicate the same information across several independent metadata formats unnecessarily.

### Manifest Versioning

If the persisted manifest contract changes incompatibly, increment its protocol version according to the established versioning rules.

Do not silently change the interpretation of existing fields while retaining an incompatible old version.

### Integrity Checks

Verify:

```text
Manifest file count = actual canonical file count.
Manifest row count = total persisted canonical rows.
Stream row totals = associated output rows.
Task contribution totals = applicable stream totals.
```

Preserve checked arithmetic.

Do not count one shared file multiple times.

---

# 18. Downstream Consumer Compatibility

This is an important acceptance requirement.

Changing the output layout may affect other historical components.

Inspect existing consumers, particularly:

```text
historical merge
historical conversion
dataset inspection
manifest readers
```

### Required Approach

Update relevant file discovery and metadata interpretation where necessary.

Prefer adapting consumers to the authoritative manifest rather than hardcoding filesystem directory assumptions.

Do not rewrite the algorithms of merge or conversion.

### Example

A consumer previously expecting:

```text
tasks/task-000001/trades/part-000000.parquet
```

may now need to read:

```text
streams/<stream-key>/trades/part-000000.parquet
```

The consumer should obtain the authoritative file path and stream identity from the manifest when supported.

### Compatibility Verification

Verify that:

- Stream-oriented trade output is discoverable.
- The manifest can be parsed.
- Input files can be opened.
- File identities remain correct.
- Source relationships remain available.
- No unsupported assumptions about one-file-per-task remain.

### Legacy Datasets

Do not silently break existing committed datasets.

Preserve appropriate reader compatibility for supported legacy manifest versions where practical.

Do not implement a large historical dataset migration system.

If backward compatibility requires an explicit version-aware reader, keep the implementation minimal and focused.

### Failure to Adapt Consumers

Do not declare Task 10 complete if an existing supported downstream consumer can no longer read the new trade output.

If a dependency cannot safely be resolved within this task, report it as a blocker rather than claiming full compatibility.

---

# 19. Resource Management

The stream-oriented execution model changes resource ownership.

Previously, a task-owned writer was usually released when its task completed.

A stream-owned writer remains alive across several tasks.

Therefore, task-level resource admission alone may no longer represent all live allocations.

### Required Investigation

Inspect:

```text
Existing memory budget.
Task resource estimator.
Worker admission.
Parquet writer buffering.
Arrow batch allocation.
Compression buffers.
Scratch accounting.
```

### Global Resource Budget

The configured memory budget must remain a shared execution limit.

Do not create an independent full memory budget for each stream writer.

### Resource Categories

Relevant live allocations may include:

```text
Source read buffers.
Decompression buffers.
Normalization state.
Canonical batch builders.
Arrow record batches.
Parquet writer buffers.
Compression workspace.
Stream metadata.
Source-to-output spans.
```

### Writer Lifetime

A resource reservation cannot automatically be released at task completion when the associated stream writer still owns buffered memory.

Resource ownership must match actual allocation lifetime.

### Existing Resource Estimator

The current estimator may be an admission heuristic rather than an exact RSS measurement.

Do not represent estimated memory as actual process memory.

Retain useful existing budgeting infrastructure.

Correct ownership and accounting where necessary.

### Bounded Execution

The number of active stream writers must remain constrained by available resources.

Do not allow unlimited writers merely because the total task count is large.

When the configured budget cannot safely support another active stream, use the existing scheduler's bounded admission behavior or fail with an actionable resource error.

### Progress Under Resource Pressure

Ensure that resource reservations cannot deadlock execution.

A stream that already owns a writer must be able to finish and release its resources.

Do not admit later work that prevents required earlier work from progressing.

### No Unbounded Queues

This task does not introduce asynchronous decode-ahead or producer-consumer queues.

Do not buffer an entire stream in memory.

Process source tasks sequentially within each stream.

### Resource Verification

Add focused tests for resource admission, writer lifetime, and resource release.

Do not use large real-data workloads to simulate memory pressure.

---

# 20. Failure and Transactional Semantics

Preserve existing dataset staging and commit behavior.

### Job-Level Staging

All output must remain within the job's staging directory until:

- Required source tasks complete.
- Stream writers finalize.
- Output accounting passes.
- Integrity evaluation passes.
- Manifests are generated.
- Required artifacts are validated.

Only then may the dataset be committed.

### Source Failure

If task B fails after task A contributed records to the shared writer:

```text
Task A succeeds
       │
       ▼
Task B fails
       │
       ▼
Stream execution fails
       │
       ▼
Job fails
       │
       ▼
No successful dataset commit
```

Do not publish task A's partial stream as a complete dataset.

### Writer Failure

If the writer fails during part rotation or finalization, the job must fail.

Do not report success based only on completed source decoding.

### Failure Diagnostics

Preserve:

```text
Failed task identity.
Affected stream identity.
Source archive identity.
Underlying error.
Available task metrics.
Available integrity observations.
Available partial output diagnostics.
```

### Independent Jobs

A failure in one job must not corrupt another independent job.

Preserve the existing multi-job failure boundaries.

### Existing Dataset Protection

Do not overwrite an existing committed dataset.

Preserve the existing staging-to-final rename semantics.

Do not silently replace atomic rename with recursive copying.

Do not claim additional crash-durability guarantees without implementing and verifying them.

---

# 21. Trade Output Determinism

Equivalent source workloads must produce equivalent canonical trade records under supported execution configurations.

### Required Behavior

For a fixed workload and profile configuration:

```text
workers = 1
```

and:

```text
workers = 4
```

must preserve equivalent canonical trade semantics.

Parallel execution may change the timing of independent stream groups.

It must not change the logical order of records within a stream.

### Deterministic Part Ordering

Within each stream, Parquet parts must have deterministic logical ordering.

Do not number parts based on global worker completion order.

### Metadata Ordering

Manifest file inventories and stream inventories should use deterministic ordering.

Do not rely on hash-map iteration order.

### Physical Parquet Equality

Do not require byte-identical Parquet output across legitimate writer configurations.

Compression and row-group boundaries can change physical bytes.

Compare:

- Canonical values.
- Record ordering.
- Stream identity.
- Source attribution.
- Integrity outcomes.
- Required metadata invariants.

---

# 22. Required Trade Consolidation Tests

Use the shared normalization test infrastructure established by Tasks 03 and 06.

Most tests should use small synthetic sources.

Artifact-producing integration tests must preserve their output under:

```text
data/tests/historical/normalize/
```

### Test A — Several Archives, One Stream

Create several small compatible trade sources.

Use a sufficiently generous Parquet file target.

Expected:

- All sources contribute to one logical stream.
- The same writer remains active across source boundaries.
- Output fragmentation is reduced where the writer target permits.
- Source-task metrics remain separate.
- Canonical values are correct.
- Source attribution is preserved.

### Test B — File Rotation

Create one logical stream large enough to exceed a deliberately small output target.

Expected:

- Multiple Parquet parts.
- Correct part ordering.
- No empty trailing part.
- Correct row counts.
- Correct file inventory.
- No lost or duplicated records.

### Test C — Different Instruments

Use two instruments within one job.

Expected:

- Separate stream writers.
- Separate output directories.
- Correct stream identities.
- No cross-instrument records.

### Test D — Same Symbol, Different Exchange

Use the same displayed symbol on different exchanges.

Expected:

- Separate streams.
- No identity collision.

### Test E — Different Market Categories

Use the same symbol across spot and perpetual markets.

Expected:

- Separate streams.
- Correct instrument metadata.

### Test F — Same Stream Across Different Jobs

Use compatible trade tasks belonging to two different jobs.

Expected:

- Separate dataset outputs.
- Separate writer lifetimes.
- Independent failure boundaries.

### Test G — Equal Timestamps

Use source events containing equal timestamps.

Expected:

- Deterministic ordering under the supported tie policy.
- No arbitrary reordering.
- No false timestamp-regression failure.

### Test H — Unsupported Overlap

Use overlapping source ranges requiring unsupported reconciliation.

Expected:

- Explicit failure or the existing verified supported reconciliation path.
- No silent deduplication.
- No false chronological-output claim.
- No committed-success dataset when reconciliation is unsupported.

### Test I — Family-Scoped Source

Where supported by the production resolver, use one family-scoped source containing several concrete instruments.

Expected:

- Correct instrument resolution.
- Correct stream routing.
- No fabricated metadata.
- No cross-stream contamination.
- Source identity remains traceable.

### Test J — Task Metrics

Use several tasks contributing different row counts to one writer.

Expected:

- Correct per-task row counts.
- Correct stream total.
- Correct job total.
- No cumulative-writer double counting.
- Correct physical file count.

### Test K — Failure After Partial Success

Process one valid source followed by a failing source.

Expected:

- Available partial diagnostics.
- Failed stream/job status.
- No successful dataset commit.
- Existing committed datasets unaffected.

### Test L — Resource Limits

Use deliberately constrained synthetic configurations.

Expected:

- Bounded active writers.
- Correct resource ownership.
- No leaked reservations.
- No deadlock.
- Actionable failure when limits cannot be satisfied.

### Test M — Worker Determinism

Run the same small workload with different supported worker counts.

Expected:

- Equivalent canonical values.
- Equivalent logical stream ordering.
- Equivalent task/source attribution.
- Equivalent applicable integrity outcomes.

### Test N — Downstream Discovery

Verify that an existing supported consumer can discover and read the new stream-oriented trade output.

Do not require a full merge or conversion workload merely to verify path discovery.

Use focused integration tests.

---

# 23. Canonical Correctness Verification

Do not validate consolidation solely through file counts.

A reduction in Parquet files is not sufficient evidence that the implementation is correct.

### Required Checks

Verify:

- Correct trade timestamps.
- Correct prices.
- Correct sides.
- Correct quantities.
- Correct instrument identities.
- Correct stream identities.
- Correct trade identifiers where available.
- No missing records.
- No duplicated records.
- Correct record ordering.
- Correct source attribution.

Use independently reviewed expected values where available.

### Old Versus New Execution

Comparing old and new output can help identify regressions.

However, both implementations may share the same upstream normalization defect.

Preserve independent semantic assertions.

### Manifest Verification

Verify the actual persisted Parquet data against the manifest.

Do not rely only on in-memory writer counters.

### Integrity Verification

Preserve integrity policy evaluation and diagnostics established by Tasks 04 and 05.

Do not weaken policies merely because shared writer ownership exposes a failure.

---

# 24. Persistent Test Artifacts

All artifact-producing normalization integration tests must retain results under:

```text
data/tests/historical/normalize/
```

Use existing shared test-output helpers.

Recommended grouping:

```text
data/tests/historical/normalize/
├── trades/
├── execution/
├── parquet/
└── resources/
```

Each execution must use a unique directory.

Preserve:

- Canonical Parquet parts.
- Dataset manifests.
- Resolved jobs.
- Source/profile provenance.
- Stream metadata.
- Source-to-output attribution.
- Task and job reports.
- Integrity diagnostics.
- Failure reports.
- Incomplete staging artifacts where available.

Do not automatically delete these outputs.

Do not write to production dataset destinations.

Do not modify source archives.

Pure unit tests may continue using disposable temporary resources.

---

# 25. Benchmarking Restriction

Tasks 08 and 09 established benchmark infrastructure.

Do not execute actual performance benchmarks during Task 10.

### Authorized Verification

You may run:

- Formatting checks.
- Compilation checks.
- Source-only unit tests.
- Small synthetic normalization tests.
- Focused persistence tests.
- Small deterministic resource tests.
- Relevant existing correctness tests.

### Prohibited

Do not execute:

- Full historical benchmark workloads.
- Worker-scaling benchmarks.
- Large archive-processing benchmarks.
- Month-long Gate.io archives for performance measurement.
- Disk I/O stress tests.
- Performance profiling sessions.

### Performance Claims

Do not claim that the implementation is faster without measurements.

A reduction in output-file count is a valid structural observation.

Actual throughput improvement must be measured separately.

If a small correctness test demonstrates fewer files, report that as output-layout verification, not a performance benchmark.

---

# 26. Deferred Decode-Ahead and Buffering

The user has substantial system RAM, but that does not justify unbounded buffering.

This task intentionally implements the simpler architecture:

```text
Parallel streams.
Sequential tasks within streams.
Persistent stream writers.
Bounded buffers.
```

Do not implement:

```text
Asynchronous source prefetch.
Background decode queues.
Speculative multi-archive decoding.
Full-stream in-memory sorting.
Unbounded canonical event buffers.
```

These may be evaluated later if benchmark evidence supports them.

### Future Resource Design

If decode-ahead is introduced later, memory reservations must remain associated with queued data until that data is released.

Do not preemptively build a complex producer-consumer framework.

Persistent writer ownership is the immediate optimization.

---

# 27. Verification Requirements

After implementation, run focused Rust verification.

### Formatting

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

### Compilation

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Also compile affected binaries and test targets using the actual Cargo feature configuration.

### Focused Tests

Run tests covering:

- Stream grouping.
- Stream identity.
- Task ordering.
- Trade writer lifetime.
- Parquet rollover.
- Source-to-output attribution.
- Task metrics.
- Manifest discovery.
- Downstream consumer compatibility.
- Job failure propagation.
- Resource admission.
- Worker determinism.
- Existing trade normalization correctness.

### Depth Regression Checks

Because this task changes shared execution infrastructure, run relevant source-only depth regression tests.

Verify that existing depth execution still works.

Do not implement depth consolidation.

### Real-Data Verification

If small, explicitly selected real-data tests are available, use only those necessary to validate the changed production path.

Do not execute the complete real-data workload automatically.

If required archives are unavailable, report the missing prerequisite.

Do not fabricate execution results.

### No Benchmarks

Do not execute the benchmark runner.

Do not generate or report performance comparisons.

---

# 28. Implementation Sequence

## Phase A — Audit

Inspect writer ownership, task execution, session scheduling, resource accounting, manifests, and downstream readers.

Identify existing assumptions that a task owns its output files.

## Phase B — Stream Identity

Implement deterministic concrete stream grouping.

Validate compatibility and source attribution.

## Phase C — Source Ordering

Establish deterministic source ordering within each compatible stream.

Integrate existing timestamp-ordering guarantees.

Reject unsupported overlap or reconciliation.

## Phase D — Execution Foundation

Introduce the minimal stream execution planning and ownership infrastructure.

Preserve job boundaries.

Support both single-job and multi-job entry points.

## Phase E — Trade Writer Ownership

Move trade writer construction and finalization from individual source tasks to stream execution contexts.

Preserve generic trade normalization and sink interfaces.

## Phase F — Metrics and Resource Accounting

Update task-specific output accounting.

Correct resource ownership for long-lived writers.

Preserve global resource limits.

## Phase G — Output Layout and Metadata

Implement stream-oriented trade output.

Update file discovery, manifests, source attribution, and provenance.

Version changed persisted contracts appropriately.

## Phase H — Downstream Compatibility

Update relevant merge, conversion, and dataset-inspection consumers to understand the new authoritative output.

Preserve existing supported legacy reading where appropriate.

## Phase I — Tests

Add focused synthetic consolidation, ordering, accounting, failure, and persistence tests.

Preserve generated integration-test artifacts.

## Phase J — Verification

Run formatting, compilation, and relevant correctness tests.

Do not run benchmarks.

Stop after completing Task 10.

---

# 29. Scope Restrictions

## Authorized

This task authorizes:

- Deterministic stream grouping.
- Compatible trade-task consolidation.
- Stream execution planning.
- Long-lived trade writers.
- Shared writer ownership.
- Sequential task execution within streams.
- Parallel stream execution.
- Source ordering validation.
- Cross-archive append compatibility checks.
- Trade output file rotation across task boundaries.
- Source-to-output attribution.
- Task and stream metrics corrections.
- Resource ownership corrections.
- Stream-oriented trade directory layout.
- Manifest and discovery updates.
- Necessary downstream reader compatibility.
- Focused correctness tests.
- Minimal documentation updates.

## Not Authorized

Do not implement:

- Stream-oriented depth output.
- Depth event-index rebasing.
- Depth reconstruction-segment consolidation.
- Cross-archive order-book continuation.
- New trade normalization algorithms.
- New quantity semantics.
- A new batch JSON protocol.
- A new profile format.
- A new runtime configuration architecture.
- New integrity-policy semantics.
- General cross-archive merging.
- Automatic deduplication.
- Asynchronous decode-ahead.
- Unbounded buffering.
- New benchmark infrastructure.
- Performance tuning based on unmeasured assumptions.
- PostgreSQL registry integration.
- Python Forge changes.
- Axum API changes.
- TUI redesign.

### Important Boundary

Task 11 will implement depth-output consolidation using this shared execution foundation.

Do not prematurely generalize trade-specific details into depth behavior.

Preserve existing depth semantics until Task 11.

---

# 30. Working-Tree Safety

Inspect the working tree before modifying files.

Preserve unrelated user changes.

Do not reset the repository.

Do not overwrite committed datasets.

Do not modify original source archives.

Do not change production resource values.

Do not remove existing depth metadata.

Do not create commits or branches unless requested.

Prefer minimal, reviewable changes.

If a required interface has already been refactored by previous tasks, build on the current implementation rather than introducing a competing abstraction.

---

# 31. Required Implementation Report

When Task 10 is complete, provide the following.

## Architecture

Describe:

- Final stream execution design.
- Stream grouping identity.
- Source ordering strategy.
- Writer ownership.
- Writer lifecycle.
- Scheduler integration.
- Single-job and multi-job behavior.

## Trade Output

Describe:

- Final output directory structure.
- Parquet rollover behavior.
- Part numbering.
- File-size target interpretation.
- Source-task boundary handling.

## Provenance

Describe:

- Source-to-output attribution.
- Task IDs.
- Stream IDs.
- Profile fingerprints.
- Source spans.
- Manifest changes.

## Metrics

Explain:

- Per-task output accounting.
- Stream-level accounting.
- Job-level accounting.
- Physical file-count accounting.
- Resource reservation lifetime.

## Compatibility

Describe:

- Manifest version changes.
- Existing consumers updated.
- Legacy dataset compatibility.
- Any unsupported source combinations.
- Any remaining assumptions requiring Task 11.

## Correctness

Report:

- Canonical trade equivalence.
- Ordering guarantees.
- Cross-archive overlap handling.
- Integrity preservation.
- Transactional commit behavior.
- Failure isolation.

## Verification

List:

- Formatting checks.
- Compilation checks.
- Tests executed.
- Tests passed.
- Tests failed.
- Tests blocked by missing prerequisites.

Do not claim tests passed unless they were executed successfully.

## Benchmark Restriction

Explicitly state:

**Real-data performance benchmarks were not executed, as instructed.**

## Deferred Work

Identify remaining depth-output requirements for Task 11.

Do not implement them now.

---

# 32. Acceptance Criteria

Task 10 is complete when:

- Compatible trade tasks within one job can share a long-lived Parquet writer.
- Writer lifetime is independent of source-task lifetime.
- Source-task completion does not force Parquet file closure.
- File rotation remains controlled by the configured output targets.
- No empty trailing Parquet parts are produced.
- Different concrete instruments remain separate.
- Different exchanges remain separate.
- Different market categories remain separate.
- Different jobs remain separate.
- Family-scoped sources are handled without fabricated instrument identities.
- Source-task ordering is deterministic and justified by the supported ordering contract.
- Source timestamp ordering is verified where required.
- Unsupported cross-archive overlap is rejected rather than silently repaired.
- Equal timestamps follow a deterministic supported tie policy.
- Single-job and multi-job entry points use the same stream-aware execution foundation.
- Independent stream groups can execute concurrently within configured resource limits.
- Source tasks retain individual metrics and diagnostics.
- Shared writer metrics are not incorrectly attributed to individual tasks.
- Physical Parquet files are counted exactly once.
- Stream and job row totals match persisted output.
- Source-to-output relationships remain reconstructable.
- Source tasks may span multiple output parts.
- Output parts may contain records from multiple tasks.
- The dataset manifest accurately represents stream-oriented trade output.
- Manifest changes are versioned where required.
- Existing supported downstream consumers can discover and read the new trade output.
- Existing committed datasets are not silently broken by the reader changes.
- Long-lived writer memory is accounted for appropriately.
- Resource reservations are released correctly.
- No unbounded buffering or deadlock is introduced.
- Failed sources or writers cannot produce committed-success datasets.
- Existing committed datasets are never overwritten.
- Canonical trade schemas remain unchanged.
- Canonical trade values remain correct.
- Integrity policies remain unchanged.
- Existing depth execution continues working.
- Depth output consolidation is not implemented prematurely.
- Persistent integration-test artifacts are retained.
- Focused correctness and compatibility tests pass.
- No performance benchmarks are executed.
- No unsupported performance claims are made.

---

## 33. Stop Condition

**Stop after Task 10 is complete.**

Do not automatically proceed to Task 11.

Do not implement stream-oriented depth output.

Do not introduce asynchronous decode-ahead.

Do not perform performance benchmarks.

The next assignment is:

**Task 11 — Stream-Oriented Depth Output and Reconstruction Metadata.**

Task 11 will extend the stream-owned output architecture to depth normalization while preserving:

- Independent source reconstruction segments.
- Snapshot and recovery boundaries.
- Source-event identities.
- Source-local ordinals.
- Stream-global output offsets.
- Zero-change events.
- Depth event indexes.
- Boundary metadata.
- Replay correctness.

These requirements must remain separate from the trade-output implementation.

---

**Final objective:** Replace task-owned trade Parquet output with deterministic, resource-bounded, stream-owned output so compatible archives can share larger Parquet files without losing source provenance, correctness, integrity guarantees, or dataset commit safety.