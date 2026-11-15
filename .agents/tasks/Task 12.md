# Task 12 — Final Historical Normalization Integration Verification

**Project:** MarketForge  
**Component:** Rust Historical Normalization Engine  
**Priority:** Critical / Final Verification and Stabilization  
**Status:** Approved for implementation  
**Dependencies:** Tasks 01–11  
**Next task:** None — Historical Normalization Finalization

## 1. Objective

Perform the final integration review, verification, and stabilization of the MarketForge historical normalization engine following the implementation of Tasks 01–11.

The previous tasks introduced substantial changes to:

- Shared runtime configuration.
- Production normalization batch loading.
- Unified normalization profiles.
- Historical test infrastructure.
- Integrity observation accounting.
- Source-error classification.
- Timestamp-ordering enforcement.
- CLI execution results.
- Dataset manifests and provenance.
- Benchmark infrastructure.
- Stream-oriented trade persistence.
- Stream-oriented depth persistence.
- Depth reconstruction metadata.
- Resource ownership and scheduling.

Each task should already have been independently verified.

However, individual components passing their tests does not necessarily establish that the complete system works correctly.

**The purpose of Task 12 is to verify the integrated system, identify incompatibilities introduced by the preceding refactors, and correct genuine integration defects.**

This is a stabilization task.

It is not another architectural redesign.

### Primary Objectives

1. Verify that all eleven previous tasks are integrated correctly.
2. Verify the complete normalization execution path.
3. Verify the authoritative input and configuration contracts.
4. Verify trade and depth correctness.
5. Verify stream-oriented persistence.
6. Verify source provenance and reconstruction metadata.
7. Verify integrity and failure handling.
8. Verify dataset manifests and CLI results.
9. Verify resource accounting and execution safety.
10. Verify relevant downstream consumers.
11. Verify persistent testing infrastructure.
12. Verify benchmark infrastructure without executing benchmarks.
13. Remove obsolete code and dependencies where safe.
14. Resolve integration defects without introducing unnecessary changes.
15. Produce a final readiness assessment supported by actual verification results.

**The final deliverable is a coherent, verified historical normalization subsystem—not merely a collection of individually completed refactors.**

---

## 2. Architectural Scope

The intended final architecture is:

```text
                  Python Forge
                       │
                       ▼
             Prepared Batch V3
                       │
                       ▼
               Rust Batch Loader
                       │
                       ├── Batch validation
                       ├── Job/task validation
                       ├── Profile resolution
                       └── Instrument resolution
                       │
                       ▼
              Shared Runtime Config
                       │
                       ▼
               Execution Planner
                       │
                       ▼
              Stream-Aware Scheduler
                       │
           ┌───────────┴───────────┐
           │                       │
           ▼                       ▼
      Trade Streams            Depth Streams
           │                       │
           ├── Source tasks        ├── Source tasks
           ├── Normalization       ├── Reconstruction
           ├── Integrity           ├── Integrity
           ├── Shared writers      ├── Shared writers
           └── Provenance          └── Event indexes
           │                       │
           │                       └── Segments/boundaries
           │                       │
           └───────────┬───────────┘
                       │
                       ▼
                Job Finalization
                       │
                       ├── Writer finalization
                       ├── Output verification
                       ├── Integrity evaluation
                       ├── Manifest generation
                       └── Atomic dataset commit
                       │
                       ▼
                 Dataset Output
                       │
                       ├── Canonical Parquet
                       ├── Reconstruction metadata
                       ├── Dataset manifest
                       ├── Resolved job
                       └── Provenance
                       │
                       ▼
                 CLI JSON Result
                       │
                       ▼
                  Python Forge
                       │
                       ▼
                PostgreSQL Registry
```

The Python registry integration itself remains outside this task.

The Rust engine must expose the necessary filesystem artifacts and machine-readable results.

The final integration must preserve the separation between:

**Control plane:** Python Forge.

**Data processing:** Rust historical engine.

**Persistent registry:** PostgreSQL, managed by Forge.

Do not introduce PostgreSQL requirements into Rust normalization.

---

# 3. Review Previous Task Implementations

Before modifying any code, inspect the current repository and the implementation results of Tasks 01–11.

The tasks were:

| Task | Area |
|---|---|
| 01 | Shared historical runtime configuration |
| 02 | Production V3 batch loading and unified profiles |
| 03 | Shared normalization test infrastructure |
| 04 | Integrity observation accounting and time-window attribution |
| 05 | Source-error handling and timestamp-ordering enforcement |
| 06 | Exchange-profile tests and test-suite migration |
| 07 | CLI results, manifests, and Forge integration contract |
| 08 | Historical normalization benchmark infrastructure |
| 09 | Benchmark statistics and comparisons |
| 10 | Stream-oriented trade output |
| 11 | Stream-oriented depth output and reconstruction metadata |

### Required Review

Determine:

- Whether each task's functionality exists.
- Whether its intended interfaces are used by the integrated engine.
- Whether any temporary compatibility implementations remain.
- Whether duplicated execution paths remain.
- Whether obsolete configuration dependencies remain.
- Whether tests cover the final implementation rather than outdated intermediate behavior.
- Whether downstream consumers understand the final output contracts.
- Whether known defects remain unresolved.

Do not assume functionality exists merely because it was specified in a previous task.

Inspect the actual implementation.

### Incomplete Previous Work

If a previous task was not fully implemented, identify the missing requirement.

For small integration gaps, implement the necessary correction.

For substantial missing functionality requiring a separate architectural change, report the blocker rather than attempting an uncontrolled rewrite.

Do not falsely report Task 12 as complete when a critical dependency is missing.

---

# 4. Establish a Verification Baseline

Before making additional changes, establish the current state of the engine.

Inspect:

```text
engine/Cargo.toml

engine/src/shared/
engine/src/historical/
engine/src/bin/

engine/tests/historical/

data/.config/historical/
data/.config/historical/general.toml
```

Use the actual repository structure.

### Record the Baseline

Identify:

- Compilation status.
- Formatting status.
- Relevant lint status.
- Registered test targets.
- Source-only test results.
- Tests requiring external archives.
- Existing failing tests.
- Existing ignored or unregistered tests.
- Known unsupported functionality.

Distinguish failures that existed before Task 12 from regressions introduced during this task.

### Working-Tree Safety

Inspect the working tree before editing.

Preserve unrelated user changes.

Do not reset the repository.

Do not overwrite configuration files.

Do not modify original source archives.

Do not overwrite committed datasets.

Do not create or switch branches. After successfully completing and verifying this task, create one local Git commit containing only its changes, following the repository's AGENTS.md. Do not push.
---

# 5. Verify Shared Runtime Configuration

The authoritative runtime configuration is:

```text
data/.config/historical/general.toml
```

Verify that the engine uses the shared infrastructure established by Task 01.

### Required Checks

Verify:

- Schema-version validation.
- TOML deserialization.
- Resource validation.
- Parquet configuration validation.
- Integrity-policy validation.
- Relative path resolution.
- Absolute path handling.
- Explicit configuration overrides.
- Unknown-field rejection.
- Missing-field handling.

### Configuration Consistency

Verify that the same effective settings are used throughout the relevant execution path.

For example:

```text
general.toml
      │
      ▼
Validated Runtime Configuration
      │
      ├── Scheduler
      ├── Resource Budget
      ├── Trade Writers
      ├── Depth Writers
      └── Integrity Evaluation
```

Do not allow different components to silently load different runtime configurations.

### Legacy Configuration

Search for obsolete runtime dependencies such as:

```text
data/.jobs/processing.json
data/.config/historical/normalize.toml
```

Remove obsolete active dependencies where safe.

Do not delete historical documentation or compatibility code blindly.

Determine whether each remaining reference is intentional.

### Preserve Values

Do not tune:

```text
workers
memory budgets
scratch budgets
Parquet targets
integrity thresholds
```

This task is not performance tuning.

---

# 6. Verify Production Batch Loading

The authoritative external normalization input is:

```text
schema_version = 3
kind = normalization_batch
status = prepared
```

Verify the complete production loading path.

### Required Checks

- Valid batch parsing.
- Job validation.
- Task validation.
- Duplicate identity detection.
- Output-path validation.
- Ordering validation.
- Source-specific option validation.
- Family-scoped task support.
- Instrument metadata validation.
- Profile resolution.
- Internal job construction.

### Preserve Identity

Verify that:

```text
run_id
job_id
dataset_id
task_id
stream_id
stream_rank
```

retain their intended meanings.

Do not generate replacement identifiers unexpectedly.

Do not convert 64-bit IDs through floating-point values.

### Multiple Jobs

A batch may contain several normalization jobs.

Verify that:

- Each job retains its identity.
- Each job retains its output destinations.
- Jobs remain independently reportable.
- Jobs do not share output writers across dataset boundaries.
- Failure isolation remains correct.

### No Alternative Production Loader

Verify that the CLI, tests, and benchmark infrastructure use the same authoritative loading and resolution implementation.

Do not retain competing production loaders with different validation behavior.

---

# 7. Verify Unified Normalization Profiles

Normalization profiles use self-contained TOML files.

Their identity convention is:

```text
exchange.instrument_type.market_category.data_type
```

For example:

```text
bybit.perpetual.linear.depth
```

### Required Checks

Verify:

- All supported profiles parse.
- Profile IDs match filenames.
- Profile identities are unique.
- No ambiguous resolution exists.
- Profiles are self-contained.
- No obsolete physical-schema dependency remains in the authoritative path.
- Supported normalization rules compile.
- Unsupported profile definitions fail explicitly.
- Task/profile compatibility is enforced.

### Operational Settings

Verify that meaningful profile settings either:

1. Control the corresponding execution behavior.
2. Are validated against explicitly supported fixed behavior.
3. Cause a clear error when unsupported.

Do not allow TOML settings to appear configurable while the decoder silently ignores them.

### Profile Fingerprints

Verify:

- Deterministic fingerprints.
- Correct propagation into resolved jobs.
- Correct association with source tasks.
- Stable behavior under equivalent configuration.
- Appropriate changes when semantic configuration changes.

### Exchange Coverage

Use the supported profile catalog to identify actual coverage.

Do not assume every exchange supports every market category or source format.

---

# 8. Verify Trade Normalization Correctness

Trade processing must preserve the existing canonical trade schema.

Verify representative supported source formats and instrument categories.

### Required Canonical Fields

Where applicable, verify:

```text
event_timestamp_ns
exchange
instrument_id
symbol
stream_id
trade_id
sequence
side
price
quantity_base
quantity_quote
quantity_contracts
```

Verify optional fields where supported.

### Independent Expectations

Use the independently reviewed expectations established by Task 06.

Do not use the normalizer's output as the sole authority.

### Important Cases

Verify:

- Timestamp interpretation.
- Price precision.
- Side interpretation.
- Spot quantities.
- Linear contract quantities.
- Inverse contract quantities.
- Instrument identity.
- Source-task attribution.
- Record rejection.
- Canonical row counts.

### No Algorithm Redesign

Do not rewrite quantity conversion or trade normalization algorithms unless a reproducible correctness defect is discovered.

Preserve existing domain semantics.

---

# 9. Verify Depth Reconstruction Correctness

Depth requires stronger verification because its output represents stateful order-book changes.

Verify:

- Initialization snapshots.
- Replacement snapshots.
- Supported update semantics.
- Sequence validation.
- Sequence gaps.
- Synchronization invalidation.
- Recovery.
- Level insertion.
- Quantity changes.
- Level deletion.
- Zero-change events.
- Source-event identity.
- Reconstruction segments.
- Source boundaries.

### Independent Book-State Verification

Verify representative intermediate book states.

Do not rely only on final book equality.

For example:

```text
Initialization
      │
      ▼
Expected Book A
      │
      ▼
Update
      │
      ▼
Expected Book B
      │
      ▼
Deletion
      │
      ▼
Expected Book C
```

### Preserve Reconstruction Semantics

Do not change source interpretation merely because stream-oriented persistence is now used.

An output-file boundary must not create a reconstruction boundary.

A source boundary must not disappear merely because several archives share one physical Parquet file.

### Unsupported Continuation

Do not silently continue one archive's book state into another archive unless the existing explicitly supported contract permits it.

The initial consolidation architecture preserves independent source reconstruction.

---

# 10. Verify Stream-Oriented Trade Persistence

Task 10 introduced long-lived trade writers.

Verify the final implementation.

### Required Cases

**Several compatible archives, one stream**

They must contribute to shared output where ordering and resource conditions permit.

**Different instruments**

They must remain separate.

**Different market categories**

They must remain separate.

**Different exchanges**

They must remain separate.

**Different jobs**

They must retain independent output destinations and writer lifetimes.

**File rotation**

Output parts must rotate according to the supported configuration.

**Task boundaries**

A task completing must not automatically force writer closure.

### Metrics

Verify:

```text
task canonical rows
stream canonical rows
job canonical rows
dataset canonical rows
```

are consistent.

A shared physical Parquet file must not be counted once per contributing task.

### Source Attribution

Verify that source-task contributions remain reconstructable.

One file may contain several tasks.

One task may span several files.

The metadata must support both.

---

# 11. Verify Stream-Oriented Depth Persistence

Task 11 introduced long-lived depth writers and consolidated reconstruction metadata.

This is one of the highest-risk integration areas.

### Required Checks

Verify:

- Correct concrete stream grouping.
- Correct source-task ordering.
- Independent source reconstruction.
- Long-lived writer ownership.
- Correct Parquet rotation.
- Preserved initialization boundaries.
- Preserved gap/recovery boundaries.
- Correct source-local event ordinals.
- Correct stream-global event ordering.
- Correct canonical row offsets.
- Correct source-event indexes.
- Correct reconstruction segment rebasing.
- Correct source boundary metadata.
- Preserved zero-change events.
- Correct replay.

### Identity Example

Two independent source tasks may both contain:

```text
source_event_ordinal = 1
```

The consolidated output must preserve their distinct source identities.

Do not allow them to collide.

### Row Offset Example

```text
Task A:
    canonical rows [0, 100)

Task B:
    canonical rows [100, 250)
```

Verify that event indexes and reconstruction segments refer to the correct global offsets.

### Zero-Change Events

An accepted depth event can produce:

```text
0 canonical level rows
```

It must still appear in the required source-event index.

### Source Boundaries

Every contributing source must preserve the boundary evidence required by the reconstruction contract.

Do not retain only the first source's initial book and the final source's ending book.

### Physical Versus Logical Boundaries

Verify that Parquet part rotation does not:

- Reset the book.
- Reset source sequence state.
- Fabricate snapshots.
- Fabricate recovery events.
- Change event ordering.
- Change row offsets incorrectly.

---

# 12. Verify Integrity Guarantees

Tasks 04 and 05 corrected integrity observation accounting and error handling.

Verify their integration with the final stream-oriented execution architecture.

### Observation Accounting

Verify the distinction between:

```text
Physical source records.
Eligible integrity observations.
Integrity violations.
Accepted source events.
Canonical rows.
```

### Rates

Verify that rate denominators include the correct eligible population.

Do not calculate failure rates using only failed observations.

### Time Windows

Verify:

- Correct hourly attribution.
- Correct daily attribution.
- Correct handling of failures with known timestamps.
- Explicit handling of unattributable failures.

### Timestamp Regressions

Verify that applicable ordering contracts generate real observations.

Do not require arbitrary monotonicity from source formats that legitimately permit other ordering.

### Depth Integrity

Verify that rejected state-changing depth events trigger the required synchronization behavior.

Do not allow invalid reconstruction to continue silently.

### Aggregation

Verify that combining tasks into streams does not duplicate integrity observations.

For equivalent source workloads:

```text
workers = 1
```

and:

```text
workers = 4
```

must preserve equivalent applicable integrity totals.

### Preserve Policies

Do not change `general.toml` thresholds.

Do not reinterpret:

```text
fail
degrade
```

actions.

This task verifies the existing policy contract.

---

# 13. Verify Source-Error Handling

Fatal input failures must remain distinguishable from recoverable malformed records.

### Required Cases

Verify:

- Recoverable malformed JSONL records.
- Fatal JSONL read errors.
- CSV decoding errors.
- Corrupted compressed sources.
- Missing archive members.
- Fatal source I/O errors.
- Unsupported decoder recovery.
- Proper task failure propagation.

### No Infinite Retry

A persistent source-read error must not be retried indefinitely as though it were a new malformed record.

Use bounded deterministic tests.

### Depth Recovery

A recoverable decoding error does not automatically imply that reconstructed book state remains valid.

Verify the appropriate synchronization invalidation and authoritative recovery behavior.

### Commit Safety

Fatal source errors must prevent successful dataset commit.

---

# 14. Verify Resource Accounting and Scheduling

Tasks 10 and 11 changed writer ownership.

Consequently, resource lifetimes may extend across multiple source tasks.

### Required Checks

Verify:

- Global worker limits.
- Resource admission.
- Memory-budget accounting.
- Scratch-budget accounting.
- Long-lived writer reservations.
- Resource release.
- Bounded active streams.
- Failure cancellation.
- No leaked reservations.
- No execution deadlocks.

### Writer Lifetime

A stream writer remains alive across compatible tasks.

Resource reservations associated with its buffers must remain valid until those resources are released.

Do not release reservations merely because one contributing source task completed.

### Bounded Memory

Do not assume configured memory budgets are exact process-RSS limits.

Distinguish scheduler-accounted memory from actual process memory.

Verify logical budget enforcement.

Actual peak RSS under large workloads remains a benchmarking concern.

### Scheduling

Verify:

- Parallel independent streams.
- Sequential task processing within a stream.
- Deterministic stream-local ordering.
- Independent job failure boundaries.
- Equivalent output under supported worker-count variations.

### No Additional Concurrency

Do not implement asynchronous decode-ahead.

Do not introduce unbounded queues.

Do not redesign the scheduler.

---

# 15. Verify Dataset Manifests

The dataset manifest is the authoritative description of committed normalized output.

Verify it against actual persisted artifacts.

### Required Metadata

Preserve:

```text
protocol_version
job_id
dataset_id
content_type
events_written
files_written
start_timestamp_ns
end_timestamp_ns
source_tasks
files
auxiliary_files
integrity_status
integrity_evaluation
processing_metrics
```

Include any stream-oriented metadata introduced by Tasks 10 and 11.

### Required Invariants

For committed datasets:

```text
Manifest canonical row count
    =
Total persisted canonical rows
```

```text
Manifest canonical file count
    =
Actual distinct canonical Parquet files
```

```text
Stream row totals
    =
Applicable source-task contributions
```

### File Inventory

Verify:

- Every referenced file exists.
- Every path is valid and relative to the dataset root.
- No path escapes the dataset root.
- Recorded file sizes match actual files.
- Recorded row counts are correct.
- Timestamp boundaries are consistent.
- File ordering is deterministic.

### Auxiliary Metadata

For depth datasets, verify required:

```text
events.parquet
segments.json
boundary.json
```

or their versioned consolidated equivalents.

Do not count auxiliary Parquet files as canonical level-update parts.

### Manifest Versions

Verify that readers handle the currently supported manifest versions.

Do not silently change field meanings.

Do not introduce unnecessary manifest versions solely for internal refactoring.

---

# 16. Verify Resolved-Job Provenance

The engine persists:

```text
processing-job.json
processing-job.sha256
```

Verify:

- The resolved job exists.
- The checksum exists.
- The checksum matches the persisted document.
- Job and dataset identities match.
- Source tasks remain identifiable.
- Resolved profiles are represented.
- Profile fingerprints are preserved.
- Effective runtime configuration is represented.
- Relevant source-specific options are retained.

### Reproducibility

A committed dataset must remain attributable to the interpretation that produced it.

Do not replace resolved provenance with references to potentially modified current TOML files.

### Scope

Do not build an additional provenance registry.

Python Forge will register the committed dataset and its relationships.

Rust remains responsible for producing accurate provenance artifacts.

---

# 17. Verify CLI Execution Results

The normalization CLI must provide one stable machine-readable interface.

### Required Input Modes

Verify the supported modes:

```text
--batch
--job
--jobs-dir
```

Use the actual interfaces established by Task 07.

### JSON Response

Verify that single-job and multi-job execution use the same response structure.

The response must identify:

```text
run identity
overall execution status
individual job results
dataset identities
manifest paths
failure-report paths
processing counters
session summary
```

### Standard Output

During normal machine-readable execution:

**stdout must contain one complete valid JSON response.**

Progress messages and human-readable diagnostics must not corrupt the JSON.

### Exit Codes

Verify:

| Outcome | Required behavior |
|---|---|
| All requested jobs complete | Exit `0` |
| One requested job fails | Nonzero exit |
| Multiple jobs fail | Nonzero exit |
| Invalid input | Nonzero exit |
| Fatal source failure | Nonzero exit |
| Output commit failure | Nonzero exit |

A structured failed-job result must not be mistaken for successful execution merely because an internal Rust function returned `Ok`.

### Multi-Job Results

Verify correct association between results and requested jobs.

Worker completion order must not misattribute results.

Do not rely on array position without validating the intended response contract.

### Early Failures

Some failures occur before a job result exists.

Verify that these are reported honestly.

Do not fabricate completed jobs or persistent failure reports.

---

# 18. Verify Staging and Transactional Commit

Dataset commit safety is mandatory.

### Successful Job

The expected lifecycle is:

```text
Prepare staging
       │
       ▼
Process sources
       │
       ▼
Finalize writers
       │
       ▼
Validate integrity
       │
       ▼
Write metadata
       │
       ▼
Verify output
       │
       ▼
Commit dataset
```

### Failure

If any required stage fails:

- The job must not report a committed-success dataset.
- Existing committed datasets must remain unchanged.
- Available failure diagnostics must remain accessible.
- Independent jobs must retain their own outcomes.

### Output Collision

Verify that an existing dataset destination is not silently overwritten.

### Rename Semantics

Preserve the established same-filesystem staging-to-final rename behavior.

Do not replace it with recursive copying.

Do not claim stronger crash-durability guarantees than the implementation actually provides.

### Failure Injection

Use small controlled tests involving:

- Source failure.
- Writer failure.
- Metadata failure.
- Output collision.
- Missing required output.

Do not require large archives.

---

# 19. Verify Downstream Consumer Compatibility

Normalization output is intended to be consumed by later historical operations.

Inspect the current consumers.

Relevant areas include:

```text
historical merge
historical conversion
dataset inspection
depth replay
manifest readers
```

### Required Verification

Verify that supported consumers can discover and interpret:

- Stream-oriented trade files.
- Stream-oriented depth files.
- Dataset manifests.
- Source-to-stream relationships.
- Depth source-event indexes.
- Reconstruction segments.
- Boundary metadata.

### Avoid Filesystem Assumptions

Consumers should not depend on obsolete task-directory layouts when the authoritative manifest provides file locations.

For example, do not assume every file lives under:

```text
tasks/task-<id>/
```

### Legacy Dataset Compatibility

Verify support for existing committed dataset formats where their versions remain officially supported.

Do not silently break historical datasets.

### No Algorithm Rewrite

Do not rewrite merge or conversion algorithms.

Correct only necessary discovery or metadata interpretation defects.

A full merge/conversion correctness review is outside this task.

---

# 20. Verify Normalization Test Infrastructure

The authoritative archive-backed workload is:

```text
engine/tests/historical/fixtures/normalize.json
```

The shared test infrastructure must use the production batch and profile loaders.

### Required Checks

Verify:

- Deterministic task selection.
- Missing-task diagnostics.
- Ambiguous-selection diagnostics.
- Safe batch subset construction.
- Runtime overrides.
- Output isolation.
- Persistent test metadata.
- Source-only test independence.
- Exchange-specific profile selection.
- Complete-workload selection availability.

### Test Registration

Verify that every retained normalization test is registered exactly once.

Ensure the obsolete normalization-specific `slow/` hierarchy is no longer required.

Do not remove unrelated test groups.

### Source-Only Tests

Source-only tests must not require downloaded historical archives.

They must remain suitable for routine development verification.

### Real-Data Tests

Tests that require actual archives must identify those prerequisites.

Do not silently substitute a different archive when a required source is missing.

Do not report unexecuted tests as passed.

---

# 21. Persistent Test Output Verification

All artifact-producing historical normalization integration tests must use:

```text
data/tests/historical/normalize/
```

### Required Properties

- Unique execution directories.
- No automatic deletion.
- No automatic overwriting.
- Source archives remain read-only.
- Production output destinations remain untouched.
- Committed datasets are preserved.
- Failed staging artifacts are preserved where available.
- Test metadata identifies execution outcomes.

### Retained Artifacts

Where generated, preserve:

```text
Canonical Parquet.
Dataset manifests.
Resolved jobs.
Profile provenance.
Trade stream metadata.
Depth event indexes.
Reconstruction segments.
Source boundaries.
Integrity reports.
Execution metrics.
Failure reports.
```

### Concurrency

Verify that concurrent test processes cannot overwrite one another's artifacts.

Do not rely solely on test names or second-resolution timestamps for directory uniqueness.

### No Cleanup

Do not delete existing test outputs.

Do not implement a retention cleanup system.

The user will manage generated test artifacts manually.

---

# 22. Verify Benchmark Infrastructure

Tasks 08 and 09 implemented the benchmark runner and comparison tooling.

**Do not execute actual benchmarks during Task 12.**

Verify the infrastructure using lightweight tests and synthetic persisted results.

### Benchmark Input

Verify:

```text
--input <PATH>
```

accepts the authoritative prepared normalization batch from any valid filesystem location.

No fixed input directory should be required.

### Workload Selection

Verify that the runner does not hardcode exclusions for:

```text
Gate.io
Monthly archives
Large files
Specific exchanges
Specific source filenames
```

The supplied workload determines the benchmark corpus.

### Benchmark Directory

Verify:

```text
data/benchmarks/historical/normalize/
├── runs/
└── comparisons/
```

### Dry-Run

Verify that dry-run:

- Parses the workload.
- Resolves selected jobs.
- Validates configuration.
- Calculates trial counts.
- Displays the execution plan.
- Does not invoke normalization.
- Does not generate benchmark datasets.
- Does not report completed measurements.

### Statistical Infrastructure

Using synthetic persisted measurements, verify:

- Mean.
- Median.
- Standard deviation.
- P95.
- Coefficient of variation.
- Throughput aggregation.
- Worker speedup.
- Parallel efficiency.
- Missing baseline handling.
- Failed-trial exclusion.
- Interrupted-run reporting.

### Comparisons

Verify that the comparison system:

- Loads existing run results.
- Checks workload equivalence.
- Checks relevant environment differences.
- Checks correctness evidence.
- Distinguishes intentional experimental variables.
- Identifies incompatible comparisons.
- Generates JSON and Markdown reports.

### Strict Restriction

Do not run:

```bash
marketforge-benchmark normalize \
  --input <real-workload> \
  --workers 1,4,8,16
```

Do not run worker-scaling experiments.

Do not perform performance profiling.

Do not generate fabricated benchmark measurements.

Actual benchmark execution remains separately authorized.

---

# 23. End-to-End Integration Scenarios

The final verification must exercise the complete production path using small deterministic workloads.

### Scenario A — Single Trade Job

```text
Prepared batch
    → Profile resolution
    → Trade normalization
    → Stream-owned writer
    → Manifest generation
    → Dataset commit
    → CLI result
```

Verify canonical values, output metadata, provenance, and successful execution status.

### Scenario B — Multiple Trade Sources, One Stream

Several compatible source tasks contribute to the same stream.

Verify shared writer behavior, ordering, source attribution, and row accounting.

### Scenario C — Multiple Instruments

Use a job containing different concrete instruments.

Verify separate streams and correct identities.

### Scenario D — Depth With Independent Sources

Use several compatible depth archives containing valid initialization.

Verify stream-oriented storage without unintended reconstruction-state continuation.

### Scenario E — Depth Gap and Recovery

Exercise a controlled gap followed by authoritative recovery.

Verify integrity observations, reconstruction segments, event indexes, and replay.

### Scenario F — Mixed Success/Failure Session

Use several independent jobs.

Ensure successful jobs commit and failed jobs remain uncommitted.

Verify the CLI response and process exit status.

### Scenario G — Writer Failure

Inject a controlled output failure.

Verify no committed-success dataset and preserved diagnostics.

### Scenario H — Resource Constraints

Use a small bounded workload under constrained resource settings.

Verify safe admission, execution, finalization, and resource release.

### Scenario I — Worker Determinism

Execute the same small workload under different supported worker counts.

Verify equivalent canonical semantics and provenance.

Do not use runtime duration as a correctness assertion.

### Scenario J — Downstream Discovery

Verify that existing supported readers can discover and interpret the resulting trade and depth datasets.

Use focused consumer tests rather than large merge/conversion workloads.

---

# 24. Verification Scope and Execution Policy

This task authorizes meaningful integration tests.

However, verification must remain controlled.

### Mandatory Verification

Run:

- Formatting checks.
- Compilation checks.
- Relevant lint checks.
- Source-only unit tests.
- Small synthetic integration tests.
- CLI protocol tests.
- Manifest and provenance tests.
- Depth replay tests.
- Stream-consolidation tests.
- Relevant consumer compatibility tests.
- Benchmark infrastructure tests using synthetic measurements.

### Representative Real-Data Verification

Where the required prepared fixture and source archives are available, narrowly selected real-data profile tests may be executed.

Prefer small representative inputs.

Do not automatically execute the complete prepared corpus.

Do not run month-long Gate.io archives merely to verify integration.

If a required real-data case is unavailable, report it as unverified or blocked.

Do not substitute an arbitrary source.

### Full Corpus

The complete real-data normalization suite remains explicitly selected.

Do not launch it automatically.

### Performance Benchmarks

**Do not run actual benchmarks under any circumstances during Task 12.**

The user will authorize performance experiments separately.

---

# 25. Compilation and Static Verification

Use the repository's actual Cargo targets and feature configuration.

### Formatting

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

### Compilation

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

### Additional Checks

Where appropriate, verify:

- Normalization binary.
- Historical library targets.
- Relevant integration-test targets.
- Benchmark executable compilation.
- Optional historical monitor/TUI feature compilation.
- Relevant merge/conversion reader consumers.

Use the actual Cargo feature combinations supported by the repository.

Do not invent feature names.

### Clippy

Run relevant Clippy checks where the toolchain and project configuration support them.

Do not disable warnings merely to obtain a successful report.

### Platform Compatibility

MarketForge currently targets Linux and macOS.

Avoid introducing Linux-only behavior into common engine code.

Platform-specific benchmark metric collectors may remain conditionally compiled.

Do not claim macOS verification unless it was actually performed.

Do not introduce Windows support requirements.

---

# 26. Legacy Code and Dependency Cleanup

After verifying the integrated implementation, review remaining obsolete code.

Potential examples include:

```text
Legacy normalization configuration loaders.
Obsolete physical-schema resolution paths.
Legacy test fixture dependencies.
Duplicate profile resolvers.
Old task-owned writer execution paths.
Unused output discovery helpers.
Obsolete normalization slow-test registration.
```

### Required Approach

Remove obsolete code only when:

1. Its replacement is complete.
2. All relevant consumers have migrated.
3. No supported compatibility requirement depends on it.
4. Existing tests confirm equivalent or intentionally updated behavior.

Do not delete useful compatibility readers for older committed datasets.

Do not remove source-format support merely because its tests require real archives.

### Avoid Broad Cleanup

Do not perform unrelated formatting or module reorganization.

Do not rewrite working modules solely to improve aesthetics.

Do not introduce new generic abstractions without an actual requirement.

### Dead Code

Remove genuinely obsolete dead code where safe.

Do not suppress compiler warnings through unnecessary attributes when a correct removal is available.

Preserve intentional compatibility interfaces.

---

# 27. Integration Defect Resolution

This task authorizes targeted corrections to integration defects discovered during verification.

Examples include:

- Incorrect imports after shared configuration migration.
- Incompatible internal type conversions.
- Incorrect manifest file discovery.
- Missing source-to-stream metadata.
- Incorrect task metrics after writer consolidation.
- Broken CLI result mapping.
- Invalid depth offset rebasing.
- Missing required auxiliary artifacts.
- Reader incompatibility with the new manifest.
- Resource reservations not released correctly.
- Test registration errors.

### Required Method

For each confirmed defect:

1. Identify the failing behavior.
2. Establish a reproducible test or concrete code-path evidence.
3. Implement the smallest appropriate correction.
4. Verify the correction.
5. Run relevant regression tests.
6. Document the change.

Do not change expected values merely to make tests pass.

Do not weaken integrity policies.

Do not disable failing tests.

### Major Architectural Issues

If verification reveals a problem requiring significant redesign, report it as a blocker.

Do not introduce a large new subsystem under Task 12 without explicit authorization.

The objective is stabilization, not uncontrolled expansion.

---

# 28. Final Readiness Classification

The final report must distinguish between implemented functionality and verified functionality.

Use the following categories.

## Verified

Functionality whose required checks were executed successfully.

## Implemented but Not Fully Verified

Functionality that exists and compiles but lacks a required execution prerequisite.

For example:

```text
A supported exchange profile exists, but its required
real-data archive was unavailable.
```

## Known Limitation

Functionality with an explicitly documented limitation that does not contradict a supported guarantee.

## Blocked

A critical requirement that cannot be validated or completed.

## Failed

A requirement whose executed verification produced a genuine failure.

### Important

Do not classify:

```text
Not executed
```

as:

```text
Passed
```

Do not classify:

```text
Compiles
```

as:

```text
End-to-end verified
```

Do not classify:

```text
Manifest internally consistent
```

as:

```text
Source normalization semantically correct
```

Preserve the distinction between evidence levels.

---

# 29. Final Verification Report

Produce a concise but comprehensive final report.

Use a version-controlled Markdown document under the historical normalization documentation, following the repository's existing documentation conventions.

Suggested location:

```text
docs/historical/normalize-integration-verification.md
```

Use the actual documentation structure if a more appropriate location already exists.

### Report Contents

#### Implementation Status

Summarize Tasks 01–11.

Identify completed functionality and remaining gaps.

#### Architecture

Describe the final production execution flow.

Include configuration, batch loading, profiles, scheduling, stream writers, finalization, and CLI reporting.

#### Supported Profiles

Provide a supported-profile inventory and verification status.

Identify untested or blocked real-data cases.

#### Trade Correctness

Describe executed trade-verification cases and their results.

#### Depth Correctness

Describe executed reconstruction and replay tests.

#### Output Contracts

Document final dataset structure, manifest versions, and auxiliary metadata.

#### Provenance

Explain source-to-stream attribution and resolved-job persistence.

#### Integrity

Summarize observation accounting, policy evaluation, timestamp attribution, and error classification.

#### Resource Safety

Summarize resource admission, long-lived writer ownership, and relevant failure tests.

#### CLI

Document input modes, machine-readable output, failure behavior, and exit codes.

#### Downstream Compatibility

Identify merge, conversion, replay, and inspection consumers verified.

#### Benchmark Infrastructure

Describe benchmark planning, measurement/reporting support, statistical summaries, and comparison functionality.

Explicitly state that real benchmarks were not executed.

#### Verification Commands

List the exact commands executed.

#### Test Results

Provide:

```text
Tests passed.
Tests failed.
Tests blocked.
Tests not executed.
```

Use actual counts where available.

#### Remaining Limitations

Identify anything that prevents a stronger readiness conclusion.

Do not invent performance measurements or unexecuted test results.

---

# 30. Implementation Sequence

## Phase A — Repository Audit

Inspect the current implementation and previous task results.

Establish the verification baseline.

## Phase B — Static Verification

Run formatting, compilation, and relevant lint checks.

Correct narrow integration failures.

## Phase C — Input and Configuration

Verify shared runtime configuration, batch loading, profile discovery, and resolved jobs.

## Phase D — Canonical Correctness

Verify representative trade and depth normalization behavior.

Preserve independent expected values.

## Phase E — Stream-Oriented Persistence

Verify trade and depth writer ownership, output grouping, part rotation, metrics, and provenance.

## Phase F — Reconstruction and Replay

Verify depth event indexing, segment offsets, source boundaries, zero-change events, and replay.

## Phase G — Integrity and Failure Safety

Verify accounting, policies, fatal errors, resource limits, and transactional commits.

## Phase H — External Contracts

Verify CLI JSON, exit codes, manifests, resolved-job provenance, and failure reports.

## Phase I — Downstream Compatibility

Verify relevant merge, conversion, replay, and dataset-inspection consumers.

## Phase J — Benchmark Infrastructure

Verify benchmark CLI planning, statistical calculations, and comparison generation using synthetic results.

Do not execute benchmarks.

## Phase K — Cleanup

Remove confirmed obsolete code and duplicate active implementations where safe.

Do not perform unrelated cleanup.

## Phase L — Final Verification

Run the relevant regression checks again after corrections.

Produce the final report.

Stop after completing Task 12.

---

# 31. Scope Restrictions

## Authorized

This task authorizes:

- Final architecture and integration review.
- Compilation and static verification.
- Focused unit and integration tests.
- Representative small real-data tests where available.
- Stream-oriented trade correctness verification.
- Stream-oriented depth correctness verification.
- Depth replay verification.
- Manifest and provenance validation.
- CLI contract verification.
- Resource and failure-safety verification.
- Downstream reader compatibility tests.
- Benchmark infrastructure verification using synthetic data.
- Targeted integration defect corrections.
- Safe obsolete-code cleanup.
- Documentation updates.
- Final readiness reporting.

## Not Authorized

Do not implement:

- New normalization algorithms without a confirmed defect.
- New source formats.
- New exchange integrations.
- New profile architectures.
- New batch JSON protocols.
- New runtime configuration systems.
- New integrity-policy semantics.
- Cross-archive book-state continuation.
- General cross-archive deduplication.
- New merge algorithms.
- New conversion algorithms.
- Asynchronous decode-ahead.
- Speculative prefetch queues.
- Unbounded buffering.
- Automatic performance tuning.
- PostgreSQL registry integration.
- Python Forge orchestration changes.
- Axum management APIs.
- TUI redesign.

### Benchmark Restriction

Do not execute actual performance benchmarks.

Do not perform worker-scaling performance experiments.

Do not produce performance claims without measurements.

### Full Workload Restriction

Do not automatically execute the complete real-data normalization corpus.

Real-data execution beyond narrowly selected representative tests requires separate authorization.

---

# 32. Working-Tree and Data Safety

Preserve unrelated user changes.

Do not reset the repository.

Do not create commits or branches unless requested.

Do not overwrite production datasets.

Do not delete existing committed datasets.

Do not modify original source archives.

Do not modify the authoritative test workload.

Do not change production resource values.

Do not weaken integrity policies.

Do not delete persistent test outputs.

Do not delete benchmark history.

### Test Output Isolation

All artifact-producing normalization integration tests must write under:

```text
data/tests/historical/normalize/
```

Preserve generated artifacts for manual inspection.

Use unique execution directories.

Do not automatically clean up those directories.

---

# 33. Final Acceptance Criteria

Task 12 is complete when the following guarantees have been verified to the extent supported by the available tests and prerequisites.

### Configuration and Inputs

- The shared historical runtime configuration loads correctly.
- Normalization uses the authoritative `general.toml`.
- The production schema-version-3 batch is supported.
- Unified profiles resolve correctly.
- Operational profile settings are enforced.
- Instrument and source identities remain correct.
- Legacy configuration dependencies are removed where obsolete.

### Execution

- Stream-aware scheduling works.
- Single-job execution works.
- Multi-job execution works.
- Independent jobs preserve failure boundaries.
- Resource admission remains bounded.
- Writer ownership and resource lifetimes are correct.
- No known deadlock or resource leak remains in the tested execution paths.

### Trades

- Canonical trade values remain correct.
- Compatible archives can share stream-owned output.
- Different instruments remain separated.
- Source ordering is preserved.
- Task-level metrics remain correct.
- Source attribution remains available.

### Depth

- Canonical depth interpretation remains correct.
- Source initialization is preserved.
- Supported update semantics are preserved.
- Gaps and recovery remain correct.
- Shared physical output preserves logical source boundaries.
- Source-event identities remain unambiguous.
- Canonical row offsets are correct.
- Zero-change events remain indexed.
- Reconstruction segments remain valid.
- Replay produces independently expected states.

### Persistence

- Parquet files are finalized correctly.
- File rotation follows the supported configuration.
- Dataset manifests are accurate.
- Source-to-output relationships remain reconstructable.
- Required auxiliary artifacts exist.
- Resolved-job provenance remains valid.
- Existing committed datasets are never silently overwritten.
- Failed jobs cannot publish successful datasets.

### Integrity

- Eligible integrity observations are correctly accounted for.
- Violation rates use correct denominators.
- Known failure timestamps are attributed correctly.
- Unattributed failures remain visible.
- Applicable timestamp regressions are detected.
- Fatal source errors terminate safely.
- Existing integrity-policy semantics remain unchanged.
- Integrity results remain consistent under supported worker variations.

### CLI

- The authoritative batch is accepted.
- Supported legacy input modes remain functional.
- JSON responses are structurally consistent.
- stdout remains machine-readable.
- Failed jobs produce nonzero process exit codes.
- Failure reports remain accessible where generated.
- Successful jobs return valid committed manifest paths.

### Downstream Compatibility

- Supported merge readers can discover new trade/depth output.
- Supported conversion readers can discover applicable output.
- Depth replay understands consolidated metadata.
- Dataset inspection uses valid manifests.
- Supported legacy dataset formats remain readable where required.

### Tests

- Relevant source-only tests pass.
- Relevant small integration tests pass.
- Test registration is correct.
- Real-data prerequisites are reported honestly.
- Persistent test outputs remain available.
- No production dataset paths are modified.
- No failing tests are hidden or disabled.

### Benchmarks

- Benchmark input paths remain flexible.
- No exchange exclusions are hardcoded.
- Dry-run works without normalization.
- Trial metadata serialization works.
- Statistical calculations are correct.
- Comparison validation works.
- Synthetic reports can be generated.
- No real benchmarks are executed.

### Final Reporting

- Actual verification results are documented.
- Remaining unsupported behavior is identified.
- Blocking defects are identified.
- Unexecuted real-data coverage is identified.
- No unsupported performance claims are made.

**If critical acceptance criteria remain unverified or fail, report Task 12 as incomplete or blocked rather than declaring full readiness.**

---

# 34. Final Stop Condition

**Stop after Task 12.**

Do not automatically proceed to another implementation task.

Do not start Python Forge integration.

Do not begin performance tuning.

Do not execute benchmark workloads.

Do not introduce additional architectural changes.

The user will review the final verification report before authorizing further work.

### Final Deliverables

Provide:

1. Integrated historical normalization implementation.
2. Relevant corrected tests.
3. Verified CLI and filesystem output contracts.
4. Updated documentation where required.
5. Final integration-verification report.
6. Clear list of remaining limitations or blockers.

The completion report must distinguish:

**Implemented functionality, verified functionality, and functionality awaiting real-data or performance verification.**

---

## Final Objective

Deliver a stable, coherent, and thoroughly verified MarketForge historical normalization engine that:

- Accepts production-prepared normalization batches.
- Uses authoritative self-contained TOML profiles.
- Produces correct canonical trade and depth data.
- Preserves source provenance and reconstruction semantics.
- Uses resource-bounded stream-oriented persistence.
- Produces accurate manifests and execution reports.
- Handles failures safely.
- Integrates with existing historical data consumers.
- Exposes a reliable interface for Python Forge.
- Supports reproducible testing and future performance comparisons.

**The ultimate requirement is correctness and reliability—not merely successful compilation, passing internal assertions, or reduced Parquet file counts.**

Historical normalization must be ready for subsequent orchestration integration without requiring another broad architectural redesign.

**Stop after verification and report the actual state of the system.**