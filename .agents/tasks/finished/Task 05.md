# Task 05 — Source-Error Handling and Timestamp-Ordering Enforcement

**Project:** MarketForge  
**Component:** Rust Historical Normalization Engine  
**Priority:** High / Data Correctness  
**Status:** Approved for implementation  
**Dependencies:** Tasks 01–04  
**Next task:** Exchange-Profile Tests and Historical Test-Suite Migration

## 1. Objective

Correct two important aspects of the historical normalization engine:

1. **Source-error handling:** Distinguish recoverable record-level errors from fatal source-stream failures.
2. **Timestamp-ordering enforcement:** Ensure that declared timestamp-ordering guarantees are actually validated without violating source-specific processing semantics.

The engine already contains substantial source-decoding, normalization, integrity, and depth-reconstruction infrastructure.

Preserve that infrastructure.

The objective is to close specific correctness gaps, not introduce another processing architecture.

**A normalization job must not complete successfully when its required source stream failed irrecoverably or when an applicable ordering guarantee was violated and the configured policy requires failure.**

Complete only Task 05.

---

## 2. Architectural Context

Tasks 01–04 establish:

- Shared historical runtime configuration.
- Production schema-version-3 batch loading.
- Unified normalization profiles.
- Shared normalization test infrastructure.
- Persistent test outputs.
- Correct integrity observation accounting.
- Correct integrity time-window attribution.

This task builds on those foundations.

### Intended Error-Handling Flow

```text
Source Archive
      │
      ▼
Container / Decompression Reader
      │
      ▼
Record Decoder
      │
      ├── Valid record
      │       │
      │       ▼
      │   Normalization
      │
      ├── Recoverable record error
      │       │
      │       ▼
      │   Integrity Policy
      │       │
      │       ├── Fail
      │       └── Recover when permitted
      │
      └── Fatal source error
              │
              ▼
         Abort Source Task
              │
              ▼
         Fail Affected Job
```

### Intended Ordering Flow

```text
Decoded Source Event
        │
        ▼
Resolve Concrete Stream Identity
        │
        ▼
Determine Applicable Ordering Contract
        │
        ▼
Timestamp / Sequence Validation
        │
        ├── Valid
        │      │
        │      ▼
        │   Continue Processing
        │
        └── Violation
               │
               ▼
          Integrity Policy
               │
               ├── Fail
               └── Recover only if safe
```

These diagrams describe responsibilities, not mandatory implementation structures.

Do not rewrite the processing pipeline merely to reproduce them.

---

# 3. Inspect the Existing Implementation First

Before modifying code, inspect the current source-reading, decoding, normalization, and ordering paths.

Relevant areas may include:

```text
engine/src/shared/io/
engine/src/shared/formats/
engine/src/shared/normalization/
engine/src/shared/validation/

engine/src/historical/normalize/
├── worker/
│   ├── trades.rs
│   └── depth.rs
├── metrics/
├── task.rs
├── executor.rs
└── session.rs
```

Use the actual repository structure where it differs.

### Source I/O

Inspect:

- CSV readers.
- JSONL readers.
- ZIP handling.
- GZIP handling.
- TAR/GZIP handling.
- XLSX readers.
- Archive-member selection.
- Decompression error propagation.
- Source-file opening.
- EOF handling.
- Record-decoding error classification.

### Ordering

Inspect:

- Trade processing.
- Depth processing.
- Timestamp normalization.
- Timestamp-regression integrity categories.
- Source sequence validation.
- Profile-defined ordering behavior.
- Batch ordering declarations.
- Stream identification.
- Family-scoped source handling.
- Worker-level and job-level integrity evaluation.

### Existing Types

Identify existing equivalents of:

```rust
MarketForgeError
IntegrityCategory
IntegrityDiagnostic
IntegrityPolicy
IntegrityScope
ScopedIntegrityMetrics
ProcessingStatus
DepthProcessor
```

Do not introduce duplicate types when existing ones are suitable.

### Revalidate Previous Findings

Previous source inspection identified two areas of concern.

**Source errors**

The reviewed JSONL decoder converted underlying read failures into JSON errors. The depth worker could subsequently treat those failures as recoverable parsing errors.

**Timestamp regressions**

The reviewed trade processor parsed timestamps independently, and its metrics recorded minimum and maximum timestamps without establishing chronological ordering.

Recheck the current implementation.

If a gap is already fixed, verify the implementation and its tests rather than replacing it.

---

# 4. Establish Explicit Error Categories

The processing pipeline must distinguish:

```text
Recoverable record-format error.
Recoverable record-validation error.
Fatal source-stream error.
Fatal output or execution error.
```

These categories have different consequences.

## 4.1 Recoverable Record Errors

A recoverable record error occurs when the decoder has identified a complete record boundary and can safely continue to the next record.

Examples may include:

- A malformed complete JSONL record.
- An invalid numeric field.
- An unsupported value in a recognized field.
- An invalid trade quantity.
- An invalid trade side.
- A record-level transformation failure.

Recovery is permitted only when the relevant integrity policy allows it and processing state remains valid.

Do not automatically consider every parsing error recoverable.

## 4.2 Fatal Source Errors

Fatal source errors indicate that the input stream itself cannot be consumed reliably.

Examples include:

- Nonrecoverable filesystem read failure.
- Truncated compressed source.
- Corrupted compression stream.
- Archive read failure.
- Failure to read the required archive member.
- An unrecoverable decoder state.
- An input reader that repeatedly fails without advancing.

These must not be treated as ordinary invalid records.

**An integrity policy allowing malformed records must not authorize ignoring a fatal source-stream failure.**

## 4.3 Fatal Output Errors

Output failures are also terminal for the affected job.

Examples include:

- Parquet writer failure.
- Output filesystem error.
- Failure to finalize a required output.
- Failure to persist required reconstruction metadata.
- Failure to commit the dataset.

Do not route output errors through record-level degradation policies.

Preserve existing output-error handling.

---

# 5. Use Typed Error Classification

Implement or reuse a strongly typed distinction between recoverable record errors and fatal source errors.

Conceptually:

```rust
enum SourceReadOutcome<T> {
    Record(T),
    RecoverableRecordError(...),
    EndOfInput,
}
```

Fatal source failures may be returned through the existing `Result` error channel.

This is illustrative, not a mandatory API.

The implementation should follow existing MarketForge conventions.

### Required Properties

- Error classification must not depend on parsing human-readable strings.
- Recoverable errors must identify the applicable source record where possible.
- Fatal errors must preserve their underlying cause.
- Error propagation must not lose relevant source-path or archive-member context.
- EOF must be distinguishable from failure.
- Unsupported recovery must fail explicitly.

Do not introduce a complex generic error framework.

Prefer the smallest type-level change that prevents incorrect error handling.

### Preserve Error Codes

Reuse existing error categories where they correctly represent the failure.

Do not introduce new public error codes without a demonstrated need.

If an existing public error code is insufficient, document the compatibility implications of extending it.

The standardized CLI response contract belongs to Task 07.

Do not redesign it here.

---

# 6. JSONL Error Handling

The JSONL decoder is a priority for this task.

## 6.1 Existing Concern

The reviewed implementation used an API conceptually equivalent to:

```rust
pub fn next_json(
    &mut self,
) -> Result<Option<Value>, serde_json::Error>
```

Underlying reader errors could be wrapped as JSON errors.

The caller could then interpret both invalid JSON and source-read failures as parsing violations.

This is unsafe when a configured policy permits continuing after malformed records.

## 6.2 Required Correction

Distinguish:

```text
Complete line containing invalid JSON
```

from:

```text
Failure while reading the source stream
```

### Recoverable JSON Error

Example:

```json
{"timestamp":123,"price":
```

If the decoder has consumed the complete line and can reliably proceed to the next record, this may be treated as a recoverable parsing failure.

The integrity policy determines whether the record can be rejected.

### Fatal Read Error

If the underlying reader fails before a reliable record boundary can be established, terminate the source task.

Do not continue calling the decoder as though each repeated I/O failure represents another malformed record.

### Decoder Progress

A recoverable error must not leave the decoder repeatedly reporting the same record without advancing.

Preserve correct source-record ordinals.

Do not increment record counters indefinitely for the same underlying error.

### Blank Lines

Preserve the existing documented behavior for blank lines.

Do not introduce new blank-line semantics unless required to correct an actual defect.

---

# 7. Source Reader Progress and Termination

A critical safety requirement is that source decoding must not enter an unbounded retry loop after a persistent error.

### Required Behavior

For a fatal source error:

1. Stop consuming the source.
2. Preserve the underlying error.
3. Stop the affected task.
4. Prevent the affected job from committing successfully.
5. Preserve available diagnostics and metrics.
6. Release associated resources according to existing ownership rules.

Do not rely on an integrity count threshold to eventually stop an unrecoverable reader.

### Retry Policy

Do not introduce automatic retry behavior unless the existing source interface explicitly supports it.

If a narrowly defined transient error is already supported:

- Preserve its existing policy.
- Ensure retries are bounded.
- Require evidence of recoverability.
- Preserve error context.
- Avoid indefinite waiting.

Do not retry corrupted compression streams as though they were network requests.

Historical source files are local inputs, not live feeds.

---

# 8. Review Other Source Formats

After correcting JSONL, review the other supported source formats for equivalent error-classification problems.

## CSV

Distinguish:

- Recoverable malformed records.
- Invalid field values.
- Fatal reader failures.
- Fatal stream or decompression failures.

Do not assume CSV record-boundary recovery is always possible.

Preserve existing CSV interpretation.

## ZIP

Distinguish:

- Invalid or missing requested members.
- Corrupt archives.
- Decompression errors.
- Record-level errors inside successfully opened members.

A corrupt archive must not be treated as a collection of tolerated malformed records.

## GZIP and TAR/GZIP

Preserve compression and archive errors as source-level failures.

Do not report successful EOF when decompression terminated unexpectedly.

Do not silently accept truncated compressed input.

## XLSX

Preserve workbook-level errors as fatal source failures where recovery is unsupported.

Do not turn a damaged workbook into a successful partial dataset.

Keep XLSX layout and profile behavior changes outside this task unless required for correct error classification.

### Scope Constraint

Fix only demonstrated problems.

Do not rewrite all decoder implementations solely for interface uniformity.

---

# 9. Preserve Depth Reconstruction Safety

Depth normalization requires stronger recovery guarantees than independent trade-record normalization.

A malformed or rejected depth event may contain state-changing information.

Skipping it can make subsequent order-book reconstruction incorrect.

### Required Behavior

Preserve existing synchronization invalidation.

When a rejected depth event could have changed the book:

```text
Current reconstructed state
          │
          ▼
Rejected source event
          │
          ▼
Invalidate synchronization
          │
          ▼
Wait for authoritative initialization
```

Do not continue applying later deltas to a potentially incomplete book.

### Recoverable Parsing Versus Recoverable Reconstruction

A malformed record may be recoverable at the decoder level.

That does not mean the reconstructed order book remains valid.

Keep these decisions separate.

For example:

```text
Malformed JSON line
        │
        ▼
Decoder can continue
        │
        ▼
Depth state invalidated
        │
        ▼
Later authoritative snapshot
        │
        ▼
Reconstruction may resume
```

Do not fabricate a snapshot.

Do not borrow a previous archive's final book state without an explicitly supported continuity contract.

### Fatal Errors

A fatal source failure must terminate the affected task regardless of whether another snapshot might have appeared later.

Do not skip unreadable source bytes and search for recovery unless the source format explicitly supports that behavior.

---

# 10. Timestamp-Ordering Contract

The normalization engine exposes timestamp-regression integrity policies.

This task must ensure those policies are connected to real, applicable checks.

However, not every source format has the same ordering semantics.

**Do not implement a universal rule that blindly requires every raw source record to have a nondecreasing timestamp.**

First establish the actual ordering contracts.

## 10.1 Distinguish Ordering Concepts

The engine must distinguish:

```text
Source record order.
Source event sequence order.
Event timestamp order.
Canonical output order.
Cross-archive order.
```

These are related but not interchangeable.

### Source Record Order

The order in which records appear in the source.

Some source formats require this order to be preserved.

### Source Sequence Order

An exchange-provided sequence relationship.

This can be more authoritative than event timestamps for depth reconstruction.

### Event Timestamp Order

Chronological ordering of event timestamps.

Not every valid source necessarily provides this guarantee.

### Canonical Output Order

The ordering guarantee declared for normalized output.

This may depend on source-specific sorting or later merge processing.

### Cross-Archive Order

The relationship between separately downloaded source archives.

This requires explicit source-range and compatibility rules.

Do not infer it from task IDs or filesystem paths.

---

# 11. Inspect Existing Ordering Metadata

The prepared normalization batch contains:

```json
{
  "ordering": {
    "primary": "event_timestamp_ns",
    "tie_break": "stream_rank"
  }
}
```

The authoritative runtime configuration also contains a commented ordering proposal.

Do not activate the commented TOML section.

Do not introduce a second competing ordering configuration.

Inspect what the production batch ordering declaration currently guarantees and which components enforce it.

### Required Outcome

Document:

- Where timestamp monotonicity is required.
- Where timestamp sorting is explicitly supported.
- Where source sequence order must be preserved.
- Where output ordering is not yet guaranteed.
- Which existing integrity categories enforce each rule.

Do not claim a stronger guarantee than the engine implements.

If a declared guarantee cannot be enforced by the current execution mode, reject that unsupported combination explicitly rather than silently accepting it.

---

# 12. Implement Applicable Timestamp-Regression Detection

Where the supported processing contract requires nondecreasing event timestamps, maintain the necessary ordering state.

Conceptually:

```text
previous accepted timestamp = 100

current timestamp = 101
    → valid

current timestamp = 101
    → valid, unless strict increase is required

current timestamp = 99
    → regression
```

### Required Comparison

For nondecreasing ordering:

```text
current_timestamp < previous_timestamp
```

is a regression.

Equal timestamps are valid unless an explicitly supported contract requires strictly increasing timestamps.

### Scope

Perform comparisons within the correct logical stream.

Do not compare unrelated instruments merely because they appear in the same job.

For example:

```text
BTCUSDT timestamp = 101
ETHUSDT timestamp = 50
```

is not necessarily a regression.

The records belong to different concrete streams.

### Family-Scoped Archives

A family-scoped archive may contain multiple instruments.

Do not maintain one global timestamp watermark that falsely compares records from different instruments.

Resolve the appropriate concrete stream identity before applying stream-scoped ordering checks.

Do not fabricate instrument identities.

### State Advancement

Define when the ordering watermark advances.

Do not automatically advance accepted-state tracking using a rejected record's timestamp.

A failed or rejected record may require additional recovery handling.

Make state advancement consistent with the accepted processing semantics.

---

# 13. Source Ordering Versus Timestamp Sorting

Some source formats may legitimately contain unordered records and explicitly permit sorting.

Others require strict source-event processing order.

These cases must remain distinct.

## 13.1 Explicitly Sortable Sources

Where a supported profile explicitly permits sorting:

- Preserve the existing sorting behavior.
- Use a deterministic tie-breaking rule.
- Validate the final applicable ordering guarantee.
- Do not classify valid raw source disorder as a regression when sorting is part of the declared interpretation.

For example, an independently sortable source containing:

```text
103
101
102
```

may legitimately produce:

```text
101
102
103
```

if that transformation is explicitly supported.

Do not introduce sorting where the profile does not authorize it.

## 13.2 Stateful Depth Sources

Depth updates may depend on source or exchange sequence order.

Do not sort state-changing depth updates by timestamp merely to eliminate regressions.

Example:

```text
Sequence 10 → timestamp 103
Sequence 11 → timestamp 102
```

The correct handling depends on the declared source semantics.

Do not automatically transform this into:

```text
Sequence 11
Sequence 10
```

Such reordering can corrupt book reconstruction.

Instead, apply the relevant source sequence and timestamp policies independently.

Where the two ordering guarantees conflict, produce an explicit integrity outcome or unsupported-contract error.

Do not silently repair the input.

---

# 14. Timestamp Regression as an Integrity Observation

Use the integrity observation infrastructure corrected in Task 04.

For an applicable timestamp comparison:

```text
eligible += 1
```

When a regression occurs:

```text
violations += 1
```

Do not record a successful timestamp comparison when no previous comparable event exists.

### Diagnostics

Preserve useful context, including:

```text
task ID
source record ordinal
stream identity
current timestamp
previous timestamp
relevant source identity
```

Use the existing diagnostic structures.

Do not introduce a new diagnostics framework.

### Policy Evaluation

Apply the configured `timestamp_regression` policy.

Preserve existing semantics for:

```text
action = "fail"
action = "degrade"
```

Do not silently change thresholds.

### Depth-Specific Recovery

If a timestamp violation causes rejection of a state-changing depth event, preserve synchronization invalidation and the requirement for authoritative recovery.

Do not treat a skipped delta as harmless.

If the policy permits degradation but the event remains accepted, preserve the documented event semantics and record the violation.

Do not silently discard a required update merely to keep timestamps monotonic.

---

# 15. Cross-Task and Cross-Archive Boundaries

Task IDs and file boundaries do not necessarily define logical stream boundaries.

However, the current normalization engine may process archives independently.

### Required Approach

Inspect the actual existing continuity contract.

Determine whether timestamp monotonicity is guaranteed:

- Within one source task.
- Within one archive member.
- Across tasks belonging to the same stream.
- Across archives in one normalization job.
- Only after a separate merge operation.

Enforce only supported guarantees.

Do not silently advertise cross-archive chronological continuity when the implementation checks only individual files.

### No Premature Stream Consolidation

Tasks 10 and 11 will introduce stream-oriented output consolidation.

Do not implement that architecture here.

Do not create persistent cross-task writer state.

Do not implement cross-archive sorting or deduplication.

### Boundary Behavior

If task-level ordering is the only supported guarantee, document it clearly.

If the current production contract already requires stronger continuity, implement the smallest necessary validation without changing output ownership.

Do not assume chronological order from:

```text
task_id
job_id
input array position
download directory hash
full filesystem path
```

Where a required ordering guarantee cannot be established, fail explicitly rather than inventing an ordering relationship.

---

# 16. Preserve Existing Integrity Policies

Use the shared runtime configuration:

```text
data/.config/historical/general.toml
```

Do not modify its values.

Preserve the existing policy categories:

```text
parse_failure
invalid_record
sequence_gap
timestamp_regression
missing_snapshot
invalid_book
transformation_failure
```

### Immediate Failure

If the existing evaluator treats:

```toml
action = "fail"
```

as immediate failure, preserve that behavior.

Do not reinterpret `max_count` or other thresholds.

### Deferred Thresholds

Where threshold evaluation is deferred to job finalization, ensure that the observed violations are actually included.

Do not depend on deferred thresholds to terminate fatal source-read failures.

### Policy-Disabled Behavior

Disabling integrity-policy evaluation must not make fatal source or output errors recoverable.

A broken input stream is not a valid completed source merely because integrity checking is disabled.

Preserve the separation between execution failure and record-integrity policy.

---

# 17. Failure Reporting and Dataset Safety

A fatal source error or policy-required ordering failure must not produce a successfully committed dataset.

Preserve the existing staging and commit architecture.

### Required Behavior

When an affected job fails:

- Stop the affected processing path.
- Preserve the actual failure cause.
- Preserve available task metrics.
- Preserve available integrity diagnostics.
- Preserve failed-task attribution.
- Retain incomplete staging artifacts according to existing behavior.
- Do not publish a completed manifest.
- Do not overwrite an existing dataset.

### Multi-Job Sessions

Preserve existing independent job-failure boundaries.

A failure in one job must not automatically corrupt another independent job.

Do not redesign the global scheduler.

Do not introduce new cancellation behavior unless necessary to prevent invalid execution after a fatal source failure.

### Error Classification

Do not classify all failures as generic record integrity violations.

Preserve meaningful distinctions between:

```text
Source unavailable.
Source read failure.
Corrupt source.
Record parsing failure.
Record validation failure.
Ordering violation.
Output failure.
```

Reuse existing error codes where possible.

---

# 18. Testing Strategy

Use the shared testing infrastructure established by Task 03.

Most tests should use small deterministic synthetic sources.

Do not require large historical archives to verify reader failure behavior or timestamp ordering.

The testing goal is to reproduce the relevant failure conditions precisely.

---

## 18.1 JSONL Recovery Tests

### Malformed Record Followed by Valid Input

Input:

```text
{"timestamp":100,"value":1}
{"timestamp":invalid}
{"timestamp":102,"value":3}
```

The example is schematic; use the actual supported source schema.

Verify:

- First record is processed.
- Second record is classified as a malformed record.
- The integrity policy determines whether recovery is allowed.
- The decoder advances to the next complete record.
- Third record is processed only when recovery is safe.
- Source-record accounting remains correct.

For stateful depth, also verify synchronization invalidation and recovery requirements.

### Fatal Reader Failure

Use a custom reader that:

1. Returns a valid prefix.
2. Produces a persistent I/O error.

Verify:

- The source task terminates.
- The error remains classified as a source failure.
- The decoder does not repeatedly treat the same failure as new records.
- The affected job does not commit.
- Diagnostics identify the source failure.

Do not rely on an uncontrolled timeout to prove termination.

Use deterministic call-count assertions or another bounded test mechanism.

---

## 18.2 Compression Failure Tests

Use small synthetic archives to test applicable formats.

Verify:

- Corrupt compression data fails.
- Truncated compressed input fails where required.
- Missing required archive members fail.
- Fatal archive errors are not treated as tolerated malformed records.
- Valid archives continue to work.

Do not create large real-data archives for these tests.

---

## 18.3 Timestamp-Regression Tests

### Increasing Timestamps

Input:

```text
100
101
102
```

Verify no regression.

### Equal Timestamps

Input:

```text
100
100
101
```

Verify no regression under a nondecreasing timestamp contract.

### Backward Timestamp

Input:

```text
100
99
101
```

Verify:

- The applicable regression is detected.
- The integrity observation is recorded.
- The configured policy is applied.
- The diagnostic identifies the relevant source event.

### Independent Streams

Input:

```text
BTCUSDT: 100
ETHUSDT: 50
BTCUSDT: 101
ETHUSDT: 51
```

Verify no false regression caused by comparing unrelated streams.

### Sortable Source

Use a small source whose supported profile permits deterministic timestamp sorting.

Verify that the profile-authorized ordering behavior is honored.

Do not introduce a new sorting mechanism solely for the test.

---

## 18.4 Depth Ordering Tests

Verify:

- Valid source sequence progression.
- Timestamp regression under the applicable contract.
- Equal timestamps where permitted.
- Preservation of source sequence order.
- No automatic reordering of state-changing deltas.
- Correct integrity diagnostics.
- Safe synchronization behavior after a rejected update.
- Recovery only after the required authoritative initialization.

Use independently reviewed expected book states where applicable.

Do not rely only on the final row count.

---

## 18.5 Cross-Task Boundary Tests

Where the production contract requires cross-task continuity, use two small related source tasks.

Verify the supported boundary behavior.

If cross-task continuity is not currently guaranteed, test that the engine does not falsely advertise it.

Do not implement stream-oriented Parquet consolidation to make this test pass.

---

## 18.6 Execution Failure Tests

Verify that a fatal input error:

- Produces a failed job result.
- Does not produce a committed-success manifest.
- Preserves available failure diagnostics.
- Does not overwrite previous datasets.
- Does not corrupt unrelated jobs.

Use the existing execution APIs.

Task 07 will standardize public CLI JSON and exit-code behavior.

Do not redesign that transport contract here.

---

# 19. Persistent Test Artifacts

Task 03 established:

```text
data/tests/historical/normalize/
```

Use this directory for artifact-producing normalization integration tests.

Relevant test groups may include:

```text
data/tests/historical/normalize/
├── depth/
├── trades/
└── execution/
```

Every persistent execution must receive its own unique run directory.

Preserve:

- Available canonical output.
- Failed staging artifacts.
- Failure reports.
- Integrity diagnostics.
- Resolved job provenance.
- Reconstruction metadata where generated.

Do not delete these outputs automatically.

### Pure Unit Tests

Small decoder and state-machine tests may continue using temporary in-memory or disposable fixtures where persistent artifacts provide no inspection value.

Do not unnecessarily persist every parser-test input.

### Safety

Never write into production dataset destinations.

Never modify original source archives.

Never alter the authoritative fixture:

```text
engine/tests/historical/fixtures/normalize.json
```

Never relax production integrity policies merely to make tests pass.

---

# 20. Performance and Resource Considerations

These corrections must not introduce unnecessary processing overhead.

### Error Handling

Normal successful decoding should remain efficient.

Avoid creating error messages or allocating diagnostic objects for every valid record.

Use typed errors without introducing expensive dynamic dispatch where unnecessary.

### Timestamp Tracking

Timestamp comparisons should require only bounded per-stream state.

Do not buffer entire source archives merely to detect regressions.

Do not sort unsortable sources.

Do not introduce unbounded maps for unknown stream identities without respecting existing resource constraints.

### XLSX and Sortable Formats

Preserve existing materialization behavior where the supported format already requires it.

Do not add another copy of the entire workbook solely for ordering validation.

### Benchmark Restriction

Do not run performance benchmarks.

Do not tune resource budgets.

Do not change Parquet file-size targets.

The benchmarking infrastructure will be implemented in Tasks 08 and 09.

---

# 21. Implementation Sequence

## Phase A — Baseline Inspection

Inspect existing decoders, source readers, workers, integrity evaluation, and ordering behavior.

Confirm the previously identified gaps.

Identify existing tests that already cover some required behavior.

## Phase B — Error Classification

Implement or reuse typed distinctions between recoverable record errors and fatal source failures.

Keep the changes minimal.

## Phase C — Decoder Integration

Correct JSONL behavior.

Review other supported source formats and fix equivalent demonstrated problems.

Preserve existing source-format semantics.

## Phase D — Ordering Contract

Identify supported ordering guarantees.

Document the applicable comparison scopes and exceptions.

Determine where timestamp-regression observations must be generated.

## Phase E — Timestamp Enforcement

Implement the missing applicable comparisons.

Integrate them with the integrity observation infrastructure.

Preserve source sequence semantics and depth safety.

## Phase F — Failure Propagation

Verify that fatal input failures and policy-required ordering failures propagate correctly through task and job execution.

Preserve staging and commit safety.

## Phase G — Verification

Run focused synthetic tests and relevant existing source-only regression tests.

Run formatting and compilation checks.

Stop after completing Task 05.

---

# 22. Verification Requirements

Use the repository's actual Cargo targets.

### Formatting

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

### Compilation

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Also compile affected test targets and relevant binaries.

### Focused Tests

Run tests covering:

- Recoverable malformed records.
- Fatal source-read errors.
- Compression errors.
- Decoder progress.
- Timestamp regressions.
- Equal timestamps.
- Independent streams.
- Profile-authorized sorting.
- Depth sequence preservation.
- Depth synchronization invalidation.
- Job failure propagation.
- Dataset commit safety.

### Existing Regression Tests

Run relevant existing tests for:

- Trade normalization.
- Depth normalization.
- Source readers.
- Integrity evaluation.
- Depth reconstruction.

Do not run the complete historical real-data workload automatically.

Do not run benchmarks.

If a test requires unavailable source archives, report that prerequisite rather than silently substituting another archive.

---

# 23. Scope Restrictions

## Authorized

This task authorizes:

- Typed source-error classification.
- Correct handling of fatal source-read errors.
- Correct handling of recoverable malformed records.
- Bounded decoder failure behavior.
- Minimal source-reader API changes.
- Applicable timestamp-regression detection.
- Per-stream timestamp tracking.
- Integration with existing integrity observations.
- Ordering-contract validation.
- Safe depth handling of rejected ordering violations.
- Correct failure propagation.
- Focused synthetic tests.
- Minimal documentation updates.

## Not Authorized

Do not implement:

- New normalization profiles.
- A new batch JSON schema.
- A new runtime configuration system.
- Broad integrity observation-accounting changes from Task 04.
- A new source-decoding framework.
- Generic cross-archive sorting.
- Cross-archive merging.
- Stream-oriented Parquet consolidation.
- Long-lived shared output writers.
- New depth reconstruction semantics.
- Dataset manifest redesign.
- CLI execution-result redesign.
- Benchmark infrastructure.
- PostgreSQL registry integration.
- Python Forge changes.

### Important Boundary

Correcting timestamp-ordering validation is not authorization to change how the engine globally orders or merges its output.

The storage and stream-ordering refactors belong to Tasks 10 and 11.

If those future changes are necessary to enforce an advertised guarantee, document the dependency and reject unsupported behavior rather than implementing the storage redesign now.

---

# 24. Working-Tree Safety

Inspect the current working tree before modifying files.

Preserve unrelated user changes.

Do not revert previous task implementations.

Do not replace working source readers merely because their APIs differ from the conceptual examples.

Do not modify production integrity thresholds.

Do not rewrite canonical schemas.

Do not create commits or branches unless requested.

Prefer focused, reviewable changes.

---

# 25. Required Implementation Report

When Task 05 is complete, report the following.

## Confirmed Findings

Identify which previously reported issues were present in the current repository.

Distinguish confirmed defects from already-correct behavior.

## Source Error Handling

Describe:

- How recoverable record errors are represented.
- How fatal source errors are represented.
- Which decoders changed.
- How decoder progress is guaranteed.
- How unrecoverable failures propagate.

## Ordering Contracts

Document:

- Which source formats require timestamp monotonicity.
- Which formats support sorting.
- Which paths preserve source sequence order.
- Which guarantees apply per task or stream.
- Which cross-archive guarantees remain unsupported.

## Timestamp Enforcement

Describe:

- Where comparisons occur.
- How previous timestamps are tracked.
- How stream identity is established.
- How equal timestamps are handled.
- How violations reach integrity evaluation.
- How rejected depth events affect reconstruction.

## Compatibility

Identify:

- Existing behavior preserved.
- Intentional correctness changes.
- Any changes to public error types.
- Any changes to serialized diagnostics.
- Any remaining unsupported behavior.

## Verification

List:

- Formatting checks.
- Compilation checks.
- Focused tests executed.
- Existing tests executed.
- Tests passed.
- Tests failed.
- Tests blocked by prerequisites.

Do not claim that full archive-backed normalization was verified unless it was actually executed.

## Deferred Work

Identify any issues that require later tasks.

Do not implement them now.

---

# 26. Acceptance Criteria

Task 05 is complete when:

- Recoverable record errors are distinguishable from fatal source errors.
- JSONL reader failures cannot be silently treated as ordinary malformed records.
- Persistent source-read failures terminate deterministically.
- Fatal source errors cannot produce successful dataset commits.
- Recoverable record errors obey the configured integrity policy.
- Decoder recovery is allowed only with a known safe continuation boundary.
- Relevant CSV, ZIP, GZIP, TAR/GZIP, and XLSX error paths are reviewed and corrected where necessary.
- Existing source-error diagnostics remain meaningful.
- Supported timestamp-ordering contracts are explicitly identified.
- Applicable timestamp regressions are actually detected.
- Timestamp comparisons use the correct logical stream.
- Equal timestamps are not incorrectly rejected.
- Valid independently sortable sources retain their supported behavior.
- Stateful depth updates are not reordered merely to satisfy timestamps.
- Depth synchronization remains safe after rejected state-changing events.
- Timestamp-regression observations use the integrity infrastructure established by Task 04.
- Existing integrity-policy semantics remain unchanged.
- Unsupported ordering guarantees are rejected or explicitly reported rather than silently claimed.
- Source and task identities remain traceable.
- Processing and integrity metrics remain consistent.
- Failed jobs do not publish committed-success manifests.
- Existing canonical schemas remain unchanged.
- No stream-oriented writer consolidation is introduced.
- Focused synthetic and relevant regression tests pass.
- No performance benchmarks or large historical workloads are executed.

---

## 27. Stop Condition

**Stop after Task 05 is complete.**

Do not automatically proceed to Task 06.

Do not reorganize the complete historical normalization test suite.

Do not redesign Parquet storage.

Do not modify Python Forge.

The next assignment is:

**Task 06 — Exchange-Profile Tests and Historical Test-Suite Migration.**

Task 06 will use the production loading, test infrastructure, integrity accounting, and error-handling guarantees established by Tasks 01–05.

It will migrate exchange-specific tests, preserve useful existing assertions, and remove obsolete normalization test registration without losing coverage.

---

**Final objective:** Ensure that fatal source errors cannot masquerade as recoverable malformed records, and that applicable timestamp-ordering guarantees are actually enforced without corrupting source semantics or depth reconstruction.