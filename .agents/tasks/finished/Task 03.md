# Task 03 — Shared Normalization Test Infrastructure and Persistent Outputs

**Project:** MarketForge  
**Component:** Rust Historical Normalization Engine  
**Priority:** Foundation / Testing Infrastructure  
**Status:** Approved for implementation  
**Dependencies:** Task 01 — Shared Historical Runtime Configuration; Task 02 — Production V3 Batch Loading and Unified Normalization Profiles  
**Next task:** Integrity Observation Accounting and Time-Window Attribution

## 1. Objective

Refactor the historical normalization testing infrastructure to provide a reproducible, maintainable, and safe foundation for all subsequent normalization tests.

The central requirements are:

- One authoritative real-data normalization workload.
- Production batch loading and profile resolution.
- Deterministic selection of individual test cases.
- Shared runtime configuration and resource overrides.
- Complete isolation from production output directories.
- Persistent generated test artifacts.
- No automatic deletion of normalization test outputs.
- Reusable test helpers without duplicating production logic.

**The goal is to establish shared testing infrastructure, not to rewrite the complete historical test suite.**

The full exchange-specific test migration belongs to Task 06.

This task should establish the infrastructure and demonstrate it using one representative trade case and one representative depth case.

Complete only Task 03.

---

## 2. Architectural Context

Tasks 01 and 02 establish the following production architecture:

```text
Prepared Normalization Batch V3
             │
             ▼
     Production Batch Loader
             │
             ▼
    Unified TOML Profiles
             │
             ▼
    Resolved Internal Jobs
             │
             ▼
     Normalization Executor
             │
             ▼
     Canonical Output
```

The test suite must exercise this production path.

Do not create an alternative normalization implementation for testing.

### Intended Test Architecture

```text
Authoritative Test Workload
          │
          ▼
    Shared Fixture Loader
          │
          ▼
   Deterministic Selection
          │
          ▼
  Production Batch Resolution
          │
          ▼
   Safe Runtime Overrides
          │
          ▼
   Isolated Test Execution
          │
          ▼
  Persistent Test Artifacts
          │
          ▼
    Independent Assertions
```

The shared testing infrastructure should be reusable by:

- Exchange-specific profile tests.
- Trade normalization tests.
- Depth normalization tests.
- Execution and scheduling tests.
- Persistence tests.
- Resource-management tests.
- Integrity tests.
- Future stream-oriented output tests.

Do not force every type of test to use this infrastructure.

Pure unit tests should remain lightweight and independent.

---

# 3. Initial Repository Inspection

Before making changes, inspect the existing historical normalization test suite.

Relevant locations include:

```text
engine/tests/
engine/tests/historical/
engine/tests/historical/normalize/
engine/tests/historical/fixtures/
```

Also inspect:

```text
engine/Cargo.toml

engine/src/historical/config/
engine/src/historical/normalize/
engine/src/historical/normalize/session.rs
engine/src/historical/normalize/executor.rs
```

Use actual repository paths where they differ.

### Identify Existing Infrastructure

Determine:

- How historical integration tests are registered.
- How exchange-specific tests are organized.
- How existing tests load source archives.
- How tests construct normalization jobs.
- How tests resolve normalization profiles.
- How tests supply runtime resources.
- Which tests depend on legacy processing jobs.
- Which tests use temporary output directories.
- Which tests automatically delete artifacts.
- Which tests modify global environment variables.
- Which tests depend on the current working directory.
- Which tests already contain reusable assertions.

Pay particular attention to legacy dependencies such as:

```text
data/.jobs/process/
data/.jobs/processing.json
data/.config/historical/normalize.toml
```

Do not automatically remove every legacy test during this task.

Record dependencies that will be addressed during Task 06.

### Preserve Existing Coverage

Do not delete existing normalization tests merely because they use outdated setup.

Do not reorganize the entire suite yet.

Introduce the new shared support alongside the existing tests where necessary.

Only migrate the representative cases needed to establish the new infrastructure.

---

# 4. Authoritative Real-Data Workload

Use exactly one maintained workload document for archive-backed normalization tests:

```text
engine/tests/historical/fixtures/normalize.json
```

This is the authoritative test workload location.

**Do not introduce a second maintained file named `jobs.json`.**

Use `normalize.json` consistently in new code and documentation.

### Input Contract

The workload must use the production schema-version-3 prepared normalization batch:

```json
{
  "schema_version": 3,
  "kind": "normalization_batch",
  "status": "prepared"
}
```

The example above is schematic, not a complete executable batch.

The actual document must follow the complete production contract established by Task 02.

### Workload Responsibilities

The fixture describes:

- Prepared jobs.
- Source tasks.
- Source archive locations.
- Exchange identities.
- Instrument types.
- Market categories.
- Instruments or family scopes.
- Source feeds where applicable.
- Stream identities.
- Legitimate batch metadata.
- Output destinations.

Do not add testing-specific fields to the production batch schema.

Examples of prohibited additions:

```text
test_name
expected_price
expected_quantity
expected_rows
test_case
sampling_limit
test_workers
test_memory_budget
```

These belong in Rust test definitions or shared test-support structures.

### Important Distinction

**One authoritative workload does not mean one enormous test.**

Different tests select different tasks or job subsets from the same workload.

For example:

```text
normalize.json
    │
    ├── Binance trade cases
    ├── Binance depth cases
    ├── Bitget trade cases
    ├── Bitget depth cases
    ├── Bybit trade cases
    ├── Bybit depth cases
    ├── Gate.io cases
    ├── OKX cases
    └── Multi-job execution cases
```

The complete workload should only execute when a test explicitly requests it.

Do not automatically normalize every archive when testing one profile.

### Existing Fixture Handling

Inspect whether `normalize.json` already exists.

If it exists, validate it using the production loader.

Do not replace user-maintained archive paths, IDs, or metadata without justification.

If it does not exist, do not fabricate real archive identities or claim an invented workload represents downloaded market data.

Establish the expected location and report the missing preparation prerequisite.

Small synthetic documents may be created independently for source-only tests.

---

# 5. Shared Test Support

Introduce reusable support under:

```text
engine/tests/historical/support/
```

Suggested organization:

```text
engine/tests/historical/
├── fixtures/
│   └── normalize.json
│
├── support/
│   ├── mod.rs
│   ├── fixtures.rs
│   ├── runtime.rs
│   └── assertions.rs
│
└── normalize/
    └── ...
```

This organization is illustrative.

Adapt it to the actual Cargo test-target structure.

Avoid creating empty modules.

### Shared Responsibilities

#### `fixtures.rs`

Responsible for:

- Loading the authoritative workload.
- Validating the production batch.
- Selecting tasks and jobs.
- Reporting missing or ambiguous selections.
- Preparing isolated execution subsets.
- Preserving source and profile provenance.

#### `runtime.rs`

Responsible for:

- Loading production runtime configuration.
- Applying deterministic test resource overrides.
- Creating isolated persistent output directories.
- Redirecting writable paths.
- Managing test execution metadata.
- Providing safe execution helpers.

#### `assertions.rs`

Responsible for reusable checks involving:

- Execution status.
- Dataset manifests.
- File inventories.
- Row counts.
- Source-task relationships.
- Output paths.
- Integrity status.
- Persisted artifacts.

These helpers must not become a second implementation of normalization.

### Keep Abstractions Minimal

Do not create a generic testing framework.

Prefer straightforward Rust helpers with explicit arguments and clear error messages.

Avoid complex test registries, unnecessary traits, and global mutable state.

---

# 6. Deterministic Test Selection

The shared fixture loader must support selecting the exact workload required by a test.

Selection must use structured source identity.

Relevant criteria include:

```text
exchange
instrument_type
market_category
content_type
symbol
family scope
source feed
archive member
source period
```

Use criteria supported by the actual production metadata.

### Example

A test requesting:

```text
exchange = bybit
instrument_type = perpetual
market_category = linear
content_type = depth
symbol = BTCUSDT
```

must select the corresponding intended source tasks.

Do not select an arbitrary task simply because it appears first.

### Prohibited Selection Methods

Do not use:

```rust
jobs[0]
tasks[0]
```

as a general-purpose test-selection mechanism.

Do not select:

- The first filesystem entry.
- The first matching archive.
- The first available instrument.
- An arbitrary task sharing an exchange name.
- A generated database identifier as the sole portable selector.

Do not silently substitute another source if the intended archive is missing.

### Selection Validation

If a test requires exactly one match:

```text
0 matches -> error
1 match   -> accepted
2+ matches -> ambiguity error
```

If a test requires several sources, validate the exact intended selection.

For example, a two-archive continuity test must not accidentally select three archives.

### Error Diagnostics

A failed selection should identify:

- Test name.
- Requested selection criteria.
- Number of matching tasks.
- Relevant matching identities.
- Missing required source information.

Do not report successful execution for an empty selection.

---

# 7. Preserve Production Batch Loading

Selected test workloads must pass through the actual production normalization loader.

Use:

```text
Selected task or job subset
             │
             ▼
   Valid V3 Batch Representation
             │
             ▼
     Production Validation
             │
             ▼
      Profile Resolution
             │
             ▼
     Resolved Internal Jobs
             │
             ▼
     Production Execution
```

Do not manually inject:

```text
quantity encoding
timestamp encoding
normalization mappings
sequence policy
raw-schema interpretation
depth event semantics
```

when production is responsible for resolving those settings.

A test may assert that a resolved setting is correct.

It must not provide that value to the processor and then claim that profile resolution was verified.

### Preparing Subsets

A selected test may need to execute only part of the authoritative batch.

Provide a reusable helper that creates a valid isolated subset.

Preserve relevant:

- Batch metadata.
- Job identities.
- Dataset identities.
- Task identities.
- Ordering declarations.
- Stream ranks.
- Instrument metadata.
- Source options.

Keep the subset internally consistent.

Do not modify the authoritative fixture on disk.

If required, serialize the selected subset into the test's own workspace and load it through the production parser.

Do not maintain separate permanent JSON files for each subset.

---

# 8. Production Runtime Configuration

Use the shared runtime configuration established by Task 01.

The authoritative file is:

```text
data/.config/historical/general.toml
```

Load configuration through the production shared configuration loader.

Do not introduce a second test-specific TOML parser.

### Test Resource Overrides

The test infrastructure must support deterministic resource overrides.

Examples include:

```text
workers
memory_budget_bytes
scratch_budget_bytes
row_group_target_bytes
file_target_bytes
```

These should be applied to isolated in-memory configuration values.

Do not modify `general.toml` during tests.

### Resource Defaults

Ordinary correctness tests should use conservative resource limits.

They must not require a 128 GB RAM machine.

Do not automatically inherit large machine-specific benchmark settings.

Specialized tests may explicitly override resource values when testing resource behavior.

### Integrity Policy

Resource overrides must not silently change integrity policies.

Do not disable integrity validation merely to make test execution succeed.

A test requiring a different integrity policy must declare that requirement explicitly.

For example:

```text
Test purpose:
    Verify degraded output under an allowed error rate.

Required configuration:
    An explicitly specified degradation policy.

Expected result:
    Dataset completes with the documented degraded integrity state.
```

Do not globally relax policy thresholds.

### Scratch Path

The production configuration may contain:

```toml
scratch_path = "data/.work/normalize"
```

Tests must not use this shared production scratch destination.

Override scratch paths into each test's isolated output directory.

---

# 9. Persistent Test Output Root

**All artifact-producing historical normalization integration tests must retain their outputs under:**

```text
data/tests/historical/normalize/
```

This is mandatory.

Do not use automatically deleted temporary directories for normalization integration tests that generate datasets.

### Recommended Directory Structure

```text
data/tests/historical/normalize/
│
├── exchanges/
│   ├── binance/
│   │   └── <test-case>/
│   │       └── <run-id>/
│   │
│   ├── bitget/
│   │   └── <test-case>/
│   │       └── <run-id>/
│   │
│   ├── bybit/
│   │   └── <test-case>/
│   │       └── <run-id>/
│   │
│   ├── gateio/
│   │   └── <test-case>/
│   │       └── <run-id>/
│   │
│   └── okx/
│       └── <test-case>/
│           └── <run-id>/
│
├── execution/
├── depth/
├── trades/
├── parquet/
└── resources/
```

These directories should be created when needed.

Do not create large numbers of empty directories.

### Example Test Execution

```text
data/tests/historical/normalize/
└── exchanges/
    └── bybit/
        └── perpetual_linear_depth/
            └── 20261106-abcdef123456/
                │
                ├── test.json
                ├── selected-batch.json
                │
                ├── dataset/
                │   ├── manifest.json
                │   ├── processing-job.json
                │   ├── processing-job.sha256
                │   └── tasks/
                │       └── ...
                │
                ├── staging/
                ├── scratch/
                └── reports/
```

This is an organizational example.

Preserve the actual production dataset structure inside the dataset directory.

Do not rename canonical output artifacts merely to simplify testing.

---

# 10. Output Isolation

Every test execution must have a unique writable workspace.

The test harness must redirect all writable paths before executing normalization.

### Required Writable Paths

At minimum, isolate:

```text
dataset_path
staging_path
scratch_path
report paths
temporary input variants
other generated execution artifacts
```

### Critical Safety Requirement

The authoritative batch may contain production output paths.

**Tests must never write to those original destinations.**

The shared test helper must override output paths consistently.

Do not require individual test authors to remember every writable field.

Centralize the safety mechanism.

### Multiple Jobs

A test may execute several jobs.

Each job must retain its distinct staging and final dataset identity.

Do not redirect all jobs into the same dataset directory.

Conceptually:

```text
<test-run>/
├── datasets/
│   ├── <dataset-id-A>/
│   └── <dataset-id-B>/
│
├── staging/
│   ├── <dataset-id-A>/
│   └── <dataset-id-B>/
│
└── scratch/
```

Use stable dataset identities where appropriate.

Do not allow directory collisions.

### Source Archives

Original source archives must remain read-only.

Do not:

- Modify source ZIP files.
- Modify source CSV files.
- Modify source JSONL files.
- Rewrite original XLSX archives.
- Delete downloaded source data.
- Move source archives into the test-output directory.

Temporary synthetic input variants may be created inside the test workspace.

### Path Safety

Verify that the resolved output paths are descendants of the designated test-run directory.

Reject paths that escape through relative traversal or invalid redirection.

Do not rely on string-prefix matching alone for path safety.

Do not modify process-global working-directory state.

---

# 11. Unique Test Execution Identity

Every artifact-producing test execution must receive a unique run directory.

The same test may execute repeatedly.

All executions must remain inspectable.

### Requirements

- Never overwrite an existing run directory.
- Never delete previous runs automatically.
- Support concurrently executing tests.
- Support separate test processes.
- Prevent directory-name collisions.
- Preserve readable test-case names.

A timestamp may be included in the directory name, but it must not be the only uniqueness guarantee.

Use an appropriate unique identifier and exclusive directory creation.

Do not depend solely on:

```text
test name
process ID
current timestamp in seconds
```

### Deterministic Assertions

The filesystem execution identifier is only for isolation.

Do not make canonical correctness assertions depend on that identifier.

For example, two executions with different output directories should produce equivalent canonical results when the workload and effective settings are equivalent.

---

# 12. Persistent Test Metadata

Every artifact-producing test execution should contain:

```text
test.json
```

This file describes the test execution and its artifacts.

It is testing metadata, not part of the production normalization protocol.

### Recommended Metadata

```json
{
  "schema_version": 1,
  "test_name": "bybit_perpetual_linear_depth",
  "run_id": "20261106-abcdef123456",
  "status": "passed",
  "profile_id": "bybit.perpetual.linear.depth",
  "job_ids": [1001],
  "task_ids": [1, 2],
  "dataset_paths": ["dataset"],
  "staging_paths": ["staging"],
  "failure": null
}
```

This is an illustrative document.

The implementation may include additional useful information.

### Required Information

Where applicable, record:

- Test name.
- Unique execution ID.
- Start timestamp.
- Completion timestamp.
- Execution status.
- Selected source tasks.
- Job IDs.
- Dataset IDs.
- Profile IDs.
- Profile fingerprints.
- Source archive identities.
- Effective runtime configuration.
- Dataset paths.
- Staging paths.
- Report paths.
- Integrity result.
- Failure reason.

Use paths relative to the test-run directory where practical.

### Test Status

Distinguish:

```text
running
passed
failed
```

A test that terminates before writing its final status must not be mistaken for a passing test.

An unfinished `running` state may remain if the process crashes or is interrupted.

Do not claim that the metadata can always be finalized after process termination.

### Status Recording

Create the initial metadata before executing the workload.

After execution and assertions, write the final result.

Where practical, use atomic metadata-file replacement.

Do not let failure to serialize optional metadata overwrite or destroy the production artifacts being inspected.

Preserve sufficient diagnostics even when a test assertion fails.

---

# 13. No Automatic Cleanup

**Persistent normalization test outputs must never be deleted automatically.**

This includes successful and failed executions.

### Prohibited Behavior

Do not:

- Delete generated datasets after assertions.
- Remove test outputs in `Drop` implementations.
- Clear the test-output root before execution.
- Delete previous run directories.
- Remove failed staging directories.
- Overwrite an older successful test execution.
- Delete generated Parquet files to reduce disk usage.
- Use automatic temporary-directory cleanup for persistent integration-test artifacts.

The user will manage retained artifacts manually.

### Failure Preservation

A failed test may leave:

```text
staging/
failure.json
partial Parquet files
partial reports
```

Preserve these where available.

Do not weaken production transactional behavior to preserve them.

A failed dataset must not be represented as committed.

Do not rename failed staging into the successful dataset destination.

### Unit-Test Exception

Pure unit tests may continue using temporary directories and automatic cleanup.

Examples include:

- TOML parser validation.
- Invalid schema-version tests.
- Decimal conversion tests.
- Timestamp parsing tests.
- Small isolated counter tests.
- Deliberately malformed configuration documents.

The persistent-output requirement applies to tests generating meaningful normalization execution artifacts.

Do not migrate every unit test into persistent storage.

---

# 14. Reusable Assertions

Implement a small set of shared assertions for successful and failed normalization executions.

Do not create a second data-validation engine.

### Execution Status

Verify:

- Expected execution status.
- Job identity.
- Dataset identity.
- Failure information where applicable.

### Manifest Validation

Verify:

- Manifest exists for committed datasets.
- Manifest job ID matches the selected job.
- Manifest dataset ID matches the selected dataset.
- Source-task identities are consistent.
- File count matches the file inventory.
- Referenced Parquet files exist.
- File sizes match recorded metadata.
- Canonical row totals are consistent.
- Timestamp boundaries are valid.
- Integrity status matches the expected execution outcome.

### Output Path Validation

Verify:

- All writable outputs remain under the test-run root.
- No production dataset destination was written.
- No output was overwritten.
- Staging and committed output paths remain distinct.

### Depth Artifact Validation

For depth outputs, verify the presence and valid references of applicable:

```text
events.parquet
segments.json
boundary.json
```

Do not implement a full depth replay framework in this task.

Preserve useful existing replay assertions for Task 06.

### Independent Semantic Assertions

Shared helpers may provide convenient methods for inspecting persisted data.

However, profile-specific expected prices, quantities, book states, and source-record facts should remain in the corresponding test definitions.

Do not automatically derive expected results from the normalizer being tested.

---

# 15. Representative Trade Test

Demonstrate the shared infrastructure with one representative trade normalization case.

Select an actually supported trade profile from the available production catalog.

Prefer a source already used by an existing integration test.

### Required Execution Path

```text
normalize.json
      │
      ▼
Deterministic trade selection
      │
      ▼
Production batch loader
      │
      ▼
Production profile resolution
      │
      ▼
Isolated normalization execution
      │
      ▼
Persistent Parquet dataset
      │
      ▼
Manifest and correctness assertions
```

### Required Checks

Verify:

- Correct task selection.
- Correct resolved profile.
- Successful normalization.
- Correct output isolation.
- Persistent dataset generation.
- Manifest consistency.
- Expected trade record counts or representative values where independent source evidence is already available.

Do not invent expected values.

Do not weaken existing trade assertions.

### Missing Source Archive

If the test is explicitly selected but its archive is missing, fail with an actionable prerequisite error.

Do not silently select another archive.

Do not report a successful empty test.

---

# 16. Representative Depth Test

Demonstrate the same infrastructure with one representative depth normalization case.

Select an actually supported depth profile.

Prefer an existing fixture with a valid initialization sequence.

### Required Checks

Verify:

- Correct task selection.
- Correct resolved depth profile.
- Successful production execution.
- Persistent canonical depth output.
- Presence of required depth auxiliary artifacts.
- Manifest consistency.
- Correct source-task attribution.
- Safe output isolation.

Where an independently reviewed expectation already exists, preserve it.

Do not invent expected order-book levels.

### Stateful Sampling

Do not create an arbitrary middle-of-file sample that lacks required initialization.

A depth sample must preserve enough source events to establish the initial book and interpret subsequent changes.

Do not truncate compressed archive bytes to manufacture a sample.

Do not silently change sequence policy to make the sample succeed.

---

# 17. Test Registration and Execution Selection

The existing historical test suite may contain a normalization-specific `slow/` hierarchy.

Its complete removal belongs to Task 06.

For now, establish a reliable way to execute the new shared infrastructure tests.

### Required Selection Categories

The testing infrastructure must support or prepare for:

```text
Source-only unit/configuration tests.
One selected normalization profile.
One selected exchange.
Complete prepared real-data workload.
```

Only the first two representative profile executions need to be migrated during Task 03.

### Real-Data Prerequisites

Ordinary source-only tests must not require downloaded archives.

Real-data tests must be explicitly selectable through the repository's supported test mechanisms.

Inspect the existing Cargo features, test targets, and execution scripts.

Use the simplest appropriate selection mechanism.

Do not create duplicate test registration.

Do not introduce blanket ignores that conceal broken tests.

A real-data test that was not selected is not evidence of a pass.

A real-data test that was explicitly selected but lacks required source data must fail clearly.

### No Automatic Complete-Workload Execution

Do not run every source archive merely to verify the shared testing infrastructure.

Large workloads must remain explicitly selected.

Do not execute performance benchmarks.

---

# 18. Concurrency and Resource Safety

Multiple tests may execute concurrently.

The shared infrastructure must prevent output collisions.

### Required Behavior

- Each test owns its run directory.
- Each test owns its staging paths.
- Each test owns its scratch directory.
- No test modifies another test's outputs.
- No test depends on previously generated results.
- No test requires execution in a specific order.

### Resource Concurrency

Avoid uncontrolled multiplication of worker counts.

For example:

```text
8 concurrent tests
×
16 normalization workers each
=
128 possible workers
```

Do not make ordinary correctness tests depend on unrestricted machine resources.

Use conservative test defaults.

Preserve explicit overrides for tests that genuinely require different resource settings.

Do not introduce a complex global test scheduler unless the existing test runner requires one.

---

# 19. Repository Hygiene

Ensure the persistent test-output directory is ignored by Git.

Add the following rule if it is not already covered:

```gitignore
/data/tests/historical/normalize/
```

Do not ignore:

```text
engine/tests/historical/fixtures/normalize.json
```

Do not ignore Rust test source files.

Do not commit generated datasets, Parquet files, execution reports, or scratch artifacts.

Do not modify unrelated ignore rules.

---

# 20. Scope Restrictions

## Authorized

This task authorizes:

- Shared normalization test-support modules.
- Production workload loading for tests.
- Deterministic case selection.
- Valid isolated batch subsets.
- Shared runtime resource overrides.
- Unique persistent test directories.
- Safe output-path redirection.
- Persistent test metadata.
- Reusable manifest assertions.
- One representative trade case.
- One representative depth case.
- Focused tests for the shared infrastructure.
- Necessary Cargo test-target wiring.
- Minimal documentation updates.

## Not Authorized

Do not implement:

- The complete exchange-profile test migration.
- Removal of the entire normalization `slow/` hierarchy.
- New normalization algorithms.
- Integrity observation-accounting corrections.
- Timestamp-regression detection changes.
- Source-read error-handling redesign.
- CLI JSON response redesign.
- Dataset manifest redesign.
- Stream-oriented Parquet consolidation.
- Benchmark infrastructure.
- Merge or conversion refactoring.
- PostgreSQL registry integration.
- Python Forge changes.

Do not rewrite unrelated unit tests.

Do not modify canonical schemas.

Do not change production output layout.

Do not silently relax integrity policies.

---

# 21. Verification Requirements

After implementation, verify the shared test infrastructure using lightweight checks.

### Formatting

Run:

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

### Compilation

Run appropriate Cargo checks using the repository's actual features and test targets.

For example:

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Also compile affected test targets.

### Source-Only Tests

Run focused tests for:

- Fixture selection.
- Missing selection.
- Ambiguous selection.
- Batch subset construction.
- Output-path redirection.
- Unique run-directory generation.
- Runtime resource overrides.
- Manifest assertion helpers.
- Persistent metadata serialization.

These tests must not require real historical archives.

### Representative Real-Data Tests

If the required prepared fixture and source archives are available, run the explicitly selected representative trade and depth cases.

Do not run the complete workload.

If real-data prerequisites are missing:

- Report the exact missing path or preparation requirement.
- Do not substitute another archive.
- Do not claim that the representative execution passed.
- Do not block unrelated source-only tests.

### Persistent Artifact Verification

Verify that:

1. An artifact-producing test writes under `data/tests/historical/normalize/`.
2. The run directory remains after test completion.
3. The dataset remains after successful execution.
4. The output contains the expected production artifacts.
5. Failed execution artifacts remain available where generated.
6. Repeating a test creates another directory.
7. Concurrent runs cannot overwrite one another.
8. The original fixture is unchanged.
9. The original source archives are unchanged.
10. No production dataset path is modified.

Do not execute benchmarks.

---

# 22. Implementation Sequence

## Phase A — Inventory

Inspect existing test registration, fixture loading, runtime setup, and output cleanup.

Identify useful helpers that can be reused.

## Phase B — Shared Fixture Support

Implement production batch loading and deterministic task selection.

Ensure selected subsets remain valid.

## Phase C — Runtime Isolation

Implement conservative runtime overrides and safe redirection of writable paths.

Create unique persistent run directories.

## Phase D — Artifact Management

Implement persistent test metadata and reusable output assertions.

Do not implement automatic cleanup.

## Phase E — Representative Cases

Migrate one trade case and one depth case to the new shared infrastructure.

Preserve meaningful existing assertions.

## Phase F — Verification

Run formatting, compilation, and focused source-only tests.

Run representative archive-backed cases only when their declared prerequisites are available.

Document the verified execution commands.

Stop after completing this task.

---

# 23. Required Implementation Report

When finished, provide a report containing the following.

## Shared Infrastructure

Describe:

- New support modules.
- Existing helpers reused.
- Production fixture-loading interface.
- Task-selection mechanism.
- Runtime override mechanism.
- Output isolation mechanism.

## Persistent Outputs

Describe:

- Final output directory structure.
- Unique execution-ID generation.
- Persisted metadata.
- Retained artifact types.
- Failure preservation.
- Git ignore changes.

## Representative Cases

Identify:

- Selected trade profile.
- Selected depth profile.
- Source archives used.
- Assertions preserved.
- Actual execution status.

Do not claim a test passed if it was not executed.

## Tests

List:

- Formatting checks.
- Compilation checks.
- Source-only tests executed.
- Real-data tests executed.
- Tests passed.
- Tests failed.
- Tests blocked by missing prerequisites.

## Remaining Work

Identify the existing tests that still require migration under Task 06.

Do not migrate them now.

Report any production loader or profile issues that block testing.

Do not hide those issues through test-only workarounds.

---

# 24. Acceptance Criteria

Task 03 is complete when:

- One maintained real-data workload location is established at `engine/tests/historical/fixtures/normalize.json`.
- The test infrastructure uses the production schema-v3 batch loader.
- Production TOML profile resolution is exercised.
- Tests can select exact tasks using structured criteria.
- Missing or ambiguous selections produce clear errors.
- The authoritative fixture is never modified during execution.
- Selected subsets preserve valid production metadata.
- Runtime resource overrides are applied in memory.
- Integrity policies are not silently weakened.
- Every artifact-producing integration test uses an isolated output directory.
- All normalization integration-test artifacts are retained under `data/tests/historical/normalize/`.
- Automatic deletion of persistent test outputs is disabled.
- Failed staging and diagnostic artifacts are preserved where available.
- Repeated and concurrent executions cannot overwrite previous runs.
- Source archives remain read-only.
- Production dataset paths are never written by tests.
- Test metadata identifies each execution and its outcome.
- Reusable manifest and output assertions are available.
- One representative trade case uses the new infrastructure.
- One representative depth case uses the new infrastructure.
- Source-only tests remain independent of real-data archives.
- Focused compilation and infrastructure tests pass.
- No complete real-data workload or performance benchmark is executed.
- Existing unrelated test coverage is preserved.

---

## 25. Stop Condition

**Stop after Task 03 is complete.**

Do not automatically proceed to Task 04.

Do not migrate all exchange-specific tests.

Do not remove the full normalization `slow/` hierarchy.

Do not modify normalization algorithms merely to make the new test infrastructure pass.

The next task will address:

**Task 04 — Integrity Observation Accounting and Time-Window Attribution.**

Task 04 will use the testing foundation established here to verify integrity denominators, successful and failed observations, and time-window attribution.

---

**Final objective:** Establish one reliable and reusable normalization testing foundation that exercises production batch loading and profile resolution, isolates every execution, preserves generated artifacts for inspection, and supports future correctness and integration testing without duplicating the normalization engine.