# Task 06 — Exchange-Profile Tests and Historical Test-Suite Migration

**Project:** MarketForge  
**Component:** Rust Historical Normalization Engine  
**Priority:** High / Test Coverage and Reliability  
**Status:** Approved for implementation  
**Dependencies:** Tasks 01–05  
**Next task:** CLI Results, Dataset Manifests, and Forge Integration Contract

## 1. Objective

Complete the migration and reorganization of the historical normalization test suite.

The previous tasks established:

- Shared historical runtime configuration.
- Production schema-version-3 batch loading.
- Unified self-contained normalization profiles.
- Shared normalization test infrastructure.
- Persistent test-output storage.
- Correct integrity observation accounting.
- Correct source-error handling.
- Timestamp-ordering enforcement.

This task must bring the existing normalization tests into alignment with that architecture.

The primary goals are:

1. Organize exchange-specific tests into clearly identifiable profile cases.
2. Exercise the actual production batch loader and profile resolver.
3. Replace legacy test fixtures and configuration dependencies.
4. Preserve useful existing assertions and coverage.
5. Establish independently meaningful source-to-canonical correctness checks.
6. Organize generic normalization tests by responsibility.
7. Remove the obsolete normalization-specific `slow/` hierarchy.
8. Ensure every test is registered exactly once.
9. Preserve generated integration-test artifacts for manual inspection.
10. Make missing profile coverage and missing real-data prerequisites explicit.

**The objective is not simply to make tests pass. It is to establish reliable evidence that the normalization engine interprets supported source formats correctly.**

Complete only Task 06.

---

## 2. Architectural Context

The authoritative normalization test workflow is:

```text
engine/tests/historical/fixtures/normalize.json
                        │
                        ▼
             Shared Fixture Loader
                        │
                        ▼
           Deterministic Case Selection
                        │
                        ▼
             Production V3 Loader
                        │
                        ▼
          Production Profile Resolution
                        │
                        ▼
             Normalization Executor
                        │
                        ▼
               Canonical Output
                        │
               ┌────────┴─────────┐
               │                  │
               ▼                  ▼
       Independent Checks   Persistent Artifacts
               │                  │
               ▼                  ▼
        Test Result       data/tests/historical/
                               normalize/
```

All archive-backed normalization tests must use this production path.

Do not construct manually resolved normalization configurations to bypass profile loading.

Do not implement an alternative test-only normalization engine.

### Important Distinction

Three different guarantees must remain separate:

**Profile validity:** The TOML is structurally valid and resolves correctly.

**Source correctness:** The resolved profile interprets real source records correctly.

**Persistence correctness:** The generated canonical output and metadata accurately preserve the interpreted records.

A successfully loaded TOML is not proof that its source data is normalized correctly.

A nonempty Parquet file is not proof of source correctness.

A dataset passing its own internal accounting checks is not necessarily evidence that its prices, quantities, or book states are economically correct.

Tests must provide meaningful evidence for the guarantee they claim to verify.

---

# 3. Inspect Existing Test Coverage Before Refactoring

Before modifying the tests, inspect the complete historical normalization test structure.

Relevant locations include:

```text
engine/tests/
engine/tests/historical/
engine/tests/historical/normalize/
engine/tests/historical/fixtures/
engine/tests/historical/support/
```

Inspect the actual repository structure.

Also inspect:

```text
engine/Cargo.toml
engine/src/historical/normalize/
engine/src/historical/config/
engine/src/shared/formats/
engine/src/shared/persistence/
```

### Establish a Coverage Baseline

Identify:

- Existing test targets.
- Existing test modules.
- Exchange-specific tests.
- Generic execution tests.
- Integrity tests.
- Trade normalization tests.
- Depth normalization tests.
- Parquet persistence tests.
- Resource-management tests.
- Depth reconstruction and replay tests.
- Tests registered through the normalization `slow/` wrapper.
- Tests using legacy processing jobs.
- Tests constructing normalization rules manually.
- Tests requiring real downloaded archives.
- Tests using synthetic source fixtures.

Record which tests are currently registered.

Identify tests that are duplicated through different entry points.

Identify tests that are not registered at all.

### Preserve Existing Coverage

Do not delete a test simply because its implementation is outdated.

Before removing or replacing a test, identify the behavior it verifies.

Ensure equivalent or stronger coverage remains.

For example, an old depth test may contain valuable assertions about:

```text
Snapshot initialization.
Sequence gaps.
Order-book recovery.
Source-event ordinals.
Reconstruction segments.
Boundary metadata.
```

Those assertions must not disappear merely because the test's legacy fixture loader is removed.

### Baseline Verification

Run lightweight source-only tests where practical.

Do not automatically run the complete real-data normalization workload.

Record existing failures separately from regressions introduced during this task.

If the baseline cannot be established because the existing suite no longer compiles, document the blocker and proceed with the smallest necessary corrections.

---

# 4. Authoritative Real-Data Workload

All archive-backed normalization tests must use:

```text
engine/tests/historical/fixtures/normalize.json
```

This is the single maintained real-data normalization workload.

**Do not introduce another maintained workload named `jobs.json`.**

Use `normalize.json` consistently.

### Input Format

The document uses the production schema-version-3 normalization batch:

```text
schema_version = 3
kind = normalization_batch
status = prepared
```

It contains multiple jobs and source tasks.

The batch is generated or maintained through the established Forge preparation workflow.

### Requirements

- Load the document using the production batch loader.
- Validate its structure.
- Resolve source profiles through the production catalog.
- Preserve task identities.
- Preserve stream identities.
- Preserve instrument metadata.
- Preserve legitimate source-specific options.
- Keep source archives read-only.
- Never modify the authoritative workload during a test.

### One Workload, Many Tests

A profile case selects the relevant tasks from the shared workload.

For example:

```text
normalize.json
      │
      ├── Binance spot trades
      ├── Binance perpetual trades
      ├── Binance depth
      │
      ├── Bybit spot trades
      ├── Bybit perpetual trades
      ├── Bybit depth
      │
      ├── Bitget trades
      ├── Bitget depth
      │
      ├── Gate.io trades
      ├── Gate.io depth
      │
      └── OKX trades and depth
```

This is illustrative.

Derive actual test cases from the supported production profile catalog and available fixture tasks.

Do not invent unsupported profile combinations.

Do not execute every task when testing one profile.

### Missing Fixture

If `normalize.json` does not exist, do not fabricate real source identities or archives.

Source-only tests must remain functional.

An explicitly requested archive-backed test must fail with a clear prerequisite error.

Do not report missing real-data tests as passed.

---

# 5. Final Test Directory Structure

Organize historical normalization tests by responsibility.

Use the following structure as the preferred design:

```text
engine/tests/historical/
│
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
    ├── main.rs
    ├── profiles.rs
    │
    ├── exchanges/
    │   ├── mod.rs
    │   ├── binance.rs
    │   ├── bitget.rs
    │   ├── bybit.rs
    │   ├── gateio.rs
    │   └── okx.rs
    │
    ├── execution/
    ├── depth/
    ├── trades/
    ├── parquet/
    └── resources/
```

This describes logical ownership.

Adapt module declarations and file placement to the actual Cargo integration-test structure.

Do not create empty directories or modules merely to match the illustration.

### Exchange Tests

Exchange-specific tests verify that production profiles correctly interpret their supported source formats.

### Generic Tests

Generic tests verify behavior that should hold independently of a particular exchange.

Examples include:

- Execution scheduling.
- Task isolation.
- Integrity handling.
- Resource limits.
- Parquet persistence.
- Canonical schemas.
- Depth reconstruction.
- Failure propagation.

Do not duplicate generic functionality in all five exchange files.

---

# 6. One Source File per Exchange

Create or consolidate exchange-specific normalization tests into:

```text
engine/tests/historical/normalize/exchanges/binance.rs
engine/tests/historical/normalize/exchanges/bitget.rs
engine/tests/historical/normalize/exchanges/bybit.rs
engine/tests/historical/normalize/exchanges/gateio.rs
engine/tests/historical/normalize/exchanges/okx.rs
```

Each file should contain independently identifiable profile tests.

### Example: Bybit

Illustrative test names:

```rust
#[test]
fn spot_trades_profile() {
    // ...
}

#[test]
fn perpetual_linear_trades_profile() {
    // ...
}

#[test]
fn perpetual_inverse_trades_profile() {
    // ...
}

#[test]
fn spot_depth_profile() {
    // ...
}
```

Use the profiles actually supported by the current catalog.

Do not create a test for an unsupported profile merely to complete a matrix.

### Required Organization

Each exchange file should make it clear:

- Which profiles are supported.
- Which profiles have execution tests.
- Which source archives are selected.
- Which canonical behaviors are asserted.
- Which profile behaviors remain untested.

Avoid one enormous test such as:

```rust
#[test]
fn test_entire_bybit_exchange() {
    // ...
}
```

A failure should identify the affected profile and behavior.

### Multiple Instruments

A profile may support several instruments.

Where useful source evidence exists, retain representative cases involving different:

```text
Symbols.
Quote assets.
Tick sizes.
Contract specifications.
Market categories.
Quantity scales.
```

Do not assume a profile is correct for every instrument because one BTCUSDT example passed.

However, do not duplicate entire archive-processing tests unnecessarily.

Use representative coverage with explicit scope.

---

# 7. Profile Naming and Resolution

Production profile IDs follow:

```text
exchange.instrument_type.market_category.data_type
```

For example:

```text
bitget.perpetual.linear.depth
```

The associated filename is:

```text
bitget.perpetual.linear.depth.toml
```

Tests must resolve profiles using the production catalog.

### Required Resolution Path

```text
Selected task
      │
      ▼
Production batch validation
      │
      ▼
Production profile discovery
      │
      ▼
Production profile resolution
      │
      ▼
Resolved normalization specification
      │
      ▼
Source decoding and normalization
```

Do not manually inject:

```text
Raw field mappings.
Timestamp encodings.
Quantity encodings.
Side transformations.
Sequence policies.
Snapshot/update rules.
```

A test may assert these settings after resolution.

It must not provide them as an independent test-only configuration.

### Profile Compatibility

Verify that a profile is not silently applied to an incompatible task.

Relevant dimensions include:

```text
exchange
instrument_type
market_category
data_type
source feed
source format
```

Reuse Task 02's production validation.

Do not create another independent profile resolver inside the test suite.

---

# 8. Profile Catalog Coverage

Implement or update a profile-catalog coverage check.

The purpose is to identify supported profiles that lack meaningful tests.

### Compare Four Things

```text
Production Profile Catalog
             │
             ▼
Declared Test Cases
             │
             ▼
Available Fixture Tasks
             │
             ▼
Actually Exercised Behaviors
```

These are not equivalent.

A profile can:

- Exist in the catalog.
- Have a declared test.
- Have a matching fixture.
- Still lack evidence for an important behavior.

For example, a depth profile may support both snapshot and incremental feed handling.

A test that exercises only snapshots does not establish incremental correctness.

### Coverage Reporting

For every supported profile, determine:

```text
Profile ID.
Executable or explicitly unsupported.
Declared test case.
Required fixture availability.
Actually exercised source operations.
Known coverage gaps.
```

A newly added supported profile without a verification case must become visible.

### Unsupported Profiles

If the production catalog explicitly marks a profile unsupported, preserve that distinction.

Do not fabricate a passing execution test.

Verify the catalog's unsupported declaration where appropriate.

### Avoid Brittle Coverage Counts

Do not use a hardcoded assertion such as:

```text
Exactly 27 profiles must exist.
```

as the sole coverage guarantee.

Use identities and declared supported capabilities.

A new supported profile should produce a meaningful coverage gap rather than merely changing an expected integer.

---

# 9. Independent Source-to-Canonical Assertions

This is one of the most important requirements.

**A successful normalization execution is not sufficient evidence of correctness.**

For representative records, tests must verify expected canonical values using independently reviewed source evidence.

Do not obtain expected values by invoking the same normalization function being tested.

Do not automatically regenerate expected values from the current implementation.

### Expected Results Location

Keep representative expected values in readable Rust test definitions.

For example:

```rust
struct ExpectedTrade {
    // Reviewed expected values.
}
```

The exact representation should follow the existing test style.

Do not introduce separate expected-result JSON or TOML files for every profile.

### Source Evidence

For archive-specific expectations, identify the source evidence sufficiently to reproduce the assertion.

Useful metadata includes:

```text
Source archive identity.
Source archive checksum, where available.
Archive member or worksheet.
Source record locator.
Relevant instrument metadata.
Profile ID.
Profile fingerprint.
```

### Important Distinction

A source checksum identifies the bytes being tested.

It does not establish that the test's economic interpretation is correct.

Expected prices, quantities, sides, and book states must be independently justified.

### No Fabricated Expectations

If the source archive is unavailable and no reviewed expected values exist, do not invent them.

Preserve any existing valid assertions.

Report the remaining independent-oracle coverage gap.

Do not weaken the requirement by replacing exact expectations with:

```rust
assert!(!output.is_empty());
```

A nonempty output assertion may be useful, but it does not replace semantic verification.

---

# 10. Trade Profile Verification

Trade profile cases should validate representative canonical trade records.

Relevant fields include:

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

Verify optional fields when supported and available in the selected source.

### Timestamp Correctness

Verify that the source timestamp is interpreted using the correct unit and encoding.

Do not accept a plausible timestamp merely because it falls within a broad date range.

Where source evidence permits, verify the exact nanosecond timestamp.

### Price Correctness

Verify exact canonical price values.

Use decimal arithmetic appropriate to the canonical representation.

Do not introduce arbitrary floating-point tolerances.

### Side Correctness

Verify the source-specific interpretation of buy and sell directions.

Do not assume identical side conventions across exchanges.

### Quantity Correctness

Verify the distinction between:

```text
Base quantity.
Quote quantity.
Contract quantity.
```

For contract-based instruments, verify that the expected conversion uses the correct contract specification.

Do not merely verify that canonical quantities are algebraically consistent with one another.

A conversion can be internally consistent while using the wrong raw field or contract size.

### Identity Correctness

Verify exchange and instrument identity.

Do not allow similarly named symbols from different market categories to be treated as interchangeable.

### Record Counts

Where the source count is independently known, verify the expected number of accepted and rejected records.

Do not assume every archive must contain a fixed number of records simply because one fixture does.

---

# 11. Depth Profile Verification

Depth profile cases must verify more than canonical level counts.

The engine performs stateful order-book reconstruction.

A correct depth test must account for source-event interpretation and resulting book state.

### Required Behaviors

Cover supported cases involving:

- Initialization snapshots.
- Replacement snapshots.
- Absolute updates.
- Relative updates where implemented.
- Level insertion.
- Level quantity updates.
- Level deletion.
- Sequence continuity.
- Sequence gaps.
- Synchronization invalidation.
- Recovery after an authoritative snapshot.
- Accepted events producing no canonical level changes.

Do not assume every profile supports every operation.

Use the actual supported source semantics.

### Initialization

Verify that the book is initialized from the correct source event.

Check representative bid and ask levels.

Do not infer the correct book solely from the normalizer's own output.

### Updates

Verify representative intermediate book states.

For example:

```text
Snapshot
    │
    ▼
Expected book A
    │
    ▼
Incremental update
    │
    ▼
Expected book B
```

At least some expected states must come from independently reviewed source behavior.

### Deletions

Verify that a source event representing a level deletion produces the correct canonical representation and book state.

Do not confuse:

```text
Zero quantity.
Missing level.
Unchanged level.
```

These may have different meanings.

### No-Change Events

A valid source event may produce no canonical level changes.

Verify that the corresponding source-event identity and metadata remain represented where required.

Do not assume:

```text
accepted source events = canonical level rows
```

### Sequence Metadata

Verify sequence information when the source provides it.

Do not fabricate sequence guarantees for unsequenced feeds.

### Reconstruction Safety

Preserve existing assertions concerning:

```text
events.parquet
segments.json
boundary.json
```

Do not change the production depth reconstruction algorithms during this task.

---

# 12. Depth Replay and Persistence Verification

The normalization engine persists depth reconstruction metadata.

Existing tests may verify:

- Source-event indexing.
- Reconstruction segments.
- Row offsets.
- Initial book boundaries.
- Final book boundaries.
- Sequence metadata.
- Recovery boundaries.
- Replayed order-book states.

Preserve meaningful coverage.

### Independent Replay Verification

Replaying persisted data and obtaining the same final state as `boundary.json` is useful.

However, both outputs may originate from the same incorrect interpretation.

For representative cases, verify independently expected intermediate book states.

### Physical Layout

Do not require Parquet file bytes to remain identical when legitimate storage settings differ.

Verify canonical semantics and metadata consistency.

### Source Event Identity

Preserve the distinction between:

```text
Physical source records.
Accepted source events.
Canonical depth level rows.
```

Do not use canonical level count as a substitute for source-event count.

### No Storage Redesign

Task 11 will introduce stream-oriented depth output consolidation.

Do not implement it here.

Preserve the current production output structure and reconstruction contracts.

---

# 13. Deterministic Sampling

Not every test needs to process a complete archive.

Support explicit and reproducible verification scopes.

A case may exercise:

```text
A bounded source prefix.
A selected complete archive member.
A complete source archive.
Several explicitly selected archives.
```

### Required Declaration

Each test must make its inspection scope clear.

Do not describe a prefix test as complete-archive verification.

Do not silently switch from a complete archive to a prefix merely because the full archive is large.

### Source Integrity

Do not create samples by truncating compressed bytes.

Do not corrupt ZIP or GZIP containers to reduce processing time.

When sampling, preserve valid source-record boundaries.

### Stateful Depth Sampling

A depth sample must include the initialization and preceding events needed to reconstruct its selected state.

Do not begin in the middle of an uninitialized delta sequence.

Do not fabricate a snapshot solely to make a sample executable.

### Deterministic Selection

Do not select a random record range without recording the seed and boundaries.

Prefer explicit source locators.

The same test should inspect the same intended source evidence when the fixture has not changed.

---

# 14. Migrate Generic Normalization Tests

After exchange-specific cases are organized, migrate remaining generic normalization tests into their appropriate functional groups.

### Execution

Relevant tests may cover:

```text
Single-job execution.
Multi-job execution.
Task accounting.
Worker scheduling.
Job failure isolation.
Source preflight.
Staging and commit behavior.
```

Preserve useful assertions.

Do not rewrite the scheduler.

### Trades

Generic trade tests may cover:

```text
Canonical validation.
Decimal precision.
Timestamp conversion.
Quantity conversion.
Record rejection.
Trade writing.
```

Keep pure unit tests independent of the real-data batch.

### Depth

Generic depth tests may cover:

```text
Book initialization.
Update semantics.
Gap handling.
Recovery.
Source-event indexing.
Reconstruction segments.
Boundary metadata.
Replay.
```

Keep reusable state-machine tests separate from exchange-specific source interpretation.

### Parquet

Preserve tests for:

```text
Schema correctness.
Writer finalization.
Row counts.
File rollover.
Manifest discovery.
Metadata consistency.
Failure handling.
```

Do not change file-rotation behavior.

### Resources

Preserve tests for:

```text
Memory budgeting.
Resource admission.
Worker limits.
Scratch budgets.
Resource rejection.
Counter consistency.
```

Do not introduce hardware-dependent performance thresholds.

### Integrity

Retain tests established by Tasks 04 and 05.

Do not duplicate those tests across every exchange.

Exchange tests should verify source-specific correctness; generic tests should verify shared integrity behavior.

---

# 15. Remove the Normalization `slow/` Hierarchy

The existing repository contains or previously contained a normalization-specific `slow/` test hierarchy.

This hierarchy must be removed.

However, **removing the directory does not mean removing the tests**.

### Required Procedure

Inspect:

- The existing `slow/` entry point.
- Relative `#[path]` module declarations.
- Cargo integration-test targets.
- Feature gates.
- Test registration.
- Existing test-selection commands.

Determine which tests are currently registered through the wrapper.

Move or reconnect those tests into their appropriate functional modules.

### Registration Requirements

Every retained test must be registered exactly once.

Avoid:

```text
Test compiled through main target.
+
Same test compiled through slow target.
```

Also avoid leaving test source files disconnected from every Cargo test target.

### Verify Registration

Compare test inventories before and after migration.

Use Cargo's test-listing functionality where appropriate.

The final inventory must account for:

- Retained tests.
- Renamed tests.
- Consolidated duplicate tests.
- Tests intentionally replaced by stronger coverage.
- Tests that remain blocked by missing prerequisites.

Do not silently delete tests.

### Execution Time Is Not a Responsibility

Do not replace:

```text
slow/
```

with:

```text
heavy/
```

or:

```text
expensive/
```

as another primary organizational hierarchy.

Organize tests by functionality.

Handle expensive execution through explicit selection.

---

# 16. Explicit Test Selection

The refactored suite must provide a predictable way to run different verification scopes.

### Source-Only Tests

These must run without downloaded market-data archives.

They should cover:

```text
Configuration.
Profile parsing.
Batch validation.
Decoder units.
Normalization arithmetic.
Integrity accounting.
State-machine behavior.
Synthetic persistence.
```

### Single Profile

A developer must be able to select one profile test.

For example:

```text
Bybit perpetual linear depth.
```

The selection must execute the intended case, not every exchange profile.

### Single Exchange

A developer must be able to select the profile tests belonging to one exchange.

### Complete Real-Data Verification

A separate explicit command or selection must execute the complete declared real-data workload.

Do not make this the default behavior of ordinary source-only tests.

### Selection Mechanism

Use existing Cargo features, integration-test targets, filters, or another established repository mechanism.

Prefer straightforward test selection.

Do not build an elaborate custom test runner unless the existing structure genuinely requires one.

### Missing Real Data

When real-data verification is explicitly selected, missing required archives must produce a clear failure.

Do not use blanket `#[ignore]` annotations, successful early returns, or silent exclusions to conceal missing inputs.

Unselected real-data tests must not prevent ordinary source-only testing.

A deliberately unselected test is not a passing test.

Document actual commands after verifying the final Cargo target names.

---

# 17. Persistent Test Outputs

Task 03 established:

```text
data/tests/historical/normalize/
```

All artifact-producing normalization integration tests must continue using this directory.

### Directory Organization

Exchange-specific tests should write under:

```text
data/tests/historical/normalize/exchanges/
```

For example:

```text
data/tests/historical/normalize/
└── exchanges/
    └── bybit/
        └── perpetual_linear_depth/
            └── <run-id>/
                ├── test.json
                ├── selected-batch.json
                ├── datasets/
                ├── staging/
                ├── scratch/
                └── reports/
```

Generic tests should use their corresponding functional directories:

```text
execution/
depth/
trades/
parquet/
resources/
```

### Required Behavior

- Preserve canonical Parquet files.
- Preserve dataset manifests.
- Preserve resolved jobs and provenance.
- Preserve depth auxiliary artifacts.
- Preserve integrity reports.
- Preserve task and job metrics.
- Preserve failure diagnostics.
- Preserve incomplete staging outputs where available.
- Never overwrite previous executions.
- Never automatically delete persistent integration-test artifacts.

### Output Isolation

Use the shared Task 03 output-redirection infrastructure.

Do not duplicate path-redirection logic in each exchange file.

Never write to the production output paths specified in `normalize.json`.

Keep source archives read-only.

### Pure Unit Tests

Pure unit tests may continue using temporary resources.

Do not persist trivial counter or parser tests merely because normalization integration tests use persistent output storage.

---

# 18. Runtime Configuration and Resource Safety

All archive-backed normalization tests must use the shared runtime configuration infrastructure.

The authoritative runtime configuration is:

```text
data/.config/historical/general.toml
```

Apply deterministic test-specific resource overrides in memory.

Do not modify the production file.

### Required Behavior

- Use conservative worker counts.
- Use bounded memory budgets.
- Use isolated scratch paths.
- Preserve integrity policies.
- Avoid uncontrolled concurrency.
- Do not rely on a 128 GB development machine.

### Test-Level Concurrency

Coordinate archive-backed tests with engine worker counts.

Avoid launching many expensive tests that each assume exclusive use of the full machine.

Use the existing test-selection mechanism or a minimal shared concurrency guard where justified.

Do not implement a new global scheduling framework merely for testing.

### No Benchmarking

These are correctness tests.

Do not introduce throughput thresholds.

Do not classify a test as failed because its runtime is slower than an arbitrary expected duration.

Performance benchmarking belongs to Tasks 08 and 09.

---

# 19. Failure Diagnostics

A test failure must provide enough information to identify the relevant source and expected behavior.

### Exchange-Profile Failure

Where applicable, include:

```text
Test case.
Exchange.
Profile ID.
Resolved profile fingerprint.
Instrument type.
Market category.
Symbol or family scope.
Source archive.
Archive member.
Source record locator.
Job ID.
Task ID.
Expected value.
Observed value.
```

### Classification

Distinguish:

```text
Fixture missing.
Fixture selection ambiguous.
Profile missing.
Profile incompatible.
Source decoding failure.
Incorrect canonical interpretation.
Integrity failure.
Persistence failure.
Test assertion failure.
```

Do not replace detailed diagnostics with a generic message such as:

```text
Normalization failed.
```

### Unexpected Engine Behavior

If a profile test reveals a genuine normalization defect, preserve the failing case and evidence.

Do not:

- Change the expected value to match the implementation.
- Relax decimal assertions.
- Substitute another archive.
- Disable the integrity policy.
- Skip the failing case.
- Delete the test.

Report the defect.

Only correct production behavior when the fix is narrow, clearly supported by evidence, and necessary to restore an established contract. Otherwise, leave the issue for explicit review.

---

# 20. Preserve Useful Existing Unit Tests

Do not migrate every isolated unit test into the real-data testing infrastructure.

Examples that should generally remain independent include:

```text
Decimal parsing.
Quantity arithmetic.
Timestamp conversions.
Side transformations.
Counter overflow.
Policy threshold arithmetic.
Sequence-state transitions.
Book-store operations.
Parquet batch construction.
```

These tests should not require:

```text
normalize.json
Downloaded archives.
PostgreSQL.
Large runtime budgets.
```

### When to Modify a Unit Test

Modify an existing unit test only when:

- Its interface changed legitimately.
- Its previous setup depended unnecessarily on legacy files.
- It duplicated another test without adding coverage.
- It asserted behavior deliberately replaced by the production contract.
- It contained an incorrect expectation supported by clear evidence.

Preserve meaningful assertions.

Do not rewrite unit tests solely for stylistic consistency.

---

# 21. Avoid Redundant Normalization Work

A profile test may need to inspect:

- Canonical values.
- Metrics.
- Manifest metadata.
- Source relationships.
- Depth reconstruction metadata.

Where possible, normalize once and perform all applicable assertions on the resulting dataset.

Do not execute the same archive separately for each assertion.

### Exceptions

Multiple executions are justified when testing differences such as:

```text
Worker-count determinism.
Parquet rollover behavior.
Different supported resource configurations.
```

### No Shared Output Cache

Do not introduce a persistent cache of previous test results as the authoritative input for correctness assertions.

Every execution test must evaluate its own newly generated output.

Tests must not depend on the execution order of other tests.

---

# 22. Implementation Sequence

## Phase A — Coverage Inventory

Inspect the existing test suite and Cargo registration.

Create a before-migration inventory.

Identify legacy dependencies and useful assertions.

## Phase B — Exchange Test Organization

Create or consolidate one file per exchange.

Move relevant profile cases into their appropriate files.

Preserve existing source expectations.

## Phase C — Production Fixture Migration

Replace legacy job loading with:

```text
engine/tests/historical/fixtures/normalize.json
```

Use deterministic selection and production profile resolution.

Remove hardcoded `jobs[0]` and `tasks[0]` selection assumptions.

## Phase D — Independent Assertions

Review existing source-to-canonical expectations.

Preserve valid assertions.

Improve representative trade and depth coverage where independently verified source evidence is available.

Record missing oracle coverage explicitly.

## Phase E — Generic Test Migration

Reconnect execution, trade, depth, Parquet, resource, and integrity tests to their appropriate functional groups.

Preserve existing useful unit tests.

## Phase F — Remove Slow Wrapper

Remove normalization's obsolete `slow/` hierarchy.

Update module declarations and Cargo registration.

Verify every retained test is registered exactly once.

## Phase G — Verification

Run formatting, compilation, and relevant source-only tests.

Run narrowly selected real-data cases only when their required sources are available and execution is appropriate.

Do not run the entire real-data workload automatically.

Do not execute benchmarks.

Stop after completing Task 06.

---

# 23. Verification Requirements

Use the repository's actual Cargo test targets and features.

### Formatting

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

### Compilation

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Compile relevant integration-test targets as well.

### Test Registration

Use Cargo's test-listing functionality to verify registration.

Confirm that:

- Exchange-specific profile cases appear.
- Generic tests appear.
- No tests are registered twice.
- No retained test files are disconnected.
- The obsolete normalization slow wrapper is no longer required.

### Source-Only Tests

Execute relevant lightweight tests.

These must not require the real-data workload.

### Representative Real-Data Tests

Where the required prepared fixture and archives are available, verify explicitly selected representative cases.

Prioritize meaningful coverage without automatically processing the complete corpus.

Do not execute month-long Gate.io archives merely to establish that the test wiring works.

Gate.io test cases must remain available for explicit selection.

### Complete Workload

Do not automatically execute the full normalization workload.

Document the appropriate command for future explicit execution.

### Missing Sources

If a selected test requires missing source data:

- Report the missing path.
- Report the required profile or task.
- Explain the preparation prerequisite.
- Do not substitute another source.
- Do not report the test as passed.

### Performance Restrictions

Do not run benchmarks.

Do not perform worker-scaling experiments.

Do not tune resource settings.

Do not produce performance conclusions from ordinary test durations.

---

# 24. Scope Restrictions

## Authorized

This task authorizes:

- Exchange-specific test organization.
- Production batch fixture integration.
- Deterministic profile-case selection.
- Independent source-to-canonical assertions.
- Profile coverage reporting.
- Generic normalization test migration.
- Removal of obsolete normalization slow-test registration.
- Cargo test-target corrections.
- Persistent test-output integration.
- Focused test-helper improvements.
- Minimal compatibility adjustments.
- Documentation of test selection and prerequisites.

## Not Authorized

Do not implement:

- New normalization algorithms.
- New source-format support.
- New production profiles without a demonstrated correctness requirement.
- A new batch JSON protocol.
- A new runtime configuration architecture.
- Broad integrity-system redesign.
- Source-reader redesign.
- CLI execution-result redesign.
- Dataset manifest redesign.
- Stream-oriented Parquet consolidation.
- New merge or conversion algorithms.
- Benchmark infrastructure.
- PostgreSQL registry integration.
- Python Forge changes.

### Important Boundary

Tasks 01–05 established production configuration, loading, integrity accounting, and source-error handling.

Do not reimplement those systems in the test suite.

Tests should consume the production implementations.

If a defect is discovered, report it with a reproducible failing test rather than creating a test-only workaround.

---

# 25. Working-Tree Safety

Inspect the current working tree before modifying files.

Preserve unrelated user changes.

Do not reset the repository.

Do not overwrite the authoritative workload fixture.

Do not modify source archives.

Do not delete existing tests without accounting for their coverage.

Do not remove unrelated slow-test groups outside historical normalization.

Do not create commits or branches unless requested.

Prefer reviewable changes organized by test responsibility.

---

# 26. Required Implementation Report

When Task 06 is complete, provide the following.

## Test Organization

Describe:

- Final directory structure.
- Exchange-specific files.
- Generic test groups.
- Shared test helpers reused.
- Removed legacy test modules.

## Coverage Inventory

Provide a before/after accounting.

Include:

```text
Existing tests retained.
Tests migrated.
Tests renamed.
Duplicate registrations removed.
Tests replaced.
New tests added.
Remaining coverage gaps.
```

Do not rely solely on the final total test count.

## Profile Coverage

For each supported profile, report:

```text
Profile ID.
Test case.
Fixture availability.
Execution status.
Behaviors verified.
Known missing evidence.
```

Distinguish profile parsing from actual source normalization.

## Legacy Dependencies

Identify:

- Legacy job dependencies removed.
- Legacy runtime configuration dependencies removed.
- Manual normalization-rule injection removed.
- Remaining legacy dependencies, if any.

## Test Registration

Confirm:

- Normalization slow wrapper removed.
- Retained tests registered exactly once.
- Source-only and real-data selections available.
- Relevant Cargo test targets compile.

## Persistent Outputs

Describe:

- Output directory structure.
- Artifact retention.
- Failure preservation.
- Output isolation.
- Git ignore behavior.

## Verification

Report:

- Formatting checks.
- Compilation checks.
- Source-only tests executed.
- Real-data cases executed.
- Tests passed.
- Tests failed.
- Tests blocked by missing prerequisites.

Do not claim that the complete historical corpus was verified unless it was actually executed.

## Discovered Defects

List production or profile defects discovered during migration.

Provide the reproducing test and relevant evidence.

Do not silently weaken assertions.

---

# 27. Acceptance Criteria

Task 06 is complete when:

- Archive-backed normalization tests use the single maintained `fixtures/normalize.json`.
- Tests use the production schema-version-3 batch loader.
- Tests resolve production self-contained TOML profiles.
- Exchange-specific cases are organized into one file per exchange.
- Supported profiles have individually identifiable test cases.
- Profile coverage gaps are visible.
- Tests use deterministic structured task selection.
- No arbitrary first-task or first-file selection remains in migrated real-data tests.
- Missing or ambiguous fixture selections fail clearly.
- Representative trade cases verify independently expected canonical values.
- Representative depth cases verify applicable event and reconstruction semantics.
- Existing useful source-to-canonical assertions are preserved.
- Existing useful generic unit and integration tests are preserved.
- The normalization-specific `slow/` hierarchy is removed.
- Every retained test is registered exactly once.
- Source-only tests do not require downloaded archives.
- Real-data execution remains explicitly selectable.
- Missing real-data prerequisites are not reported as passing tests.
- No production output directory is modified.
- Source archives remain read-only.
- Artifact-producing integration tests retain outputs under `data/tests/historical/normalize/`.
- Previous test outputs are never automatically deleted.
- Test results remain independent of execution order.
- Relevant formatting, compilation, and source-only tests pass.
- Any unverified real-data behavior is reported explicitly.
- No performance benchmarks or complete historical workloads are executed automatically.
- Canonical schemas and production normalization algorithms remain unchanged.

---

## 28. Stop Condition

**Stop after Task 06 is complete.**

Do not automatically proceed to Task 07.

Do not redesign the normalization CLI.

Do not modify the dataset manifest protocol.

Do not implement stream-oriented Parquet consolidation.

The next assignment is:

**Task 07 — CLI Results, Dataset Manifests, and Forge Integration Contract.**

Task 07 will finalize the machine-readable normalization execution boundary so Python Forge can reliably interpret successful and failed jobs, committed datasets, provenance, and diagnostics.

---

**Final objective:** Establish a clean, comprehensive, and reproducible historical normalization test suite that verifies supported exchange profiles through the actual production pipeline, preserves meaningful existing coverage, and produces inspectable persistent artifacts without depending on obsolete job formats or private development assumptions.