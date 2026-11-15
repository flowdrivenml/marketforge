## MarketForge — Historical Normalization Test Refactor Note

**Status:** Agreed design for future implementation.

**Scope:** Historical normalization tests, shared test support, fixture selection, profile verification, test registration, and reproduction guidance.

This note records the intended testing architecture. It is not an instruction to begin implementation immediately.

The objective is to make the normalization test suite reproducible, understandable, and independent of one developer’s private job files and machine configuration. Preserve useful existing coverage rather than rewriting the suite indiscriminately.

The central principle is:

**One authoritative real-data workload file, production normalization profiles, shared test infrastructure, and independently meaningful assertions.**

## Agreed Decisions

Use one maintained workload document:

```text
engine/tests/historical/fixtures/normalize.json
```

All archive-backed normalization tests must obtain their input tasks from this file. Do not maintain separate job JSON files for individual exchanges, instruments, or test groups.

Organize exchange-specific profile tests under:

```text
engine/tests/historical/normalize/exchanges/
```

Use one Rust source file per exchange, with individually identifiable tests for its supported profiles.

Remove the normalization-specific `slow/` hierarchy and reconnect its tests to their normal functional organization. Execution selection must remain explicit, but source files should not be organized primarily by execution time.

Preserve existing unit tests unless their interfaces, responsibilities, or external dependencies genuinely require changes.

Use the actual production input contract, profile loader, resolver, and normalization execution path. Do not build a separate test implementation of normalization.

## Architectural Context

The external normalization input is the prepared schema-version-3 batch:

```text
schema_version: 3
kind: normalization_batch
status: prepared
jobs: multiple jobs, each containing tasks
```

`prepared` describes input preparation, not successful normalization.

Normalization profiles are self-contained TOML documents. They must not depend on separate physical-schema files.

Their identity convention is:

```text
exchange.instrument_type.market_category.data_type
```

For example:

```text
bitget.perpetual.linear.depth
```

The profile filename must be:

```text
bitget.perpetual.linear.depth.toml
```

Runtime resource settings, including worker counts and Parquet sizing, belong to:

```text
data/.config/historical/general.toml
```

Inspect the current production configuration model before implementing the test helper. Do not assume that every integrity-policy setting moved into `general.toml` merely because resource settings did.

Forge supplies the documented preparation workflow for real archives and the batch document. Treat that workflow as a prerequisite; this refactor does not redesign or retest acquisition.

The normalization I/O refactor is a separate workstream. This note defines its testing requirements but does not authorize implementing the storage redesign.

## Migration Baseline

The reviewed depth-format helper loads named legacy documents from `data/.jobs/process`, selects `job.tasks[0]`, loads `data/.jobs/processing.json`, and receives a sequence policy directly from the test. Replace that setup without losing its useful assertions. :chatgpt-content-reference{index="0"}

The reviewed slow entry point includes tests from the existing normalization directories through relative `#[path]` declarations. Check registration and build configuration before removing that wrapper; some tests may need reconnection rather than physical relocation. :chatgpt-content-reference{index="1"}

Some runtime configuration tests still reference `normalize.toml` and assert particular resource defaults. Migrate tests affected by the configuration change while separating configuration validation from local operating preferences. :chatgpt-content-reference{index="2"}

These observations describe the reviewed exports. Recheck the repository at implementation time because files and interfaces may have changed.

## Responsibilities of the Shared Fixture

### One workload document, not one enormous test

`jobs.json` describes which archives and instruments are available for testing. It does not require every test to execute every task.

Each test selects the cases relevant to its responsibility.

```text
fixtures/normalize.json
        │
        ├── Binance profile cases
        ├── Bitget profile cases
        ├── Bybit profile cases
        ├── Gate.io profile cases
        ├── OKX profile cases
        ├── Selected cross-archive cases
        └── Selected multi-job execution cases
```

A profile test may select one representative archive or several explicitly chosen symbols. A continuity test selects its required adjacent sources. A batch test deliberately selects several jobs.

Only a designated complete-workload test should execute the entire batch.

### Keep the production schema unchanged

Use the real batch contract. Do not add test-only fields for expected results, case names, sampling limits, resource settings, or coverage annotations.

Keep normalization mappings in the production TOMLs rather than embedding them in `jobs.json`.

The responsibility split is:

| Component | Responsibility |
|---|---|
| `fixtures/jobs.json` | Jobs, tasks, source locations, instrument metadata, and legitimate batch fields |
| Production profile TOMLs | Source decoding and normalization interpretation |
| Production runtime configuration | Operational settings loaded through the real configuration path |
| Rust case definitions | Selection criteria, expected results, and verification scope |
| Shared test support | Loading, safe execution, temporary paths, and reusable checks |

Preserve integer identifiers exactly. Do not convert them through floating-point representations.

### Temporary test documents are permitted

“One workload file” means one maintained archive-backed workload definition.

Negative tests may clone a valid document, modify it in memory, and serialize it into an isolated temporary directory when testing the file-loading interface.

Examples include duplicate IDs, invalid versions, missing metadata, or unsupported field combinations.

Temporary documents, generated reports, and dataset manifests are not additional maintained workload files.

Pure unit tests and small synthetic integration tests may construct minimal values directly. They must not become dependent on real archives merely to satisfy the single-file convention.

## Portable and Deterministic Input Selection

### Select explicit cases

Select tasks using structured identity and explicit source criteria.

Relevant criteria may include exchange, instrument type, market category, content type, symbol or family scope, source feed, archive member, and the intended source period.

Do not select by:

```text
job.tasks[0]
the first matching filesystem entry
the first available archive
a developer-specific generated database ID
```

A test requiring one case must reject an ambiguous selection. A test requiring several sources must verify that the intended set was selected.

Preserve job/task IDs for execution and diagnostics, but do not make portable selection depend on incidental IDs created by another developer’s database.

A required selection returning no tasks is a failure, not a successful empty test.

### Preserve the production loading boundary

Use production parsing and validation to load the batch. Prepare isolated subsets through the production document/types rather than rebuilding resolved normalization rules by hand.

Where a temporary subset document is needed, keep its batch metadata and output paths internally consistent and pass it through the actual production loader.

Do not introduce a test-only legacy fallback that makes new-contract tests pass while the production loader remains incompatible.

A selected exchange test should not unnecessarily open every unrelated archive in the batch. Separate metadata validation from source execution where the production interfaces permit it.

### Use one documented path policy

Prefer repository-relative input paths when supported by the production contract.

If Forge generates machine-local absolute paths, its reproduction workflow must regenerate the single `jobs.json` appropriately. Do not scatter path-rewriting rules throughout the tests.

Resolve paths from an explicit root, not the current shell working directory.

Do not search the raw-data directory for an arbitrary replacement when the requested source is missing.

Load a locally generated real-data workload at runtime rather than making its presence a compilation prerequisite for unrelated unit tests.

### Do not infer chronology from incidental identifiers

Task IDs, job IDs, download-directory hashes, and complete filesystem paths are not chronological keys.

Cross-archive cases must explicitly identify their intended source order and verify the relevant ordering contract.

Do not assume adjacent filenames establish sequence continuity or compatible reconstruction boundaries.

## Proposed Test Organization

```text
engine/tests/historical/
├── fixtures/
│   └── normalize.json
├── support/
│   ├── mod.rs
│   ├── fixtures.rs
│   ├── runtime.rs
│   └── assertions.rs
└── normalize/
    ├── main.rs
    ├── profiles.rs
    ├── exchanges/
    │   ├── mod.rs
    │   ├── binance.rs
    │   ├── bitget.rs
    │   ├── bybit.rs
    │   ├── gateio.rs
    │   └── okx.rs
    ├── execution/
    ├── depth/
    ├── trades/
    ├── parquet/
    └── resources/
```

This expresses responsibilities, not a requirement to create empty modules or a large new testing framework. Adapt module wiring to the repository’s actual Cargo test targets.

Exchange files contain profile-specific cases and independently reviewed expectations. Generic groups retain tests for execution, accounting, reconstruction, persistence, and resource behavior.

Shared support should centralize repeated setup without becoming a second normalization engine.

### Individually identifiable profile tests

Use separate named tests for supported profile combinations.

Illustrative names inside `bybit.rs`:

```text
spot_trades_profile
perpetual_linear_trades_profile
perpetual_inverse_trades_profile
future_linear_trades_profile
spot_depth_profile
perpetual_linear_depth_profile
perpetual_inverse_depth_profile
future_linear_depth_profile
```

Derive the actual case set from the supported catalog. Do not invent unsupported profiles to complete an exchange matrix.

One exchange file must not become one opaque test function that reports only “exchange failed.”

Multiple instruments may exercise one profile. Preserve useful cross-symbol cases to detect assumptions tied to a particular symbol, quote asset, or decimal scale.

## Production Profile Verification

### Exercise the actual resolution path

Profile tests must follow this path:

```text
Selected task from jobs.json
              │
              ▼
Production batch/task validation
              │
              ▼
Production TOML discovery and resolution
              │
              ▼
Resolved normalization specification
              │
              ▼
Actual archive decoding and normalization
              │
              ▼
Canonical records, metrics, and persisted output
              │
              ▼
Independent assertions
```

Do not manually inject the quantity encoding, timestamp encoding, field mappings, sequence policy, or event semantics that production is expected to resolve.

A test may assert that the resolved value is correct. It must not force that value into the execution path and then claim the resolver was tested.

Where production resolution is not yet implemented for the new contract, report that dependency instead of bypassing it.

### Separate catalog validity from source correctness

Catalog tests should verify that production profiles load, use valid identities and matching filenames, remain self-contained, and resolve without unintended ambiguity.

Profile execution tests should establish whether the resolved configuration correctly interprets the source data.

Persistence tests should establish whether stored output preserves the resulting canonical data and reconstruction metadata.

These are separate claims. Successful TOML parsing is not sufficient evidence for source correctness.

### Make coverage explicit

Compare:

```text
Supported production profiles
        ↔ Declared Rust test cases
        ↔ Available fixture tasks
        ↔ Actually exercised rules and behaviors
```

Report missing cases by profile identity rather than relying mainly on fixed total-profile counts.

A newly added supported profile without a verification case must create a visible coverage gap.

Loading a profile does not count as exercising it. A snapshot-and-update profile needs evidence for both applicable event paths.

Do not hide missing coverage by silently declaring a profile unsupported or adding a blanket exclusion. Any approved exception must identify the missing evidence and its reason.

## Independent Expected Results

### Do not use the implementation as its own authority

For each supported profile, establish a small set of reviewed source-to-canonical expectations.

Keep these as readable Rust case data or constants in the corresponding exchange file. Do not create another family of expected-result configuration files.

Do not obtain expected values by calling the normalization function under test or by blindly copying the profile’s interpretation.

Comparing old and new implementations is useful migration evidence, but agreement does not prove that either interpretation is correct.

Do not regenerate expected values automatically from a failing implementation.

### Tie exact expectations to known source evidence

Record enough context to identify the reviewed input: archive identity, member or worksheet where applicable, record locator, and relevant instrument metadata.

For exact archive-specific expectations, establish the identity of the tested bytes with an appropriate checksum. A checksum identifies the input; it does not prove its economic interpretation.

Keep source identity separate from the resolved profile fingerprint. Report the effective fingerprint so failures can be associated with a configuration change.

Distinguish fixture facts from general guarantees.

```text
Valid fixture assertion:
This identified archive contains the reviewed record count.

Invalid generalization:
Every archive using this profile must contain that count.
```

The same distinction applies to populated book depth, first and last sequence values, and exact price levels.

### Trade expectations

Verify representative timestamps, prices, sides, trade identifiers where available, instrument identity, and applicable base/quote/contract quantities.

Use exact decimal expectations where appropriate. Any tolerance must have an explicit reason tied to the declared arithmetic or rounding contract.

Do not accept approximate comparisons merely because they make a failure disappear.

Quantity checks must establish that the correct raw field and units were used, not merely that two derived values are algebraically consistent.

### Depth expectations

Verify representative source events and resulting book states.

Cover initialization, applicable update semantics, deletion, replacement snapshots, and available sequence metadata. Exercise no-change events and recovery behavior in controlled cases.

Do not assume every supported feed provides the same event model or sequence guarantees.

A profile sample and a generic synthetic state-machine test have complementary roles. Synthetic evidence must not be reported as proof that a real archive contains an unobserved event type.

### Persistence and replay checks

Preserve useful source-event indexing, segment, boundary, quantity, and replay assertions.

Compare persisted data with independent expectations as well as internally generated metadata.

Replay matching `boundary.json` is valuable, but both artifacts could reflect the same upstream mistake. Check intermediate replay states in small controlled cases, not only the final book.

Keep physical source-record counts, accepted source-event counts, and emitted canonical row counts distinct. Depth events can emit many rows or no changed rows.

## Sampling and Verification Scope

Each real-data case must clearly declare whether it checks a bounded sample, selected complete members, or the complete archive.

Use deterministic sampling and preserve record boundaries. For stateful depth, include the initialization and preceding events needed to interpret the selected window.

Do not manufacture invalid input by cutting compressed bytes, truncating a workbook, or beginning a depth sample midway through an uninitialized sequence.

Do not label a prefix test as a complete-archive verification.

Record which required event types were actually exercised. “Not observed in this sample” is not equivalent to “not supported by the source.”

Keep source archives read-only. Temporary samples, when needed, must be created in the test’s isolated workspace and remain attributable to the original input.

## Shared Runtime Configuration and Output Isolation

### Centralize runtime setup

Integration tests should load runtime settings through the production configuration path and apply explicit test resource overrides in memory.

Centralize ordinary correctness-test resource limits. Specialized tests may deliberately override worker counts, row-group targets, file targets, or resource budgets to exercise their responsibility.

Do not create separate maintained runtime configuration files for each test or exchange.

Do not make routine correctness tests inherit large production budgets or require a 128 GB machine.

### Keep resource overrides separate from integrity policy

Never silently disable integrity checks or relax thresholds to make real data pass.

A test expecting degraded output must declare that policy and expected outcome explicitly. A failure-policy test must verify the relevant failure and accounting behavior.

Pure configuration-validation tests may use minimal deterministic in-memory configurations or temporary TOML documents. They should test validation rules rather than local operating preferences.

Production configuration files must remain unchanged during test execution.

### Isolate every writable path

Each test must own a unique temporary workspace containing its staging, datasets, scratch files, reports, and temporary input variants.

Rebase writable batch metadata and job paths before any operation that can create files. Never execute against production destinations copied from `jobs.json`.

The shared workload and raw archives are read-only.

Do not share mutable jobs, writers, output directories, or cached normalized results between independent tests. Immutable parsed fixture metadata may be shared where useful.

Tests must succeed independently and in arbitrary execution order. Cleanup must remove only artifacts owned by that test.

Avoid process-global working-directory or environment mutations as a path-resolution mechanism.

## Removing the Slow-Test Hierarchy

Remove the normalization-specific slow wrapper and restore the included tests to their normal functional groups.

Inspect Cargo target definitions, module declarations, scripts, and existing verifier entry points before changing registration.

Every retained test must remain discoverable exactly once. Do not accidentally compile the same module into two test targets or leave a source file disconnected.

Do not remove unrelated slow-test groups outside this scope.

Execution selection remains necessary, but it must describe prerequisites or purpose rather than create another directory called `slow`, `heavy`, or `expensive`.

Provide distinct ways to run:

| Selection | Expected behavior |
|---|---|
| Source-only verification | No downloaded market data or acquisition services required |
| One profile | Execute the explicitly selected profile case |
| One exchange | Execute that exchange’s profile cases |
| Complete real-data verification | Require and verify the declared prepared corpus |
| Performance measurement | Run a controlled workload with explicit resource settings |

Use the repository’s existing test mechanisms where possible. Document actual verified commands after inspecting target names.

A deliberately unselected real-data case is not a pass. An explicitly selected case with missing data must fail clearly with the missing path and preparation guidance.

Do not use early successful returns, blanket ignores, or exclusions to conceal missing fixtures, broken profiles, or engine defects.

## Preserve and Improve Generic Coverage

Keep generic engine guarantees outside exchange files so that they are not reimplemented five times.

| Area | Required focus |
|---|---|
| Input validation | Batch version, identities, duplicate IDs, task selection, and invalid metadata |
| Profile resolution | Missing, ambiguous, incompatible, or mismatched profile expectations |
| Execution | Multi-task and multi-job accounting, isolation, and failure attribution |
| Integrity | Correct classification, thresholds, diagnostics, and expected degraded/failed outcomes |
| Persistence | Canonical values, metadata consistency, rollover, and failure cleanup |
| Depth reconstruction | Initialization, updates, gaps, recovery, event indexing, and replay |
| Determinism | Equivalent canonical semantics under supported worker/resource variations |

Use small synthetic inputs for controlled failures and edge cases. These must not load private historical jobs merely to obtain a configuration object.

Compare semantic results rather than requiring identical Parquet bytes when legitimate buffering or layout changes occur.

Do not sort outputs in the assertion helper merely to hide an ordering violation. Verify the declared output ordering separately.

### Future I/O-refactor coverage

The separate stream-oriented output refactor will require tests for several archives sharing output parts, stream separation, source attribution, output rollover, repeated source-local ordinals, and reconstruction boundaries.

Treat these as explicit future acceptance requirements until that behavior is implemented.

Do not silently change depth continuity semantics or implement the writer redesign as part of test organization.

Do not report a future requirement as verified merely because its test is ignored or has not been written.

## Preserve Unit Tests Unless Necessary

Leave focused unit tests in place when their behavior and interfaces remain valid.

Do not route decimal parsing, timestamp conversion, quantity arithmetic, counter overflow, or similar isolated checks through the real-data batch.

Modify a unit test only when an actual contract change requires it, its setup depends unnecessarily on external files, or it asserts behavior deliberately removed from the product.

For retired physical-schema or legacy-job assumptions, document the replacement coverage. Do not preserve obsolete architecture solely to keep old tests passing.

Conversely, do not delete compatibility tests unless the corresponding compatibility requirement has actually been retired.

## Avoid Redundant Work and Uncontrolled Concurrency

Within a profile case, normalize once and perform the applicable value, metric, metadata, and persistence checks on that execution.

Repeat execution only when a comparison requires it, such as worker-count or rollover-policy equivalence.

Do not introduce a shared persistent output cache that can conceal stale results or make tests depend on execution order.

Coordinate test-level concurrency with engine worker counts. Multiple archive tests must not each assume exclusive access to the full machine.

Keep ordinary correctness runs bounded and reproducible. Performance runs may use larger explicit settings, but should not redefine correctness-test defaults.

Performance results should record workload identity, effective settings, elapsed time, output volume, file counts, and relevant memory measurements. Do not add hardware-dependent throughput thresholds to ordinary profile tests.

## Failure Diagnostics

Failures should identify the test case, expected and resolved profile, job/task identity, archive and member, relevant source-record locator, and expected versus observed behavior.

Distinguish fixture preparation failures from profile-resolution errors, source decoding errors, incorrect semantic expectations, and engine persistence defects.

For bounded verification, report the actual inspection scope and any unexercised behaviors.

Do not replace detailed assertions with a generic “normalization failed” message.

Where a profile or engine defect is discovered, retain a reproducible case and report the evidence. Do not weaken the assertion, switch the archive, or silently rewrite expected output to obtain a passing run.

## Implementation Sequence for the Later Handoff

### Establish the baseline

Inventory the registered tests, legacy job dependencies, runtime configuration loaders, real-data requirements, and existing useful assertions.

Record which tests pass, fail, cannot compile, or cannot run because prerequisites are unavailable. Do not describe blocked tests as passing.

Preserve unrelated working-tree changes.

### Introduce shared support

Implement the single-workload loader, deterministic case selection, production profile resolution, safe runtime overrides, and isolated output handling.

Use one representative trade case and one representative depth case to validate the new setup.

Keep helper code focused; do not build a general-purpose testing framework.

### Migrate exchange verification

Move exchange-specific assertions into one file per exchange, retaining individually identifiable profile cases.

Replace legacy setup, establish reviewed expectations for the selected inputs, and add coverage checks.

Preserve useful assertions unless their underlying contract or fixture changed.

### Reconnect generic tests and remove obsolete wiring

Restore tests from the slow wrapper, update affected registration, and migrate generic tests away from private job files.

Keep source-only tests independent of real-data preparation.

Remove obsolete helpers and maintained workload variants only after their consumers and coverage have been accounted for.

### Verify and document

Run the available source-only and prepared-data selections. Verify that the documented profile, exchange, and complete-suite commands execute the intended tests.

Document fixture preparation, path resolution, test selection, resource limits, and missing-data behavior using the existing Forge reproduction workflow.

Keep production compatibility fixes and the normalization I/O redesign identifiable as separate changes rather than hiding them inside test cleanup.

## Acceptance Criteria

The refactor is complete when archive-backed normalization tests use one maintained `fixtures/jobs.json`, production TOML profiles are exercised through the real resolution path, and profile cases are individually selectable within exchange-specific files.

There must be no dependency on private legacy job directories, no silent fallback to arbitrary archives, no successful empty selection, and no writes to production output paths.

The normalization slow wrapper must be removed without losing or duplicating registered coverage. Routine unit tests must remain independent of real archives and large-machine resource settings.

Supported-profile coverage must identify missing cases explicitly. Correctness must be established through reviewed source expectations, meaningful integrity assertions, and persistence/replay checks—not merely nonempty output or agreement with the previous implementation.

The implementation report must distinguish work completed, tests actually executed, tests blocked by prerequisites, remaining coverage gaps, and production/profile defects requiring separate correction.

## Additional Requirement — Persistent Test Outputs

### Objective

Historical normalization integration tests must preserve their generated artifacts so they can be inspected manually after execution.

Previously, some tests created temporary directories and automatically deleted their outputs when the test completed.

We are changing this behavior for normalization tests that generate datasets, Parquet files, manifests, or other persistent execution artifacts.

**All normalization test outputs must be stored under:**

```text
data/tests/historical/normalize/
```

Do not automatically delete these outputs after successful or failed test execution.

### Output Directory Structure

Organize outputs by test case, preserving enough context to identify the exchange, profile, instrument, and execution.

Recommended structure:

```text
data/tests/historical/normalize/
├── exchanges/
│   ├── binance/
│   ├── bitget/
│   ├── bybit/
│   │   ├── spot_spot_trades/
│   │   │   └── <run-id>/
│   │   └── perpetual_linear_depth/
│   │       └── <run-id>/
│   ├── gateio/
│   └── okx/

```

This is an organizational example. Avoid creating unnecessary empty directories.

Each test execution that writes persistent artifacts must have a unique output directory.

### Output Retention

The following artifacts must be preserved whenever generated:

- Canonical Parquet files.
- Dataset manifests.
- Resolved normalization job specifications.
- Profile fingerprints and provenance.
- Depth reconstruction boundaries.
- Depth event indexes.
- Reconstruction segment metadata.
- Integrity reports.
- Task and job metrics.
- Failure diagnostics.
- Any other artifacts produced by the normalization execution.

Preserve the actual production artifact formats and directory structures.

Do not simplify, truncate, or transform artifacts merely for test output storage.

Tests must still validate these artifacts before reporting success.

**Preserving an artifact does not mean the test passed.**

### Execution Isolation

Tests must never write to the production dataset paths specified in the original `jobs.json`.

Before execution, redirect all writable paths into the designated test output directory.

This includes:

```text
dataset_path
staging_path
scratch_path
report paths
other generated artifacts
```

Keep original source archives read-only.

Do not modify the authoritative fixture:

```text
engine/tests/historical/fixtures/jobs.json
```

The shared test helper must handle output path redirection consistently.

### Multiple Executions

Do not overwrite previous test results.

Every execution must receive a unique directory, even when the same test is executed repeatedly.

The directory naming mechanism must support concurrent test processes without collisions.

Use a unique execution identifier rather than relying only on the test name or process ID.

Avoid nondeterministic output identifiers in correctness assertions; execution identifiers are solely for filesystem isolation.

Tests must not depend on artifacts created during previous executions.

### Failure Preservation

When a test fails, retain the artifacts that were successfully generated before the failure.

This includes incomplete staging data and diagnostic reports where available.

Do not automatically delete failed test directories.

Do not bypass or weaken production transactional commit rules to preserve failed datasets.

A failed dataset must never be represented as successfully committed.

Preserve failed execution artifacts separately from committed datasets, maintaining their actual status.

### Test Output Metadata

Each persistent test execution should include a small machine-readable test metadata document.

For example:

```json
{
  "test_name": "bybit_spot_depth_profile",
  "profile_id": "bybit.spot.spot.depth",
  "status": "passed",
  "job_id": 12345,
  "task_ids": [1, 2],
  "dataset_path": "dataset",
  "staging_path": "staging"
}
```

This is illustrative test-run metadata, not a modification to the normalization batch protocol.

Include useful information such as:

- Test name.
- Execution identifier.
- Start and completion timestamps.
- Test status.
- Selected profile and source tasks.
- Source archive identities.
- Effective runtime configuration or configuration fingerprint.
- Paths to generated datasets and reports.
- Failure reason when applicable.

Use relative artifact paths within the test run directory where practical.

Ensure the metadata can distinguish a completed test from a process that terminated before finishing.

### No Automatic Cleanup

Do not implement automatic deletion of persistent normalization test outputs.

Specifically:

- Do not delete test datasets after assertions.
- Do not delete outputs in `Drop` implementations.
- Do not clear the output root before test execution.
- Do not automatically remove previous executions.
- Do not delete failed staging directories merely to make later tests pass.

Output retention is intentional.

The user will inspect and manage these artifacts manually.

### Exception — Pure Unit Tests

This requirement applies to tests that generate normalization execution artifacts.

Pure unit tests and small configuration-validation tests may continue using temporary directories when persistence provides no inspection value.

For example, a test that creates a temporary malformed TOML solely to verify error handling does not need to retain that file.

Do not unnecessarily change existing unit tests.

### Repository Hygiene

The persistent output directory contains generated data, potentially including large Parquet datasets.

Ensure it is excluded from version control.

Add an appropriate `.gitignore` rule if one does not already exist:

```gitignore
/data/tests/historical/normalize/
```

Do not commit generated datasets, reports, or temporary execution artifacts.

The fixture `jobs.json` and Rust test source files remain part of the test infrastructure and should not be ignored.

### Validation

The refactored test infrastructure must verify that:

1. Every artifact-producing normalization test writes under `data/tests/historical/normalize/`.
2. Each execution receives an isolated output directory.
3. Existing outputs are never overwritten.
4. Test artifacts remain available after the test process exits.
5. Failed tests preserve available diagnostic artifacts.
6. Tests do not write into production dataset directories.
7. Tests do not depend on previous executions.
8. Concurrent tests cannot collide or corrupt each other's outputs.
9. Dataset manifests and artifact references remain valid after execution.
10. Generated outputs are excluded from Git.

### Final Requirement

**Normalization integration-test artifacts are persistent inspection outputs, not disposable temporary files.**

The test suite must retain its correctness assertions, production validation behavior, and execution isolation while writing inspectable artifacts to:

```text
data/tests/historical/normalize/
```

Do not introduce automatic cleanup, automatic overwriting, or production-output modifications.


**Final objective: one reproducible workload definition, independently testable production profiles, shared safe test infrastructure, and useful existing coverage preserved without unnecessary unit-test rewrites.**


