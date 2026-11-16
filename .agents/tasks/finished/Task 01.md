````markdown
# Task 01 — Shared Historical Runtime Configuration

**Project:** MarketForge  
**Component:** Rust Historical Engine  
**Priority:** Foundation  
**Status:** Approved for implementation  
**Dependencies:** None  
**Next task:** Production v3 batch loading and unified normalization profiles

## 1. Objective

Refactor MarketForge's historical runtime configuration so that common configuration types, parsing, validation, and path resolution are implemented in reusable shared Rust infrastructure.

The authoritative configuration file is:

```text
data/.config/historical/general.toml
```

This configuration will be used by historical normalization and, where applicable, merge and conversion.

**The goal is to establish shared configuration infrastructure without changing existing processing behavior.**

This is the first task in a larger sequence of historical-engine refactors.

Complete only this task. Do not implement subsequent tasks.

## 2. Context

MarketForge consists of a Rust processing engine and Python orchestration.

The historical engine contains the following conceptual operations:

```text
Historical Engine
    │
    ├── Normalize
    ├── Merge
    └── Convert
```

All three operations may need access to common runtime settings.

These include:

- Worker limits.
- Memory budgets.
- Scratch-storage configuration.
- Parquet resource configuration.
- Integrity-policy definitions where applicable.

Historically, some configuration functionality was implemented inside normalization-specific modules.

The intention is to move genuinely reusable configuration responsibilities into `engine/src/shared/`.

However, each historical operation must retain control over its own execution behavior.

**Sharing configuration does not mean sharing schedulers, processors, or operation-specific algorithms.**

## 3. Required Architectural Direction

Use the following dependency direction:

```text
data/.config/historical/general.toml
                   │
                   ▼
          Shared TOML Loader
                   │
                   ▼
       Validated Runtime Config
                   │
          ┌────────┼────────┐
          │        │        │
          ▼        ▼        ▼
      Normalize   Merge   Convert
```

The shared configuration module must not depend on normalization, merge, or conversion implementation modules.

Avoid circular dependencies.

Prefer extending existing infrastructure instead of introducing a second configuration framework.

### Code Organization

Inspect the repository before choosing exact implementation locations.

Relevant areas include:

```text
engine/src/
├── shared/
│   └── ...
└── historical/
    ├── normalize/
    ├── merge/
    └── convert/
```

The preferred ownership is:

| Responsibility | Owner |
|---|---|
| Shared configuration structures | `shared` |
| Shared TOML parsing | `shared` |
| Shared configuration validation | `shared` |
| Shared path resolution | `shared` |
| Shared Parquet resource settings | Existing shared persistence/config infrastructure |
| Normalization execution policy | `historical/normalize` |
| Merge execution policy | `historical/merge` |
| Conversion execution policy | `historical/convert` |
| Operation-specific resource estimation | Relevant operation |
| Operation-specific integrity detection | Relevant operation |

Do not move functionality merely because it has the word `config`, `resource`, or `policy` in its name.

Move only genuinely reusable responsibilities.

## 4. Initial Repository Inspection

Before writing code, inspect the existing implementations and their consumers.

Focus on:

```text
engine/src/shared/
engine/src/shared/persistence/
engine/src/shared/persistence/config.rs

engine/src/historical/normalize/config.rs
engine/src/historical/normalize/resources/

engine/src/historical/merge/
engine/src/historical/convert/

engine/src/bin/marketforge-normalize.rs

data/.config/historical/general.toml
```

Inspect the actual project structure rather than assuming all paths or modules exist.

Identify the current definitions or equivalents of:

```rust
ProcessingConfig
ParquetResourceConfig
ResourceBudget
ResourceRequirements
IntegrityPolicy
IntegrityRule
```

Determine:

1. Which types already belong to shared infrastructure.
2. Which types unnecessarily duplicate shared structures.
3. Which validation functions are reusable.
4. Which path-resolution functions are duplicated.
5. Which public interfaces are currently consumed by other modules.
6. Which tests rely on legacy configuration paths.
7. Which historical operations actually consume these settings.

Use this inspection to design the smallest safe refactor.

Do not introduce generic traits, service registries, or elaborate configuration factories without a demonstrated need.

## 5. Authoritative Configuration Contract

The authoritative file is:

```text
data/.config/historical/general.toml
```

It currently uses (but at the time of the task may be updated):

```toml
schema_version = 1

[resources]
workers = 1
memory_budget_bytes = 1073741824
scratch_path = "data/.work/normalize"
scratch_budget_bytes = 1073741824

[resources.parquet]
row_group_target_bytes = 67108864
file_target_bytes = 268435456

[integrity_policy]
enabled = true

[integrity_policy.parse_failure]
action = "fail"
max_count = 1000

[integrity_policy.invalid_record]
action = "fail"
max_count = 1000

[integrity_policy.sequence_gap]
action = "fail"
max_count = 1000

[integrity_policy.timestamp_regression]
action = "fail"
max_count = 1000

[integrity_policy.missing_snapshot]
action = "fail"
max_count = 1000

[integrity_policy.invalid_book]
action = "fail"
max_count = 1000

[integrity_policy.transformation_failure]
action = "fail"
max_count = 1000
```

The actual file contains additional documentation and may support optional integrity-policy thresholds and windows.

Inspect the complete file and the existing deserialization models.

Do not replace its current structure with the abbreviated example above.

### Mandatory Preservation Rules

- Preserve `schema_version = 1`.
- Preserve all existing configuration values.
- Preserve existing optional supported fields.
- Preserve relevant comments.
- Preserve worker count.
- Preserve memory budgets.
- Preserve scratch configuration.
- Preserve Parquet sizing.
- Preserve integrity-policy thresholds.
- Preserve existing policy interpretation.
- Preserve supported path-resolution behavior.
- Preserve existing error semantics.

Do not tune performance during this task.

Do not increase memory budgets because the development machine has substantial RAM.

Do not silently change runtime defaults.

## 6. Shared Configuration Model

Introduce or reuse one strongly typed representation of the shared historical runtime configuration.

Conceptually:

```rust
struct HistoricalRuntimeConfig {
    resources: ResourceConfig,
    integrity_policy: IntegrityPolicy,
}
```

The exact names should follow existing project conventions.

Prefer reusing existing types where possible.

### Resource Configuration

The shared resource representation should cover:

```text
workers
memory_budget_bytes
scratch_path
scratch_budget_bytes
parquet
```

Parquet configuration contains:

```text
row_group_target_bytes
file_target_bytes
```

Avoid unnecessary duplication.

For example, do not maintain three identical configuration structures:

```rust
NormalizationResourceConfig
MergeResourceConfig
ConversionResourceConfig
```

if they only contain the same fields and semantics.

However, do not force an operation to consume settings it does not currently use.

### Immutable Configuration

The resulting runtime configuration should be validated before execution and thereafter treated as immutable.

Operations may receive references or cloned configuration values where ownership requires it.

Avoid global mutable state.

Avoid reparsing TOML for each task.

Do not introduce configuration hot-reloading.

## 7. Shared Configuration Loader

Implement or reuse a shared loader with a clearly defined interface.

Conceptually:

```rust
load_historical_config(path, project_root)
```

This is illustrative naming.

### Required Behavior

The loader must:

1. Read the requested TOML file.
2. Deserialize it into strongly typed structures.
3. Validate the schema version.
4. Validate required sections.
5. Reject unsupported fields.
6. Validate resource settings.
7. Validate Parquet settings.
8. Validate integrity-policy configuration.
9. Resolve applicable filesystem paths.
10. Return the validated configuration.

Use the established MarketForge error-handling infrastructure.

Errors should identify the relevant file and invalid field where practical.

### Path Resolution

Paths such as:

```toml
scratch_path = "data/.work/normalize"
```

must resolve relative to the explicitly supplied project root.

Do not resolve paths using arbitrary assumptions about the shell's working directory.

Absolute paths must remain absolute.

Do not mutate global process working-directory state.

### Side Effects

Configuration loading and validation should not start processing, create datasets, or allocate execution resources.

Avoid unnecessary filesystem writes during loading.

Keep loading and validation separate from runtime execution.

## 8. Resource Validation

Preserve or implement strict validation.

### Worker Count

```text
workers > 0
```

Reject zero workers.

### Memory Budget

```text
memory_budget_bytes > 0
```

Reject zero or invalid memory budgets.

### Scratch Budget

```text
scratch_budget_bytes > 0
```

Reject zero or invalid scratch budgets.

### Scratch Path

Reject empty paths and invalid path configurations.

Maintain the existing behavior for relative and absolute paths.

### Parquet Settings

Require:

```text
row_group_target_bytes > 0
file_target_bytes > 0
row_group_target_bytes <= file_target_bytes
```

Use the existing Parquet configuration representation where appropriate.

### Numeric Safety

Reject unsupported negative values, integer overflow, and invalid numeric conversions.

Do not silently clamp invalid values.

Do not substitute arbitrary defaults for explicitly invalid configuration.

## 9. Integrity Policy Configuration

The shared runtime configuration includes the existing integrity-policy definitions.

Supported categories include:

```text
parse_failure
invalid_record
sequence_gap
timestamp_regression
missing_snapshot
invalid_book
transformation_failure
```

Preserve existing rule fields and their semantics.

These may include:

```text
action
max_count
max_rate
minimum_samples
windows.hourly
windows.daily
```

### Critical Requirement: Do Not Change Policy Behavior

The configuration contains rules such as:

```toml
[integrity_policy.sequence_gap]
action = "fail"
max_count = 1000
```

Inspect the actual evaluator.

If `action = "fail"` requires immediate failure, preserve that behavior.

Do not reinterpret the count threshold as permission to tolerate 1,000 violations.

Do not change the policy engine under this task.

### Shared Policy Versus Shared Processing

It may be appropriate to share:

- Policy configuration structures.
- Integrity rule types.
- Threshold validation.
- Common policy evaluation primitives.

It is not necessarily appropriate to share:

- Source-specific integrity detection.
- Depth reconstruction recovery.
- Trade normalization rejection handling.
- Merge continuity decisions.
- Conversion-specific validation behavior.

Preserve operation-specific responsibilities.

### Deferred Correctness Work

A later task will address known issues involving:

- Eligible integrity observation accounting.
- Time-window attribution.
- Timestamp-regression detection.
- Fatal source-read errors.
- Profile behavior enforcement.

**Do not implement those correctness changes during Task 01.**

This task must preserve current runtime policy behavior while consolidating configuration ownership.

If an existing policy inconsistency is discovered, document it for the later correctness tasks.

## 10. Ordering Configuration

The existing `general.toml` contains a commented section:

```toml
# [ordering]
# primary = "event_timestamp_ns"
# tie_break = "stream_rank"
```

Do not uncomment it automatically.

The prepared normalization batch already contains its own ordering declaration:

```json
{
  "ordering": {
    "primary": "event_timestamp_ns",
    "tie_break": "stream_rank"
  }
}
```

Do not introduce conflicting sources of truth.

Inspect the existing ordering types and consumers.

Reuse common ordering validation if it already exists and is genuinely applicable.

Do not implement new sorting, merging, or ordering behavior.

Do not change the authoritative batch ordering contract.

## 11. Integrate Normalization

Normalization is the primary consumer to update during this task.

Make normalization use the shared runtime configuration infrastructure.

### Required Changes

- Replace duplicated configuration parsing where safe.
- Replace duplicated resource structures where safe.
- Reuse shared validation.
- Update relevant imports and consumers.
- Preserve effective runtime values.
- Preserve scheduler behavior.
- Preserve existing source-processing semantics.
- Preserve output behavior.
- Preserve existing error handling.

### CLI Configuration

Inspect:

```text
engine/src/bin/marketforge-normalize.rs
```

Update its default configuration path to:

```text
data/.config/historical/general.toml
```

Preserve explicit configuration-path overrides.

Do not modify the CLI input protocol or execution-result JSON contract.

Those belong to later tasks.

### Compatibility

Preserve existing public Rust interfaces when doing so requires little additional complexity.

If an interface must change because the previous ownership is inappropriate, update its direct consumers.

Do not retain parallel implementations of the same loader indefinitely merely to avoid updating imports.

Simple re-exports or compatibility aliases are acceptable where they prevent unnecessary disruption.

Avoid permanent compatibility layers that duplicate logic.

## 12. Integrate Merge and Conversion Conservatively

Inspect the existing merge and conversion implementations.

Identify whether they already have resource, Parquet, or integrity configuration that should reuse the shared types.

### Required Approach

Reuse shared infrastructure where there is a genuine existing consumer.

Do not force integration simply to demonstrate reuse.

Do not modify merge or conversion processing algorithms.

Do not change their execution behavior.

Do not introduce new configuration requirements for currently unsupported operations.

### Expected Result

After this task, common historical configuration infrastructure must be available for normalization, merge, and conversion.

Normalization must use it.

Merge and conversion should reuse it only where currently appropriate.

If a subsystem has no suitable consumer yet, explain that in the implementation report.

**Do not create placeholder runtime integrations solely for architectural symmetry.**

## 13. Essential Configuration Extensions

You are authorized to add configuration fields only when an essential requirement is demonstrated by the current codebase.

Examples of potentially justified additions include:

- A shared resource limit currently duplicated between operations.
- A configuration value already enforced but not represented centrally.
- A required validation rule that prevents invalid runtime settings.

Every addition must have:

- A specific purpose.
- A real consumer.
- A defined Rust type.
- Validation behavior.
- A documented default or required-value policy.
- Focused tests.
- An explanation in the final report.

Do not add speculative settings.

Do not introduce options for future features.

Do not add configuration for the planned stream-oriented writer refactor.

Do not silently change the schema version.

If a necessary extension is incompatible with schema version 1, report the issue rather than disguising an incompatible change as a compatible one.

## 14. TOML Formatting

Preserve the current configuration's readability.

Follow these conventions when modifying TOML:

- Prefer standard TOML tables.
- Keep configuration fields on separate lines.
- Avoid unnecessarily long lines.
- Separate logical sections clearly.
- Preserve useful comments.
- Preserve current values.
- Avoid unrelated formatting changes.
- Validate TOML syntax after editing.

Do not reformat unrelated profile TOMLs.

The unified normalization-profile migration belongs to Task 02.

## 15. Testing Requirements

Implement focused configuration tests.

Do not reorganize the complete historical normalization test suite.

That belongs to later tasks.

### Loader Tests

Verify:

- Valid `general.toml` loads.
- Schema version 1 is accepted.
- Unsupported schema versions fail.
- Missing required sections fail.
- Unknown fields fail.
- Invalid TOML produces actionable errors.
- Relative paths resolve against the project root.
- Absolute paths remain unchanged.
- Loading is independent of the invocation directory.

### Resource Tests

Verify:

- Zero workers fail.
- Zero memory budgets fail.
- Zero scratch budgets fail.
- Empty scratch paths fail.
- Zero Parquet row-group targets fail.
- Zero Parquet file targets fail.
- Row-group targets larger than file targets fail.
- Invalid numeric values are rejected.

### Integrity Configuration Tests

Verify:

- Existing policy rules deserialize correctly.
- Supported optional thresholds remain accepted.
- Invalid rates fail validation.
- Unsupported actions fail.
- Supported window rules load.
- Existing immediate-failure semantics remain unchanged.
- Existing degradation semantics remain unchanged.

### Integration Compatibility

Verify that normalization receives the same effective runtime configuration before and after the refactor.

Use deterministic configuration fixtures for tests that must assert specific values.

Do not make generic validation tests depend on whichever resource budget the developer currently prefers.

### Preserve Existing Tests

Do not delete useful unit tests.

Update tests affected by configuration ownership or path changes.

Where existing integration tests depend on legacy private job files, do not rewrite the entire fixture system in Task 01.

Report those dependencies for Task 03 or Task 06.

Do not bypass failing tests by weakening assertions.

## 16. Verification Commands

After implementation, run appropriate checks using the existing Cargo structure.

Suggested commands:

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Run the relevant configuration unit tests and compilation checks for affected binaries and feature combinations.

Inspect actual Cargo targets and test names before selecting commands.

Run the smallest relevant verification set that establishes the configuration refactor's correctness.

Do not launch full historical archive processing.

Do not execute benchmarks.

Do not run unrelated long-running real-data tests.

If the existing test suite contains unrelated failures or missing real-data prerequisites, report them accurately.

## 17. Implementation Boundaries

### Authorized

This task authorizes:

- Shared historical configuration types.
- Shared TOML loading.
- Shared configuration validation.
- Shared path resolution.
- Reuse of existing Parquet configuration.
- Reuse of existing integrity-policy structures.
- Minimal integration into normalization.
- Applicable reuse in merge and conversion.
- Updating stale runtime configuration paths.
- Essential, justified configuration extensions.
- Focused unit tests.
- Minimal documentation updates.

### Not Authorized

Do not implement:

- The schema-version-3 batch migration.
- Unified normalization-profile migration.
- The normalization test-suite reorganization.
- Persistent integration-test output infrastructure.
- Integrity observation-accounting corrections.
- Timestamp-regression detection.
- Source-error handling changes.
- CLI JSON response redesign.
- Dataset manifest redesign.
- Benchmarking refactors.
- Stream-oriented Parquet consolidation.
- New merge algorithms.
- New conversion algorithms.
- PostgreSQL registry integration.

Do not change:

- Canonical data schemas.
- Parquet writer behavior.
- Existing normalization transformations.
- Dataset output layouts.
- Task scheduling algorithms.
- Depth reconstruction semantics.
- Existing runtime configuration values.

Do not refactor unrelated modules.

## 18. Working-Tree Safety

Inspect the working tree before modifying files.

Preserve unrelated user changes.

Do not reset or overwrite existing modifications.

Do not create unnecessary commits or branches unless explicitly requested.

Prefer minimal changes with clear ownership.

If a partially completed configuration refactor already exists, build on it rather than duplicating functionality.

## 19. Required Implementation Report

When finished, provide a concise but complete report.

### Architecture

Describe:

- Shared modules added or modified.
- Configuration types reused.
- Configuration types introduced.
- Parsing and validation ownership.
- Any compatibility interfaces retained.

### Consumers

Describe:

- Normalization integration.
- Merge integration, if applicable.
- Conversion integration, if applicable.
- Any intentionally unchanged consumers.

### Configuration

State:

- Whether `general.toml` changed.
- Whether values changed.
- Whether schema version 1 was preserved.
- Any essential fields added and why.

### Verification

List:

- Formatting checks.
- Compilation checks.
- Tests executed.
- Tests passed.
- Tests failed.
- Tests blocked by missing prerequisites.

Do not claim tests passed unless they were executed successfully.

### Remaining Work

Identify dependencies or issues belonging to later tasks.

Do not implement those later tasks.

## 20. Acceptance Criteria

Task 01 is complete when:

- `general.toml` is the authoritative historical runtime configuration.
- Its schema remains version 1.
- All existing configuration values are preserved.
- Common configuration types are reusable through shared Rust infrastructure.
- Common parsing and validation logic is centralized.
- Normalization uses the shared configuration infrastructure.
- The normalization CLI defaults to `general.toml`.
- Explicit configuration overrides continue to work.
- Existing integrity-policy semantics remain unchanged.
- Parquet resource validation remains correct.
- Relative and absolute paths resolve predictably.
- Merge and conversion can reuse relevant shared infrastructure without being forced into normalization-specific behavior.
- No unnecessary new configuration fields are introduced.
- No normalization, merge, or conversion processing algorithms are rewritten.
- Relevant compilation and configuration tests pass.
- Any unresolved compatibility issues are reported.

## 21. Stop Condition

**Stop after Task 01 is complete.**

Do not automatically proceed to Task 02.

Do not attempt to complete the entire historical-engine refactor in one implementation.

The next task will address the authoritative schema-version-3 prepared normalization batch and unified profile resolution.

Those changes must remain separate from this configuration refactor.

---

**Final objective:** Establish one authoritative historical runtime configuration and reusable shared Rust infrastructure while preserving existing engine behavior and preparing a clean foundation for the remaining MarketForge historical-engine tasks.
````