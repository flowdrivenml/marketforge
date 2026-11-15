# MarketForge — Shared Historical Runtime Configuration Refactor

## Objective

Refactor MarketForge's historical runtime configuration infrastructure so common settings and their loading, validation, and resolution logic can be reused across historical engine operations.

The authoritative configuration file is:

```text
data/.config/historical/general.toml
```

This file currently contains:

- Runtime resource limits.
- Worker configuration.
- Scratch storage configuration.
- Parquet resource configuration.
- Integrity policies.
- A commented-out ordering configuration proposal.

These settings should be represented through reusable Rust types and configuration-loading infrastructure.

**Primary goal: centralize shared configuration without introducing unnecessary abstractions or changing existing behavior.**

You are authorized to implement this refactor now.

## 1. Architectural Requirements

The Rust engine currently separates functionality into:

```text
engine/src/
├── historical/
│   ├── normalize/
│   ├── merge/
│   └── convert/
└── shared/
```

Inspect the actual repository structure before making changes.

The goal is to introduce or reuse common configuration infrastructure under:

```text
engine/src/shared/
```

Prefer extending existing shared modules where appropriate.

Do not create a second configuration framework if equivalent functionality already exists.

### Intended Dependency Direction

```text
data/.config/historical/general.toml
                   │
                   ▼
          Shared Config Loader
                   │
                   ▼
       Validated Configuration
                   │
          ┌────────┼────────┐
          │        │        │
          ▼        ▼        ▼
      Normalize   Merge   Convert
```

The shared configuration infrastructure must not depend on normalization-specific code.

Avoid circular module dependencies.

Historical operations should consume reusable types and resolved settings rather than maintaining independent implementations of common configuration functionality.

## 2. Inspect Existing Infrastructure First

Before implementing changes, inspect:

- `engine/src/shared/`
- `engine/src/shared/persistence/`
- `engine/src/historical/normalize/config.rs`
- Existing configuration-loading utilities.
- Existing resource configuration types.
- Existing Parquet resource configuration types.
- Existing integrity policy definitions.
- Existing resource-budgeting infrastructure.
- Historical merge and conversion configuration consumers.
- CLI configuration loading.
- Relevant unit and integration tests.

Determine which abstractions already exist.

Specifically, inspect whether the following types or equivalent implementations are already defined:

```rust
ProcessingConfig
ParquetResourceConfig
ResourceBudget
ResourceRequirements
IntegrityPolicy
IntegrityRule
```

Do not blindly move every type into `shared`.

Only share concepts that are genuinely reusable.

Keep implementation-specific behavior in the appropriate historical operation.

## 3. Authoritative Configuration File

The current configuration is:

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

The actual file contains additional comments explaining these settings.

Preserve those comments where appropriate.

### Mandatory Rules

- Keep `schema_version = 1`.
- Preserve all existing configuration values.
- Preserve current runtime behavior.
- Do not change worker counts.
- Do not increase or decrease memory budgets.
- Do not change Parquet sizing.
- Do not change integrity-policy thresholds.
- Do not silently change error-handling semantics.
- Do not introduce operation-specific defaults that override these values unexpectedly.

The current values are authoritative starting values.

This task is about sharing configuration infrastructure, not tuning the engine.

## 4. Shared Configuration Types

Create or refactor reusable Rust configuration types for genuinely common settings.

The preferred conceptual organization is:

```text
SharedHistoricalConfig
    │
    ├── ResourceConfig
    │      ├── workers
    │      ├── memory_budget_bytes
    │      ├── scratch_path
    │      ├── scratch_budget_bytes
    │      └── parquet
    │             ├── row_group_target_bytes
    │             └── file_target_bytes
    │
    └── IntegrityPolicy
           ├── enabled
           ├── parse_failure
           ├── invalid_record
           ├── sequence_gap
           ├── timestamp_regression
           ├── missing_snapshot
           ├── invalid_book
           └── transformation_failure
```

This is a conceptual design, not a mandatory naming scheme.

Use naming consistent with the existing Rust codebase.

Prefer reusing existing structs over introducing nearly identical replacements.

### Important: Avoid Unnecessary Duplication

Do not create:

```rust
NormalizationResourceConfig
MergeResourceConfig
ConversionResourceConfig
```

if they only duplicate the same fields and semantics.

Instead, define reusable resource types and allow operations to consume the relevant settings.

However, do not force unrelated operation-specific settings into one struct merely because they are all configuration.

For example, a Parquet writer configuration is reusable, but a normalization-specific reconstruction policy is not necessarily meaningful to merge or conversion.

**Share configuration primitives and validation where semantics are common. Preserve operation-specific behavior where they differ.**

## 5. Shared Configuration Loader

Implement or reuse a single authoritative TOML-loading function.

Conceptually:

```rust
load_historical_config(path, project_root)
```

The loader should:

1. Read the TOML file.
2. Deserialize it into typed Rust configuration structures.
3. Validate the schema version.
4. Reject invalid or unsupported fields.
5. Validate resource settings.
6. Validate integrity policies.
7. Resolve filesystem paths.
8. Return the validated configuration.

Use the existing error-handling infrastructure.

Do not introduce an unnecessary configuration dependency.

### Path Resolution

Relative paths must resolve against the explicit project root.

For example:

```toml
scratch_path = "data/.work/normalize"
```

must not be interpreted relative to an arbitrary current working directory.

Absolute paths must remain absolute.

Preserve existing path-validation behavior.

### Configuration Loading Efficiency

Configuration should normally be loaded and validated once per execution.

Historical operations may receive references or shared immutable configuration objects.

Do not repeatedly parse the same TOML for every task or source record.

Avoid global mutable configuration state.

Do not introduce complex runtime configuration hot-reloading.

## 6. Resource Configuration

The common resource configuration includes:

```toml
[resources]
workers = 1
memory_budget_bytes = 1073741824
scratch_path = "data/.work/normalize"
scratch_budget_bytes = 1073741824
```

These fields must remain typed and validated.

### Validation

Preserve or implement checks ensuring:

- Worker count is greater than zero.
- Memory budget is positive.
- Scratch budget is positive.
- Scratch path is nonempty.
- Numeric conversions do not overflow.
- Invalid values produce actionable errors.

Do not create directories unnecessarily during configuration parsing.

Keep configuration validation separate from resource allocation and execution.

### Resource Budgeting

Inspect existing resource-budgeting implementations.

Reuse their common accounting primitives where appropriate.

Do not move the complete normalization scheduler into `shared` merely because it consumes resource configuration.

The scheduler owns execution policy.

Shared resource configuration describes available limits.

These are different responsibilities.

## 7. Parquet Configuration

The shared Parquet configuration is:

```toml
[resources.parquet]
row_group_target_bytes = 67108864
file_target_bytes = 268435456
```

Preserve the existing configuration structure.

### Validation

Ensure:

```text
row_group_target_bytes > 0
file_target_bytes > 0
row_group_target_bytes <= file_target_bytes
```

Reuse existing validation functions where possible.

Do not create separate validation implementations for normalization, merge, and conversion when the rules are identical.

### Scope Restriction

Do not redesign Parquet writers during this task.

Do not change:

- Compression algorithms.
- Row-group flushing behavior.
- File rotation.
- Canonical schemas.
- Output partitioning.
- Dataset manifests.

The previously discussed Parquet I/O refactor is a separate task.

This task only standardizes configuration ownership and reuse.

## 8. Integrity Policy Configuration

The shared TOML contains:

```toml
[integrity_policy]
enabled = true
```

and rule definitions for:

```text
parse_failure
invalid_record
sequence_gap
timestamp_regression
missing_snapshot
invalid_book
transformation_failure
```

Each rule may support:

```text
action
max_count
max_rate
minimum_samples
windows.hourly
windows.daily
```

Preserve the existing definitions and serialization behavior.

### Shared Versus Operation-Specific Semantics

The configuration data structures, threshold types, and validation logic may be shared when genuinely equivalent.

However, do not assume every historical operation produces the same integrity categories.

For example:

- Normalization may detect source parsing failures.
- Depth normalization may detect missing snapshots and sequence gaps.
- Merge may detect cross-source ordering or continuity problems.
- Conversion may detect unavailable references or arithmetic failures.

Do not force merge or conversion to manufacture irrelevant integrity observations.

Share the policy representation where appropriate.

Keep operation-specific detection and evaluation logic in the correct modules.

### Important: Verify Existing Threshold Semantics

The current TOML contains rules such as:

```toml
[integrity_policy.sequence_gap]
action = "fail"
max_count = 1000
```

Inspect the existing implementation carefully.

Determine whether `action = "fail"` causes immediate failure regardless of `max_count`.

If so, preserve this behavior during the refactor.

Do not assume that `max_count = 1000` permits 1,000 sequence gaps.

Do not silently reinterpret or modify the configured threshold semantics.

Report any mismatch between configuration comments and actual implementation behavior.

Any semantic redesign of integrity-policy actions must be proposed separately.

## 9. Ordering Configuration

The existing `general.toml` contains a commented-out ordering section:

```toml
# [ordering]
# primary = "event_timestamp_ns"
# tie_break = "stream_rank"
```

These values are intentionally not active.

The current prepared normalization batch already contains:

```json
{
  "ordering": {
    "primary": "event_timestamp_ns",
    "tie_break": "stream_rank"
  }
}
```

### Required Behavior

Do not automatically uncomment the TOML ordering section.

Do not introduce a second competing source of truth for ordering.

Inspect how ordering is currently validated and consumed by normalization, merge, and conversion.

If common ordering types or validation functions already exist, reuse them.

The existing batch ordering contract must not be changed by this refactor.

If shared ordering configuration becomes genuinely necessary, document:

- Which component owns the ordering contract.
- Why runtime configuration is required.
- How conflicts with job-level ordering are prevented.
- Whether the same ordering applies to all historical operations.

Do not implement additional ordering settings solely because a commented example exists.

## 10. Integrating Historical Operations

### Normalization

Normalization currently uses a processing-specific configuration model.

Refactor it to consume the shared configuration infrastructure.

Preserve its existing public functionality and runtime semantics.

Update configuration-loading paths so the authoritative runtime configuration is:

```text
data/.config/historical/general.toml
```

Eliminate obsolete normalization-specific configuration duplication where safe.

Do not preserve duplicate configuration loaders merely for convenience.

### Merge

Inspect the current merge implementation.

Identify which shared settings it can genuinely reuse.

For example:

- Memory limits.
- Worker or concurrency limits where applicable.
- Scratch storage.
- Parquet settings where it writes Parquet.

Do not rewrite merge algorithms or change their execution strategy.

Do not force merge to consume normalization-specific settings.

If merge already has operation-specific configuration, preserve those fields and compose them with shared settings where appropriate.

### Conversion

Apply the same principles to conversion.

Reuse shared configuration types and loading logic when applicable.

Preserve conversion-specific behavior and settings.

Do not change conversion arithmetic, reference resolution, or canonical output behavior.

### Important

Do not mechanically migrate all historical modules simply to maximize usage of the new shared types.

Prefer minimal, correct integration over unnecessary architectural churn.

If a subsystem does not currently consume a shared setting, do not invent behavior that makes it consume that setting.

## 11. Permission to Extend Configuration

You are authorized to extend `general.toml` or shared configuration types **only when an essential requirement is demonstrated by existing code or tests**.

Examples of potentially justified extensions:

- A missing resource limit already enforced by a historical operation.
- A shared configuration field that currently exists in duplicated code.
- A missing validation rule necessary to prevent invalid runtime configuration.
- A genuinely common option required by more than one historical operation.

### Restrictions

Every new setting must have:

1. A clearly defined operational purpose.
2. A concrete consumer.
3. A documented type.
4. A validation rule.
5. An explicit default or required-value policy.
6. Corresponding tests.
7. A short explanation in the final implementation report.

Do not add speculative configuration fields.

Do not introduce settings for functionality that has not been implemented.

Do not add configuration merely because it could be useful someday.

Do not silently change schema versioning.

If a necessary addition cannot be represented compatibly within schema version 1, report the required version change rather than introducing incompatible behavior under the same version.

**Only essential, justified additions are preapproved.**

## 12. TOML Formatting

Keep the configuration clean and readable.

Apply the established MarketForge TOML conventions:

- Prefer standard TOML tables over large nested inline tables.
- Keep configuration values on separate lines.
- Avoid excessively long lines.
- Separate sections with exactly one blank line.
- Preserve logical grouping and key ordering.
- Preserve useful existing comments.
- Use multiline arrays when appropriate.
- Preserve configuration values during formatting.
- Validate TOML syntax after modifications.

Do not reformat unrelated TOML files.

Do not alter runtime behavior solely for formatting.

## 13. Testing Requirements

Add or update focused tests for the shared configuration infrastructure.

### Configuration Loading

Verify:

- Valid TOML loads successfully.
- Schema version 1 is accepted.
- Unsupported schema versions are rejected.
- Unknown fields are rejected.
- Missing required sections are rejected.
- Invalid numeric values fail clearly.
- Relative paths resolve against the explicit project root.
- Absolute paths remain unchanged.
- Configuration loading does not depend on the invoking working directory.

### Resource Validation

Verify:

- Zero workers are rejected.
- Zero memory budgets are rejected.
- Zero scratch budgets are rejected.
- Empty scratch paths are rejected.
- Invalid Parquet size targets are rejected.
- Row-group targets larger than file targets are rejected.
- Numeric overflow is handled safely.

### Integrity Policy

Verify:

- Existing integrity rules deserialize correctly.
- Existing thresholds retain their semantics.
- Invalid rates are rejected.
- Unsupported actions are rejected.
- Valid window thresholds are accepted.
- Existing immediate-failure behavior remains unchanged.
- Existing degradation behavior remains unchanged.

### Consumer Compatibility

Verify that normalization receives the same effective settings before and after the refactor.

Where merge and conversion already consume equivalent settings, verify they resolve the shared values correctly.

Do not make tests depend on the developer's local machine resources.

Preserve existing useful unit tests.

Modify tests only when required by the new configuration ownership or an actual interface change.

## 14. Scope Restrictions

This task authorizes:

- Shared configuration types.
- Shared TOML loading and validation.
- Reuse of existing resource and integrity configuration models.
- Minimal integration into historical operations.
- Updating stale configuration paths.
- Essential configuration additions.
- Focused tests.
- Minimal documentation updates where needed.

This task does **not** authorize:

- Rewriting normalization algorithms.
- Rewriting merge algorithms.
- Rewriting conversion algorithms.
- Changing canonical schemas.
- Changing batch JSON protocols.
- Redesigning Parquet writers.
- Implementing stream-oriented output consolidation.
- Changing dataset registry behavior.
- Adding PostgreSQL dependencies.
- Changing integrity thresholds without explicit justification.
- Introducing unrelated configuration options.
- Reorganizing the entire Rust engine.

Avoid unrelated cleanup.

Preserve existing working-tree changes.

## 15. Expected Implementation Approach

Start by examining the current configuration architecture.

Identify reusable types and duplicated functionality.

Then implement the smallest appropriate shared abstraction.

Update the existing normalization configuration consumer.

Integrate merge and conversion only where common functionality already exists and can be reused safely.

Update affected tests.

Run formatting, compilation checks, and relevant unit/integration tests.

Do not rewrite the entire module hierarchy.

Do not introduce complicated generic frameworks or unnecessary traits.

Prefer straightforward, strongly typed Rust code.

Keep validation strict and errors actionable.

## 16. Final Implementation Report

After implementation, report:

### Shared Infrastructure

- Existing shared types reused.
- New shared types introduced.
- Configuration loaders consolidated.
- Validation logic consolidated.

### Consumers

- Normalization changes.
- Merge changes.
- Conversion changes.
- Any subsystem intentionally left unchanged.

### Configuration

- Whether `general.toml` changed.
- Every new field, if any.
- Why each addition was necessary.
- Whether schema version 1 was preserved.

### Compatibility

- Previous behavior preserved.
- Any changed configuration-loading paths.
- Any remaining duplicated configuration logic.
- Any unresolved incompatibilities.

### Tests

- Tests executed.
- Tests passed.
- Tests failed.
- Tests blocked by missing prerequisites.

Do not claim compatibility or test success without verification.

## 17. Acceptance Criteria

The refactor is complete when:

- `general.toml` is the authoritative shared historical runtime configuration.
- Common configuration types live in appropriate shared Rust modules.
- Duplicate loaders and validators are removed where safe.
- Normalization consumes the shared configuration correctly.
- Merge and conversion can reuse common configuration infrastructure without being forced into normalization-specific behavior.
- All existing configuration values are preserved.
- Existing integrity-policy semantics remain unchanged.
- The current batch and canonical data protocols remain unchanged.
- Configuration loading is independent of the shell's working directory.
- Configuration validation remains strict.
- No unnecessary configuration fields are introduced.
- Relevant tests pass.
- No unrelated processing algorithms are modified.

**Final objective: One authoritative historical runtime configuration, reusable shared Rust infrastructure, and clean separation between common resource policies and operation-specific processing behavior.**