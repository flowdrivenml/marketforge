# Task 02 — Production V3 Batch Loading and Unified Normalization Profiles

**Project:** MarketForge  
**Component:** Rust Historical Normalization Engine  
**Priority:** Foundation / Input Contract  
**Status:** Approved for implementation  
**Dependencies:** Task 01 — Shared Historical Runtime Configuration  
**Next task:** Shared Normalization Test Infrastructure and Persistent Test Outputs

## 1. Objective

Finalize the Rust historical normalization input infrastructure so that the engine correctly consumes the authoritative **schema-version-3 prepared normalization batch** and resolves source formats through the new **self-contained TOML normalization profiles**.

Python Forge prepares normalization batches. Rust owns their validation, profile resolution, and conversion into executable normalization jobs.

The primary objective is:

**One authoritative batch protocol, one authoritative set of self-contained normalization profiles, and one production resolution path.**

Do not maintain competing legacy and modern configuration architectures.

This task is primarily about input loading, validation, profile resolution, and compatibility with the existing normalization engine.

It is not an opportunity to rewrite the normalization algorithms.

Complete only Task 02.

---

## 2. Architectural Context

The intended architecture is:

```text
Python Forge
    │
    ▼
Prepared Normalization Batch V3
    │
    ▼
Rust Batch Loader
    │
    ├── Validate batch
    ├── Validate jobs and tasks
    ├── Load shared runtime configuration
    └── Resolve normalization profiles
               │
               ▼
      Self-Contained TOML Profiles
               │
               ▼
       Resolved Internal Jobs
               │
               ▼
      Existing Normalization Engine
               │
               ├── Trades
               └── Depth
```

### Responsibility Boundaries

Python Forge owns:

- Source archive discovery.
- Source selection.
- Instrument metadata preparation.
- Run, job, task, and dataset identities.
- Prepared normalization-batch generation.
- Output destination planning.

Rust owns:

- Batch validation.
- Source configuration validation.
- Profile discovery and resolution.
- Raw-source interpretation.
- Normalization-rule compilation.
- Conversion into executable internal jobs.
- Validation of profile and task compatibility.

Do not introduce PostgreSQL requirements.

Do not make Rust depend on Forge's Python implementation.

The JSON and TOML contracts are the integration boundary.

---

## 3. Inspect the Current Implementation First

Before modifying the repository, inspect:

```text
engine/src/historical/job/
engine/src/historical/config/
engine/src/historical/normalize/

engine/src/shared/formats/
engine/src/shared/normalization/
engine/src/shared/io/

engine/src/bin/marketforge-normalize.rs

data/.config/historical/
data/.config/historical/general.toml
```

Use the actual repository structure where it differs.

Identify the existing implementations of:

```rust
ProcessingJob
WorkTask
FormatCatalog
load_normalization_job
load_document_with_catalog
```

Also inspect:

- Existing profile discovery.
- Profile fingerprint generation.
- Source-format resolution.
- Normalization-rule compilation.
- Instrument matching.
- Sequence-policy resolution.
- Source-container detection.
- Depth feed interpretation.
- Family-scoped archive handling.
- Existing schema-v3 models and validators.
- Relevant tests and documentation.

Some functionality may already be implemented.

**Do not replace working implementations merely because their names or internal structures differ from this specification.**

Identify the actual gaps first.

---

# 4. Authoritative Normalization Batch V3

The external input contract is a prepared normalization batch.

The top-level structure is:

```json
{
  "schema_version": 3,
  "kind": "normalization_batch",
  "status": "prepared",
  "run_id": "normalize-example",
  "run_directory": "/data/runs/normalize-example",
  "batch_path": "/data/runs/normalize-example/normalize-batch.json",
  "ranking_snapshot": null,
  "ranking_policy": "turnover",
  "requested_interval": {
    "start": "2026-10-27T00:00:00Z",
    "end": "2026-10-30T00:00:00Z"
  },
  "jobs": [
    {
      "operation": "normalize",
      "job_id": 1001,
      "dataset_id": 2001,
      "tasks": [],
      "ordering": {
        "primary": "event_timestamp_ns",
        "tie_break": "stream_rank"
      },
      "output": {
        "content_type": "trades",
        "staging_path": "/data/normalized/.2001.staging",
        "dataset_path": "/data/normalized/2001"
      }
    }
  ]
}
```

This example illustrates the structure. A valid executable job requires appropriate tasks.

Use the actual existing v3 producer contract as the source of truth for required fields, optional fields, and their accepted values.

Do not introduce a different schema because it is more convenient for Rust.

## 4.1 Version Validation

The loader must validate:

```text
schema_version = 3
kind = normalization_batch
status = prepared
```

Reject unsupported versions, kinds, and statuses.

An input marked `prepared` describes a workload ready for execution.

It does not mean normalization has already succeeded.

## 4.2 Batch Metadata

Preserve legitimate batch metadata such as:

```text
run_id
run_directory
batch_path
ranking_snapshot
ranking_policy
requested_interval
```

Do not discard this information unnecessarily.

However, do not invent ranking policies or reinterpret source selection inside Rust.

Forge has already selected the source workload.

The Rust loader must not require PostgreSQL or reproduce Forge's ranking process.

## 4.3 Job Validation

Each normalization job includes:

```text
operation
job_id
dataset_id
tasks
ordering
output
```

Validate:

- Supported operation.
- Job identity.
- Dataset identity.
- Task collection.
- Ordering declaration.
- Content type.
- Staging path.
- Final dataset path.
- Conflicting output destinations.
- Duplicate identities.
- Unsupported combinations of fields.

Preserve 64-bit integer identifiers exactly.

Do not convert them through floating-point representations.

Do not silently generate replacement identities for invalid or missing identifiers.

### Job Independence

A batch can contain multiple jobs.

Each job retains its own:

```text
job_id
dataset_id
tasks
output paths
success/failure boundary
```

Do not combine jobs merely because they contain related instruments.

The existing session executor remains responsible for scheduling executable jobs.

---

# 5. Task Contract

The prepared task identifies the source archive and the instrument interpretation required for normalization.

Typical fields include:

```text
task_id
input_path
exchange
instrument_type
market_category
base_asset
quote_asset
symbol
instrument
stream_id
stream_rank
```

Additional source-specific fields may include:

```text
scope
source_options
```

These are legitimate parts of the input contract.

Do not remove them.

## 5.1 Task Identity

Preserve:

```text
task_id
stream_id
stream_rank
```

These represent different concepts.

A task identifies an input unit.

A stream identifies a logical source stream.

A stream rank participates in the declared ordering contract.

Do not assume task IDs establish chronology.

Do not require every task to have a different stream rank if multiple tasks legitimately represent the same stream.

Validate ranks according to their actual semantics.

## 5.2 Instrument Identity

A concrete instrument must be interpreted using the full relevant identity:

```text
exchange
instrument_type
market_category
symbol
base_asset
quote_asset
```

Where applicable, instrument metadata includes:

```text
tick_size
contract_value
contract_value_asset
```

Validate required metadata according to the instrument type and market category.

Examples:

- Spot instruments generally do not require contract values.
- Linear contracts require suitable contract metadata.
- Inverse contracts require suitable contract metadata.

Do not fabricate contract specifications.

Do not reuse contract metadata from another task simply because the symbols look similar.

Do not identify an instrument using symbol alone.

## 5.3 Source Paths

Validate source-path syntax and resolve paths according to the production batch contract.

Preserve absolute paths.

Resolve relative paths using an explicit, documented root.

Do not depend on arbitrary shell working-directory assumptions.

Separate structural validation from source-availability preflight where practical.

The loader should be testable without requiring every referenced archive to exist.

Actual execution must still reject missing required sources.

Do not silently substitute another archive.

---

# 6. Self-Contained Normalization Profiles

## 6.1 New Profile Architecture

Normalization profiles are authoritative TOML documents.

Previously, physical raw-schema information and normalization rules could be defined separately.

The new design combines both into a single self-contained profile.

A profile must contain all information needed to interpret its supported physical source format and normalize it into the corresponding canonical schema.

**Do not require a second physical-schema TOML to complete profile resolution.**

## 6.2 Profile Naming Convention

The authoritative profile ID format is:

```text
exchange.instrument_type.market_category.data_type
```

Examples:

```text
binance.perpetual.linear.trades
bitget.spot.spot.depth
bybit.perpetual.inverse.trades
gateio.perpetual.linear.depth
okx.future.inverse.depth
```

These are illustrative identities; verify actual supported combinations in the repository.

The filename must match the profile ID:

```text
<profile-id>.toml
```

For example:

```text
bitget.spot.spot.depth.toml
```

must contain:

```toml
id = "bitget.spot.spot.depth"
```

### Required Validation

Verify:

- Profile ID is nonempty.
- Profile identity uses the expected namespace.
- Profile filename matches its ID.
- Exchange is supported.
- Instrument type is supported.
- Market category is supported.
- Data type is supported.
- Profile schema version is supported.
- Profile definitions are unique.
- No two files resolve ambiguously to the same profile identity.

Do not silently select one of several conflicting profiles.

Do not fall back to an unrelated profile.

---

# 7. Profile Discovery

Use the existing profile directory structure under:

```text
data/.config/historical/
```

Inspect the actual repository layout before changing it.

Do not reorganize the directory tree unnecessarily.

The resolver should identify profiles deterministically.

### Required Behavior

For a concrete task:

```text
exchange = bitget
instrument_type = perpetual
market_category = linear
content_type = depth
```

the expected profile identity is:

```text
bitget.perpetual.linear.depth
```

The engine must resolve that exact supported profile.

Do not infer profile identity using:

- Source filename alone.
- Symbol alone.
- Asset pair alone.
- First matching TOML file.
- Directory iteration order.

Keep resolution deterministic.

### Missing Profiles

A missing required profile must produce an actionable error.

The error should identify:

```text
Expected profile ID.
Exchange.
Instrument type.
Market category.
Data type.
Relevant task identity.
```

### Unsupported Profiles

If a profile explicitly declares a supported form of `unsupported_reason`, preserve the existing unsupported-profile behavior.

Do not classify an unsupported profile as successfully executable merely because it parses as TOML.

Do not fabricate missing normalization rules.

---

# 8. Profile Content and Validation

The profile must provide the relevant source interpretation and canonical normalization rules.

Depending on the source format, this can include:

```text
source container
source member rules
source encoding
raw record format
header handling
field definitions
field mappings
timestamp encoding
quantity encoding
side interpretation
normalization rules
depth event interpretation
sequence semantics
```

Use the actual existing profile schema.

Do not invent another schema or rename its fields without a demonstrated need.

## 8.1 Self-Containment

The profile must not depend on separate physical-schema definitions.

If legacy fields reference:

```text
physical
raw_schema_id
physical_schema_id
```

or equivalent external physical definitions, inspect their current meaning.

Remove obsolete dependencies when appropriate.

Preserve necessary source-format information inside the unified profile.

Do not retain a second authoritative representation merely for compatibility with old tests.

## 8.2 Validation

Validate profile content before executing normalization.

Examples of invalid definitions include:

- Missing required source fields.
- Unsupported timestamp encodings.
- Unsupported quantity encodings.
- Invalid field references.
- Unsupported transformations.
- Conflicting normalization rules.
- Invalid source-format declarations.
- Incompatible profile and task identities.
- Invalid depth sequence semantics.
- Unsupported source operation definitions.

Use the existing normalization-rule compiler and validation infrastructure.

Avoid maintaining separate implementations of the same rule validation.

## 8.3 Strict Interpretation

An invalid profile must not become executable through silent defaults.

Do not guess a field mapping.

Do not guess the unit of a timestamp.

Do not guess whether quantities represent base, quote, or contracts.

Do not guess snapshot or incremental semantics.

Reject unsupported operational configurations with useful diagnostics.

---

# 9. Operational Profile Settings Must Be Honored

The existing source decoders may contain fixed assumptions.

This is acceptable when those assumptions correspond to an explicitly supported format.

However, a TOML profile must not claim to configure behavior that the engine silently ignores.

For each operational profile setting, establish one of three outcomes:

| Classification | Required behavior |
|---|---|
| Configurable and supported | The setting controls the relevant execution component |
| Fixed supported behavior | The profile is validated against the fixed implementation |
| Unsupported | The profile is rejected with an actionable error |

### Example: XLSX

Inspect the existing XLSX depth decoder.

The reviewed implementation contained assumptions about worksheet layout, column positions, and sorting behavior.

If the unified profile declares those properties, either:

1. Pass the validated settings into the decoder and honor them; or
2. Validate that the profile matches the decoder's explicitly supported format.

Do not silently accept incompatible settings.

### Scope

Make only the changes necessary to establish truthful profile behavior.

Do not build a universal spreadsheet decoder.

Do not rewrite the source I/O subsystem.

Do not implement unrelated decoder optimizations.

The later correctness tasks will address integrity observation accounting, timestamp-regression enforcement, and fatal source-read error classification.

---

# 10. Trade Profile Resolution

Trade tasks must resolve to the correct canonical trade normalization specification.

Preserve existing support for:

```text
event timestamps
prices
quantities
trade sides
trade identifiers
source sequence fields
optional supported metadata
```

Use the production trade processor.

Do not reimplement trade normalization.

### Quantity Semantics

Preserve the distinction between:

```text
base quantity
quote quantity
contract quantity
```

The resolved profile must select the correct quantity representation.

The task's instrument metadata must provide the information required for contract conversion.

Do not duplicate normalization rules in the prepared batch.

### Validation

A profile for:

```text
bybit.perpetual.inverse.trades
```

must not silently resolve for an incompatible spot task.

Reject mismatches before processing.

---

# 11. Depth Profile Resolution

Depth tasks must resolve the correct source-event interpretation and sequence policy.

Preserve the existing depth processor and reconstruction machinery.

Relevant concepts include:

```text
initialization snapshots
replacement snapshots
absolute updates
relative updates where supported
source sequence information
reconstruction boundaries
```

The profile must supply or resolve the applicable semantics.

### Sequence Policy

Do not manually inject sequence policy values in production execution.

The policy must be resolved from the supported source/profile configuration.

Supported policies may include:

```text
consecutive
ranged
unsequenced
```

Use the existing engine definitions.

Do not invent additional sequence policies.

### Depth Safety

Do not change:

- Snapshot processing.
- Order-book update semantics.
- Sequence-gap detection.
- Reconstruction segment behavior.
- Boundary metadata.
- Canonical level emission.
- Recovery behavior.

This task establishes how the correct profile reaches existing processing.

It does not redesign depth reconstruction.

---

# 12. Source Feed Options

Some source archives require additional interpretation.

For example, Gate.io depth tasks may contain:

```json
{
  "source_options": {
    "feed": "snapshot"
  }
}
```

or:

```json
{
  "source_options": {
    "feed": "incremental"
  }
}
```

These options must be preserved.

### Required Behavior

- Validate supported feed values.
- Validate feed compatibility with the resolved profile.
- Preserve the source feed distinction in resolved tasks.
- Select the applicable supported source interpretation.
- Reject unsupported combinations.

Do not treat snapshot and incremental inputs as interchangeable merely because they use the same instrument and profile namespace.

Do not combine the feeds into one processing stream during this task.

That belongs to the later output/continuity work.

---

# 13. Family-Scoped Archives

Some source archives are family-scoped rather than specific to one concrete instrument.

For example:

```json
{
  "symbol": null,
  "instrument": null,
  "scope": {
    "kind": "family",
    "identifier": "BTC-USD"
  }
}
```

This must remain a legitimate input form where supported by the production contract.

### Required Behavior

Do not reject family-scoped tasks merely because their symbol or concrete instrument metadata is null.

Validate them according to the supported family-scope rules.

Determine how the existing engine identifies the concrete instrument represented by a source record or archive member.

Preserve source identity and instrument matching.

Do not assign all family-scoped source records to one fabricated instrument.

Do not invent tick sizes or contract values.

### Important Constraint

If the source requires per-record or per-member instrument metadata, use the existing supported resolution infrastructure.

If the current engine cannot resolve the required concrete metadata correctly, implement only the minimal necessary resolution for the supported format or report the unsupported case explicitly.

Do not silently normalize a family archive using an arbitrary representative instrument.

Do not implement a generalized multi-instrument routing engine or stream-oriented output writer under this task.

Such functionality belongs to later execution/storage work unless it is strictly necessary for the existing supported v3 task to execute correctly.

---

# 14. Profile Fingerprints and Provenance

Profiles must retain deterministic fingerprints.

The existing code already contains fingerprint-related infrastructure.

Reuse it.

### Required Properties

A profile fingerprint must represent the effective semantic profile definition.

Equivalent profile content should not produce different fingerprints solely because the file is located elsewhere.

Formatting-only changes should not alter the fingerprint unless the established fingerprint contract explicitly includes formatting.

Semantic configuration changes must alter the fingerprint.

Avoid hashing incidental filesystem metadata.

### Resolved Jobs

The internal resolved task must preserve sufficient information to identify:

```text
profile ID
profile fingerprint
resolved source format
resolved normalization rules
relevant instrument metadata
```

The existing resolved-job persistence mechanism should continue to capture this information.

Do not change the committed dataset manifest format during this task.

Do not redesign `processing-job.json`.

The later output-contract task will handle any required changes to external execution reporting.

---

# 15. Resolve Into Existing Internal Jobs

The prepared batch is the external input representation.

The normalization executor currently operates on internal job/task structures.

Preserve this separation where useful.

Conceptually:

```text
PreparedNormalizationBatch
          │
          ▼
       Validate
          │
          ▼
     Resolve Profiles
          │
          ▼
      ProcessingJob
          │
          ▼
        WorkTask
```

The exact Rust type names are illustrative.

### Required Behavior

The loader must:

1. Parse the prepared batch.
2. Validate batch-level metadata.
3. Validate jobs and tasks.
4. Resolve required profiles.
5. Validate task/profile compatibility.
6. Resolve source-format information.
7. Compile supported normalization specifications.
8. Attach effective runtime configuration where the current internal model requires it.
9. Produce executable internal jobs.
10. Preserve relevant source and profile provenance.

Do not require Python to generate Rust-specific normalization-rule structures.

Do not require Python to embed physical schemas.

Do not duplicate parsing logic across execution entry points.

### Shared Loading Interface

Expose a reusable Rust loading interface suitable for later use by:

```text
Normalization CLI
Normalization integration tests
Historical benchmark runner
Python-orchestrated batch execution
```

Do not introduce special parsing behavior for benchmarks or tests.

All consumers should use the production loader.

---

# 16. Runtime Configuration Integration

Task 01 established shared historical runtime configuration infrastructure.

Reuse it.

The authoritative configuration file is:

```text
data/.config/historical/general.toml
```

The prepared batch must not embed runtime resource configuration.

Preserve the actual effective values supplied by the shared configuration loader.

Do not introduce another normalization-specific resource parser.

Do not change:

```text
worker count
memory budgets
scratch budgets
Parquet row-group target
Parquet file-size target
integrity-policy thresholds
```

Do not reinterpret integrity-policy actions.

Do not activate the commented `[ordering]` section in `general.toml`.

The job-level ordering declaration remains part of the prepared batch.

---

# 17. Legacy Configuration Removal

The repository may still contain legacy references to:

```text
data/.jobs/process/
data/.jobs/processing.json
data/.config/historical/normalize.toml
```

It may also contain legacy separate raw-schema and physical-profile structures.

### Required Approach

Remove obsolete dependencies from the new authoritative v3 loading and profile-resolution path.

Do not make the production v3 loader depend on generated legacy processing jobs.

Do not preserve separate physical-schema definitions as an additional source of truth.

### Compatibility Boundary

Do not remove historical compatibility support blindly.

Inspect whether legacy parsing is still required by existing public interfaces or unmodified consumers.

Where compatibility is genuinely required, keep it clearly separated from the authoritative v3 path.

Do not silently translate unsupported legacy configurations into v3 without validation.

Do not retain duplicate active resolution implementations indefinitely.

### Tests

Some existing tests still use legacy job files and old profile IDs.

The comprehensive test-suite migration belongs to Tasks 03 and 06.

Update focused tests directly affected by this task.

Do not reorganize the entire historical test suite now.

If unrelated existing tests cannot compile because their interfaces reference removed legacy code, provide the smallest safe compatibility adjustment or update the affected test setup.

Do not delete useful assertions.

---

# 18. Testing Requirements

Add focused, deterministic tests for the new production loading and profile-resolution behavior.

Use small synthetic inputs where practical.

Do not require large real market-data archives for parser and resolver tests.

## 18.1 Batch Validation

Test:

- Valid schema-v3 batch parsing.
- Unsupported schema versions.
- Incorrect batch kind.
- Unsupported preparation status.
- Empty job collections.
- Duplicate job IDs.
- Duplicate dataset IDs.
- Duplicate task IDs within a job.
- Invalid output paths.
- Invalid ordering declarations.
- Unsupported operations.
- Unsupported content types.
- Missing required fields.
- Invalid or unknown fields.

Use the actual production contract to determine which fields are mandatory.

Do not introduce stricter requirements than the established Forge producer supports.

## 18.2 Profile Inventory

Verify:

- Every discovered production profile parses.
- Profile IDs match filenames.
- Profile IDs are unique.
- No ambiguous profile resolution exists.
- Supported profile definitions compile.
- Explicitly unsupported profiles are rejected appropriately.
- External physical-schema dependencies are not required.
- Required normalization mappings are present.

Avoid tests that only assert a hardcoded total profile count.

Prefer checking identities and supported combinations.

## 18.3 Profile Resolution

Verify representative cases for:

```text
Binance
Bitget
Bybit
Gate.io
OKX
```

Use the actual supported profiles.

At minimum, test representative trade and depth resolution.

Verify that incompatible instrument types or market categories are rejected.

Verify that missing profiles fail clearly.

Verify that the correct profile ID and fingerprint are attached to the resolved task.

## 18.4 Source-Specific Options

Test:

- Supported snapshot feed selection.
- Supported incremental feed selection.
- Invalid feed values.
- Incompatible feed/profile combinations.
- Family-scoped task parsing.
- Family-scoped task validation.
- Concrete instrument matching where supported.

Do not fabricate archive contents that would conceal unsupported processing behavior.

## 18.5 Profile Configuration Enforcement

Use small controlled examples to verify that meaningful profile settings are either honored or rejected.

Examples:

- Source field mapping.
- Header handling.
- Timestamp encoding.
- Quantity encoding.
- Source format.
- Sequence-policy selection.
- XLSX layout constraints where applicable.

Do not manually inject the expected configuration into the processor when testing resolver behavior.

## 18.6 Fingerprints

Verify:

- Deterministic fingerprint generation.
- Semantic changes affect fingerprints.
- Equivalent content produces equivalent fingerprints under the established contract.
- Profile path changes do not unexpectedly change semantic fingerprints.

## 18.7 Internal Job Resolution

Verify that a valid prepared batch produces internal jobs containing:

```text
correct identities
correct task counts
correct source paths
correct stream metadata
correct instrument metadata
correct normalization specifications
correct profile provenance
correct output destinations
```

Preserve ordering metadata.

Do not silently reorder tasks during loading unless the production contract explicitly requires it.

---

# 19. Representative Execution Verification

After parser and resolver tests pass, verify that resolved tasks are compatible with the existing normalization execution interfaces.

Use small synthetic source fixtures.

At minimum, demonstrate:

- One resolved trade task.
- One resolved depth task.

Exercise the production loading path.

Do not manually construct equivalent internal normalization rules to bypass the resolver.

### Output Isolation

Use isolated temporary output paths for these small verification tests.

Do not write to the production destinations present in the prepared input.

Do not overwrite existing datasets.

### Future Persistent Test Infrastructure

Task 03 will introduce the standardized persistent test-output directory:

```text
data/tests/historical/normalize/
```

Do not build the complete persistent testing framework during Task 02.

Use minimal safe test isolation until Task 03 is implemented.

---

# 20. Verification Commands

Use the repository's actual Cargo targets.

Run appropriate formatting and compilation checks.

Suggested commands:

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Also compile affected normalization binaries and run focused batch/profile-resolution tests.

Use the actual package features and test targets defined by the repository.

### Restrictions

Do not run:

- Full historical archive normalization.
- Large real-data integration suites.
- Historical performance benchmarks.
- Worker-scaling benchmarks.
- Merge or conversion workloads unrelated to this task.

Do not require PostgreSQL.

If a test needs unavailable real data, identify the missing prerequisite rather than silently skipping the test and reporting success.

---

# 21. Implementation Boundaries

## Authorized

This task authorizes:

- Production schema-v3 batch parsing.
- Batch and task validation.
- Unified self-contained TOML profile discovery.
- Unified profile validation.
- Removal of obsolete physical-schema dependencies.
- Profile-to-task resolution.
- Source-format configuration resolution.
- Sequence-policy resolution from supported profiles.
- Feed-specific source configuration.
- Family-scope validation and essential supported resolution.
- Deterministic profile fingerprinting.
- Conversion to existing executable internal jobs.
- Minimal compatibility updates.
- Focused tests.
- Minimal documentation updates.

## Not Authorized

Do not implement:

- Shared historical runtime configuration refactoring from Task 01.
- Full historical test-suite reorganization.
- Persistent integration-test output infrastructure.
- General integrity observation-accounting corrections.
- Timestamp-regression detection changes.
- Source-read error-handling redesign.
- New normalization algorithms.
- Stream-oriented writer consolidation.
- Cross-archive merge logic.
- New depth reconstruction semantics.
- Dataset manifest redesign.
- CLI execution-result JSON redesign.
- Benchmark infrastructure.
- PostgreSQL registration.
- Python Forge changes.

### CLI Boundary

Task 07 will standardize the CLI execution protocol.

For Task 02, implement a reusable production batch loader that Task 07 can invoke.

Do not redesign CLI stdout, exit codes, or input-mode flags under this task.

If the current CLI requires a minimal adapter to remain compilable, implement only that adapter.

Do not introduce a competing batch-loading implementation.

---

# 22. Working-Tree Safety

Inspect the current working tree before editing.

Preserve unrelated user changes.

Do not reset configuration or profile files without inspecting their current contents.

Do not overwrite manually updated unified profiles with legacy definitions.

Do not regenerate profiles from old physical schemas without explicit justification.

Do not create commits or branches unless requested.

Prefer small, reviewable implementation changes.

If the required functionality is already partially implemented, complete or correct it rather than replacing it wholesale.

---

# 23. Required Implementation Report

When Task 02 is complete, report the following.

## Input Contract

Describe:

- Authoritative schema-v3 model.
- Batch parsing and validation.
- Job/task validation.
- Supported source-specific fields.
- Remaining unsupported input combinations.

## Profile Architecture

Describe:

- Profile discovery paths.
- Profile identity rules.
- Self-contained profile structure.
- Physical-schema dependencies removed.
- Profile fingerprint implementation.
- Unsupported-profile handling.

## Resolution

Describe:

- Profile selection.
- Instrument metadata handling.
- Source-format resolution.
- Sequence-policy resolution.
- Snapshot/incremental feed handling.
- Family-scoped archive handling.

## Internal Integration

Describe:

- Internal job/task structures affected.
- Production loader entry points.
- Existing execution consumers updated.
- Compatibility interfaces retained.

## Tests

List:

- Formatting checks.
- Compilation checks.
- Focused unit tests.
- Synthetic execution tests.
- Tests passed.
- Tests failed.
- Tests blocked by unavailable prerequisites.

Do not claim full real-data compatibility solely from successful compilation or profile parsing.

## Remaining Work

Identify issues that belong to later tasks.

Do not implement those tasks.

---

# 24. Acceptance Criteria

Task 02 is complete when:

- The authoritative schema-version-3 prepared normalization batch is supported.
- Batch parsing and validation use one production implementation.
- Job and task identities are preserved.
- Legitimate source-specific fields are supported.
- Concrete instrument tasks resolve correctly.
- Supported family-scoped tasks are handled without fabricated instrument identities.
- The new self-contained TOML profiles are authoritative.
- Profile IDs match filenames.
- Profile discovery is deterministic.
- Duplicate or ambiguous profiles are rejected.
- Unsupported profiles fail explicitly.
- Obsolete physical-schema dependencies are removed from the authoritative resolution path.
- Profile operational settings are honored or explicitly constrained.
- The correct trade and depth normalization specifications are generated.
- Sequence policies are resolved through supported production configuration.
- Profile fingerprints are deterministic and preserved.
- Resolved tasks remain compatible with the existing execution engine.
- The shared runtime configuration from Task 01 is reused.
- Canonical schemas and processing algorithms remain unchanged.
- Focused batch and profile tests pass.
- No unnecessary legacy architecture remains in the new production path.
- No full historical benchmark or large real-data workload is executed.

---

## 25. Stop Condition

**Stop after Task 02 is complete.**

Do not automatically proceed to Task 03.

Do not reorganize the complete historical normalization test suite.

Do not implement stream-oriented Parquet consolidation.

Do not redesign the CLI response contract.

The next task will establish the shared normalization test infrastructure, including the single maintained workload fixture and persistent test outputs under:

```text
data/tests/historical/normalize/
```

Those changes must remain separate.

---

**Final objective:** Establish one reliable production loading path from Forge's prepared normalization batch to the Rust engine's executable internal jobs, using the authoritative self-contained TOML profiles and preserving existing normalization behavior.