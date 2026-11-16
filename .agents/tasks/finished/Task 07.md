# Task 07 — Normalization CLI, Dataset Manifests, and Forge Integration Contract

**Project:** MarketForge  
**Component:** Rust Historical Normalization Engine  
**Priority:** High / Production Integration  
**Status:** Approved for implementation  
**Dependencies:** Tasks 01–06  
**Next task:** Historical Normalization Benchmark Infrastructure

## 1. Objective

Finalize the historical normalization engine's external execution interface so Python Forge can reliably invoke normalization, identify successful and failed jobs, inspect committed datasets, and register their metadata in PostgreSQL.

The Rust engine already produces important artifacts:

- Canonical Parquet datasets.
- Dataset manifests.
- Resolved processing-job specifications.
- Resolved-job checksums.
- Depth reconstruction metadata.
- Integrity evaluations.
- Processing metrics.
- Failure reports.
- Machine-readable execution results.

These capabilities must be preserved.

The main work is to **standardize the interface between the Rust engine and Python Forge**, eliminate ambiguous execution outcomes, and ensure output metadata accurately describes what the engine produced.

### Primary Goals

1. Support direct execution of the authoritative schema-version-3 normalization batch.
2. Standardize CLI JSON responses.
3. Correct process exit-code semantics.
4. Preserve accurate job and dataset identities.
5. Make successful and failed execution results unambiguous.
6. Ensure dataset manifests are reliable and internally consistent.
7. Preserve execution provenance.
8. Expose persistent failure diagnostics.
9. Ensure Python can consume results without parsing human-readable logs.
10. Preserve existing dataset commit and failure-safety guarantees.

**Rust remains responsible for processing and producing datasets. Python Forge remains responsible for orchestration and PostgreSQL registration.**

Do not implement database registration inside Rust.

Complete only Task 07.

---

## 2. Architectural Context

The intended integration flow is:

```text
Python Forge
    │
    ├── Allocate run, job, task, and dataset identities
    ├── Prepare normalization batch
    ├── Select staging and final dataset paths
    │
    ▼
marketforge-normalize
    │
    ├── Validate batch
    ├── Load shared runtime configuration
    ├── Resolve normalization profiles
    ├── Execute normalization
    ├── Validate integrity
    ├── Generate dataset artifacts
    ├── Commit successful datasets
    └── Return structured JSON
    │
    ▼
Python Forge
    │
    ├── Check process exit status
    ├── Parse structured result
    ├── Verify individual job statuses
    ├── Read committed manifests
    ├── Register datasets
    ├── Register files and source relationships
    └── Update PostgreSQL lifecycle state
```

The engine must remain independently executable from the terminal.

Python must not be required to run normalization.

PostgreSQL must not be required by the normalization engine.

### Responsibility Separation

| Responsibility | Rust Engine | Python Forge |
|---|:---:|:---:|
| Prepare historical workloads | | ✓ |
| Allocate dataset identities | | ✓ |
| Select output destinations | | ✓ |
| Validate prepared batches | ✓ | |
| Resolve normalization profiles | ✓ | |
| Normalize source archives | ✓ | |
| Write canonical Parquet | ✓ | |
| Generate depth reconstruction metadata | ✓ | |
| Evaluate integrity | ✓ | |
| Generate dataset manifests | ✓ | |
| Preserve resolved-job provenance | ✓ | |
| Commit successful datasets | ✓ | |
| Report execution results | ✓ | |
| Register datasets in PostgreSQL | | ✓ |
| Register dataset relationships | | ✓ |
| Manage dataset lifecycle | | ✓ |

Do not duplicate Forge's responsibilities inside Rust.

---

# 3. Inspect Existing Implementation

Before modifying code, inspect the actual normalization execution and output infrastructure.

Relevant locations may include:

```text
engine/src/bin/marketforge-normalize.rs

engine/src/historical/config/
engine/src/historical/job/

engine/src/historical/normalize/
├── executor.rs
├── session.rs
├── task.rs
├── manifest/
├── metrics/
└── ...
```

Also inspect:

```text
engine/src/shared/persistence/
engine/src/shared/persistence/parquet/

engine/tests/historical/
```

Use actual repository paths where they differ.

### Review Existing Types

Identify current definitions or equivalents of:

```rust
ProcessingResult
ProcessingStatus
ProcessingFailure
ProcessingFailureReport
DatasetManifest
DatasetFile
AuxiliaryFile
ProcessingMetricsReport
ProcessingJob
```

Inspect:

- JSON serialization.
- CLI argument parsing.
- Single-job execution.
- Multi-job execution.
- Batch loading from Task 02.
- Execution-result aggregation.
- Manifest generation.
- Output discovery.
- Resolved-job persistence.
- Failure-report persistence.
- Staging-directory handling.
- Dataset commit behavior.
- Existing CLI tests.

### Revalidate Previous Findings

Earlier inspection identified several issues:

**Different stdout response shapes**

Single-job execution prints one JSON object, while multi-job execution prints an array.

**Incorrect exit-code behavior**

An internal execution can return a failed job result inside a successful Rust `Result`, allowing the CLI to exit with status `0`.

**Batch execution interface**

The CLI previously exposed `--job` and `--jobs-dir` but not a direct `--batch` input mode.

**Failure-report location**

Structured job results did not explicitly identify the location of an existing persistent `failure.json`.

Recheck the current repository.

If some issues have already been corrected, preserve the existing solution and verify it through tests.

Do not implement duplicate functionality.

---

# 4. Authoritative Input Contract

Task 02 established production loading of the schema-version-3 prepared normalization batch.

Reuse that implementation.

The authoritative external input contains:

```text
schema_version = 3
kind = normalization_batch
status = prepared
```

A batch contains multiple normalization jobs.

Each job contains its tasks, ordering declaration, output destinations, and identities.

### Required Behavior

- Use the production batch parser.
- Use the production profile resolver.
- Use the shared runtime configuration.
- Preserve input identities.
- Preserve source metadata.
- Preserve task and stream identities.
- Preserve staging and final output paths.
- Reject invalid batches before executing them.

Do not create another batch schema specifically for the CLI.

Do not make Python split a prepared batch into separate processing-job documents merely to satisfy the Rust executable.

---

# 5. CLI Input Modes

The normalization executable must support the authoritative batch directly.

### Required Batch Mode

```bash
marketforge-normalize \
  --batch /path/to/normalize-batch.json
```

The batch file may reside anywhere on the filesystem.

Absolute paths must remain absolute.

Relative paths must follow the established project-root and batch-loading contracts.

### Existing Modes

Review and preserve useful existing options:

```text
--job
--jobs-dir
--config
--project-root
--output-root
--tui
```

Do not remove them unnecessarily.

### Input Exclusivity

The following input modes must be mutually exclusive:

```text
--batch
--job
--jobs-dir
```

Exactly one input mode must be selected.

Do not silently prioritize one mode when multiple modes are supplied.

### Shared Execution Path

All input modes should converge on the existing validated execution infrastructure.

Conceptually:

```text
--batch ───────┐
               │
--job ─────────┼──► Validated Jobs ──► Executor
               │
--jobs-dir ────┘
```

Do not duplicate normalization execution logic for each CLI mode.

### Batch Execution

A batch may contain multiple jobs.

Preserve each job's independent identity, output destinations, and failure boundary.

Do not merge jobs simply because they contain related instruments.

Do not alter the normalization scheduler during this task.

---

# 6. Standardize Machine-Readable CLI Results

The CLI must expose one consistent JSON response structure.

**Single-job and multi-job executions must use the same response envelope.**

Do not serialize unrelated internal structures directly when their representation is not intended to be a stable public protocol.

Prefer a dedicated versioned CLI response type that can be constructed from existing execution results.

## 6.1 Proposed Response V1

Use a versioned response with:

```text
schema_version = 1
kind = normalization_result
```

The response must include:

```text
schema_version
kind
run_id
status
jobs
summary
error
```

### Example: Successful Execution

```json
{
  "schema_version": 1,
  "kind": "normalization_result",
  "run_id": "normalize-example",
  "status": "complete",
  "jobs": [
    {
      "job_id": 1001,
      "dataset_id": 2001,
      "status": "complete",
      "manifest_path": "/data/normalized/2001/manifest.json",
      "failure_report_path": null,
      "events_written": 1500000,
      "files_written": 6,
      "start_timestamp_ns": 1793059200000000000,
      "end_timestamp_ns": 1793145599999999999,
      "failure": null
    }
  ],
  "summary": {
    "jobs_total": 1,
    "jobs_complete": 1,
    "jobs_failed": 0,
    "jobs_not_reported": 0
  },
  "error": null
}
```

This defines the intended external response shape.

Inspect the current Rust serialization conventions before implementation.

Preserve existing meaningful result fields and error-code values.

Do not silently change serialized field names or status values without updating the versioned contract and its consumers.

## 6.2 Top-Level Status

Use:

```text
complete
failed
```

A session is complete only when all requested jobs completed successfully.

If one job fails, the overall status is:

```json
{
  "status": "failed"
}
```

Individual jobs retain their own statuses.

Do not introduce a separate overall `partial` status merely because some jobs succeeded.

A job that commits an integrity-approved degraded dataset remains a completed execution; the manifest separately records its degraded integrity status.

## 6.3 Job Results

Each job result must preserve:

```text
job_id
dataset_id
status
manifest_path
failure_report_path
events_written
files_written
start_timestamp_ns
end_timestamp_ns
failure
```

Preserve exact identifier values.

Do not generate replacement job or dataset IDs.

Do not convert 64-bit identifiers through floating-point values.

### Successful Job

A successful job must satisfy:

```text
status = complete

manifest_path != null
failure_report_path = null
failure = null
```

The manifest must exist in the committed dataset.

Reported counts and timestamp boundaries must agree with the committed manifest.

### Failed Job

A failed job must satisfy:

```text
status = failed

manifest_path = null
failure != null
```

`failure_report_path` may be null when no persistent failure report could be created.

Do not present partial staging output as a successfully committed dataset.

Preserve available failure details.

## 6.4 Session Summary

The summary must contain:

```text
jobs_total
jobs_complete
jobs_failed
jobs_not_reported
```

These values must be internally consistent.

For identified jobs:

```text
jobs_total =
    jobs_complete
  + jobs_failed
  + jobs_not_reported
```

A job that has not produced a definitive result must not be fabricated as successful or failed merely to balance counters.

The response's `jobs` array should contain the definitive job results that are actually available.

## 6.5 Run Identity

For schema-v3 batch execution, return the batch's `run_id`.

For legacy input modes without a run identity, use:

```json
{
  "run_id": null
}
```

Do not fabricate a Forge run identity inside Rust.

---

# 7. Process Exit-Code Semantics

**This is a critical correctness requirement.**

The CLI must not return exit code `0` when a requested normalization job failed.

### Required Behavior

| Execution outcome | Exit code |
|---|---|
| Every requested job completed successfully | `0` |
| One or more jobs failed | Nonzero |
| Invalid input document | Nonzero |
| Invalid runtime configuration | Nonzero |
| Profile resolution failure | Nonzero |
| Fatal execution failure | Nonzero |
| Output commit failure | Nonzero |

### Preserve Structured Results

For a completed execution attempt containing failed jobs:

1. Construct the structured response.
2. Serialize it as JSON.
3. Write it to stdout.
4. Return a nonzero process exit code.

Do not discard useful per-job results merely because the overall session failed.

### Important Implementation Detail

The internal executor may legitimately return:

```rust
Ok(ProcessingResult {
    status: ProcessingStatus::Failed,
    // ...
})
```

This means the executor successfully reported a failed processing outcome.

It does **not** mean normalization succeeded.

The CLI must inspect the reported status before deciding its process exit code.

### Fatal Errors Before Job Execution

Some failures occur before individual job results can be produced.

Examples include invalid input JSON, invalid runtime configuration, and unavailable profile catalogs.

Where practical, return a structured top-level error response with:

```text
status = failed
error != null
```

Do not invent job results for work that was never executed.

If argument parsing or an unrecoverable initialization failure prevents producing a structured response, exit nonzero with an actionable stderr diagnostic.

Do not misrepresent such an error as a successful empty session.

---

# 8. Standard Output and Logging

Machine-readable stdout must remain clean.

### Required Rule

**For a normal machine-readable execution, stdout must contain exactly one complete JSON response document.**

Do not mix:

- Progress bars.
- Human-readable tables.
- Debug messages.
- Informational logs.
- Warnings.
- Performance summaries.

with the JSON response.

### Standard Error

Use stderr for human-readable diagnostics and progress where appropriate.

Preserve useful existing logging.

Do not remove diagnostics merely to keep stdout clean.

### TUI Compatibility

Inspect how the existing Ratatui monitor interacts with the CLI.

Do not allow terminal rendering to corrupt machine-readable JSON output.

If necessary, define a clear separation between TUI and machine-readable execution modes.

Preserve existing TUI functionality where practical.

Do not redesign the TUI.

### Help and Version Output

Normal help and version requests are not normalization execution results.

They may remain human-readable.

Do not emit a fabricated normalization result for `--help`.

---

# 9. Dataset Manifest Contract

The engine already produces:

```text
manifest.json
```

This must remain the authoritative description of a committed normalized dataset.

### Existing Manifest

The current manifest contains fields including:

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

Preserve these capabilities.

Do not redesign the manifest merely because the CLI response is changing.

### Versioning

Preserve the existing manifest version when its contract remains compatible.

Do not increment the version solely because internal Rust modules were refactored.

If an incompatible manifest change is genuinely required, version it explicitly and update relevant consumers and tests.

Keep manifest versioning independent of the CLI response and batch input versions.

---

# 10. Manifest File Inventory

The manifest must accurately describe the canonical output files.

Each Parquet file entry should preserve:

```text
path
rows
size_bytes
start_timestamp_ns
end_timestamp_ns
```

### Relative Paths

File paths inside the manifest must remain relative to the committed dataset root.

Example:

```json
{
  "path": "tasks/task-000001/trades/part-000000.parquet",
  "rows": 500000,
  "size_bytes": 32000000,
  "start_timestamp_ns": 1793059200000000000,
  "end_timestamp_ns": 1793062800000000000
}
```

The values are illustrative.

Do not embed machine-specific absolute paths for every Parquet file.

### Validation

Ensure:

- Every referenced file exists.
- Every referenced path remains within the dataset root.
- Every file is a regular output artifact.
- Recorded sizes match actual file sizes.
- Recorded row counts are accurate.
- Aggregate row totals are consistent.
- File counts agree with the manifest inventory.
- Timestamp boundaries are consistent.
- Required files are fully finalized before commit.

Do not assume the presence of a file proves that its contents are valid.

Use existing Parquet metadata-reading and validation infrastructure where appropriate.

### Output Layout

Preserve the current task-oriented output layout.

Do not implement stream-oriented Parquet consolidation.

That work belongs to Tasks 10 and 11.

---

# 11. Auxiliary Depth Artifacts

Depth normalization produces additional metadata required for reconstruction.

Existing artifacts include:

```text
events.parquet
segments.json
boundary.json
```

Preserve them.

The manifest must continue identifying required auxiliary files.

### Required Checks

Verify:

- Required auxiliary files exist.
- Their paths are valid.
- Their recorded metadata is consistent.
- Depth event indexes remain accessible.
- Reconstruction segments remain attributable to their sources.
- Boundary metadata is preserved.
- A successful depth dataset cannot omit required reconstruction artifacts silently.

Do not rewrite the depth replay subsystem.

Do not change snapshot, sequence, or update semantics.

Do not combine depth metadata from separate tasks into a new stream-level structure during this task.

---

# 12. Dataset and Stream Identity

A dataset may contain multiple tasks and multiple logical streams.

Do not assume:

```text
one dataset = one task
```

or:

```text
one dataset = one instrument
```

The engine must preserve sufficient provenance for Python Forge to reconstruct dataset relationships.

### Required Identifiers

Preserve:

```text
job_id
dataset_id
task_id
stream_id
profile_id
profile fingerprint
```

where applicable to existing models.

### Stream Metadata

Inspect whether Forge can reconstruct dataset-to-stream relationships using:

```text
manifest.json
processing-job.json
processing_metrics
```

If the existing persisted information is sufficient, do not add duplicate fields merely for convenience.

If essential relationships cannot be recovered reliably, introduce the smallest appropriate metadata extension.

### Potential Stream Inventory

A concise manifest stream inventory may be useful.

For example:

```json
{
  "streams": [
    {
      "stream_id": "binance.perpetual.linear.BTCUSDT.trades",
      "task_ids": [1, 2, 3],
      "rows_written": 500000,
      "start_timestamp_ns": 1793059200000000000,
      "end_timestamp_ns": 1793145599999999999
    }
  ]
}
```

This is a proposed extension, not an existing requirement to change the manifest.

Before adding it:

1. Determine whether the information is already available.
2. Establish its actual consumers.
3. Avoid redundant metadata.
4. Preserve deterministic ordering.
5. Ensure totals remain consistent.
6. Handle manifest versioning correctly.

Do not expand the manifest into a copy of the PostgreSQL registry.

---

# 13. Resolved Job Provenance

The engine currently persists:

```text
processing-job.json
processing-job.sha256
```

Preserve both.

### Resolved Job

The resolved job describes how normalization was performed.

It should preserve relevant information such as:

```text
Job identity.
Dataset identity.
Source tasks.
Source archive paths and members.
Instrument metadata.
Resolved normalization profiles.
Profile fingerprints.
Effective normalization rules.
Effective runtime configuration.
Integrity-policy configuration.
Output destinations.
```

Do not replace the resolved job with a reference to the current profile TOML.

The current profile may have changed after execution.

### Checksum

The checksum must correspond to the exact persisted resolved-job document.

Verify that the stored checksum matches the document actually written.

Do not describe this checksum as verification of source archives or Parquet contents.

Those require separate checks.

### No Provenance Redesign

Do not create another authoritative job-provenance format unnecessarily.

Reuse the production resolved-job serialization established by Tasks 01 and 02.

---

# 14. Failure Reporting

The engine already supports persistent failure reports where execution has reached an appropriate stage.

These reports must remain available.

### Existing Failure Artifact

```text
failure.json
```

Relevant fields include:

```text
protocol_version
job_id
dataset_id
failed_task_id
error_code
error_message
processing_metrics
integrity_evaluation
```

Preserve their existing meanings.

### Failure Report Location

Extend the structured job execution result with an optional failure-report path.

Example:

```json
{
  "job_id": 1002,
  "dataset_id": 2002,
  "status": "failed",
  "manifest_path": null,
  "failure_report_path": "/data/staging/2002/failure.json",
  "events_written": 0,
  "files_written": 0,
  "start_timestamp_ns": null,
  "end_timestamp_ns": null,
  "failure": {
    "code": "processing_failure",
    "message": "Depth sequence discontinuity"
  }
}
```

This is illustrative.

Use actual existing error-code serialization.

### Path Accuracy

Only populate:

```text
failure_report_path
```

when the persistent file exists.

Do not return invented paths.

### Early Failures

Some failures occur before staging exists.

For example:

- Invalid batch JSON.
- Invalid runtime configuration.
- Invalid profile catalog.
- Invalid output path.
- Source preflight failure before failure-report persistence.

Such failures may not have a `failure.json`.

The CLI must represent them honestly.

Do not fabricate persistent diagnostics that were never written.

### Preserve Partial Metrics

When available, failure reporting should preserve:

- Failed task identity.
- Source context.
- Processing counters.
- Integrity observations.
- Error classification.
- Diagnostic message.

Do not present partial output counters as successfully committed output.

---

# 15. Dataset Staging and Commit Safety

Preserve the existing staging-and-commit architecture.

Each job specifies:

```text
staging_path
dataset_path
```

The intended lifecycle is:

```text
Prepared Job
     │
     ▼
Create Staging
     │
     ▼
Normalize Sources
     │
     ▼
Finalize Writers
     │
     ▼
Evaluate Integrity
     │
     ▼
Generate Manifest
     │
     ▼
Validate Required Artifacts
     │
     ▼
Commit Dataset
```

### Successful Execution

A job is complete only after its required artifacts have been finalized and the dataset has been committed.

Do not return a successful result merely because worker tasks finished.

### Failed Execution

A failed job must not publish a committed-success manifest.

Preserve failed staging artifacts according to existing behavior.

Do not convert failed staging into a committed dataset merely to preserve diagnostic files.

### Existing Dataset Protection

Never silently overwrite an existing committed dataset.

Preserve output collision checks.

### Same-Filesystem Commit

Preserve the existing same-filesystem rename requirement.

Do not silently replace atomic directory rename with recursive copying.

Do not claim stronger crash-durability guarantees than the implementation actually provides.

### Python Registry Boundary

Rust commits the filesystem dataset.

Python later registers it in PostgreSQL.

Do not attempt to create a distributed transaction across Rust filesystem operations and Forge's database.

Forge will handle idempotent registration and recovery.

---

# 16. Output Path Handling

Python Forge assigns output destinations before execution.

Rust must respect and validate those destinations.

### Requirements

- Preserve absolute paths where applicable.
- Resolve relative paths according to the production contract.
- Reject invalid staging/final path relationships.
- Reject output collisions.
- Preserve dataset identity.
- Avoid accidental writes outside assigned destinations.
- Do not relocate committed datasets automatically.

### `--output-root`

Inspect the existing `--output-root` behavior.

Preserve its useful ability to redirect output safely.

Where appropriate, ensure it works consistently across supported input modes.

If the option cannot be applied safely to a particular mode, fail clearly.

Do not silently ignore it.

Do not rewrite source archive paths when redirecting output.

### Test Compatibility

The isolated output handling established by Task 03 must continue working.

Do not make normalization tests write to production destinations.

---

# 17. Processing Metrics and Counter Semantics

The CLI result and manifest must preserve existing processing metrics.

However, different counters represent different things.

### Source Records

```text
records_read
records_matched
records_processed
records_rejected
```

### Canonical Output

```text
events_normalized
events_written
files_written
```

### Trade Output

For trades:

```text
events_written = canonical trade rows
```

### Depth Output

For depth:

```text
events_written = canonical L2 level-update rows
```

One depth source event may generate many canonical rows or no changed rows.

Do not interpret canonical depth rows as original source-message counts.

### Required Behavior

- Preserve existing counter meanings.
- Ensure returned counts match committed manifests.
- Ensure manifest totals are internally consistent.
- Preserve integrity metrics established by Tasks 04 and 05.
- Avoid double-counting tasks or output files.
- Do not replace accurate counters with approximate values.

If naming is confusing, clarify it in documentation.

Do not change established serialized counter semantics without a versioned compatibility decision.

---

# 18. Parquet Resource Semantics

The engine uses settings such as:

```toml
[resources.parquet]
row_group_target_bytes = 67108864
file_target_bytes = 268435456
```

The current writers may use different internal measurements when deciding whether to flush or rotate files.

### Required Review

Inspect how the trade and depth writers interpret:

```text
row_group_target_bytes
file_target_bytes
```

Determine whether these represent:

- Estimated in-memory bytes.
- Encoded bytes.
- Persisted bytes.
- Another explicitly defined quantity.

### Scope

This task may clarify documentation and validate output metadata.

Do not redesign writer rotation.

Do not change Parquet target values.

Do not introduce stream-level writers.

Do not change compression settings.

The full output consolidation refactor belongs to Tasks 10 and 11.

If inconsistent target interpretation requires changes beyond documentation and contract correctness, report the issue for the storage refactor rather than implementing a large writer change here.

---

# 19. Versioning and Compatibility

The system has several independently versioned artifacts.

These include:

```text
Prepared normalization batch.
CLI execution result.
Dataset manifest.
Failure report.
```

Do not assume they must all use the same version number.

### Required Rules

- Preserve existing input schema v3.
- Introduce a clearly versioned CLI response.
- Preserve existing manifest versions when compatible.
- Preserve existing failure-report versions when compatible.
- Version incompatible serialization changes explicitly.
- Reject unsupported versions clearly.
- Preserve exact numeric identifiers.
- Avoid implicit field renaming.

### Rust Internal Models

Internal Rust structures may evolve without changing the external contract.

Prefer explicit conversion into public response types.

Do not accidentally expose unstable internal implementation details through direct serialization.

### Python Compatibility

The external protocol must be sufficiently documented for Python Forge to parse it.

However, **do not implement Python-side integration during this task**.

The Rust contract must stand independently.

---

# 20. Testing Requirements

Use the testing infrastructure established by Tasks 03 and 06.

Tests should exercise the actual production execution interfaces.

Use small synthetic workloads wherever possible.

Do not require large downloaded archives for CLI protocol tests.

## 20.1 CLI Input Tests

Verify:

- `--batch` accepts a valid prepared batch.
- `--job` remains supported where applicable.
- `--jobs-dir` remains supported where applicable.
- Input modes are mutually exclusive.
- Missing input is rejected.
- Unsupported batch versions are rejected.
- Invalid batch metadata is rejected.
- Relative paths resolve correctly.
- Absolute input paths work.
- Explicit runtime configuration paths work.
- Invalid runtime configuration fails clearly.

## 20.2 JSON Response Tests

Verify:

- Single-job execution returns one response envelope.
- Multi-job execution returns the same envelope structure.
- Batch execution returns the same envelope structure.
- The response is valid JSON.
- All required fields are present.
- Run identity is preserved.
- Job identities are preserved.
- Dataset identities are preserved.
- Counts remain accurate.
- Nullable fields serialize consistently.
- No human-readable logs corrupt stdout.

## 20.3 Exit-Code Tests

Verify:

- Complete execution returns exit code `0`.
- Failed job execution returns a nonzero exit code.
- Mixed success/failure sessions return nonzero.
- Invalid input returns nonzero.
- Fatal execution errors return nonzero.
- Structured failed-job results are preserved despite nonzero exit status.

Do not test only the internal Rust function result.

Test the actual CLI process behavior where practical.

## 20.4 Manifest Tests

Verify:

- Committed datasets contain a manifest.
- Manifest job ID matches the requested job.
- Manifest dataset ID matches the requested dataset.
- Source-task relationships are correct.
- Parquet file inventory is complete.
- Relative paths are safe.
- File sizes are correct.
- Row counts are consistent.
- Timestamp boundaries are consistent.
- Required auxiliary artifacts are present.
- Integrity status matches the accepted outcome.
- Manifest paths returned by the CLI exist.

## 20.5 Provenance Tests

Verify:

- Resolved-job document exists.
- Resolved-job checksum exists.
- Checksum matches the persisted document.
- Profile identities are preserved.
- Profile fingerprints are preserved.
- Effective runtime settings are preserved.
- Source-task identities remain traceable.

## 20.6 Failure Tests

Verify:

- Failed execution does not produce a committed-success result.
- Failure reports are retained where generated.
- `failure_report_path` points to an existing file when non-null.
- Early failures can be represented without a fabricated report.
- Available failure metrics are preserved.
- Existing committed datasets are not overwritten.
- Failed staging remains distinguishable from committed output.

## 20.7 Multi-Job Tests

Use small synthetic jobs.

Verify:

- Each job keeps its identity.
- Each job keeps its output destination.
- One failed job does not invalidate a successful independent job.
- Session summary counts are correct.
- Per-job result ordering is deterministic under the declared response contract.
- Worker completion order does not corrupt job/result association.

Do not rely on incidental completion order to identify jobs.

---

# 21. Persistent Test Outputs

Artifact-producing normalization integration tests must continue storing results under:

```text
data/tests/historical/normalize/
```

Preserve the Task 03 retention requirements.

### Required Behavior

- Every execution uses a unique directory.
- Canonical output remains available.
- Manifests remain available.
- Resolved jobs remain available.
- Failure reports remain available where generated.
- Failed staging remains available.
- Previous test results are not overwritten.
- No automatic cleanup occurs.

Pure serialization and argument-validation tests may use disposable temporary resources.

Do not persist large artifacts unnecessarily for tests that produce no normalization dataset.

---

# 22. Verification Requirements

After implementation, run focused verification.

### Formatting

```bash
cargo fmt --manifest-path engine/Cargo.toml --all -- --check
```

### Compilation

```bash
cargo check --manifest-path engine/Cargo.toml --features historical
```

Also compile affected CLI binaries and test targets using the repository's actual feature configuration.

### Focused Tests

Run:

- CLI input-mode tests.
- JSON serialization tests.
- Exit-code tests.
- Manifest tests.
- Provenance tests.
- Failure-report tests.
- Multi-job result tests.
- Relevant existing normalization tests.

Use small synthetic workloads where possible.

### Real-Data Restrictions

Do not automatically execute the complete historical normalization fixture.

Do not execute large source archives simply to verify the CLI.

Do not run performance benchmarks.

Do not run worker-scaling experiments.

If a real-data test is blocked by missing prerequisites, report it rather than substituting another archive.

---

# 23. Implementation Sequence

## Phase A — Audit

Inspect the current CLI, result models, manifests, provenance, failure reporting, and commit logic.

Identify existing behavior and compatibility requirements.

## Phase B — Batch CLI Integration

Add direct `--batch` support using the production loader established in Task 02.

Unify input-mode handling without duplicating execution logic.

## Phase C — Standardized JSON Results

Introduce the versioned CLI response model.

Preserve all meaningful existing job-result fields.

## Phase D — Exit-Code Correction

Ensure process success corresponds to actual successful execution.

Preserve structured failed-job responses.

## Phase E — Manifest and Provenance Verification

Review output inventories, metadata consistency, resolved jobs, checksums, and artifact references.

Implement only necessary corrections.

## Phase F — Failure Reporting

Expose persistent failure-report paths where available.

Preserve early-failure handling and diagnostics.

## Phase G — Integration Tests

Implement focused tests for input modes, JSON responses, exit codes, manifests, failures, and multi-job execution.

## Phase H — Verification

Run formatting, compilation, and relevant lightweight tests.

Stop after completing Task 07.

---

# 24. Scope Restrictions

## Authorized

This task authorizes:

- Direct CLI batch input.
- CLI argument validation.
- Standardized machine-readable execution results.
- Correct process exit-code behavior.
- Structured session summaries.
- Reliable per-job results.
- Explicit failure-report paths.
- Manifest consistency corrections.
- Resolved-job provenance verification.
- Safe output-path handling.
- Minimal serialization/versioning updates.
- Focused integration tests.
- Minimal documentation updates.

## Not Authorized

Do not implement:

- A new normalization batch schema.
- A new profile-resolution architecture.
- A new runtime configuration framework.
- New normalization algorithms.
- New integrity-policy semantics.
- New depth reconstruction algorithms.
- Stream-oriented Parquet consolidation.
- Parquet writer redesign.
- New merge or conversion algorithms.
- Benchmark infrastructure.
- PostgreSQL registration.
- Python Forge orchestration changes.
- A new service API or Axum management layer.
- A TUI redesign.

### Important Boundary

This task finalizes the **CLI and filesystem artifact contract**.

It does not implement the Python orchestration layer.

It does not implement a network API.

It does not redesign the historical storage layout.

---

# 25. Working-Tree Safety

Inspect the working tree before modifying files.

Preserve unrelated user changes.

Do not reset the repository.

Do not overwrite existing production datasets.

Do not modify source archives.

Do not change `general.toml` resource values.

Do not remove established provenance or reconstruction artifacts.

Do not create commits or branches unless requested.

Prefer minimal, reviewable changes.

---

# 26. Required Implementation Report

When Task 07 is complete, provide the following.

## CLI Changes

Describe:

- Final supported input modes.
- Batch execution behavior.
- Shared execution path.
- Argument-validation behavior.
- TUI compatibility.

## JSON Contract

Document:

- Response schema version.
- Required top-level fields.
- Per-job result fields.
- Overall status semantics.
- Session summary semantics.
- Error representation.
- Nullable fields.

Include representative successful, failed, and mixed-result JSON examples.

## Exit Codes

Describe:

- Successful execution exit behavior.
- Failed-job exit behavior.
- Fatal pre-execution error behavior.
- How structured results are preserved on failure.

## Dataset Artifacts

Describe:

- Manifest changes, if any.
- Provenance verification.
- Auxiliary-file verification.
- Output path behavior.
- Stream/source relationship availability.

## Failure Reporting

Describe:

- Persistent failure-report behavior.
- Failure-report path representation.
- Early failures without persistent reports.
- Preserved diagnostics.

## Compatibility

Identify:

- Existing interfaces preserved.
- Intentional breaking changes.
- Serialization version changes.
- Any Python consumer changes required later.

Do not implement those Python changes now.

## Verification

Report:

- Formatting checks.
- Compilation checks.
- Tests executed.
- Tests passed.
- Tests failed.
- Tests blocked by prerequisites.

Do not claim complete real-data execution was verified unless it was actually performed.

## Remaining Work

Identify dependencies belonging to later benchmark and stream-oriented storage tasks.

Do not implement them.

---

# 27. Acceptance Criteria

Task 07 is complete when:

- The normalization CLI accepts the authoritative schema-v3 prepared batch directly.
- Existing useful CLI input modes remain supported.
- All input modes use the same underlying validated execution infrastructure.
- Single-job and multi-job executions return the same JSON envelope structure.
- The CLI response has an explicit independent schema version.
- Every job result preserves job and dataset identity.
- Session summaries are internally consistent.
- A failed job cannot cause a successful overall exit code.
- A mixed session returns a nonzero exit code while preserving individual job results.
- stdout remains machine-readable JSON during normal execution.
- Human-readable diagnostics do not corrupt stdout.
- Successful jobs return valid committed manifest paths.
- Failed jobs never report committed-success manifests.
- Existing failure-report paths are exposed when available.
- Early failures are represented without fabricating persistent artifacts.
- Dataset manifests accurately describe committed canonical files.
- Depth auxiliary artifacts remain discoverable.
- Resolved-job provenance and checksums remain valid.
- Source-task relationships remain reconstructable.
- Dataset-to-stream relationships remain reconstructable.
- Existing committed datasets are never silently overwritten.
- Staging and commit semantics remain intact.
- Existing canonical schemas remain unchanged.
- Existing integrity-policy semantics remain unchanged.
- No PostgreSQL registration is introduced into Rust.
- Focused CLI and artifact tests pass.
- No performance benchmarks or large historical workloads are executed automatically.

---

## 28. Stop Condition

**Stop after Task 07 is complete.**

Do not automatically proceed to Task 08.

Do not implement the benchmark runner.

Do not implement stream-oriented Parquet writers.

Do not modify Python Forge.

The next assignment is:

**Task 08 — Historical Normalization Benchmark Infrastructure.**

Task 08 will use the finalized input and output contracts to build reproducible benchmark execution, persistent measurements, and environment provenance under:

```text
data/benchmarks/historical/normalize/
```

Benchmark execution itself must remain separately authorized.

---

**Final objective:** Establish a stable and trustworthy machine-readable interface between Rust normalization and Python Forge, where execution results, resolved-job provenance, and committed dataset manifests remain consistent, failures are reported accurately, and Python can manage dataset registration without duplicating processing responsibilities.