````markdown
# MarketForge — Normalization Output and Forge Integration Contract

**Status:** Design note for future implementation.

**Scope:** Rust historical normalization output, CLI execution protocol, dataset manifests, failure reporting, and integration with Python Forge.

**Implementation:** Deferred until explicitly requested.

## 1. Objective

Standardize the outputs of the MarketForge historical normalization engine so they can be reliably consumed by Python Forge.

Rust is responsible for executing normalization, producing canonical datasets, validating integrity, and reporting execution results.

Python Forge is responsible for orchestration, dataset registration, storage planning, lifecycle management, and PostgreSQL persistence.

**Rust must not directly register datasets, runs, tasks, or files in PostgreSQL.**

The existing output architecture is largely appropriate. Preserve its established capabilities while addressing specific inconsistencies at the CLI and metadata boundaries.

Do not redesign the entire normalization engine.

## 2. Responsibility Boundaries

### Rust Normalization Engine

Rust owns:

- Loading and validating normalization input documents.
- Resolving the authoritative normalization profile TOMLs.
- Resolving effective runtime and integrity configuration.
- Decoding source archives.
- Normalizing trades and depth.
- Enforcing applicable integrity policies.
- Writing canonical Parquet datasets.
- Writing depth reconstruction metadata.
- Recording task-level and job-level processing metrics.
- Writing dataset manifests.
- Preserving resolved execution provenance.
- Managing staging and atomic dataset commits.
- Producing machine-readable execution results.
- Producing machine-readable failure information.

### Python Forge

Python owns:

- Discovering and selecting historical source archives.
- Preparing normalization jobs and batches.
- Allocating run, job, task, and dataset identities.
- Determining dataset staging and final output locations.
- Invoking the Rust normalization engine.
- Parsing execution results.
- Verifying committed datasets and manifests.
- Registering datasets and files in PostgreSQL.
- Recording source-to-dataset relationships.
- Recording dataset-to-stream relationships.
- Tracking run and job lifecycle states.
- Managing dataset retention and storage organization.
- Recovering interrupted orchestration workflows.
- Initiating subsequent merge and conversion operations.

Do not duplicate Python registry responsibilities inside Rust.

## 3. Preserve the Existing Dataset Architecture

The existing normalization engine already generates useful filesystem artifacts.

A successful trade dataset currently resembles:

```text
<dataset_path>/
├── manifest.json
├── processing-job.json
├── processing-job.sha256
└── tasks/
    ├── task-000001/
    │   └── trades/
    │       ├── part-000000.parquet
    │       └── part-000001.parquet
    └── task-000002/
        └── trades/
            └── part-000000.parquet
```

A successful depth dataset includes:

```text
<dataset_path>/
├── manifest.json
├── processing-job.json
├── processing-job.sha256
└── tasks/
    └── task-000001/
        └── depth/
            ├── part-000000.parquet
            ├── part-000001.parquet
            ├── events.parquet
            ├── segments.json
            └── boundary.json
```

Preserve the responsibilities of these artifacts.

| Artifact | Responsibility |
|---|---|
| `part-*.parquet` | Canonical normalized data |
| `events.parquet` | Depth source-event indexing |
| `segments.json` | Order-book reconstruction segments |
| `boundary.json` | Depth reconstruction boundary and continuity metadata |
| `manifest.json` | Authoritative committed dataset metadata |
| `processing-job.json` | Resolved execution configuration and provenance |
| `processing-job.sha256` | Checksum of the resolved job document |
| `failure.json` | Persistent diagnostic information for applicable failed executions |

Do not remove depth reconstruction metadata.

Do not replace canonical Parquet with a custom storage format.

Do not introduce PostgreSQL dependencies into the normalization engine.

The proposed stream-oriented Parquet consolidation is documented separately and is outside this task.

## 4. Authoritative Dataset Manifest

### Purpose

`manifest.json` must remain the authoritative machine-readable description of a successfully committed normalized dataset.

The manifest describes what Rust actually produced.

It must not describe planned output as though it were successfully committed.

### Preserve Existing Metadata

The current manifest contains:

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

Each canonical Parquet file entry includes:

```text
path
rows
size_bytes
start_timestamp_ns
end_timestamp_ns
```

File paths inside the manifest must remain relative to the committed dataset root.

Avoid absolute paths for individual Parquet files.

The existing `auxiliary_files` inventory must continue identifying required depth reconstruction artifacts.

### Dataset Identity

A dataset is identified by `dataset_id`.

A dataset may contain multiple source tasks and multiple streams.

Do not assume:

```text
one dataset = one instrument
```

Likewise:

```text
one task != one dataset
```

Keep job, dataset, task, and stream identities distinct.

### Stream Inventory

Consider extending the manifest with a concise stream inventory.

Illustrative structure:

```json
{
  "streams": [
    {
      "stream_id": "binance.linear.perpetual.BTCUSDT.trades",
      "task_ids": [1, 2, 3],
      "rows_written": 500000,
      "start_timestamp_ns": 1793059200000000000,
      "end_timestamp_ns": 1793145599999999999
    }
  ]
}
```

This is a proposed extension, not part of the current manifest contract.

Its purpose is to make dataset-to-stream relationships directly discoverable.

Before implementing it, determine whether Python can already reconstruct the required relationships reliably from `processing-job.json` and the existing task metrics.

Avoid unnecessary duplicated metadata.

If a stream inventory is added:

- Use deterministic ordering.
- Preserve exact stream identities.
- Preserve contributing task IDs.
- Distinguish canonical rows from source events.
- Avoid duplicating large quantities of per-task metadata.
- Validate stream totals against applicable dataset totals.
- Version the manifest schema appropriately.

Do not expand the manifest merely to reproduce the PostgreSQL registry.

## 5. Preserve Resolved Job Provenance

The existing engine writes:

```text
processing-job.json
processing-job.sha256
```

Keep both.

The resolved job must preserve the effective normalization specifications and runtime configuration actually used.

This is important because normalization profiles may change after a dataset has been produced.

The resolved job should allow future inspection of:

- Source task identities.
- Source archive paths and members.
- Instrument metadata.
- Selected normalization profiles.
- Profile identities and fingerprints.
- Resolved normalization rules.
- Effective runtime resources.
- Effective integrity policies.
- Output locations.
- Other relevant execution parameters.

Do not replace this with a pointer to the current profile TOML.

The current TOML may differ from the version used during execution.

The checksum must be calculated over the exact serialized resolved-job document.

Do not describe this checksum as verification of source archives or Parquet contents.

Those are separate concerns.

## 6. Standardize CLI Execution Results

### Current Behavior

The current CLI supports:

```text
--job
--jobs-dir
```

Single-job execution prints one JSON result object.

Multi-job execution prints an array of job results.

This creates two different response shapes for Python.

### Desired Behavior

Introduce one consistent machine-readable response envelope for all normalization execution modes.

The response should contain:

```text
protocol_version
operation
status
jobs
summary
```

Preserve all meaningful fields from the existing `ProcessingResult`.

Illustrative response:

```json
{
  "protocol_version": 1,
  "operation": "normalize",
  "status": "complete",
  "jobs": [
    {
      "job_id": 12345,
      "dataset_id": 67890,
      "status": "complete",
      "manifest_path": "/data/normalized/67890/manifest.json",
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
    "jobs_failed": 0
  }
}
```

This is a proposed response schema.

The exact version must be defined and maintained independently of the input batch and dataset manifest versions.

Do not assume all protocol versions must have the same number.

### Requirements

- The response must remain valid JSON.
- Single-job execution must use the same envelope as multi-job execution.
- `jobs` must always be an array.
- Preserve individual job results.
- Preserve job and dataset identifiers.
- Preserve output manifest paths.
- Preserve event/row counts and file counts.
- Preserve timestamp boundaries.
- Preserve structured failure information.
- Return a deterministic overall status.
- Preserve sufficient information when some jobs succeed and others fail.

Do not invent new per-job statuses unless the execution model actually supports them.

### Standard Output

When machine-readable execution is requested:

**stdout must contain exactly one complete JSON response document.**

Use stderr for:

- Informational logs.
- Progress messages.
- Warnings.
- Human-readable diagnostics.
- Performance information.

Do not mix terminal tables, progress bars, or debug messages with machine-readable stdout.

Ensure the TUI does not corrupt the JSON output contract.

## 7. Fix Process Exit Semantics

### Current Issue

The normalization executor can return a valid `ProcessingResult` containing:

```json
{
  "status": "failed"
}
```

while the Rust function itself returns `Ok(...)`.

The CLI currently serializes that result and returns successfully.

Consequently, a failed normalization operation can produce process exit code `0`.

This must be corrected.

### Required Behavior

A successful process exit requires successful completion of every requested job.

Use the following semantics:

| Condition | Process Exit |
|---|---|
| All requested jobs complete successfully | `0` |
| One requested job fails | Nonzero |
| Several jobs fail | Nonzero |
| Invalid input document | Nonzero |
| Invalid runtime configuration | Nonzero |
| Profile resolution failure | Nonzero |
| Unhandled execution error | Nonzero |

For structured job failures:

1. Preserve the complete machine-readable JSON response.
2. Ensure the failed status is represented.
3. Exit with a nonzero process status.

For failures occurring before a job result exists, provide an unambiguous diagnostic. Prefer a structured top-level error response where practical.

Do not fabricate a committed manifest or successful job result.

Do not silently swallow errors to preserve exit code `0`.

### Python Compatibility

Python must continue verifying both:

```text
process exit status
structured JSON execution status
```

A zero exit status alone is insufficient evidence that a committed dataset exists.

Likewise, a successful job result must be verified against its committed manifest.

## 8. Support the Final Schema-Version-3 Batch

The authoritative external normalization input is:

```text
schema_version = 3
kind = normalization_batch
status = prepared
```

It contains multiple normalization jobs and their source tasks.

The production input schema must remain authoritative.

Do not create another batch representation solely for the CLI.

### Proposed CLI Support

Add direct batch execution support:

```bash
marketforge-normalize \
  --batch data/runs/<run-id>/normalize-batch.json
```

The implementation must:

- Parse the authoritative batch document.
- Validate its schema version, kind, and preparation status.
- Validate contained jobs and tasks.
- Resolve the configured normalization profiles.
- Load authoritative runtime configuration.
- Execute jobs through the existing shared session machinery.
- Produce the standardized response envelope.

Do not split the batch into manually generated per-job JSON files unless a documented compatibility requirement makes this unavoidable.

### Existing CLI Options

Review and preserve useful existing entry points:

```text
--job
--jobs-dir
--config
--project-root
--output-root
--tui
```

Do not remove them without an explicit compatibility decision.

The new `--batch` option must be mutually exclusive with alternative input modes.

Do not duplicate executor logic between input modes.

All modes should converge on the same validated execution path where possible.

## 9. Runtime Configuration

The authoritative runtime configuration is being standardized under:

```text
data/.config/historical/general.toml
```

Inspect the actual current file and loader before implementing changes.

Update stale CLI defaults and affected tests to match the authoritative configuration layout.

Do not assume integrity policies, source profiles, and resource configuration must all live in one TOML.

Keep their responsibilities distinct.

Runtime settings may include:

```text
workers
memory budgets
scratch resources
Parquet row-group targets
Parquet file-size targets
```

These must remain outside the prepared batch task definitions when the final protocol assigns them to runtime configuration.

The resolved job snapshot must record the effective values used during execution.

Do not introduce machine-specific resource values into the input batch schema.

## 10. Dataset Commit Semantics

### Preserve Staging and Atomic Commit

The existing engine uses:

```text
staging_path
dataset_path
```

The dataset is written to staging, validated, and committed afterward.

Preserve this architecture.

A successful commit must:

1. Finish all required writers.
2. Validate processing metrics.
3. Evaluate integrity.
4. Construct the dataset manifest.
5. Verify required output artifacts.
6. Write the manifest.
7. Commit the staging directory to the final destination.

The committed dataset must not be visible as complete before these steps succeed.

### Filesystem Requirements

Preserve the existing same-filesystem rename requirement.

Do not silently replace the commit operation with recursive copying.

Do not overwrite an existing committed dataset.

Do not reuse an occupied staging destination without an explicit recovery policy.

Python should allocate valid staging and final paths during job preparation.

Rust should validate those paths and enforce its commit rules.

## 11. Failure Reporting

### Preserve Existing Failure Artifacts

The current engine can write:

```text
failure.json
```

into staging when a failure occurs after staging has been prepared and the relevant failure-reporting path is reached.

Preserve this capability.

The failure report contains useful information such as:

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

Not every failure can produce a persistent `failure.json`.

Some failures occur before staging is created.

Distinguish these cases explicitly.

### Expose Failure Report Paths

Consider extending the job execution result with:

```text
failure_report_path
```

For example:

```json
{
  "status": "failed",
  "manifest_path": null,
  "failure_report_path": "/data/staging/67890/failure.json",
  "failure": {
    "code": "processing_failure",
    "message": "Sequence discontinuity"
  }
}
```

Only populate the failure-report path when the file actually exists.

Do not construct or return a path to a nonexistent artifact.

### Failed Dataset Semantics

A failed dataset must never be reported as committed.

Preserve incomplete staging artifacts where the existing failure policy requires it.

Do not let a failed run overwrite a successful dataset.

Failure diagnostics must remain traceable to their job, dataset, and task identities.

Python will be responsible for recording the failure in the orchestration registry.

## 12. Parquet Output Semantics

The existing implementation has configurable row-group and file-size targets.

Preserve these capabilities.

However, the current trade and depth writers use different internal measurements when deciding whether to rotate output files.

Review and standardize the documented meaning of:

```text
row_group_target_bytes
file_target_bytes
```

A target is not necessarily a strict physical byte limit.

Do not promise that every completed Parquet part has exactly the configured size.

Use clear terminology and tests that verify the actual rotation behavior.

### Canonical Row Counts

Keep the distinction between source records and emitted canonical rows.

For trades:

```text
events_written = canonical trade rows
```

For depth:

```text
events_written = canonical L2 level-update rows
```

One depth source message can produce multiple canonical level rows or no changed level rows.

Do not interpret depth `events_written` as the number of original exchange messages.

Where necessary, clarify naming or documentation without changing existing counter semantics.

### Deferred Storage Refactor

A separate agreed design proposes:

```text
multiple archives
    → one compatible instrument stream
    → size-targeted Parquet parts
```

Do not implement that redesign under this output-contract task.

When the storage refactor is implemented later, update manifest discovery, file inventories, provenance, and stream attribution together.

Do not change canonical data schemas or depth reconstruction semantics merely to reduce file counts.

## 13. Python Forge Integration Contract

### Recommended Execution Flow

```text
Python Forge
    │
    ├── Allocate run and dataset identities
    ├── Select source archives
    ├── Prepare the v3 batch
    ├── Allocate staging and final dataset paths
    │
    ▼
Rust Normalization Engine
    │
    ├── Validate the batch
    ├── Resolve profiles and runtime configuration
    ├── Normalize source archives
    ├── Write canonical Parquet
    ├── Write auxiliary reconstruction metadata
    ├── Evaluate integrity
    ├── Write manifest and resolved job provenance
    ├── Commit successful datasets
    └── Return structured execution results
    │
    ▼
Python Forge
    │
    ├── Parse JSON response
    ├── Verify process exit status
    ├── Verify individual job statuses
    ├── Verify committed manifest paths
    ├── Validate referenced dataset artifacts
    ├── Register datasets in PostgreSQL
    ├── Register files and streams
    ├── Register source-to-dataset relationships
    └── Finalize orchestration state
```

Python must not trust subprocess exit code alone.

Python must not treat a planned dataset as successfully produced.

Rust must not mark PostgreSQL records complete.

### Registry Recovery

The following interruption is possible:

```text
Rust commits dataset successfully
            │
            ▼
Python process crashes
            │
            ▼
PostgreSQL registration is incomplete
```

This does not require Rust to own the database.

Forge must implement idempotent registration and reconciliation.

The committed manifest and resolved job provide evidence needed for recovery.

Use stable dataset identity to avoid duplicate registration.

Do not make a successful dataset depend on an atomic transaction spanning the filesystem and PostgreSQL.

Such a transaction is not provided by the current architecture.

### Storage Ownership

Forge chooses where datasets are stored.

Rust writes into the assigned staging destination and commits to the assigned final destination.

Prefer assigning the final location before execution rather than moving committed datasets afterward.

This avoids unnecessary copying and invalidating paths returned by Rust.

If relocation is introduced later, Forge must manage path updates explicitly.

## 14. Required Testing

Extend the standardized historical normalization test suite with tests for the finalized output contract.

Use:

```text
engine/tests/historical/fixtures/jobs.json
```

for archive-backed normalization workloads.

Use the actual production profiles and execution path.

Retain generated integration-test artifacts under:

```text
data/tests/historical/normalize/
```

as established in the separate testing note.

### CLI Protocol Tests

Verify:

- A single job returns the standard response envelope.
- Multiple jobs return the same envelope structure.
- A schema-version-3 batch is accepted.
- Invalid batch metadata is rejected.
- Unsupported profile resolution fails clearly.
- Completed jobs return exit code `0`.
- Failed jobs return a nonzero exit code.
- Mixed success/failure sessions retain individual job results.
- stdout remains valid machine-readable JSON.
- Informational output does not corrupt stdout.
- Fatal errors are not silently reported as success.

### Manifest Tests

Verify:

- Committed datasets contain `manifest.json`.
- Manifest dataset ID matches the requested dataset identity.
- File entries reference existing files.
- Relative paths resolve within the committed dataset root.
- File row counts and sizes match actual files.
- Aggregate row counts are consistent.
- Timestamp boundaries are consistent.
- Source tasks are correctly represented.
- Depth auxiliary artifacts are present where required.
- Integrity status matches the evaluated policy.
- Resolved job provenance is present and valid.

### Failure Tests

Verify:

- Failed execution cannot produce a committed-success result.
- Failure paths are reported only when they exist.
- Available failure metrics and diagnostics are preserved.
- Pre-staging failures are distinguishable from processing failures.
- Existing committed datasets are never overwritten.
- Existing staging destinations are handled safely.
- Failed operations do not corrupt previously committed data.

### Runtime Configuration Tests

Verify:

- The CLI loads the authoritative runtime configuration.
- Explicit configuration overrides are handled correctly.
- The resolved job records effective runtime settings.
- Invalid resource settings fail clearly.
- Test execution does not alter production configuration.
- Configuration paths resolve independently of the invoking shell directory.

### Python-Orchestration Compatibility

Provide fixture-based contract examples that Python can parse without executing the entire normalization workload.

Verify the documented JSON shapes, required fields, versioning, status interpretation, and manifest paths.

Do not require PostgreSQL for Rust normalization tests.

Forge's database and registry tests belong to the Python orchestration test suite.

## 15. Compatibility and Migration Rules

Do not simultaneously rewrite the normalization algorithms, Parquet storage layout, and CLI protocol.

Make the contract changes independently verifiable.

Preserve useful existing entry points where practical.

When replacing an existing serialized contract:

- Define the new protocol version.
- Document changed fields and response shapes.
- Update affected tests.
- Update direct consumers within the authorized scope.
- Do not silently retain conflicting output formats.
- Report compatibility requirements involving Python Forge.

Do not modify canonical trade or depth schemas merely to simplify the CLI response.

Do not delete existing provenance or integrity artifacts.

Do not implement database registration inside Rust.

## 16. Implementation Priorities

### Critical

Correct CLI exit semantics.

A failed normalization job must not result in a successful process exit.

### High

Standardize the machine-readable execution response.

Use the same top-level response structure for single-job and multi-job execution.

### High

Verify and implement direct support for the authoritative v3 normalization batch.

Do not create a second batch protocol.

### High

Update stale runtime configuration defaults to the current authoritative configuration layout.

### High

Ensure successful dataset manifests and resolved-job provenance provide sufficient information for Python registration.

### Medium

Expose existing failure-report paths through structured execution results.

### Medium

Clarify Parquet sizing and canonical row-count semantics.

### Deferred

Implement stream-oriented Parquet consolidation as a separate refactor.

## 17. Acceptance Criteria

The output-contract refactor is complete when:

- Rust produces canonical Parquet and required auxiliary artifacts.
- Dataset manifests remain authoritative and internally consistent.
- Resolved-job provenance is retained.
- Single-job and multi-job execution expose a consistent JSON contract.
- The authoritative v3 batch can be executed directly.
- Failed normalization produces a nonzero process exit status.
- Structured failure information remains available.
- Successful datasets are committed only after required validation.
- Existing datasets are never silently overwritten.
- Python can identify committed datasets and associated artifacts without parsing human-readable logs.
- Dataset-to-stream and source-task relationships are reconstructable from the persisted metadata.
- Rust has no responsibility for PostgreSQL dataset registration.
- The required CLI and manifest tests pass.
- Existing normalization semantics and canonical schemas remain unchanged unless a separate correctness issue is identified.

## 18. Final Architectural Principle

Maintain three distinct artifacts:

**Execution result:** What happened during normalization.

**Resolved job:** How normalization was performed.

**Dataset manifest:** What was actually produced and committed.

These artifacts must remain consistent but serve different responsibilities.

Python Forge consumes them to manage registry and lifecycle state.

Rust remains a self-contained data-processing engine.

**Final objective: A stable, versioned, machine-readable Rust normalization contract that Python Forge can orchestrate and register reliably, without introducing database responsibilities or unnecessary architectural changes into the engine.**
````