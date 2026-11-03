## Preliminary Note: Historical Pipelines and Dataset Registration

### Purpose

Introduce historical workflows that combine bulk acquisition with Rust processing, merge and conversion.

Users should be able to describe an entire workflow in one configuration file. Forge should resolve its dependencies, allocate internal IDs, execute the work and register every resulting dataset in PostgreSQL.

This note builds on the earlier notes covering persistent archive discovery, asynchronous HTTP, rate limiting and database-backed local acquisition records.

The scope is historical functionality. Leave live-engine behavior and unrelated scripts unchanged.

### User-facing workflow

Support both standalone operations and complete pipelines.

Standalone acquisition, processing, merge and conversion should remain available. Pipeline steps must reuse those same application services.

A typical pipeline is:

```text
Bulk acquisition → Processing → Merge → Conversion
```

The execution model must also support branches, including separately prepared reference datasets required by conversion.

Pipeline configuration should express intent and dependencies. Users should not manually supply IDs for datasets that earlier steps in the same pipeline will produce.

### Configuration and validation

Support versioned configuration files loaded into Pydantic models.

Each operation should have its own configuration model. Reject unknown fields and invalid operation combinations rather than silently ignoring mistakes.

Separate three actions:

| Action | Responsibility |
|---|---|
| Validate | Check configuration structure, values and internal references. |
| Resolve | Select concrete inputs, check capabilities and construct executable work. |
| Run | Execute resolved work and persist outcomes. |

Structural validation should cover dates, positive resource limits, unique step names, valid output references and acyclic dependencies.

Resolution should additionally check source support, discovery completeness, dataset availability, input compatibility, required metadata and engine capabilities.

A structurally valid configuration must not be presented as proof that remote data exists or that execution will succeed.

### Bulk acquisition

Allow one request to include multiple exchanges, instruments, archive scopes and data types.

Provide shared defaults with explicit per-entry overrides. Reuse the asynchronous HTTP, shared quota limiting and persistent discovery catalogue described in the previous notes.

Before execution, show a resolved plan containing:

- Selected sources, instruments and archive scopes.
- Archives requiring download.
- Locally reusable archives.
- Known sizes and unknown-size counts.
- Unknown discovery coverage and unavailable periods.
- Effective request policies and concurrency settings.

Persist the resolved selection. Execution should use concrete archive identities rather than silently reevaluating a changing selection.

Temporary URL refresh may update an archive's download location without changing its identity.

### Logical outputs and internal IDs

Each step exposes named, typed outputs.

For example:

| Step | Named output | Meaning |
|---|---|---|
| `acquire_market_data` | `archives` | A collection of registered, completed local raw archives. |
| `normalize_market_data` | `datasets` | A collection of registered canonical datasets. |
| `merge_market_data` | `dataset` | A registered published merge result. |
| `convert_market_data` | `dataset` | A registered published conversion result. |

A downstream step references these outputs by step name.

Illustrative configuration:

```yaml
version: 1
name: historical-market-data

steps:
  - id: acquire_market_data
    type: acquire
    request_file: acquisition.yaml

  - id: normalize_market_data
    type: process
    inputs:
      archives:
        step: acquire_market_data
        output: archives
    profile: default

  - id: merge_market_data
    type: merge
    inputs:
      datasets:
        step: normalize_market_data
        output: datasets
    merge_spec_file: merge.yaml

  - id: convert_market_data
    type: convert
    inputs:
      dataset:
        step: merge_market_data
        output: dataset
    conversion_spec_file: conversion.yaml
```

This example illustrates relationships, not a finalized configuration schema or existing CLI syntax.

Merge and conversion specifications must declare any necessary grouping, selection and reference-data requirements. Resolve configuration paths consistently relative to the containing configuration file.

Existing datasets may enter a pipeline through explicit IDs or catalogue selections. Newly produced datasets require no manually entered IDs.

### Collections and grouping

One processing step may expand into multiple engine jobs and output datasets.

Retain metadata identifying each collection member, including its source, instrument or scope, data type and represented interval.

Require explicit grouping or selection when multiple outputs can feed a merge. Do not automatically merge every upstream dataset without checking the declared intent.

Resolve reference datasets as explicit conversion dependencies. They may come from another pipeline branch or from existing registered datasets.

### PostgreSQL as the dataset registry

PostgreSQL is the authoritative registry for dataset identity, lifecycle, location and lineage.

Use and extend the existing `catalog.datasets` structure rather than introducing a competing dataset registry.

Allocate a pending dataset record before submitting each output-producing engine job. Generate the engine job using the appropriate internal identity mapping.

Keep pipeline run IDs, step IDs, job IDs, attempt IDs and dataset IDs distinct. Do not assume they are interchangeable.

The registry should record:

| Information | Requirement |
|---|---|
| Identity | Dataset ID and association with its producing execution. |
| Operation | Processing, merge or conversion. |
| Inputs | Raw acquisition inputs and/or upstream dataset IDs. |
| Location | Storage root, dataset path and manifest/report references. |
| Content | Dataset kind, schema identity/version and relevant instrument metadata. |
| Coverage | Declared interval and separately reported observed event bounds where available. |
| Results | Reported counts, integrity outcomes and useful execution statistics. |
| Lifecycle | Status, timestamps and failure details. |

Do not infer continuous or complete market-data coverage solely from the earliest and latest observed timestamps.

### Lineage

Preserve lineage for every output:

- Processing links an output dataset to its selected raw archive inputs.
- Merge links its output to all contributing input datasets.
- Conversion links its output to the primary input and any reference datasets.

Extend the existing dataset-to-dataset lineage representation where necessary.

Add an explicit relation for dataset-to-raw-archive inputs. Reference the acquired local file identity or immutable acquisition version so lineage identifies the actual bytes used, not merely a mutable remote URL.

Retain relevant input checksums and resolved configuration references.

### Engine result reports

Processing, merge and conversion must provide durable machine-readable completion information that Forge can consume.

Before implementation, inspect the actual Rust job, report and manifest schemas for every operation. Do not assume the three entry points currently expose identical fields or output behavior.

Define the adapter mapping explicitly. If required information is missing, identify the necessary engine-interface change rather than fabricating values.

Retain the original report as an artifact. Store searchable management fields in structured database columns, with optional operation-specific report content preserved separately.

Do not use formatted terminal summaries as the primary result contract.

Rust dataset manifests remain part of the engine's publication and validation contract. The earlier removal of mandatory raw-download sidecars does not imply removing engine-produced dataset manifests.

### Completion and registration

An engine execution is not fully available to downstream steps until its output has been validated and registered.

The completion procedure should:

1. Observe the engine's execution outcome.
2. Read its durable report and expected publication metadata.
3. Validate the report against the expected job and output identities.
4. Validate the published dataset location and manifest.
5. Register the result, statistics and lineage.
6. Commit dataset completion and the producing task's success together where they share a database transaction.
7. Release dependent work.

A process exit code alone, a printed success message or the presence of an output directory is insufficient to establish dataset completion.

Materialization-only conversion must not register a dataset as published. If materialization and publication are separate phases, track them separately and release downstream work only after successful publication.

### Registration failure and recovery

Filesystem publication and PostgreSQL registration are separate operations.

An engine may finish successfully while database registration fails. Record or recover that situation as registration pending; do not immediately rerun the expensive engine operation.

On recovery:

- Locate the expected report and published output.
- Revalidate their identities and integrity.
- Retry registration idempotently.
- Avoid duplicate dataset and lineage records.

A retry of registration must preserve the allocated dataset identity.

Execution retries require a separate attempt record and an explicit policy for existing staging or published outputs. Never blindly overwrite an existing dataset directory.

### Pipeline execution records

Persist pipeline execution state separately from discovery scans and dataset records.

Provide records for:

- Pipeline runs.
- Step executions.
- Concrete task attempts.
- Resolved input/output bindings.

A run should retain the submitted configuration and its resolved snapshot, including selected input identities, effective profiles, engine-interface version and relevant code revision.

Tasks should record their engine invocation, expected output identities, attempt status, logs and result-report locations.

If work expands incrementally after upstream steps finish, persist each resolved expansion before executing it.

### Scheduling and failures

Run independent work concurrently within configured resource budgets.

Apply network limits across acquisition tasks sharing a quota. Apply CPU, memory, disk and process limits across concurrent engine jobs.

Do not assume that independently valid worker settings remain safe when many engine jobs run together.

A failed task should block its dependents. Independent branches may continue according to the selected failure policy.

Do not silently omit failed inputs and mark a reduced merge or conversion successful. Partial-input operation must be explicitly requested and reflected in output metadata.

Cancellation must propagate to active acquisition tasks and engine processes, with their final state recorded.

Restart recovery must reconcile recorded attempts with actual processes and published outputs before resuming work.

### CLI and future TUI

The CLI, TUI and Python entry points must use the same validation, resolution and execution services.

The TUI should support opening or editing configuration, displaying validation errors, reviewing the resolved plan and observing persisted runs.

Initially, display reliable task/phase status, available transfer progress, logs and errors. Detailed engine progress requires a structured interface where one is not already available.

Define explicitly whether execution is attached to the invoking client or managed independently. Closing a screen must not accidentally determine run ownership.

### Responsibility boundaries

```text
forge/
├── historical/
│   ├── archives/
│   ├── engine/
│   └── pipelines/
├── orchestration/
├── shared/
│   └── http/
├── cli/
└── tui/
```

`historical/archives` owns historical discovery and acquisition.

`historical/engine` translates resolved historical operations into Rust jobs and interprets their results.

`historical/pipelines` owns historical step definitions, compatibility checks and workflow resolution.

`orchestration` owns execution scheduling, attempts, process supervision and recovery.

Keep operation handlers explicit and extensible. Initially support acquisition, processing, merge and conversion without requiring a general-purpose workflow language.

### Initial acceptance criteria

- Bulk acquisition works independently and as a pipeline step.
- Pipeline users do not manually supply IDs for newly produced datasets.
- Invalid dependencies and incompatible input types are rejected before affected engine execution.
- Every published processing, merge and conversion output is registered in PostgreSQL.
- Raw-input and dataset-input lineage is preserved.
- Standalone operations use the same registration path as pipeline operations.
- Downstream tasks wait for successful output registration.
- Registration can be retried without repeating successful engine execution.
- Resume does not blindly rerun completed work or overwrite existing outputs.
- CLI and TUI observe the same persisted execution state.
- Tests cover collection expansion, dependency resolution, registration failure, retries and publication reconciliation.
