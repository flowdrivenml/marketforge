## Amendment: Historical Execution, Rust Configuration Authority, and Initial Integration

### Purpose and precedence

This note updates the earlier discovery, acquisition, pipeline, TUI, and installation specifications.

Where earlier notes conflict with this amendment, this amendment takes precedence. Remove superseded instructions when preparing the final Codex implementation assignment.

The current scope remains historical acquisition, normalization, merge, conversion, and their Python orchestration.

### Use normalization terminology

Rename the Python capability and user-facing operation previously called “processing” to “normalization.”

Use normalization consistently in Python modules, application services, YAML operation types, CLI commands, TUI labels, reports, and documentation.

The historical engine operations are:

```text
Normalization
Merge
Conversion
```

Inspect the current Rust interfaces before changing executable names, protocol fields, or existing engine artifacts. If a Rust interface still uses an older identifier, translate it explicitly in the Python adapter until that interface is intentionally migrated.

Do not rename persisted values or historical records without an explicit migration or compatibility mapping.

### Rust owns historical format and operation contracts

The Rust historical engine and its established configuration mechanism are the source of truth for normalization formats and engine operation contracts.

Forge must orchestrate those capabilities rather than maintain a competing Python definition of how raw records are normalized.

Inspect the existing Rust-managed historical configuration directory and its loaders before implementation. Preserve its actual path and ownership; do not invent a replacement directory or move configuration merely to match an earlier proposed tree.

Formats are maintained manually when required. Do not introduce automatic format discovery, a format-history service, or a new format-management interface in this task.

Python may validate orchestration YAML, resolve selected engine configurations, construct supported jobs, and interpret engine reports. It must not independently redefine raw-field mappings, quantity semantics, or normalization rules already owned by Rust.

Existing Python catalogue tables or seed definitions must not silently override the Rust-managed configuration. Determine which remain useful and migrate callers away from duplicate authoritative definitions.

### Record the configuration actually used

For each engine execution, retain the submitted job and identify the effective engine configuration, including a configuration snapshot or content checksum where appropriate.

Resolve and freeze configuration before execution. Editing a configuration afterward must not change the recorded meaning of an existing run.

This provides execution traceability without introducing a general historical-format versioning system.

Retaining a configuration snapshot records what was used; it does not prove that the configuration is appropriate for every historical period.

### File-count download progress

Use file counts as the primary download-progress measure.

Exact remote file sizes are not required for planning, execution, or the progress display. Do not issue additional network requests solely to obtain sizes.

After resolving and deduplicating the selected physical archives, show:

| Counter | Meaning |
|---|---|
| Selected | Total selected physical archives |
| Reused | Selected archives already available locally |
| Downloaded | Newly downloaded files finalized and registered |
| Failed | Files that could not be acquired |
| Remaining | Files without a final outcome |
| Registration pending | Finalized files awaiting database registration |

Calculate readiness as:

```text
Ready files = Reused files + Newly downloaded and registered files
```

Retries do not increase the selected count. Failed or registration-pending files do not count as ready.

Do not add mandatory exact-size verification or a new archive-inspection subsystem as part of this rewrite.

Retain ordinary HTTP error handling, interrupted-transfer detection, safe temporary-file handling, and failure reporting. Record actual bytes written and the locally computed checksum for inventory and later comparison.

A locally computed checksum identifies the stored bytes. Do not present it as independent proof of source authenticity or complete market-data coverage.

### One Forge execution owner

Allow one primary Forge historical application instance for a configured workspace and data root.

A running TUI and a separately invoked operational CLI must not become independent execution owners. A competing invocation should report that Forge is already running and exit clearly.

The ownership boundary must also cover checkouts configured to write to the same managed data root.

Use an operating-system-backed exclusive lock or an equivalent reliable mechanism. A database status row or PID file alone is insufficient.

Acquire ownership before starting historical work. Retain it while supervised work is running and release it during orderly shutdown.

Internal helpers and separate progress-window processes belong to the existing owner. They must not start a second independent orchestrator.

Do not introduce a daemon, distributed scheduler, or multiple-owner coordination system for this version.

This ownership policy applies to Forge historical operation. The separate live-engine interface remains outside this task.

### Asynchronous acquisition within the single owner

Single ownership does not mean sequential HTTP requests.

The owning application must support bounded asynchronous discovery and downloading across instruments and exchanges.

All concurrent tasks sharing a provider quota must use the same limiter state. Request pacing, retries, cooldowns, and concurrent-transfer limits remain required.

Deduplicate selected archive identities and prevent internal workers from writing to the same destination.

Use bounded scheduling and streaming so large acquisition requests do not create unbounded tasks or memory consumption.

### Initial engine scheduling policy

Run historical engine jobs sequentially in the initial Forge implementation, including when presentation is silent.

Rust binaries retain their own internal worker parallelism. Asynchronous acquisition retains its bounded network concurrency.

Pipeline branches may express dependencies, but their engine jobs must execute through the same sequential scheduler. Do not introduce concurrent engine-job scheduling in this version.

Use the same scheduling policy for standalone operations and complete pipelines.

Interactive execution must have one terminal owner at a time. Suspend Forge rendering and input handling during current-terminal handoff, then restore them afterward.

A separate progress window changes presentation only. It does not create another execution owner or a background scheduling service.

### Cancellation, crashes, and recovery

The owner supervises its active HTTP tasks, helpers, and engine processes.

On supported cancellation paths, cancel acquisition tasks, stop or await child processes as appropriate, and record the outcome before releasing ownership.

A parent crash must not permit a replacement execution to conflict with a surviving engine child. Choose a process-supervision and lock-lifetime mechanism that preserves this requirement.

On restart, reconcile unfinished attempts against actual execution outcomes and durable reports before retrying.

Do not assume a recorded PID still identifies the same process. Do not blindly rerun work, overwrite published outputs, or interpret a closed progress window as success.

### Registry backup and restore

Provide a small CLI maintenance path for backing up and restoring the PostgreSQL registry and necessary application configuration.

A backup must preserve the database state needed to recover archive inventory, dataset identities, lineage, run records, and configuration associations.

Use a supported PostgreSQL backup mechanism. Record the backup’s database/schema version and relevant application version.

Preserve required storage-root mappings and configuration snapshots. Handle credential-bearing configuration separately and securely.

Database backups do not include archive or dataset bytes unless those files are explicitly backed up through a separate mechanism. Report this boundary clearly.

Restore into a fresh database, validate schema compatibility, and check registered artifact references against the configured storage roots.

Missing files must be reported as unavailable. Do not invent successful acquisitions or silently redownload data during restore.

A persistent Docker volume is operational storage, not a backup.

Destructive restore or overwrite operations must be explicit. Ordinary setup and application launch must never reset an existing registry.

### First implementation milestone

Before developing the complete historical TUI, prove one complete acquisition-to-dataset path through the shared application services.

Use a small deterministic archive fixture served through a controlled HTTP endpoint. Exercise the real download, filesystem finalization, database registration, and Rust normalization paths.

The milestone must demonstrate:

```text
Resolve fixture archive
→ Download and register raw acquisition
→ Allocate output dataset identity
→ Invoke the real Rust normalization binary
→ Read and validate its durable result
→ Register the normalized dataset and raw-input lineage
→ Inspect the registered result
```

Use the explicitly configured testing database and isolated temporary storage.

The test must use the actual Rust normalization executable and its supported configuration. Mocking the normalization result does not satisfy this milestone.

### Registration-failure recovery milestone

Extend the same scenario by deliberately failing dataset registration after Rust has successfully published its output.

Verify that:

- The published dataset and durable report remain intact.
- The output is not made available to downstream work before registration succeeds.
- The attempt remains recoverable as registration pending.
- Recovery validates the existing report and output.
- Registration retries preserve the allocated dataset identity.
- Recovery does not rerun normalization.
- Repeated recovery does not duplicate dataset or lineage records.
- A completed result is visible through the registry after recovery.

Use a controlled failure point so the scenario is repeatable.

### Implementation order

First establish the authoritative Rust configuration boundary and the single-owner execution model.

Then implement the fixture acquisition, real normalization, registration, and recovery milestone.

Once that path works, extend the same adapters and registration services to merge and conversion, connect pipeline execution, and build the TUI on those verified services.

Do not create separate execution paths for CLI, TUI, standalone operations, and pipelines.

### Acceptance criteria

The revised implementation must use normalization terminology, preserve Rust’s authority over engine formats, and present download progress through file counts without requiring exact remote sizes.

Only one Forge historical owner may execute against the managed workspace/data root, while bounded asynchronous acquisition remains supported.

Engine jobs execute sequentially under that owner.

Registry backup and restore must have a tested maintenance path.

The first complete integration scenario must prove real Rust normalization and recovery from registration failure without repeating successful engine work.
