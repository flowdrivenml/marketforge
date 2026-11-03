## MarketForge Python Implementation Plan

### Purpose

Implement the agreed Python architecture in thirteen reviewable parts.

Each part should deliver working behavior, relevant verification, and a bounded change that can be reviewed before continuing.

The first three parts are suitable for chat-guided implementation. Later parts require progressively more database, network, process, and Rust integration testing.

Benchmark execution and the live TUI remain explicit placeholders. Documentation and README development remain outside this plan.

### Part 1: Repository and Python foundation

Establish the agreed Python application layout under `forge/`, with `forge/pyproject.toml`, the committed dependency lockfile, and the repository-root `.venv/`.

The public application command is `marketforge`. Its installed launcher will be `.bin/marketforge`.

Configure Ruff, Pyright, and pytest to inspect the maintained source, tests, and scripts. Record existing diagnostics without hiding them through broad exclusions or suppressions.

Map existing modules to their intended responsibility boundaries. Preserve useful implementation logic and migrate it incrementally rather than combining every move, rename, and rewrite into one change.

Acceptance requires working imports, a functioning application help entry point, and tooling that checks the intended files.

### Part 2: Shared application foundation

Implement shared configuration loading, project and storage-root resolution, typed identities, application errors, report-location conventions, and terminal presentation infrastructure.

Use the agreed Tokyo Night palette and shared formatting rules.

Establish single-instance ownership before enabling commands that execute work or mutate managed state. Ownership must cover the configured workspace/data root across CLI and historical TUI entry points.

Define the application-level run and attempt concepts needed by acquisition and engine execution. Their persistence follows in Part 4.

Acceptance requires paths that resolve independently of the caller’s working directory, rejection of competing owners, and verification that these foundations work without PostgreSQL or Rust execution.

### Part 3: CLI structure and YAML validation

Build the agreed command groups under the `marketforge` command.

Implement structural YAML validation, useful field-level diagnostics, help, version information, and common presentation options such as JSON, silent mode, and color controls.

Provide explicit `historical-tui`, `live-tui`, and benchmark entry points. Unimplemented capabilities must report their status clearly and return a nonzero exit code when execution is requested.

Keep orchestration configuration separate from Rust-owned normalization format definitions.

This part establishes command structure, not simulated implementations. Operational handlers arrive alongside their corresponding services in later parts.

Acceptance requires correct argument handling, useful invalid-configuration errors, help without a database connection, and honest placeholder behavior.

### Part 4: Database foundation and migration

Introduce SQL-file migrations and establish a safe transition from the existing database schema.

Maintain distinct production and testing databases using the same core migration stream. Testing-only benchmark additions remain separate.

Implement connection selection, database-identity checks, migration tracking, and the initial registry and execution-record foundations.

Preserve existing catalogue, configuration, and dataset records. Add feature-specific tables through subsequent migrations as their services are implemented.

Introduce registry backup and restore, including a verified restore into a fresh database. Extend artifact-reference checks as acquisition and dataset services become available.

Prepare a usable testing PostgreSQL instance for integration work. Complete public installation automation remains in Part 13.

Acceptance requires successful fresh initialization, a non-destructive existing-schema upgrade, testing/production separation, and a working backup-and-restore procedure.

### Part 5: Shared asynchronous HTTP

Implement reusable asynchronous sessions, connection pooling, response streaming, request pacing, concurrency limits, retries, shared cooldowns, and cancellation.

Requests sharing a provider quota must share limiter state. Retries must pass through the same rate controls.

Keep scheduling and memory consumption bounded. Do not create an unbounded task for every archive.

Verify behavior against controlled HTTP endpoints rather than relying exclusively on external exchanges.

Acceptance requires demonstrated request pacing, bounded concurrency, correct retry behavior, and cancellation that releases resources.

### Part 6: Markets capability

Consolidate instrument catalogue browsing, metadata synchronization, and market metrics under `forge/markets/`.

Preserve useful existing exchange-provider logic and connect the corresponding CLI commands.

Retain filtering, sorting, metric timestamps, and clearly identified units. Metadata synchronization and metric refresh remain explicit actions.

Acceptance requires correct catalogue reads, supported synchronization and refresh operations, and preservation of the existing useful market views.

### Part 7: Persistent historical discovery

Implement stable remote archive identities, discovery scans, searched coverage, cache freshness, and acquisition planning.

Separate archive identity from its current download location. Support targeted resolution of expired or dynamic links.

Distinguish available archives, successfully searched empty coverage, unknown coverage, and partial or failed discovery.

Begin with a representative adapter and then verify the different mechanisms used by the supported exchanges, including listings, windowed requests, and candidate probing.

Acceptance requires cached requests to avoid redundant discovery where supported, failed discovery to remain incomplete, and temporary URL changes to preserve archive identity.

### Part 8: Asynchronous downloads and raw inventory

Implement bounded concurrent downloading, attempt-specific temporary files, filesystem finalization, and PostgreSQL acquisition registration.

Use file-count progress. Exact remote file sizes and additional size-probing requests are not required.

Import legacy acquisitions where possible without requiring new raw-download sidecars.

Add raw-storage reconciliation and the relevant inspection and maintenance commands.

Persist execution and attempt outcomes so interrupted transfers and registration-pending files remain recoverable.

Acceptance requires safe concurrent transfers, no competing writes to the same destination, idempotent registration, and recovery without unnecessary redownloading.

Verify registry restore before using the rewritten acquisition path with valuable data.

### Part 9: Real normalization and registration recovery

Inspect and confirm the actual Rust normalization job, configuration, report, manifest, and publication contracts.

Implement the Python adapter and minimal process supervision, including child outcomes, cancellation, durable reports, and output registration.

Rust remains authoritative for normalization formats and semantics.

Register normalized datasets, raw-input lineage, effective configuration references, and useful result metadata. Connect basic dataset inspection.

Prove this complete path with small deterministic inputs:

```text
Resolve fixture archive
→ Download and register acquisition
→ Invoke real Rust normalization
→ Validate published output and report
→ Register normalized dataset and lineage
```

Then deliberately fail registration after Rust publishes its output.

Acceptance requires recovery to register the existing output under the allocated identity without rerunning normalization or duplicating lineage.

Do not substitute a mocked engine result for this integration milestone.

### Part 10: Merge and conversion

Implement standalone merge and conversion through the established execution, reporting, and registration services.

Review merge and conversion as separate changes within this part.

Validate grouping, input compatibility, reference datasets, and operation-specific publication requirements.

Extend dataset inspection and storage reconciliation for their outputs.

Acceptance requires real engine execution, correct output registration and lineage, and recovery behavior consistent with normalization.

Engine jobs remain sequential, while each Rust operation retains its internal parallelism.

### Part 11: Historical pipelines

Implement typed step outputs, dependency resolution, collection expansion, explicit grouping, and persisted input/output bindings.

Users must not manually supply IDs for datasets generated earlier in the same pipeline.

Compose the existing standalone services rather than introducing separate acquisition or engine execution paths.

Use bounded asynchronous acquisition and sequential engine scheduling. Block dependent work when required inputs fail or remain unregistered.

Implement pipeline reports and resume using the established reconciliation behavior.

Acceptance requires a YAML workflow covering acquisition, normalization, merge, and conversion, including failure and resume scenarios.

### Part 12: Historical TUI

Connect the working application services to the six agreed tabs:

```text
Markets
Acquisition
Pipelines
Datasets
Logs
Settings
```

Implement external YAML editing, validation feedback, execution dispatch, result inspection, and shared presentation styles.

Support current-terminal handoff and the configured separate-window adapter without creating independent execution owners.

Keep live controls, testing-database selection, and benchmark screens outside the historical TUI.

Acceptance requires TUI actions to produce the same effects as CLI actions, with verified terminal restoration and useful failure presentation.

Split screen implementation into smaller reviewable changes rather than delivering the entire interface in one unchecked batch.

### Part 13: Installation and release distribution

Implement root `setup.sh` around the now-working application.

Automate uv preparation, Python environment setup, locked dependencies, compatible prebuilt engine installation, launcher creation, and readiness checks.

Provide the explicit source-build option.

Complete managed local PostgreSQL provisioning through Docker and support existing PostgreSQL connections.

Add GitHub release bundles, platform manifests, checksums, and release automation for supported Linux and macOS targets.

Acceptance requires a fresh supported installation to reach a usable application and database through the agreed setup flow.

Validate actual platform compatibility rather than assuming compilation establishes support.

### Quality gates for every part

Each part must include its relevant tests and checks. Do not defer verification to a final testing phase.

New and changed code must satisfy the agreed typing, linting, and formatting standards.

Application behavior must remain consistent across CLI and TUI entry points. Recovery, reporting, and aesthetics belong alongside the capability being implemented.

Use the following completion pattern:

```text
Bounded implementation
→ Relevant static and runtime checks
→ Observable working result
→ Review and commit
→ Next part
```

Report skipped checks and remaining limitations explicitly.

### Initial work to undertake together

Start with Parts 1–3: a runnable application foundation, shared configuration and presentation, single ownership, and the CLI/YAML interface.

Keep this first batch independent of full database provisioning, exchange networking, and Rust execution.

Proceed to infrastructure and integration work after the foundation can launch and its checks pass.
