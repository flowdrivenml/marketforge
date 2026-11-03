## Specification: MarketForge CLI and Benchmark Entry Points

### Purpose and scope

Provide a comprehensive CLI for historical acquisition, normalization, merge, conversion, pipeline execution, dataset inspection, recovery, and application maintenance.

Every implemented historical capability must be usable without opening the TUI.

The public command is `marketforge`. The directory `forge/` contains the Python implementation; it is not the application’s command name.

Documentation-site and README development remain outside this task.

### Installation entry point and launcher

Keep the root installation command:

```bash
bash setup.sh
```

Install the application launcher at:

```text
<repository-root>/.bin/marketforge
```

The repository-local invocation is:

```bash
./.bin/marketforge --help
```

Examples below use `marketforge` for readability. They do not imply that setup must modify the user’s shell `PATH`.

Replace earlier references to `.bin/forge` with `.bin/marketforge`.

### Command organization

Use explicit command groups:

| Group | Responsibility |
|---|---|
| `markets` | Instrument catalogue, specifications, metadata synchronization, and market metrics |
| `acquisition` | Bulk acquisition configuration, archive discovery, planning, and downloading |
| `normalize` | Standalone historical normalization |
| `merge` | Standalone historical merging |
| `convert` | Standalone historical conversion |
| `pipeline` | Saved workflow configurations, planning, and execution |
| `datasets` | Raw acquisition and produced-dataset inspection |
| `runs` | Execution history, logs, reports, reconciliation, and resume |
| `config` | Forge defaults, profiles, and effective settings |
| `storage` | Storage status, inventory reconciliation, and controlled cleanup |
| `database` | Database lifecycle, migrations, backup, and restore |
| `engine` | Installed executable status and Rust-owned configuration inspection |
| `benchmark` | Benchmark entry points and testing-database result management |
| `doctor` | Non-mutating installation and runtime diagnostics |
| `historical-tui` | Launch the historical interface |
| `live-tui` | Reserved launcher for the separate Rust live interface |

Do not expose a generic `tui` command that leaves the historical/live distinction ambiguous.

Calling `marketforge` without a command should display concise help. Launch a TUI through its explicit command.

### Common operation vocabulary

Use consistent meanings across applicable command groups:

| Action | Meaning |
|---|---|
| `validate` | Validate saved configuration structure, values, and internal references |
| `plan` | Resolve concrete inputs, compatibility, effective configuration, and intended work |
| `run` | Execute the operation and persist its outcome |

Acquisition uses `download` as its execution action.

Structural validation must not contact exchange APIs or execute engine jobs.

Planning must not download archive contents or execute normalization, merge, or conversion. Acquisition planning may perform required discovery unless `--cache-only` is selected.

A valid configuration is not proof that requested source data exists or that execution will succeed.

### Markets

Provide:

```bash
marketforge markets list
marketforge markets show <instrument-reference>
marketforge markets sync
marketforge markets refresh
```

`list` and `show` read the catalogue and stored metrics.

Support the agreed filters and sorting, including exchange, symbol, instrument type, market category, turnover, volume where supported, and open-interest value.

Display metric timestamps and units. Do not treat base volume, quote volume, contract volume, and turnover as interchangeable.

`sync` explicitly synchronizes instrument metadata.

`refresh` explicitly refreshes supported market metrics.

Ordinary browsing must not trigger exchange synchronization or historical archive discovery.

### Acquisition

Provide:

```bash
marketforge acquisition new bulk.yaml
marketforge acquisition edit bulk.yaml
marketforge acquisition validate bulk.yaml
marketforge acquisition map bulk.yaml
marketforge acquisition plan bulk.yaml --cache-only
marketforge acquisition download bulk.yaml
```

Use the same bulk configuration model for single-entry and multi-entry requests.

`new` creates an editable example without overwriting an existing file implicitly.

`edit` opens the selected file through the configured editor.

`map` performs cached discovery and persists archive identities, source locations, searched coverage, and scan outcomes.

`plan` reports the selected archives, reusable local acquisitions, required downloads, and unresolved coverage. `--cache-only` prohibits discovery requests and reports what remains unknown.

`download` uses the same catalogue and planning services. It may resolve required discovery before freezing the selected physical archive set.

Use bounded asynchronous discovery and downloads with shared quota limiting.

Use file-count progress. Exact remote sizes and additional size-probing requests are not required.

Persist the final acquisition report, including failed and registration-pending items.

### Normalization, merge, and conversion

Provide the same structure for each engine operation:

```bash
marketforge normalize validate normalization.yaml
marketforge normalize plan normalization.yaml
marketforge normalize run normalization.yaml

marketforge merge validate merge.yaml
marketforge merge plan merge.yaml
marketforge merge run merge.yaml

marketforge convert validate conversion.yaml
marketforge convert plan conversion.yaml
marketforge convert run conversion.yaml
```

`run` must execute the complete operation, including job construction, engine invocation, report interpretation, output validation, and registry updates.

Do not present job-file generation alone as successful operation execution.

Use normalization terminology in the public CLI. Inspect existing Rust contracts before changing executable names or protocol identifiers that still use older terminology.

Rust remains authoritative for historical formats and operation contracts. Python translates orchestration intent into supported engine inputs.

### Pipelines

Provide:

```bash
marketforge pipeline list
marketforge pipeline new historical.yaml
marketforge pipeline edit historical.yaml
marketforge pipeline validate historical.yaml
marketforge pipeline plan historical.yaml
marketforge pipeline run historical.yaml
```

`list` reads the saved pipeline configuration library.

Use:

```text
<repository-root>/data/.config/pipeline/
```

Also support explicitly supplied configuration files outside that directory.

Resolve referenced configuration paths relative to their containing configuration file.

Freeze the submitted configuration and resolved bindings before executing the corresponding work.

Users reference earlier step outputs rather than manually supplying identities for datasets produced within the same pipeline.

Standalone operations and pipeline steps must call the same services.

### Dataset inspection

Provide:

```bash
marketforge datasets list --kind raw
marketforge datasets list --kind normalized
marketforge datasets list --kind merged
marketforge datasets list --kind converted

marketforge datasets show <reference>
marketforge datasets schema <reference>
marketforge datasets sample <reference> --rows 20
marketforge datasets lineage <reference>
marketforge datasets verify <reference>
```

Keep raw acquisition identities distinct from produced-dataset identities.

Use an unambiguous reference syntax, for example:

```text
raw:123
dataset:456
```

Do not infer identity solely from a numeric ID or filesystem path.

Inspection must expose relevant storage locations, lifecycle state, coverage declarations, reports, and lineage.

Schema and sample operations must be bounded and supported only for applicable representations. Do not recursively scan or load a large dataset merely to render a preview.

`verify` reports the checks actually performed. It must not equate file availability or a matching local checksum with complete market-data coverage.

### Runs, reports, and recovery

Provide:

```bash
marketforge runs list
marketforge runs show <run-id>
marketforge runs logs <run-id>
marketforge runs report <run-id>
marketforge runs reconcile <run-id>
marketforge runs resume <run-id>
```

Use execution records for standalone operations as well as complete pipelines.

Expose step and attempt outcomes, resolved inputs, output identities, failure details, and artifact references.

`reconcile` determines the actual state of unfinished attempts and may complete valid pending registration. It must not repeat expensive engine execution.

`resume` reconciles before continuing eligible unfinished work.

Preserve completed outputs and existing dataset identities. Record execution retries as separate attempts.

Do not introduce remote job submission, a daemon, or a second client controlling an already-running TUI.

### Configuration

Provide:

```bash
marketforge config show
marketforge config edit
marketforge config validate
marketforge config effective --file historical.yaml
```

Manage Forge defaults and established application profiles.

`effective` should show the resolved settings for a submitted operation or pipeline, including relevant override sources.

Do not duplicate every YAML field as a CLI flag. Provide a small, consistent set of operational overrides.

Rust-owned historical formats remain under their established configuration mechanism. Forge configuration must not become a competing authority for normalization rules.

### Storage

Provide:

```bash
marketforge storage status
marketforge storage reconcile
marketforge storage cleanup
```

`status` shows configured roots, filesystem free space, registered local usage, and known incomplete material.

`reconcile` checks inventory against local artifacts and updates availability appropriately. Do not silently redownload or delete material.

`cleanup` must show the selected material before deletion and require explicit authorization to remove it.

Preserve editable configurations, active inputs, registry identities, lineage, and required execution records.

Storage reconciliation concerns local availability. Run reconciliation concerns execution outcomes and registration.

### Database maintenance

Provide:

```bash
marketforge database status
marketforge database start
marketforge database stop
marketforge database migrate
marketforge database backup
marketforge database restore <backup-file>
```

`start` and `stop` apply only to the recorded MarketForge-managed PostgreSQL container.

Do not attempt to manage an externally configured PostgreSQL service.

Migrations and restore are explicit operations. Ordinary launches must not apply migrations or overwrite databases.

Backup and restore must follow the agreed registry recovery requirements. Restoring the database does not restore missing archive or dataset bytes.

### Engine inspection and diagnostics

Provide:

```bash
marketforge engine status
marketforge engine formats
marketforge engine profiles
marketforge doctor
```

`engine status` reports installed executables, versions, installation provenance, and compatibility problems.

Format and profile inspection must use the Rust-owned configuration mechanism or an established native interface. Do not invent unsupported binary flags or recreate competing format definitions in Python.

`doctor` checks the environment, executable compatibility, database readiness, schema compatibility, storage configuration, and required engine configuration.

Diagnostics must report problems without changing installation state.

Help, version information, and basic diagnostic reporting must remain useful when the database is unavailable.

### Separate TUI entry points

Provide:

```bash
marketforge historical-tui
marketforge live-tui
```

`historical-tui` launches the Python historical orchestration interface defined in the TUI specification.

`live-tui` is initially a placeholder for launching the separate native Rust live interface.

Do not add live controls to the historical interface.

Until integration is implemented, invoking `live-tui` must return a clear integration-pending message and a nonzero exit code. It must not launch an unrelated interface or simulate success.

## Benchmark Commands

### Initial implementation status

Reserve the benchmark command interfaces now, but leave execution and report integration as explicit placeholders until the historical engine benchmark contracts are settled.

Do not guess report schemas or implement speculative adapters.

Benchmark functionality remains separate from the end-user historical TUI.

### Command interface

Reserve:

```bash
marketforge benchmark list
marketforge benchmark run <benchmark-name> --config benchmark.yaml
marketforge benchmark import <report-file>
marketforge benchmark show <execution-id>
```

| Action | Intended behavior |
|---|---|
| `list` | Show known benchmark types and their implementation availability |
| `run` | Invoke the corresponding Rust benchmark binary and register its results |
| `import` | Register an existing report without executing the benchmark again |
| `show` | Inspect a registered execution, its trials, and artifact references |

Initially, help and availability listing may work. Unimplemented actions must fail explicitly with a nonzero exit code.

Do not create successful execution records, fabricate metrics, or produce empty reports to satisfy placeholder commands.

### Testing database only

All benchmark execution records and imported results must use the explicitly configured testing database.

Require:

```text
MARKETFORGE_TEST_DATABASE_URL
```

Never fall back to the production connection.

Validate the actual connected database identity and its designated testing purpose. Reject production targets even if their connection string was supplied through the testing environment variable.

Do not provide a flag that bypasses this restriction.

The testing database receives the shared core migrations plus the separate benchmarking migration stream. Production must not receive benchmark tables through normal setup.

### Benchmark storage and results

Benchmark outputs must use isolated benchmark/testing destinations.

Production datasets may be explicitly selected as read-only inputs. Benchmarks must not overwrite production artifacts.

When implemented, the Python runner will invoke the real benchmark binary, retain its original report, validate the operation-specific schema, and register execution and trial results.

Report import must be idempotent. Failed and partial benchmark outcomes must remain distinguishable from successful executions.

## Shared CLI Behavior and Architecture

### Presentation and automation

Support `--silent` for applicable execution commands. It suppresses progress presentation while retaining final outcomes, errors, reports, and registration.

Support `--json` for applicable inspection and result commands, using a versioned output structure.

Keep machine-readable results on stdout and diagnostics or progress on stderr. Do not mix terminal styling into JSON output.

Define consistent exit behavior. Failed, cancelled, partially completed, and registration-pending operations must not return unconditional success.

### Single ownership and scheduling

CLI and historical TUI entry points must obey the same single-owner policy for the configured workspace/data root.

A competing operational invocation must report the active owner and exit.

Help and version parsing must not initialize databases or acquire execution resources. Diagnostics may report an existing ownership conflict.

Asynchronous acquisition runs within the single owner. Historical engine jobs execute sequentially and retain their own internal parallelism.

### Responsibility boundaries

Keep command handlers thin:

| Responsibility | Application owner |
|---|---|
| Catalogue, metadata, and market metrics | `forge/markets/` |
| Acquisition | `forge/historical/archives/` |
| Rust historical adapters | `forge/historical/engine/` |
| Workflow definitions and resolution | `forge/historical/pipelines/` |
| Attempts, supervision, and recovery | `forge/orchestration/` |
| Dataset inspection and lineage | `forge/datasets/` |
| Shared infrastructure | Appropriate modules under `forge/shared/` |
| Argument parsing and CLI presentation | `forge/cli/` |
| Historical interface presentation | `forge/tui/` |

CLI and TUI must call the same application services. Do not duplicate SQL, discovery, limiting, process supervision, or registration inside presentation handlers.

### Acceptance criteria

The public launcher is `marketforge`, installed locally as `.bin/marketforge`.

Historical operations are independently accessible through the CLI, with consistent validation, planning, execution, reporting, and recovery behavior.

Historical and live TUI entry points are explicit and separate.

Benchmark interfaces are reserved with honest placeholder behavior and an enforced testing-database-only persistence policy.

CLI verification must cover argument handling, service dispatch, machine-readable output, failure exit behavior, singleton enforcement, placeholder failures, and rejection of production database targets for benchmarks.
