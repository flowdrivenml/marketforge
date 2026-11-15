I would make Forge a workspace for selecting data, preparing historical workflows and following their execution. Most of the existing CLI commands fit naturally into five main areas.

The attachment also confirms that the old process and merge commands only generate jobs. The TUI’s execution controls should use the new orchestration services we’ve planned.

My suggested navigation would be:
Workspace	What the user does
Markets	Browse instruments, filter exchanges and market types, inspect specifications and metrics, select acquisition targets.
Acquisition	Discover archives, inspect cached availability, prepare bulk downloads, manage local raw files and select them for processing.
Runs	Start individual operations or saved pipelines; inspect active/history runs, steps, logs, failures and results.
Datasets	Browse registered outputs, inspect schemas, coverage, reports and lineage; select compatible datasets for merging or conversion.
Settings	Manage profiles, request limits, storage roots, database connections and maintenance operations.

Benchmark executions can appear in Runs, with testing-only comparison tools. Detailed research remains in the laboratory and its chosen tools.

Use a consistent layout throughout.

Keep the environment, database connection state and active-run count visible in the header. Use a narrow navigation column, a main table or form, and a contextual inspector for the selected item. At smaller terminal sizes, open the inspector separately.

This makes the interface predictable: select something, inspect it, then choose an action. Less common operations can be available through a command palette.

Markets should feed a selection basket. Users could filter Bybit instruments, select several, switch to Binance and add more without losing their selection. Include family/base-coin targets where archives are bundled. Show unsupported source/data combinations with an explanation.

Browsing should read the catalogue. Exchange refresh and discovery should be explicit actions, with freshness displayed—moving through table rows must never trigger network requests.

Acquisition should follow the workflow we agreed on:

    Configure → Validate → Resolve → Review → Run

Offer both a guided form and configuration-file loading. They should produce the same validated request.

The review should clearly distinguish locally reusable files, required downloads, unknown discovery coverage and unavailable periods. Display unknown sizes as unknown, rather than presenting a misleading total.

Local archive inventory belongs here too, with actions to verify files, prepare processing or remove selected local copies. Deletion should show the exact affected files and update their database records.

Runs is where the pipeline becomes understandable.

Selecting a run should expose its steps and tasks, with separate views for inputs, outputs, logs, reports and configuration. Users should see why a step is waiting—for example, its input dataset has not been published or registered.

Keep important states distinct:

    Configuration valid, but unresolved.
    Ready to execute.
    Running or waiting for a dependency.
    Engine finished, registration pending.
    Complete, failed or cancelled.

Show measurable progress: bytes when a total exists, task counts and phases otherwise. Avoid inventing an overall percentage or ETA for a pipeline whose remaining work is unknown.

Datasets should make IDs convenient rather than burdensome. Show recognizable names, content, source and interval first, with IDs available for traceability and copying. The inspector should expose lineage, storage location, report, manifest and a bounded schema/data preview. Pipeline users continue referencing step outputs internally.

For implementation, I would use Textual. Its widgets, styling and asynchronous workers provide suitable building blocks for these screens. The CLI and TUI should call the same application services; screen handlers should not contain SQL, archive-discovery rules or subprocess lifecycle logic.

For long-running jobs, I recommend a separate local execution coordinator under orchestration. That would let users close the TUI and reconnect. PostgreSQL persistence alone cannot keep work alive when its owning process exits.

Cancellation and retry must use that coordinator. I would avoid a generic pause button until each operation has a defined safe pause mechanism.

The styling can follow the engine exactly. I checked
its palette: it uses Tokyo Night. Keep those colors in one Python theme mapping, alongside consistent borders, spacing, status labels and number formatting. Invoke the Rust engine without its own interactive TUI while Forge controls the terminal.

I would start with the shell and catalogue/registry browsers, then connect acquisition, engine execution and pipelines as their shared services become ready.

Below is a clickable layout proposal using the actual palette and illustrative data. You can switch workspaces, inspect rows and explore dataset details.


## Detailed Specification: Forge Historical TUI, Progress Displays and Operation Logs

### Purpose and scope

Build a compact TUI for advanced users operating the historical part of MarketForge.

The interface should support browsing markets, mapping historical archives, running bulk downloads, executing historical pipelines and inspecting registered data.

Configuration is primarily expressed through YAML files edited in an external editor. The TUI provides navigation, previews, validation, execution and result inspection.

This specification supersedes earlier proposals where they conflict with the simplified interface described here.

The main navigation is:

```text
Markets
Acquisition
Pipelines
Datasets
Logs
Settings
```

Do not include live-engine controls, benchmark tools or development/testing database selection.

The existing live TUI remains separate.

### Presentation and shared behavior

Use the engine's semantic palette from:

```text
engine/src/shared/terminal/palette.rs
```

Apply the same palette to Forge and to the redesigned mapping/download progress displays.

Keep the interface compact, with tables, readable details and a small number of contextual actions.

Do not build graphical pipeline editors or extensive configuration forms.

Use consistent units, timestamps, status labels and number formatting. Accompany status colors with text.

The interface uses the application's configured database. Display connection status without introducing production/testing environment controls.

### Configuration files

The pipeline configuration directory is:

```text
<project-root>/data/.config/pipeline/
```

Resolve this from the project root, independently of the shell's current working directory.

Create the directory when needed. Pipeline YAML files stored there form the pipeline configuration library.

A separate fixed acquisition configuration directory is not mandated by this note, following the withdrawal of that earlier proposal. Acquisition must support opening, creating and editing a user-selected bulk-acquisition YAML file. Do not hard-code `data/.config/acquisition/` without a subsequent decision.

Configuration files are user-owned documents. Generated job specifications and execution snapshots must be stored separately.

Do not delete configuration files during storage cleanup.

### YAML editing and validation

Acquisition and Pipelines share the same basic file interaction:

| Action | Behavior |
|---|---|
| New | Create a minimal configuration from an appropriate example and save it to the selected filename. |
| Open | Load a selected YAML file. |
| Edit | Temporarily release the terminal and open the file in the configured editor. |
| Validate | Parse the saved file, validate its schema and display errors. |
| Inspect | Display the saved configuration or a concise summary. |

After the editor exits, reload the saved file. Do not assume it remains valid because it was valid before editing.

Do not replace the external editor with a complex form builder.

Distinguish:

- YAML syntax errors, including line and column where available.
- Pydantic field/type errors, including the affected field path.
- Semantic errors, such as invalid intervals or incompatible operation settings.
- Resolution errors, such as unavailable inputs or missing reference datasets.

For example:

```text
sources[1].symbols
Expected a non-empty list.

interval.end
Must be later than interval.start.
```

A valid YAML structure does not establish that remote data exists or that engine inputs are compatible.

Before execution, reload and validate the saved file, then perform the required resolution checks. Freeze the submitted configuration and resolved plan for that execution.

Editing the original file afterward must not modify an already started run.

### Markets

Provide an instrument and archive-scope browser using the existing catalogue and market-metrics services.

Retain existing sorting by turnover, open-interest value and symbol.

Support useful volume views and sorting where implemented, with the quantity basis clearly identified:

- Base-asset volume.
- Quote-asset volume.
- Contract volume.
- Source-native volume where relevant.

Do not present these measurements as interchangeable. Display turnover denomination and metric timestamps where applicable.

The main table should support filtering by exchange, symbol, instrument type, market category and relevant assets.

The details view should expose instrument specifications, available metadata and supported historical acquisition scopes.

Historical browsing must allow inactive instruments and known archive-only scopes where supported by the catalogue.

Metadata synchronization and metric refresh are explicit operations. Ordinary browsing must not trigger exchange discovery or refresh requests.

Provide practical copy/export actions for using selected instruments or scopes in acquisition YAML. An extensive selection wizard is unnecessary.

### Acquisition

Acquisition supports bulk operations. A request containing one instrument is still handled through the same bulk-acquisition path.

Primary actions are:

```text
New
Open
Edit
Validate
Map
Download
```

Display the selected YAML filename, its validation status and a concise request summary.

#### Map

Map performs historical archive discovery for the scopes and intervals described by the configuration.

It must use the persistent discovery catalogue and cache policies defined in the earlier archive specification.

Persist:

- Discovered archive identities and metadata.
- Current download locations.
- Successfully searched scopes/windows.
- Successful searches returning no archives.
- Partial or failed searches.
- Observation and freshness information.

Mapping does not download archive contents.

Preserve every valid archive returned by a source operation, including useful entries outside the immediate selection when the provider returns a broader listing.

Produce a durable mapping report and make its outcome visible in the global Logs tab.

#### Download

Download uses the same bulk configuration and persistent catalogue.

Reuse known archive identities and completed local acquisitions where appropriate.

If discovery is insufficient, resolve the required unknown or stale scopes before establishing the execution plan. Do not silently treat unknown coverage as absent data.

Download-location refresh must be independent of archive identity. Temporary or expired URLs may be refreshed through the smallest supported source scope.

Do not repeat discovery of an entire request solely because one download URL expired.

Standalone downloading and pipeline acquisition must call the same application services.

### Mapping progress

Mapping may not know how many archives it will discover.

Its progress display should show available measures such as:

- Scopes or discovery windows scheduled.
- Scopes/windows completed.
- Cached scopes reused.
- Archives discovered.
- Failed or incomplete scopes.
- Current source and operation.

When a meaningful total is unknown, show counts and current activity without inventing a percentage or ETA.

Mapping completion means the requested discovery work completed successfully. It does not certify gap-free market data inside the archives.

### Download progress

Redesign the current download presentation to match the engine palette and terminal style.

Use file counts as the primary progress measure. Total archive bytes must not be required.

After resolution, establish the selected physical archive set and its total count.

The display should distinguish:

| Counter | Meaning |
|---|---|
| Selected | Total unique physical archives in the resolved selection. |
| Reused | Selected files already available locally under the applicable verification policy. |
| Downloaded | Newly transferred files successfully finalized and registered. |
| Failed | Selected files that could not be made ready. |
| Remaining | Files still awaiting a final outcome. |
| Registration pending | Transfers finalized on disk but awaiting successful database registration. |

The primary readiness measure is:

```text
Ready files = Reused files + Newly downloaded and registered files
```

A failed attempt must not count as a successfully acquired file.

Retries must not increase the total archive count or count the same archive as multiple successes.

Do not show unconditional successful completion merely because every transfer was attempted.

A newly downloaded file becomes ready only after applicable verification, filesystem finalization and PostgreSQL registration succeed.

If database registration fails after file finalization, preserve the recoverable registration-pending state. Do not automatically download the same bytes again.

Byte counts or transfer rates may be supplementary information. Unknown sizes must remain unknown.

Do not issue additional requests solely to obtain file sizes for the progress display.

Keep the selected file set stable during execution. Any necessary plan change must be explicit and recorded.

### Pipelines

Pipelines is a configuration launcher and result viewer, not a background-job dashboard.

Primary actions are:

```text
New
Open
Edit
Validate
Run
```

Resolution and compatibility checks remain part of execution preparation, even without a separate Resolve button.

Support pipelines containing acquisition/download, normalization, merge, conversion and future registered operation types.

A standalone operation may be represented as a one-step pipeline.

Do not implement a separate downloader for pipelines. Pipeline acquisition uses the same cache, limiter, download service, registration and reporting behavior as standalone Acquisition.

Users reference step outputs rather than manually entering IDs for datasets produced earlier in the pipeline.

Forge allocates the required identities and translates the resolved workflow into engine jobs.

### Attached execution

The initial TUI executes one operation or pipeline at a time and waits for its outcome.

Do not require a daemon, detached coordinator or active-runs dashboard for this version.

Continue persisting run, step and attempt state for reporting and recovery.

Independent transfers within a bulk download may run concurrently. Rust stages retain their own internal parallelism.

When interactive terminal displays are used, serialize terminal ownership. Do not launch several engine TUIs into the same terminal simultaneously.

### Execution display modes

Support a global default presentation mode, with optional configuration overrides for an operation or pipeline step.

| Mode | Behavior |
|---|---|
| Progress | Display mapping/download progress or the engine's native TUI for the current operation. |
| Silent | Suppress progress presentation while retaining reports, logs, checks and database updates. |

Support current-terminal execution and a separate progress window through the terminal-launch integration.

A separate progress window is used when selected and supported by the configured launcher. Current-terminal handoff remains the fallback execution method offered when a compatible window launcher is unavailable.

Do not silently change a requested mode without reporting why.

Silent mode does not mean “do not report failures” or “do not register outputs.” Do not promise a fixed performance improvement.

#### Current-terminal handoff

Temporarily suspend Forge's rendering and terminal input handling.

Run the editor, progress application or engine stage attached to the terminal, then restore Forge afterward.

Restoration must occur after normal completion, errors and supported cancellation paths.

Do not keep Forge and the child TUI drawing concurrently.

#### Separate progress window

Use a terminal-launch adapter rather than scattering emulator-specific commands throughout the TUI.

The launched execution must have a known operation/run and attempt identity.

Track actual execution completion through its runner status, exit receipt and durable report. The terminal emulator's launcher process may return before the command finishes and is not sufficient evidence of completion.

Closing a progress window must never be interpreted as successful execution. Detect interruption or an unknown outcome and reconcile it before retrying.

Prefer one progress window per operation or pipeline invocation. For a pipeline, reuse it across sequential stages where supported, rather than opening a window for each archive.

Step-specific presentation overrides may suppress a stage's TUI or request progress presentation. They must not create overlapping interactive terminal owners.

The parent Forge interface may show a simple waiting state. It does not need to become a live activity dashboard.

### Pipeline completion

After each output-producing engine stage:

- Read its durable completion report.
- Validate the expected job/output identities and publication metadata.
- Register the dataset and lineage in PostgreSQL.
- Release dependent work only after registration succeeds.

If Rust succeeds but registration fails, retain registration pending and support reconciliation without blindly repeating the engine work.

At the end, show a concise result containing:

- Overall outcome.
- Step outcomes.
- Produced archive/dataset identities.
- Report locations.
- Failures, blocked dependencies or pending registration.

Write a durable final pipeline report and expose it in Logs.

### Datasets

Provide these categories:

```text
Downloaded archives
Normalized
Merged
Converted
```

“Normalized” is the interface label for the engine operation currently called `process`.

Derive categories from registry metadata, not directory names.

Downloaded archives continue using the acquisition inventory. Produced datasets use the dataset registry. A unified TUI browser does not require forcing these records into one database table.

Retain the previously agreed detail views:

- Summary and storage location.
- Schema and bounded sample preview.
- Declared coverage and reported event bounds.
- Available integrity results.
- Raw-input and dataset lineage.
- Manifests, reports and execution references.

Make IDs and paths easy to inspect and copy without requiring users to enter generated identities during routine pipeline execution.

### Global Logs tab

Logs is a main application tab, separate from Acquisition and Pipelines.

It provides one place to inspect successful and failed historical operations.

Include explicit actions and execution types such as:

- Metadata/metric refresh.
- YAML validation and resolution.
- Mapping.
- Bulk downloading.
- Normalization.
- Merge.
- Conversion.
- Complete pipelines.
- Reconciliation and cleanup operations.

The main list should show timestamp, operation kind, configuration/run name, outcome, duration where known and a concise summary.

Support filtering by operation, outcome, time interval and relevant run/dataset identity.

Opening an entry should expose:

- Structured summary.
- Detailed warnings and errors.
- Configuration snapshot.
- Inputs and outputs.
- Step/attempt information where applicable.
- Report and available log locations.

Link stage entries to their parent pipeline run so users can inspect either the complete workflow or one failed stage.

### Persistent reports and events

Create an operation record early enough to retain launch, validation and mapping failures.

All execution modes must use the same reporting path.

Preserve original engine reports. Normalize useful management fields into PostgreSQL and retain references to the original artifacts.

Use structured application events for significant transitions. Do not write a database event for every network chunk or progress repaint.

Do not treat captured terminal escape sequences as structured logs. Where native engine TUIs do not provide a separate log stream, retain their structured result reports and the application-level execution events.

Standalone and pipeline invocation of the same operation must produce compatible reports.

### Mapping report

Record at least:

- Operation ID and report version.
- Submitted/resolved configuration references.
- Requested scopes and intervals.
- Cached discovery reused.
- Successfully searched scopes/windows.
- Empty, partial and failed searches.
- Archives discovered or updated.
- Start/completion timestamps.
- Errors and overall outcome.

A mapping report must distinguish observed source availability from actual data-content integrity.

### Download report

Record at least:

- Operation ID and report version.
- Selected remote archive identities.
- Reused local files.
- Successfully downloaded and registered files.
- Failed and unresolved acquisitions.
- Registration-pending files.
- Final local inventory references.
- Applicable checksums, sizes and verification results.
- Attempts and errors.
- Start/completion timestamps and overall outcome.

Preserve enough identity information to refresh a URL without confusing it with a new archive.

### Pipeline report

Record at least:

- Run ID and report version.
- Submitted configuration snapshot and checksum.
- Resolved configuration and input bindings.
- Effective operation profiles and display settings.
- Step and attempt outcomes.
- References to individual stage reports.
- Produced archive/dataset IDs.
- Dataset-registration outcomes.
- Failed or blocked dependencies.
- Start/completion timestamps and overall outcome.

Reports must remain available after the progress window closes.

### Settings

Settings provides meaningful global defaults for all historical operations.

Include:

| Area | Controls |
|---|---|
| HTTP | Request rates, concurrency, retries and timeouts. |
| Downloads | Concurrent transfers and relevant acquisition policies. |
| Normalization | Worker, memory and scratch budgets. |
| Merge | Applicable resource and output profiles. |
| Conversion | Applicable resource profiles and reusable conversion defaults. |
| Parquet output | Target file sizes, row-group sizes and supported compression settings. |
| Presentation | Progress/silent default, terminal presentation and external editor. |
| Storage | Usage, budgets and maintenance information described below. |

Display effective settings so users can understand the defaults applied to a run.

Keep configuration minimal. Do not expose raw-format definitions, normalization mappings or other stable internal specifications as ordinary editable settings.

Keep operation-specific choices in the relevant YAML or an established reusable profile.

Apply profile changes to future executions. Do not imply that editing Settings dynamically resizes a running offline engine operation.

### Managed paths and storage

Use the configured project data root. Routine users should not need to enter job IDs, output directories, staging paths or manifest locations.

Forge generates and records those locations internally.

A possible layout for new outputs is:

```text
data/
├── .config/
│   └── pipeline/
├── raw/
├── datasets/
│   ├── normalized/
│   ├── merged/
│   └── converted/
└── runs/
    └── <run-id>/
        ├── submitted.yaml
        ├── resolved-plan.json
        ├── report.json
        ├── events.jsonl
        └── steps/
            └── <step-id>/
                └── <attempt-id>/
                    ├── job.json
                    └── ...
```

This layout does not authorize moving existing registered data.

Respect each engine operation's actual output and staging contracts. Reports and manifests may remain beside datasets when that is where the engine publishes them; record their actual locations.

Normalization, merge and conversion produce distinct datasets. Do not move an upstream dataset merely to change its category.

### Storage information and cleanup

Storage, within Settings, should show:

- Configured data root and applicable scratch locations.
- Filesystem free space.
- Registered size by dataset category.
- Actual staging locations and incomplete outputs.
- Relevant scratch/storage budgets.
- Explicit usage refresh and reconciliation actions.

Normal navigation should use registry metadata rather than recursively scanning every file.

Keep filesystem free space and registered dataset size distinct.

Start with explicit cleanup. Show the selected local material and its known size before deletion.

Protect active inputs, editable configuration files and required run/report records. Preserve remote catalogue entries, dataset identities and lineage after local bytes are removed, updating local availability accordingly.

Do not introduce a general automatic retention system in this task.

### Application architecture

The TUI calls shared application services. It must not duplicate archive discovery, rate limiting, engine invocation, result validation or registry updates inside screen handlers.

Separate:

```text
TUI navigation and file previews
    Application validation and execution services
        Archive services
        Historical engine adapters
        Registry and reporting services
        Terminal presentation adapter
```

Progress and silent presentations consume the same operation state and outcomes.

The CLI should continue using the same application services.

Keep existing live interfaces and development benchmark tooling outside this implementation.

### Acceptance criteria

- The main tabs are Markets, Acquisition, Pipelines, Datasets, Logs and Settings.
- Markets retains existing metric views and sorting, with clearly identified units.
- Acquisition operates through bulk YAML and explicit Map/Download actions.
- Mapping persists discoveries and search completeness and produces a report.
- Downloads reuse cached archive identities and refresh temporary locations when necessary.
- File counts are the primary download-progress measure.
- Failed or registration-pending files are not counted as fully acquired.
- YAML can be created, opened and edited without a complex form builder.
- Validation errors identify the affected syntax location or configuration field.
- Pipelines can include acquisition, normalization, merge and conversion.
- Current-terminal and supported separate-window execution preserve correct process ownership.
- Only one interactive progress application owns a terminal at a time.
- Silent mode preserves reports, logs, checks and registration.
- Every operation and pipeline outcome is inspectable through global Logs.
- Produced datasets are registered before dependent steps proceed.
- Generated output paths are managed internally.
- Storage cleanup preserves configuration, active inputs and provenance.
- No live controls, benchmark screens or testing-database selector appear in the TUI.

