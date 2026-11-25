# Forge workspace architecture and action inventory

The workspace is a Textual client of Forge domain services. The direct CLI
continues to own its own parsing, presentation, JSON formatting, and exit
status. Workspace forms have an explicit catalogue of supported actions and
typed fields. Displayed shell commands are for interoperability.

```mermaid
flowchart LR
    CLI[Direct CLI] --> Services[Forge services]
    TUI[Textual screens and forms] --> Queries[Debounced screen queries]
    TUI --> Controller[Application operation controller]
    Queries --> Services
    Controller --> Lock[Workspace ownership]
    Lock --> Services
    Services --> Engine[Owned process and readers]
    Services --> Report[Existing run artifacts]
    Report --> TUI
```

Query workers are exclusive. Responses also carry a generation so an obsolete
response cannot replace current filters. Application-owned operations survive
navigation and retain the workspace lock through cleanup. Engine jobs use the
async historical supervisor and retained diagnostic logs. Synchronous tooling
with subprocesses uses a joined process-group bridge. Textual owns the terminal
while rendering.

The shell handles navigation, selection, and query generations. `resources.py`
adapts bounded query results to typed selections and `presentation.py` defines
resource-specific columns and contextual argument bindings. `inspection.py`
presents subject sections and tables with a separate raw evidence view.
`operation_view.py` renders structured snapshots and updates available actions
as the controller records results. Shared Tokyo Night styling lives in
`workspace.tcss`.

To add an action, define its fields, workspace area, and side-effect class in
`forge/tui/actions.py`. Connect it to a service in `workflows.py` or
`services.py`. Reuse existing request models and receipts. Add a parser
coverage assertion or an interaction test for new forms. There is no
shell or argparse dispatch from a widget.

## Supported actions

These rows are grouped by shared boundary. Forms expose the supported input
routes explicitly. The terminal-adapter options of direct CLI commands
such as per-command `--tui`, `--silent`, and JSON presentation remain CLI
options. Workspace views supply their own presentation.

| CLI family and actions | Workspace area | Main shared service boundary | Side effects and evidence |
| --- | --- | --- | --- |
| Pipeline validate, plan, run, resume, status | Workflows, Runs | Pipeline recipe loader, read-only plan, PipelineRun and retained report | Network and Engine only on run or resume. Snapshots and exact registration recovery evidence |
| Acquisition list, new, edit, validate, plan, map, download | Workflows, Recipes | Configuration library, archive plan and acquisition operation | Map updates discovery evidence. Download also verifies and registers local bytes |
| Acquisition import new, inspect, validate, plan, run | Workflows | Draft creation, freeze inputs, sample, run_import | Bounded explicit samples and raw import reports |
| Acquisition register new, edit, inspect, validate, plan, run | Workflows | Registration declaration and registration apply/validate | Registry-attached plans and registration evidence |
| Acquisition profiles list, sample, inspect, validate, add, register | Recipes & Profiles | Bounded sample, profile discovery, Rust validation and installed profile registration | Add and register publish the validated profile |
| Normalize new, validate, prepare, execute, run, register | Workflows, Runs | Batch preparation, async execution, publication verification and registration recovery | Prepared batches and execution intents stay distinct |
| Merge new, validate, prepare, run, register | Workflows, Runs | Source selection, direct or derived preparation, async execution and registration | Direct and derived source routes, exact file selection and pinned input evidence |
| Convert new, list, show, validate, prepare, run | Workflows | Conversion recipes, managed Engine validation, prepared collections and execution | Upstream recipe routes and exact merged-publication binding. Compatible reference streams and target currency are editable in the TUI |
| Convert config show, get, schema, validate | Workflows | Managed converter runtime inspection | Configs → Convert offers typed and validated draft updates |
| Datasets list, find, show, streams, lineage, coverage, verify, schema, head, migrate-registration, refresh-summary | Data | DatasetRequest, retained publication query, bounded Parquet inspection and migration | Registry file and publication identities stay separate. Sampling and verification are explicit |
| Storage coverage, list, show, verify, reconcile, prune, delete | Data | StorageFilters, registry browsing, maintenance and saved cleanup plans | Verify and reconcile may mutate state. Cleanup applies the reviewed digest |
| Markets sync, refresh, list, show | Markets | Collected metadata service and stored observation queries | Network only for sync and refresh |
| Markets rank new, validate, build, list, show, update, delete | Markets | Ranking policy, snapshots, workflow and reviewed snapshot deletion | Build uses retained observations. Update refreshes sources. Deletion recomputes eligibility |
| Catalog exchange, instrument and specs add, list, show, validate | Catalogue | Declaration models and the selected PostgreSQL registry | Draft reviews check dependencies and conflicts. Add uses the existing immutable identity and specification revision rules. |
| Runs list, show, logs, report | Runs | Retained report/log inspection | External liveness stays unknown |
| Engine status, build | Command palette | Managed inventory, build planning and installation | Dry-run shows the plan. Builds own Cargo and preserve approved installations |
| Database status, migrate, backup, restore | Command palette | Testing identity, migrations and guarded backup utilities | Restore uses a fresh explicitly named disposable database |
| Init, doctor, config show | Command palette | Initialization services, live prerequisite evidence, effective settings | Init can collect and write. Edit Forge application settings opens the application draft editor |

The retired `acquisition import format new` compatibility command remains
a CLI error and is omitted from working actions. The `tui` launcher is the
workspace entry point. The inventory test compares supported parser leaf
actions with the explicit form catalogue. Bounded tests exercise form
validation, navigation during work, query generations, cancellation and
workspace ownership. The guarded PostgreSQL integration enters the
pipeline through a Textual form, then verifies its retained preparation
and runs the real Engine and registration paths. Destructive and remote
operations are tested through their established service fixtures.

## Registry-backed workflow handoffs

Workspace view state owns the current cursor, query, sort, page and selection
basket. Switching registry environments reopens the Textual workspace with a
fresh application context after terminating workers. The launch loop carries
over only the selected area and view. Background queries retain the latest
version and discard an older response if a newer query started.

Exact archive selection stores registry-scoped evidence hashes and optional
compiled profile pins. Preparation re-resolves each retained archive through
Forge and checks the exact local-copy identities during execution. The batch
planner groups inputs by content and submits schema v4 jobs to Rust.
Family-scoped discovery retains its request-based route.

Direct Merge recipes list registry primary-file IDs in precedence order and
pin reviewed publication membership and registry identity. The derived route
uses normalization recipe discovery. Both enter the same preparation,
validation, execution and registration services.

Conversion uses a catalogue currency picker and paged external publication
search. Reference selection uses shared registry binding and stream
projections. Preparation and execution share the standalone converter
services. Indexed conversion references are work keys in the selected registry.

Application settings derive constraints from the Pydantic model. Runtime
fields use the Rust schema. Pipeline editing shares runtime composition and
stage validators with preparation. Draft edits preserve supported TOML
formatting, validate candidates and check live file identity at apply time.

Pipeline window mode delegates ownership descriptors to the stage launcher.
Workers send operation snapshots and retain stage results. Publication and
run maintenance use the CLI services and their durable reviewed plans.

The run adapter consumes shared report and log projections. Full log access
uses bounded range reads or explicit byte-preserving export. The clipboard
adapter sanitizes terminal controls and credential fields, enforces a size
cap, and calls desktop and tmux providers in a worker. Textual sends the
OSC 52 fallback.

Data recipe drafts use exact archive pins, registry-bound primary-file
selections, and committed Conversion source bindings. A Merge recipe with
`file_subset = true` verifies the listed file IDs against the pinned
publication IDs and selection fingerprint during preparation. Existing
whole-publication Merge recipes retain whole-publication membership checks.
Data tables select displayed render cells, while inspector summaries use
Textual text selection. Clipboard requests share the standard provider adapter.

Textual Pilot covers navigation, settings, recipe forms and exact copying.
The disposable PostgreSQL fixture drives archive, normalized and merged
publication handoffs through the Engine services. See [Testing](testing.md)
for database and Engine setup.


## Configuration Explorer

The main workspace embeds one shared editor under Configs. General and the four runtime tabs retain their own drafts, search filters, expanded groups, and selection. Field help opens on demand from the field table. Environment switching stays global and routes through the existing registry confirmation.

The explorer projects Rust runtime schemas and the Pydantic application
settings model into fields. Textual retains one draft per file and delegates
validation and replacement to the shared runtime and composition services.
Runtime metadata is refreshed when the managed executable identity changes.
A preview binds the candidate bytes and the inspected configuration identity.

The field documentation contract is an explicit `setting-<dotted-path>` anchor
on the component reference page. Pipeline locators also include the stage
prefix, for example `setting-merge.resources.workers`. Update the reference
when adding a field. The explorer coverage test checks the installed schemas
against the reference entries.


## Textual visual design

Use the Tokyo Night roles in `forge/shared/terminal/palette.py` for Rich
renderables. `forge/tui/design.py` configures the Textual theme for native
widgets. `forge/tui/workspace.tcss` declares the matching palette and surface
tokens used by the workspace and standalone screens. Keep the native theme,
TCSS roles, and Rich accents synchronized when changing the palette.

- Use the application background for reading surfaces and a slightly elevated
  background for panels, field controls, table headers, and dialogs. Use quiet
  outlines where they clarify grouping. Keep dashboard height driven by content.
- Use bold cyan for headings, blue for selected navigation and primary actions,
  and foreground text for values. Use the secondary text token for labels and
  hints. Reserve the darker muted token for unavailable or disabled information.
- Keep navigation, tabs, and action bars one row tall. Use one cell of horizontal
  padding for controls. Use a blue indicator for the selected tab or area, and a
  brighter surface with underlining for keyboard focus.
- Reuse `Results` for resource tables and `RecordTable` for inspector records.
  Use `Cell` projections for width budgets and right-align numeric values. Use
  ellipses for long cells and expose exact values in the inspector or clipboard.
  Keep semantic Rich colors legible in the selected row.
- Reuse `Form` for typed fields, `Confirm` for a review, and `Inspection` for
  structured results. Forms keep field errors next to their inputs and overlay
  actions in a bounded scrollable dialog. Use `variant="primary"` for the main
  action, `classes="tertiary"` for lower-emphasis controls, and
  `destructive=True` for a destructive `Confirm`.
- Preserve the recorded status label when applying colors. Use green for
  successful evidence, cyan for active processing, yellow for pending changes
  or actionable warnings, and red for failures. Verification and registration
  remain separate fields. Use the shared success notification marker for a
  successful action and retain durable outcomes in existing inspectors.
- Keep loading, no matches, and errors distinct. Retain a populated table during
  refresh. Use fixed-height, repaint-only updates for the monitoring bar.
  Preserve scrolling on dense tabs and wide evidence tables, and check the
  workspace at 80×24 and a wide terminal with Textual Pilot.

## Dialog layouts

Use the existing `Form`, `Confirm`, `Detail` and picker classes in
`forge/tui/screens.py`. They share the Tokyo Night roles in
`forge/tui/workspace.tcss`. A modal starts with `.dialog` and a
`.dialog-title`. Use `.dialog-compact` for short forms and confirmations,
the default width for selectors, and `.dialog-large` for long forms that benefit from a viewport-sized body.

- Give a form field a meaningful `Field.group`. The form groups active
  controls in `.form-section` containers with cyan `.form-heading`
  labels and a subtle rule. Leave one row between field groups and keep
  labels next to their controls. Use `.field-pair` for related controls
  such as exchange and instrument type. Fields stack below 96 columns.
  At 116 columns and up, the two historical endpoints share a row.
  Each endpoint retains separate date and UTC time controls.
- Place the fields inside `.dialog-body` and the validation summary
  and buttons in `.form-footer`. Long forms allocate the remaining
  height to the body. Keep one vertical scroll container and the
  actions visible below it, with the primary action at the end. Use
  `.buttons` and `.dialog-buttons` for short confirmation and picker
  action rows. Detail dialogs move additional actions to a second row
  when both groups would exceed the available width.
- Put validation messages in `.field-error` under their own control.
  The footer summarizes remaining required fields, draft edits and
  issues. Leave untouched required fields neutral. Use the existing
  yellow indicators for required labels and changed values, red for
  errors, and green for successful validation. Maintain the visible
  focus background for all three states.
- Use Textual disclosures for repeated requests and Advanced Options.
  Keep the field widgets mounted across disclosure changes. A closed
  header reports enabled state, pending changes or errors where
  applicable. Reveal and focus the first invalid active control after
  validation, including controls in a collapsed section.
- Keep context-menu actions one row high. Use a single gap between
  handoff actions and inspection where both appear. Inspector summaries
  group identity, market data, storage and recorded outcomes. Retain the
  exact values and full-evidence actions for copying and Neovim.

Check dialogs with Textual Pilot at 80×24, 100×30, 120×32 and 160×48.
Exercise the scroll body, keyboard focus, dropdowns and validation with
populated and empty fixtures. Set `MARKETFORGE_FORM_SNAPSHOTS` to an
existing directory to export the workflow form tests as SVGs.

## Recipe field coverage

The authoring form walks the selected Pydantic model's JSON Schema. Counts
below describe top-level model fields. Nested fields use the same descriptors.
The managed filename is a separate form field.

| Recipe model | Total | Required at schema level | Optional or defaulted | Conditional and read-only values |
| --- | ---: | --- | ---: | --- |
| Acquisition | 3 | Interval, requests | 1 | Request classification and scope constrain valid feeds. |
| Normalization, historical requests | 4 | Interval, requests | 2 | Equity requires a source key. Feed options depend on source and data kind. |
| Normalization, exact archives | 4 | Registry ID, archive pins | 2 | Picker supplies verified archive pins. An explicit profile requires a corresponding fingerprint. |
| Normalization, explicit preparation | 7 | Job ID, dataset ID, content type, inputs | 3 | Each archive/member and task ID is unique. Output paths are optional. |
| Merge, exact files | 9 | Dataset IDs, start and end | 6 | Publication selection derives the primary-file IDs, registry identity and selection fingerprint. |
| Merge, derived | 5 | Version, interval, selection | 2 | The interval derives Engine nanosecond bounds. Source recipe selection and current preparation are separate. |
| Conversion | 8 | Version, source, interval, conversion policy | 4 | A bound publication fixes the source, whole-publication selection, and interval. External reference publications require stream IDs. |
| Pipeline | 6 | Version, interval, requests | 3 | Stage switches expose explicit overrides. Reference publication requires stream IDs. |
| Import | 13 | Source key, root, identity, data type | 9 | Requires explicit files or bounded patterns. Specification overrides require base and quote assets. |
| Registration | 4 | Registration, source, inputs, coverage | 0 | Input identity and declaration relationships are validated by the registration model. |

The form places resource requests, fingerprints where editable, bindings,
and other advanced fields under their respective schema-backed groups or
Advanced Options. Complex TOML values remain editable where dedicated
controls are unavailable. Source pickers supply derived binding values that
are locked in the form.


## Existing-recipe handoffs

Markets uses the verified registry row and the historical source adapter to
offer supported data kinds for the exact instrument scope. Acquisition and
Pipeline use the same request mutation service for preview and save. The
service checks canonical request equality, validates the complete recipe,
and compares the reviewed bytes while holding the writer lock.

Data uses the same existing-recipe selector. Exact archive and publication
resolvers pin the selected source evidence and validate the reviewed draft
before writing to the managed recipe. New recipe entry points remain under
Workflows. Terminal clipboard transport is shared with inspectors.

## External editor lifecycle

The editor and processing windows share the Ghostty launcher and graphical
display probe. Each launch uses argv, an explicit working directory, detached
standard streams, and a new process session. The Ghostty GTK arguments
disable single-instance forwarding and close the editor window when its
command exits. The child environment excludes tmux pane and socket identifiers.

A supervisor runs on the new window's PTY. It records editor readiness and
then an atomic exit receipt in the private session directory. Forge checks
both receipts and allows a bounded reconciliation period after Ghostty exits.
The worker forwards PTY interruption signals and records the editor's actual
exit status. Missing completion evidence leaves the draft available for
explicit validation and review. The Textual
driver stays active and keeps its own input, mouse reporting, and screen state.

Editable sessions are keyed by canonical target path. A session keeps the
original bytes, its private draft, and its validated candidate for explicit
review. Keep the validator and writer in the owning Forge service. Settings
use the existing snapshot and apply flow, including integrity acknowledgement
and pinned inputs. Recipe and catalogue writes compare against the original
and use existing atomic replacement. Inspection uses private copies of
authorized artifacts or structured JSON and removes them after the window
closes. The editor worker never dispatches a managed processing job.

The Pilot tests cover review, navigation, invalid drafts and conflicts.
The Linux PTY tests exercise the real Textual driver with controlled editor
processes and independent terminals. For graphical verification, use
disposable recipes and observe the editor window opening and closing under
Ghostty directly and from inside tmux. Select Wayland or X11 explicitly
when testing both backends on the same desktop.

## Historical configuration evidence

The execution registry indexes run lifecycle and links to evidence in the
existing run storage. Registered recipe loaders retain the attempt's exact TOML
before processing. Recipe metadata records the artifact path, SHA-256, source
identity and registry. Effective configuration records the validated model and
its defaults. Runtime snapshots, prepared jobs, receipts and publication
manifests remain the authority for Engine settings and resolved data identities.

Within a logical run, the attempt's `recipe-source.json` names its original
recipe and fingerprints it. The usual filenames are `recipe.toml` and
`effective-configuration.json`. File-backed Acquisition keeps its original
in `submitted.toml` under that operation's artifact directory. Pipeline keeps
`submitted.toml` and `resolved-plan.json` at the parent run root, and stage
evidence under `steps/`. Engine jobs and publication evidence are linked from
the existing preparation and execution receipts.

| Workflow | Evidence in the original implementation | Lifecycle extension |
| --- | --- | --- |
| Acquisition | Submitted TOML, request and scan records, archive selections, reports and event logs. Acquisition used its parsed configuration. | Immutable publication, attempt provenance, effective HTTP settings and overrides. |
| Normalization | Registered attempt recipe TOML and source hash, prepared batch, profile snapshots, execution intent and registration results. Prepared execution used retained inputs. | Explicit resolved recipe and runtime settings, with recipe lineage through a separately submitted batch. |
| Merge | Registered attempt recipe TOML and source hash, prepared job, publication and primary-file evidence, runtime settings and execution intent. | Extended identity and configuration metadata, with recipe lineage through a separately submitted job. |
| Conversion | Attempt and collection recipe snapshots, optional upstream copy, compatibility results, catalogue and runtime pins, per-job intents and manifests. | Effective recipe settings and recipe snapshot association for separately submitted collections. |
| Pipeline | Submitted TOML, resolved plan, stage runtime fingerprints, jobs, reports and child acquisition references. Resume verified the original and plan. | Original recipe published without replacement and exposed through the run report. |
| Import / Registration | Submitted TOML, frozen file selections, registration intents and results. Legacy Import reread the TOML when writing evidence. | Import verifies that retained bytes match the executed model. Registration commits the original and effective configuration before preflight. |

The managed path records provenance. Recipe archival is blocked when a matching
retained execution intent still points to the managed file and lacks a retained
original. Current configuration is never labeled as an older run's snapshot.


## Workflow terminal lifecycle

The workspace authorizes one CLI worker over a private Unix socket. Before
authorization, a failed graphical launch can fall back to the structured
in-app operation. After authorization, the worker uses the normal CLI workspace
locks, pinned registry, and durable run intent. The workspace observes the
retained run ID and outcome without starting a second execution.

The worker owns the Ghostty PTY and publishes its final receipt before the
terminal enters its hold state. The control socket also carries cancellation
and emergency-stop requests. Losing the workspace connection interrupts active
work. A missing final receipt is reconciled with retained run evidence.
