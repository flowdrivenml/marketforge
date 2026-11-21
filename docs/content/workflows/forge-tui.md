# Interactive Forge workspace

Forge provides a Textual application alongside the direct CLI. Install Forge
with Python 3.12 or newer on Linux or macOS. Textual is included in the Forge
lockfile. Use an interactive terminal and the verified `marketforge_test`
registry for registry operations. The application resolves the same project
root and application settings as the CLI.

```bash
mf tui --testing
```

A separate workspace or settings file can be selected with the global options.

```bash
mf --project-root /path/to/marketforge --config /path/to/settings.toml tui --testing
```

Startup reads local workspace evidence. Network acquisition, market refresh,
processing, verification, and storage mutation are explicit actions.

## Navigation

| Area | Contents |
| --- | --- |
| Overview | Workspace paths, retained activity, and diagnostics |
| Workflows | Pipeline, acquisition, normalization, merge, conversion, import, and raw registration |
| Data | Registry publications, registered primary files, and raw archives |
| Markets | Instruments, observations, policies, and ranking snapshots |
| Catalog | Manual exchange, instrument, and specification declarations |
| Recipes & Profiles | Saved TOML recipes, declarations, and installed normalization profiles |
| Runs | Retained reports and operations owned by this application |
| System | Engine installations, the testing registry, and application settings |

Press Ctrl+P to find actions such as `pipeline resume`, `merge prepare`,
`storage coverage`, or `dataset lineage`. Use the arrow keys to select rows
and Enter to open details. Tab and Shift+Tab move focus. Escape closes forms
and details. Use `/` to focus the filter, Ctrl+R to reload retained data,
Page Up and Page Down to move between available result pages, and Ctrl+N
to toggle navigation. The application also supports mouse selection and
scrolling. Data tables scroll horizontally and detail views retain full
values.

Forms include selectors and optional Advanced fields. Copy command displays
the equivalent quoted CLI invocation for use outside the app. Copy value
requests clipboard transfer through the terminal. If terminal or tmux clipboard
forwarding is disabled, the displayed text remains selectable.

## Pipeline and standalone workflows

Choose a saved pipeline recipe under Workflows. Validate checks its structure.
Plan reads retained evidence in the testing registry. Run begins acquisition
and continues through publication and registration. Choose **Stop after**
`normalize.prepare` to inspect validated jobs before execution. Open the run
report and use Pipeline Resume to continue with its retained inputs, ranking,
settings, and jobs.

Standalone workflows accept the same saved recipes and applicable submitted
paths as their CLI counterparts. Preparation, execution, verification,
registration, and recovery have separate service outcomes. The converter
reports physical publication and verification. Conversion registration
remains unavailable.

Editable source TOML can be opened with **Edit TOML**. Forge uses the configured
editor or nvim, suspends terminal drawing for the editor, then restores the
workspace and revalidates the saved source. Operation supervision continues
during the edit. Retained submissions remain read-only evidence.

To edit a recipe directly from a shell, use nvim.

```bash
nvim data/.config/pipeline/bybit_spot.toml
```

## Data and maintenance

The Publications view lists a registry publication once. It retains the
engine publication ID, registry publication ID, full published stream
membership, and a separate page of primary-file identities. Open registered
files to inspect coverage, lineage, schema, or a bounded Parquet sample.
Execution and verification evidence retain original timestamps and units.
Observed event endpoints are inclusive and do not establish continuity for
the requested half-open interval.

Raw Storage uses local archive registry identities. Reloading reads retained
metadata. Archive verification and reconciliation can update registry state,
and dataset verification hashes the requested file and shared manifest.
These actions run only when selected.

Review and apply storage cleanup using the saved preview. Forge binds apply
to the displayed preview and revalidates targets before deleting anything.
Ranking deletion also checks reviewed snapshot identities. Database restore
accepts a verified backup and an explicitly named fresh disposable testing
database. Existing storage and database safeguards remain in force.

## Active work and shutdown

Ctrl+O opens the owned operation and its current activity. Pipeline and
acquisition progress comes from their typed snapshots. Processing shows
phases and retained results. Inner-engine throughput, ETA, and row progress
are unavailable in Forge.

Changing screens leaves owned work running. Workspace ownership lasts for
the mutation, including child and reader cleanup. Ctrl+C opens interruption
confirmation. Ctrl+Q with work active offers Back to keep the application
open, or cancellation and graceful exit.

Retained external reports show the last recorded state. They carry no
promise of process liveness, and Forge only cancels operations that it owns.
Pipeline Resume and normalization or merge registration recovery use the
existing service paths. For work that should survive leaving a terminal,
launch Forge inside tmux and detach from the session.

See the [CLI and TUI action inventory](../development/forge-tui.md) for action
classes, service boundaries, and verification coverage.
