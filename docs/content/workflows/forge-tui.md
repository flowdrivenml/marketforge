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
| Overview | Data inventory, workspace status, current operation, attention, and recent results |
| Workflows | Pipeline, acquisition, normalization, merge, conversion, and import |
| Data | Merged, Converted, Normalized, and Raw |
| Markets | Instruments, observations, policies, and ranking snapshots |
| Catalog | Manual exchange, instrument, and specification declarations |
| Recipes & Profiles | Saved TOML recipes, declarations, and installed normalization profiles |
| Runs | Retained reports and operations owned by this application |
| System | Engine installations, the testing registry, and application settings |

Press Ctrl+P to find actions. Registration commands remain in the palette,
including raw registration and supported recovery. Workflows has no separate
Registration tab.

Arrow keys change selection. Enter opens the selected object. For an editable
recipe, Enter or a click on its name opens the configured editor. The table
uses the full width. Escape returns from explicit details while retaining the
list state. Right-click a row, use **F2**, or press Shift+F10 to open its
actions. **F3** in details opens raw evidence. Tab and Shift+Tab move focus.
Use `/` for search, Ctrl+R to reload, Page Up and Page Down to change pages,
and Ctrl+N to toggle navigation.

Use **Filters** for structured constraints and **Reset** to clear them. Click
a supported heading or choose a sort in the filter form. The arrow indicates
direction, and a second click reverses it. Registry sorting and filtering
happen before pagination. Turnover ordering keeps native denominations
separate. Missing numeric and time values sort last. The result strip displays
the current page and applied criteria.

| Inventory | Structured filters |
| --- | --- |
| Merged and Normalized | Publication ID, exchange, instrument identity, instrument type, market, trade or depth content, observed interval |
| Raw | Archive ID, origin and raw source, exchange, type and market, instrument or scope, feed, assets, status, and interval |
| Markets instruments | Exchange and market lists, exact symbol and symbol pattern, base, quote and settlement, instrument type, status, observation availability |
| Ranking snapshots | Comparison currency and complete or partial universe, with policy and snapshot search |
| Converted | Manifest availability and recorded stream kinds |
| Catalog instruments | Exchange, symbol, type, market, assets, status |
| Retained runs | Run ID, operation, recipe, recorded state, exchange, scope, started interval, minimum duration, recorded outputs |

Publication member constraints match the same constituent. Retained run
context comes from retained reports and plans. Older records can lack scope
or duration. A recorded running state has unconfirmed liveness.

Forms include selectors and optional Advanced fields. Copy command displays
the equivalent quoted CLI invocation for use outside the app. Copy value
requests clipboard transfer through the terminal. If terminal or tmux clipboard
forwarding is disabled, the displayed text remains selectable.

## Overview

Overview puts registered data ahead of run history. Its headline statistics
cover completed primary files of registered normalized and merged publications.
Instrument identities include their canonical namespace and venue. Logical stream
IDs retain feed distinctions and are deduplicated across stages. Publication IDs
and primary files are counted separately. Recorded primary-file size excludes
sidecars and shows how many file sizes are missing. It is distinct from free
filesystem capacity. **F2 → Inventory evidence** shows the exact recorded
bytes, the check time, and missing size or stream evidence counts.

The stage table counts raw archives recorded complete for this workspace's raw
root, registered normalizations and merges, and known local conversion outputs
from the latest eight converter execution receipts. Conversion receipts can
establish committed outputs and physical rows. They do not provide recorded
file sizes or an exhaustive inventory. Overview does not reverify files.
Physical rows describe stored representations, which can overlap.

Select a stage or market with Enter. The market list shows trade and depth
membership and the latest recorded observed event time in UTC. Presence does
not establish continuous interval coverage or current market freshness. Use
**Markets** in the footer to page through the complete matching registry
inventory. Each market opens publications for its exact canonical identity.
Recent runs are limited to three on narrow screens and five otherwise. F2
opens the full Runs area.

The status strip shows the observed testing registry connection, verified
managed installations, and filesystem free space. Use **F2 → Workspace status
details** for each source’s check time. Ctrl+R refreshes these read-only checks
as well as inventory and recent history. Returning to Overview reloads inventory and history and rechecks expired
observations. Registry outages leave local history and owned operations usable.

The current operation shows the latest reported phase and any measured work.
The completion summary stays until dismissed through F2 or replaced by another
operation. A requested validation, preparation, or stop-after boundary can
succeed without producing a publication. Results show publishing and registration
evidence separately. Recent run times are shown in UTC. A recorded running state
in retained history has unconfirmed liveness.

Use Enter to inspect the selected run and F2 to open its report, outputs, or
available recovery actions. Attention highlights current blockers and interrupted
pipelines with retained evidence. Resume opens the exact pipeline run, and
registration recovery opens the retained execution intent. These actions use the
existing review and verification flows.

## Pipeline and standalone workflows

Choose a saved pipeline recipe under Workflows. Validate checks its structure.
Plan reads retained evidence in the testing registry. Run begins acquisition
and continues through publication and registration. Choose **Stop after**
`normalize.prepare` to inspect validated jobs before execution. Open the run
report and use Pipeline Resume to continue with its retained inputs, ranking,
settings, and jobs. Pipeline Resume and Status select retained run IDs.

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

## Author recipes

**New recipe** uses the workflow tab's creator and opens the new source in the
configured editor. The pipeline creator also supports `mf pipeline new NAME`.
Review draft fields before planning or running. A successful editor exit
triggers a reload and local validation. Removing or invalidating the source
produces an error. Retained submissions remain immutable.

From a Markets or Catalog instrument, choose **Add to acquisition…** or
**Add to pipeline…**. Select an existing recipe or supply a name and explicit
UTC interval for a new one, then choose trades, depth, or both. The review
identifies the exact file and interval. Apply writes the supported TOML
requests while preserving unrelated settings and comments. An exact duplicate
is a no-op. Forge coordinates its writers and editor sessions, and changes
observed after review require a reload. Pipeline requests are embedded in the
selected pipeline TOML.

Acquisition recipes can seed normalization and conversion. Normalization
recipes can seed merge or conversion, or replace the single source of a
compatible derived merge that has no pinned publication IDs. Merge recipes
can seed conversion. Conversion source routes still resolve committed merge
inputs during preparation. Creating or editing a recipe does not execute it.

## Data and maintenance

Merged and Normalized each list registered publications once. It retains the
engine publication ID, registry publication ID, full published stream
membership, and a separate page of primary-file identities. Open registered
files to inspect lineage, schema, or a bounded Parquet sample. The publication
view provides publication lineage and complete stream membership. File
verification checks the named primary file and shared manifest checksum.
Its result explicitly identifies the scope and whether the whole publication
was verified.
Execution and verification evidence retain original timestamps and units.
Observed event endpoints are inclusive and do not establish continuity for
the requested half-open interval.

Converted browses known commits from the newest 100 converter execution
receipts. Forge reads metadata from their known managed manifests. Missing or
invalid manifests remain identifiable from the retained receipt. This source
does not enumerate every local conversion and does not infer registry
registration. The Overview keeps its separate shorter evidence window.

Raw uses local archive registry identities. Reloading reads retained
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
are unavailable in Forge. An open operation gains report, output, and recovery
actions as evidence becomes available. When the operation finishes while you
browse elsewhere, Forge shows a notification and keeps your current view.

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
