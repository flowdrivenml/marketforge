# Historical pipeline

A single Forge recipe acquires and registers raw archives, prepares and runs
normalization, then publishes and registers a merged dataset. Conversion is a
separate workflow.

## Recipe

Save named recipes under `data/.config/workflows/data_engineering/pipeline/`. The version 1 recipe uses
the acquisition request schema and one half-open UTC interval `[start, end)`.
Use the existing scopes and feed options for each exchange. Forge resolves
archive, instrument, job, dataset, and registry identities.

The example is checked in at `data/.config/workflows/data_engineering/pipeline/bybit_spot.toml`. To
inspect or edit it, run `nvim data/.config/workflows/data_engineering/pipeline/bybit_spot.toml`.

Use the [maintained sample recipe](https://github.com/flowdrivenml/marketforge/blob/main/data/.config/workflows/data_engineering/pipeline/bybit_spot.toml) as the complete example. See the [recipe reference](../configuration/pipeline.md) for field definitions.

`[normalize]` and `[merge]` are optional. Normalization defaults to turnover
ranking. Merge accepts `causal` and `chronological`. When omitted, the existing
merge planner picks the ordering required by the selected inputs. It derives
trade, depth, or mixed processing from registered files.

## Inspect and run

Use a configured `marketforge_test` database with the testing identity and
current migrations. Install the managed normalize and merge engines and use the
operation runtime files, pipeline runtime and normalization profiles. The catalogue must supply instrument
identity and applicable historical specifications for the requested archives.
Forge reads its ordinary application and HTTP configuration.

```bash
set -o pipefail
mf engine build normalize
mf engine build merge
mf pipeline validate bybit_spot
mf pipeline plan bybit_spot --testing
```

Validation checks recipe structure and options. Planning reads existing
registered evidence and identifies deferred work. It issues no HTTP requests,
allocates no output identities, and writes no recipe, job, or registry rows.
Local archive readiness is confirmed on run.

```bash
mf pipeline run bybit_spot --testing --tui
```

Forge runs processing stages sequentially to preserve their configured engine
budgets. Acquisition uses the usual HTTP concurrency and source rate limits.
Preparation and validation are distinct phases. A processing stage completes
only once the required outputs are verified and registered. This can come from a new execution or [verified reuse](../reference/cli/merge.md#completed-work-reuse). Archive discovery, nominal archive intervals, verified
local files, and observed event extents describe different evidence.

Acquisition passes its exact verified local archive IDs, paths, checksums, and
source revision evidence to normalization. Normalization keeps its existing
coverage and readiness checks, and uses only those inputs. After normalization
registers every required publication, merge takes the exact registered
primary-file dataset IDs in that receipt. Engine job and publication IDs stay
distinct from registry publication and primary-file IDs.

## Inspect a preparation and resume

```bash
mf pipeline run bybit_spot --testing --stop-after normalize.prepare --tui
```

This stops with validated normalization jobs before execution. The command
prints the retained run ID and the resume command.

```bash
set -o pipefail
mf pipeline status RUN_ID
mf --json pipeline status RUN_ID
```

```bash
mf pipeline resume RUN_ID --testing --tui
```

Resume uses the original workspace, storage root and testing registry. It
checks retained TOML, settings, profile and executable hashes, prepared jobs,
and acquisition evidence. Retained jobs preserve the ranking and stream
ordering selected during preparation. Completed acquisition scans and verified
archives from an interrupted acquisition are retained. A request whose
discovery never finished may still need discovery. Changed or missing pinned
inputs require investigation. A new selection belongs to a fresh run.

If processing committed a valid publication but registration failed, resume
verifies that publication and retries registration through the existing
registration service. It keeps failed attempts and any published outputs. A
processing retry allocates fresh output identities. A fresh pipeline run also
allocates fresh outputs and may reuse verified raw archives.

## Progress and reports

The Rich dashboard owns the terminal for the entire run. Ctrl+C requests
cancellation. Forge signals and joins the engine, retains diagnostic logs and
writes an interrupted report. Redirected output uses a plain summary.
Acquisition exposes typed request and file counts, with provisional discovery
totals identified during discovery. Processing exposes stage and job lifecycle
counts. It has no live row throughput, ETA, or partition counters from Rust.

Run artifacts live under `<storage>/runs/<run-id>/`. Start with `report.json`
and `events.jsonl`. The run also retains `submitted.toml`,
`resolved-plan.json`, frozen engines, and historical settings and profiles.
`steps/` links execution attempts and stores the exact acquisition selection.
The report points to the existing child preparation, execution, verification,
and registration receipts. Each engine attempt retains separate diagnostic
logs and its structured result when available.

Read the reported child error before retrying. Build missing managed engines,
verify access to the testing registry, and supply missing catalogue or profile
evidence as indicated. Preserve existing outputs and staging when inspecting
interrupted attempts. Use a fresh run when the recipe, data selection, or
interpretation needs to change.
