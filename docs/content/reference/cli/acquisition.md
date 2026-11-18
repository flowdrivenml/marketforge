# Acquisition CLI reference

Use `marketforge` or its installed alias `mf`. Uppercase tokens below are placeholders: replace `NAME`/`IMPORT` with a managed configuration name, `LOCAL_ID` with a returned local archive ID, `RUN_UUID` with a returned run UUID, and `PATH` with an actual path. Examples are parser/source-verified, not executed data jobs.

## Global options

Place global options **before** the command group:

```bash
marketforge --project-root . --json acquisition plan september --testing --cache-only
```

| Option | Meaning |
| --- | --- |
| `--project-root PATH` | Explicit checkout root; otherwise workspace resolution uses the configured environment or enclosing directory. |
| `--config PATH` | Application TOML settings file, not the acquisition/import recipe. |
| `--json` | Structured result on stdout where supported; setup/validation errors are JSON on stderr. |
| `--version` | Installed package version, exits without a data operation. |
| `-h`, `--help` | Available on each command, without initializing a database operation. |

Registry commands use the [testing prerequisites](../../workflows/acquisition/index.md#prerequisites). `--testing` does not disable writes. The application takes workspace/data-root ownership for mutation-capable operations; a conflict is an error, not permission to remove another operation's lock.

## Saved acquisition recipes

Recipes resolve in `data/.config/acquisition/`; names begin with a letter or digit and otherwise accept letters, digits, hyphens and underscores, optionally ending in `.toml`, not paths.

| Syntax | Purpose and effects | Options / output |
| --- | --- | --- |
| `acquisition new NAME` | Create template exclusively; fails if file exists. No editor/network/database. | Human path/instructions or JSON creation result. |
| `acquisition edit NAME` | Open existing file and structurally validate saved content. Writes through the editor. | Editor: `presentation.editor`, then `VISUAL`, `EDITOR`, then `vi`. Human validation output; this path does not provide a JSON result. |
| `acquisition validate NAME` | Read TOML and validate structure only. | Human summary or JSON with `validation = structural`, request count and configuration. No source/availability check. |
| `acquisition list` | List managed recipe names, not acquired files. | Human names or JSON `configurations`; does not parse each recipe. |

See the [recipe schema](../../configuration/acquisition.md).

## Discovery and downloading

```text
marketforge acquisition map NAME --testing [--refresh] [--requests-per-second RATE] [--silent]
marketforge acquisition download NAME --testing [--refresh] [--requests-per-second RATE] [--silent]
```

`NAME` is required. `map` discovers archive identities/locations and writes scan/report records, without downloading archive bodies. `download` maps as necessary and downloads or verifies/reuses local copies. Both may make network requests and mutate the registry. Neither executes processing.

| Option | Default / behavior |
| --- | --- |
| `--testing` | Required; verified `marketforge_test`, no production fallback. |
| `--refresh` | Off; bypass fresh discovery and supersede matching completed overlapping scans after success. Does not force redownload of reusable bytes. |
| `--requests-per-second RATE` | Application rate if omitted; positive finite number, capped by provider budget. See [HTTP settings](../../configuration/acquisition.md#application-httpstorage-settings). |
| `--silent` | Off; suppress progress but keep final result/failures. Global JSON also suppresses progress. |

Human progress uses stderr, final summary stdout. JSON returns one result with `schema_version`, `run_id`, `status`, `file_counts`, `request_counts`, request outcomes, whether totals are final, and report/event locations. Detailed file outcomes are in append-only events rather than an invented terminal transcript. Discovery success can be empty; nominal interval observations do not certify market-event coverage.

```bash
marketforge acquisition map september --testing --refresh
marketforge --json acquisition download september --testing --requests-per-second 0.5
```

Fresh successful scans, including empty ones, are reusable for six hours; failed/partial/cancelled scans do not certify reusable discovery. Local reuse requires actual size/hash verification. See [management](../../workflows/acquisition/managing-acquisitions.md).

## Cache-only planning

```text
marketforge acquisition plan NAME --testing --cache-only [--verify-local] [--limit N]
```

Both `--testing` and `--cache-only` are required. Reads registry/cache only; no HTTP, registry writes or file changes. `--verify-local` hashes the bounded sample read-only. `--limit` defaults to 100, allowed 1–10000, across the sampled archives; it does not limit a future download.

The result reports requests (possibly unresolved), `sampled_archives`, and verification scope. Sample states include `download_required`, `registered_unverified`, `verified_reusable`, and `missing_or_corrupt`. A plan is neither a saved execution contract nor a total-byte estimate. Absent fresh discovery is unresolved, not proof no archives exist.

## External imports

Import recipes resolve in `data/.config/acquisition/import/`. No import command infers the real venue or market semantics. See the [external workflow](../../workflows/acquisition/external-data.md) and [schema](../../configuration/acquisition.md#external-import-configuration).

| Syntax | Prerequisites, effects and output |
| --- | --- |
| `acquisition import new IMPORT --from PATH` | Required existing file/directory; writes a draft TOML, refuses overwrite. Deliberately invalid `UNSPECIFIED` identity must be edited. No database/network. |
| `acquisition import inspect IMPORT` | Reads selected sources; returns bounded layout/member/sample evidence for at most 20 inputs. No database/network or registration. |
| `acquisition import validate IMPORT` | Checks configuration and frozen-input plan structure against existing local files; does not validate profile semantics or coverage. |
| `acquisition import plan IMPORT` | Returns configuration and frozen input references, stat identities and sizes. No registry/file writes or network. |
| `acquisition import run IMPORT --testing` | Copies/reuses stable source bytes into managed raw storage; registers origin/content identities and writes run plan/report/events. Testing flag required. No HTTP. |

All five accept `--silent`; final results remain. They support global JSON. Failed per-file import results produce a failed run with individual outcomes; sources remain unchanged. Source changes during hashing/copying fail rather than being treated as a stable import.

```text
# Legacy automatic format drafting is retired; use acquisition profiles validate/add.
```

`--profile` and `--input` are required. Profile names use lowercase letters, digits and hyphens (must begin with a letter, at most 101 characters). `REFERENCE` is an exact selected root-relative input, not a filesystem path outside the selection. `--member` chooses a ZIP member. Header defaults to unresolved. `--overwrite` explicitly allows replacing the draft directory's generated files, not source bytes.

Writes physical/profile TOML and diagnostics outside the active Engine catalogue. Requires existing physical templates under `data/.config/historical/formats/trades/` or `formats/depth/`; neither directory exists in the inspected checkout, so drafting is currently blocked here. It neither updates the import recipe nor validates an executable normalization mapping. Result includes unresolved fields and pending Engine validation. Global JSON is supported; this command has no `--silent` flag.

## Storage inspection

All browsing requires `--testing` even when a subparser does not mark it syntactically required. Reads use the configured raw root and a read-only database transaction. No network or content hashing occurs.

| Syntax | Behavior |
| --- | --- |
| `storage --testing [FILTERS] [--gaps]` | Default nominal coverage overview. |
| `storage coverage --testing [FILTERS] [--gaps]` | Grouped complete-record interval union; aggregates all matches before group pagination. |
| `storage list --testing [FILTERS] [--status STATUS] [--wide] [--archive LOCAL_ID]` | Paginated stored local records; `--wide` includes filenames in human view. |
| `storage show LOCAL_ID --testing` | One managed local record; plain positive bigint or `local:ID`. Missing record is an error. |

Common `FILTERS`: `--exchange binance|bybit|okx|gateio|bitget`, `--market spot|linear|inverse|option`, `--instrument-type spot|perpetual|future|option`, `--instrument SYMBOL`, `--scope-kind instrument|family|base_coin`, `--data-type trades|order_book_l2`, `--feed incremental|snapshot|unknown`, `--origin remote|external`, `--base-asset ASSET`, `--quote-asset ASSET`, `--match GLOB`, `--start DATETIME`, `--end DATETIME`, `--limit N`, `--offset N`.

Asset filters are case-insensitive exact matches; glob semantics match [markets filtering](../../workflows/markets/instrument-discovery.md#browse-without-network-access). Dates require aware timestamps and use half-open overlap. Bounds must be ordered. Limit defaults to 100 (1–10000), offset 0. `--status` choices: pending, downloading, complete, missing, corrupt, failed, deleted. Global JSON exposes full detail/pagination; list/coverage limits still apply. Nominal gaps do not certify bytes/events.

## Verification and reconciliation

```text
marketforge storage verify --testing [--archive LOCAL_ID] [--limit N]
marketforge storage reconcile --testing [--archive LOCAL_ID] [--limit N]
```

Both require a guarded database and workspace ownership. Default limit is 100 (1–10000); without ID they select recent records in the configured root. Verification hashes existing files and can update status/timestamps. Reconciliation can recover durable reservations and publish/finalize reserved copies, with unverified partials retained. No HTTP/redownload is performed. Parser options `--status` and `--origin` are currently not forwarded to maintenance selection; they must not be relied on for targeting.

Results include per-record verification/action/error. Missing, corrupt, unverifiable or failed outcomes can return exit 1. Prefer one ID when diagnosing a specific acquisition. Neither is a read-only substitute for browsing.

## Cleanup reference

```text
marketforge storage prune --testing [FILTERS] [--archive LOCAL_ID] [--status STATUS] [--all]
marketforge storage delete [LOCAL_ID] --testing [FILTERS] [--status STATUS] [--overlap] [--all]
marketforge storage prune --testing --apply --preview PATH
marketforge storage delete --testing --apply --preview PATH
```

Prune defaults to failed attempts. Delete requires a specific meaningful selector: unscoped/status-only bulk deletion is rejected. `--all` means display/review every target, not select everything. Previews can create managed preview/lock files but do not remove archive bytes or mutate archive rows. Filters combine with AND; limits/offset affect display, never target selection.

Delete dates require full containment by default; `--overlap` changes that and requires a date bound. Unknown intervals cannot qualify through date filters. Prune dates retain browsing-overlap behavior.

`PATH` must be the **exact managed preview path returned by a fully reviewed preview**, not a guessed filename. Scripts never prompt with `--apply --preview`. Delete always requires a saved preview. Interactive `prune --testing --apply` is the one immediate-confirmation path: it displays all targets, then asks once, default no. Noninteractive immediate prune cannot apply eligible mutations.

Apply revalidates the frozen target set and blockers. Eligible prune retires failed records and removes positively owned partials if present; delete removes eligible managed final copies. Both retain registry lineage and original external sources. Concrete dependencies, active operations, unsafe/changed paths or files can block work. Receipts support retry after interrupted registration; retain preview/receipt files while recovery is pending. Non-complete apply returns 1; successful/no-op preview returns 0. These commands are not needed for normal inspection or reuse.

## Run inspection

```text
marketforge runs list [--limit N]
marketforge runs show RUN_UUID [--limit N]
marketforge runs report RUN_UUID [--limit N]
marketforge runs logs RUN_UUID [--limit N]
```

No database/network or `--testing`. Default limit 100; list/log bounds 1–1000. `show` and `report` read the same report and ignore the accepted limit. Logs read first events, capped at 1 MiB; report inspection caps at 4 MiB. Missing/interrupted reports and truncated logs are explicit. Global JSON returns the corresponding document.

## Exit behavior

| Code | Meaning in this command surface |
| --- | --- |
| 0 | Successful operation/inspection, including successful empty discovery. Check detailed completeness separately. |
| 1 | Acquisition/import item failures, unsuccessful verification/recovery, or partial maintenance apply. Already completed files are not rolled back. |
| 2 | Parser, configuration, filesystem, guarded database, setup or ownership failure handled by the CLI. |
| 130 | Interruption handled by the CLI; a hard kill cannot promise a final report. |

Global JSON setup errors go to stderr with an error message; operation reports can describe individual failures in stdout results. Human editor commands retain their editor/human output contract. Use command `--help` for parser-level inspection without triggering operational work.

See the [unified Forge workflow](forge.md) for managed builds, manually authored profiles, historical registration declarations, namespace mappings and dataset-registration recovery.
