# Inspect and manage acquisitions

## Browse managed records

These commands read the configured raw root's registry records without contacting providers or hashing files:

```bash
marketforge storage --testing
marketforge storage list --testing --exchange bybit --instrument BTCUSDT --status complete --wide
marketforge storage coverage --testing --exchange bybit --instrument BTCUSDT --data-type trades --gaps
```

`storage` defaults to coverage. Lists include pending, downloading, complete, missing, corrupt, failed and deleted records unless filtered. `--wide` adds filenames; details/JSON expose paths and checksums. Replace `LOCAL_ID` below with a returned positive local ID, optionally prefixed `local:`:

```bash
marketforge storage show LOCAL_ID --testing
```

Filters include origin (`remote` or `external`), market/type, scope kind, data type, feed, assets, symbol glob and half-open date bounds. Browsing dates use overlap. Coverage merges nominal intervals from complete records; duplicate copies increase file/byte counts, not covered duration. With no requested bounds, gaps are only holes inside the observed envelope. With both bounds, leading/trailing gaps are included. Missing interval metadata is unknown, not zero gaps. A group must have matching local records to appear.

> **Coverage is nominal.** Zero registered gaps does not prove file integrity, parseability or complete market events. Browsing does not reverify bytes. Asset metadata can be unknown or ambiguous for historical families and options.

## Inspect runs and reports

```bash
marketforge runs list --limit 10
marketforge runs report RUN_UUID
marketforge runs show RUN_UUID
marketforge runs logs RUN_UUID --limit 50
```

Replace `RUN_UUID` with the operation's UUID. These commands read local artifacts and need no `--testing`. `show` and `report` read the same report. Logs return the first events within a 1 MiB read cap, not a live tail; limits are 1–1000. A missing/interrupted or invalid report is reported explicitly. Reports over 4 MiB cannot be read through this inspection command.

Acquisition runs retain the submitted TOML, `report.json` and append-only `events.jsonl` under the configured data root's `runs/`. Imports also retain a resolved plan. Request results distinguish discovery/cache outcomes from file outcomes. File counters distinguish queued, completed, reused, failed and cancelled work; discovery totals can remain provisional until all requests finish. Per-archive events provide local identity, status, bytes, checksum, relative path, or diagnostic. Keep reports for recovery and diagnosis; inspect them before sharing, especially external-import plans with source paths/samples.

## Cache and reuse

Completed discovery is fresh for six hours, including successful empty searches, provided its scope, covering interval and adapter version match. `--refresh` supersedes overlapping successful scans for that selection, including an authoritative empty result. Failed refreshes do not invalidate the previous completed scan. A cache result is a source interpretation, not a claim of continuous data.

Download reuse verifies size and SHA-256. Missing/corrupt files are recorded and a new acquisition can replace their availability without overwriting the old completed path. URL signatures are not archive identity. If there is no reusable local copy, expired/missing locations trigger selection-level renewal. Invisible upstream changes without revision metadata cannot be detected by local reuse.

## Verification and recovery are maintenance

The following commands do more than display records:

```bash
marketforge storage verify --testing --archive LOCAL_ID
marketforge storage reconcile --testing --archive LOCAL_ID
```

Replace `LOCAL_ID`. Verification reads bytes and can update verification time or mark missing/corrupt records. Reconciliation can recover downloading reservations from durable size/hash evidence, publish a reserved temporary file, and finalize registration. Neither contacts providers or redownloads. Unverified partials are retained by maintenance reconciliation. The `--limit` default is 100; maintenance `--status`/`--origin` are accepted by the parser but are not passed to the selection service, so do not rely on them to narrow maintenance. Use `--archive` for one record.

Remote download validation checks nonempty content, ZIP/gzip integrity and rejects HTML/error documents; it does not extract archives or validate market schemas. External imports preserve arbitrary stable bytes and do not apply the same container checks automatically.

After ordinary failure or interruption, rerun the acquisition/import recipe to reuse valid copies and recover durable reservations. Incomplete transfers without committed hash evidence restart rather than resume. Hard termination can leave no final report; registry reservations remain important. Do not manually edit reservation rows or replace files to force success.

## Cleanup is explicit

`storage prune --testing` previews failed-download retirement; `storage delete LOCAL_ID --testing` previews one managed-copy deletion. Previews can create managed preview/lock files, but do not remove archive bytes or update archive records. They are not pure filesystem-read commands.

Prune apply in an interactive terminal shows all targets and asks for confirmation. Scripted prune and every delete apply require the exact saved preview path. Review every target using `--all` or `--json` before applying. Display pagination never narrows the selected set. Delete date filters use full containment unless `--overlap` is explicit, unlike browsing overlap. Dependencies, active ownership and unsafe/changed targets can block removal. Original external source files are preserved. See [maintenance syntax](../../reference/cli/acquisition.md#cleanup-reference) before any deletion.

## Troubleshooting

| Symptom | What to check |
| --- | --- |
| TOML rejected | Unknown fields, timezone-aware dates, interval order, scope and provider matrix. Structural validity alone does not prove source support. |
| Cache-only plan unresolved | Run mapping deliberately if network/registry writes are intended; planning cannot discover new availability. |
| No archives | Check provider, native identifier, dates and scope. Gate candidate absence is not proof the product is unsupported. |
| Database access rejected | Testing URL key, explicit database name, testing identity and applied migrations; no production fallback exists. |
| HTTP/rate failure | Read request outcomes; lower pacing if needed and respect server cooldowns. Do not repeatedly refresh to bypass a cooldown. |
| Missing/corrupt file | Inspect one ID, then deliberately verify/reconcile or rerun download. Do not equate an old `complete` record with current byte validity. |
| Interrupted run | Inspect report/events and reservation state; retries can recover a durable publication without HTTP. |

See the [unified Forge workflow](forge.md) for managed builds, manually authored profiles, historical registration declarations, namespace mappings and dataset-registration recovery.
