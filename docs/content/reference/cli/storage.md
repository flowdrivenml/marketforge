# Raw storage commands

Storage archives and nominal coverage groups default to descending start. See [sort fields and aliases](sorting.md) for both views.

Browse, verify, reconcile and clean up managed raw copies. [Workflow and prerequisites](../../workflows/acquisition/managing-acquisitions.md) · [Shared CLI behavior](index.md)


## Synopsis and arguments

### `mf storage coverage`

```text
mf storage coverage [-h] [--testing | --production] [--base-asset BASE_ASSET]
                                    [--quote-asset QUOTE_ASSET]
                                    [--match MATCH] [--exchange EXCHANGE]
                                    [--market {spot,linear,inverse,option,equity}]
                                    [--instrument-type {spot,perpetual,future,option,equity}]
                                    [--instrument INSTRUMENT]
                                    [--scope-kind {instrument,family,base_coin}]
                                    [--data-type {trades,order_book_l2}]
                                    [--feed {incremental,snapshot,unknown}]
                                    [--origin {remote,external}]
                                    [--start START] [--end END]
                                    [--limit LIMIT] [--offset OFFSET] [--gaps]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--base-asset` | Default `==SUPPRESS==`.  Exact base asset, case-insensitive, e.g. ETH. |
| `--quote-asset` | Default `==SUPPRESS==`.  Exact instrument quote asset, case-insensitive; independent of ranking comparison currency. |
| `--match` | Default `==SUPPRESS==`.  Case-insensitive glob over the full native symbol: * matches zero or more characters, ? one. Quote it in the shell: --match 'ETH*' or --match '*BTC'. Percent and underscore are literal. |
| `--exchange` | Default `==SUPPRESS==`.  Exact registered execution venue code. |
| `--market` | Default `==SUPPRESS==`.  Accepted `spot`, `linear`, `inverse`, `option`, `equity`. |
| `--instrument-type` | Default `==SUPPRESS==`.  Accepted `spot`, `perpetual`, `future`, `option`, `equity`. |
| `--instrument` | Default `==SUPPRESS==`.  Exact registered native instrument/family/scope identifier. |
| `--scope-kind` | Default `==SUPPRESS==`.  Accepted `instrument`, `family`, `base_coin`. |
| `--data-type` | Default `==SUPPRESS==`.  Accepted `trades`, `order_book_l2`. |
| `--feed` | Default `==SUPPRESS==`.  Accepted `incremental`, `snapshot`, `unknown`. Registered feed; unknown selects missing feed identities. |
| `--origin` | Default `==SUPPRESS==`.  Accepted `remote`, `external`. |
| `--start` | Default `==SUPPRESS==`.  Timezone-aware inclusive start, e.g. 2026-10-27T00:00:00Z; interval overlap. |
| `--end` | Default `==SUPPRESS==`.  Timezone-aware exclusive end; interval overlap. |
| `--limit` | Default `==SUPPRESS==`.  Integer. Rows/groups displayed, 1..10000 (default 100); coverage aggregates all matching files. |
| `--offset` | Default `==SUPPRESS==`.  Integer. Zero-based result offset (default 0). |
| `--gaps` | Optional. Show missing half-open UTC intervals. |

### `mf storage list`

```text
mf storage list [-h] [--testing | --production] [--base-asset BASE_ASSET]
                                [--quote-asset QUOTE_ASSET] [--match MATCH]
                                [--exchange EXCHANGE]
                                [--market {spot,linear,inverse,option,equity}]
                                [--instrument-type {spot,perpetual,future,option,equity}]
                                [--instrument INSTRUMENT]
                                [--scope-kind {instrument,family,base_coin}]
                                [--data-type {trades,order_book_l2}]
                                [--feed {incremental,snapshot,unknown}]
                                [--origin {remote,external}] [--start START]
                                [--end END] [--limit LIMIT] [--offset OFFSET]
                                [--status {pending,downloading,complete,missing,corrupt,failed,deleted}]
                                [--wide] [--archive ARCHIVE_REFERENCE]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--base-asset` | Default `==SUPPRESS==`.  Exact base asset, case-insensitive, e.g. ETH. |
| `--quote-asset` | Default `==SUPPRESS==`.  Exact instrument quote asset, case-insensitive; independent of ranking comparison currency. |
| `--match` | Default `==SUPPRESS==`.  Case-insensitive glob over the full native symbol: * matches zero or more characters, ? one. Quote it in the shell: --match 'ETH*' or --match '*BTC'. Percent and underscore are literal. |
| `--exchange` | Default `==SUPPRESS==`.  Exact registered execution venue code. |
| `--market` | Default `==SUPPRESS==`.  Accepted `spot`, `linear`, `inverse`, `option`, `equity`. |
| `--instrument-type` | Default `==SUPPRESS==`.  Accepted `spot`, `perpetual`, `future`, `option`, `equity`. |
| `--instrument` | Default `==SUPPRESS==`.  Exact registered native instrument/family/scope identifier. |
| `--scope-kind` | Default `==SUPPRESS==`.  Accepted `instrument`, `family`, `base_coin`. |
| `--data-type` | Default `==SUPPRESS==`.  Accepted `trades`, `order_book_l2`. |
| `--feed` | Default `==SUPPRESS==`.  Accepted `incremental`, `snapshot`, `unknown`. Registered feed; unknown selects missing feed identities. |
| `--origin` | Default `==SUPPRESS==`.  Accepted `remote`, `external`. |
| `--start` | Default `==SUPPRESS==`.  Timezone-aware inclusive start, e.g. 2026-10-27T00:00:00Z; interval overlap. |
| `--end` | Default `==SUPPRESS==`.  Timezone-aware exclusive end; interval overlap. |
| `--limit` | Default `==SUPPRESS==`.  Integer. Rows/groups displayed, 1..10000 (default 100); coverage aggregates all matching files. |
| `--offset` | Default `==SUPPRESS==`.  Integer. Zero-based result offset (default 0). |
| `--status` | Default `==SUPPRESS==`.  Accepted `pending`, `downloading`, `complete`, `missing`, `corrupt`, `failed`, `deleted`. |
| `--wide` | Optional. Include registered filenames; full paths/checksums remain in show and JSON. |
| `--archive` | Default `==SUPPRESS==`. |

### `mf storage show`

```text
mf storage show [-h] [--testing | --production] archive_reference
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `archive_reference` | Required. Local registry ID: local:536 or 536. |

### `mf storage verify`

```text
mf storage verify [-h] [--testing | --production] [--archive ARCHIVE_REFERENCE]
                                  [--limit LIMIT] [--status STATUS]
                                  [--origin {remote,external}]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--archive` | Default `==SUPPRESS==`. |
| `--limit` | Default `100`.  Integer. |
| `--status` | Default `==SUPPRESS==`. |
| `--origin` | Default `==SUPPRESS==`.  Accepted `remote`, `external`. |

### `mf storage reconcile`

```text
mf storage reconcile [-h] [--testing | --production]
                                     [--archive ARCHIVE_REFERENCE]
                                     [--limit LIMIT] [--status STATUS]
                                     [--origin {remote,external}]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--archive` | Default `==SUPPRESS==`. |
| `--limit` | Default `100`.  Integer. |
| `--status` | Default `==SUPPRESS==`. |
| `--origin` | Default `==SUPPRESS==`.  Accepted `remote`, `external`. |

### `mf storage prune`

```text
mf storage prune [-h] [--testing | --production] [--base-asset BASE_ASSET]
                                 [--quote-asset QUOTE_ASSET] [--match MATCH]
                                 [--exchange EXCHANGE]
                                 [--market {spot,linear,inverse,option,equity}]
                                 [--instrument-type {spot,perpetual,future,option,equity}]
                                 [--instrument INSTRUMENT]
                                 [--scope-kind {instrument,family,base_coin}]
                                 [--data-type {trades,order_book_l2}]
                                 [--feed {incremental,snapshot,unknown}]
                                 [--origin {remote,external}] [--start START]
                                 [--end END] [--limit LIMIT] [--offset OFFSET]
                                 [--status {pending,downloading,complete,missing,corrupt,failed,deleted}]
                                 [--apply] [--preview PREVIEW] [--all]
                                 [--archive ARCHIVE_REFERENCE]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--base-asset` | Default `==SUPPRESS==`.  Exact base asset, case-insensitive, e.g. ETH. |
| `--quote-asset` | Default `==SUPPRESS==`.  Exact instrument quote asset, case-insensitive; independent of ranking comparison currency. |
| `--match` | Default `==SUPPRESS==`.  Case-insensitive glob over the full native symbol: * matches zero or more characters, ? one. Quote it in the shell: --match 'ETH*' or --match '*BTC'. Percent and underscore are literal. |
| `--exchange` | Default `==SUPPRESS==`.  Exact registered execution venue code. |
| `--market` | Default `==SUPPRESS==`.  Accepted `spot`, `linear`, `inverse`, `option`, `equity`. |
| `--instrument-type` | Default `==SUPPRESS==`.  Accepted `spot`, `perpetual`, `future`, `option`, `equity`. |
| `--instrument` | Default `==SUPPRESS==`.  Exact registered native instrument/family/scope identifier. |
| `--scope-kind` | Default `==SUPPRESS==`.  Accepted `instrument`, `family`, `base_coin`. |
| `--data-type` | Default `==SUPPRESS==`.  Accepted `trades`, `order_book_l2`. |
| `--feed` | Default `==SUPPRESS==`.  Accepted `incremental`, `snapshot`, `unknown`. Registered feed; unknown selects missing feed identities. |
| `--origin` | Default `==SUPPRESS==`.  Accepted `remote`, `external`. |
| `--start` | Default `==SUPPRESS==`.  Timezone-aware inclusive start, e.g. 2026-10-27T00:00:00Z; interval overlap. |
| `--end` | Default `==SUPPRESS==`.  Timezone-aware exclusive end; interval overlap. |
| `--limit` | Default `==SUPPRESS==`.  Integer. Preview rows displayed only; never narrows target selection. |
| `--offset` | Default `==SUPPRESS==`.  Integer. Zero-based result offset (default 0). |
| `--status` | Default `==SUPPRESS==`.  Accepted `pending`, `downloading`, `complete`, `missing`, `corrupt`, `failed`, `deleted`. Failed attempts by default; active/published attempts stay blocked. |
| `--apply` | Optional. Prune: preview and confirm once in a terminal. Scripts/delete: apply the exact saved --preview file. |
| `--preview` | Default `==SUPPRESS==`.  Saved managed preview for scripted --apply; delete always requires it. |
| `--all` | Optional. Show and review every selected target. Pagination never narrows target selection. |
| `--archive` | Default `==SUPPRESS==`. |

### `mf storage delete`

```text
mf storage delete [-h] [--testing | --production] [--base-asset BASE_ASSET]
                                  [--quote-asset QUOTE_ASSET] [--match MATCH]
                                  [--exchange EXCHANGE]
                                  [--market {spot,linear,inverse,option,equity}]
                                  [--instrument-type {spot,perpetual,future,option,equity}]
                                  [--instrument INSTRUMENT]
                                  [--scope-kind {instrument,family,base_coin}]
                                  [--data-type {trades,order_book_l2}]
                                  [--feed {incremental,snapshot,unknown}]
                                  [--origin {remote,external}] [--start START]
                                  [--end END] [--limit LIMIT]
                                  [--offset OFFSET]
                                  [--status {pending,downloading,complete,missing,corrupt,failed,deleted}]
                                  [--apply] [--preview PREVIEW] [--all]
                                  [--overlap]
                                  [archive_reference]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--base-asset` | Default `==SUPPRESS==`.  Exact base asset, case-insensitive, e.g. ETH. |
| `--quote-asset` | Default `==SUPPRESS==`.  Exact instrument quote asset, case-insensitive; independent of ranking comparison currency. |
| `--match` | Default `==SUPPRESS==`.  Case-insensitive glob over the full native symbol: * matches zero or more characters, ? one. Quote it in the shell: --match 'ETH*' or --match '*BTC'. Percent and underscore are literal. |
| `--exchange` | Default `==SUPPRESS==`.  Exact registered execution venue code. |
| `--market` | Default `==SUPPRESS==`.  Accepted `spot`, `linear`, `inverse`, `option`, `equity`. |
| `--instrument-type` | Default `==SUPPRESS==`.  Accepted `spot`, `perpetual`, `future`, `option`, `equity`. |
| `--instrument` | Default `==SUPPRESS==`.  Exact registered native instrument/family/scope identifier. |
| `--scope-kind` | Default `==SUPPRESS==`.  Accepted `instrument`, `family`, `base_coin`. |
| `--data-type` | Default `==SUPPRESS==`.  Accepted `trades`, `order_book_l2`. |
| `--feed` | Default `==SUPPRESS==`.  Accepted `incremental`, `snapshot`, `unknown`. Registered feed; unknown selects missing feed identities. |
| `--origin` | Default `==SUPPRESS==`.  Accepted `remote`, `external`. |
| `--start` | Default `==SUPPRESS==`.  Timezone-aware inclusive start, e.g. 2026-10-27T00:00:00Z; archive start must be at or after this bound unless --overlap. |
| `--end` | Default `==SUPPRESS==`.  Timezone-aware exclusive end; archive end must be at or before this bound unless --overlap. |
| `--limit` | Default `==SUPPRESS==`.  Integer. Preview rows displayed only; never narrows target selection. |
| `--offset` | Default `==SUPPRESS==`.  Integer. Zero-based result offset (default 0). |
| `--status` | Default `==SUPPRESS==`.  Accepted `pending`, `downloading`, `complete`, `missing`, `corrupt`, `failed`, `deleted`. Optional status filter; not sufficient as a bulk selector. |
| `--apply` | Optional. Prune: preview and confirm once in a terminal. Scripts/delete: apply the exact saved --preview file. |
| `--preview` | Default `==SUPPRESS==`.  Saved managed preview for scripted --apply; delete always requires it. |
| `--all` | Optional. Show and review every selected target. Pagination never narrows target selection. |
| `archive_reference` | Optional. Single managed local:ID; other filters combine with AND. |
| `--overlap` | Optional. Use half-open overlap instead of containment; requires a date bound. |

## Check Historical Coverage

One of the most useful storage features is the ability to inspect which historical periods are represented by your downloaded files.

Imagine you want to research BTCUSDT trading activity over an entire month.

You have downloaded many archives, but you are not sure whether every day is represented.

You could manually compare filenames.

However, this becomes difficult when working with several exchanges, different archive intervals, and multiple data types.

MarketForge provides **coverage inspection** to help with this.

### View coverage for an instrument

Use:

```bash
mf storage coverage \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --data-type trades
```

MarketForge examines the registered historical intervals of the matching completed files.

It combines overlapping or adjacent intervals to summarize the periods represented by those files.

For example, suppose the registry contains:

```text
October 25    Complete
October 26    Complete
October 27    Complete
```

The combined nominal coverage may be represented as:

```text
October 25 ───────────────── October 28
```

The end boundary is exclusive.

If the same period is represented by more than one matching copy, overlapping intervals do not create extra covered time.

### Find missing periods

Suppose one day is missing:

```text
October 25    Complete
October 26    Missing
October 27    Complete
```

MarketForge can identify the gap between the registered intervals.

Use:

```bash
mf storage coverage \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --data-type trades \
    --gaps
```

The `--gaps` option includes missing periods in the coverage result.

This helps you identify dates that may need another acquisition.

### Check a specific date range

You can restrict the coverage inspection to a historical interval.

For example:

```bash
mf storage coverage \
    --testing \
    --exchange bybit \
    --instrument BTCUSDT \
    --data-type trades \
    --start 2026-10-01T00:00:00Z \
    --end 2026-11-01T00:00:00Z \
    --gaps
```

This requests a coverage view for October 2026.

When both date boundaries are provided, the gap calculation can include missing periods at the beginning or end of the requested interval.

Without explicit boundaries, MarketForge can only report gaps within the observed coverage envelope.

For example, if your earliest recorded archive starts on October 10, MarketForge cannot assume that October 1–9 was part of your intended collection unless you specify that period.

### Understanding nominal coverage

**An important limitation: archive coverage is not the same as complete market-data coverage.**

Suppose an archive is registered as covering October 27.

This means the archive's recorded period represents that day.

It does not prove that every trade or order-book update from that day is present.

A file might:

- Contain missing events.
- Have gaps in its source sequence.
- Start later than its declared interval.
- End before its declared interval.
- Contain invalid records.
- Lack the snapshots required for continuous order-book reconstruction.

Storage coverage does not inspect these details.

Those questions belong to source processing and integrity analysis.

Similarly, missing or unknown interval information must not be interpreted as proof of complete coverage.

### Coverage and completeness

Keep these three ideas separate:

| Question | Where to check |
|---|---|
| Do I have registered archives for this period? | `mf storage coverage` |
| Are the local file bytes still intact? | `mf storage verify` |
| Does the data contain valid, complete market events? | Normalization and relevant integrity evidence |

A collection can have no nominal coverage gaps while still containing incomplete market data.

---

## Understand File Statuses

Every managed archive has a recorded status.

These statuses describe the current known state of the local record.

Some common statuses are:

| Status | Meaning |
|---|---|
| `pending` | The file has been selected or reserved for acquisition. |
| `downloading` | A download or managed-copy operation is in progress or has an unfinished reservation. |
| `complete` | The managed copy was accepted as complete. |
| `missing` | A previously expected local file could not be found during an applicable check. |
| `corrupt` | A file failed an applicable integrity check. |
| `failed` | The acquisition or managed-copy attempt failed. |
| `deleted` | The managed copy has been removed through the supported lifecycle. |

A recorded `complete` status is useful, but it should not be confused with a fresh verification of the file.

If a file was accepted yesterday and deleted manually today, its registry status may remain unchanged until an operation checks the actual file.

### Filter by status

To list completed archives:

```bash
mf storage list \
    --testing \
    --status complete
```

To find failed records:

```bash
mf storage list \
    --testing \
    --status failed
```

To inspect missing files:

```bash
mf storage list \
    --testing \
    --status missing
```

These filters are useful for browsing known outcomes.

They do not perform new verification.

---

## Understand Cleanup Safety

Both pruning and deletion use safeguards to prevent unsafe removal.

These safeguards matter because MarketForge may have active operations, registered dependencies, and files whose recorded state has changed since the last inspection.

### Previews are not deletion commands

Commands without `--apply` normally perform the review stage.

They do not remove the selected archive bytes.

However, they may write the managed preview and lock files needed for a later operation.

### Pagination does not limit deletion targets

This is especially important.

A display limit only controls how many results are shown.

It does not necessarily restrict the underlying cleanup selection.

For example, displaying the first twenty matching records does not mean only those twenty records will be affected by a later approved operation.

Always review the complete selection before applying cleanup.

Use `--all` or the full JSON preview for that review.

### Applying a preview checks the selection again

MarketForge does not blindly trust an old preview.

Before applying cleanup, it checks whether the saved target selection and relevant safety conditions are still valid.

This can prevent removal when files or dependencies have changed.

### Some targets cannot be removed

A target may be blocked because of:

- A dependency on the managed file.
- An active operation.
- Missing or changed ownership information.
- An unsafe or changed filesystem path.
- An incomplete or unverifiable operation.
- Another safety condition enforced by the cleanup service.

Do not bypass these protections through manual database edits or direct filesystem deletion.

Investigate the blocker first.

### Keep recovery information

Cleanup operations retain registry history and can produce receipts that help explain or recover interrupted work.

Keep preview and receipt files while recovery is still pending.

A partially completed cleanup should be investigated through its reported outcomes rather than repeated blindly.

---

## Understanding Storage Errors

When a storage operation fails, the most useful next step depends on the type of problem.

| Problem | What to investigate |
|---|---|
| Archive not found | Check the local ID, selected data root, and registry. |
| File marked missing | Inspect the registered path and run verification if appropriate. |
| Checksum mismatch | Check whether the file was changed or corrupted; consider reacquisition. |
| Coverage gap | Review the selected interval and map the missing period through acquisition. |
| Unknown coverage | Check whether the source has reliable historical interval metadata. |
| Interrupted download | Inspect the run report and retained reservation information. |
| Reconciliation unsuccessful | Check whether the required durable verification evidence exists. |
| Cleanup blocked | Inspect dependencies, ownership, paths, and changed target state. |
| Database rejected | Verify the testing database URL, identity, and migrations. |

Avoid manually changing database statuses or moving managed files to make a failing operation appear successful.

Use the supported inspection and recovery commands so that the registry and filesystem remain consistent.

### Exit codes

Storage and cleanup commands use the common MarketForge CLI error handling.

| Code | Meaning |
|---|---|
| `0` | The operation or preview completed successfully. A successful preview does not mean deletion was applied. |
| `1` | An applicable verification, recovery, or cleanup operation was unsuccessful or incomplete. |
| `2` | A command, configuration, database, filesystem, ownership, or setup error was reported. |
| `130` | The command was interrupted through the handled cancellation path. |

Inspect detailed outcomes rather than relying only on the exit code.

For example, a cleanup preview with no eligible targets can complete successfully without deleting anything.

---
