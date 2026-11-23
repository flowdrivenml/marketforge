# Managing acquisitions

Use the [acquisition guide](historical-data.md) for the first run. This page covers discovery freshness, managed copies, and recovery.

## Discovery cache and refresh

A completed scan is fresh for six hours when its source scope, covering interval, and adapter version match. A successful empty scan is cacheable. Unsupported requests and partial or failed scans do not provide reusable completed coverage.

```bash
mf acquisition map september --testing --refresh
mf acquisition plan september --testing --cache-only --verify-local --limit 20 | cpout
```

A successful refresh supersedes overlapping completed discovery for that selection. A failed refresh leaves previous completed discovery intact. Refresh updates discovery evidence. Download can still reuse matching verified bytes.

The optional cache-only plan reads fresh completed scans from the selected registry. Unresolved requests need discovery. The archive list is a bounded sample (default 100, range 1–10000). With `--verify-local`, planning checks size and SHA-256 on eligible local copies in that sample without updating the registry. Use mapping to discover current upstream availability.

## Archive identity and managed copies

An archive's source identity includes its provider-native object key and revision evidence. Download URLs can expire or change independently of that identity. A changed upstream revision gets a separate managed copy. Changes that the provider exposes without distinct revision evidence may go undetected.

Download verifies matching completed local files by size and SHA-256 before reuse. If a matching copy is missing or corrupt, the registry records that condition. A needed download can renew an expired or missing source location. Forge checks supported container integrity and publishes verified bytes as immutable managed raw files, then finalizes their local archive registration.

Storage lists registered copies and their states. Nominal intervals describe the published archive boundaries. Even gap-free nominal intervals leave record integrity and market-event completeness unknown.

```bash
mf storage list --testing --exchange bybit --instrument BTCUSDT --wide | cpout
mf storage show LOCAL_ID --testing | cpout
mf storage coverage --testing --exchange bybit --instrument BTCUSDT --data-type trades --gaps | cpout
```

Use the local archive ID returned by the list command. See [storage commands](../../reference/cli/storage.md) for inspection, verification and cleanup options.

## Inspect runs and reports

```bash
mf runs list --testing --operation acquisition --limit 10 | cpout
mf runs show RUN_ID --testing | cpout
mf runs report RUN_ID --testing | cpout
mf runs logs RUN_ID --testing --limit 50 | cpout
```

Use the run ID from the acquisition result. The execution index provides searchable status. The report and recorded events contain detailed request and file outcomes. The download byte total covers measured completed transfers. Failed or interrupted transfers can consume additional bandwidth. The report path points into the configured data root's `runs/` directory. The event log is a bounded read of retained events. See [execution history and retained evidence](../../reference/cli/runs.md).

## Recover partial or failed runs

1. Read the report and events. Identify unsupported requests, successful empty searches, discovery failures and file failures separately. A partial run can leave completed copies available for reuse. A hard termination can leave a reservation without a final report.
2. Correct unsupported scopes in the recipe. For rate or network errors, let the provider cooldown expire before retrying. Forge uses bounded retries and honors cooldowns. Add `--requests-per-second 0.5` to lower pacing when appropriate. Provider budgets still apply.
3. Verify a suspect local copy, or reconcile an interrupted publication when a local archive ID is available.
4. Rerun `mf acquisition download september --testing` to reuse verified files and retrieve outstanding archives. An incomplete HTTP transfer starts again from the beginning.

```bash
mf storage verify --testing --archive LOCAL_ID | cpout
mf storage reconcile --testing --archive LOCAL_ID | cpout
```

Verification reads local bytes and can update registry state. Reconciliation uses durable size and hash evidence to recover an eligible reservation, publish a verified temporary file, or finalize registration. Both are local maintenance operations. Partials without sufficient evidence remain unverified.

## Cleanup

Use the [storage cleanup reference](../../reference/cli/storage.md#understand-cleanup-safety) to preview failed attempts with `storage prune` or managed copies with `storage delete`. Apply cleanup only after reviewing the selected targets and any dependencies. See the [Forge workflow](forge.md) for dataset build and registration recovery.
