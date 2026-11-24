# Troubleshoot and recover

From the checkout, use `mf config show` to confirm the effective data root and `mf engine status` to check the managed binaries. Run `mf database status --testing` before registry work and `mf database migrate --testing` to apply pending migrations. [Database maintenance](../reference/cli/maintenance.md#database) documents backup scope, restore destinations and privileges.

Use [storage inspection](../reference/cli/storage.md) and [run reports](../reference/cli/runs.md) to trace an acquisition or verify its raw copy. For normalization and merge, retain the operation-specific reports, committed manifest and auxiliary files. When a publication succeeds and registration fails, use the [registration command](../reference/cli/normalize.md) with the retained execution intent and the original registry. When storage reconciliation reports a partial copy, inspect its status before any cleanup.

Keep the database backup and filesystem artifacts together for recovery. Validate configuration snapshots before activation and use a disposable destination to exercise a restore.

## Find the failing boundary

| Symptom | First check | Where to go next |
| --- | --- | --- |
| Installer reports partial initialization | Check which catalogue, enrichment, or ranking stage failed, then retry the selected initialization command | [Installer results](../getting-started/installation.md#installer-options-and-results) |
| Registry unavailable or identity rejected | Run `mf database status` with the intended environment | [Connection setup](../getting-started/installation.md#inspect-a-database) |
| Recipe validates but no raw input is selected | Compare requested scope and interval with local status, origin, and feed | [Raw storage](../workflows/acquisition/managing-acquisitions.md) |
| Engine command or option is unavailable | Compare Forge and the managed target installed under `.bin/` | [Engine builds and runtime inspection](../reference/cli/engine.md) |
| Prepared run cannot resume | Check retained registry identity, input digests, and executable evidence | [Pipeline resume](../workflows/pipeline.md#inspect-a-preparation-and-resume) |
| Output exists but has no registry rows | Check whether the execution report verified the publication before attempting registration recovery | [Publication and registration](../concepts/evidence.md) |
| Run deletion is blocked or interrupted | Read the blocker or receipt. For interrupted cleanup, retry the original saved plan after checking the reported failure. | [Run deletion and recovery](../reference/cli/runs.md#apply-and-recovery) |

## Maintenance procedures

Use [database backup and restore](database.md) to protect or restore registry state.

For publication deduplication, deletion and run index recovery, see [Dataset and run maintenance](publications.md). For converter compatibility, unavailable valuations and committed output recovery, see [Troubleshoot conversion](conversion.md).
