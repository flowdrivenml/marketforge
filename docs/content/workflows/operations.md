# Operate MarketForge

From the checkout, use `mf config show` to confirm the effective data root and `mf engine status` to check the managed binaries. Run `mf database status --testing` before registry work and `mf database migrate --testing` to apply pending migrations. [Database maintenance](../reference/cli/maintenance.md#database) documents backup scope, restore destinations and privileges.

Use [storage and runs](../reference/cli/acquisition.md#storage-inspection) to trace an acquisition or verify its raw copy. For normalization and merge, retain the operation-specific reports, committed manifest and auxiliary files. When a publication succeeds and registration fails, use the [registration command](../reference/cli/normalize.md) with the frozen execution intent or committed manifest. When storage reconciliation reports a partial copy, inspect its status before any cleanup.

Keep the database backup and filesystem artifacts together for recovery. Validate configuration snapshots before activation and use a disposable destination to exercise a restore.
