# Manage database backups and restores

Use the workspace registry and its retained artifacts together when preparing or exercising a recovery.

## Back up and restore the registry

### Create a Database Backup

MarketForge provides:

```text
mf database backup BACKUP_DIR --testing
```

`BACKUP_DIR` is the destination directory for the new backup.

For example:

```bash
mf database backup ./metadata-backup --testing
```

This creates a backup under:

```text
./metadata-backup/
```

The destination must be a new directory.

Choose a location outside MarketForge's managed configuration, raw-data, and dataset roots.

Do not use an existing directory containing unrelated files.

### What files are created?

A completed backup has the following main structure:

```text
metadata-backup/
├── registry.dump
├── configuration/
└── manifest.json
```

Each part serves a different purpose.

### `registry.dump`

This contains the PostgreSQL registry backup.

MarketForge uses PostgreSQL's custom-format dump support to create it.

The backup contains the selected registry schemas, not the full historical data files stored on disk.

### `configuration/`

This contains a snapshot of the managed TOML configuration files included in the backup.

It helps preserve the configuration associated with the backed-up registry state.

However, restoring the database does not automatically activate these configuration files.

They remain available for deliberate review.

### `manifest.json`

This records information about the completed backup, including SHA-256 checksums used to verify its files.

Before restoring, MarketForge checks the completed backup and relevant configuration checksums.

### PostgreSQL backup utility

The backup command requires:

```text
pg_dump
```

You can check whether the utility is available with:

```bash
mf doctor --testing | cpout
```

Doctor reports PostgreSQL backup and restore tool availability along with workspace and registry readiness. Missing backup tools are nonblocking for ordinary processing. [Doctor reference](../reference/cli/maintenance.md#mf-doctor)

### Does backup modify the source database?

The backup operation reads the registry and writes backup files to the selected destination.

It does not perform a schema migration or replace the source database.

However, it creates real filesystem artifacts, so the destination must be chosen carefully.

### What happens if backup is interrupted?

An interrupted backup may leave an incomplete destination directory.

Do not treat that directory as a successful backup.

Review the operation result and the retained files.

For a clean retry, use a new destination rather than assuming an incomplete directory can safely be overwritten.

---

### Backing Up the Complete MarketForge Workspace

A database backup protects the registry.

However, a complete MarketForge recovery may also require its managed filesystem artifacts.

This matters because the registry contains references to files that exist outside PostgreSQL.

For example:

```text
PostgreSQL Registry
        |
        v
Registered Dataset
        |
        v
Filesystem Path
        |
        v
Canonical Parquet File
```

If the Parquet file is missing, restoring the registry record alone does not recreate the dataset.

Similarly, restoring a raw archive registration does not recreate the archive bytes.

### What should be preserved?

For a complete recovery strategy, consider the following components:

| Component | Why it matters |
|---|---|
| PostgreSQL backup | Preserves registered identities and metadata. |
| Managed raw archives | Preserves original acquired or imported data. |
| Normalized datasets | Preserves canonical processing outputs. |
| Merged datasets | Preserves completed merge outputs. |
| Publication manifests | Preserves publication structure and processing evidence. |
| Run receipts | Preserves operation and recovery information. |
| Managed configuration | Preserves the settings and recipes used to operate MarketForge. |

The appropriate backup strategy depends on the value of your data and whether the original sources can be obtained again.

For example, some public exchange archives may still be downloadable later.

However, purchased recordings or privately collected data may not be replaceable.

**Do not assume that every historical source will remain available indefinitely.**

Keep independent copies of data that would be difficult or impossible to obtain again.

### Keep metadata and files consistent

Ideally, a recovery set should contain registry information and filesystem artifacts representing a consistent application state.

For example, the restored registry should not refer to dataset files that were created later but were never included in the filesystem backup.

Similarly, copied dataset files should retain their associated manifests and verification information.

The current `mf database backup` command audits representative artifact references, but it is not a complete snapshot of the entire MarketForge filesystem.

A full workspace recovery procedure must account for the external files separately.

---

## Database Restore

### Restore a Database Backup

The restore command has this form:

```text
mf database restore BACKUP_DIR \
    --testing \
    --destination NAME \
    --disposable-testing
```

For example, after creating and verifying a backup at `./metadata-backup`, a restore invocation could be:

```bash
mf database restore ./metadata-backup \
    --testing \
    --destination marketforge_restore_a1b2c3d4 \
    --disposable-testing
```

The destination name is illustrative.

Choose a fresh, unused name that follows the required format.

### Requirements

The restore operation requires:

- A completed MarketForge database backup.
- A correctly configured source testing database.
- The `pg_restore` PostgreSQL utility.
- PostgreSQL permission to create a new database.
- An unused disposable destination name.

The PostgreSQL role must have the required `CREATEDB` permission.

### How are destination names restricted?

Restore destinations must follow this pattern:

```text
marketforge_restore_<hex>
```

The suffix must contain between 8 and 32 lowercase hexadecimal characters.

For example:

```text
marketforge_restore_a1b2c3d4
```

is structurally valid.

A destination outside the accepted naming scheme is rejected.

This restriction helps distinguish disposable restore targets from ordinary application databases.

### What does `--disposable-testing` mean?

The flag explicitly confirms that the restore destination is intended to be a disposable testing database.

It is required by the current restore command.

The flag does not make restoration a dry run.

MarketForge actually creates and restores the new PostgreSQL database when the operation succeeds.

---

