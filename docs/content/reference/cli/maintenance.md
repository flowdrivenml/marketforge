# Configuration and maintenance commands

Inspect configuration, initialize workspace services, migrate and back up or restore registry state. [Workflow and prerequisites](../../maintenance/index.md) · [Shared CLI behavior](index.md)


## Synopsis and arguments

### `mf init`

```text
mf init [-h] [--testing | --production] [--silent]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--silent` | Default `false`. |

### `mf doctor`

```text
mf doctor [-h] [--testing | --production]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

Doctor is a read-only readiness assessment. It checks these areas together and
expands the findings that need attention.

| Area | What Doctor checks |
| --- | --- |
| Environment | Supported Python, workspace root, and application TOML parsed with the Forge settings schema. Omitted configuration uses built-in defaults. |
| Database | PostgreSQL connection, expected database name and Forge UUID/purpose, registry metadata, required schemas, migration checksum history, and pending migrations. The inspection uses a read-only transaction with a 10-second connection timeout and 5-second statement and lock timeouts. |
| Rust Engines | Managed normalize, merge and convert binaries, installation checksums, and recorded smoke-test evidence when available. Doctor uses the installation inventory without launching a binary. |
| Storage | Resolved data root, directory and mount access, and filesystem capacity. A missing directory can report capacity from its nearest existing parent while writability remains unverified. Less than 1 GiB free is a warning. |
| PostgreSQL Tools | `pg_dump` and `pg_restore` on PATH. Missing tools warn about backup and recovery. |

A green **✓** indicates a passed check. Yellow **!** indicates a warning, red
**✗** indicates a failed check, and **?** indicates an unknown result. Dependent
database checks remain unknown when an earlier prerequisite blocks inspection.
Recommendations use the selected environment and include migration commands
only when local history permits them or the registry is cleanly uninitialized.

| Readiness | Meaning | Exit |
| --- | --- | --- |
| Ready | Required checks passed | 0 |
| Ready with warnings | Required checks passed with nonblocking findings | 0 |
| Not ready | A required check failed | 1 |
| Indeterminate | A required check could not be established | 1 |

Invalid CLI usage exits 2. The default environment is production. Use
`--testing` to inspect the testing registry explicitly.

```bash
mf doctor | cpout
mf doctor --testing | cpout
mf --json doctor | cpout
mf --json doctor --testing | cpout
```

JSON retains `checked_at`, Python and database fields, migration lists,
storage observations, PostgreSQL tool availability, and the Engine inventory.
It adds `environment`, `project_root`, `readiness`, `summary`, and the
individual `checks` with stable IDs and status values. Unavailable storage
observations use null. Use the individual database checks to interpret an
empty migration list when inspection was blocked.

Review [Forge application settings](../configuration/application.md),
[database prerequisites](../../getting-started/installation.md#postgresql-credentials),
and [managed Engine setup](engine.md) as directed by the report.

### `mf config show`

```text
mf config show [-h]
```

### `mf database backup`

```text
mf database backup [-h] --testing backup_path
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing` | Required. |
| `backup_path` | Required. New backup directory or existing verified backup for restore. |

### `mf database restore`

```text
mf database restore [-h] --testing --destination DESTINATION
                                    --disposable-testing
                                    backup_path
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing` | Required. |
| `backup_path` | Required. New backup directory or existing verified backup for restore. |
| `--destination` | Required. Fresh marketforge_restore_<hex> disposable database only. |
| `--disposable-testing` | Required. |

### `mf database migrate`

```text
mf database migrate [-h] (--production | --testing)
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Choose exactly one environment. |

### `mf database status`

```text
mf database status [-h] [--testing | --production]
```

| Argument or option | Parser contract |
| --- | --- |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

## Application configuration

You do not have to use the default application configuration path.

MarketForge supports selecting a configuration file explicitly through the global `--config` option.

For example:

```bash
mf --config ./workspace-settings.toml config show
```

This loads the application settings from:

```text
./workspace-settings.toml
```

The option must appear before the main command.

### Why use a separate configuration?

You may want different settings for different environments.

For example:

- A development environment with a small data root.
- A workstation with a larger local storage directory.
- A separate research environment with different HTTP limits.

Instead of repeatedly editing one file, you can keep separate application configurations and select the appropriate one when invoking MarketForge.

### JSON output

You can inspect the selected configuration in JSON:

```bash
mf --config ./workspace-settings.toml --json config show
```

The result includes the selected public settings and their resolved paths.

Sensitive database credentials are not included.

### Application settings are not acquisition recipes

The global `--config` option selects application settings.

It does not select an acquisition recipe.

For example:

```bash
mf acquisition map bybit-btc-20261027 --testing
```

uses a named acquisition recipe.

Meanwhile:

```bash
mf --config ./workspace-settings.toml \
    acquisition map bybit-btc-20261027 \
    --testing
```

uses the specified application settings while still selecting the named acquisition recipe.

These are two different configuration responsibilities.

---

<a id="database-management"></a>

## Database

Database-backed commands require a valid PostgreSQL connection.

MarketForge keeps database credentials separate from its public application configuration.

This means you should not put PostgreSQL passwords into:

```text
data/.config/forge/marketforge.toml
```

### Database credentials

Production is the default and uses `PRODUCTION_DATABASE_KEY`. Pass
`--testing` for `TEST_DATABASE_KEY`, or `--production` for explicit production
selection. The two flags are mutually exclusive.

MarketForge resolves this setting from the environment or the project's supported credentials file:

```text
.secrets/postgres.env
```

The credential loader checks for a nonempty environment value first.

If that is unavailable, it can read the corresponding value from the credentials file.

If neither supplies a usable value, the command reports a configuration error.

The testing and production connection settings are separate.

MarketForge does not silently replace a missing testing connection with the production database.

### Database identity protection

The current testing database is expected to be named:

```text
marketforge_test
```

MarketForge also checks the recorded database identity when accessing the testing registry.

This protects against accidentally using a database that does not belong to the expected testing environment.

For complete credential setup, see [PostgreSQL credentials](../../getting-started/installation.md#postgresql-credentials).

---

## Check the Database Status

Before performing database operations, it is useful to know whether the database is initialized and up to date.

Use:

```bash
mf database status --testing
```

MarketForge connects to the configured testing database and inspects its current state.

### What does the status command check?

The result includes information such as:

| Information | Meaning |
|---|---|
| Database name | The PostgreSQL database being inspected. |
| Database identity | The recorded MarketForge database identifier. |
| Purpose | Whether the database has the expected testing identity. |
| Initialization state | Whether the required application metadata exists. |
| Applied migrations | Database schema updates already recorded as applied. |
| Pending migrations | Schema updates that have not yet been applied. |

This helps you determine whether the database is ready for operations that require current registry tables.

### Does checking status modify anything?

No.

`mf database status` inspects the database without applying migrations.

This makes it the appropriate command when you want to understand the database state before changing anything.

### What if the database has pending migrations?

Pending migrations mean the database has not yet received all the schema updates required by the current application code.

For example, a newer MarketForge version may introduce additional tables or columns.

The database must be updated before features depending on those changes can work correctly.

Review the pending versions, then use `mf database migrate --production` or `mf database migrate --testing` to apply them to the intended registry.

---

## Update the Database Schema

As MarketForge evolves, its database structure may change.

For example, a new feature may require a table for dataset lineage or additional information about historical instrument specifications.

These changes are introduced through **database migrations**.

### What is a migration?

A migration is a controlled update to the database structure.

Instead of requiring users to manually create or modify tables, MarketForge provides migration scripts that are applied in the correct order.

The database records which migrations have already been applied.

This allows MarketForge to identify which updates are still needed.

### Apply pending migrations

Run:

```bash
mf database migrate --testing
```

MarketForge checks the existing migration history and applies the required pending core migrations.

The migration process uses database locking and a transaction to protect the schema update.

It also checks migration names and recorded checksums.

### Why are migration checksums important?

Suppose a migration was applied previously.

Later, someone changes the original migration file.

If MarketForge silently accepted the modified file as the same migration, the recorded database history might no longer match the source code.

Checksums help detect this kind of inconsistency.

MarketForge checks the migration history before proceeding.

This helps ensure that the database structure is consistent with the expected migration sequence.

### What if the database is already up to date?

If all current migrations are already applied, the command reports that there are no pending updates.

You do not need to manually recreate tables.

### Verify the migration result

After applying migrations, run:

```bash
mf database status --testing
```

Confirm that there are no pending migrations.

You can also use:

```bash
mf doctor --testing | cpout
```

to inspect additional environment requirements.

### When should I run migrations?

Run migrations when:

- Setting up the MarketForge database for the first time.
- Updating to a version that introduces schema changes.
- A command reports missing or outdated database structures.
- You need to confirm that the registry matches the current code.

For ordinary database preparation, migrations are sufficient.

**You do not need to run the complete `mf init` workflow solely to create or update the database schema.**

---

<a id="environment-checks"></a>

## What does initialization do?

MarketForge provides a command that performs several setup and market-information tasks together:

```bash
mf init --testing
```

This is more than a database initialization command.

### Why does MarketForge collect market information?

Suppose you want to work with historical BTCUSDT perpetual data from Bybit.

MarketForge needs information about the instrument, including its market identity and economic specifications.

Supported exchanges provide current instrument information through their APIs.

MarketForge can collect this information and store it in its catalogue.

It also maintains additional evidence used to prepare historical normalization jobs.

The full initialization workflow combines these tasks with database setup and standard ranking updates.

### What happens during initialization?

The current initialization workflow performs several stages.

| Stage | What happens |
|---|---|
| Application directories | Creates missing directories required by the application. |
| Database migrations | Testing initialization applies pending migrations. Production initialization requires a verified, up-to-date registry. |
| Catalogue collection | Retrieves current market metadata from supported exchange APIs. |
| Historical metadata enrichment | Adds or updates the supported historical instrument evidence. |
| Rankings | Refreshes and publishes standard turnover and volume rankings. |

These stages are related, but they solve different problems.

For example, database migrations prepare the registry structure.

Catalogue collection gathers information about markets.

Historical metadata enrichment helps establish the information required for historical processing.

Ranking updates produce market-selection results based on their supported inputs and policies.

### Initialization can contact external exchanges

Because initialization collects current market information, it can make network requests.

It requires access to the supported provider endpoints involved in that collection.

It may also take longer than a simple database migration because it performs multiple operations.

**Do not use `mf init` when you only need to inspect the configuration or apply database migrations.**

Use the narrower command for the task you actually need.

### Initialization does not download historical archives

The initialization workflow is concerned with application setup, market metadata, historical instrument evidence, and rankings.

It is not a replacement for historical archive acquisition.

If you want to download BTCUSDT trades and order-book archives, use the acquisition commands.

For example:

```bash
mf acquisition download bybit-btc-20261027 --testing
```

This assumes the named acquisition recipe already exists.

See [Historical Data Acquisition](acquisition.md) for the full workflow.

---

## What If Initialization Is Incomplete?

Initialization involves several independent operations.

For example, database migrations may succeed while a later exchange metadata request fails.

MarketForge can retain successfully completed work even when the overall initialization result is incomplete.

### A failed stage does not undo everything

Suppose initialization performs:

```text
Application directories       Complete
Database migrations           Complete
Catalogue collection          Complete
Historical metadata           Failed
Rankings                      Not completed
```

This is an illustrative sequence, not a reproduced run result.

The earlier successful stages may have already changed the application state.

You should not assume that an unsuccessful initialization automatically rolls back every completed operation.

### Inspect the failing stage

Review the initialization result.

Identify the stage that failed and read its reported error.

For example, the problem might involve:

- Database permissions.
- Network access to an exchange.
- Missing or conflicting market metadata.
- Incomplete historical specification evidence.
- Ranking prerequisites.

The appropriate next step depends on the actual failure.

### Should you run initialization again?

You can repeat initialization, but understand what it does.

A repeated run may collect newer market observations and publish new ranking snapshots.

It is not necessarily a no-op.

If only the database schema needs updating, use `mf database migrate --production` or `mf database migrate --testing`.

If only a particular market needs a metadata refresh, inspect the supported market-management commands rather than assuming the full initialization workflow is necessary.

### Initialization exit codes

The current initialization command uses these outcomes:

| Exit code | Meaning |
|---|---|
| `0` | The overall initialization completed successfully. |
| `1` | Initialization was unsuccessful or incomplete. |
| `130` | The operation was cancelled through the handled interruption path. |

For an incomplete result, inspect the individual stage information before retrying.

---

## Database Backups

## Why Is Database Restoration Restricted?

Restoring a database is potentially destructive.

A careless restore could overwrite existing registry information or cause a database to refer to the wrong filesystem artifacts.

MarketForge therefore restricts the current restore workflow to a **new, explicitly disposable testing database**.

It does not overwrite your existing testing database.

This is useful when you want to:

- Verify that a backup can be restored.
- Inspect an older registry state.
- Test database migrations against restored information.
- Investigate a problem without changing the original registry.

### What does disposable mean?

A disposable database is a separate database created for temporary or controlled testing.

For example:

```text
marketforge_test
```

may be your existing testing registry.

A restored copy might use a different name:

```text
marketforge_restore_a1b2c3d4
```

These are separate PostgreSQL databases.

The restore command requires the destination to be new.

It refuses to overwrite an existing destination.

---

## What Happens During Restoration?

MarketForge checks the supplied backup before restoring it.

The workflow includes several important safeguards.

### Verify the backup

MarketForge checks that the backup is complete and that the expected backup files and configuration checksums are valid.

A damaged or incomplete backup is not treated as a valid restore source.

### Check the destination

MarketForge requires a fresh destination database.

If the destination already exists, the operation refuses to overwrite it.

This protects existing databases from accidental replacement.

### Restore the registry

MarketForge creates the disposable destination and restores the registry data using PostgreSQL restore functionality.

The registry restoration is performed within the supported restore transaction.

### Establish a new database identity

After restoration, MarketForge checks the restored testing identity and assigns a fresh database UUID.

This allows the restored database to be distinguished from the original source database.

### What happens to configuration files?

The backup's configuration snapshot remains in the backup directory.

It is not automatically copied into the active application configuration.

Review the backed-up settings before choosing whether to reuse them.

This prevents an old configuration from silently replacing your current workspace settings.

### What happens to raw archives and datasets?

They are not recreated by the database restore.

The restored registry may contain references to files from the original workspace.

Those files must exist and remain consistent with the restored metadata before the corresponding data operations can work as expected.

### What if restoration fails?

A failed restore can leave the new disposable database available for inspection.

Do not assume that every unsuccessful restore automatically removes all created resources.

Review the reported error and inspect the destination before deciding whether to retry.

For a new clean attempt, choose another unused destination name.

---

## Command Reference

## Which Commands Change State?

Not every maintenance command performs the same type of work.

| Command | Database access | Network access | Can change state |
|---|---|---|---|
| `config show` | No | No | No |
| `database status` | Yes | No | No |
| `database migrate` | Yes | No | Yes |
| `doctor` | Yes | No | No |
| `init` | Yes | Yes | Yes |
| `database backup` | Yes | No | Writes backup files |
| `database restore` | Yes | No | Creates and restores a new database |
| `engine status` | No | No | No |

The table describes the main external-service and state-changing behavior of these commands.

For example, `init` may contact supported providers for metadata collection, while `database migrate` operates on PostgreSQL without performing exchange discovery.

Similarly, `database backup` does not change the source registry but creates real files on disk.

### JSON output

The relevant maintenance commands support global JSON output.

For example:

```bash
mf --json database status --testing
```

```bash
mf --json init --testing
```

```bash
mf --json config show
```

The global `--json` option must appear before the command name.

JSON output can be useful for diagnostics and automation.

Do not include credentials or private database connection strings in published logs.

---

## Common Questions

## Common Configuration and Maintenance Problems

### The application cannot find PostgreSQL credentials

**Possible cause:** The required testing database URL is missing or empty.

Check that the supported testing credential key is configured:

```text
TEST_DATABASE_KEY
```

Review the environment and the project's private credentials file.

Do not paste the connection string into public issue reports or documentation.

See [PostgreSQL credentials](../../getting-started/installation.md#postgresql-credentials).

### The testing database identity is rejected

**Possible cause:** The selected database does not have the required name or recorded testing identity.

Inspect:

```bash
mf database status --testing
```

Confirm that the configured connection points to the intended testing database.

Do not bypass the identity check by modifying database metadata manually.

### The database has pending migrations

**Possible cause:** The current application expects schema updates that have not yet been applied.

Run:

```bash
mf database migrate --testing
```

Then inspect:

```bash
mf database status --testing
```

### A migration fails

**Possible cause:** Database permissions, a conflicting schema state, or an inconsistent migration history.

Inspect the reported migration error.

MarketForge validates migration names and checksums.

Do not rewrite previously applied migration files or remove migration-history records merely to make the command succeed.

Investigate the mismatch before retrying.

### Doctor reports an unwritable storage location

**Possible cause:** The current user cannot write to the inspected storage directory or its nearest existing parent.

Inspect the configured data root:

```bash
mf config show
```

Check the filesystem permissions and available space.

If necessary, configure an appropriate data root.

### Doctor reports missing PostgreSQL tools

**Possible cause:** `pg_dump` or `pg_restore` is not available in the environment.

These tools are required for the corresponding backup and restore operations.

Install the appropriate PostgreSQL client utilities for your environment and rerun Doctor.

### Initialization cannot contact an exchange

**Possible cause:** Network access, provider availability, rate limits, or another provider-specific failure.

Review the initialization stage results.

Check which exchange or scope failed rather than assuming the entire catalogue is unavailable.

Completed stages may have retained useful information.

### Initialization reports incomplete historical metadata

**Possible cause:** The required historical instrument evidence could not be established for one or more scopes.

Inspect the enrichment result and its recorded reasons.

Do not invent historical contract values or extend specification validity without evidence.

Where appropriate, supply verified historical declarations through the catalogue workflow.

### Initialization rankings fail

**Possible cause:** Required market observations, conversion information, or ranking-policy conditions were not satisfied.

Inspect the ranking results independently from database migration and catalogue collection.

A ranking failure does not automatically mean earlier initialization stages failed.

See [Markets and Rankings](markets.md).

### Backup destination already exists

**Possible cause:** The backup command requires a new destination directory.

Choose an unused backup location.

Do not overwrite an existing backup without first reviewing and preserving it.

### Backup is incomplete

**Possible cause:** The backup process was interrupted or failed.

Do not assume that the presence of `registry.dump` alone means the backup is valid.

Review the completion result and manifest.

Use a new destination for a clean retry.

### Restore destination is rejected

**Possible cause:** The destination already exists or does not follow the required disposable testing naming format.

Choose a fresh name such as:

```text
marketforge_restore_a1b2c3d4
```

The suffix must contain 8–32 lowercase hexadecimal characters.

### Restore fails after creating a database

**Possible cause:** A restore or verification step failed after the destination database was created.

The disposable database may remain available for inspection.

Review the error and destination state before retrying.

Do not assume an unsuccessful restore automatically removes every created resource.

---
