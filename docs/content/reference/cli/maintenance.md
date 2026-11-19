# Initialization, configuration and maintenance

## Initialization and Doctor

| Invocation | Requirements and effects |
| --- | --- |
| `mf init --testing [--silent]` | Creates missing application directories, applies core migrations, collects current catalogue metadata over HTTP, enriches historical evidence and refreshes/publishes standard turnover and volume rankings. Uses workspace ownership and a ranking lock. Requires testing credentials, PostgreSQL permissions and access to supported providers. |
| `mf doctor --testing` | Connects to the initialized testing database and checks recorded core migration names/checksums. Reports applied/pending versions, Python version and its 3.12 readiness check, the nearest existing data-root parent and its writability/free space, and availability of `pg_dump` and `pg_restore`. |

Initialization's `--silent` and global `--json` suppress collection progress. The result contains per-stage and ranking outcomes. Exit `1` means incomplete initialization, and earlier successful writes can remain. A cancelled workflow returns `130`. Review stage diagnostics before retrying. A repeat can fetch new observations and publish new snapshots.

Doctor reads metadata and reports boolean checks. It neither installs nor validates the managed binaries and does not run example workloads. Pending migrations or false readiness flags appear in the result; a successful command exit does not mean every check passed. Use `mf engine status` to verify managed installations.

## Application configuration

```bash
mf config show
mf --config ./workspace-settings.toml --json config show
```

`config show` reads public settings without creating directories, connecting to PostgreSQL or loading a recipe. The output includes the resolved project root, settings file or built-in defaults, configured and resolved data roots, editor and presentation choices, and HTTP settings. It excludes credentials.

Here is a complete application settings example using built-in defaults:

```toml
version = 1

[storage]
data_root = "data"

[presentation]
mode = "progress"
color = "auto"

[http]
requests_per_second = 2.0
max_concurrent = 4
timeout_seconds = 30.0
max_attempts = 3
```

Relative `storage.data_root` is resolved against the workspace. Raw data, runs and dataset publications follow that root. Managed TOML libraries still live under the checkout's `data/.config/`. `version` is `1`; unknown keys are rejected. HTTP rate and timeout must be positive and finite, concurrency is 1–64, and attempts are 1–10. Provider budgets still cap requests. `presentation.editor` is optional; interactive edit paths fall back to `VISUAL`, `EDITOR`, then `vi`. `mode` is `progress` or `silent` and controls the acquisition progress default. `color` is `auto`, `always`, or `never`. Auto detects terminal capabilities and respects `NO_COLOR`. [Dataset presentation](../../workflows/datasets.md#tokyo-night-colors)

## Database

Use the [testing credential loader](../../getting-started/installation.md#postgresql-credentials). Commands guard the database identity before registry access or mutation.

| Invocation | Behavior and output |
| --- | --- |
| `mf database status --testing` | Inspect database name, purpose, identity and migration status without applying migrations. JSON reports initialization and migration state. |
| `mf database migrate --testing` | Acquire workspace ownership and apply pending core migrations in a batch transaction, with a database migration lock and checksum/history checks. Reports applied and already-applied versions. |
| `mf database backup BACKUP_DIR --testing` | Write a custom-format `registry.dump`, `configuration/` TOML snapshot and SHA-256 `manifest.json` to a new directory outside the configuration, raw and dataset roots. Requires `pg_dump`. |
| `mf database restore BACKUP_DIR --testing --destination NAME --disposable-testing` | Verify the completed backup and TOML checksums, create a fresh disposable database and restore its registry in one restore transaction. Requires `pg_restore` and `CREATEDB`. |

Backup covers the `forge_meta`, `catalog`, `historical` and `ranking` schemas. It audits representative artifact references. Retain the actual raw archives, normalized and merged files, shared manifests and run receipts separately. An interrupted backup may leave an incomplete directory. A fresh retry needs a new destination.

Restore verifies the source testing database and requires a fresh destination matching `marketforge_restore_` plus 8–32 lowercase hexadecimal characters. It refuses existing destinations and checks the restored testing identity before assigning a fresh database UUID. A failed restore can leave the disposable database for inspection. The verified configuration snapshot remains in the backup directory for deliberate review and activation.

```bash
mf --json database status --testing
mf database backup ./metadata-backup --testing
```

After verifying the backup and choosing an unused disposable name, a restore command has this form:

```text
mf database restore ./metadata-backup --testing --destination marketforge_restore_a1b2c3d4 --disposable-testing
```

The maintenance commands support global `--json`. An identity, tool, migration-history or filesystem error is a setup failure. See also [retained run reports](acquisition.md#run-inspection).
