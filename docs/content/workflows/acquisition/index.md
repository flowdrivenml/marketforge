# Historical data acquisition

Acquisition discovers provider archives, records their identities, and downloads verified local copies. It does not normalize market events. Register external recordings and purchased files through the [BYOD workflow](../../examples/bring-your-own-data/index.md) to preserve their declared origin and bytes.

Use [Markets](../markets/index.md) to inspect current instrument metadata and statistics. Check the historical provider and date separately before selecting archives.

Start with [the historical-data workflow](historical-data.md), use the [supported historical sources](../../reference/acquisition-sources.md) and [configuration reference](../../configuration/acquisition.md) to select inputs, then [inspect and manage acquisitions](managing-acquisitions.md). The [CLI reference](../../reference/cli/acquisition.md) lists exact arguments and effects.

## Prerequisites

Install Forge using the [installation guide](../../getting-started/installation.md) and run commands from the repository root. Registry-backed commands default to production. Pass `--testing` for the
dedicated `marketforge_test` database with a verified testing identity.

Forge reads `PRODUCTION_DATABASE_KEY` or `TEST_DATABASE_KEY` for the selected environment, first from the process environment and then from the workspace's literal `.secrets/postgres.env` file. Do not source shell expressions or put credentials into shared recipes. Ask the workspace administrator to provision the database, testing identity and current schema. `mf database status --testing` inspects migration status. `mf doctor --testing` checks the initialized environment using the guarded testing loader. These commands connect to PostgreSQL; neither downloads archives.

If the provisioned testing database needs pending migrations, the explicit `mf database migrate --testing` command applies them and writes schema/identity state. Coordinate schema migration with other testing database users.

Application settings default to `data/.config/forge/marketforge.toml`. Raw copies live beneath the configured data root's `raw/`; recipes remain in the workspace's `data/.config/workflows/data_engineering/acquisition/`. A different storage root does not relocate the recipe library. Configuration, database access, writable storage and internet access are distinct prerequisites.

## Understand command effects

| Operation | Effects |
| --- | --- |
| `acquisition list`, `validate` | Read local configuration with no provider or database access. |
| `acquisition sources` | Read historical capabilities from the installed adapters. |
| `acquisition new`, `edit` | Write a saved TOML recipe; do not acquire data. |
| `acquisition plan ... --testing --cache-only` | Read registry/cache; optional read-only local hashing. No HTTP or writes. |
| `acquisition map ... --testing` | May contact providers; writes discovery/cache records and run artifacts, without downloading archive bodies. |
| `acquisition download ... --testing` | Maps as needed, downloads/reuses files, and updates registry and run artifacts. |
| `storage list`, `show`, `coverage` | Read registry records without rehashing files. Use `--testing` to select the testing registry. |
| `storage verify`, `reconcile` | Maintenance: can update records and recover reserved files, without HTTP. |

> **Testing is a destination guard, not a dry run.** Mapping, downloading, importing, synchronization, and publication perform real work when passed `--testing`.
