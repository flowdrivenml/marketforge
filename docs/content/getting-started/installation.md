# Installation

Managed normalizer builds were verified on Linux. See the [Forge build reference](../reference/cli/engine.md) for approved features, offline builds and installation status.

Forge requires Python 3.12+ and uses Hatchling packaging at `forge/pyproject.toml`. Engine uses Rust edition 2024 with explicit Cargo features at `engine/Cargo.toml`. Linux and macOS are the project targets; platform installation recipes still need verification.

From the repository root, in your chosen application Python environment:

```bash
python3 -m pip install -e ./forge
mf engine build normalize
```

The editable Python package exposes `marketforge` and `mf`. Forge resolves the installed release binaries `.bin/marketforge-normalize` and `.bin/marketforge-merge` through checksum-verified installation metadata. Other Rust executables require their own feature/target selection; this is not an all-components installation.

Registry-backed operations require PostgreSQL, configured connections, and appropriate schema initialization. Workspace resolution expects `forge/pyproject.toml` and `engine/`; run from the checkout or use the CLI's `--project-root` option. Do not copy private connection values into documentation.

### PostgreSQL credentials

Database infrastructure resolves credentials automatically, in this order:

1. A nonempty environment variable.
2. The same key in `<project-root>/.secrets/postgres.env`.
3. An explicit configuration error if neither supplies a value.

Blank environment values allow file fallback. Testing exclusively uses
`MARKETFORGE_DATABASE_URL_TEST`; production resolution uses
`MARKETFORGE_DATABASE_URL`. Neither substitutes for the other. Existing testing
database-name and `forge_meta.database_identity` checks still apply.

The file is read as dotenv data, never executed or sourced. Ordinary assignments,
optional `export`, whitespace, comments and single/double quoted values are supported.
Shell commands and variable references remain literal; no interpolation occurs.
The file is resolved through project-root discovery or the CLI's `--project-root`,
so database commands also work from repository subdirectories or another directory
with an explicit root. No credentials are copied into application configuration.

### Prepare the testing registry

Create a dedicated PostgreSQL database named `marketforge_test` and provide a role that can apply its migrations. For a PostgreSQL role with database creation rights, run `createdb marketforge_test` once. Set the testing URL for that database using the credential loader above. Run from the checkout:

```bash
mf database migrate --testing
mf database status --testing
mf doctor --testing
mf engine status
```

Confirm identity purpose `testing`, no pending migrations, and a verified normalizer. Testing commands can write real files and registry rows. `mf init --testing` also collects provider metadata, enriches historical evidence and publishes rankings. Use [database migration](../reference/cli/maintenance.md#database) for schema setup alone.

### Inspection and notebook tools

Install Arrow in the same application environment for `datasets schema` and `datasets head`:

```bash
python -m pip install 'pyarrow>=18'
```

For optional notebook work, the repository's lab group also includes Polars and a Jupyter kernel. With uv installed:

```bash
uv sync --project forge --group lab
uv run --project forge --group lab python -m ipykernel install --user --name marketforge
```

Install the merger separately with `mf engine build merge` when needed. Continue to the [Bybit Quick Start](quickstart.md).
