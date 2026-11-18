# Installation

Managed normalizer builds were verified on Linux. See the [Forge build reference](../reference/cli/forge.md) for approved features, offline builds and installation status.

Forge requires Python 3.12+ and uses Hatchling packaging at `forge/pyproject.toml`. Engine uses Rust edition 2024 with explicit Cargo features at `engine/Cargo.toml`. Linux and macOS are the project targets; platform installation recipes still need verification.

From the repository root, in your chosen application Python environment:

```bash
python3 -m pip install -e ./forge
mf engine build normalize
mf engine build merge
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

Next, [explore the CLI and run the AAPL example](quickstart.md).


uv add --project forge --group lab ipykernel
uv sync --project forge --group lab
uv run --project forge --group lab \
    python -m ipykernel install \
    --user \
    --name marketforge \
    --display-name "MarketForge (Python)"
