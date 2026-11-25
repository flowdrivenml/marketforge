# Local development

Set up a checkout and verify the Python and Rust toolchains before using providers or the testing registry. All commands here start at the repository root unless shown in a subshell.

## Install the toolchains

On Linux or macOS, install the contributor environment and select it for the
`uv run` commands below:

```bash
./install.sh --dev --skip-market-init
export UV_PROJECT_ENVIRONMENT="$PWD/.install/dev"
```

The development installer selects testing for provisioning, migrations, and market initialization. It verifies Rustfmt and Clippy and installs pinned Zensical in `.install/docs`. The development and standard installations can coexist. Forge commands still default to production. Pass `--testing` for testing registry commands, including `mf tui --testing`.

For manual setup, use Python 3.12 or newer, uv, Neovim, and Rust/Cargo 1.89 or newer. The repository has no checked-in Rust toolchain pin or declared Cargo minimum Rust version. Cargo.lock pins dependencies. Managed Engine builds target the host and reject a configured foreign target. Conversion's atomic no-replace publication implementation is Linux-only.

```bash
uv sync --project forge --locked --group dev --group lab
uv run --project forge --locked mf --version
rustc --version
cargo --version
```

uv uses the Forge project environment, normally `forge/.venv`. The `dev` group includes pytest, Ruff and Pyright. The `lab` group includes PyArrow, Polars, JupyterLab, and ipykernel. Keep `--group lab` on commands that exercise dataset inspection or its tests. Forge installs the `mf` and `marketforge` entry points from the same package. Use `uv run --project forge …` from the checkout to select that environment explicitly. Configuration and paths derive from the checkout or the global `--project-root` argument. [Python project configuration](https://github.com/flowdrivenml/marketforge/blob/main/forge/pyproject.toml) · [Cargo features and targets](https://github.com/flowdrivenml/marketforge/blob/main/engine/Cargo.toml)

## Check a small baseline

```bash
uv run --project forge --locked --group dev pytest -c forge/pyproject.toml \
  forge/tests/unit/historical/archives/test_source_regressions.py -k observed_bybit_directory_variants
cargo test --locked --manifest-path engine/Cargo.toml \
  --features historical --test historical_normalize depth_boundary::
```

The Python selection exercises three saved Bybit listing variants through an HTTP mock. The Rust selection covers depth boundary checks. Each command should report tests passing with a nonzero number selected. Neither requires provider access nor a registry. Cargo typically uses `engine/target` and may download locked dependencies on the first build. See [Testing](testing.md) for wider selections and fixture prerequisites.

## Build the executable Forge will use

```bash
uv run --project forge --locked mf engine build normalize --dry-run
uv run --project forge --locked mf engine build normalize
uv run --project forge --locked mf engine status
```

Use `merge` or `convert` as the explicit target when that stage changes. The default managed build selects normalization. Forge builds approved release targets with the historical and monitor features and installs independent copies under `.bin/`. Cargo's own build output is separate. A direct Cargo test or build leaves Forge's installed executable as it was. `engine status` reports the installed checksum and recorded revision. Compare that revision to the code you intend to exercise, and rebuild after relevant edits. A verified checksum establishes installation integrity. Recorded revision and dirty state are limited source provenance. [Managed Engine implementation](https://github.com/flowdrivenml/marketforge/blob/main/forge/historical/engine/managed.py) · [Engine CLI](../reference/cli/engine.md)

## Set up the testing registry when needed

Create a dedicated PostgreSQL database named `marketforge_test`. Give its intended role the privileges to own its objects and apply migrations. See [PostgreSQL setup](../getting-started/installation.md#postgresql-credentials) for connection configuration. Use a nonblank `TEST_DATABASE_KEY` or the same key in the checkout's `.secrets/postgres.env`. Forge reads that file as data. Keep the URL out of commands, reports, and Git.

```bash
uv run --project forge --locked mf database migrate --testing
uv run --project forge --locked mf database status --testing
uv run --project forge --locked mf doctor --testing
```

Migration writes to the selected database. Verify purpose `testing` and review pending migrations in the status output. Testing workflows check both the database name and the purpose marker. Some pytest fixtures require the environment variable explicitly even when a CLI would use the file. See [Testing database](testing.md#testing-database-and-artifacts) for fixture behavior. Schema setup needs no provider metadata download. Use the separate [advanced Bybit tutorial](../examples/bybit/manual-normalization.md) when you intend to populate market metadata and acquire archives.

## Verify installer changes

The installer tests use mocked package managers by default. Run them with the
Forge development environment from the checkout:

```bash
uv run --project forge --locked --group dev pytest scripts/install/tests
```

To include the PostgreSQL test, set `MARKETFORGE_INSTALL_TEST_PG_BIN` to the
directory containing `initdb`, `pg_ctl`, and `psql`. The test starts its own
temporary cluster and uses separate databases. It requires a non-root user.

## Build and preview the handbook

Zensical is pinned in `docs/requirements.txt`. The development installer makes it available in `.install/docs`:

```bash
(cd docs && ../.install/docs/bin/zensical build --strict)
python3 docs/check_links.py
```

For a manual installation, use a separate documentation environment.

```bash
python3 -m venv docs/.venv
docs/.venv/bin/python -m pip install -r docs/requirements.txt
(cd docs && .venv/bin/zensical build --strict)
python3 docs/check_links.py
(cd docs && .venv/bin/zensical serve)
```

The build writes to ignored `docs/site/`. Stop the preview with Ctrl-C. Inspect navigation, links and diagrams in the browser. The native Mermaid fence added in `docs/zensical.toml` lets Zensical initialize rendering on pages with diagrams. A Markdown build verifies page generation. A browser check verifies Mermaid rendering. The pinned Zensical theme fetches the Mermaid runtime when a diagram page loads. [Zensical diagrams](https://zensical.org/docs/authoring/diagrams/)

The commands in this handbook work without a clipboard helper. If your shell has `cpout`, use it separately for human-readable output. In Bash or Zsh, enable `set -o pipefail` before piping a check so the check's failure reaches the shell. Keep interactive commands and machine-consumed output in their original form.
