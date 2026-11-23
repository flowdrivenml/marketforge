# Installation

## Standard installation

On Ubuntu, Debian, or a Homebrew-supported macOS release, clone the repository and run:

```bash
git clone https://github.com/flowdrivenml/marketforge.git
cd marketforge
./install.sh
```

The installer supports x86-64 and ARM64. Run it as your regular user. Ubuntu and Debian may request `sudo` for packages or local PostgreSQL administration. On macOS, finish the Apple command-line tools installation and rerun if prompted. The installer asks before bootstrapping Homebrew. It does not run Homebrew as root.

The installer verifies or installs Neovim, Python 3.12 or newer, uv, and Rust/Cargo. The current locked Engine requires Rust 1.89 or newer. It synchronizes Forge in `.install/runtime` and builds and checksums the managed normalize, merge, and convert binaries through Forge. The installer creates a new Forge configuration with `presentation.editor = "nvim"` when there is no existing configuration. Existing editor settings, Neovim configuration, and plugins are preserved. Conversion’s atomic publication step currently requires Linux.

It installs the `mf` and `marketforge` entry points under `~/.local/bin`. If they are missing from your PATH, add that directory using your shell’s setup or run `~/.local/bin/mf` directly. An occupied launcher path is reported and left alone. Run workspace commands from the checkout or pass `--project-root PATH` before the command.

## Development installation

```bash
./install.sh --dev
```

Development selects the testing database for provisioning, migrations, and initialization. It verifies the runtime tools and adds the locked `dev` and `lab` groups, including pytest, pytest-asyncio, Ruff, Pyright, Polars, Arrow, JupyterLab, and ipykernel. It also checks Rustfmt and Clippy and installs the pinned Zensical requirement in `.install/docs`. The contributor environment is `.install/dev`. The two environments coexist and working launchers from this checkout are reused. Use the [contributor setup](../development/setup.md) for environment selection and test commands.

Forge itself still defaults to production. Use `mf tui --testing` and pass `--testing` for testing registry commands.

## Installer options and results

| Option | Behavior |
| --- | --- |
| `--dev` | Install development tools and select testing. |
| `--dry-run` | Inspect the platform and available executables, then print planned stages without making changes. |
| `--skip-postgres` | Skip PostgreSQL packages, services, and provisioning. Require an explicit connection and still apply the selected migrations. |
| `--skip-market-init` | Finish local setup and database migrations without collecting provider metadata or building rankings. |
| `--help` | Show usage. |

On success, the installer exits with status 0. Market initialization is a separate stage after local setup and database verification. It can contact providers to collect market metadata, enrich historical evidence, and publish rankings. A partial or failed initialization exits with status 3 and retains the completed setup. Retry with `mf init --production` or `mf init --testing` for the selected registry. Cancellation is reported separately. A failed required installation stage exits with status 1.

Reruns reuse compatible tools, verified binaries, credentials, and migrations. They retain existing configuration and data. The installer remembers successful market initialization for the same registry identity and migration version. A run with `--skip-market-init` leaves that stage available for a later run.

## Use an existing PostgreSQL server

PostgreSQL 14 or newer is required. With a matching credential already configured, the installer uses that connection without installing a local server or changing its service. A valid registry is checked for name, purpose, identity, and migration history before migrations. An explicitly selected empty database with schema creation privileges can be initialized. An existing database with user objects and no Forge history needs an administrator’s review. The installer refuses to adopt it.

For local provisioning without credentials, the installer uses one running APT cluster or a Homebrew PostgreSQL server on the default socket and port. It can install and start a new server through the native backend. It creates an unprivileged, dedicated owner role and the database `marketforge` or `marketforge_test`. If the target name already exists, configure its verified credential or use a different approved production database. The installer leaves existing clusters, roles, and authentication settings in place.

```bash
./install.sh --skip-postgres
./install.sh --dev --skip-postgres --skip-market-init
```

### PostgreSQL credentials

Database infrastructure resolves credentials automatically, in this order:

1. A nonempty environment variable.
2. The same key in `<project-root>/.secrets/postgres.env`.
3. An explicit configuration error if neither supplies a value.

Blank environment values allow file fallback. Production is the default and
uses `PRODUCTION_DATABASE_KEY`. Pass `--testing` to use `TEST_DATABASE_KEY`.
Use `--production` to select production explicitly. The flags are mutually
exclusive and each environment uses its own key.

The file is read as dotenv data, never executed or sourced. Ordinary assignments,
optional `export`, whitespace, comments and single/double quoted values are supported.
Shell commands and variable references remain literal; no interpolation occurs.
The file is resolved through project-root discovery or the CLI's `--project-root`,
so database commands also work from repository subdirectories or another directory
with an explicit root. Local provisioning adds the generated credential to the
file with mode `0600`, preserving unrelated entries. Use a dedicated role and
keep this file private and out of version control.

### Inspect a database

Run `mf database status` for production or `mf database status --testing` for
testing. Credentials must explicitly name the database. Forge compares that
name with the server response, verifies the stored database UUID and purpose
in `forge_meta.database_identity`, and checks applied migration checksums.
Testing additionally requires the database name `marketforge_test`.

If PostgreSQL is unreachable, check role access, network access, and which key
is set in the process environment. If the name or purpose check fails, verify
the credential assignment with the database administrator. For a newly
provisioned database, review schema availability before applying migrations
with `mf database migrate --production` or `mf database migrate --testing`.
Status checks and TUI startup are read-only with respect to the registry.
Production initialization requires a verified registry with all migrations
already applied.

## Manual installation

Use this route to manage packages, tools, and PostgreSQL yourself. Install Neovim,
Python 3.12 or newer, uv, Rust 1.89 or newer, and the native compiler tools.
Provision a PostgreSQL connection as described above.

```bash
uv sync --project forge --locked --no-default-groups
uv run --project forge --locked --no-default-groups mf engine build normalize
uv run --project forge --locked --no-default-groups mf engine build merge
uv run --project forge --locked --no-default-groups mf engine build convert
```

Use the environment's executable directly or prefix the commands below with
`uv run --project forge --locked --no-default-groups`. For a production registry,
review its status before applying migrations or initializing market data.

```bash
mf database status
mf database migrate --production
mf init --production
```

### Prepare the testing registry

Create a dedicated database named `marketforge_test` and give its intended role
the privileges to own its objects and apply migrations. Configure
`TEST_DATABASE_KEY`, then run:

```bash
mf database migrate --testing
mf database status --testing
mf doctor --testing
```

Confirm purpose `testing` and zero pending migrations. Use `mf init --testing`
when you are ready to collect metadata.

### Inspection and notebook tools

The development installer includes these tools. For a standard or manual installation, install Arrow in the Forge environment for `datasets schema` and `datasets head`:

```bash
uv pip install --python .install/runtime/bin/python 'pyarrow>=18'
```

For a manual Forge environment, point `--python` at that environment’s interpreter. Use `--dev` if you also want Polars and JupyterLab.

## Verify and troubleshoot

Run `mf normalize profiles list` to inspect the installed normalization catalogue locally. Before compiling a custom profile, check the managed normalizer with `mf engine status`. The [profile authoring guide](../workflows/catalogue/profiles.md) covers the draft and validation steps.

```bash
mf --help
nvim --version | head -n 1
mf config show
mf engine status
mf database status
```

Check that all three Engine targets are verified. For a development install,
use `mf database status --testing` and open `mf tui --testing`.

- If `mf` is unavailable, use the absolute launcher path and check which installation your PATH selects.
- If a selected local cluster is stopped or ambiguous, inspect it with the local administrator. Supply the intended connection and use `--skip-postgres`.
- If a configured registry is rejected, check the credential key, database name, purpose, and migration history before retrying. See [database troubleshooting](../workflows/operations.md#find-the-failing-boundary).
- If a newer toolchain is required, update the active toolchain deliberately, then rerun. A verified binary with an unsupported runtime option needs a managed rebuild using the current checkout.

Continue with the [Bybit quick start](quickstart.md).
