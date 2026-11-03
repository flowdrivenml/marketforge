## Specification: MarketForge Installation, Binary Distribution, and PostgreSQL Setup

### Purpose and scope

Provide a simple installation process for Linux and macOS.

Users should prepare MarketForge through a root-level setup script and launch Forge without manually installing Python, creating a virtual environment, activating it, or compiling Rust.

Distribute prebuilt Rust binaries from the first public releases. Building from source remains an explicit option for development and unsupported release targets.

PostgreSQL is mandatory. Database preparation must be part of setup, with a recommended local container installation and support for existing PostgreSQL servers.

Windows support is outside the current scope.

### User-facing entry points

The installation entry point must be:

```bash
bash setup.sh
```

After successful installation, users launch Forge through:

```bash
./.bin/forge
```

These are the intended interfaces to implement.

Keep `setup.sh` at the repository root. Supporting installation logic belongs inside `forge/`, with shell helpers under `forge/scripts/`.

The root script should coordinate installation phases and delegate implementation details.

### Repository layout

Use the following locations:

| Location | Responsibility |
|---|---|
| `setup.sh` | Root installation entry point |
| `.bin/` | Forge launcher, installed engine binaries, installation manifest |
| `.venv/` | Repository-local Python environment |
| `forge/pyproject.toml` | Python dependencies and tooling configuration |
| `forge/uv.lock` | Committed Python dependency lockfile |
| `forge/scripts/` | Installation and maintenance helpers |
| `forge/shared/database/` | Shared database infrastructure and SQL migrations |
| `engine/` | Rust source and existing Cargo workspace |
| `data/` | Application data, runtime configuration, reports, and logs |

Ignore generated environments, installed binaries, and private runtime configuration in Git.

Cargo build output remains in the configured Cargo target directory. Do not copy caches or intermediate build artifacts into `.bin/`.

### Installation flow

Setup should coordinate these phases:

- Detect the operating system, architecture, and prerequisites.
- Prepare uv, Python, and the repository-local environment.
- Install the locked Forge dependencies.
- Download and install compatible engine binaries, or perform an explicitly selected source build.
- Configure the required PostgreSQL connection.
- Provision local PostgreSQL when that installation mode is selected.
- Initialize or migrate the MarketForge schemas.
- Create the launcher and installation manifest.
- Verify the installation and report readiness.

Check prerequisites early where possible, before lengthy downloads or compilation.

Installation must only report the application as ready after the required database checks and schema initialization succeed. If database preparation remains incomplete, clearly distinguish installed application files from a ready application.

### Python installation through uv

Use uv to manage the Python interpreter, virtual environment, and application dependencies.

Detect an available uv installation or obtain a supported version when missing. This initial step must work without an existing Python installation.

Declare the supported Python version in project-controlled configuration. Obtain a compatible interpreter when necessary.

Create the environment at:

```text
<repository-root>/.venv/
```

Keep Python project configuration at:

```text
<repository-root>/forge/pyproject.toml
```

Explicitly configure the environment location for uv project operations using `UV_PROJECT_ENVIRONMENT` and the resolved absolute repository path.

Installation and maintenance tools must consistently use this location. Do not depend on the caller’s current directory or an already activated environment.

Install dependencies from the committed lockfile. Ordinary installation must not silently change dependency versions or rewrite the lockfile.

Do not modify the user’s system Python or active Conda environment.

### Default and optional dependencies

The default Python installation should support historical acquisition, pipeline orchestration, dataset management, the CLI, and the historical TUI.

Development tools, benchmarks, and future research or ML dependencies must be optional groups defined in `forge/pyproject.toml`.

A normal historical installation must not require the future laboratory stack.

Development database provisioning must also be optional and separate from normal application setup.

### Forge launcher

Create the launcher at:

```text
<repository-root>/.bin/forge
```

The launcher must locate the repository and execute Forge through its `.venv/` interpreter.

Users must not need to activate the environment or edit their shell configuration.

Resolve paths independently of the caller’s working directory. If the environment or required binaries are missing, explain that setup must be run.

Normal launches must not rebuild binaries, upgrade dependencies, download new releases, or apply database migrations automatically.

Database availability checks and starting the existing MarketForge-managed database container are permitted startup operations, as described below.

### Prebuilt engine binaries

Use GitHub Releases to distribute compiled engine binaries.

Release assets must remain separate from Git history. Do not commit binaries or use Git LFS as the primary executable distribution mechanism.

Initially, publish one compressed bundle per supported and tested platform.

Example asset names:

```text
marketforge-v0.1.0-linux-x86_64.tar.gz
marketforge-v0.1.0-macos-arm64.tar.gz
marketforge-v0.1.0-macos-x86_64.tar.gz
SHA256SUMS
```

Only advertise targets that have actually been built and tested.

Bundles should contain the normal application executables, required distributable runtime libraries where applicable, and a machine-readable manifest.

Exclude benchmarks, development-only executables, caches, and intermediate artifacts.

Use release builds, remove unnecessary debug symbols, and compress the bundles. Measure actual distribution sizes rather than estimating them from Cargo build directories.

### Platform compatibility

Detect both operating system and CPU architecture.

Define the supported Linux runtime baseline and minimum macOS version for each published target. Avoid binaries that accidentally require the build machine’s specific CPU features.

Identify runtime shared-library requirements. Bundle dependencies where appropriate and permitted, or document and check the required system prerequisites.

When no compatible bundle exists, explain the available source-build option. Do not silently select a different architecture or begin compilation.

### Engine version matching

Declare the engine release or compatibility requirement in project-controlled configuration.

Select release assets using that requirement. Do not download an unrelated “latest” release for an arbitrary source checkout.

For development commits without compatible published binaries, require the explicit source-build path.

Install a coherent executable set. Do not leave incompatible versions mixed together after an update.

### Binary installation and manifest

Download bundles into temporary staging storage and verify their published SHA-256 checksums before installation.

Validate the bundle manifest, executable set, target platform, and reported versions before replacing installed binaries.

Preserve working binaries until their replacement has passed validation. A failed download or extraction must not destroy the previous installation.

Maintain:

```text
<repository-root>/.bin/manifest.json
```

Record the manifest format version, engine release, source revision when available, target platform, installation method, installed executables, relevant build features, and downloaded bundle checksum.

Use the shared executable resolver and absolute paths when invoking engine binaries.

Checksums detect corrupted or mismatched downloads; do not describe them as independent publisher authentication.

### Source-build installation

Provide an explicit source-build mode.

Detect Rust tooling and help obtain the required toolchain when missing. Respect the repository’s pinned toolchain and committed Cargo lockfile.

Check native compiler, linker, and other build prerequisites before compilation. Provide platform-specific instructions for missing prerequisites.

Do not automatically modify system packages through `sudo`, Linux package managers, or Homebrew.

Build the required release executables and install them into the same `.bin/` layout used by downloaded bundles. Generate the same installation manifest and run the same final executable checks.

### GitHub Actions releases

Create a release workflow that builds and checks supported Linux and macOS targets, packages executables, generates manifests and checksums, and attaches assets to the corresponding GitHub Release.

Tie each release to a version tag and source revision.

Verify that the packaged executables can start and report their versions on supported platforms. Compilation alone is insufficient.

Use release assets for durable installer downloads. Temporary workflow artifacts may support development but must not be the public installation source.

## Required PostgreSQL Setup

### Installation choices

PostgreSQL is required, but users must not be forced to install and configure it manually.

Offer these choices during setup:

| Mode | Behavior |
|---|---|
| Local PostgreSQL — recommended | Provision a dedicated PostgreSQL container and persistent storage |
| Existing PostgreSQL server | Use supplied connection details and prepare MarketForge’s database schemas |

Remember the selected mode so repeated setup does not ask the user to configure it again unnecessarily.

For noninteractive installation, require explicit configuration or previously saved settings. Do not guess an existing server’s address or credentials.

### Local container prerequisites

The local mode requires an installed and running Docker engine and the supported Compose tooling.

Check these prerequisites before provisioning PostgreSQL.

If Docker is missing or unavailable, provide actionable platform-specific guidance and allow setup to be rerun afterward. Do not claim database setup is complete.

Automatic Docker installation and native PostgreSQL package installation are outside the initial implementation scope.

Docker is required only for the managed local database mode. Users connecting to an existing PostgreSQL server do not need Docker.

### Local PostgreSQL provisioning

Keep the version-controlled container definition alongside the shared database infrastructure.

Use the official PostgreSQL image and pin a tested version. Do not automatically track the `latest` tag or upgrade PostgreSQL’s major version.

Setup must:

- Create a dedicated MarketForge database instance.
- Generate credentials on initial provisioning.
- Configure a dedicated application role and database.
- Allocate persistent database storage.
- Publish the database port on localhost only.
- Start PostgreSQL and wait for a successful readiness check.
- Apply the SQL migrations.
- Verify connectivity using the application credentials.

Do not use PostgreSQL administrative credentials as the normal application connection.

Handle an occupied host port explicitly. Do not stop another database or container to free the port.

### Persistent storage and instance identity

Use a dedicated persistent Docker volume for PostgreSQL storage. Mount it at the correct location for the pinned image version.

Database files in that volume are separate from archive and dataset files under `data/`.

Record the managed instance identity, volume name, connection settings, and selected PostgreSQL version in private runtime configuration.

Use a stable instance identity that survives setup reruns and avoids accidental collisions with other installations.

Restarting or replacing the container must reuse the existing volume and credentials.

Application reinstallation, binary updates, container shutdown, and ordinary cleanup must never delete the database volume.

Deleting a database or its storage must be a separate, explicit destructive maintenance action.

### Credentials and private configuration

Store generated credentials and connection settings in private runtime configuration under:

```text
<repository-root>/data/.config/
```

Protect credential files with restrictive permissions and ensure they are ignored by Git.

Do not print passwords, full credential-bearing connection URLs, or administrative secrets in progress output or logs.

Reuse existing credentials when setup is rerun. Do not regenerate passwords while an initialized database still expects the previous values.

Avoid duplicate credential stores. Forge and engine configuration should obtain the required connection details through the established configuration mechanism.

### Existing PostgreSQL servers

Accept connection details for an existing local or remote server.

Validate connectivity, supported PostgreSQL version, and required permissions before migration.

Support either an existing application database or an explicit provisioning step using suitable administrative access. An ordinary application connection must not be assumed to have permission to create databases or roles.

Scope migrations to MarketForge-owned schemas and objects. Do not reset databases or modify unrelated application data.

Preserve configured connection security requirements, including TLS where applicable.

### SQL initialization and migrations

Use the version-controlled SQL migrations under the shared database infrastructure.

Track applied migrations and make repeated setup safe.

Initialize a fresh database automatically as part of the selected setup mode. Apply upgrades through setup or an explicit migration command, with clear reporting.

Do not silently perform schema migrations during ordinary application launch.

If the schema is incompatible or requires migration, fail the database readiness check with an actionable instruction.

### Database lifecycle during application launch

For the managed local mode, Forge startup should check the recorded container and database state.

If the existing MarketForge container is stopped and Docker is available, start it and wait for readiness.

Do not create a replacement empty database when an expected container or volume is missing. Report the missing instance and provide a recovery or setup instruction.

If Docker is unavailable, explain that the managed database cannot start.

For an existing-server configuration, only check connectivity. Do not attempt to start, stop, or otherwise manage that external server.

Closing Forge must not stop or delete PostgreSQL. The database may also be serving engine processes or other MarketForge clients.

Provide explicit CLI maintenance commands for checking and stopping the managed instance. Do not add development database controls to the historical TUI.

### Development and testing databases

Normal setup provisions or connects to the production application database only.

An optional development setup may create a separate testing database with the required core schemas and development-only benchmark schema.

Test tooling must use explicit test configuration and must never fall back to the production connection.

Do not expose testing database selection or benchmark administration in the end-user TUI.

## Reliability and Acceptance

### Repeatable installation

Setup must be safe to rerun.

Reuse compatible tools, binaries, environments, database containers, volumes, and saved configuration. Preserve user data and existing working components when a phase fails.

Display concise progress and identify the failed phase with an actionable explanation. Retain useful diagnostic output without exposing credentials.

Do not silently upgrade PostgreSQL, reset schemas, replace credentials, or delete persistent storage.

### Acceptance criteria

On a supported platform, a user must be able to run root `setup.sh`, install Python dependencies and compatible prebuilt engine binaries, prepare the required database, and launch Forge through `./.bin/forge`.

With Docker already installed and running, the recommended local database mode must require no manual PostgreSQL installation, SQL initialization, or credential creation.

Existing-server users must be able to complete setup without Docker.

Verify supported platform installation, interrupted downloads, checksum failures, unsupported binary targets, source builds, setup reruns, database readiness timeouts, occupied ports, unavailable Docker, existing-server connections, migration failures, and managed database restart.

A failed installation must not destroy existing datasets, database contents, credentials, persistent volumes, or working engine binaries.
