# MarketForge CLI reference

The Forge Python package installs `mf` and `marketforge` as two entry points to the same parser. Python manages recipes, registry access, reports, and installed engines. Rust performs historical processing.

```bash
mf --help
marketforge --help
mf --version
mf datasets list --help
```

## Commands

| Command | Use |
| --- | --- |
| [`mf init`](cli/maintenance.md#initialization-and-doctor) | Set up testing directories and schema, synchronize marketplace metadata, and publish standard rankings. |
| [`mf doctor`](cli/maintenance.md#initialization-and-doctor) | Check the guarded testing database, migration history, storage, Python, and PostgreSQL tools. |
| [`mf config`](cli/maintenance.md#application-configuration) | Inspect effective application settings. |
| [`mf engine`](cli/engine.md) | Inspect or build managed Rust executables. |
| [`mf acquisition`](cli/acquisition.md) | Plan, discover, download, import or register raw data, and manage profiles. |
| [`mf storage`](cli/acquisition.md#storage-inspection) | Browse, verify, reconcile, and clean up managed raw archives. |
| [`mf catalog`](cli/catalog.md) | Validate, register, and inspect venues, instruments and specifications. |
| [`mf normalize`](cli/normalize.md) | Prepare or run historical normalization, and recover processed registrations. |
| [`mf datasets`](cli/datasets.md) | Inspect registered primary files, lineage, coverage and publication evidence. |
| [`mf merge`](cli/merge.md) | Prepare and execute file-selected merges and register their results. |
| [`mf markets`](cli/markets.md) | Synchronize instruments, refresh native observations, and manage immutable ranking snapshots. |
| [`mf database`](cli/maintenance.md#database) | Inspect and migrate the testing schema, back up metadata, and restore into disposable databases. |
| [`mf runs`](cli/acquisition.md#run-inspection) | Inspect UUID-based run reports and event logs. |

## Global options

| Option | Behavior |
| --- | --- |
| `-h`, `--help` | Show help for the parser at that position and exit. Running `mf` by itself prints root help. |
| `--version` | Print the installed Forge package version and exit. |
| `--project-root PATH` | Select a checkout containing `forge/pyproject.toml` and `engine/`. The default is `MARKETFORGE_PROJECT_ROOT` if set, then the nearest enclosing checkout. |
| `--config PATH` | Select an application TOML file relative to the caller's working directory. The workspace default is `data/.config/marketforge.toml`. A missing workspace default uses built-in settings. An explicit missing file is an error. |
| `--json` | Print the command result as JSON on stdout where supported. CLI-handled errors are JSON on stderr. Argparse usage errors retain standard usage text. |

Put root flags before the command. Put action flags after their command path, except where a reference explicitly documents inherited options.

```bash
mf --project-root . --json config show
mf --json datasets list --testing --limit 10
```

## Conventions

- Managed names resolve in each command's library, usually with an optional `.toml` suffix. Check the command reference for explicit path support. In examples, uppercase operands such as `FILE_ID`, `NAME`, or `PREVIEW_PATH` stand for values from your workspace or a previous command.
- `--testing` selects the guarded testing database. Commands can still write records, access the network, or remove eligible managed files. A preview or dry run is available only where documented. [Testing credentials and setup](../getting-started/installation.md#postgresql-credentials)
- Normalization and merge `--tui` require an interactive terminal and are incompatible with global `--json`. `--silent` is action-specific. It generally controls progress while preserving results. Some editor actions require an interactive session.
- Exit `0` means the action completed under its stated policy. Check result fields for empty selections, incomplete evidence, or explicitly accepted partial normalization. Exit `1` generally means an operation-level failure, an unaccepted partial result, or a blocked apply. Exit `2` means a parser or handled setup/configuration error. Handled cancellation can return `130`. Processing actions can instead return `1` with an interrupted receipt.
- Raw archive IDs, PostgreSQL file dataset IDs, engine job IDs, and engine publication IDs serve different purposes. Committed publications retain their original manifests and paths. Inspect the reported artifacts under the configured data root. Registration-only recovery uses the committed publication, primary file or execution intent as documented for each processor. [File dataset model](../workflows/datasets.md)

Implementation notes outside the site include `docs/historical/`, `engine/docs/`, and module-local documentation. Use the command reference for current Python CLI syntax.

## File and interface contracts

Use [application settings](cli/maintenance.md#application-configuration), [acquisition and import recipes](../configuration/acquisition.md), [normalization recipes](../configuration/normalization.md), [registration declarations](registration.md) and [merge recipes and restrictions](../workflows/merge.md). [Profile schemas](profiles.md), [runtime policy](runtime.md), [prepared Engine inputs](prepared.md) and [publication formats](publications.md) describe the artifacts exchanged across processing stages.
