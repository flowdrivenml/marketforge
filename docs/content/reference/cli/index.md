# Forge command index

[Collection sorting](sorting.md) lists the supported fields, aliases, and defaults for each browsing command.

`mf` and `marketforge` use the same parser. Run `mf` or `mf --help` to browse the
command groups. Follow a family into its nested commands, then use `-h` or
`--help` at the action you want. Action help shows operands, choices, required
flags, defaults and mutually exclusive options from the installed parser.

| Root help group | Command families and references |
| --- | --- |
| Getting Started | [Interactive workspace](forge.md) · [Initialization and diagnostics](maintenance.md) |
| Data Engineering | [Acquisition](acquisition.md) · [Import, profiles and registration](registration.md) · [Normalization](normalize.md) · [Merge](merge.md) · [Conversion](convert.md) · [Pipeline](pipeline.md) |
| Data & Markets | [Datasets](datasets.md) · [Storage](storage.md) · [Markets and rankings](markets.md) · [Catalogue](catalog.md) |
| Operations & System | [Runs](runs.md) · [Engine](engine.md) · [Database and configuration](maintenance.md) · [Run and dataset maintenance](../../maintenance/publications.md) |

```bash
mf --help | cpout
mf acquisition import -h | cpout
mf markets rank --help | cpout
mf normalize prepare --help | cpout
```

Help works outside a workspace and exits before command execution. It uses the
Tokyo Night palette when terminal and application settings permit it.
Redirected output is plain text in automatic color mode. The
[`presentation.color` setting](../configuration/application.md) can force or
disable color.

## Global options

| Option | Behavior |
| --- | --- |
| `-h`, `--help` | Show help for the parser at that position and exit. Running `mf` by itself prints root help. |
| `--version` | Print the installed Forge package version and exit. |
| `--project-root PATH` | Select a checkout containing `forge/pyproject.toml` and `engine/`. The default is `MARKETFORGE_PROJECT_ROOT` if set, then the nearest enclosing checkout. |
| `--config PATH` | Select an application TOML file relative to the caller's working directory. The workspace default is `data/.config/forge/marketforge.toml`. A missing workspace default uses built-in settings. An explicit missing file is an error. |
| `--json` | Print the command result as JSON on stdout where supported. CLI-handled errors are JSON on stderr. Argparse usage errors retain standard usage text. |

Put root flags before the command. Put action flags after their command path, except where a reference explicitly documents inherited options.

```bash
mf --project-root . --json config show | cpout
mf --json datasets list --testing --limit 10
```

## Database environments

Production is the default for database-backed commands. Actions that expose
`--testing` and `--production` accept only one at a time. Actions that require
an explicit selection show it in their usage. Some maintenance actions are
restricted to testing.

```bash
mf database status | cpout
mf database status --testing | cpout
mf database status --production | cpout
```

## Conventions

- Managed names resolve in each command's library, usually with an optional `.toml` suffix. Check the command reference for explicit path support. In examples, uppercase operands such as `FILE_ID`, `NAME`, or `PREVIEW_PATH` stand for values from your workspace or a previous command.
- `--testing` selects the guarded testing database. Commands can still write records, access the network, or remove eligible managed files. A preview or dry run is available only where documented. [Testing credentials and setup](../../getting-started/installation.md#postgresql-credentials)
- Normalization and merge `--tui` require an interactive terminal and are incompatible with global `--json`. `--silent` is action-specific. It generally controls progress while preserving results. Some editor actions require an interactive session.
- Exit `0` means the action completed under its stated policy. Check result fields for empty selections, incomplete evidence, or explicitly accepted partial normalization. Exit `1` generally means an operation-level failure, an unaccepted partial result, or a blocked apply. Exit `2` means a parser or handled setup/configuration error. Handled cancellation can return `130`. Processing actions can instead return `1` with an interrupted receipt.
- Raw archive IDs, PostgreSQL file dataset IDs, engine job IDs, and engine publication IDs serve different purposes. Committed publications retain their original manifests and paths. Inspect the reported artifacts under the configured data root. Registration-only recovery uses the committed publication, primary file or execution intent as documented for each processor. [File dataset model](../../workflows/inspection/datasets.md)

Handled errors in JSON mode use `{"schema_version":1,"error":{"message":"…"}}` on stderr. The command's successful or partial result is a separate stdout document when the handler supports JSON. Some failures return an operation report on stdout with a nonzero code. Read both channels and the process status. The command references describe result fields as implemented, subject to the explicitly versioned artifact contracts.
