## Specification: Consistent MarketForge Terminal Presentation

### Purpose and scope

Use one visual language across MarketForge’s CLI, historical TUI, acquisition progress displays, and terminal-rendered results.

Follow the existing Rust Tokyo Night palette. Keep colors, status meanings, spacing, units, and message structure consistent across Python and Rust presentation boundaries.

This specification concerns application presentation. Documentation and README design remain separate work.

### Authoritative palette

Use the colors defined in:

```text
engine/src/shared/terminal/palette.rs
```

Mirror their exact values in the shared Python presentation infrastructure.

| Token | RGB | Hex | Meaning |
|---|---|---|---|
| `BG` | `26, 27, 38` | `#1a1b26` | TUI background and applicable panels |
| `FG` | `192, 202, 245` | `#c0caf5` | Main text and values |
| `BLUE` | `122, 162, 247` | `#7aa2f7` | Headings, active navigation, operation titles |
| `CYAN` | `125, 207, 255` | `#7dcfff` | Identifiers, configuration references, selected references |
| `GREEN` | `158, 206, 106` | `#9ece6a` | Successful completion and ready outputs |
| `YELLOW` | `224, 175, 104` | `#e0af68` | Warnings, partial outcomes, registration pending |
| `RED` | `247, 118, 142` | `#f7768e` | Failures and invalid configuration |
| `MUTED` | `86, 95, 137` | `#565f89` | Secondary annotations and decorative separators |

Do not scatter color literals through command handlers or screens.

Use muted text sparingly. Essential instructions, actionable paths, errors, and important values must remain readable.

### Semantic styles

Define reusable styles by meaning rather than choosing colors independently in each component.

Examples include:

```text
heading
body
identifier
secondary
success
warning
failure
selection
```

Python CLI renderers and TUI components should consume the same semantic mapping while using their respective rendering mechanisms.

Do not parse Rust source files at application startup to obtain colors.

### Status presentation

Apply the same status meanings throughout the application:

| Outcome | Presentation |
|---|---|
| Complete and registered | Green with an explicit success label |
| Running | Blue or cyan with an explicit activity label |
| Pending or blocked | Neutral styling with an explanatory label |
| Partial completion | Yellow |
| Registration pending | Yellow |
| Failed | Red |
| Cancelled | Yellow with an explicit cancellation label |
| Unavailable or unknown | Neutral or yellow according to context, always explained |

Green must represent the completed application-level outcome.

An engine finishing successfully while registration remains incomplete is registration pending, not complete.

Never rely on color alone. Every status must also have readable text.

### Standard output layouts

Use a small set of predictable layouts:

| Layout | Purpose |
|---|---|
| Compact table | Markets, datasets, runs, profiles, and other collections |
| Label/value details | One dataset, execution, configuration, or installation |
| Progress display | Mapping, acquisition, and execution phases |
| Diagnostic message | Validation errors, warnings, and failures |
| Completion summary | Final outcome, counts, identities, and artifact references |

Ordinary commands should present an operation heading, relevant content, and a concise outcome.

Avoid large startup banners, excessive borders, decorative icons, or repeated branding on every command.

Use restrained spacing and consistent alignment.

### Tables and detail views

Use consistent column names and ordering for the same entities across commands.

Align numeric columns appropriately. Display units in headings or alongside values.

Distinguish missing values from zero. Use a consistent placeholder such as an em dash for unavailable values.

Do not silently truncate identifiers or paths in a way that makes them ambiguous. Provide full values in detail or machine-readable output.

Adapt presentation to terminal width. Wrapping or reducing optional columns is preferable to unreadable overflow.

Large collections should support bounded output through applicable filtering and limit controls.

### Formatting conventions

Centralize formatting for:

- Counts and file totals.
- UTC timestamps and durations.
- Storage sizes and resource budgets.
- Market metrics and their denominations.
- Missing or unknown values.
- Identifiers and paths.

Use the established half-open interval convention when displaying requested coverage:

```text
[start, end)
```

Distinguish declared coverage from observed event bounds.

Do not round away meaningful financial precision or imply that base volume, quote volume, contract volume, and turnover share the same unit.

Human-readable formatting must not alter stored values or machine-readable results.

### Diagnostic messages

Use a consistent diagnostic structure:

```text
Invalid configuration · historical.yaml

steps[2].inputs.dataset
References unknown step "normalize_trdaes".

Available step: normalize_trades
```

Show the affected filename, field path, and line/column when available.

Explain the problem and provide an actionable correction where possible.

Routine configuration mistakes should not display a Python traceback by default. Unexpected internal failures may expose detailed diagnostics through the established debug mechanism.

Do not expose credentials or credential-bearing connection URLs.

### Acquisition completion summaries

Use a consistent count-based summary, for example:

```text
Acquisition · bybit-trades.yaml

Selected     120 files
Reused        35
Downloaded    82
Failed         3

Partial completion · 117/120 files ready
Run          acq-…
Report       …
```

Style the operation heading blue, useful references cyan, ordinary values foreground, and the partial outcome yellow.

This example illustrates presentation only; actual counts must come from the execution result.

### Download progress

Use selected physical archive counts as the primary measure.

Distinguish:

- Reused files.
- Newly downloaded and registered files.
- Failed files.
- Remaining files.
- Registration-pending files.

Calculate readiness as:

```text
Ready files = Reused files + Newly downloaded and registered files
```

Retries must not inflate the selected count or count one archive multiple times.

Do not present an operation as successful merely because all transfers were attempted.

Exact remote file sizes are not required. Do not add size-probing requests for presentation.

Transfer rate or actual bytes received may appear as supplementary information when available.

### Mapping progress

Mapping may not know how many archives will be discovered.

Display useful activity and counts such as completed scopes, completed windows, cached coverage reused, discovered archives, and failed searches.

Do not invent a percentage or ETA when a meaningful total is unknown.

Discovery completion must not be presented as proof of complete market data inside the archives.

### Native engine presentation

Use the existing native engine progress interfaces for normalization, merge, and conversion where supported.

During current-terminal handoff, suspend Forge rendering and input handling before attaching the child application. Restore Forge after completion, failure, or supported cancellation.

Do not let Forge and an engine TUI draw into the same terminal concurrently.

Separate progress windows remain presentation adapters under the existing execution owner.

The future live TUI stays separate. Its placeholder command should use the same CLI diagnostic styling without simulating an implemented interface.

### Terminal background behavior

The historical TUI may use `BG` as its full-screen background.

Ordinary CLI commands must not change the user’s terminal theme or repaint the entire terminal background.

Apply background styling only to appropriate controlled presentation regions.

Color capability limitations must not prevent commands from working. Provide readable plain output when styling is unavailable.

### Color controls

Support:

```bash
marketforge <command> --color auto
marketforge <command> --color always
marketforge <command> --color never
```

Use `auto` by default.

In automatic mode, respect `NO_COLOR` and disable styling for noninteractive output streams.

An explicit `--color` choice takes precedence over automatic detection. Machine-readable output remains unstyled regardless of the color setting.

Detect stdout and stderr capabilities independently where relevant.

### Silent and machine-readable output

`--silent` suppresses progress presentation while retaining the final outcome, errors, durable reports, and registry updates.

For commands supporting `--json`:

- Emit only the defined machine-readable result on stdout.
- Disable banners, progress rendering, colors, and terminal control sequences.
- Keep necessary diagnostics on stderr as plain text.
- Preserve the command’s correct exit status.

Presentation controls must not change execution semantics.

### Shared Python organization

Place reusable terminal presentation infrastructure under:

```text
forge/shared/terminal/palette.py
forge/shared/terminal/theme.py
forge/shared/terminal/formatting.py
```

These modules own colors, semantic styles, and common value formatting.

CLI-specific tables and summaries belong with CLI presentation code. TUI-specific widgets and layouts belong with TUI code.

Application services return structured state and results. They must not embed ANSI styling or presentation-specific strings into persisted outcomes.

Durable JSON reports and structured logs must remain free of terminal escape sequences.

### Acceptance criteria

CLI commands, historical screens, acquisition progress, and terminal results use the agreed palette and status meanings.

Partial outcomes and registration-pending outputs remain visually distinct from completed results.

Output stays readable without color and at supported terminal widths.

Download progress is based on file counts, while mapping handles unknown totals honestly.

Silent mode preserves results and failures. JSON output remains parseable and contains no presentation noise.

Verify meaningful behaviors such as status rendering, redirected output, color controls, machine-readable output, and terminal restoration without relying exclusively on large visual snapshots.
