# Acquisition commands

Recipes default to ascending name and source capabilities default to ascending identity. See [collection sorting](sorting.md).

Discover, plan, download, inspect and edit managed acquisition recipes. [Workflow and prerequisites](../../workflows/acquisition/historical-data.md) · [Shared CLI behavior](index.md)


## Synopsis and arguments

### `mf acquisition plan`

```text
mf acquisition plan [-h] [--testing | --production] --cache-only
                                    [--verify-local] [--limit LIMIT]
                                    configuration_name
```

| Argument or option | Parser contract |
| --- | --- |
| `configuration_name` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--cache-only` | Required. No HTTP requests, registry writes or file changes. |
| `--verify-local` | Default `false`. Read-only integrity verification of the bounded plan sample. |
| `--limit` | Default `100`.  Integer. |

### `mf acquisition list`

```text
mf acquisition list [-h]
```

### `mf acquisition sources`

```text
mf acquisition sources [-h]
```

Lists adapter-supported instrument types, market categories, data types and scopes. The command reads adapter definitions locally. See [historical sources](../acquisition-sources.md) for discovery constraints.

### `mf acquisition new`

```text
mf acquisition new [-h] configuration_name
```

| Argument or option | Parser contract |
| --- | --- |
| `configuration_name` | Required. Saved configuration name, optionally ending in .toml. |

### `mf acquisition edit`

```text
mf acquisition edit [-h] configuration_name
```

| Argument or option | Parser contract |
| --- | --- |
| `configuration_name` | Required. Saved configuration name, optionally ending in .toml. |

### `mf acquisition validate`

```text
mf acquisition validate [-h] configuration_name
```

| Argument or option | Parser contract |
| --- | --- |
| `configuration_name` | Required. Saved configuration name, optionally ending in .toml. |

### `mf acquisition map`

```text
mf acquisition map [-h] [--testing | --production] [--refresh]
                                   [--requests-per-second REQUESTS_PER_SECOND]
                                   [--silent]
                                   configuration_name
```

| Argument or option | Parser contract |
| --- | --- |
| `configuration_name` | Required. Saved acquisition configuration name. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--refresh` | Default `false`. Force authoritative discovery, superseding overlapping cached coverage. |
| `--requests-per-second` | Optional. Number. Positive request rate override, capped by the adapter's upstream budget. |
| `--silent` | Default `false`. Suppress progress; retain final results and failures. Global --json emits JSON results. |

### `mf acquisition download`

```text
mf acquisition download [-h] [--testing | --production] [--refresh]
                                        [--requests-per-second REQUESTS_PER_SECOND]
                                        [--silent]
                                        configuration_name
```

| Argument or option | Parser contract |
| --- | --- |
| `configuration_name` | Required. Saved acquisition configuration name. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |
| `--refresh` | Default `false`. Force authoritative discovery, superseding overlapping cached coverage. |
| `--requests-per-second` | Optional. Number. Positive request rate override, capped by the adapter's upstream budget. |
| `--silent` | Default `false`. Suppress progress; retain final results and failures. Global --json emits JSON results. |

## Acquisition Command Reference

The following table summarizes the exchange archive-acquisition commands.

| Command | Contacts exchange | Writes registry | Downloads archives |
|---|---|---|---|
| `new` | No | No | No |
| `edit` | No | No | No |
| `validate` | No | No | No |
| `list` | No | No | No |
| `sources` | No | No | No |
| `map` | When discovery is needed | Yes | No |
| `plan --cache-only` | No | No | No |
| `download` | When discovery or downloading is needed | Yes | Yes, when required |

`new` and `edit` can create or modify local configuration files. The table's registry column refers specifically to PostgreSQL changes.

### Configuration inventory and validation

```bash
mf acquisition list | cpout
mf acquisition validate getting_started | cpout
```

The inventory reads the managed TOML files locally and shows request counts and
requested periods in UTC. Malformed recipes remain visible as invalid entries.
Validation checks the named file's structure and reports the failing field or
TOML location when possible. These commands require no database or network
access. Source support and archive availability are checked during discovery.

### Mapping and download options

Both `map` and `download` accept these options:

| Option | Meaning |
|---|---|
| `--testing`, `--production` | Choose the verified registry. Production is the default. |
| `--refresh` | Check the exchange again rather than reusing fresh discovery results. |
| `--requests-per-second RATE` | Set the requested HTTP request rate, subject to provider limits. |
| `--silent` | Hide progress updates while keeping the final result and failures. |

Examples:

```bash
mf acquisition map getting_started --testing --refresh
```

```bash
mf acquisition download getting_started \
    --testing \
    --requests-per-second 0.5
```

```bash
mf acquisition download getting_started \
    --testing \
    --silent
```

### Request-rate limits

MarketForge applies request-rate limits when contacting exchanges.

This helps avoid overwhelming providers or exceeding their permitted request rates.

You can lower the requested rate using `--requests-per-second`.

For example:

```bash
mf acquisition map getting_started \
    --testing \
    --requests-per-second 0.5
```

This requests a pacing rate of half a request per second, subject to the implementation's provider controls.

The rate must be a positive, finite number.

If the requested rate exceeds a provider's configured limit, that limit still applies.

Retries, redirects, and provider cooldowns can also affect how quickly discovery or downloading proceeds.

See the [HTTP configuration reference](../configuration/acquisition.md#application-httpstorage-settings) for the current limits and application settings.

### Cache-only planning options

The planning command uses:

```text
mf acquisition plan NAME --testing --cache-only [--verify-local] [--limit N]
```

| Option | Meaning |
|---|---|
| `--testing` | Select the guarded testing registry. |
| `--cache-only` | Use stored information without making network requests or registry changes. |
| `--verify-local` | Read and hash local files in the selected sample. |
| `--limit N` | Set the maximum number of sampled archive entries. Default: 100. |

`--cache-only` is required. The registry defaults to production. Pass `--testing` to inspect the testing registry.

Planning is a read-only inspection tool, not a saved download job or a complete estimate of future transfer size.

### JSON output

Use the global `--json` option when you need machine-readable results:

```bash
mf --json acquisition map getting_started --testing
```

```bash
mf --json acquisition download getting_started --testing
```

The human output summarizes request outcomes, coverage evidence, and the retained
report location. JSON mode emits the full operation result for scripts. The list
JSON continues to return configuration names. Validation JSON returns the
structural result and parsed configuration.

```bash
mf --json acquisition list | cpout
mf --json acquisition validate getting_started | cpout
mf --json runs report RUN_ID --testing | cpout
```

The `--json` option belongs before the command name.

Interactive editing does not provide a JSON result.

---

## Understanding Acquisition Results

Acquisition records two different kinds of information:

**Discovery information** describes the archive files the exchange exposes.

**Local file information** describes files that MarketForge has downloaded and stored.

These are related, but they are not the same thing.

For example, mapping may find an archive even though it has never been downloaded.

Likewise, a previously downloaded file may still exist locally even when the original discovery result is no longer fresh.

### Successful discovery

A successful mapping operation means MarketForge completed discovery for the applicable request.

It may find many files, a few files, or no matching files.

It does not establish that every market event is present inside those archives.

### Successful download

A completed download means the operation finished its applicable download and reuse work.

It does not mean the files have already been normalized or that order-book continuity has been verified.

Normalization is responsible for interpreting records and evaluating the relevant processing and data-integrity conditions.

### Failed or interrupted operations

Network problems, provider errors, missing files, and local filesystem problems can interrupt an acquisition.

MarketForge keeps run information so you can investigate what happened.

A partially completed operation can leave successfully downloaded files available for later reuse.

Do not assume an unsuccessful acquisition automatically removes previously completed downloads.

For details about retained operations and recovery, see [Managing acquisitions](../../workflows/acquisition/managing-acquisitions.md).

---

## Command Exit Codes

MarketForge uses exit codes to indicate whether an operation succeeded or encountered a problem.

| Code | Meaning |
|---|---|
| `0` | The command completed successfully. Inspect the result for request coverage and file outcomes. |
| `1` | One or more acquisition operations failed. Some completed files may remain available. |
| `2` | A command, configuration, filesystem, database, or setup error was reported. |
| `130` | The command was interrupted through the handled cancellation path. |

A successful empty mapping result can return zero.

A zero exit code should not be interpreted as proof that the requested historical market data is complete.

When investigating an unsuccessful run, inspect the reported status, individual request outcomes, and saved run report rather than relying only on the exit code.

---

## Storage inspection

Use [raw storage commands](storage.md) to inspect registered archives, date filters and file coverage.

## Run inspection

Use [run evidence commands](runs.md) to inspect retained fetch and processing runs.

## Cleanup reference

Use the [cleanup options and effects](storage.md) and the [managed storage workflow](../../workflows/acquisition/managing-acquisitions.md).

## Mapping and download results

Mapping discovers remote archive identities. The final report shows each
requested scope, including family and base-coin scopes, and identifies reused
discovery scans as **Cached** and fresh scans as **Refreshed**. A cached discovery does not establish that the
files are present locally. A successful empty scan remains an empty result.

Download reports completed, reused, failed and cancelled file outcomes.
The registered local archive count counts distinct retained local IDs. Bytes
in completed downloads use recorded size evidence. Unknown measurements display
as **Unknown**. Nominal archive intervals describe the retained objects and
do not establish uninterrupted market-data coverage.

The terminal shows at most 20 request rows, with incomplete requests first.
Read the retained report for all requests and the event log for individual
archive outcomes. A partial acquisition remains a nonzero command outcome.
Live progress is emitted separately from the final result.

```bash
mf acquisition map getting_started --testing | cpout
mf --json acquisition map getting_started --testing | cpout
mf --json runs list --testing --operation acquisition --limit 10 | cpout
mf runs report RUN_ID --testing | cpout
```

Use an actual ID from the acquisition result. The PostgreSQL record, retained
report and event metadata use the same `acquisition-<uuid>` identity.
[Execution history and recovery](runs.md)
