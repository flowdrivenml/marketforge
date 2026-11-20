# Historical Data Acquisition

MarketForge can download historical market data from multiple cryptocurrency exchanges, including Bybit, Binance, OKX, Bitget, and Gate.io.

These exchanges provide historical trades, order-book data, and other market information. However, each exchange maintains its own data archives, with different file formats, naming conventions, and availability.

**MarketForge does not host these historical archives itself.** Instead, it connects to the exchanges, finds the files you need, downloads them, and keeps track of what has been downloaded.

This process is called **historical data acquisition**.

## How acquisition works

Suppose you want to download BTCUSDT perpetual trades and order-book data from Bybit for October 27, 2026.

There are three things MarketForge needs to know:

- **What data you want:** The exchange, instrument, market type, and data type.
- **When you want it:** The historical date or date range.
- **Whether the data exists:** Which matching files the exchange actually provides.

The first two are specified by you. The third requires checking the exchange.

MarketForge handles this through three main stages:

| Stage | Purpose |
|---|---|
| **Configure** | Describe the historical data you want to acquire. |
| **Map** | Check which matching archive files the exchange provides. |
| **Download** | Download the matching files and save them locally. |

A separate `plan` command lets you inspect previously discovered files without contacting the exchange again.

```text
           Acquisition Recipe
                  |
                  v
            Map Availability
                  |
                  v
          Exchange Archives
                  |
                  v
             Download
                  |
                  v
          Managed Raw Files
                  |
                  v
             Normalize
```

You do not always need to run every stage separately. For example, `download` performs mapping automatically when needed.

However, understanding these stages gives you more control over what MarketForge downloads, especially when working with large amounts of historical data.

This guide explains the available acquisition commands, how they work, and when you should use them.

---

## 1. Create an Acquisition Recipe

Before MarketForge can search for historical files, it needs to know what you are looking for.

An **acquisition recipe** is a TOML configuration file that describes your selection.

It tells MarketForge which exchange to search, which instruments and data types to select, and which historical period to use.

For example, a recipe can request:

- Bybit BTCUSDT perpetual trades for one day.
- BTCUSDT order-book data for the same day.
- Several instruments across a longer period.
- Multiple data types in one acquisition run.

You can save these selections and reuse them whenever necessary.

### Create a new recipe

Run the following command from the MarketForge project root:

```bash
mf acquisition new bybit-btc-20261027
```

This creates a new configuration file:

```text
data/.config/acquisition/bybit-btc-20261027.toml
```

The file initially contains an editable template.

Open it in your preferred editor:

```bash
nvim data/.config/acquisition/bybit-btc-20261027.toml
```

Alternatively, MarketForge provides an interactive editing command:

```bash
mf acquisition edit bybit-btc-20261027
```

The `edit` command opens the existing recipe using the configured editor and checks its structure after you save and close it.

### Example: Bybit BTC perpetual trades and depth

Replace the template with the following configuration:

```toml
version = 1

[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-28T00:00:00Z

[[requests]]
exchange = "bybit"
instrument_type = "perpetual"
market_category = "linear"
data_type = "trades"

[requests.scope]
kind = "instrument"
identifier = "BTCUSDT"

[[requests]]
exchange = "bybit"
instrument_type = "perpetual"
market_category = "linear"
data_type = "order_book_l2"

[requests.scope]
kind = "instrument"
identifier = "BTCUSDT"
```

This recipe requests two datasets from Bybit:

| Selection | Value |
|---|---|
| Exchange | Bybit |
| Instrument | BTCUSDT |
| Instrument type | Perpetual |
| Market category | Linear |
| Historical period | October 27, 2026 |
| Data types | Trades and L2 order book |

Both requests use the same date range.

The first selects executed trades. The second selects order-book data.

### Understanding the configuration

Let's examine the important fields.

#### Historical interval

```toml
[interval]
start = 2026-10-27T00:00:00Z
end = 2026-10-28T00:00:00Z
```

The interval tells MarketForge which historical period you are interested in.

The start is included, while the end is excluded. This is commonly written as `[start, end)`.

In this example, the requested period covers October 27 in UTC.

**Important:** Acquisition selects archive files that overlap the requested period.

It does not download individual trades or trim an archive to the exact timestamps you requested.

For example, if an exchange publishes one archive per day, requesting a single hour within that day may still require downloading the entire daily file.

Different exchanges also organize archives differently. Some use hourly files, others daily files, and some use monthly archives.

MarketForge accounts for the supported exchange-specific archive calendars when searching for files.

#### Exchange and instrument

```toml
exchange = "bybit"
instrument_type = "perpetual"
market_category = "linear"
```

These fields identify the market you want to download.

The distinction between instrument type and market category matters.

For example, BTCUSDT on Bybit can refer to more than one market.

| Instrument | Type | Category |
|---|---|---|
| BTCUSDT spot | `spot` | `spot` |
| BTCUSDT USDT perpetual | `perpetual` | `linear` |
| BTCUSD inverse perpetual | `perpetual` | `inverse` |

A symbol alone is not always enough to identify the correct instrument.

By declaring these fields separately, you tell MarketForge exactly which type of market to search.

#### Data type

```toml
data_type = "trades"
```

This selects executed trades.

For order-book data, use:

```toml
data_type = "order_book_l2"
```

Not every exchange provides every data type through its historical archive service.

See the [supported sources](../../workflows/acquisition/historical-data.md#supported-sources) for the combinations currently supported by MarketForge.

#### Instrument selection

```toml
[requests.scope]
kind = "instrument"
identifier = "BTCUSDT"
```

The scope tells MarketForge what to search for within the exchange's archive system.

For Bybit BTCUSDT perpetual data, we select one specific instrument.

Some exchanges organize archives around groups of instruments instead. For example, a provider may publish several related futures contracts together.

MarketForge supports different selection types for these cases, including concrete instruments, instrument families, and base-asset groups where the provider supports them.

The correct selection depends on how the exchange organizes its historical files.

See the [acquisition configuration reference](../../configuration/acquisition.md) for the complete field definitions and exchange-specific options.

<!-- SCREENSHOT PLACEHOLDER: acquisition-recipe.png

Suggested image:
The completed Bybit acquisition recipe open in an editor.

Purpose:
Help new users recognize the TOML structure and the two requests.

Suggested alt text:
"MarketForge acquisition recipe selecting Bybit BTCUSDT perpetual trades and L2 depth."

-->

---

## 2. Validate the Recipe

Before contacting an exchange, check that your configuration is written correctly.

Use:

```bash
mf acquisition validate bybit-btc-20261027
```

MarketForge reads the TOML file and checks that its structure follows the acquisition configuration rules.

For example, it checks that required fields are present and that values such as the historical interval are valid.

If the configuration is accepted, MarketForge reports that the structure is valid.

### What does validation actually check?

**Validation checks the recipe, not the exchange.**

This is an important distinction.

A valid recipe does not necessarily mean the exchange supports the selected data or has files available for the requested dates.

For example, you could write a structurally valid request for an instrument that has no historical archives.

The recipe could pass validation, but mapping might find no matching files.

Think of validation as checking that MarketForge understands your request.

Mapping, which comes next, checks what the exchange provides.

| Check | `validate` | `map` |
|---|---|---|
| TOML structure | Yes | Yes, as part of loading |
| Required configuration fields | Yes | Yes, as part of loading |
| Contact the exchange | No | Yes, when discovery is needed |
| Check supported archive selection | Not fully | Yes, through the provider adapter |
| Find matching archive files | No | Yes |
| Download historical files | No | No |

Validation does not require a database connection or internet access.

---

## 3. Find Available Historical Data

Now that we have a valid recipe, we need to find out which historical files the exchange provides.

### Why is mapping necessary?

MarketForge connects to external exchanges rather than maintaining its own complete historical archive collection.

Each exchange decides which files it publishes.

For example, Bybit might provide BTCUSDT trades for a particular date, while its order-book archive could have different availability.

Similarly, a newly listed instrument may have no files before its listing date, and an exchange may change or remove older archives.

**We cannot assume a historical file exists just because we request it.**

MarketForge therefore needs a way to discover the files that match your selection.

This process is called **mapping**.

Mapping searches the exchange's historical archive system, identifies matching files, and saves the results.

It does not download the contents of those files.

### Run mapping

Use:

```bash
mf acquisition map bybit-btc-20261027 --testing
```

MarketForge loads the acquisition recipe and checks the corresponding Bybit archive listings.

For each request, it attempts to identify the available files that match the selected instrument, data type, and historical interval.

The discovery results are saved in MarketForge's PostgreSQL registry.

This means later commands can reuse the information without always repeating the same search.

### What should you look for?

For our example, the expected discovery concerns two types of files:

**Trade archives**

These contain the executed trades for BTCUSDT.

**Order-book archives**

These contain the exchange's historical order-book recordings for the selected perpetual market.

Check that the mapping result covers both requests.

A successful mapping operation can also return an empty selection. That means no matching files were found through the discovery process; it does not prove the instrument never traded or that another source cannot provide the data.

<!-- SCREENSHOT PLACEHOLDER: acquisition-map.png

Suggested image:
Terminal output after running:
mf acquisition map bybit-btc-20261027 --testing

Capture:
- Exchange and selected interval
- Trades request
- Depth request
- Discovered file counts
- Final discovery status

Purpose:
Show what a real mapping result looks like.

Suggested alt text:
"MarketForge mapping Bybit historical trade and order-book archives."

-->

### Does mapping download anything?

No.

Mapping may contact the exchange and write information to the local registry, but it does not download the historical archive contents.

This makes it useful when you want to investigate the availability of a large dataset before downloading it.

For example, you could map several months of historical data to discover which files are listed before deciding whether to download the entire selection.

### Reusing previous mapping results

MarketForge keeps successful discovery results so that repeated operations do not always need to contact the exchange.

A fresh successful mapping result can be reused for **six hours**.

This includes successful searches that found no matching files.

Failed, incomplete, or cancelled searches are not treated as complete discovery results.

The six-hour period applies to discovery information, not to downloaded files.

A previously downloaded file can remain reusable independently of whether its discovery result is still fresh.

### Force a new availability check

Sometimes you may want to contact the exchange again even if MarketForge already has recent discovery results.

Use:

```bash
mf acquisition map bybit-btc-20261027 --testing --refresh
```

The `--refresh` option asks MarketForge to perform a new discovery instead of relying on fresh cached results.

This is useful when:

- You expect the exchange to have published new files.
- You want to check whether previously missing files have appeared.
- You suspect the exchange's archive listings have changed.

A successful refresh updates the relevant discovery information.

**Refreshing mapping does not force downloaded files to be downloaded again.**

Existing local files are handled separately by the download and verification logic.

---

## 4. Review the Download Plan

After mapping, you may want to check which files are already stored locally and which still need to be downloaded.

For this, MarketForge provides a cache-only planning command.

### Why use a plan?

Suppose you downloaded several days of trades last week.

Today, you want to download the same period again, together with order-book data.

You probably do not want MarketForge to download every file a second time.

A plan can help you inspect the previously discovered selection and see which sampled files appear ready for reuse.

### Inspect the cached selection

Run:

```bash
mf acquisition plan bybit-btc-20261027 --testing --cache-only
```

This command uses information already saved in the registry.

It does not contact the exchange, download files, or modify the registry.

A cache-only plan is useful when you want to inspect the current stored information without starting another network operation.

### Check existing files

You can also ask MarketForge to verify the local files included in the plan's sample:

```bash
mf acquisition plan bybit-btc-20261027 \
    --testing \
    --cache-only \
    --verify-local
```

With `--verify-local`, MarketForge reads and hashes sampled local files to check whether they can be reused.

This can help identify files that are missing or have changed since they were originally downloaded.

### Understanding plan results

A plan may report states such as:

| State | Meaning |
|---|---|
| `download_required` | The sampled archive needs a local download. |
| `registered_unverified` | A local record exists, but this plan has not verified it for reuse. |
| `verified_reusable` | The local file was verified and can be reused. |
| `missing_or_corrupt` | The expected local file is missing or failed verification. |

These states describe the files examined by the plan.

They are not guarantees about every archive in a large acquisition request.

### Limit the number of files inspected

By default, planning samples up to 100 archives.

You can change that limit:

```bash
mf acquisition plan bybit-btc-20261027 \
    --testing \
    --cache-only \
    --verify-local \
    --limit 10
```

This examines at most ten archive entries.

The accepted limit is between 1 and 10,000.

**The limit only affects the planning sample.**

It does not change your saved acquisition recipe or restrict a later download to ten files.

If you need a smaller download, change the selection in the acquisition recipe itself.

### What if the plan says availability is unresolved?

A cache-only plan cannot discover files that have never been mapped.

If the registry does not contain fresh discovery information for a request, the plan may report that its availability is unresolved.

This does not mean the exchange has no matching data.

Run `map` to check the exchange and update the saved discovery information.

<!-- SCREENSHOT PLACEHOLDER: acquisition-plan.png

Suggested image:
Terminal or JSON output showing a cache-only acquisition plan.

Capture:
- Selected requests
- Sampled archive count
- Download-required or reusable states
- Any unresolved selections

Purpose:
Demonstrate how users can inspect a selection before downloading.

Suggested alt text:
"MarketForge acquisition plan showing which historical archives require downloading."

-->

---

## 5. Download Historical Archives

Once you have configured your selection, you can download the historical files.

Use:

```bash
mf acquisition download bybit-btc-20261027 --testing
```

MarketForge checks the matching archives and downloads the files that are not already available as verified local copies.

The downloaded files are stored in MarketForge's managed raw-data storage.

### What happens during a download?

The acquisition workflow performs several operations.

First, it resolves the requested archives using previously saved discovery results or performs new mapping when needed.

Next, it checks whether a suitable local copy already exists.

For archives that still need downloading, MarketForge requests the files from the exchange and stores them locally.

It records information about the downloaded files so they can be found and reused later.

This includes information used to identify the source and verify the local copy.

The goal is to avoid unnecessary repeated downloads while keeping track of where the data came from.

### You can download without running `map` first

Mapping is useful, but it is not a mandatory separate step.

For example:

```bash
mf acquisition download bybit-btc-20261027 --testing
```

If MarketForge needs fresh discovery information, it performs mapping as part of the download workflow.

You have two valid approaches:

| Workflow | When to use it |
|---|---|
| `map` → `download` | You want to review exchange availability before downloading. |
| `download` directly | You already know what you want and prefer one command. |

Both use the same acquisition recipe.

### Reusing existing downloads

MarketForge does not need to download a file again when an existing local copy passes its reuse checks.

For example, if your acquisition contains ten files and seven already exist as verified copies, the workflow can reuse those seven and download the remaining three.

This is particularly useful when working with large historical datasets or rerunning acquisitions after interrupted work.

Local reuse depends on verification of the actual stored file, not merely on the existence of a database record.

A missing or changed local file must not be treated as a valid reusable copy simply because it was downloaded previously.

### Downloading larger periods

Historical market-data files can be large, especially order-book archives.

Before requesting several weeks or months, consider checking the provider's archive organization and mapping the selected period.

A short requested interval does not necessarily correspond to a small download.

For example, a provider that publishes daily order-book archives may require an entire day's file even if you only need a few minutes of data.

The download size depends on the actual source files, not simply on the duration written in the recipe.

<!-- SCREENSHOT PLACEHOLDER: acquisition-download-progress.png

Suggested image:
The MarketForge acquisition progress display while downloading
Bybit BTCUSDT perpetual trades and depth.

Capture:
- Source or request identity
- Download progress
- File counts
- Bytes transferred, if displayed

Purpose:
Show the acquisition experience while files are downloading.

Suggested alt text:
"MarketForge downloading historical BTCUSDT trade and order-book archives from Bybit."

-->

### Inspect the download result

When the operation finishes, MarketForge displays a summary of the acquisition.

The result includes information about the operation's status, file outcomes, and the saved report location.

For more detailed inspection, you can request JSON output:

```bash
mf --json acquisition download bybit-btc-20261027 --testing
```

The global `--json` option must appear before `acquisition`.

JSON output includes the operation status, run identifier, file and request counts, request outcomes, and paths to retained reports or events.

Use these reports when you need to understand partial or failed downloads.

Do not assume a successful command exit means every requested historical event exists. Acquisition checks archive files and local copies; it does not prove the completeness of their market-data contents.

<!-- SCREENSHOT PLACEHOLDER: acquisition-download-summary.png

Suggested image:
The final acquisition result after successfully downloading
the selected Bybit trade and depth archives.

Capture:
- Overall status
- Completed or reused files
- Failed files, if any
- Report location

Purpose:
Show users how to recognize the end of an acquisition.

Suggested alt text:
"Completed MarketForge acquisition showing downloaded and reused historical archives."

-->

---

## 6. Manage Saved Acquisition Recipes

Acquisition recipes are ordinary TOML files stored under:

```text
data/.config/acquisition/
```

You can create several recipes for different exchanges, instruments, or periods.

For example:

```text
data/.config/acquisition/
├── bybit-btc-20261027.toml
├── bybit-eth-trades.toml
├── okx-btc-futures.toml
└── gate-spot-depth.toml
```

The names are illustrative.

Recipes make it possible to repeat the same selection without entering every parameter on the command line.

### Available recipe commands

| Command | Purpose |
|---|---|
| `mf acquisition new NAME` | Create a new acquisition recipe template. |
| `mf acquisition edit NAME` | Open an existing recipe in the configured editor and validate it after editing. |
| `mf acquisition validate NAME` | Check the structure of an existing recipe. |
| `mf acquisition list` | Show saved acquisition recipe names. |

For example, to list your saved recipes:

```bash
mf acquisition list
```

To validate one:

```bash
mf acquisition validate bybit-btc-20261027
```

To edit it:

```bash
mf acquisition edit bybit-btc-20261027
```

These commands manage configuration files. They do not perform archive discovery or download historical data.

### Recipe names and paths

Acquisition commands identify saved recipes by name.

For example:

```bash
mf acquisition map bybit-btc-20261027 --testing
```

MarketForge looks for the corresponding TOML file under `data/.config/acquisition/`.

You can also supply the name with the `.toml` suffix.

Recipe names begin with a letter or digit and may otherwise contain letters, digits, hyphens, and underscores.

The commands do not accept arbitrary filesystem paths in place of saved recipe names.

### Creating an existing recipe again

`acquisition new` refuses to overwrite an existing file.

If you need to change an existing recipe, edit that file instead.

This protects saved selections from accidental replacement.

---

## 7. Acquisition Command Reference

The following table summarizes the exchange archive-acquisition commands.

| Command | Contacts exchange | Writes registry | Downloads archives |
|---|---|---|---|
| `new` | No | No | No |
| `edit` | No | No | No |
| `validate` | No | No | No |
| `list` | No | No | No |
| `map` | When discovery is needed | Yes | No |
| `plan --cache-only` | No | No | No |
| `download` | When discovery or downloading is needed | Yes | Yes, when required |

`new` and `edit` can create or modify local configuration files. The table's registry column refers specifically to PostgreSQL changes.

### Mapping and download options

Both `map` and `download` accept these options:

| Option | Meaning |
|---|---|
| `--testing` | Use the configured, identity-verified testing database. Required for these operations. |
| `--refresh` | Check the exchange again rather than reusing fresh discovery results. |
| `--requests-per-second RATE` | Set the requested HTTP request rate, subject to provider limits. |
| `--silent` | Hide progress updates while keeping the final result and failures. |

Examples:

```bash
mf acquisition map bybit-btc-20261027 --testing --refresh
```

```bash
mf acquisition download bybit-btc-20261027 \
    --testing \
    --requests-per-second 0.5
```

```bash
mf acquisition download bybit-btc-20261027 \
    --testing \
    --silent
```

### Request-rate limits

MarketForge applies request-rate limits when contacting exchanges.

This helps avoid overwhelming providers or exceeding their permitted request rates.

You can lower the requested rate using `--requests-per-second`.

For example:

```bash
mf acquisition map bybit-btc-20261027 \
    --testing \
    --requests-per-second 0.5
```

This requests a pacing rate of half a request per second, subject to the implementation's provider controls.

The rate must be a positive, finite number.

If the requested rate exceeds a provider's configured limit, that limit still applies.

Retries, redirects, and provider cooldowns can also affect how quickly discovery or downloading proceeds.

See the [HTTP configuration reference](../../configuration/acquisition.md#application-httpstorage-settings) for the current limits and application settings.

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

Both `--testing` and `--cache-only` are required.

Planning is a read-only inspection tool, not a saved download job or a complete estimate of future transfer size.

### JSON output

Use the global `--json` option when you need machine-readable results:

```bash
mf --json acquisition map bybit-btc-20261027 --testing
```

```bash
mf --json acquisition download bybit-btc-20261027 --testing
```

JSON results can be useful for scripts, automation, and inspecting detailed run information.

The `--json` option belongs before the command name.

Interactive editing does not provide a JSON result.

---

## 8. Understanding Acquisition Results

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

## 9. Common Questions

### Why doesn't MarketForge already know which files exist?

MarketForge does not control the exchanges' historical archive collections.

It needs to contact the supported provider to learn which files are currently listed for a selection.

Successful discovery results are cached to avoid repeating unnecessary searches.

### Do I always need to run `map` before `download`?

No.

`download` performs mapping automatically when necessary.

Use `map` separately when you want to inspect availability before transferring historical files.

### Does mapping guarantee the files can be downloaded?

No.

Mapping identifies matching archive files through the provider's discovery interface.

A later download can still fail because of network problems, changes on the provider's side, or local errors.

### Can I map a large period without downloading it?

Yes.

Mapping does not download archive contents.

However, it may still require network requests and database writes to discover and save the archive listings.

### Does `--refresh` download everything again?

No.

`--refresh` forces new discovery.

It does not automatically replace verified local archive copies.

### Does a successful download mean my dataset is complete?

Not necessarily.

Acquisition manages archive files.

It does not prove that every trade or order-book event occurred in the downloaded recordings.

Processing and integrity checks provide additional evidence about the contents, but complete historical coverage may still require independent source guarantees.

### What happens if an archive is already downloaded?

MarketForge can reuse the file if it passes the required local verification.

It should not rely only on the existence of a filename or registry entry.

### Can I acquire trades and depth together?

Yes.

An acquisition recipe can contain multiple requests, including trades and L2 depth for the same instrument.

MarketForge selects the relevant archive files for each request.

These remain separate source data types and are processed according to their respective formats.

### Can I request only a few minutes?

You can define a short interval, but acquisition operates on the exchange's published archive files.

If a provider publishes one file per day, a five-minute request may still select a complete daily archive.

The recipe interval does not automatically trim downloaded file contents.

### Why does MarketForge require `--testing`?

The current acquisition workflow requires an explicitly configured, verified testing database.

This prevents acquisition operations from silently falling back to another registry.

**`--testing` does not mean the command is a dry run.**

Mapping can contact exchanges and update the testing registry. Downloading can transfer and store real files.

See [Installation and database setup](../../getting-started/installation.md).

---

## 10. What Comes After Acquisition?

Once the historical files have been downloaded, they become part of MarketForge's managed raw-data collection.

You can then inspect them, prepare normalization jobs, and process the data into a consistent format.

The main next steps are:

| Next task | Where to go |
|---|---|
| Inspect downloaded archives | [Managing acquisitions](../../workflows/acquisition/managing-acquisitions.md) |
| Normalize historical data | [Normalize registered data](../../workflows/normalize.md) |
| Inspect normalized Parquet datasets | [Dataset inspection](../../workflows/datasets.md) |
| Bring your own recordings | [Bring your own historical data](../../examples/bring-your-own-data/index.md) |
| Create custom source profiles | [Profile authoring](../../workflows/catalogue/profiles.md) |

### Acquisition and storage are different responsibilities

Acquisition answers:

**"Which historical files can I obtain, and how do I download them?"**

Storage answers:

**"Which raw files do I already have, where are they, and are they still valid?"**

For this reason, archive browsing, coverage inspection, verification, reconciliation, and deletion belong to the separate `mf storage` command family.

Similarly, working with files you already own is a separate workflow. Commands under `mf acquisition import`, `mf acquisition register`, and `mf acquisition profiles` manage user-supplied files, their identities, and their formats.

These workflows use the same managed data infrastructure but solve different problems.

---

## 11. Command Exit Codes

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

## Further Reading

- [Acquisition configuration](../../configuration/acquisition.md) — Complete TOML fields, defaults, and supported selections.
- [Supported historical sources](../../workflows/acquisition/historical-data.md#supported-sources) — Exchanges, markets, data types, and source-specific limitations.
- [Bybit BTCUSDT Quick Start](../../getting-started/quickstart.md) — Complete example from acquisition through normalization.
- [Managing acquisitions](../../workflows/acquisition/managing-acquisitions.md) — Inspecting downloaded files and handling acquisition problems.
- [Bring your own historical data](../../examples/bring-your-own-data/index.md) — Working with recordings, purchased files, and other external sources
