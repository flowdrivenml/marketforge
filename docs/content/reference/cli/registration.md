# Import, profile and registration commands

Manage raw import and registration declarations and normalization profiles. [Workflow and prerequisites](../../workflows/acquisition/external-data.md) · [Shared CLI behavior](index.md)


## Synopsis and arguments

### `mf acquisition register new`

```text
mf acquisition register new [-h] --from SOURCE name
```

| Argument or option | Parser contract |
| --- | --- |
| `name` | Required. |
| `--from` | Required. |

### `mf acquisition register edit`

```text
mf acquisition register edit [-h] name
```

| Argument or option | Parser contract |
| --- | --- |
| `name` | Required. |

### `mf acquisition register inspect`

```text
mf acquisition register inspect [-h] name
```

| Argument or option | Parser contract |
| --- | --- |
| `name` | Required. |

### `mf acquisition register validate`

```text
mf acquisition register validate [-h] [--testing | --production] name
```

| Argument or option | Parser contract |
| --- | --- |
| `name` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf acquisition register plan`

```text
mf acquisition register plan [-h] [--testing | --production] name
```

| Argument or option | Parser contract |
| --- | --- |
| `name` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf acquisition register run`

```text
mf acquisition register run [-h] [--testing | --production] name
```

| Argument or option | Parser contract |
| --- | --- |
| `name` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf normalize profiles list`

```text
mf normalize profiles list [-h] [--type TYPE]
                                             [--exchange EXCHANGE]
                                             [--instrument-type INSTRUMENT_TYPE]
                                             [--market-category MARKET_CATEGORY]
```

| Argument or option | Parser contract |
| --- | --- |
| `--type` | Optional. |
| `--exchange` | Optional. |
| `--instrument-type` | Optional. |
| `--market-category` | Optional. |

### `mf normalize profiles sample`

```text
mf normalize profiles sample [-h] --file FILE
                                               [--member MEMBER]
                                               [--header | --no-header]
```

| Argument or option | Parser contract |
| --- | --- |
| `--file` | Required. |
| `--member` | Optional. |
| `--header / --no-header` | Optional. |

### `mf normalize profiles inspect`

Opens the original in read-only Neovim on a terminal. Use `profiles show` for text output or global `--json` for the inspection record. See [profile commands](normalize.md#profile-commands) for the complete inventory, schema and draft workflow.

```text
mf normalize profiles inspect [-h] profile_id
```

| Argument or option | Parser contract |
| --- | --- |
| `profile_id` | Required. |

### `mf normalize profiles validate`

```text
mf normalize profiles validate [-h] [--file FILE]
                                                 [profile_id]
```

| Argument or option | Parser contract |
| --- | --- |
| `profile_id` | Optional. |
| `--file` | Optional. |

### `mf normalize profiles add`

```text
mf normalize profiles add [-h] --from SOURCE [--testing | --production]
                                            profile_id
```

| Argument or option | Parser contract |
| --- | --- |
| `profile_id` | Required. |
| `--from` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf normalize profiles register`

```text
mf normalize profiles register [-h] --from SOURCE
                                                 [--testing | --production]
                                                 profile_id
```

| Argument or option | Parser contract |
| --- | --- |
| `profile_id` | Required. |
| `--from` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf acquisition import new`

```text
mf acquisition import new [-h] [--silent] --from FROM_PATH
                                          import_name
```

| Argument or option | Parser contract |
| --- | --- |
| `--silent` | Default `false`. Final result only; per-file outcomes remain in the run report/events. |
| `import_name` | Required. |
| `--from` | Required. |

### `mf acquisition import inspect`

```text
mf acquisition import inspect [-h] [--silent] import_name
```

| Argument or option | Parser contract |
| --- | --- |
| `--silent` | Default `false`. Final result only; per-file outcomes remain in the run report/events. |
| `import_name` | Required. |

### `mf acquisition import validate`

```text
mf acquisition import validate [-h] [--silent] import_name
```

| Argument or option | Parser contract |
| --- | --- |
| `--silent` | Default `false`. Final result only; per-file outcomes remain in the run report/events. |
| `import_name` | Required. |

### `mf acquisition import plan`

```text
mf acquisition import plan [-h] [--silent] import_name
```

| Argument or option | Parser contract |
| --- | --- |
| `--silent` | Default `false`. Final result only; per-file outcomes remain in the run report/events. |
| `import_name` | Required. |

### `mf acquisition import run`

```text
mf acquisition import run [-h] [--silent] [--testing | --production]
                                          import_name
```

| Argument or option | Parser contract |
| --- | --- |
| `--silent` | Default `false`. Final result only; per-file outcomes remain in the run report/events. |
| `import_name` | Required. |
| `--testing`, `--production` | Mutually exclusive. Defaults to production. |

### `mf acquisition import format new`

```text
mf acquisition import format new [-h] --profile PROFILE
                                                 --input INPUT_REFERENCE
                                                 [--member MEMBER]
                                                 [--header | --no-header]
                                                 [--overwrite]
                                                 import_name
```

| Argument or option | Parser contract |
| --- | --- |
| `import_name` | Required. |
| `--profile` | Required. |
| `--input` | Required. |
| `--member` | Optional. Exact member of a multi-member ZIP input. |
| `--header / --no-header` | Optional. Explicit CSV header evidence; otherwise left unresolved. |
| `--overwrite` | Default `false`. |

## How Profiles Are Identified

MarketForge uses a consistent naming convention for normalization profiles.

A profile ID has four components:

```text
exchange.instrument_type.market_category.content_type
```

For example:

```text
bybit.perpetual.linear.trades
```

This describes the trade profile for Bybit linear perpetual data.

The corresponding depth profile is:

```text
bybit.perpetual.linear.depth
```

### Understanding the components

| Component | Example | Meaning |
|---|---|---|
| Exchange | `bybit` | The execution venue. |
| Instrument type | `perpetual` | The financial product type. |
| Market category | `linear` | The market or contract category. |
| Content type | `trades` | The type of source data the profile interprets. |

The final component must be:

```text
trades
```

or:

```text
depth
```

### Why do profiles use this naming convention?

The naming convention helps MarketForge associate a profile with the corresponding market and data type.

It also makes installed profiles easier to discover.

For example:

```text
bitget.perpetual.linear.trades
bitget.perpetual.linear.depth
```

describe different source interpretations for the same market classification.

One handles trades.

The other handles order-book data.

### Profile filenames

The filename must match the profile ID exactly, followed by `.toml`.

For example:

```text
bitget.perpetual.linear.depth.toml
```

The profile's internal `id` field must agree with its declared identity.

This prevents a profile from being installed under a misleading filename.

### Where are profiles installed?

MarketForge manages installed profiles under:

```text
data/.config/engine/normalize/profiles/
```

The relevant content-type directories are:

```text
data/.config/engine/normalize/profiles/
├── trades/
└── depth/
```

These installed files form the profile catalogue used by the historical normalizer.

You can keep your original profile declarations elsewhere and install them through the supported CLI.

---

## Validate Registration

Once the recipe is complete, validate it.

Use:

```bash
mf acquisition register validate synthetic-equity --testing
```

This performs registration preflight checks against the relevant testing catalogue and source information.

### Why is registration validation necessary?

A valid TOML file is not enough.

For example, a recipe might contain a correctly formatted instrument reference that does not exist in PostgreSQL.

Or it might select a historical interval for which the required specifications are unavailable.

Similarly, a source file may refer to a profile that has not been installed.

Registration validation checks these relationships before managed copies are created.

### What does validation check?

The supported validation includes checks involving:

- The registration declaration.
- Selected source files.
- Registered catalogue identities.
- Applicable historical specifications.
- Installed profiles.
- Relevant source preflight requirements.

The exact checks depend on the supplied inputs and current implementation.

### Does validation copy the files?

No.

Registration validation checks the inputs and required bindings.

It does not perform the final managed raw-copy operation.

### Does validation prove the data is correct?

Not completely.

For example, the registration may correctly associate a file with the installed profile while the file contains unexpected records later in the recording.

Complete decoding and processing checks happen during normalization.

Similarly, a declared interval does not independently prove complete event coverage.

**Registration validation establishes that the selected source is ready for the supported registration workflow, not that every market-data event has already been verified.**

---

## Prepare a Registration Plan

Before copying files into managed storage, you can ask MarketForge to prepare a registration plan.

Use:

```bash
mf acquisition register plan synthetic-equity --testing
```

### Why use a registration plan?

Suppose your recipe selects hundreds of files.

Before registering them, you may want to know:

- Which files MarketForge selected.
- Which instrument each file belongs to.
- Which profile will interpret it.
- Whether the required historical specifications are available.
- Whether any inputs are duplicated or invalid.
- Which registration work is ready to proceed.

Planning helps you review the operation before the managed-copy stage.

### What happens during planning?

MarketForge performs the relevant preflight checks and writes a retained operation plan.

The plan records the resolved registration work.

It does not create managed raw-file copies or final registration rows.

### Is planning completely read-only?

Not in the same sense as simple inspection.

The registration planning command can write its retained operation-plan artifacts.

However, it does not publish the selected raw files into managed storage.

This is different from the acquisition cache-only planning command.

For example:

```text
mf acquisition plan NAME --testing --cache-only
```

is a read-only inspection of cached acquisition information.

Meanwhile:

```text
mf acquisition register plan NAME --testing
```

performs registration preflight and retains an operation plan.

These commands solve different problems.

### Why are selected inputs frozen?

A registration operation needs a stable selection of source files.

Imagine that a directory contains ten files when registration begins.

While the operation is running, another process creates an eleventh file.

MarketForge should not silently change the operation's selected inputs halfway through.

Registration therefore resolves and freezes the selected files for the operation.

Files that appear afterward require a new registration operation.

This helps make each registration traceable and reproducible.

---

## Verify the Registered Files

After registration, you should confirm that the expected files are now available through MarketForge's managed storage.

For the synthetic example, you expect two raw inputs:

| File | Content |
|---|---|
| `synthetic-trades.csv` | Executed trades |
| `synthetic-depth.jsonl` | Order-book observations |

These files belong to the same registered AAPL instrument but use different profiles.

### Inspect managed storage

You can inspect the registered raw-file collection through the storage commands.

For example:

```bash
mf storage list \
    --testing \
    --exchange example_exchange \
    --instrument AAPL \
    --wide
```

Look for the expected synthetic trade and depth files.

The storage records provide the local IDs needed for more detailed inspection.

### Inspect one file

Use the returned ID:

```bash
mf storage show LOCAL_ID --testing
```

Replace `LOCAL_ID` with the actual local archive ID.

This lets you inspect the registered source identity, location, status, and retained evidence.

### Registration is not the final correctness check

At this point, MarketForge knows which managed raw files belong to the selected instrument and which profiles should be used.

However, you have not yet established that the complete normalized output matches the source's intended meaning.

For example, you should still check that:

- Trade timestamps are interpreted correctly.
- Prices retain the intended precision.
- Quantities use the correct units.
- Trade sides are mapped correctly.
- Depth observations produce the expected book states.
- Processing integrity results are acceptable.

Those checks belong to normalization and dataset inspection.

See [Raw Data Storage](storage.md) for detailed storage inspection and verification.

---

## Working with Multiple Files

Real datasets often contain many files rather than one recording.

For example:

```text
recordings/
├── trades-2024-10-28.csv
├── trades-2024-10-29.csv
├── trades-2024-10-30.csv
└── trades-2024-10-31.csv
```

MarketForge supports selecting multiple local files in a registration recipe.

### Explicit file selections

A registration input can name specific files.

For example:

```toml
files = [
    "trades-2024-10-28.csv",
    "trades-2024-10-29.csv",
    "trades-2024-10-30.csv",
]
```

These filenames are illustrative.

The paths are relative to the declared source root.

### File patterns

Registration also supports file patterns where appropriate.

This can be useful when a directory contains a larger collection of consistently named recordings.

However, patterns should be chosen carefully.

For example, a broad pattern might accidentally include unrelated files or a different source format.

Before running registration, inspect the resolved selection.

### Why does MarketForge limit file selection?

A misconfigured pattern could select thousands of files unintentionally.

For this reason, input enumeration is bounded.

The default `max_files` value is:

```text
1000
```

The maximum supported value is:

```text
10000
```

These limits help prevent unexpectedly broad registration operations.

### Can the same physical file appear twice?

Duplicate physical inputs within a registration selection are rejected.

This prevents one source file from being accidentally registered multiple times through overlapping selections in the same operation.

### What happens if new files arrive later?

A registration freezes the selected files for that operation.

Files created after the selection is frozen are not silently added.

To register newly arrived recordings, perform another registration using the appropriate selection.

This makes the input set of each operation explicit.

---

## Source Identity and Provenance

Historical data may come from many different providers.

A single instrument can also appear in recordings from multiple sources.

For example, AAPL trades may be distributed by a market-data vendor while the actual trading venue is Nasdaq.

MarketForge therefore keeps source identity separate from instrument identity.

### Provider and venue are different

Consider the following:

| Property | Example |
|---|---|
| Provider | Databento |
| Product | XNAS.ITCH |
| Venue | Nasdaq |
| Instrument | AAPL |

The provider supplies the recording.

The venue identifies where trading occurred.

The registered catalogue instrument should describe the execution venue, not the vendor.

### Source keys

The registration recipe uses a source key to identify a particular source collection.

For example:

```toml
[source]
key = "synthetic-equity"
```

This identifier helps associate registered files with the declared source.

It is also useful when selecting those files later during normalization.

### Native instrument identifiers

Some providers use their own numeric IDs for instruments.

For example, a source record might contain:

```text
publisher_id = 2
instrument_id = 38
```

These are source-native identifiers.

They are not interchangeable with PostgreSQL's canonical instrument IDs.

Registration can retain native identity information, including publisher and instrument IDs, within the appropriate source namespace.

### Why preserve native identities?

Source-native identities can be important when:

- A provider distributes data for several instruments.
- Symbols change over time.
- Different publishers use overlapping numeric ID ranges.
- Historical records must be traced back to their original source.
- Multiple providers supply recordings of the same venue.

Preserving this information helps keep the interpretation of the data auditable.

### Historical specification fingerprints

Registration declarations can also include supported specification fingerprints.

These allow an input to be tied to particular historical specification evidence.

This is useful when exact economic interpretation matters and the catalogue contains multiple observations or historical revisions.

A fingerprint should refer to actual registered evidence.

Do not invent one or use it as a substitute for verifying the applicable specification.

---

## What Does Successful Registration Mean?

A successful registration means MarketForge accepted the selected source files into its managed raw-data workflow with the applicable bindings.

It establishes information about:

- The source files.
- Their origin and provider.
- The registered instrument.
- The selected profiles.
- Their declared historical coverage.
- Their managed copies and verification evidence.

However, registration does not prove that every record inside those files is correct.

### Three different checks

| Stage | What it establishes |
|---|---|
| Profile validation | The declared interpretation is accepted by the supported Rust compiler. |
| Registration validation | The files have the required catalogue, profile, and source bindings. |
| Normalization | The source records can be decoded and processed under the applicable rules. |

These checks are complementary.

For example, a profile can be valid while a recording contains malformed rows.

Similarly, a registration can succeed while a later normalization job identifies a sequence gap.

Do not treat registration success as a substitute for processing and integrity verification.

### What should you inspect after registration?

Inspect the operation result and retained evidence.

Confirm that the expected files were accepted or reused.

Then inspect the managed storage records.

Finally, normalize a representative sample and compare its output with independently known source values.

For depth data, checking reconstructed states is more informative than merely checking the number of generated Parquet rows.

---

## Common Questions

## Common Profile and Registration Problems

### Profile file not found

**Possible cause:** The supplied path is incorrect or the profile is not installed.

Check the exact filename and location.

For an installed definition:

```bash
mf normalize profiles list
```

For a manually authored file, validate the actual path:

```bash
mf normalize profiles validate --file PATH
```

### Profile ID does not match the filename

**Possible cause:** The file is named differently from its declared four-component profile ID.

Check the filename and the internal `id` field.

Use the required naming convention:

```text
exchange.instrument_type.market_category.trades|depth
```

### Profile semantic validation fails

**Possible cause:** The profile contains an unsupported field mapping, transformation, source format, or combination of rules.

Inspect the Rust validation diagnostics.

Compare the declaration with the supported profile reference.

Do not remove important interpretation rules simply to make validation pass.

### Profile installation reports a conflict

**Possible cause:** Another definition with the same ID is already installed and has a different interpretation.

Inspect the existing profile:

```bash
mf normalize profiles inspect PROFILE_ID
```

Compare it with the new declaration.

Resolve the identity and interpretation conflict explicitly.

### The profile validates, but the output is wrong

**Possible cause:** The profile follows the supported schema but does not match the real source semantics.

For example:

- Timestamps use the wrong unit.
- Trade sides are reversed.
- Quantities use the wrong denomination.
- The source contains unexpected values.
- Depth messages were interpreted as the wrong kind of observation.

Compare representative source records with the normalized result.

Use provider documentation to establish the correct meaning.

### Registration cannot find the source files

**Possible cause:** The declared source root or file selection is incorrect.

Inspect the recipe:

```bash
mf acquisition register inspect NAME
```

Check that the files exist beneath the declared source root.

Remember that relative source roots are resolved against the project root.

### Registration reports a missing exchange

**Possible cause:** The declared instrument belongs to a venue that is not registered.

Inspect the exchange through `mf catalog`.

Register the correct venue before continuing.

### Registration reports a missing instrument

**Possible cause:** The selected catalogue instrument has not been registered.

Inspect the instrument reference:

```bash
mf catalog instrument show REFERENCE --testing
```

Register the required instrument with correct identity information.

### Historical specifications do not cover the recording

**Possible cause:** The relevant specification evidence does not apply to the complete required interval.

Inspect the declared coverage and the registered specification validity periods.

Supply historically applicable evidence.

Do not extend validity intervals without support.

### Registration reports a missing profile

**Possible cause:** The recipe references a profile that is not installed.

Inspect the installed profile list and validate the required definition.

Install the correct profile before retrying.

### Registration selects the same physical file twice

**Possible cause:** Two explicit selections or patterns resolve to the same source file.

Review the selected inputs.

Remove the duplicate selection rather than attempting to register it twice.

### Registration exceeds the file limit

**Possible cause:** A file pattern selects more inputs than the configured bound.

Inspect the pattern and its intended scope.

Use a narrower selection when possible.

Increase `max_files` only when the larger selection is intentional and remains within the supported limit.

### Registration fails after copying some files

**Possible cause:** A source file changed, a copy or verification step failed, or the operation was interrupted.

Inspect the registration report and retained copy receipts.

Previously completed files may be reusable.

Do not manually overwrite managed files or alter registry statuses.

### Registration succeeds, but normalization cannot prepare

**Possible cause:** Registration and normalization have different readiness requirements.

For example, normalization may reject an invalid prepared source contract, missing specification applicability, or unsupported profile interpretation.

Inspect the normalization preparation report.

Do not assume that successful registration guarantees executable normalization.

---
