# Managed raw registration declarations

Schemas, identity requirements and plan checks for managed raw registration. See [Register external recordings](../workflows/acquisition/external-data.md) for the procedure and [Import, profile and registration commands](cli/registration.md) for flags.


## Serialized declaration

## Registration Metadata Fields

| Field | Required | Type / constraint |
|---|---|---|
| `registration.version` | Yes | Integer `1` |
| `registration.name` | Yes | Nonempty string |

### Important rules

The version must be exactly 1.

The name must be nonempty.

The registration metadata identifies the declaration, not the source-provider product or instrument.

---

## Source Fields

| Field | Required | Type / constraint |
|---|---|---|
| `source.key` | Yes | Nonempty string |
| `source.provider` | Yes | Nonempty string |
| `source.product` | Yes | Nonempty string |
| `source.root` | Yes | Nonempty path string |
| `source.provenance` | Yes | Nonempty string |
| `source.kind` | No | `recording`, `purchased`, `other`; default `recording` |

### Important rules

Relative roots resolve against the project root.

Source-key reuse requires a compatible retained source identity.

Provider, product, and provenance should describe the actual source.

The provider and execution venue are separate concepts.

---

## Coverage Fields

| Field | Required | Type / constraint |
|---|---|---|
| `coverage.start` | Yes | Timezone-aware TOML datetime |
| `coverage.end` | Yes | Timezone-aware TOML datetime later than start |

### Important rules

The interval is half-open:

```text
[start, end)
```

It applies to every input in the recipe.

The declared coverage is not independently inferred from all source records.

Historical specification evidence must be applicable to the interval.

---

## Input Fields

| Field | Required | Type / constraint |
|---|---|---|
| `inputs.instrument` | Yes | Concrete catalogue instrument reference |
| `inputs.profile` | Yes | Matching four-component profile ID |
| `inputs.content` | Yes | `trades` or `depth` |
| `inputs.files` | Conditional | List of root-relative paths |
| `inputs.patterns` | Conditional | List of root-relative patterns |
| `inputs.max_files` | No | Integer 1–10000; default 1000 |
| `inputs.specification` | No | Exactly 64 lowercase hexadecimal characters |
| `inputs.native_identity` | No | Complete native identity table |

### Important rules

At least one nonempty file or pattern selection is required.

The profile must match the instrument's venue, type, category, and selected content.

The input must resolve to a registered concrete instrument.

The selected files must be valid local managed-copy candidates.

Duplicate physical files within the recipe are rejected.

---

## Native Identity Fields

| Field | Required when table present | Type / constraint |
|---|---|---|
| `inputs.native_identity.namespace` | Yes | Nonempty string |
| `inputs.native_identity.publisher_field` | Yes | Nonempty string |
| `inputs.native_identity.publisher_id` | Yes | Nonnegative strict integer |
| `inputs.native_identity.instrument_field` | Yes | Nonempty string |
| `inputs.native_identity.instrument_id` | Yes | Nonnegative strict integer |
| `inputs.native_identity.provenance` | Yes | Nonempty string |

### Important rules

Publisher and instrument field names must be different.

Numeric IDs must not be negative.

The namespace identifies the source-native scope.

Native instrument IDs are not canonical MarketForge instrument IDs.

The selected source must support the corresponding bounded identity checks.

---

## Registration Recipe Structure

## Where Registration Recipes Are Stored

Managed registration recipes live under:

```text
data/.config/workflows/data_engineering/import/registration/
```

For example:

```text
data/.config/workflows/data_engineering/import/registration/
├── synthetic-equity.toml
├── databento-aapl.toml
├── private-recorder.toml
└── purchased-data.toml
```

These names are illustrative.

### Create a managed recipe

You can create one from an existing TOML declaration.

For example:

```bash
mf acquisition register new synthetic-equity \
    --from docs/content/examples/catalogue/registration.toml
```

This creates a managed copy of the supplied recipe.

### Does `new` overwrite an existing recipe?

No.

The destination must be new.

The command refuses to overwrite an existing file.

### Create a starter from a directory

You can also create an incomplete draft from an existing source directory.

For example:

```bash
mf acquisition register new private-recorder \
    --from ./recordings
```

The command creates a starter declaration.

You must supply the missing identity, provenance, coverage, and input information.

### Edit the recipe

Use:

```bash
nvim data/.config/workflows/data_engineering/import/registration/synthetic-equity.toml
```

Or use the configured editor:

```bash
mf acquisition register edit synthetic-equity
```

The editor command inspects the saved declaration after editing.

### Can I use an explicit path?

Yes.

Registration commands accept managed names or explicit paths.

For example:

```bash
mf acquisition register inspect \
    docs/content/examples/catalogue/registration.toml
```

Use a path containing a directory component when referring to an explicit file.

---

## The Four Main Sections

A managed registration recipe uses version 1 of its TOML contract.

The main sections are:

| Section | Purpose |
|---|---|
| `[registration]` | Identifies the registration declaration. |
| `[source]` | Describes the external source and its provenance. |
| `[coverage]` | Declares the historical interval represented by the inputs. |
| `[[inputs]]` | Binds selected files to instruments, profiles, and specification evidence. |

An input can also contain an optional native identity declaration:

```toml
[inputs.native_identity]
```

### Overall structure

```text
Registration Recipe
        |
        +-- Registration Identity
        |
        +-- Source Provenance
        |
        +-- Historical Coverage
        |
        +-- Input 1
        |      |
        |      +-- Instrument
        |      +-- Profile
        |      +-- Content Type
        |      +-- Files / Patterns
        |      +-- Optional Specification Fingerprint
        |      +-- Optional Native Identity
        |
        +-- Input 2
               |
               +-- Instrument
               +-- Profile
               +-- Content Type
               +-- Files / Patterns
```

The recipe must contain at least one input.

Each input identifies a concrete registered instrument.

---

## Registration Identity

## The `[registration]` Section

Every recipe requires:

```toml
[registration]
version = 1
name = "synthetic-equity"
```

### Version

The required version is:

```toml
version = 1
```

The current managed registration schema supports only version 1.

Unlike some other MarketForge configuration formats, this field is required.

### Name

The name identifies the registration declaration.

For example:

```toml
name = "synthetic-equity"
```

It must be a nonempty string.

### Why does registration have its own identity?

A source collection may participate in several operations.

The registration name describes the particular declaration used to bind its files.

This helps distinguish the managed recipe from the source collection itself.

### Is the registration name the same as the source key?

Not necessarily.

For example:

```toml
[registration]
name = "aapl-october"

[source]
key = "databento-xnas-itch-aapl-20241028"
```

The registration name identifies the declaration.

The source key identifies the retained source collection.

For small examples, the two values may be identical.

---

## Source Provenance

## The `[source]` Section

The source section describes where the files came from.

For example:

```toml
[source]
key = "synthetic-equity"
provider = "example_recorder"
product = "synthetic_csv_and_jsonl"
kind = "recording"
root = "docs/content/examples/catalogue"
provenance = "Two synthetic example records with explicitly assigned semantics"
```

### Why is source provenance necessary?

Imagine two providers supply historical trades for the same market.

For example:

```text
Provider A:
AAPL Trades

Provider B:
AAPL Trades
```

Both may describe Nasdaq executions.

However, their recordings can differ in:

- Coverage.
- Record format.
- Source timestamps.
- Native identifiers.
- Data quality.
- Processing history.

MarketForge should preserve the distinction between the providers.

The source declaration supplies this information.

---

## Source Key

The required source key is:

```toml
key = "synthetic-equity"
```

It identifies the source collection.

### Why use a stable key?

The registry associates source information with the declared key.

Repeating registration can reuse compatible previously registered source information.

However, the same key must not silently change meaning.

### What should a key identify?

Ideally, a key identifies one stable source collection.

For example:

```text
databento-xnas-itch-aapl-20241028
```

is more descriptive than:

```text
data
```

### Can a source key be reused?

Yes, when the registered source identity remains compatible.

For example, repeating the same registration can reuse the source key and verified managed files.

### Can the same key point to another directory?

Not arbitrarily.

The registration service checks the source root and source kind.

It also checks retained provider, product, and provenance information.

Conflicting reuse can be rejected.

### Why?

A stable source key should not silently become an unrelated dataset.

That would make source provenance difficult to trust.

---

## Provider

The provider field identifies who supplied the data.

For example:

```toml
provider = "Databento"
```

### Why is provider separate from exchange?

Because a market-data provider is not necessarily the execution venue.

For example:

```text
Provider:
Databento

Execution venue:
Nasdaq

Instrument:
AAPL
```

The provider distributes the recording.

The venue identifies where trading occurred.

### What happens if I confuse them?

You could register the recording under the wrong market identity.

For example, assigning Nasdaq executions to a fictional Databento execution venue would be incorrect.

Use the actual catalogue exchange for the instrument.

### Does provider need to match a built-in acquisition adapter?

No.

An external provider may be any appropriately identified source organization or recording system.

The registered instrument must still use the correct execution venue.

---

## Product

The required product field identifies the source dataset or product.

For example:

```toml
product = "XNAS.ITCH"
```

### Why preserve the product?

A provider may distribute several different datasets.

For example:

```text
Trades
Order-book depth
Different market feeds
Different publishers
Different archive products
```

These products may use different formats and source identifiers.

Recording the product helps distinguish them.

### Is product the same as a normalization profile?

No.

The product describes the provider's source offering.

The profile describes how MarketForge interprets the source records.

For example:

```text
Provider product:
XNAS.ITCH

Profile:
xnas.equity.equity.trades
```

These are related, but they are not interchangeable.

---

## Source Kind

The source section supports:

```toml
kind = "recording"
```

The accepted values are:

| Value | Meaning |
|---|---|
| `recording` | Data captured or recorded by a system. |
| `purchased` | Data obtained as a purchased dataset. |
| `other` | Another explicitly identified source type. |

The default is:

```text
recording
```

### Why record source kind?

It helps distinguish different acquisition origins.

For example, a file purchased from a vendor and a file captured by your own software may require different provenance explanations.

### Does source kind change the execution venue?

No.

It describes how the data was obtained.

The execution venue remains part of the instrument identity.

### Does `purchased` automatically verify the data?

No.

Source kind is provenance metadata.

It does not establish the correctness or completeness of the recording.

---

## Source Root

The required root field identifies the directory containing the files.

For example:

```toml
root = "docs/content/examples/catalogue"
```

### How are relative roots resolved?

For managed registration recipes, relative source roots resolve against the MarketForge project root.

For example, suppose the checkout is:

```text
/home/user/projects/marketforge/
```

Then:

```toml
root = "docs/content/examples/catalogue"
```

resolves to:

```text
/home/user/projects/marketforge/docs/content/examples/catalogue/
```

### Does the working directory matter?

Not for this relative-root resolution.

The registration service resolves the source root against the project root rather than using the current invocation directory.

### Why is this useful?

It makes the recipe more predictable when invoked from different shell directories.

### Can I use an absolute root?

Yes.

For example:

```toml
root = "/mnt/market-data/recordings"
```

This identifies an absolute source location.

### Should I use relative or absolute paths?

Repository-relative roots are convenient for examples and files stored within the checkout.

Absolute roots may be appropriate for large external recordings stored on separate disks.

The choice depends on your source layout.

### Important distinction

The legacy raw-only import configuration resolves relative roots differently.

Its relative root is interpreted from the command working directory.

Do not assume that import and registration use identical path-resolution rules.

---

## Provenance

The required provenance field describes the origin of the source.

For example:

```toml
provenance = "User-supplied Databento XNAS.ITCH recording for 2024-10-28"
```

### Why is a provenance description useful?

A source key and provider name may not explain everything about a recording.

For example, you may want to preserve:

- How the dataset was obtained.
- Which product it came from.
- The original recording date.
- Whether files were manually exported.
- Known provider-specific information.
- Important interpretation assumptions.

### What should provenance contain?

Supply a factual description supported by the source.

For example:

```toml
provenance = "Trades and MBP-10 JSONL exports supplied by Databento for AAPL on 2024-10-28"
```

### Does provenance prove the source is correct?

No.

It records the declared origin information.

The correctness of the source identity remains the responsibility of the declaration and its supporting evidence.

### Why not leave provenance empty?

The schema requires a nonempty string.

A meaningful description also makes later investigation easier.

---

## Historical Coverage

## The `[coverage]` Section

Every managed registration recipe requires a historical coverage interval.

For example:

```toml
[coverage]
start = 2024-10-28T00:00:00Z
end = 2024-10-29T00:00:00Z
```

This declares the historical period represented by the selected files.

### Why is coverage required?

MarketForge needs to know when the registered source applies.

The interval is used when resolving historical specification evidence and preparing the source for later processing.

### Does the interval apply to every input?

Yes.

The top-level coverage interval applies to all inputs in the recipe.

### What if trades and depth cover different periods?

The current managed registration recipe uses one shared interval.

If independently different coverage declarations are required, create separate recipes with the appropriate intervals.

Do not declare a misleading shared interval merely to combine unrelated recordings.

---

## Half-Open Coverage Intervals

Registration uses a half-open interval:

```text
[start, end)
```

The start is included.

The end is excluded.

### Example

```toml
[coverage]
start = 2024-10-28T14:30:00Z
end = 2024-10-28T14:40:00Z
```

This declares ten minutes of source coverage.

The requested interval includes:

```text
14:30:00
```

but excludes:

```text
14:40:00
```

### Why use half-open intervals?

They make adjacent intervals easier to express.

For example:

```text
Recording A:
[14:30, 14:40)

Recording B:
[14:40, 14:50)
```

The declared intervals meet at the same boundary.

### Does the end need to be later?

Yes.

An empty or reversed interval fails validation.

### Are timezones required?

Yes.

The timestamps must be timezone-aware TOML datetimes.

For example:

```toml
start = 2024-10-28T14:30:00Z
```

The `Z` indicates UTC.

---

## Declared Coverage vs. Observed Events

This is an important distinction.

**The coverage section is a declaration, not a measurement of every event inside the file.**

For example, a registration may declare:

```text
14:30 ───────────────── 14:40
```

However, the recording might contain events only at:

```text
14:30:01
14:35:00
14:39:58
```

The declaration does not prove that every possible market event exists between those timestamps.

### Why not infer coverage from the filename?

Because filenames do not necessarily describe the exact event interval.

For example:

```text
AAPL_2024-10-28.jsonl
```

might contain:

- One complete trading day.
- Only regular trading hours.
- A short recording.
- A filtered subset of events.

The actual source provenance must establish the declared coverage.

### Does registration verify every timestamp?

No.

Registration performs bounded source-layout checks and the relevant metadata validation.

It does not normalize every record simply to establish observed timestamp coverage.

### When are actual timestamps inspected?

Normalization processes the source records and records the applicable observed bounds and integrity evidence.

Dataset inspection can then show the observed timestamps of the generated Parquet files.

---

## Why Historical Specifications Must Cover the Interval

Registration requires applicable historical instrument specifications.

For example, the AAPL source needs the relevant equity specifications covering the declared recording period.

### Why?

MarketForge must know the economic rules used to interpret the source.

For example:

- Quantity type.
- Tick size.
- Base and quote assets.
- Contract value when applicable.
- Other required specifications.

### Example

Suppose the registration covers:

```text
2024-10-28 14:30 ─── 14:40
```

The selected historical specification evidence must cover that interval.

### What happens if it covers only part?

Registration can reject the binding.

For example:

```text
Registration:
14:30 ───────────── 14:40

Specification:
14:35 ───────────── 14:40
```

The specification does not cover the complete declared interval.

### Does a current specification automatically qualify?

No.

The current registration workflow requires applicable explicit historical specification evidence.

A current catalogue observation or operationally inferred specification is not automatically an acceptable substitute.

This is stricter than merely having a current instrument record.

### What kind of evidence is accepted?

The registration preflight uses the supported explicit historical evidence categories, including:

```text
historical_declaration
historically_verified
```

These must be applicable to the complete registered interval.

For ordinary catalogue preparation, follow the supported historical specification declaration workflow.

See [Historical Specifications](../workflows/catalogue/specifications.md).

---

## Input Declarations

## The `[[inputs]]` Array

Each registration recipe must contain at least one input.

An input identifies a set of source files and the interpretation required for them.

For example:

```toml
[[inputs]]
instrument = "example_exchange.equity.equity.AAPL"
profile = "example_exchange.equity.equity.trades"
content = "trades"
files = ["synthetic-trades.csv"]
```

### What does this describe?

It declares:

```text
Instrument:
AAPL

Content:
Trades

Profile:
Example Exchange Equity Trades

Source file:
synthetic-trades.csv
```

### Why use an array?

A registration recipe may contain several related inputs.

For example:

```text
AAPL Trades
AAPL Depth
```

Each input has its own content type, profile, and file selection.

### Can one input select several files?

Yes.

A single input may select multiple files when they share the declared instrument and interpretation.

### Can one recipe contain several instruments?

Yes.

Each input can declare a different concrete instrument, provided the applicable catalogue, profile, and historical evidence requirements are satisfied.

---

## Instrument Reference

Every input requires:

```toml
instrument = "example_exchange.equity.equity.AAPL"
```

This is a concrete catalogue instrument reference.

### Reference format

The required structure is:

```text
exchange.instrument_type.market_category.symbol
```

For example:

```text
example_exchange.equity.equity.AAPL
```

Another example:

```text
bybit.perpetual.linear.BTCUSDT
```

### Why use the complete reference?

A symbol alone may be ambiguous.

For example:

```text
BTCUSDT
```

might refer to spot or perpetual trading.

The complete reference preserves the market identity.

### Is this the same as an acquisition scope?

No.

Acquisition scopes use source-native archive selectors.

For example:

```text
BTCUSDT
```

or:

```text
BTC-USD
```

Registration uses the canonical catalogue reference.

### Does the instrument need to exist?

Yes.

Registration preflight resolves the declared instrument against the testing catalogue.

A syntactically valid reference is not sufficient.

The corresponding registered instrument must exist and agree with the historical identity authority.

### Can I use a family reference?

No.

The managed registration contract requires a concrete instrument reference.

It does not use an arbitrary family name in place of an instrument.

---

## Profile Reference

Every input requires:

```toml
profile = "example_exchange.equity.equity.trades"
```

This identifies the installed normalization profile.

### Why is the profile necessary?

The source file may use its own timestamp, quantity, and event format.

The profile describes how Rust interprets those fields.

### Profile ID format

A profile ID has exactly four components:

```text
exchange.instrument_type.market_category.content
```

The content must be:

```text
trades
depth
```

### Why must the profile match the instrument?

The selected profile must agree with the instrument's exchange, instrument type, and market category.

For example:

```toml
instrument = "example_exchange.equity.equity.AAPL"
profile = "example_exchange.equity.equity.trades"
content = "trades"
```

is consistent.

### Invalid example

```toml
instrument = "example_exchange.equity.equity.AAPL"
profile = "bybit.perpetual.linear.trades"
content = "trades"
```

The profile describes another exchange and market type.

The registration model rejects the mismatch.

### Does a matching ID prove profile validity?

No.

Registration also checks that the profile is installed and accepted by the Rust compiler.

A correctly named but invalid profile is not sufficient.

---

## Content Type

Every input requires:

```toml
content = "trades"
```

or:

```toml
content = "depth"
```

### Trades

Use:

```toml
content = "trades"
```

for executed-trade recordings.

### Depth

Use:

```toml
content = "depth"
```

for supported L2 order-book recordings.

### Why is the name `depth` used here?

Different MarketForge interfaces use related but distinct content names.

| Interface | Trades | L2 order book |
|---|---|---|
| Acquisition recipe | `trades` | `order_book_l2` |
| Registration recipe | `trades` | `depth` |
| Profile ID | `trades` | `depth` |
| Normalization recipe | `trades` | `order_book_l2` |

Use the values accepted by the relevant schema.

### Does one input support both types?

No.

Each input declares one content type.

To register both trades and depth, use two input entries.

---

## Profile and Specification Compatibility

Registration does more than check profile names.

It also checks whether the profile's quantity interpretation agrees with the applicable historical specification.

### Example

Suppose the installed trade profile declares:

```toml
[normalization]
target = "trade"
quantity_encoding = "base"
```

The historical instrument specification declares:

```toml
quantity_type = "base"
```

These interpretations agree.

### Conflicting example

The profile declares:

```toml
quantity_encoding = "contracts"
```

but the historical specification declares:

```toml
quantity_type = "base"
```

The interpretations disagree.

Registration rejects this conflict.

### Why is this necessary?

The source quantity cannot simultaneously be interpreted as base units and contract counts under the same binding.

Allowing the mismatch could produce incorrect normalized quantities.

### Can I change the specification to make validation pass?

Only when the new value is supported by actual historical evidence.

Do not alter the quantity type merely to satisfy the validator.

### Can I change the profile instead?

Only when the source format actually uses the revised quantity encoding.

The correct solution depends on the real source semantics.

---

## Selecting Source Files

## Explicit File Selection

Each input can list its source files explicitly.

For example:

```toml
files = ["synthetic-trades.csv"]
```

### How are paths resolved?

The filenames are relative to the declared source root.

For example:

```toml
[source]
root = "docs/content/examples/catalogue"
```

combined with:

```toml
files = ["synthetic-trades.csv"]
```

selects:

```text
docs/content/examples/catalogue/synthetic-trades.csv
```

relative to the project root.

### Multiple files

For example:

```toml
files = [
    "trades-2024-10-28.csv",
    "trades-2024-10-29.csv",
    "trades-2024-10-30.csv",
]
```

This can be useful when several recordings share the same market identity and source interpretation.

### Why use explicit filenames?

Explicit selection is appropriate when:

- You know the exact files.
- The source directory contains unrelated data.
- You want to avoid broad pattern matching.
- You are registering a small research example.
- You need reproducible input selection.

---

## Pattern-Based Selection

The registration schema also supports file patterns.

For example:

```toml
patterns = ["trades-*.csv"]
```

This can select several matching files beneath the source root.

### Why use patterns?

Suppose your directory contains:

```text
recordings/
├── trades-2024-10-28.csv
├── trades-2024-10-29.csv
├── trades-2024-10-30.csv
└── notes.txt
```

A pattern such as:

```toml
patterns = ["trades-*.csv"]
```

can select the trade recordings without selecting the notes file.

### Recursive selection

The underlying source-file selection supports bounded recursive pattern matching.

For example:

```toml
patterns = ["**/*.csv"]
```

can select matching files in nested directories.

### Why be careful with recursive patterns?

A broad pattern may select files from several instruments or source formats.

For example:

```text
recordings/
├── BTCUSDT/
└── ETHUSDT/
```

If one input declares BTCUSDT but recursively selects both directories, the source identity would be incorrect for some files.

Use appropriately narrow patterns.

### Can I combine files and patterns?

Yes.

For example:

```toml
files = ["special-recording.csv"]
patterns = ["trades-*.csv"]
```

The resulting selection is resolved through the supported input enumeration logic.

### What if neither is supplied?

The recipe is invalid.

Each input must supply at least one nonempty selection.

---

## File Selection Limits

Each input supports:

```toml
max_files = 1000
```

The accepted range is:

```text
1–10000
```

The default is:

```text
1000
```

### Why limit file selection?

A broad pattern could accidentally select thousands of files.

The limit prevents unexpectedly large registration selections.

### Is the limit per recipe?

The `max_files` field belongs to each input selection.

A recipe can contain several inputs with their own limits.

### Does this limit file size?

No.

It limits the number of selected files.

A single large recording can still require substantial storage.

### Does the input selection have other bounds?

Yes.

The shared source-selection implementation also bounds recursive traversal.

For example, pattern expansion stops after inspecting too many filesystem entries.

This prevents unbounded source-directory scanning.

### What if the limit is exceeded?

Registration rejects the selection.

Narrow the pattern or adjust the limit deliberately.

---

## Deterministic Selection

MarketForge resolves selected input files into a deterministic order.

### Why is this important?

Suppose a pattern selects:

```text
trades-2024-10-28.csv
trades-2024-10-29.csv
trades-2024-10-30.csv
```

The registration operation should not depend on incidental filesystem enumeration order.

Deterministic selection helps make the registered input inventory traceable.

### Does deterministic enumeration mean chronological processing?

No.

The sorted file selection is an input-inventory property.

Actual event ordering depends on the source profile and Rust normalization contract.

For example, a file with a lexically later name may contain events with earlier timestamps.

Do not use filename sorting as a substitute for source-ordering evidence.

---

## Duplicate Physical Inputs

The registration workflow rejects duplicate physical source files within the selected recipe.

### Why?

Imagine the same file is selected twice:

```toml
files = ["trades.csv"]
patterns = ["*.csv"]
```

The overlapping selections may resolve to the same physical file.

Registering it twice would create ambiguous input ownership.

### What does MarketForge compare?

Registration inspection tracks the physical source identity using filesystem information.

It checks the selected files across input groups.

### Why is this stronger than comparing filenames?

Two paths can refer to the same physical file.

For example, hard links can provide different names for one underlying file.

A filename-only check would not reliably detect that situation.

### What happens if a duplicate is found?

The registration operation fails.

Remove the duplicate selection.

### Can one file legitimately contain several instruments?

Some source formats can contain multiple instrument identities.

However, the current managed registration selection still rejects duplicate physical inputs within one recipe.

For mixed-instrument sources, use the supported source-specific registration and normalization contracts rather than listing the same physical file repeatedly under different input groups.

---

## Source Symlinks and Path Safety

Registration inherits the shared external-input path safety rules.

### Root-relative selections

Individual file entries must remain beneath the declared source root.

### Parent traversal

Selections such as:

```text
../other-recordings/trades.csv
```

are rejected.

### Absolute file entries

Individual file selectors must be root-relative.

Use `source.root` to specify an absolute source directory when necessary.

### Symlinks

Selected source symlink files are not accepted as managed-copy inputs.

Recursive enumeration does not follow symlinked source directories.

### Why enforce these rules?

The registration operation must know exactly which physical files belong to the declared source.

Unexpected path traversal or symlink resolution could select data outside the intended collection.

These checks protect the integrity of the frozen input inventory.

---

## Historical Specification Fingerprints

## What Is `inputs.specification`?

An input can optionally pin a particular historical specification fingerprint.

For example:

```toml
specification = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
```

This is a syntactically valid illustrative fingerprint.

It is not a real specification fingerprint from the MarketForge registry.

### Why use a fingerprint?

A registered instrument may have several historical specification evidence records.

For example, different observations or historical declarations may describe different periods.

You may want to explicitly identify the evidence used for registration.

### What does the fingerprint pin?

It identifies the exact applicable historical specification evidence selected for the input.

### Required format

The value must contain exactly:

```text
64 lowercase hexadecimal characters
```

This corresponds to a SHA-256-style hexadecimal fingerprint.

### Are uppercase hexadecimal characters accepted?

Not by the current field pattern.

Use lowercase hexadecimal.

### Does the fingerprint replace the instrument reference?

No.

The input still requires a concrete registered instrument.

### Does it replace coverage validation?

No.

The selected specification must still be applicable to the complete declared coverage interval.

### Is the field required?

No.

It is optional.

When omitted, the registration service resolves the applicable explicit historical specification evidence through its supported authority rules.

### When should I supply it?

Use it when you need to pin a particular known historical declaration or verified evidence record.

For example, it may be useful when several specification revisions exist.

### Why not invent a fingerprint?

Because the fingerprint identifies registered evidence.

An arbitrary 64-character hexadecimal string does not create a valid specification.

---

## How Registration Resolves Historical Specifications

During registration validation, Forge resolves historical specification evidence for the selected instrument.

The process includes:

```text
Registered Instrument
        |
        v
Historical Authority
        |
        v
Applicable Explicit Evidence
        |
        v
Optional Fingerprint Match
        |
        v
Historical Specification
```

### What happens if no applicable evidence exists?

Registration fails.

### What if evidence exists but does not cover the interval?

Registration fails.

### What if the fingerprint does not match?

Registration fails.

### What if several conflicting declarations apply?

The historical authority resolver can reject conflicting evidence.

### Why not silently choose the latest observation?

Because the latest observation is not necessarily the correct historical specification.

The declaration must apply to the actual source period.

### What about inferred catalogue specifications?

The managed registration preflight explicitly requests declared historical evidence.

It does not use arbitrary catalogue inference as a substitute for the required historical declaration.

This is an important distinction from some other MarketForge metadata-enrichment workflows.

---

## Native Instrument Identity

## Why Preserve Source-Native IDs?

Some market-data providers use their own numeric identifiers for instruments.

For example, Databento records can include:

```text
publisher_id
instrument_id
```

These identifiers belong to the provider's source namespace.

They are not the same as MarketForge's canonical PostgreSQL instrument IDs.

### Example

Suppose a source record contains:

```text
Publisher ID: 2
Instrument ID: 38
```

MarketForge may separately identify the corresponding canonical AAPL instrument through its catalogue.

### Why keep both?

Because the source-native IDs help trace the original recording.

For example:

```text
Source:
Publisher 2
Instrument 38
        |
        v
Registered Interpretation
        |
        v
Canonical AAPL Instrument
```

The source identity and canonical identity serve different purposes.

### Why not use the native instrument ID as the canonical ID?

Source-native IDs may be scoped to:

- A provider.
- A publisher.
- A product.
- A dataset.
- A historical period.

The same numeric ID may refer to different instruments in different namespaces.

MarketForge therefore preserves the source namespace separately.

---

## The `[inputs.native_identity]` Table

An input can declare native identity information.

For example:

```toml
[inputs.native_identity]
namespace = "databento:XNAS.ITCH:2024-10-28"

publisher_field = "hd.publisher_id"
publisher_id = 2

instrument_field = "hd.instrument_id"
instrument_id = 38

provenance = "Header identity in the supplied XNAS.ITCH exports."
```

This is the native identity declaration used by the repository's Databento example.

### What does it describe?

| Field | Meaning |
|---|---|
| `namespace` | Source-native identity scope. |
| `publisher_field` | Source field containing the publisher ID. |
| `publisher_id` | Expected native publisher ID. |
| `instrument_field` | Source field containing the native instrument ID. |
| `instrument_id` | Expected native instrument ID. |
| `provenance` | Explanation of the identity evidence. |

### Why use a namespace?

Numeric IDs are not necessarily globally unique.

The namespace records the context in which those IDs have meaning.

For example:

```text
databento:XNAS.ITCH:2024-10-28
```

identifies the relevant source product and date context.

### Is this namespace a MarketForge instrument namespace?

No.

It describes the source-native identifiers.

The canonical instrument namespace is managed separately by the historical catalogue.

---

## Native Identity Validation Rules

When `[inputs.native_identity]` is supplied, all required fields must be present.

### String fields

The following must be nonempty strings:

```text
namespace
publisher_field
instrument_field
provenance
```

### Numeric fields

The following must be nonnegative integers:

```text
publisher_id
instrument_id
```

For example:

```toml
publisher_id = 2
instrument_id = 38
```

is valid.

Negative values are rejected.

### Can I use floating-point IDs?

No.

These fields use strict integer validation.

For example:

```toml
instrument_id = 38.0
```

is not accepted as an integer identity.

### Can publisher and instrument fields be identical?

No.

For example:

```toml
publisher_field = "hd.instrument_id"
instrument_field = "hd.instrument_id"
```

is invalid.

The two field references must be different.

### Why?

The declaration identifies two distinct source-native concepts.

Using one field for both would make the binding ambiguous.

### Does the schema verify the provider's ID assignment?

It validates the declaration and supported source evidence.

It does not independently prove that a provider's historical identity documentation is correct.

Use the actual source contract to establish the meaning of the IDs.

---

## Native Identity Sampling

When native identity is supplied, registration preflight can check the corresponding fields against bounded source samples.

### Why is this useful?

Suppose a recipe declares:

```toml
publisher_id = 2
instrument_id = 38
```

but the sampled source records contain a different publisher or instrument ID.

This may indicate that the wrong file was selected or the declaration is incorrect.

Registration can detect such mismatches during preflight.

### Does sampling read the entire file?

No.

The supported source-layout check is bounded.

It does not independently verify every record in an arbitrarily large recording.

### Does every source format support native identity sampling?

Not necessarily.

The current bounded sampler supports particular CSV and JSONL identity paths.

For example, the specialized XLSX sample path does not provide the same native identity sampling behavior.

### What happens if the source format is unsupported?

Registration reports the applicable source or sampling incompatibility.

Do not bypass the check by inventing field names.

### Does successful sampling guarantee complete source validity?

No.

A bounded sample can establish useful evidence about selected records.

Complete record processing and integrity evaluation belong to normalization.

---

## Validation and plan review

Follow the [registration workflow](../workflows/acquisition/external-data.md#validation-stages) to validate, inspect, and apply a declaration.

## Why Registration Uses the Production Profile Compiler

The profile declaration controls how Rust interprets source records.

For example, it may describe:

```text
Timestamp encoding
Trade-side mapping
Quantity interpretation
Order-book snapshot behavior
Sequence handling
```

These rules are consumed by the Rust Engine.

### Why not validate only the TOML syntax?

Because a syntactically valid declaration may contain unsupported normalization behavior.

For example, an arbitrary transformation name might be valid TOML but not supported by the Engine.

### What does Forge do?

During preflight, Forge loads the installed profile and invokes the supported production semantic validation.

The resulting compiled definition and fingerprint become part of the registration evidence.

### Why preserve the fingerprint?

It identifies the approved interpretation associated with the registered source.

This helps establish which source-format definition was used.

### Does successful compilation prove all source records are valid?

No.

It proves the declaration is accepted by the compiler.

Complete record validity belongs to normalization.

---

## Bounded Source-Layout Checks

Registration preflight also checks representative source data.

### Why?

Suppose a profile declares:

```text
CSV with five columns
```

but the selected file contains a different structure.

The mismatch may be detectable before registering the file.

### What can preflight inspect?

Depending on the source format, the bounded checks can examine:

- Source container compatibility.
- Physical record layout.
- Field structure.
- Expected instrument identity.
- Optional native identity values.

### What if the source is unsupported?

Registration reports the applicable incompatibility.

For example, an unsupported binary format does not become a valid JSONL source merely because the profile ID is correct.

### What does bounded mean?

The preflight checks only the source evidence required by the supported sampling procedure.

They do not scan every record in an arbitrarily large source file.

### Why is this distinction important?

An early sample might be valid while later records contain malformed values.

Such failures may only become visible during full normalization.

**Registration preflight checks source compatibility. Normalization checks actual processing behavior.**

---
