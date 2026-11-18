# Bring external data

See the [BYOD guide](../../examples/bring-your-own-data/index.md) for the complete workflow and a real recording example.

Register the [venue, instrument, historical specifications, and Rust profiles](../catalogue/index.md) first. Archive recipes refer to those existing entities and record separate source provider and product information.

The complete [synthetic archive example](../../examples/catalogue/registration.toml) registers one trade file and one L2 depth file. Each input gives its canonical instrument, profile ID, content type, and file selection. The declared half-open coverage interval applies to every file in the recipe. Add an optional `specification` fingerprint to an input to pin its applicable historical evidence.

```bash
recipe="$PWD/docs/content/examples/catalogue/registration.toml"
mf acquisition register inspect "$recipe"
mf acquisition register validate "$recipe" --testing
mf acquisition register plan "$recipe" --testing
mf acquisition register run "$recipe" --testing
mf storage list --testing --origin external --exchange example_exchange --instrument-type equity
```

Validation requires the previously registered catalogue identity, one applicable historical declaration covering the interval, and an installed profile whose Rust fingerprint can be verified. Registration rejects mismatched profiles, conflicting source identities, and inconsistent sampled layouts before publishing managed copies. It retains the declaration and profile fingerprints beside the input checksum. The sample is bounded. Complete event validity belongs to normalization.

Repeating registration can reuse an existing managed copy after checksum verification. Provider, product, source root, and source key describe acquisition provenance. Supply the real execution venue separately. Native publisher and instrument identifiers can be recorded as provenance when available and remain scoped to the source.

## Raw-only preservation

Use `mf acquisition import` to preserve local bytes whose interpretation is still incomplete. For example, create a draft with `mf acquisition import new recorder --from ./recordings/trades.csv`, edit its explicit venue and source identity, and run `mf acquisition import run recorder --testing`. The [legacy import configuration](../../configuration/acquisition.md#external-import-configuration) describes its fields. Raw-only import can retain declarations and reuse managed copies. Use `acquisition register` to bind a source to a verified catalogue and profile interpretation.


## Creating Historical Normalization Profiles

A historical normalization profile tells MarketForge how to interpret a particular source format and convert its records into canonical trades or order-book events.

Profiles are written manually as **self-contained TOML files using `schema_version = 2`**. Each file contains the source format, raw field definitions, and normalization mappings. The production Rust loader validates and compiles these declarations. [^profile-schema]

A profile describes a reusable interpretation of data. The concrete instrument, input files, historical contract specifications, and output locations are supplied separately.

| Configuration | Responsibility |
| --- | --- |
| Profile | Source layout, timestamp and quantity units, field mappings, trade-side interpretation, and order-book event semantics. |
| Registration and instrument metadata | Source provenance, concrete instrument identity, historical specifications, and coverage. |
| Normalization job | Selected inputs, selected profile, resolved instruments, and output destinations. |
| Runtime configuration | Workers, memory budgets, Parquet sizing, and other execution settings. |

Create a profile only after inspecting representative source records. A syntactically valid declaration can still assign the wrong timestamp unit, reverse trade sides, or interpret contract counts as asset quantities.

## Profile Names and Locations

### Profile identity

Every profile ID has exactly four components:

```text
exchange.instrument_type.market_category.data_type
```

Examples:

```text
bybit.spot.spot.trades
binance.perpetual.linear.trades
binance.perpetual.inverse.depth
okx.future.inverse.depth
```

The final component is `trades` or `depth`. Use `depth` in profile IDs even where other parts of acquisition use the term `order_book_l2`.

The filename must exactly match the declared ID:

```text
binance.spot.spot.trades.toml
```

The corresponding metadata must agree:

```toml
id = "binance.spot.spot.trades"
exchange = "binance"
instrument_type = "spot"
market_category = "spot"
```

Native acquisition includes `bybit`, `binance`, `okx`, `bitget`, and `gateio`. For a user-supplied dataset, declare the execution venue in the catalogue and use its code in the profile. The supported instrument/category combinations are: [^profile-schema] [^shared-models]

| `instrument_type` | Valid `market_category` |
| --- | --- |
| `spot` | `spot` |
| `perpetual` | `linear`, `inverse` |
| `future` | `linear`, `inverse` |
| `option` | `option` |
| `equity` | `equity` |

IDs cannot contain an additional version or variant component. For example, these are invalid:

```text
binance.spot.spot.trades.v2
binance.spot.spot.trades.custom
```

### Catalogue directories

The orchestration catalogue uses these directories relative to the project root: [^profile-cli]

```text
data/.config/historical/profiles/trades/
data/.config/historical/profiles/depth/
```

For example:

```text
data/.config/historical/profiles/trades/binance.spot.spot.trades.toml
data/.config/historical/profiles/depth/binance.spot.spot.depth.toml
```

Keep installed profiles directly inside the appropriate directory so the orchestration discovery command finds them.

Prepare new definitions in a separate working directory and validate them before installation. Profile files must be regular files, must not be symlinks, and must not exceed 1 MiB.

### One interpretation per profile ID

The ID represents the exchange, instrument type, market category, and data type. It does not provide a general namespace for multiple independently named source layouts.

If an installed profile already uses an ID, `profiles add` compares the compiled interpretations. It reuses an equivalent definition and rejects a different interpretation under the same ID.

Consequently, an arbitrary new layout cannot coexist with an existing layout merely by adding `.custom` to its ID. Changes to an installed interpretation require a deliberate catalogue change; the current CLI does not provide a profile overwrite command. [^profile-cli]

## Structure of a Profile

A schema-2 profile has three main sections after its identity metadata:

| Section | Meaning |
| --- | --- |
| `[source]` | Packaging, physical format, record layout, ordering, and optional source contracts. |
| `[fields.<name>]` | The names, types, positions, and relevant encodings of raw fields. |
| `[normalization]` | Mappings from raw fields to canonical values and event operations. |

The required top-level metadata looks like this:

```toml
schema_version = 2
kind = "profile"
id = "binance.spot.spot.trades"
format_code = "BINANCE-T1"
exchange = "binance"
instrument_type = "spot"
market_category = "spot"
```

This is only the metadata portion of a profile. The source, fields, and normalization sections are also required.

Use ordinary TOML tables for mappings. A field reference is written as:

```toml
[normalization.price]
field = "price"
```

The compiler translates this into its internal runtime representation. User-authored schema-2 files should use `field`, rather than copying internal `source` mappings or generated `raw_schema` objects from resolved jobs.

For multiple depth operations, use `[[normalization.rules]]`. Do not use a top-level `[[rules]]` array in a schema-2 profile. [^profile-schema]

### Format codes

The format code identifies the source format used in profile/task matching. Its prefix must match the exchange, and its letter must match the data type:

| Exchange | Trades | Depth |
| --- | --- | --- |
| Bybit | `BYBIT-T<number>` | `BYBIT-B<number>` |
| Binance | `BINANCE-T<number>` | `BINANCE-B<number>` |
| OKX | `OKX-T<number>` | `OKX-B<number>` |
| Bitget | `BITGET-T<number>` | `BITGET-B<number>` |
| Gate.io | `GATE-T<number>` | `GATE-B<number>` |

The number must be positive and cannot start with zero. Gate.io uses the prefix `GATE`.

Use the code assigned to the actual source format. A code that satisfies this naming rule does not establish compatibility with a particular exchange archive. [^profile-schema]

## Describing the Source

### Required source settings

For a plain CSV trade file:

```toml
[source]
container = "plain"
format = "csv"
record_format = "rows"
header = true
ordering = "chronological"
granularity = "daily"
```

The supported declaration values are: [^profile-schema] [^shared-models]

| Setting | Values and requirements |
| --- | --- |
| `container` | `plain`, `gzip`, `zip`, `tar_gzip`. |
| `format` | `csv`, `jsonl`, `xlsx`. Actual execution support also depends on the data type and layout. |
| `record_format` | `rows`, `events`, `snapshot_rows`, `book_rows`. Trades require `rows`; depth requires one of the other values. |
| `header` | Boolean. Required for CSV and XLSX. |
| `ordering` | `chronological` or `timestamp_sortable`. `not_chronological` is also accepted as an alias for `timestamp_sortable`. |
| `granularity` | `daily`, `hourly`, `monthly`. |

`container` describes the outer packaging. `format` describes the records inside that packaging. For example, a ZIP archive containing CSV records uses `container = "zip"` and `format = "csv"`.

Trade processing supports CSV and JSONL. JSONL trade mappings use named JSON paths such as `hd.ts_event` and omit `header` and field positions.

Depth supports specific JSONL, positional CSV, and fixed XLSX paths. In particular, do not assume that `header = true` enables arbitrary headered CSV depth input, or that `decode = "json"` enables arbitrary JSON arrays embedded in CSV cells. Start from an existing profile for those layouts. [^runtime]

### Ordering

Use `ordering = "chronological"` when the source order is the order the engine must preserve.

Use a sortable declaration only when the source has a supported sorting path. Stateful order-book updates depend on event order and sequence continuity. The current runtime rejects timestamp sorting for ordinary stateful depth input; its supported workbook snapshot path has separate sorting behavior. [^runtime]

### Optional source metadata

The source table also accepts these settings:

| Setting | Accepted declaration |
| --- | --- |
| `archive_day_boundary` | `UTC` or `UTC+8`. |
| `daily_packaging` | `numbered_zip_chunks`. |
| `maximum_depth_per_side` | A positive integer. |
| `workbook_sheets` | XLSX only; the current supported layout requires `1`. |
| `trade_id_scope` | A supported native trade identity scope. |
| `trade_id_reset_domain` | Required or forbidden depending on the chosen identity scope. |

Declare these settings only when their meaning is established for the source. Source depth metadata is not a general instruction to truncate canonical output. [^profile-schema]

## Defining Raw Fields

### Names and positions

Declare a table for each raw field that the profile needs to describe:

```toml
[fields.time]
type = "integer"
position = 4
encoding = "unix_milliseconds"
```

The table name identifies the raw field. The normalization mapping references that exact name:

```toml
[normalization.event_timestamp]
field = "time"
```

For headered trade CSV, field references must match actual CSV header names.

For headerless CSV, declare every column with its position, including columns that are not mapped into canonical output. Positions are **1-based, unique, and contiguous**. If any field has a position, every declared field must have one, covering exactly `1` through the number of fields. [^profile-schema] [^trade-rules]

For example, this describes three columns in their physical order:

```toml
[fields.id]
type = "integer"
position = 1

[fields.price]
type = "decimal"
position = 2

[fields.qty]
type = "decimal"
position = 3
```

Writing field tables in a particular textual order does not establish headerless CSV column order. Use `position` explicitly.

### Field types

The accepted type strings are:

```text
decimal
integer
string
array
boolean
integer/string
uuid/string
integer/bool
string/integer
signed_integer
signed_decimal
```

Choose the type that describes the source representation. A descriptive type declaration does not introduce an arbitrary conversion operation. [^profile-schema]

Besides `type`, permitted field metadata keys include:

```text
encoding
position
precision
fractional_digits
observed_effective_precision
values
level_format
ordering
decode
update_precision
snapshot_precision
level_fields
```

Use only metadata relevant to the existing decoder. For example, `values` can describe known field values, but executable snapshot/update selection still requires an `event_filter`.

### Nested JSON fields

For nested JSON objects, use the dotted path as the field name and quote it in the TOML table header:

```toml
[fields."data.ts"]
type = "integer"
encoding = "unix_milliseconds"

[normalization.event_timestamp]
field = "data.ts"
```

The quotes make `data.ts` one field name in the TOML field map.

The depth JSON reader resolves dotted object paths. This is a simple path mechanism; it does not provide JSONPath wildcards, array-index expressions, or computed selectors. [^depth-rules]

### Timestamp encoding

Timestamp encoding belongs on the raw timestamp field:

```toml
[fields.timestamp]
type = "integer"
encoding = "unix_microseconds"
```

The supported values are: [^profile-schema] [^shared-models]

| Encoding | Source unit |
| --- | --- |
| `unix_seconds` | Seconds since the Unix epoch. Fractional seconds are supported when exactly representable in nanoseconds. |
| `unix_milliseconds` | Integer milliseconds since the Unix epoch. |
| `unix_microseconds` | Integer microseconds since the Unix epoch. |
| `unix_nanoseconds` | Integer nanoseconds since the Unix epoch. |
| `iso8601_nanoseconds` | String with an explicit UTC offset or `Z` and up to nine fractional digits. |

The engine converts these values into canonical nanoseconds. If either timestamp uses ISO 8601, declare that encoding for both the event timestamp and the receive timestamp when a receive timestamp is mapped.

Choose the unit from the source specification and verified sample records. Do not infer it solely from an exchange name or assume that all archives from an exchange use the same unit.

In schema 2, do not add `timestamp_encoding` to `[normalization]`. The compiler derives it from the field referenced by `event_timestamp.field`.

### Quantity encoding

Every normalization section must declare the unit of the raw quantity:

```toml
[normalization]
target = "trade"
quantity_encoding = "base"
```

| Encoding | Meaning |
| --- | --- |
| `base` | Quantity is expressed in base-asset units. |
| `quote` | Quantity is expressed in quote-asset units. |
| `contracts` | Quantity counts contracts. |

For trades, base quantity can be converted to quote quantity using price, and quote quantity can be converted to base quantity using price.

Contract normalization requires authoritative instrument metadata, including a positive contract value and its denomination. Supply those specifications through instrument metadata and the job-building process. A reusable profile should not contain an invented multiplier.

Spot profiles cannot use `quantity_encoding = "contracts"`. [^profile-schema] [^shared-models]

## Complete Trade Profile Example

This example describes the following **synthetic CSV**, using a Binance identity to demonstrate the supported schema. `BINANCE-T99` is an illustrative format code, not a claim about an official Binance archive format.

```csv
id,price,qty,time,is_buyer_maker
10001,100,0.5,1767225600000,false
10002,101,0.25,1767225601000,true
```

Save the definition as `binance.spot.spot.trades.toml` in your draft directory:

```toml
schema_version = 2
kind = "profile"
id = "binance.spot.spot.trades"
format_code = "BINANCE-T99"
exchange = "binance"
instrument_type = "spot"
market_category = "spot"

[source]
container = "plain"
format = "csv"
record_format = "rows"
header = true
ordering = "chronological"
granularity = "daily"
trade_id_scope = "source_local"

[fields.id]
type = "integer"
position = 1

[fields.price]
type = "decimal"
position = 2

[fields.qty]
type = "decimal"
position = 3

[fields.time]
type = "integer"
position = 4
encoding = "unix_milliseconds"

[fields.is_buyer_maker]
type = "boolean"
position = 5

[normalization]
target = "trade"
quantity_encoding = "base"

[normalization.event_timestamp]
field = "time"

[normalization.price]
field = "price"

[normalization.quantity]
field = "qty"

[normalization.side]
field = "is_buyer_maker"
transform = "map"

[normalization.side.values]
true = "sell"
false = "buy"

[normalization.trade_id]
field = "id"
```

This interpretation produces a buy trade of `0.5` base units at price `100`, followed by a sell trade of `0.25` base units at price `101`. Their derived quote quantities are `50` and `25.25`.

The example declares its IDs as source-local. That declaration belongs to the synthetic example; use the established identity contract for a real feed.

An existing `binance.spot.spot.trades` profile may have a different interpretation. The installation command will reject that conflict, as described below.

### Required trade mappings

A trade profile requires mappings for:

| Mapping | Purpose |
| --- | --- |
| `event_timestamp` | Exchange event time. |
| `price` | Trade price. |
| `quantity` | Raw quantity, interpreted using `quantity_encoding`. |
| `side` | Canonical buy, sell, or explicitly unknown aggressor. |

`trade_id` and `sequence` are optional mappings. The implemented optional trade flag is `optional.is_rpi`. A trade profile compiles to exactly one trade normalization. [^trade-rules] [^runtime]

### Trade-side transformations

For existing buy/sell text:

```toml
[normalization.side]
field = "side"
transform = "casefold"
```

The current implementation accepts `buy`, `Buy`, `BUY`, `sell`, `Sell`, and `SELL`. Do not depend on whitespace trimming or arbitrary mixed-case normalization.

For a coded or Boolean field, use `transform = "map"` with explicit values, as in the complete example. Mapping keys must be lowercase; use `buy`, `sell`, and `unknown` as the canonical values. An absent or unmapped source side is invalid.

For signed quantities, apply the magnitude and side interpretations separately:

```toml
[normalization.quantity]
field = "size"
transform = "abs"

[normalization.side]
field = "size"
transform = "sign_to_side"
positive = "buy"
negative = "sell"
```

Both mappings read the original raw value. The quantity mapping takes its magnitude, while the side mapping interprets its sign. Zero cannot establish a sign-based trade side.

These transformations are a fixed supported vocabulary. Fields do not accept arbitrary expressions, multiplication formulas, defaults, or custom functions. [^trade-rules]

### Optional trade flags

A Boolean-like RPI field can be mapped as:

```toml
[normalization.optional.is_rpi]
field = "RPI"
transform = "bool"
```

A source code can be tested for equality instead:

```toml
[normalization.optional.is_rpi]
field = "source"
transform = "equals"
value = 1
```

The referenced raw field must also be declared under `[fields]`. [^trade-rules]

### Trade ID scope

A native trade ID mapping and an identity scope answer different questions. The mapping identifies the source column; the scope declares where the values are reliable identities.

Supported contracts are: [^trade-rules]

| `trade_id_scope` | Meaning | `trade_id_reset_domain` |
| --- | --- | --- |
| `global` | IDs have the declared global uniqueness contract. | Omit. |
| `instrument_scoped` | Identity is scoped to the concrete instrument and declared reset domain. | Required, nonempty. |
| `ordered_run` | Identity is established within a checked, non-reset run of one concrete stream. | Required, nonempty. |
| `source_local` | IDs are local to a source and do not establish cross-source uniqueness. | Omit. |
| `unreliable` | IDs are not reliable event identities. | Omit. |

For example, when supported by the source contract:

```toml
[source]
# Include the other required source settings here.
trade_id_scope = "instrument_scoped"
trade_id_reset_domain = "instrument_lifetime"
```

An explicit identity contract requires a native `[normalization.trade_id]` field mapping. A reset domain without a scope is invalid.

Only declare a scope supported by source evidence. Equal timestamps, prices, or quantities do not establish that two observations represent the same trade.

## Complete Depth Profile Example

This example describes a **synthetic JSONL stream** containing snapshots and absolute level updates. `BINANCE-B99` is illustrative.

```jsonl
{"type":"snapshot","timestamp":1767225600000,"bids":[["99","2"],["98","1"]],"asks":[["101","3"]]}
{"type":"delta","timestamp":1767225601000,"bids":[["99","0"]],"asks":[["101","4"]]}
```

Save the definition as `binance.spot.spot.depth.toml` in your draft directory:

```toml
schema_version = 2
kind = "profile"
id = "binance.spot.spot.depth"
format_code = "BINANCE-B99"
exchange = "binance"
instrument_type = "spot"
market_category = "spot"

[source]
container = "plain"
format = "jsonl"
record_format = "events"
ordering = "chronological"
granularity = "daily"

[fields.type]
type = "string"

[fields.timestamp]
type = "integer"
encoding = "unix_milliseconds"

[fields.bids]
type = "array"
level_format = ["price", "quantity"]

[fields.asks]
type = "array"
level_format = ["price", "quantity"]

[normalization]
quantity_encoding = "base"

[normalization.event_timestamp]
field = "timestamp"

[normalization.bids]
field = "bids"

[normalization.asks]
field = "asks"

[[normalization.rules]]
target = "l2_snapshot"

[normalization.rules.event_filter]
field = "type"
equals = "snapshot"

[normalization.rules.snapshot]
type = "single_event"

[[normalization.rules]]
target = "l2_update"

[normalization.rules.event_filter]
field = "type"
equals = "delta"

[normalization.rules.update_semantics]
type = "absolute_level"
zero_quantity = "delete"
nonzero_quantity = "set"
```

The snapshot initializes the book. The second event removes the bid at `99` and sets the ask at `101` to quantity `4`. The bid at `98` remains at quantity `1`.

This is an unsequenced example because its records contain no native sequence counter.

### Common mappings and operation-specific rules

Mappings directly under `[normalization]` are shared by every rule. In the example, timestamp, bids, asks, and quantity encoding are common.

Each `[[normalization.rules]]` entry adds operation-specific settings. A rule cannot override a mapping already declared in the common section, even by repeating the same value.

Choose one of these structures:

- A single `normalization.target`.
- A nonempty `normalization.rules` array.

Do not declare both. The catalogue also rejects duplicate targets: a depth profile can have one `l2_snapshot` rule and one `l2_update` rule, rather than several snapshot rules distinguished only by filters. [^profile-schema]

### Event filters and rule order

For JSON/event depth processing, every record must match exactly one normalization rule.

Filters require a declared source field and exactly one of:

```toml
[normalization.rules.event_filter]
field = "type"
equals = "snapshot"
```

or:

```toml
[normalization.rules.event_filter]
field = "type"
in = ["delta", "update"]
```

The actual filter value must be a string, and matching is exact.

Rule order does not provide first-match precedence. A record matching both rules is an error, and a record matching neither rule is also an error. Use disjoint filters that cover the records intended for that profile. [^depth-rules]

### Array and object level layouts

A depth mapping must use either both bid/ask arrays or all three scalar mappings: `price`, `quantity`, and `side`.

For positional array levels, the supported declarations are:

```toml
level_format = ["price", "quantity"]
```

or:

```toml
level_format = ["price", "quantity", "order_count"]
```

The compiler derives the array extraction indexes.

For levels represented as objects, such as `{"p":"99","q":"2"}`, declare the property names:

```toml
[fields.bids]
type = "array"

[fields.bids.level_fields]
price = "p"
quantity = "q"

[normalization.bids]
field = "bids"
```

Provide the corresponding declaration for asks. `level_fields` requires `price` and `quantity` and can include `order_count`. When both sides share the same array, put a separate `level_fields` table under each normalization side. [^profile-schema] [^depth-rules]

### Snapshot and update semantics

Supported snapshot types include:

| Snapshot type | Intended representation |
| --- | --- |
| `single_event` | A complete snapshot in one event. |
| `single_row` | A supported snapshot contained in one row. |
| `group_rows` | A snapshot assembled from several rows; requires `group_by`. |
| `bounded_observation` | One top-N state per chronological JSONL message. Set `levels_per_side` to the same value as `source.maximum_depth_per_side`. With `sequence_start` mapped, `sequence_order = "nondecreasing"` allows repeats and jumps and rejects decreases. |

For grouped rows:

```toml
[normalization.rules.snapshot]
type = "group_rows"
group_by = "timestamp"
```

Here, `group_by` is a raw field name, and that field must be declared.

For absolute updates, the supplied quantity replaces the existing level quantity; zero deletes the level. For relative updates, the supplied quantity changes the existing level quantity.

The implemented scalar CSV relative-update path uses settings such as:

```toml
[normalization.rules.update_semantics]
type = "relative_level"
action_source = "action"
add_action = "add"
subtract_action = "subtract"
emit = "resulting_absolute_level"
zero_result = "delete"
nonzero_result = "set"
```

`action_source` must name a declared field, and action values must match the actual source.

Relative updates are not generally implemented for array-based JSON events. Use an existing supported scalar profile for that case. [^depth-rules]

### Sequence declarations

Declare native sequence fields only when their continuity meaning is established.

The runtime derives its sequence policy from the presence of these mappings: [^runtime]

| `sequence_start` | `sequence_count` | Policy |
| --- | --- | --- |
| Absent | Absent | Unsequenced. |
| Present | Absent | Consecutive sequence. |
| Present | Present | Ranged sequence. |
| Absent | Present | Invalid configuration. |

For a consecutive sequence:

```toml
[normalization.sequence_start]
field = "update_id"
```

For a source that supplies merged sequence ranges:

```toml
[normalization.sequence_start]
field = "begin_id"

[normalization.sequence_count]
field = "merged_count"
```

All rules in a depth task must imply the same sequence policy. Do not add empty sequence mappings as placeholders; their presence changes runtime behavior.

Optional `normalization.book` metadata has its own restricted vocabulary. It does not replace the executable sequence mappings or provide general depth truncation controls. Copy such declarations from a compatible existing profile and validate them against the compiler. [^profile-schema]

## Instrument Identity and Specialized Formats

### Mixed-instrument inputs

A profile exposes native identity where the supported reader requires it. For depth records containing a native symbol:

```toml
[fields."data.s"]
type = "string"

[normalization.instrument]
field = "data.s"
lookup = "catalog"
```

The concrete instruments and their historical specifications must still be available to the job. `lookup = "catalog"` does not fetch missing specifications from an exchange.

Family routing supports specific CSV and JSONL identity paths. The current runtime rejects mixed-instrument XLSX family routing. Trade CSV identity extraction has its own supported header recognition. JSONL trades can map the catalogue symbol explicitly with `normalization.instrument` using `lookup = "catalog"`. [^depth-rules] [^runtime]

### XLSX profiles

XLSX support has a fixed layout. The current compiler requires one worksheet with headers, exactly the field names `timestamp`, `asks`, and `bids`, a timestamp encoded as `unix_seconds`, and supported ascending timestamp sorting.

The ordering declaration is:

```toml
[normalization.input_ordering]
order = "ascending"
sort_by = "timestamp"
```

Use the existing workbook profile as the starting point. Adding field declarations does not enable arbitrary worksheets or column layouts. [^profile-schema]

### Gate.io feed variants

Gate.io depth profiles can contain `snapshot` and `incremental` feed variants.

Each variant provides its own complete `source`, `fields`, and `normalization` sections under `feeds.snapshot` or `feeds.incremental`. These sections are not partial overlays that inherit missing declarations from the default.

The top-level profile still requires its own source, fields, and normalization. Feed selection is a supported Gate.io depth feature, rather than a general variant mechanism for every exchange. [^profile-schema]

## Validating and Installing a Profile

### Inspect the existing catalogue

Run commands from the project root:

```bash
mf acquisition profiles list

mf acquisition profiles list \
  --exchange binance \
  --instrument-type spot \
  --market-category spot \
  --type trades

mf acquisition profiles inspect binance.spot.spot.trades
```

Prefer starting from an existing profile with the same physical layout and event semantics.

`list` and `inspect` check TOML syntax and namespace consistency. They do not perform full Rust semantic validation. [^profile-cli]

### Build the managed normalizer

Semantic validation uses the managed Rust normalizer. Build it when it is not already installed or needs updating:

```bash
mf engine build normalize
mf engine status
```

The managed build installs the normalizer and its installation metadata. Merely copying an arbitrary executable into `.bin` does not satisfy the managed installation checks. [^profile-cli]

### Validate a draft

To validate a manually authored file:

```bash
mf acquisition profiles validate \
  --file ./profile-drafts/binance.spot.spot.trades.toml
```

For machine-readable output:

```bash
mf --json acquisition profiles validate \
  --file ./profile-drafts/binance.spot.spot.trades.toml
```

Validation stages a temporary catalogue containing the installed profiles and the candidate, then invokes production Rust semantic compilation.

An installed profile can also be validated by ID:

```bash
mf acquisition profiles validate binance.spot.spot.trades
```

Supply either the installed ID or `--file`, not both.

Leave `unsupported_reason` absent for an executable profile. Profiles with that setting are inventory-only and are rejected by the executable-profile validation workflow. [^profile-cli]

### Install a validated definition

```bash
mf acquisition profiles add binance.spot.spot.trades \
  --from ./profile-drafts/binance.spot.spot.trades.toml
```

Installation validates frozen candidate bytes and installs an independent copy.

When the ID already exists:

| Candidate interpretation | Result |
| --- | --- |
| Equivalent compiled interpretation | Existing profile is reused. |
| Different compiled interpretation | Installation fails with a conflict. |

A changed same-ID candidate can pass `validate --file` and subsequently fail `add`. Validation substitutes it into a temporary catalogue; installation additionally compares it with the currently installed interpretation.

The current CLI has no `profiles update` or `--force` option. Resolve a same-ID interpretation change deliberately in the catalogue and associated fixtures. [^profile-cli]

### Verify against actual records

Semantic validation establishes that the profile compiles. It does not establish that the profile correctly describes the source bytes.

Use the normal orchestration workflow to process a small, registered sample with authoritative instrument metadata. Check representative output values and the integrity report.

A useful verification sample should establish:

- Timestamps correspond to known source times after conversion to nanoseconds.
- Prices, quantity units, and contract conversions are correct.
- Trade sides match the intended aggressor-side convention.
- Native trade IDs have the declared scope.
- Snapshots initialize the book correctly.
- Updates set, add, subtract, or delete levels according to the source contract.
- Sequence behavior matches the actual feed.
- Parsing failures, rejected records, and integrity findings are understood.

For a profile covering several archive periods or source layouts, include representative samples from those periods. Passing a single recent file does not establish that an older layout has the same interpretation.

## Common Authoring Problems

| Problem | What to check |
| --- | --- |
| Filename mismatch | The filename must be exactly `<id>.toml`. |
| Invalid namespace | Check all four ID components and the corresponding metadata. |
| Invalid format code | Check the exchange prefix, `T`/`B` letter, and positive numeric suffix. |
| Unknown field reference | Every `field` mapping must reference a declared raw field exactly. |
| Wrong headerless CSV values | Declare all columns with explicit, contiguous, 1-based positions. |
| Incorrect timestamps | Verify the raw field’s `unix_*` encoding against source evidence. |
| Incorrect derivative quantities | Verify `quantity_encoding` and authoritative contract specifications. |
| Conflicting normalization rule | Move the mapping into either the common section or the individual rule; do not duplicate it. |
| Depth record matches multiple rules | Make the event filters disjoint. Reordering rules does not resolve ambiguity. |
| Depth record matches no rule | Check actual filter values, string types, and coverage of the intended record types. |
| Validation succeeds but input processing fails | Check the actual decoder path, packaging, header behavior, and source bytes. |
| Installation rejects a validated candidate | Check whether the installed same-ID profile has a different compiled interpretation. |

## Implementation References

The guide follows the schema-2 compiler and shared decoder sources available in the 17 November 2026 exports, together with the orchestration and normalization runtime exports from 18 November 2026.

The complete examples above were checked against those source contracts and parsed as TOML. Production Rust compilation and normalization of their sample records were not executed in the exported-source workspace; use the validation and sample-processing workflow in your checkout.

[^profile-schema]: `engine/src/historical/config/catalog/unified.rs`, `engine/src/historical/config/catalog.rs`, and `engine/src/historical/config/catalog/tests.rs`. These define schema-2 authoring, namespace compatibility, source and field validation, normalization compilation, and feed variants.

[^profile-cli]: `forge/historical/profiles/service.py`, `forge/cli/commands/profiles.py`, `forge/cli/commands/engine.py`, `forge/historical/engine/managed.py`, and `forge/shared/paths.py`. These define discovery, profile paths, validation, installation, and managed engine commands.

[^shared-models]: `engine/src/shared/domain/canonical/enums.rs`, `engine/src/shared/io/container.rs`, and `engine/src/shared/normalization/{config,quantity,timestamp}.rs`. These define supported exchanges, containers, and numeric encodings.

[^trade-rules]: `engine/src/shared/formats/trades/{spec,transforms,headers,native_identity}.rs` and `engine/src/shared/normalization/side.rs`. These define trade mappings, transforms, CSV field resolution, and identity contracts.

[^depth-rules]: `engine/src/shared/formats/depth/{spec,json,processor,scalar,csv,transforms}.rs`. These define depth mappings, filters, level extraction, and supported snapshot/update behavior.

[^runtime]: The supplied normalization runtime export, particularly `resources/classification.rs`, `worker/trades.rs`, `worker/depth.rs`, `worker/ordering.rs`, `sequence_policy.rs`, `family.rs`, and `task.rs`.
