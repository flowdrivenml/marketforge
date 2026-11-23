# Create a normalization profile

For processing budgets, integrity, and reconciliation, see [Normalization runtime configuration](../configuration/normalization-runtime.md).

## What a profile does

A profile tells the Rust Engine how to read one source layout and interpret its records as canonical trades or order-book events. Acquisition recipes select what to acquire. [Normalization runtime settings](../configuration/normalization.md) control execution. The profile defines what the raw values mean.

<a id="profile-identity-and-installation"></a>
A profile ID has four components, `exchange.instrument_type.market_category.data_type`. The first three match the venue and market, and the final component is `trades` or `depth`. Name the file `<id>.toml`. Installation places it under `data/.config/engine/normalize/profiles/trades/` or `depth/`, according to the suffix. Use the execution venue for data supplied by an external vendor.

## 1. Inspect a small source sample

Run from the project root. For example, inspect the bundle’s headerless trade CSV.

```bash
mf normalize profiles sample --file docs/content/examples/catalogue/synthetic-trades.csv --no-header | cpout
```

Pass `--header` for named CSV columns or `--no-header` for positional columns. If neither is set, the preview shows the first record as data and marks header status unknown. Use `--member` to select a stored or deflated ZIP entry. The inspector also accepts plain and gzip-compressed CSV and JSONL. For TAR.GZ and workbook inputs, inspect a small extracted text example or use a workbook viewer.

The **Source structure** table shows inferred representations, up to three distinct observed examples, and missing counts. JSONL also counts explicit nulls. Arrays stay grouped. Use `--fields` to skip the record preview or `--rows 10` to see more records. Human output defaults to five data records and accepts 1–20, within a 65,536-byte decoded prefix and a 64-field display limit. The sample covers the beginning of the input. It cannot establish total records or whole-file types.

For CSV, use the displayed one-based position when the profile declares positional fields. Resolve duplicate or empty headers before relying on name-based mappings. For nested JSONL, a row labeled `data.ts` maps to `[fields."data.ts"]` in TOML. The quotes keep the dotted path as one TOML key, and the Engine follows the nested JSON components. A literal JSON key named `"data.ts"` is ambiguous for this lookup and is omitted. Suggested types reflect the observed values. Choose actual field types and encodings using the source specification, then use `mf normalize profiles new PROFILE_ID` and `mf normalize profiles validate --file PATH`.

Write down the source facts before choosing mappings.

| Check | What to establish |
| --- | --- |
| Packaging and layout | Outer wrapper, inner file type, and how records are organized. Identify the real header, column positions, JSON paths, or worksheet columns. |
| Time and order | Confirm the timestamp unit or text encoding, and whether file order must be preserved. |
| Price, size and side | Identify the relevant fields, whether size is in base units, quote units or contracts, and what side tokens or signs mean. |
| Depth events | Find the initialization snapshot and subsequent updates. Confirm how to distinguish them, whether updates replace or change quantities, what zero means, and whether sequence numbers carry continuity guarantees. |

A sample shows the shape and actual values. Use the vendor’s specification to confirm facts the sample cannot establish, such as quantity units or sequence resets. Then choose which raw fields will supply each canonical value.

## 2. Create a trade profile

The fictional source below has one execution with four columns and no header.

```csv
1730125800000000000,233.62,10,B
```

Save this complete definition as `example_exchange.equity.equity.trades.toml`, or copy the [example profile](../examples/catalogue/example_exchange.equity.equity.trades.toml).

```toml
schema_version = 2
kind = "profile"
id = "example_exchange.equity.equity.trades"
format_code = "EXAMPLE-EXCHANGE-T1"
exchange = "example_exchange"
instrument_type = "equity"
market_category = "equity"

[source]
container = "plain"
format = "csv"
record_format = "rows"
header = false
ordering = "chronological"
granularity = "daily"

[fields.timestamp]
type = "integer"
position = 1
encoding = "unix_nanoseconds"

[fields.price]
type = "decimal"
position = 2

[fields.size]
type = "decimal"
position = 3

[fields.side]
type = "string"
position = 4

[normalization]
target = "trade"
quantity_encoding = "base"

[normalization.event_timestamp]
field = "timestamp"

[normalization.price]
field = "price"

[normalization.quantity]
field = "size"

[normalization.side]
transform = "map"
field = "side"

[normalization.side.values]
b = "buy"
s = "sell"
```

<a id="schema-version"></a>
<a id="required-trade-mappings"></a>

1. **Identity.** Use schema 2 and `kind = "profile"`. Set the venue, instrument type and market category together with the ID and [format code](profile-fields.md#format_code).
2. **`[source]`.** `plain` is the wrapper, `csv` is the physical format, and `rows` describes the record layout. Set each independently to match the input.
3. **`[fields.*]`.** Positions are one-based. Here the third column is named `size` in the profile. Timestamp `encoding` controls decoding. Optional precision metadata describes the source.
4. **`[normalization]`.** Map `size` to canonical quantity and declare its vendor units. The side lookup lowercases the source token before matching `b` or `s`.

The row represents a buy at timestamp `1730125800000000000` ns, priced at 233.62 with 10 base units and 2336.20 quote units. The source provides no native trade ID.

## 3. Create a depth profile

This JSONL feed starts with a snapshot, then sends an update. Save both lines or use the [input sample](../examples/catalogue/incremental/synthetic-depth.jsonl).

```jsonl
{"timestamp":1730125800000000000,"type":"snapshot","bids":[[233.61,10]],"asks":[[233.63,12]]}
{"timestamp":1730125800000000001,"type":"delta","bids":[[233.61,4]],"asks":[[233.63,0],[233.64,8]]}
```

Save the matching profile as `example_exchange.equity.equity.depth.toml`, or copy the [example profile](../examples/catalogue/incremental/example_exchange.equity.equity.depth.toml).

```toml
schema_version = 2
kind = "profile"
id = "example_exchange.equity.equity.depth"
format_code = "EXAMPLE-EXCHANGE-B1"
exchange = "example_exchange"
instrument_type = "equity"
market_category = "equity"

[source]
container = "plain"
format = "jsonl"
record_format = "events"
ordering = "chronological"
granularity = "daily"

[fields.timestamp]
type = "integer"
encoding = "unix_nanoseconds"

[fields.type]
type = "string"

[fields.bids]
type = "array"
level_format = ["price", "quantity"]

[fields.asks]
type = "array"
level_format = ["price", "quantity"]

[normalization]
quantity_encoding = "base"
event_timestamp = { field = "timestamp" }
bids = { field = "bids" }
asks = { field = "asks" }

[[normalization.rules]]
target = "l2_snapshot"
event_filter = { field = "type", equals = "snapshot" }
snapshot = { type = "single_event" }

[[normalization.rules]]
target = "l2_update"
event_filter = { field = "type", equals = "delta" }
update_semantics = { type = "absolute_level", zero_quantity = "delete", nonzero_quantity = "set" }
```

<a id="snapshot-and-update-semantics"></a>

Use either one `target` or an array of rules. Here each rule selects a different raw `type`. Both inherit the common timestamp and side mappings and cannot override them. Each bid or ask entry is a positional pair of price and quantity.

The snapshot initializes the book. The delta sets the bid size at 233.61 to 4, deletes the ask at 233.63 because its size is zero, and adds an ask at 233.64. The resulting book is **bid 233.61 × 4** and **ask 233.64 × 8**. This sample relies on chronological source order and has no native sequence. A relative feed needs its own supported rule. Independent bounded observations describe the recorded levels and cannot establish continuous reconstruction. If the source supplies standalone snapshots, start with the [single-target example](../examples/catalogue/example_exchange.equity.equity.depth.toml) instead.

## 4. Validate, check, and install

With the [managed normalizer installed](../getting-started/installation.md), create a draft with `new` and replace its starter values, or copy a complete example to a scratch directory. This sequence shows the trade draft. Use the depth filename and definition for depth.

```bash
id=example_exchange.equity.equity.trades
draft="data/.config/engine/normalize/profile-drafts/$id.toml"
mf normalize profiles new "$id" | cpout
nvim "$draft"
mf normalize profiles validate --file "$draft" | cpout
```

Validation asks the Rust compiler to check the definition and candidate catalogue. To check interpretation, use an isolated workspace with its own profile directory and the testing registry. Install the draft there, then follow the [small recording check](../examples/catalogue/index.md) to register and normalize the sample and inspect the emitted records. Compare the exact timestamp, side, and units for trades and the initial and final levels for depth.

Install the checked definition in the intended workspace using the same command. Use `--testing` for the testing registry as shown. Profiles are installed in the workspace’s data root, so keep the testing data root separate when checking a draft. The database flag alone selects the registry. The venue must already exist in the selected registry. Installation reuses an equivalent definition without replacing its bytes and rejects a conflicting interpretation under the same ID. Extra ID suffixes cannot create another version.

```bash
mf normalize profiles add "$id" --from "$draft" --testing | cpout
```

## 5. Other supported layouts

| If your source uses… | See the field reference |
| --- | --- |
| Headerless CSV or column positions | [Header](profile-fields.md#sourceheader), [position](profile-fields.md#fieldsnameposition) |
| Nested JSON | [Raw field names and paths](profile-fields.md#fields) |
| Epoch timestamps or ISO 8601 | [Encoding](profile-fields.md#fieldsnameencoding) |
| Base, quote or contract quantities | [Quantity encoding](profile-fields.md#normalizationquantity_encoding) |
| Signed quantities or side tokens | [Side transforms](profile-fields.md#normalizationsidetransform), [quantity transform](profile-fields.md#normalizationquantitytransform) |
| Tuple or object depth levels | [Positional layout](profile-fields.md#fieldsnamelevel_format), [object members](profile-fields.md#fieldsnamelevel_fields) |
| Grouped snapshots or bounded observations | [Snapshot modes](profile-fields.md#normalizationsnapshottype), [bounded depth](profile-fields.md#normalizationsnapshotlevels_per_side) |
| Relative depth updates | [Update semantics](profile-fields.md#normalizationupdate_semanticstype) |
| Native symbols, execution IDs or sequences | [Instrument](profile-fields.md#normalizationinstrument), [trade ID](profile-fields.md#normalizationtrade_id), [sequence policy](profile-fields.md#sequence-and-book-metadata) |
| XLSX snapshots | [Fixed workbook layout](profile-fields.md#sourceworkbook_sheets) |
| Gate.io feed variants | [Complete feed definitions](profile-fields.md#feeds) |
