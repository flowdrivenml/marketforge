# Normalized publications

Canonical Arrow rows, manifest and auxiliary evidence emitted by historical normalization. [Inspect published files](../../workflows/inspection/datasets.md) · [Inspect a Bybit depth boundary](../../getting-started/quickstart.md#verify-both-outputs)

A managed run records Engine execution, committed publication verification, and PostgreSQL registration as separate outcomes. A publication has an Engine `dataset_id` and a shared manifest. Registration assigns a publication ID and a separate catalogue ID to each eligible primary Parquet file. Use the primary-file ID with [dataset inspection](../../workflows/inspection/datasets.md), including `mf datasets verify ID --testing`. Use the [logical run ID](../cli/runs.md) for execution history and the execution result for attempt-specific report and intent paths.

Forge checks the committed job destination, manifest inventory, Parquet and auxiliary evidence, and applicable provenance hashes before registration. The registry links the publication and primary files to their source archives in the selected database. Keep the manifest, `processing-job.json`, `processing-runtime.json`, checksum companions, and auxiliary files with the publication. See [registration and recovery](../cli/normalize.md#registration-and-recovery) for the retained intent and original registry requirements.

## Physical Parquet Layout

## How Normalized Data Is Organized

The current normalization output is organized by logical stream.

A stream identifies a particular market and content type.

For example:

```text
bybit.linear.perpetual.BTCUSDT.trades
```

represents a logical trade stream.

Another stream might represent:

```text
bybit.linear.perpetual.BTCUSDT.depth
```

These streams can belong to different content-specific publications.

### Why separate streams?

Different instruments and content types have different identities and processing histories.

For example:

```text
BTCUSDT Trades
ETHUSDT Trades
```

should remain distinguishable even when they are processed by one normalization job.

### Stream directories

A normalized publication can contain:

```text
streams/
    <stream-fingerprint>/
```

The fingerprint identifies the stream's managed physical directory.

### Why use fingerprints instead of native symbols?

Physical directory identities need to be stable and unambiguous.

Source symbols may contain punctuation or differ across exchanges.

MarketForge retains the actual logical stream identity in publication metadata rather than requiring the directory name to encode every market property.

---

## Trade Publication Layout

A normalized trade stream can have the following structure:

```text
data/datasets/normalized/<engine-dataset-id>/
├── manifest.json
└── streams/
    └── <stream-fingerprint>/
        └── trades/
            ├── part-000000.parquet
            ├── part-000001.parquet
            └── sources.jsonl
```

This is an illustrative multi-part layout.

The actual number of Parquet parts depends on the output.

### Primary Parquet files

Files such as:

```text
part-000000.parquet
```

contain canonical trade records.

### Source ownership

The file:

```text
sources.jsonl
```

can retain the source ownership information required by supported publication layouts.

### Manifest

The shared:

```text
manifest.json
```

describes the committed publication.

### Does every trade publication contain `sources.jsonl`?

The presence and requirement depend on the publication protocol and processing behavior.

In particular, newer source-ownership-aware publications retain this evidence where required.

Do not assume every older normalization publication has identical auxiliary files.

Use the manifest to determine which files belong to the committed result.

---

## Depth Publication Layout

A normalized depth stream can have the following structure:

```text
data/datasets/normalized/<engine-dataset-id>/
├── manifest.json
└── streams/
    └── <stream-fingerprint>/
        └── depth/
            ├── part-000000.parquet
            ├── part-000001.parquet
            ├── events.parquet
            ├── segments.json
            └── boundary.json
```

### Why does depth need additional files?

Order-book observations are not always independent records.

For example, a source may publish:

```text
Snapshot
    |
    v
Update
    |
    v
Update
```

The updates depend on the state established by the snapshot.

A canonical Parquet file containing individual price-level changes does not necessarily preserve every relationship needed to reconstruct the original event sequence by itself.

The auxiliary files provide that additional information.

### Depth auxiliary files

| File | Purpose |
|---|---|
| `events.parquet` | Source-event index and canonical-row offsets. |
| `segments.json` | Reconstruction segments and their boundaries. |
| `boundary.json` | Source-boundary and book-state evidence. |

### Are these optional?

For the supported depth publication layouts requiring this evidence, they are necessary publication artifacts.

The manifest and publication validator establish the applicable requirements.

### Can I delete them after normalization?

No.

Preserve the auxiliary files together with the primary Parquet data.

Removing them can make correct reconstruction or downstream validation impossible.

---

## Parquet File Sizing

## Why Does MarketForge Split Output into Parts?

Historical recordings can produce very large datasets.

For example, several days of order-book data may contain millions of canonical price-level rows.

Writing everything into one enormous file can make storage management and inspection inconvenient.

MarketForge therefore supports configurable Parquet output targets.

### Relevant settings

The historical runtime contains:

```toml
[resources.parquet]
row_group_target_bytes = 67108864
file_target_bytes = 268435456
```

These values represent:

| Setting | Example |
|---|---:|
| Row-group target | 64 MiB |
| File target | 256 MiB |

The values are examples from the maintained strict runtime fixture.

### Where are they configured?

In:

```text
data/.config/engine/normalize.toml
```

They are runtime resource settings.

They are not independent fields added manually to each normalization task.

See [Normalization runtime](../configuration/normalization-runtime.md#4-parquet-output).

---

## Parquet Row Groups

Apache Parquet is a columnar storage format.

A Parquet file organizes its records into row groups.

Conceptually:

```text
part-000000.parquet
        |
        +-- Row Group 1
        |
        +-- Row Group 2
        |
        +-- Row Group 3
```

Each row group contains columnar data for a subset of the file's rows.

### Why use row groups?

They provide a physical organization suitable for analytical workloads.

For example, columnar readers can select relevant fields rather than decoding every column for every query.

### Does one row group equal one source archive?

No.

A row group is a physical storage unit.

It does not automatically correspond to:

- One exchange.
- One instrument.
- One day.
- One source archive.
- One order-book snapshot.

### Does row-group size change the market events?

The target controls writer organization.

It does not intentionally change the canonical interpretation of the source records.

### Why might row groups affect performance?

Their size can influence:

- Memory buffering.
- Writer batching.
- Compression behavior.
- Read performance.
- Physical file organization.

The appropriate value depends on the workload and the analytical access pattern.

---

## Parquet File Targets

The file-size target is:

```toml
file_target_bytes = 268435456
```

This is a desired physical file size.

### What happens when the output becomes large?

The writer can rotate into additional Parquet parts.

For example:

```text
part-000000.parquet
part-000001.parquet
part-000002.parquet
```

### Is the target a hard upper bound?

No.

The configured target is not necessarily an exact maximum for every physical file.

Actual sizes depend on:

- Writer buffering.
- Row-group boundaries.
- Encoded data size.
- Compression.
- File-rotation behavior.
- Finalization.

### What if a file exceeds the target?

A file larger than the configured target is not automatically invalid.

Investigate the actual writer behavior and publication evidence before classifying it as incorrect.

### Does the target determine the number of files?

It influences file rotation, but the actual number of parts also depends on the output volume and encoded representation.

---

## Canonical Trade Schema

## Why Does MarketForge Use Canonical Columns?

Different exchanges use different source field names.

For example:

```text
price
px
trade_price
```

may represent execution price in different source formats.

MarketForge normalizes supported source records into a consistent set of columns.

### Why is this useful?

It allows research code to work with a common representation.

For example, a program analyzing normalized trades can use:

```text
event_timestamp_ns
price
quantity_base
side
```

without separately implementing every exchange's raw source decoder.

### Does canonical mean all source details disappear?

No.

The publication retains relevant source and processing evidence separately.

Some supported source-specific values also have dedicated nullable canonical fields.

However, canonical normalization is not a guarantee that every original source field appears as its own Parquet column.

The source archives remain preserved in managed raw storage.

---

## Trade Columns

The current Rust trade schema contains the following fields.

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `event_timestamp_ns` | `int64` | No | Event timestamp in Unix nanoseconds. |
| `system_timestamp_ns` | `int64` | Yes | Receive/system timestamp when available. |
| `exchange` | `string` | No | Execution venue. |
| `instrument_id` | `int64` | No | Canonical instrument identity. |
| `symbol` | `string` | No | Instrument symbol. |
| `stream_id` | `string` | No | Logical stream identity. |
| `trade_id` | `string` | Yes | Native trade ID when available. |
| `sequence` | `uint64` | Yes | Applicable native sequence. |
| `side` | `string` | No | Canonical trade-side interpretation. |
| `price` | `decimal256(76,28)` | No | Execution price. |
| `quantity_base` | `decimal256(76,28)` | Yes | Quantity in base units. |
| `quantity_quote` | `decimal256(76,28)` | Yes | Quantity in quote units. |
| `quantity_contracts` | `decimal256(76,28)` | Yes | Quantity in contracts. |
| `is_rpi` | `boolean` | Yes | Retail price improvement indicator when available. |
| `trade_iv` | `decimal256(76,28)` | Yes | Trade implied volatility when available. |
| `mark_iv` | `decimal256(76,28)` | Yes | Mark implied volatility when available. |
| `index_price` | `decimal256(76,28)` | Yes | Index price when available. |
| `mark_price` | `decimal256(76,28)` | Yes | Mark price when available. |

This is the current canonical trade schema.

### Why are some fields nullable?

Not every source provides every kind of information.

For example, an ordinary spot trade may not provide:

```text
trade_iv
mark_iv
```

Those fields can remain null.

### Does null mean zero?

No.

A null value means the field does not contain an available value for that record.

It should not automatically be interpreted as zero.

### Why preserve several quantity representations?

Different financial products use different quantity conventions.

For example:

- Spot trades commonly use base quantity.
- Some feeds report quote quantity.
- Derivative sources can use contract counts.

MarketForge preserves the applicable canonical quantity representations rather than treating every source quantity as the same unit.

---

## Understanding Event and System Timestamps

A canonical trade can contain two timestamps.

```text
event_timestamp_ns
system_timestamp_ns
```

### Event timestamp

The event timestamp identifies the event time according to the supported source interpretation.

For example:

```text
1730125800510827641
```

This is an integer Unix nanosecond timestamp.

### System timestamp

The system timestamp can describe when the event was received or observed by the relevant source system.

Its exact origin depends on the source.

### Why preserve both?

An exchange event timestamp and a receiver timestamp can differ.

For example:

```text
Event time:
14:30:00.100

Receive time:
14:30:00.105
```

The difference may be relevant to latency analysis.

### Can I assume system timestamp always exists?

No.

It is nullable.

Some source recordings do not provide a supported receive/system timestamp.

### Can I assume system timestamp always comes after event time?

Do not make that assumption without checking the source's timestamp semantics and clock synchronization.

The column's presence alone does not establish a reliable latency measurement.

---

## Why Use Nanosecond Integers?

Financial market events can occur within very small time intervals.

For example:

```text
14:30:00.100000001
14:30:00.100000002
14:30:00.100000003
```

If timestamps were stored only to whole seconds, these events would appear to have identical times.

MarketForge uses integer nanoseconds to preserve the canonical timestamp precision.

### Does the terminal always show nanoseconds?

No.

The dataset CLI displays timestamps to whole seconds by default.

Use:

```bash
mf datasets head FILE_ID \
    --testing \
    --precise \
    --limit 10
```

to display nanosecond precision.

### Does display formatting change stored timestamps?

No.

The original canonical integer values remain in Parquet.

### What about JSON output?

Global JSON preserves the stored integer nanosecond fields.

This is useful for programmatic inspection.

---

## Why Use Decimal256?

The current canonical financial decimal type is:

```text
decimal256(76,28)
```

This is an Apache Arrow decimal type.

### What do the numbers mean?

The schema specifies:

```text
Precision: 76
Scale:     28
```

Precision describes the maximum number of decimal digits.

Scale describes the fixed number of digits after the decimal point.

### Why use this instead of floating point?

Financial values often require exact decimal representation.

For example:

```text
0.000001
```

is a precise decimal quantity.

Binary floating-point representations can introduce small approximation errors.

### Example

Consider:

```text
Price:    233.62
Quantity: 10
```

The corresponding monetary value is:

```text
2336.20
```

Using exact decimals helps preserve the intended arithmetic representation.

### Does Decimal256 mean every calculation automatically remains exact?

No.

The type provides a precise storage representation.

Calculations still need to use compatible decimal arithmetic and appropriate rounding rules.

### What about Polars?

Some analytical operations or library versions may not support every Decimal256 operation directly.

Inspect the actual Arrow schema and use supported decimal-aware processing.

Avoid silently converting exact financial values into lower-precision floating-point numbers without considering the consequences.

---

## Trade Identity and Side

The canonical schema contains:

```text
trade_id
side
sequence
```

These fields describe different things.

### Trade ID

A native trade identifier, when available.

### Side

The trade-side interpretation produced by the installed normalization profile.

### Sequence

An applicable source-native sequence value, when available.

### Why are they separate?

A trade ID identifies an execution under its supported native identity contract.

A sequence can describe source ordering or another provider-defined sequence domain.

The side describes the relevant trade direction.

### Does `trade_id` guarantee global uniqueness?

No.

Its uniqueness depends on the source contract.

For example, an ID may only be unique within one instrument or source run.

The source profile and retained metadata establish the applicable scope.

### Does equal timestamp and price prove duplicate trades?

No.

Two different trades can have identical timestamps, prices, and quantities.

Verified duplicate reconciliation requires stronger identity and content evidence.

See [Trade reconciliation](../configuration/normalization-runtime.md#7-trade-reconciliation).

---

## Canonical Depth Schema

## Why Is Depth Stored as Price-Level Rows?

Order-book data describes available liquidity or changes to the book.

For example:

```text
Bids:
99 → 2
98 → 1

Asks:
101 → 3
102 → 4
```

A source observation may contain several price levels.

MarketForge normalizes these into canonical depth rows.

### Example

One source snapshot might produce:

```text
Bid 99  Quantity 2
Bid 98  Quantity 1
Ask 101 Quantity 3
Ask 102 Quantity 4
```

This is one logical source observation represented by four canonical rows.

### Why is this important?

A source observation count and a canonical row count are different measurements.

For example:

```text
Source observations: 1
Canonical rows:       4
```

The difference is expected.

### Does one canonical row represent one complete order-book snapshot?

No.

A row can represent one price level or level operation.

The event index and reconstruction metadata describe how rows relate to the original source observations.

---

## Depth Columns

The current canonical depth schema contains:

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `event_timestamp_ns` | `int64` | No | Event timestamp in Unix nanoseconds. |
| `system_timestamp_ns` | `int64` | Yes | Receive/system timestamp when available. |
| `exchange` | `string` | No | Execution venue. |
| `instrument_id` | `int64` | No | Canonical instrument identity. |
| `symbol` | `string` | No | Instrument symbol. |
| `stream_id` | `string` | No | Logical depth stream. |
| `side` | `string` | No | Book side. |
| `price` | `decimal256(76,28)` | No | Price level. |
| `quantity_base` | `decimal256(76,28)` | Yes | Base-denominated quantity. |
| `quantity_quote` | `decimal256(76,28)` | Yes | Quote-denominated quantity. |
| `quantity_contracts` | `decimal256(76,28)` | Yes | Contract-denominated quantity. |
| `order_count` | `uint64` | Yes | Number of orders at the level when available. |

### Why is order count nullable?

Some L2 sources provide aggregate quantity at each price level without the number of individual orders.

Other sources include an order count.

The canonical schema supports both.

### Why does depth have fewer fields than trades?

Trade-specific fields such as:

```text
trade_id
trade_iv
mark_iv
```

do not belong to ordinary canonical depth rows.

Depth has its own physical schema and auxiliary reconstruction information.

---

## Absolute Depth Updates

Some sources publish absolute level updates.

For example:

```text
Existing bid:
Price 99 → Quantity 2
```

The source then reports:

```text
Price 99 → Quantity 5
```

Under absolute-level semantics, the resulting quantity is:

```text
5
```

not:

```text
7
```

### Why does the update type matter?

Some sources instead publish relative quantity changes.

The normalization profile must describe the actual source semantics.

### Zero quantity

For supported absolute updates, a zero quantity removes the keyed side and price from the corresponding book state.

For example:

```text
Bid 99 → Quantity 0
```

removes the bid at price 99.

### Why preserve zero updates?

A deletion is a meaningful order-book operation.

It should not be discarded simply because its resulting quantity is zero.

### Does the Parquet row alone describe the complete operation?

Not necessarily.

The row's meaning depends on the normalized source operation and its event context.

Use the corresponding event index and reconstruction metadata.

---

## Depth Event Indexes

## Why Does Depth Need `events.parquet`?

Consider a depth source containing two messages.

The first message is a snapshot with four price levels.

The second is an update affecting two levels.

Conceptually:

```text
Source Event 1
    |
    +-- Level Row 1
    +-- Level Row 2
    +-- Level Row 3
    +-- Level Row 4


Source Event 2
    |
    +-- Level Row 5
    +-- Level Row 6
```

The primary Parquet data contains six canonical rows.

However, there were only two source observations.

MarketForge needs to preserve that relationship.

This is the purpose of:

```text
events.parquet
```

### What does the event index provide?

It identifies each supported source observation and the corresponding canonical-row span.

### Why is this important?

Without an event index, a reader may not know which rows came from the same source message.

For order-book reconstruction, event boundaries can be important.

---

## Event Index Schema

The current depth event-index schema contains:

| Column | Arrow type | Nullable | Meaning |
|---|---|---|---|
| `event_ordinal` | `uint64` | No | Native source-event ordinal. |
| `canonical_row_offset` | `uint64` | No | Beginning of the event's canonical-row span. |
| `canonical_row_count` | `uint64` | No | Number of canonical rows produced by the observation. |
| `event_timestamp_ns` | `int64` | No | Source-event timestamp. |
| `system_timestamp_ns` | `int64` | Yes | Receive/system timestamp when available. |
| `sequence_start` | `uint64` | Yes | Applicable source sequence start. |
| `sequence_end` | `uint64` | Yes | Applicable source sequence end. |
| `event_boundary` | `uint8` | No | Event boundary classification. |
| `source_operation` | `uint8` | No | Source operation classification. |

### Why does the index contain timestamps?

The event index represents source observations.

Their original event timing is relevant to processing and reconstruction.

### Why preserve sequence information?

Some depth feeds require native sequence continuity.

The index preserves the applicable sequence range when available.

### Why include operation classification?

A source observation may represent a snapshot or update.

The operation information helps downstream readers interpret the corresponding canonical rows.

---

## Understanding Canonical Row Offsets

Suppose an event-index row contains:

```text
canonical_row_offset = 100
canonical_row_count  = 25
```

This means the observation corresponds to canonical rows:

```text
100 through 124
```

in the relevant logical row space.

### Why does it end at 124?

The span is half-open:

```text
[offset, offset + count)
```

Therefore:

```text
[100, 125)
```

contains 25 rows.

### Why use half-open spans?

They make adjacent event ranges easy to represent.

For example:

```text
Event A:
[0, 4)

Event B:
[4, 6)

Event C:
[6, 10)
```

The spans meet without overlap.

### Does the offset restart for every Parquet part?

Do not assume so.

The documented offset is in the stream's logical canonical-row space.

When a stream contains several physical parts, the reader must account for their manifest-defined order.

### Why is this important?

Suppose one event spans a physical file boundary.

A reader must not assume that its complete row span exists inside one arbitrary `part-*.parquet` file.

Use the logical offsets and manifest-ordered parts.

---

## Why Can an Event Have Zero Canonical Rows?

An event-index row can contain:

```text
canonical_row_count = 0
```

This is a supported representation.

### What does it mean?

The indexed source observation did not produce canonical rows in that event span.

### Why retain such an event?

The observation can still be relevant to source-event ordering or processing evidence.

For example, an event boundary or source operation may be meaningful even when no canonical price-level rows are emitted.

### Does zero rows mean the event is invalid?

Not necessarily.

The interpretation depends on the event operation and processing outcome.

### Why not remove it from the index?

Because the index describes source observations and their relationship to canonical output.

Removing every zero-row observation could lose relevant source-event evidence.

---

## Event Boundary Codes

The Rust event-index writer uses numeric boundary codes.

The current implementation maps:

```text
0 → Changes
1 → Initialization
```

### Changes

Represents the supported change-event boundary classification.

### Initialization

Represents an initialization boundary.

### Why is this useful?

Order-book reconstruction needs to distinguish initializing observations from later changes.

For example:

```text
Initialization
      |
      v
Changes
      |
      v
Changes
```

### Is `event_boundary` the same as `source_operation`?

No.

They are separate fields.

One describes the event boundary classification.

The other preserves the applicable source operation code.

### Should I guess every source-operation code?

No.

Interpret the numeric values according to the current Rust `DepthSourceOperation` definition.

Do not assume that the numeric representation is interchangeable with a provider's native operation codes.

---

## Reconstruction Segments

## What Are Reconstruction Segments?

Order-book reconstruction depends on valid initialization and event ordering.

For example:

```text
Snapshot A
    |
    v
Update A1
    |
    v
Update A2
```

This forms one reconstruction history.

However, the source may later encounter a reset or another independent initialization.

For example:

```text
Snapshot A
    |
    v
Updates A
    |
    v
Snapshot B
    |
    v
Updates B
```

These observations may belong to distinct reconstruction segments.

### Why distinguish segments?

Because the relationship between two segments may not establish uninterrupted order-book continuity.

For example, a later snapshot can initialize a valid book state without proving that every intervening update was observed.

### What does `segments.json` describe?

It records the supported reconstruction-segment information associated with the depth stream.

### Why preserve it?

A reader reconstructing the book must respect the original segment structure.

It should not simply concatenate all price-level rows and assume one continuous source history.

---

## What Is `boundary.json`?

The depth publication also contains:

```text
boundary.json
```

This preserves source-boundary and book-state evidence.

### Why are boundary records necessary?

Imagine combining two source recordings.

```text
Source A
    |
    v
End State A


Source B
    |
    v
Initial State B
```

The Engine needs evidence about whether the histories can be connected safely.

### What can boundary evidence describe?

Depending on the publication layout, it can describe:

- Source boundaries.
- Initial book state.
- Terminal book state.
- Sequence information.
- Reconstruction continuity.
- Source transitions.
- Relevant state fingerprints.

### Does the boundary prove complete exchange history?

No.

It describes the supported source and reconstruction evidence.

It does not prove the provider supplied every historical market event.

### Can independent segments be valid?

Yes.

Two independently initialized segments can each contain valid book histories.

However, their continuity may remain unproven.

### Why is this distinction important?

Some research tasks require continuous order-book reconstruction.

Others may be able to use independently valid segments.

The publication evidence allows readers to distinguish those cases.

---

## Why You Must Not Sort Depth Rows Arbitrarily

This is one of the most important rules for working with MarketForge depth publications.

**Do not reconstruct native-order depth simply by sorting canonical price-level rows by timestamp.**

### Why not?

A single source event can produce several canonical rows.

For example:

```text
Snapshot
    |
    +-- Bid Row
    +-- Bid Row
    +-- Ask Row
    +-- Ask Row
```

The order and grouping of these rows belong to one source observation.

A later update can depend on the complete prior state.

### What can go wrong?

Suppose you independently sort all rows by timestamp.

Rows belonging to one event may share the same timestamp.

The sort may not preserve every source-event boundary or causal dependency.

For some layouts, the physical ordering intentionally preserves native reconstruction requirements.

### Correct approach

Use the supported combination of:

```text
Primary Parquet Parts
        |
        v
    Event Index
        |
        v
    Segment Evidence
        |
        v
    Boundary Evidence
        |
        v
  Valid Book Reconstruction
```

### Does this mean timestamps are unimportant?

No.

Event timestamps remain important.

However, timestamp order alone is not always sufficient to reconstruct stateful market data.

---

## Trade Source Ownership

## Why Does Trade Output Have `sources.jsonl`?

Trade normalization may combine observations from several raw archives.

For example:

```text
Archive A
    |
    v
Trade 1
Trade 2


Archive B
    |
    v
Trade 3
Trade 4
```

The normalized output may contain all four trades.

However, researchers may need to know which source observation contributed each canonical row.

### What does `sources.jsonl` provide?

The supported trade source-ownership file maps canonical rows to information such as:

- Source task.
- Original record ordinal.

### Why preserve original ordinals?

A source record's ordinal identifies its position within the relevant source processing sequence.

This can help explain how a canonical row relates to its original recording.

### Why is this useful for reconciliation?

Two archives may overlap.

Some trades may be verified duplicates.

Other trades may be unique.

The ownership evidence helps preserve which source observations survived the applicable reconciliation.

### Does `sources.jsonl` replace the original raw archive?

No.

The raw archive remains the source evidence.

The ownership file connects canonical output to the processing source.

### Does every historical trade publication use the same ownership representation?

Not necessarily.

Publication protocol versions differ in how they represent ownership and source spans.

Inspect the actual manifest.

---

## Dataset Manifests

## What Is `manifest.json`?

Every committed normalization publication contains a manifest.

For example:

```text
data/datasets/normalized/<engine-dataset-id>/manifest.json
```

The manifest describes the completed dataset.

### Why is a manifest necessary?

A directory containing several Parquet files is not automatically a valid publication.

MarketForge needs to know:

- Which files belong to the dataset.
- Which job created them.
- Which instruments and streams they represent.
- Whether processing completed.
- Whether integrity evaluation passed.
- Which auxiliary files are required.
- How many rows were written.

The manifest records this information.

### Why not infer everything from directory names?

Directory names are not sufficient authority.

For example, a file named:

```text
part-000000.parquet
```

does not identify its source instrument or processing history.

The manifest provides the publication-level information.

---

## Manifest Fields

The current normalization manifest model includes the following main fields.

| Field | Meaning |
|---|---|
| `protocol_version` | Publication manifest protocol version. |
| `job_id` | Engine job that created the publication. |
| `dataset_id` | Engine dataset identity. |
| `content_type` | `trades` or `depth`. |
| `events_written` | Canonical output rows written. |
| `files_written` | Number of primary Parquet files. |
| `start_timestamp_ns` | First observed timestamp, when available. |
| `end_timestamp_ns` | Last observed timestamp, when available. |
| `source_tasks` | Source-task inventory. |
| `instrument_namespace` | Canonical instrument authority namespace, when available. |
| `resolved_instruments` | Applicable resolved instrument evidence. |
| `streams` | Logical stream metadata. |
| `files` | Primary Parquet file inventory. |
| `auxiliary_files` | Supporting publication artifacts. |
| `integrity_status` | Resulting integrity state. |
| `integrity_evaluation` | Applicable integrity evaluation. |
| `processing_metrics` | Processing and task counters. |
| `processing_quality` | Additional quality and policy evidence when present. |

### Why are some fields optional?

Different publication versions and processing modes preserve different kinds of evidence.

For example, older publications may not contain the same processing-quality information as newer ones.

### Does one manifest describe every file?

Yes.

The manifest is shared publication metadata.

Individual primary files are listed under its file inventory.

---

## Primary File Inventory

The manifest contains an array named:

```text
files
```

Each entry describes one primary Parquet file.

The current entry structure includes:

| Field | Meaning |
|---|---|
| `path` | Manifest-relative primary file path. |
| `rows` | Canonical rows in that file. |
| `size_bytes` | Physical file size. |
| `start_timestamp_ns` | First recorded timestamp when available. |
| `end_timestamp_ns` | Last recorded timestamp when available. |

### Example

```json
{
  "path": "streams/STREAM/trades/part-000000.parquet",
  "rows": 100000,
  "size_bytes": 24000000,
  "start_timestamp_ns": 1793059200000000000,
  "end_timestamp_ns": 1793062800000000000
}
```

This is an illustrative manifest file entry.

Replace `STREAM` with the actual stream fingerprint.

### Why store row counts per file?

A publication may contain several physical parts.

Each part can have a different number of rows.

### Why store timestamp bounds?

They help establish the observed event-time envelope for each file.

### Does the envelope prove continuous coverage?

No.

A file's first and last observed events do not establish that every expected event exists between them.

---

## Auxiliary File Inventory

The manifest also contains:

```text
auxiliary_files
```

This records supporting publication artifacts.

### Why are auxiliary files separate?

Primary Parquet files contain canonical market-data rows.

Auxiliary files contain additional information needed to interpret, validate, or reconstruct those rows.

### Auxiliary file kinds

The current manifest model includes:

| Kind | Purpose |
|---|---|
| `trade_source_positions` | Trade source ownership. |
| `depth_events` | Depth event index. |
| `depth_segments` | Reconstruction segments. |
| `depth_boundary` | Boundary and state evidence. |

The serialized kind names follow the current Rust enum representation.

### Example

A depth publication may retain references to:

```text
events.parquet
segments.json
boundary.json
```

### Why preserve the manifest inventory?

It allows readers and validators to identify the required supporting artifacts.

A publication cannot always be understood correctly by copying only its primary Parquet parts.

---

## Manifest Versions

## Why Are Manifests Versioned?

MarketForge's publication format has evolved.

For example, newer processing capabilities can require more detailed source-ownership and quality evidence.

The normalization manifest validator currently accepts protocol versions:

```text
3
4
5
```

### Why support multiple versions?

Previously generated datasets may remain valid after the processing infrastructure evolves.

A versioned manifest allows the reader to interpret a publication according to its supported protocol.

### Does accepting a manifest version mean every operation can use it?

No.

A publication may be valid for inspection and registration while being incompatible with a particular merge layout.

Publication validation and downstream operation compatibility are separate checks.

---

## Manifest Version 3

Version 3 is the base normalization publication format.

It records the core dataset information.

For example:

- Processing identity.
- Primary files.
- Logical streams.
- Source tasks.
- Timestamps.
- Integrity information.
- Processing metrics.
- Required auxiliary files.

### Why is version 3 important?

It provides the baseline publication representation accepted by the current normalization manifest validator.

### Does it contain every newer source-ownership feature?

No.

Later manifest versions add support for more explicit ownership relationships.

---

## Manifest Version 4

Version 4 adds explicit source-ownership information.

### Why is this necessary?

A publication may contain data from several overlapping source archives.

For example:

```text
Source A
Source B
Source C
```

The output needs to preserve which source observations contributed which canonical rows.

### What does explicit ownership enable?

It provides more detailed information about source contributions and the physical row relationships.

This is useful for:

- Reconciliation evidence.
- File-level lineage.
- Downstream merge validation.
- Reproducibility.
- Source ownership inspection.

### Does version 4 imply every layout is supported by merge?

No.

The merger still applies physical-layout constraints.

For example, some trade layouts require timestamp-first ordering and contiguous source ownership.

---

## Manifest Version 5

Version 5 supports the newer processing-policy and source-ownership extensions.

It can represent repeated source spans and richer processing-quality evidence.

### Why are repeated source spans necessary?

Consider a source-ownership sequence:

```text
Source A
Source B
Source A
```

A representation that only allows one contiguous span per source cannot describe this ownership pattern directly.

Version 5 can represent more complex relationships.

### What else can version 5 preserve?

Activated processing policies can retain additional information about:

- Effective policies.
- Timestamp ordering.
- Source observation preservation.
- Reconstruction validity.
- Continuity.
- Processing exclusions.
- Historical metadata assertions.

### Does version 5 change canonical trade and depth schemas?

The documented activated-policy publication retains the canonical Parquet row schemas.

The additional information belongs to publication evidence and auxiliary structures.

### Does version 5 automatically support every merger?

No.

Some v4/v5 physical layouts remain outside the current managed timestamp-first merge contract.

Inspect the specific source-ownership and ordering requirements.

---

## Publication Commit Rules

## Why Does MarketForge Use Staging?

A normalization job does not write directly into an already authoritative completed dataset.

It uses separate staging and final destinations.

Conceptually:

```text
Rust Engine
    |
    v
Staging Directory
    |
    v
Write Parquet Parts
    |
    v
Write Auxiliary Files
    |
    v
Evaluate Integrity
    |
    v
Validate Publication
    |
    v
Commit Final Dataset
```

### Why separate staging from publication?

Processing can fail.

For example:

- A source record may violate integrity requirements.
- Order-book reconstruction may fail.
- Resource limits may be exceeded.
- Output finalization may fail.

If incomplete staging files were treated as authoritative datasets, downstream research could unknowingly use partial results.

### What is staging?

The staging directory contains intermediate output created during execution.

It is not automatically a committed publication.

---

## Successful Publication

A dataset becomes committed only after completing the required publication process.

### What does this involve?

The supported workflow includes:

- Processing the selected source work.
- Writing canonical Parquet files.
- Writing required auxiliary evidence.
- Evaluating integrity.
- Validating the publication structure.
- Finalizing the manifest.
- Committing the dataset.

### Why is integrity evaluated before commit?

A failed required integrity evaluation cannot produce an accepted committed dataset manifest.

### Does successful commit prove the source is historically complete?

No.

It establishes that the publication satisfies the applicable processing and publication requirements.

Complete historical exchange coverage is a separate claim requiring additional evidence.

### Can a committed dataset have degraded quality?

Under supported non-strict processing policies, a publication can preserve degraded-quality evidence while satisfying its applicable acceptance requirements.

For example, bounded event exclusions may be permitted under an explicit policy.

Such a publication should not be represented as fully lossless.

---

## Failed Publication

Suppose processing reaches staging but later fails.

Conceptually:

```text
Source Records
     |
     v
Canonical Rows Generated
     |
     v
Staging Files Written
     |
     v
Integrity Failure
     |
     v
No Committed Publication
```

### Can staging files remain?

A failed operation may leave diagnostic or staging artifacts.

### Are those files authoritative?

No.

They should not be treated as completed normalized datasets.

### Why retain failure evidence?

It can help explain:

- How far processing progressed.
- Which source task failed.
- Whether any records were normalized.
- Which integrity condition failed.
- Whether output had already been staged.

### Does a failed job always leave the same artifacts?

No.

The retained files depend on when and how the failure occurred.

Use the actual execution report.

---

## Processing Counters

## Why Does MarketForge Track Processing Counters?

A completed publication needs more information than its final row count.

For example, suppose a source contains:

```text
1,000 source records
```

The Engine may:

- Skip records outside the selected instrument.
- Reject malformed records.
- Process valid observations.
- Expand depth snapshots into several canonical rows.
- Suppress verified duplicates.
- Exclude observations under an explicit policy.

The final Parquet row count alone cannot explain these transformations.

MarketForge therefore tracks several processing counters.

### Three important levels

```text
Physical Source Records
        |
        v
Logical Source Observations
        |
        v
Canonical Output Rows
```

These are different quantities.

### Why does this matter?

A depth snapshot may contain one source observation but generate hundreds of canonical rows.

A verified duplicate may be processed but suppressed during reconciliation.

A malformed source record may never produce canonical output.

The counters preserve these distinctions.

---

## Source Counters

The processing metrics include:

| Counter | Meaning |
|---|---|
| `records_read` | Physical source records examined. |
| `records_matched` | Records matched to the selected instrument. |
| `records_skipped_instrument` | Records excluded by instrument selection. |
| `records_rejected` | Rejected source records. |
| `records_rejected_unmatched` | Rejections without an established instrument match. |
| `records_processed` | Successfully processed source records. |
| `records_unprocessed` | Matched source records not processed. |
| `records_unrouted` | Records not routed to a concrete instrument. |

### Why distinguish matched and skipped?

A family archive may contain several contracts.

For example:

```text
BTC Futures Archive
    |
    +-- Contract A
    +-- Contract B
    +-- Contract C
```

If the normalization job selects Contract A, records belonging to B and C may be outside the selection.

Those records are different from malformed records.

### Why preserve unmatched rejections?

A source record may fail before its instrument identity is established.

For example, a malformed record may not provide a usable native symbol.

It would be misleading to attribute that rejection to one specific instrument without evidence.

The unmatched-rejection counter preserves this distinction.

---

## Records Read

The counter:

```text
records_read
```

describes physical source records examined.

### Does one source record always produce one canonical row?

No.

For trades, one source record commonly represents one execution.

For depth, one source record may produce many price-level rows.

### Example

```text
Source snapshot:
1 record

Canonical rows:
200
```

This is expected when the snapshot contains 200 represented levels.

### Does records read prove successful processing?

No.

Some read records may be rejected, skipped, or remain unprocessed.

Inspect the related counters.

---

## Records Matched

The counter:

```text
records_matched
```

tracks source records matched to the selected instrument.

### Why is this important for family archives?

A single archive may contain several concrete instruments.

For example:

```text
Archive:
BTC-USD Futures Family
```

The selected normalization task may only include part of that family.

### Does matched mean normalized?

No.

A matched record can still fail later processing checks.

### Why not use only records processed?

Because matching and processing are separate stages.

A record can be correctly identified but fail its source transformation.

---

## Records Rejected

The counter:

```text
records_rejected
```

tracks rejected records.

For example, rejection can occur because a record cannot satisfy the applicable parsing or validation requirements.

### Why distinguish rejection from suppression?

A rejected source record may never become a valid canonical observation.

A suppressed canonical row may have been normalized successfully but removed during reconciliation.

Those are different events in the processing pipeline.

### What about unmatched rejections?

The subset:

```text
records_rejected_unmatched
```

preserves rejections for which a concrete instrument match was not established.

### Why is this useful?

It prevents the metrics from falsely attributing every source failure to a selected instrument.

---

## Records Processed and Unprocessed

Two additional counters are:

```text
records_processed
records_unprocessed
```

### Processed

Tracks successfully processed source records.

### Unprocessed

Tracks matched records that were not processed.

### Why distinguish them?

A record can be selected and matched to an instrument without reaching successful normalization.

For example, a task may fail before every matched record has completed processing.

The distinction helps explain partial source work.

---

## Unrouted Records

The counter:

```text
records_unrouted
```

tracks records that were not routed to a concrete instrument.

### Why does routing matter?

Family-scoped archives can contain several native instrument identities.

Rust needs to map each selected record to the appropriate canonical instrument.

### Example

```text
Native symbol:
BTC-USD-261225
```

may resolve to a supported concrete futures instrument.

However, another record may contain an unknown or malformed identity.

### Does unrouted always mean invalid?

Interpret the counter together with the corresponding source and task diagnostics.

The actual reason may depend on the supported family-selection and native identity rules.

---

## Canonical Output Counters

## Canonical Row Accounting

The Engine also tracks canonical output.

The main counters are:

| Counter | Meaning |
|---|---|
| `events_normalized` | Canonical rows generated. |
| `events_written` | Canonical rows recorded as written. |
| `rows_suppressed` | Canonical rows suppressed through reconciliation. |
| `rows_unpublished` | Normalized rows not published. |

### Why call them events?

These are the established processing counter names.

However, for depth normalization, canonical rows and logical source observations are not necessarily one-to-one.

### Why is this important?

Do not assume:

```text
events_normalized
```

always equals the number of original source messages.

For depth, one observation can expand into multiple canonical rows.

---

## The Canonical Row Equation

The Engine validates the following relationship:

```text
events_normalized
    =
events_written
    + rows_suppressed
    + rows_unpublished
```

### What does this mean?

Every normalized canonical row must be accounted for as:

1. Written.
2. Suppressed.
3. Unpublished.

### Example

Suppose the Engine normalizes:

```text
1,000 canonical rows
```

Of those:

```text
900 rows written
50 rows suppressed
50 rows unpublished
```

Then:

```text
1,000 = 900 + 50 + 50
```

The accounting is consistent.

### Why is this important?

Without such a relationship, rows might disappear from the accounting without explanation.

### Does the equation prove that all original source records were valid?

No.

It accounts for canonical rows generated by normalization.

Source parsing failures and rejected records are tracked separately.

### Does it prove that every canonical row was committed?

No.

The counters distinguish rows written or staged from rows that ultimately belong to a committed publication.

Inspect the publication result and final counters.

---

## Suppressed Rows

The counter:

```text
rows_suppressed
```

describes canonical rows removed through the applicable reconciliation.

### Example

Suppose two archives contain a verified duplicate trade.

The Engine may normalize both source observations and suppress the duplicate representation.

### Does suppression always mean data loss?

No.

Verified duplicate suppression can preserve all unique logical market activity.

However, other processing policies may explicitly exclude unique or unverified observations.

Those cases require separate quality accounting.

### Why not treat all suppression equally?

Because:

```text
Verified duplicate
```

and:

```text
Excluded unique trade
```

are not equivalent.

The processing-quality evidence distinguishes their implications.

---

## Unpublished Rows

The counter:

```text
rows_unpublished
```

tracks normalized rows that did not become part of published output.

### Why might this happen?

For example, processing may generate rows before a later operation fails.

### Does unpublished mean the record was malformed?

Not necessarily.

A normalized row can be valid while the publication fails for another reason.

### Why preserve this counter?

It helps explain the difference between processing progress and committed output.

### Important distinction

A row written into staging is not automatically a committed canonical row.

Publication requires the applicable final validation and commit steps.

---

## Task Outcomes

## Task Counters

The processing metrics also include:

```text
tasks_completed
tasks_failed
```

### Completed tasks

Counts tasks that reached the corresponding successful processing outcome.

### Failed tasks

Counts tasks that failed.

### Why are these useful?

A normalization job may contain hundreds of source tasks.

For example:

```text
Task 1: Complete
Task 2: Complete
Task 3: Failed
Task 4: Complete
```

These counters summarize the processing outcomes.

### Are these the same as dashboard task states?

No.

The task counters represent processing outcomes.

The execution dashboard can use broader state classifications for scheduling, progress, or orchestration.

Do not assume every dashboard state maps directly to one task counter.

---

## Integrity Evaluation

## Why Does the Manifest Record Integrity?

Historical processing can encounter source-data problems.

For example:

- Parse failures.
- Invalid records.
- Sequence gaps.
- Timestamp regressions.
- Missing snapshots.
- Invalid reconstructed books.
- Transformation failures.

MarketForge tracks these conditions through its integrity system.

### Why include integrity in the manifest?

A researcher needs to know whether the resulting publication passed the configured integrity rules.

A file containing the expected columns is not necessarily suitable for every research task.

### Main fields

The normalization manifest includes:

```text
integrity_status
integrity_evaluation
```

### Integrity status

Summarizes the resulting integrity state.

### Integrity evaluation

Preserves the relevant rule evaluation and supporting information.

### Does successful processing always mean clean integrity?

Not necessarily.

Explicit non-strict policies can permit degraded outcomes.

The resulting status and evidence should be inspected.

---

## Processing Quality

Newer publication layouts can include:

```text
processing_quality
```

This preserves additional information about how source observations were handled.

The current Rust model includes fields such as:

| Field | Meaning |
|---|---|
| `effective_policies` | Policies used during processing. |
| `timestamp_ordering` | Actual physical timestamp ordering. |
| `native_reconstruction_valid` | Whether native reconstruction requirements were satisfied. |
| `supplied_observations_preserved` | Whether supplied observations were preserved under the applicable accounting. |
| `continuity_proven` | Whether the relevant continuous-history relationship was established. |
| `external_historical_coverage_established` | Whether external historical coverage was independently established. |
| `inherited_metadata_is_batch_assertion` | Whether inherited metadata remains an assertion from the batch. |
| `logical_source_events` | Logical source-observation count. |
| `verified_duplicate_observations` | Verified duplicates. |
| `known_unique_event_exclusions` | Known unique observations excluded. |
| `unverified_observation_exclusions` | Excluded observations whose uniqueness was not established. |
| `loss_rate_denominator` | Denominator used for the applicable loss accounting. |
| `integrity_rejected_physical_rows` | Physical source rows rejected under integrity processing. |

### Why preserve all these fields?

Because several independent properties can affect dataset usability.

For example:

```text
Valid reconstructed book
```

does not automatically imply:

```text
Continuous history across every source boundary
```

Similarly:

```text
No known unique-event exclusions
```

does not establish:

```text
Every exchange event was supplied historically
```

These are different claims.

---

## Verified Duplicates vs. Event Loss

Suppose two source archives contain the same trade.

If the Engine establishes reliable identity and matching complete contents, the duplicate can be suppressed.

### Verified duplicate

Conceptually:

```text
Source A → Trade 100
Source B → Trade 100
```

Both represent the same logical execution.

Suppressing one duplicate representation does not remove a unique trade.

### Known unique exclusion

Now suppose Source A contains a trade that is absent from Source B.

If a source-preference policy excludes that trade, unique supplied activity is lost.

### Unverified exclusion

If identity evidence is insufficient, the Engine may not know whether an excluded observation is a duplicate or a unique event.

That uncertainty is preserved.

### Why distinguish these?

The quality implications are different.

| Outcome | Meaning |
|---|---|
| Verified duplicate suppression | Repeated evidence of one logical observation. |
| Known unique exclusion | A distinct supplied observation was omitted. |
| Unverified exclusion | An observation was omitted without proof of equivalence. |

Known-unique and unverified exclusions degrade supplied-observation completeness.

Verified duplicate suppression does not.

---

## Reconstruction Validity vs. Continuity

Depth publications need two different kinds of evidence.

### Reconstruction validity

Can the selected events form valid book states under the applicable processing rules?

### Continuity

Can the relevant source histories be proven to connect into one continuous event history?

### Example

```text
Segment A:
Valid Snapshot
Valid Updates


Segment B:
Valid Snapshot
Valid Updates
```

Both segments may be valid.

However, there may be no independent proof that the interval between them is complete.

### Why is this important?

For some research tasks, independently valid snapshots may be useful.

Other tasks require uninterrupted event-by-event reconstruction.

The publication's processing-quality evidence helps distinguish these cases.

### Does a snapshot repair missing history?

No.

A snapshot can establish state at its own boundary.

It does not recreate omitted historical updates.

---

## Historical Coverage Is a Separate Guarantee

A publication can pass every supported processing check while still lacking complete exchange history.

For example, suppose an exchange archive contains valid trades for:

```text
12:00–12:10
```

but the provider omitted an earlier period.

The supplied records may all normalize correctly.

The resulting Parquet file may be valid.

Its checksum may match.

However, none of those facts proves that the exchange supplied every expected event.

### Three different questions

| Question | Relevant evidence |
|---|---|
| Were the provided records processed according to the rules? | Processing and integrity results. |
| Are the committed file bytes intact? | File and manifest verification. |
| Is the external historical record complete? | Independent source coverage and continuity evidence. |

### Why preserve unknown coverage?

Because unknown is not equivalent to complete.

MarketForge should not claim historical completeness merely because the processing pipeline succeeded.

---

## Publication Compatibility

## Why Valid Publications Can Have Different Layouts

A normalization publication may preserve different source-ordering and reconciliation evidence depending on the processing rules.

For example:

```text
Timestamp-first trades
```

and:

```text
Native-order segmented depth
```

have different physical requirements.

### Why does this matter?

A downstream operation may support one format but not another.

For example, a merger expecting timestamp-first rows cannot automatically consume every native-order depth layout.

### Does a valid manifest guarantee merge compatibility?

No.

The merger performs additional checks.

These can depend on:

- Manifest protocol version.
- Content type.
- Physical event ordering.
- Stream ownership.
- Source-span representation.
- Required auxiliary files.
- Reconstruction dependencies.

---

## Current Merge Layout Restrictions

The managed timestamp-first merger supports compatible normalization manifest v3 selections.

It also supports certain v4/v5 trade layouts when the selected primary files satisfy the required conditions.

### Supported trade conditions

The relevant v4/v5 trade selection must use:

- Timestamp-first physical ordering.
- Contiguous source ownership.
- No repeated source spans.

### Current restrictions

The documented timestamp-first merge contract does not support arbitrary:

- Native-order depth layouts.
- Version 4/5 depth layouts outside its supported reconstruction contract.
- Interleaved trade source spans.

### Why not silently convert them?

Because doing so could change source ownership or reconstruction semantics.

For example, depth events cannot be treated as independent price rows without considering the original source-event structure.

### What should users do?

Inspect the actual publication layout.

Then use Forge merge preparation to determine whether the selection satisfies the supported contract.

See [Historical Merge](../../workflows/merge.md#supported-layouts).

---

## Causal Merge Publications

MarketForge also supports causal timeline processing for compatible selections.

### Why is causal ordering relevant?

Certain depth histories require preserving source-event dependencies.

For example:

```text
Initialize Book
      |
      v
Apply Updates
      |
      v
Continue Reconstruction
```

A causal timeline must preserve the supported dependencies.

### Is the causal merge schema the same as canonical normalization depth?

No.

The merged timeline has its own physical schema.

For example, the documented causal timeline includes fields such as:

```text
timeline_sequence
reconstruction_only
```

### Why is `reconstruction_only` important?

Some timeline rows may be retained for reconstruction rather than ordinary market-event analysis.

### Can I delete these rows?

Do not remove them without a supported transformation that preserves the required reconstruction contract.

### Does causal mean one event caused another economically?

No.

Here, causal refers to processing dependencies.

It does not establish market causation.

---

## Publication Recovery and Registration

## Publication vs. PostgreSQL Registration

A successfully committed publication and a registered dataset are related but distinct.

### Engine publication

Rust commits the physical dataset and its manifest.

### Forge verification

Forge verifies the applicable committed output.

### PostgreSQL registration

Forge records the eligible primary files in the dataset registry.

Conceptually:

```text
Rust Execution
      |
      v
Committed Publication
      |
      v
Forge Verification
      |
      v
Database Registration
      |
      v
Discoverable Dataset Files
```

### Why distinguish these stages?

Database registration can fail even after Rust successfully committed a valid publication.

For example, PostgreSQL may be temporarily unavailable.

### Does that mean normalization must run again?

Not necessarily.

The committed publication may still be verifiable and eligible for registration recovery.

---

## Register an Existing Publication

For recovery tied to a managed execution, use the retained `execution-intent.json` with its sibling submitted batch. For registration scoped to a publication or one primary file, use one of the references below.

### Register by publication directory

```bash
mf normalize register \
    /absolute/path/to/normalized-publication \
    --testing
```

### Register by manifest

```bash
mf normalize register \
    /absolute/path/to/normalized-publication/manifest.json \
    --testing
```

### Register one primary file

```bash
mf normalize register \
    /absolute/path/to/normalized-publication/streams/STREAM/trades/part-000000.parquet \
    --testing
```

### Why provide these options?

You may want to register the complete publication or a particular supported primary file.

### Does registration reprocess the source archives?

No.

It verifies existing committed output and registers the applicable files.

### Can I register incomplete staging output?

No.

A staging directory is not automatically a committed publication.

Registration requires the applicable publication evidence.

---
