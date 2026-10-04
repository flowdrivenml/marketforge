## Purpose

MarketForge normalizes exchange-specific historical market data into a small set of canonical schemas.

The canonical layer removes differences in:

- timestamp representation and precision
- symbol representation
- trade-side encoding
- quantity units
- contract sizing
- order-book event models
- sequence representation
- field naming
- optional exchange-specific metadata

The initial canonical record types are:

```text
Trade
L2Snapshot
L2Update
```

Supporting canonical structures include:

```text
L2Level
L2LevelUpdate
```

All timestamps use Unix epoch nanoseconds.

All normalized records reference the MarketForge instrument catalog. Raw quantity interpretation is resolved using `instrument_specs`.

## Quick Navigation

- [Data Flow](#data-flow)
- [Common Conventions](#common-conventions)
- [Trade](#trade)
- [L2 Price Level](#l2-price-level)
- [L2 Level Update](#l2-level-update)
- [L2Snapshot](#l2snapshot)
- [L2Update](#l2update)
- [L2 Sequencing](#l2-sequencing)
- [L2 Reconstruction Models](#l2-reconstruction-models)
- [Raw Versus Canonical Responsibility](#raw-versus-canonical-responsibility)
- [Canonical Guarantees](#canonical-guarantees)
- [Live Gap Recovery](#live-gap-recovery)

## Data Flow

```text
Raw archive
    ↓
raw_formats
    ↓
Exchange-specific parser
    ↓
Parsed raw records
    ↓
normalization_rules
    +
instrument_specs
    ↓
Canonical records
    ↓
Trade
L2Snapshot
L2Update
    ↓
Parquet / live storage / downstream research
```

## Common Conventions

### Time

Canonical timestamps are:

```text
Unix epoch nanoseconds
int64
UTC
```

Exchange timestamps expressed as seconds, fractional seconds, milliseconds, or microseconds are converted to nanoseconds.

Two timestamps may be retained when the source provides distinct event/matching-engine and system timestamps:

| Field | Meaning |
|---|---|
| `event_timestamp_ns` | Exchange event or matching-engine timestamp |
| `system_timestamp_ns` | Exchange system timestamp, when independently available |

`system_timestamp_ns` is nullable.

### Instrument Identity

Canonical records contain:

| Field | Meaning |
|---|---|
| `exchange` | Canonical exchange code |
| `instrument_id` | MarketForge catalog instrument ID |
| `symbol` | Native exchange instrument symbol |

`instrument_id` provides stable catalog identity. `symbol` is retained so normalized files remain independently inspectable.

### Price

`price` always represents the market price of the trade or order-book level.

Exchange-specific numeric representations are converted without changing the economic meaning of the price.

### Quantity

MarketForge distinguishes three quantity representations:

| Field | Meaning |
|---|---|
| `quantity_base` | Quantity expressed in base asset |
| `quantity_quote` | Quantity/notional expressed in quote asset |
| `quantity_contracts` | Number of derivative contracts |

Fields that do not apply to an instrument are null.

Quantity conversion uses:

```text
instrument_specs.quantity_type
instrument_specs.contract_value
instrument_specs.contract_value_asset
```

#### Base Quantity

For:

```text
quantity_type = base
```

normalize as:

```text
quantity_base = raw_quantity
quantity_quote = quantity_base × price
quantity_contracts = null
```

#### Quote Quantity

For:

```text
quantity_type = quote
```

normalize as:

```text
quantity_quote = raw_quantity
quantity_base = quantity_quote / price
quantity_contracts = null
```

#### Contract Quantity

For:

```text
quantity_type = contracts
```

first preserve:

```text
quantity_contracts = raw_quantity
```

If:

```text
contract_value_asset = base
```

then:

```text
quantity_base =
    quantity_contracts × contract_value

quantity_quote =
    quantity_base × price
```

If:

```text
contract_value_asset = quote
```

then:

```text
quantity_quote =
    quantity_contracts × contract_value

quantity_base =
    quantity_quote / price
```

Contract values are obtained from instrument metadata rather than hardcoded into raw-format parsers.

---

## Trade

A canonical `Trade` represents one executed trade.

### Schema

| Field | Type | Nullable | Meaning |
|---|---|---:|---|
| `event_timestamp_ns` | int64 | No | Execution timestamp in Unix ns |
| `exchange` | string | No | Canonical exchange code |
| `instrument_id` | int64 | No | Catalog instrument ID |
| `symbol` | string | No | Native exchange symbol |
| `trade_id` | string | Yes | Native trade identifier |
| `sequence` | int64 | Yes | Native trade/event sequence |
| `side` | enum | No | Aggressor/taker side |
| `price` | decimal | No | Execution price |
| `quantity_base` | decimal | Yes | Base-asset quantity |
| `quantity_quote` | decimal | Yes | Quote-asset quantity/notional |
| `quantity_contracts` | decimal | Yes | Contract quantity |
| `is_rpi` | boolean | Yes | Retail Price Improvement trade flag |
| `trade_iv` | decimal | Yes | Trade implied volatility |
| `mark_iv` | decimal | Yes | Mark implied volatility |
| `index_price` | decimal | Yes | Underlying/index price at execution |
| `mark_price` | decimal | Yes | Mark price at execution |

### Side

Canonical trade side is restricted to:

```text
buy
sell
```

Exchange-specific representations such as:

```text
Buy / Sell
buy / sell
1 / 2
buyer-is-maker boolean
signed quantity
```

are converted during normalization.

`side` represents the aggressor/taker side.

### Trade IDs

`trade_id` is stored as a string because native identifiers may be:

```text
integer
large integer
UUID
exchange-specific string
```

A trade ID is not assumed to be globally unique.

Some sources scope trade IDs to an instrument. Raw uniqueness validation may therefore require:

```text
(exchange, instrument_id, trade_id)
```

or another format-specific identity defined by the normalization rule.

### Sequence

`sequence` is separate from `trade_id`.

It is populated when the source exposes a meaningful trade/event sequence and is otherwise null.

### Optional Trade Metadata

Fields such as:

```text
is_rpi
trade_iv
mark_iv
index_price
mark_price
```

are preserved when available.

Their absence does not change the core Trade schema.

---

## L2 Price Level

`L2Snapshot` uses canonical `L2Level` values to represent complete price levels.

### Schema

| Field | Type | Nullable | Meaning |
|---|---|---:|---|
| `price` | decimal | No | Price level |
| `quantity_base` | decimal | Yes | Base quantity at level |
| `quantity_quote` | decimal | Yes | Quote quantity/notional at level |
| `quantity_contracts` | decimal | Yes | Contract quantity at level |
| `order_count` | int64 | Yes | Number of orders represented by level |

Quantity normalization follows the same instrument-aware rules used for trades.

`order_count` is null when the source does not provide it.

---

## L2 Level Update

`L2LevelUpdate` represents one normalized mutation of one L2 price level.

Multiple `L2LevelUpdate` values may belong to one atomic `L2Update`.

### Schema

| Field | Type | Nullable | Meaning |
|---|---|---:|---|
| `side` | enum | No | `bid` or `ask` |
| `action` | enum | No | `set` or `delete` |
| `price` | decimal | No | Price level |
| `quantity_base` | decimal | Yes | New base quantity |
| `quantity_quote` | decimal | Yes | New quote quantity/notional |
| `quantity_contracts` | decimal | Yes | New contract quantity |
| `order_count` | int64 | Yes | New order count |

### Side

Canonical book side is:

```text
bid
ask
```

Exchange representations such as explicit side fields or signed quantities are converted during normalization.

### Action

Canonical L2 level updates describe the resulting state of a price level rather than preserving the exchange-specific update operation.

Canonical `action` is restricted to:

```text
set
delete
```

`set` means:

```text
after this level update, the price level has the supplied quantity
```

It covers both creation of a new level and replacement of the quantity at an existing level.

`delete` means:

```text
after this level update, the price level does not exist
```

The canonical action may be derived from the raw update.

For example, exchanges that publish absolute level quantities commonly represent both insertion and modification as a new quantity, while a zero quantity represents removal:

```text
raw quantity > 0
    → set

raw quantity = 0
    → delete
```

Other exchanges may expose different update semantics such as:

```text
delta
update
make
take
```

These exchange-specific operations are interpreted during normalization and do not survive into the canonical representation.

Canonical `L2LevelUpdate` therefore describes the resulting level state:

```text
set(side, price, quantity)
delete(side, price)
```

It does not describe the quantity change relative to the previous state.

State-derived information such as:

```text
previous quantity
quantity delta
liquidity added
liquidity removed
```

can be computed by replaying canonical updates against an in-memory order book.

These derived values are not part of the core canonical L2 schema.

### Delete Records

For a delete:

```text
action = delete
```

quantity fields may be zero or null according to the final physical storage implementation.

Consumers must use `action`, not quantity value alone, to determine deletion semantics.

---

## L2Snapshot

A canonical `L2Snapshot` represents complete known order-book state at a point in time.

### Schema

| Field | Type | Nullable | Meaning |
|---|---|---:|---|
| `event_timestamp_ns` | int64 | No | Book event/matching timestamp |
| `system_timestamp_ns` | int64 | Yes | Exchange system timestamp |
| `exchange` | string | No | Canonical exchange code |
| `instrument_id` | int64 | No | Catalog instrument ID |
| `symbol` | string | No | Native exchange symbol |
| `bids` | list<L2Level> | No | Complete known bid side |
| `asks` | list<L2Level> | No | Complete known ask side |
| `sequence_first` | int64 | Yes | First native update sequence represented |
| `sequence_last` | int64 | Yes | Last native update sequence represented |
| `sequence_previous` | int64 | Yes | Explicit previous sequence when supplied by the source |
| `cross_sequence` | int64 | Yes | Independent secondary/cross-sequence when supplied by the source |

### Book Ordering

Canonical snapshots are ordered:

```text
bids → descending price
asks → ascending price
```

Normalization must enforce this ordering when the raw source does not guarantee it.

### Snapshot Semantics

A snapshot represents standalone book state.

It must not require a previous canonical snapshot or update to interpret its contents.

Exchange formats that represent one logical snapshot using many physical rows must be assembled before emitting `L2Snapshot`.

---

## L2Update

A canonical `L2Update` represents one atomic normalized order-book update event.

One `L2Update` may contain one or more `L2LevelUpdate` mutations.

### Schema

| Field | Type | Nullable | Meaning |
|---|---|---:|---|
| `event_timestamp_ns` | int64 | No | Book event/matching timestamp |
| `system_timestamp_ns` | int64 | Yes | Exchange system timestamp |
| `exchange` | string | No | Canonical exchange code |
| `instrument_id` | int64 | No | Catalog instrument ID |
| `symbol` | string | No | Native exchange symbol |
| `sequence_first` | int64 | Yes | First native update sequence represented |
| `sequence_last` | int64 | Yes | Last native update sequence represented |
| `sequence_previous` | int64 | Yes | Explicit previous sequence when supplied by the source |
| `cross_sequence` | int64 | Yes | Independent secondary/cross-sequence when supplied by the source |
| `changes` | list<L2LevelUpdate> | No | Atomic price-level mutations belonging to this update |

### Atomic Update Semantics

All `changes` belonging to one `L2Update` represent one atomic source book transition.

For example:

```text
native update
sequence = 500

bid 100 → 5
bid  99 → delete
ask 101 → 8

        ↓

L2Update
├── sequence_first = 500
├── sequence_last = 500
└── changes
    ├── set bid 100 → 5
    ├── delete bid 99
    └── set ask 101 → 8
```

Consumers must apply all `changes` before treating the book transition as complete.

Individual level mutations belonging to one `L2Update` must not be interleaved with another canonical event.

This provides a consistent atomic unit across:

```text
raw source update
        ↓
canonical L2Update
        ↓
Parquet event
        ↓
book replay
```

---

## L2 Sequencing

Canonical `L2Snapshot` and `L2Update` records expose:

```text
sequence_first
sequence_last
sequence_previous
cross_sequence
```

rather than assuming every exchange provides one simple sequence number.

All fields are nullable.

`sequence_first` and `sequence_last` describe the native update sequence or sequence range represented by the source event.

For a source exposing one sequence identifier:

```text
sequence_first = native sequence
sequence_last = native sequence
```

For a source exposing an update range:

```text
sequence_first = first native update
sequence_last = last native update
```

For example:

```text
U = 100
u = 120

↓

sequence_first = 100
sequence_last = 120
```

`sequence_previous` preserves an explicit previous-sequence identifier when supplied by the source:

```text
previous native sequence = 100
current native sequence = 120

↓

sequence_previous = 100
sequence_first = 120
sequence_last = 120
```

`cross_sequence` preserves an independent secondary sequence domain when supplied by the source.

It must not be interpreted as the primary L2 continuity sequence.

For example:

```text
book sequence = 100
cross sequence = 5000

↓

sequence_first = 100
sequence_last = 100
cross_sequence = 5000
```

Sources without a particular sequence concept use null for that field:

```text
sequence_first = null
sequence_last = null
sequence_previous = null
cross_sequence = null
```

Only unavailable fields need to be null.

A source may populate some sequence fields while leaving others null.

Raw sequence semantics, bootstrap rules, and continuity validation remain format-specific.

---

## L2 Reconstruction Models

Raw exchange formats may use different representations:

```text
snapshot + incremental updates
REST snapshot + incremental updates
periodic snapshots
standalone snapshots
multi-row snapshot + incremental updates
```

Normalization converts these representations into:

```text
L2Snapshot
L2Update
```

When one native update contains multiple price-level mutations, normalization emits **one atomic `L2Update` containing multiple `L2LevelUpdate` entries**.

### Snapshot + Incremental Updates

Some live sources provide an authoritative snapshot directly through the stream:

```text
raw snapshot
    ↓
L2Snapshot

raw update
    ↓
L2Update
    └── changes[]
```

The snapshot establishes the initial book state.

Subsequent updates mutate that state.

### REST Snapshot + Incremental Updates

Some live sources provide only incremental WebSocket updates and require a separate REST snapshot.

```text
buffer raw updates
        +
REST snapshot
        ↓
synchronize using native sequence metadata
        ↓
L2Snapshot
        ↓
L2Update
L2Update
...
```

The REST snapshot establishes an authoritative book state.

Buffered updates already represented by the snapshot are discarded.

Updates after the snapshot are replayed in sequence.

Each atomic `L2Update` is applied completely before the next canonical event is processed.

If sequence continuity cannot be established, the book is not considered valid and must be re-bootstrapped.

### Periodic Snapshots

Each new raw snapshot emits a new canonical `L2Snapshot` and resets the reconstructed source book.

Subsequent raw updates emit canonical `L2Update` records.

Periodic REST snapshots may also be acquired independently without maintaining an incremental book.

### Standalone Snapshots

Sources containing independent snapshots emit:

```text
L2Snapshot
L2Snapshot
L2Snapshot
...
```

No synthetic `L2Update` records are required.

### Multi-Row Snapshots

If one logical snapshot is represented by many raw rows:

```text
raw snapshot rows
        ↓
assemble complete book
        ↓
one L2Snapshot
```

The physical row representation must not leak into the canonical model.

---

## Raw Versus Canonical Responsibility

### `raw_formats`

Defines what physically exists in the source:

```text
archive/container format
compression
record structure
field names
field positions
raw timestamp representation
raw event representation
raw sequence fields
```

`raw_formats` describes source data without assigning canonical meaning to exchange-specific fields.

### `instrument_specs`

Defines instrument economics:

```text
quantity_type
contract_value
contract_value_asset
tick_size
quantity step
```

Instrument-specific economic interpretation belongs here rather than in parsers.

### `normalization_rules`

Defines how parsed raw values become canonical values:

```text
field mapping
side mapping
timestamp selection
quantity source
sign handling
RPI mapping
event mapping
snapshot assembly
update interpretation
update grouping
sequence mapping
optional-field mapping
```

`update grouping` determines which raw price-level mutations belong to the same atomic canonical `L2Update`.

Sequence mapping determines how native sequence fields populate:

```text
sequence_first
sequence_last
sequence_previous
cross_sequence
```

The meaning and validation rules of those native sequences remain format-specific.

### Canonical Schemas

Define the output contract:

```text
Trade
L2Snapshot
L2Update
```

Supporting structures include:

```text
L2Level
L2LevelUpdate
```

No downstream consumer should need exchange-specific raw-format knowledge after normalization.

---

## Canonical Guarantees

Normalized MarketForge data must guarantee:

- timestamps use Unix epoch nanoseconds
- exchange and instrument identity are explicit
- trade side is `buy
sell
```

- book side is:

```text
bid
ask
```

- L2 level-update action is:

```text
set
delete
```

- quantity semantics are explicit
- contract quantities are preserved when applicable
- base and quote quantities have consistent meanings across exchanges
- snapshots have bids descending and asks ascending
- one `L2Update` represents one atomic normalized book transition
- one `L2Update` may contain one or more `L2LevelUpdate` mutations
- all level mutations belonging to one source update remain grouped
- mutations belonging to one `L2Update` must not be interleaved with another canonical event
- consumers apply all `changes` before treating an `L2Update` as complete
- exchange-specific field names do not leak into canonical schemas
- exchange-specific quantity interpretation is resolved using instrument metadata
- unavailable optional values are represented as null rather than fabricated
- native L2 sequence ranges, previous-sequence links, and independent cross-sequences are retained when available
- sequence metadata belongs to the atomic `L2Update`, not individual `L2LevelUpdate` mutations
- sequence continuity and bootstrap validation remain format-specific
- normalization never fabricates missing sequence information
- normalization never hardcodes instrument-specific contract values that belong to `instrument_specs`

---

## Live Gap Recovery

Live sequence metadata allows MarketForge to detect when required market-data events are missing.

Gap detection and recovery are format-specific.

Canonical sequence fields preserve the source information required to perform that validation.

### Order Books

If L2 sequence continuity fails, the reconstructed book is no longer trusted:

```text
valid book
    ↓
sequence gap
    ↓
invalid book
```

MarketForge must not infer or fabricate the missing update.

Recovery uses a new authoritative snapshot.

For sources requiring REST synchronization:

```text
detect gap
    ↓
mark book invalid
    ↓
continue buffering WebSocket updates
        +
request fresh REST snapshot
    ↓
synchronize snapshot sequence with buffered updates
    ↓
discard updates already represented by snapshot
    ↓
replay subsequent atomic L2Updates
    ↓
valid book
```

Each buffered `L2Update` is treated as one atomic transition:

```text
L2Update
    ↓
apply all changes[]
    ↓
transition complete
    ↓
next L2Update
```

For sources that provide native WebSocket snapshots:

```text
detect gap
    ↓
mark book invalid
    ↓
obtain new WebSocket snapshot
    ↓
replace local book
    ↓
resume validated updates
```

A fresh snapshot recovers the **book state** rather than reconstructing the individual missing update.

### Trades

Trades are events rather than reconstructable state.

A later snapshot cannot replace a missing trade.

When the exchange provides suitable recent or historical trade retrieval, MarketForge may attempt:

```text
detect gap / disconnect
    ↓
buffer new WebSocket trades
        +
request recent trades
    ↓
identify overlap
    ↓
recover missing executions
    ↓
deduplicate
    ↓
merge in source order
```

Recovery is complete only when the retrieved trade history fully bridges the missing interval.

If the gap cannot be recovered:

```text
record integrity gap
    ↓
resume live ingestion
```

MarketForge must never fabricate missing trades or silently treat a known incomplete interval as complete.

### Recovery Guarantee

Live recovery follows one fundamental rule:

```text
known missing data
    ≠
complete canonical stream
```

A reconstructed order book becomes trusted again only after authoritative state and subsequent sequence continuity have been established.

A trade stream becomes complete again only when the missing executions can be recovered and reconciled.

Otherwise, the gap remains explicit.

---

## Combined Event Representation

Canonical event semantics are designed to support a single chronological historical stream.

The logical event model is:

```text
CanonicalEvent
├── Trade
├── L2Snapshot
└── L2Update
```

One combined dataset may therefore contain:

```text
L2Snapshot
L2Update
Trade
L2Update
Trade
L2Update
...
```

ordered chronologically.

Each canonical event remains atomic.

In particular:

```text
L2Update
└── changes[]
```

is one event regardless of how many price levels it modifies.

This allows downstream consumers to replay:

```text
read event
    ↓
Trade
    → process execution

L2Snapshot
    → replace book

L2Update
    → apply all changes atomically
    ↓
continue
```

without requiring exchange-specific source-format knowledge.

The final Arrow/Parquet physical schema may represent these event variants using a shared event envelope and typed nullable payloads:

```text
CanonicalEvent
├── common event envelope
│
├── trade
├── l2_snapshot
└── l2_update
```

The exact physical Arrow/Parquet representation is defined separately from the canonical semantic model.

---

## Canonical Model Summary

The final logical hierarchy is:

```text
CanonicalEvent
│
├── Trade
│
├── L2Snapshot
│   ├── bids: list<L2Level>
│   └── asks: list<L2Level>
│
└── L2Update
    └── changes: list<L2LevelUpdate>
```

Where:

```text
L2Level
    = complete state of one price level

L2LevelUpdate
    = one set/delete mutation of one price level

L2Snapshot
    = complete known book state

L2Update
    = one atomic book transition containing one or more level mutations
```

The normalization boundary is therefore:

```text
exchange-specific raw representation
                ↓
        parser / normalization
                ↓
         canonical semantics
                ↓
Trade | L2Snapshot | L2Update
                ↓
      storage / replay / research
```

The core rule is:

> One canonical event represents one complete market event. A native order-book update containing multiple level mutations becomes one atomic `L2Update`, not multiple independently interleavable canonical events.