If a processing job contains depth, MarketForge always maintains current order-book state.

`BookStore` is shared infrastructure for offline processing and later live processing.

## Quick Navigation

- [Book Identity](#book-identity)
- [Book State](#book-state)
- [Price Levels](#price-levels)
- [Snapshots](#snapshots)
- [Atomic Updates](#atomic-updates)
- [Book Validity](#book-validity)
- [Sequence Validation](#sequence-validation)
- [Book Views](#book-views)
- [Architecture](#architecture)

## Book Identity

`BookStore` maintains independent books by stream:

```text
BookStore
├── Book[Bybit BTCUSDT depth]
├── Book[Binance BTCUSDT depth]
├── Book[OKX BTC-USDT-SWAP depth]
└── ...
```

Conceptually:

```rust
struct BookKey {
    instrument_id: i64,
    stream_id: StreamId,
}
```

`stream_id` distinguishes separate depth feeds for the same instrument.

Books may initially be stored in:

```rust
HashMap<BookKey, BookState>
```

## Book State

Each book maintains:

```text
bids
asks
status
current sequence state
```

Conceptually:

```rust
struct BookState {
    bids: ...,
    asks: ...,
    status: BookStatus,
    sequence: SequenceState,
}
```

Only the current state is retained.

Historical book states are not kept in memory.

## Price Levels

Initial implementation should use ordered maps:

```rust
BTreeMap<Price, BookLevel>
```

Required operations are:

```text
set level
delete level
best bid
best ask
ordered bid iteration
ordered ask iteration
snapshot replacement
```

`BTreeMap` provides efficient ordered insertion, deletion, and traversal without requiring a custom order-book structure.

Price should never use `f64` as the ordered key.

A strong candidate is exact integer price ticks:

\[
P_{\text{ticks}}
=
\frac{P}{\text{tick size}}
\]

Example:

```text
price     = 84500.3
tick_size = 0.1

price_ticks = 845003
```

This gives an exact integer map key:

```rust
BTreeMap<i64, BookLevel>
```

The exact price representation should be finalized before implementation.

## Snapshots

An `L2Snapshot` establishes authoritative book state:

```text
L2Snapshot
    ↓
clear previous state
    ↓
load bids[]
    ↓
load asks[]
    ↓
update sequence state
    ↓
BookStatus::Valid
```

A new authoritative snapshot replaces previous state completely.

## Atomic Updates

Canonical `L2Update` contains:

```text
L2Update
└── changes[]
    ├── L2LevelUpdate
    ├── L2LevelUpdate
    └── ...
```

Application is atomic:

```text
L2Update
    ↓
apply changes[0]
apply changes[1]
apply changes[2]
    ↓
complete book transition
```

Individual mutations must not become externally visible as separate completed book states.

Operations reduce to:

```text
set
    → insert / replace level

delete
    → remove level
```

## Book Validity

Book validity is explicit:

```rust
enum BookStatus {
    Uninitialized,
    Valid,
    Invalid,
}
```

Lifecycle:

```text
Uninitialized
    ↓ authoritative snapshot
Valid
    ↓ continuity failure
Invalid
    ↓ authoritative snapshot
Valid
```

State-dependent calculations must only use a valid book.

```text
Valid
    → BBO / mid / transformations allowed

Uninitialized
Invalid
    → state-derived values unavailable
```

## Sequence Validation

Sequence semantics remain format-specific.

```text
Canonical L2 event
        ↓
Format Sequence Validator
        ↓
valid transition
        ↓
BookStore
```

On continuity failure:

```text
sequence gap
    ↓
IntegrityMetrics
    ↓
BookStatus::Invalid
```

`BookStore` does not invent universal sequence rules.

Format-specific validators determine whether native sequence continuity is valid.

## Book Views

`BookState` exposes a stable API:

```text
best_bid()
best_ask()
mid()

bids()
asks()

depth_bids(n)
depth_asks(n)
```

For example:

\[
P_{\text{mid}}
=
\frac{P_{\text{best bid}} + P_{\text{best ask}}}{2}
\]

Consumers operate through this API rather than accessing the internal map representation directly.

This allows the underlying book implementation to change later without affecting:

```text
relative-depth transformations
cross-market conversions
book validation
depth aggregation
checkpoint generation
live processing
```

## Architecture

```text
Canonical Event Stream
        ↓
Format Sequence Validation
        ↓
BookStore
        │
        ├── Book A
        │   ├── bids
        │   └── asks
        │
        ├── Book B
        │   ├── bids
        │   └── asks
        │
        └── ...
        ↓
BookState API
        │
        ├── BBO
        ├── Mid
        ├── Validation
        ├── Transformations
        └── Checkpoints
```

Core rules:

- every depth stream has independent book state
- depth jobs always maintain `BookStore`
- snapshots replace current state
- `L2Update.changes[]` are applied atomically
- one book is mutated sequentially
- invalid books are never used for state-derived calculations
- sequence validation remains format-specific
- consumers access state through the `BookState` API
- initial implementation favors simple ordered structures over premature custom optimization
- exact internal price representation must be finalized before implementation