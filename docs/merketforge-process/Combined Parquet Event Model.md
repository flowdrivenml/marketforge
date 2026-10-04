## Combined Parquet Event Model

Combined datasets preserve one chronological stream containing:

```text
Trade
L2Snapshot
L2Update
```

Parquet uses one stable event envelope with nullable typed payloads.

## Quick Navigation

- [Event Envelope](#event-envelope)
- [Event Payloads](#event-payloads)
- [Atomic L2 Updates](#atomic-l2-updates)
- [Rust Model](#rust-model)
- [Required Changes](#required-changes)

## Event Envelope

Every Parquet row represents **one atomic canonical market event**.

```text
CanonicalEvent
├── event_timestamp_ns
├── system_timestamp_ns
├── exchange
├── instrument_id
├── symbol
├── stream_id
├── event_type
│
├── trade: nullable struct
├── l2_snapshot: nullable struct
└── l2_update: nullable struct
```

Exactly one payload is populated according to `event_type`.

This preserves:

```text
Trade
L2Update
Trade
L2Snapshot
L2Update
...
```

as one directly replayable chronological dataset.

## Event Payloads

### Trade

```text
trade {
    trade_id
    sequence
    side
    price
    quantity_base
    quantity_quote
    quantity_contracts
    is_rpi
    trade_iv
    mark_iv
    index_price
    mark_price
}
```

### L2 Snapshot

```text
l2_snapshot {
    sequence_first
    sequence_last
    sequence_previous
    cross_sequence

    bids[]
    asks[]
}
```

Each level contains:

```text
price
quantity_base
quantity_quote
quantity_contracts
order_count
```

### L2 Update

```text
l2_update {
    sequence_first
    sequence_last
    sequence_previous
    cross_sequence

    changes[]
}
```

Each change contains:

```text
side
action
price
quantity_base
quantity_quote
quantity_contracts
order_count
```

## Atomic L2 Updates

`L2Update` should represent **one atomic source book-update event**, not one individual price-level mutation.

Example native update:

```text
sequence = 500

bid 100 → 5
bid  99 → delete
ask 101 → 8
```

Canonical representation:

```text
L2Update
├── sequence = 500
└── changes
    ├── set    bid 100 → 5
    ├── delete bid  99
    └── set    ask 101 → 8
```

The entire update becomes one Parquet row.

This guarantees that consumers never observe a partially applied native book update.

```text
read L2Update
    ↓
apply all changes
    ↓
book transition complete
    ↓
continue
```

No separate `source_event_id`, `source_event_index`, or `source_event_count` is required.

## Rust Model

The canonical Rust model should approximately follow:

```rust
enum CanonicalEvent {
    Trade(Trade),
    L2Snapshot(L2Snapshot),
    L2Update(L2Update),
}
```

with:

```rust
struct L2Update {
    sequence: SequenceMetadata,
    changes: Vec<L2LevelUpdate>,
}
```

and:

```rust
struct L2LevelUpdate {
    side: BookSide,
    action: L2Action,
    price: Decimal,
    quantity_base: Option<Decimal>,
    quantity_quote: Option<Decimal>,
    quantity_contracts: Option<Decimal>,
    order_count: Option<u64>,
}
```

This gives the same semantic unit across:

```text
raw exchange message
        ↓
canonical L2Update
        ↓
temporary Arrow run
        ↓
combined Parquet row
        ↓
mlfindgen replay
```
