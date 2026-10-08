
MarketForge validates datasets before creating a `MergeJob`.

A merge does **not** require instruments to share the same underlying asset, quote asset, market category, or price scale. MarketForge does not rescale, align, or economically normalize prices during merging.

The purpose of a merge is narrower:

> **Combine canonical market-event streams into one deterministic chronological stream while preserving the identity and original canonical values of every event.**

### Canonical Dataset Requirements

Every merge input must already be a valid canonical MarketForge dataset.

Inputs must:

- have status `complete`
    
- use Parquet storage
    
- have a published storage path
    
- use a supported canonical data type
    
- overlap the requested merge time range
    

Supported canonical data types are:

```
trade
l2
trade_l2
```

Raw archives cannot participate directly in a merge. They must first be processed into canonical datasets.

### Instrument Compatibility

Canonical datasets from different instruments may be merged freely.

For example:

```
BTCUSDT spot
BTCUSDT perpetual
BTCUSD inverse perpetual
BTC dated future
ETHUSDT spot
ETHBTC spot
SOLUSDC spot
```

may all participate in the same chronological merge.

MarketForge does not require:

```
same base asset
same quote asset
same settlement asset
same exchange
same instrument type
same market category
same contract type
same price scale
```

These differences remain part of the market information preserved by the merged dataset.

### Spot and Contract Markets

Spot, perpetual, and futures datasets may be merged together.

For example:

```
BTCUSDT spot
        +
BTCUSDT linear perpetual
        +
BTCUSD inverse perpetual
        +
BTC dated future
        ↓
✓ chronological merge
```

MarketForge does not attempt to remove the price differences between these instruments.

A spot price, perpetual premium, futures basis, cross-exchange spread, or other price difference is preserved exactly as represented in the canonical source streams.

These relationships can later be used by downstream software for features such as:

```
spot ↔ perpetual basis
spot ↔ future basis
perpetual ↔ future spread
cross-exchange spread
price discovery
lead/lag
relative-value signals
```

### Different Underlying Assets

Different underlying assets may also participate in the same merge.

For example:

```
BTCUSDT
ETHUSDT
SOLBTC
ETHBTC
```

may be merged chronologically.

MarketForge does not claim that their absolute prices are directly comparable. It only preserves when their events occurred and which stream produced each event.

This allows downstream systems to derive cross-asset relationships such as:

```
returns
relative returns
lead/lag
correlation
beta
relative momentum
cross-asset order flow
liquidity response
volatility transmission
```

### Trades and Depth

Trade and L2 datasets may be merged together.

```
trades ─┐
        ├── chronological event stream
depth  ─┘
```

For example:

```
09:00:00.050  BTC spot        L2 update
09:00:00.100  BTC perpetual   trade
09:00:00.300  ETH spot        L2 snapshot
09:00:00.450  BTC spot        trade
09:00:00.900  BTC future      L2 update
```

No resampling, forward filling, aggregation, or price adjustment occurs.

Output data type is inferred from the selected canonical datasets:

```
trade + trade       → trade

l2 + l2             → l2

trade + l2          → trade_l2

trade_l2 + anything → trade_l2
```

### Stream Identity

Different instruments remain distinguishable after merging.

Each input dataset receives a stable stream identity:

```
dataset:<dataset_id>
```

and a deterministic `stream_rank`.

The merge job orders events primarily by:

```
event_timestamp_ns
```

with:

```
stream_rank
```

used as the deterministic tie-break when events share the same timestamp.

Conceptually:

```
dataset 12 ─┐
dataset 19 ─┤
dataset 27 ─┼── sort by event_timestamp_ns
dataset 41 ─┘             │
                           ↓
                    timestamp tie?
                           │
                           ↓
                      stream_rank
                           │
                           ↓
                ONE deterministic stream
```

### Prices and Quantities

MarketForge preserves canonical prices and quantities rather than converting all instruments onto a common price scale during merge.

For example:

```
BTC spot       85,000
BTC perpetual  85,025
BTC future     86,100
```

remain:

```
85,000
85,025
86,100
```

in their respective streams.

MarketForge does not replace these values with an internal index or artificially remove their differences.

Downstream systems may derive:

```
spread
basis
basis in bps
implied cross-price
relative return
cross-market divergence
synthetic index
```

without losing the original observations.

### Time Compatibility

Selected datasets must currently share a common time interval.

For example:

```
Dataset A:  Sep 1 ───────────── Sep 10
Dataset B:        Sep 3 ───────────── Sep 12
Dataset C:              Sep 5 ─ Sep 8
```

produces the available merge interval:

```
                        Sep 5 ─ Sep 8
```

Explicit `--start` and `--end` values may narrow this interval but cannot extend beyond it.

This ensures every selected stream has coverage throughout the merged interval.

### Options

Options processing and merging remain postponed.

Option metadata and architectural extensibility may remain in MarketForge, but option-specific processing, archive-member orchestration, BookStore handling, and merge semantics are outside the initial implementation scope.

```
Spot        → supported

Contracts
├── linear perpetual → supported
├── inverse perpetual → supported
├── linear futures    → supported
└── inverse futures   → supported

Cross-market combinations → supported

Trades + L2 → supported

Options → postponed
```

### Validation Flow

Python performs structural validation before Rust is invoked:

```
selected datasets
        ↓
resolve datasets
        ↓
all IDs found?
        ↓
status = complete?
        ↓
storage = Parquet?
        ↓
published storage path?
        ↓
supported canonical data type?
        ↓
common time range?
        ↓
resolve output type
        ↓
assign deterministic stream ranks
        ↓
MergeJob
        ↓
Rust chronological merge
```

Python does **not** reject a merge merely because instruments have different:

```
base assets
quote assets
settlement assets
instrument types
market categories
contract kinds
exchanges
price levels
```

Those distinctions are intentionally preserved for downstream analysis.

> **Merge rule:** Any supported canonical MarketForge datasets with a common time interval may be chronologically merged. MarketForge preserves their original canonical events and stream identities rather than forcing economic equivalence between the instruments.