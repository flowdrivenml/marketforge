Options support is postponed until a later MarketForge release.

The initial processing engine focuses on:

```text
Markets
├── Spot
├── Linear Perpetual
└── Inverse Perpetual

Data
├── Trades
└── L2 Order Book
```

This applies to the complete processing pipeline, including:

```text
raw processing
normalization
validation
BookStore
temporary runs
Parquet generation
chronological merging
cross-exchange merging
derived transformations
live processing
```

Option acquisition/archive metadata may remain in MarketForge, and existing canonical/protocol structures may retain option-compatible fields where already defined. However, no option-specific processing or merging logic is required for the initial implementation.

In particular, defer:

```text
option-chain archive member discovery
multi-instrument archive handling
option trade processors
option L2 processors
option BookStore orchestration
option merging
option transformations
option-specific live processing
```

The architecture should remain extensible enough to add options later without redesigning the core engine.

> **Scope rule:** MarketForge v1 processing supports spot, linear perpetual, and inverse perpetual markets. Options processing and merging are explicitly deferred.