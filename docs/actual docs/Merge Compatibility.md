MarketForge validates dataset compatibility **before creating a `MergeJob`**.

Compatibility is determined from structured instrument metadata in PostgreSQL rather than exchange-specific symbol names.

### Spot Markets

Spot datasets may only be merged with other **spot datasets**.

All selected spot datasets must share the same quote/common denominator.

Compatible:

```text
BTC/USDT
ETH/USDT
SOL/USDT
    ↓
✓ merge
```

Incompatible:

```text
BTC/USDT
ETH/BTC
SOL/USDC
    ↓
✗ reject
```

The base assets may differ:

```text
BTC/USDT
ETH/USDT
SOL/USDT
```

because all prices share the same `USDT` denominator.

Exchange-specific symbol notation does not matter:

```text
BTCUSDT
BTC-USDT
BTC_USDT
```

MarketForge uses structured metadata such as:

```text
base_asset
quote_asset
settlement_asset
instrument_type
market_category
```

rather than comparing symbol strings directly. `catalog.instruments` already stores these fields. :chatgpt-content-reference{index="0"}

### Contract Markets

Contract datasets may be merged together.

This includes different:

```text
underlying assets
exchanges
linear contracts
inverse contracts
perpetual contracts
futures contracts
```

For example:

```text
Bybit BTCUSDT linear perpetual
Binance ETHUSDT linear perpetual
OKX SOL-USDT-SWAP
Bybit BTCUSD inverse perpetual
```

may participate in the same merged contract dataset.

Conceptually:

```text
contract dataset A ─┐
contract dataset B ─┤
contract dataset C ─┼→ chronological merge
contract dataset D ─┘
                         ↓
                  ONE canonical dataset
```

Each canonical event retains its own exchange, instrument, and stream identity, so different contracts remain distinguishable after merging.

### Spot and Contracts

Spot and contract datasets are separate market families and must not be mixed.

```text
BTCUSDT spot
        +
BTCUSDT perpetual
        ↓
        ✗
```

Even when the symbols or underlying assets are related:

```text
SPOT + SPOT           → allowed if common denominator

CONTRACT + CONTRACT   → allowed

SPOT + CONTRACT       → rejected
```

### Options

Options processing and merging are postponed.

Option metadata and architectural extensibility may remain in MarketForge, but option-specific processing, BookStore handling, archive-member orchestration, and merging are outside the initial implementation scope.

```text
Spot        → supported

Contracts
├── linear  → supported
└── inverse → supported

Options     → postponed
```

### Validation Flow

Python performs compatibility validation before Rust is invoked:

```text
selected datasets
        ↓
load catalog metadata
        ↓
classify market family
        ↓
┌─────────────────────────────────────┐
│ all spot?                           │
│   require common quote denominator │
│                                     │
│ all contracts?                      │
│   allow                             │
│                                     │
│ spot + contracts?                   │
│   reject                            │
│                                     │
│ options?                            │
│   unsupported for now               │
└─────────────────────────────────────┘
        ↓
compatible?
   │         │
  yes        no
   │         │
   ↓         ↓
MergeJob    CLI validation error
   ↓
Rust
```

> **Merge rule:** Spot datasets may only be merged with spot datasets sharing a common quote denominator. Contract datasets may be merged together. Spot and contract datasets cannot be mixed. Options are postponed.