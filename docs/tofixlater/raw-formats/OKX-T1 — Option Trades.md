### Identity

| Field | Value |
|---|---|
| Exchange | OKX |
| Instrument Type | Option |
| Market Category | Option |
| Data Type | Trade ticks |
| Format ID | `OKX-T1` |
| Fixture | `BTC-USD-optionchain-trades-2026-09-01.zip` |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | CSV with header |
| Members | 1 |
| Granularity | Daily option chain |
| Archive Boundary | UTC+8 |

Container:

```text
BTC-USD-optionchain-trades-2026-09-01.zip
└── BTC-USD-optionchain-trades-2026-09-01.csv
```

The single CSV contains trades from many option instruments.

### Schema

```text
instrument_name,trade_id,side,price,size,created_time,source
```

| Column | Type | Meaning |
|---|---|---|
| `instrument_name` | string | Option instrument ID |
| `trade_id` | integer/string | Trade ID within instrument |
| `side` | string | Taker side: `buy` / `sell` |
| `price` | decimal | Execution price |
| `size` | decimal | Option contract quantity |
| `created_time` | integer | Unix timestamp in milliseconds |
| `source` | integer | Order source |

### Time

| Field | Value |
|---|---|
| Timestamp | `created_time` |
| Representation / Unit | Unix epoch milliseconds |
| Precision | Millisecond |
| Raw Global Ordering | Not chronological |
| Archive Boundary | UTC+8 |

Observed coverage:

```text
Archive: 2026-09-01

UTC:
2026-08-31 16:00:12
        ↓
2026-09-01 15:59:33
```

The first and last physical CSV rows do not represent the chronological boundaries.

Observed:

```text
GLOBAL OUT OF ORDER: 311
```

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `size`, option contracts |
| Side | Taker side |
| Trade ID Scope | Per instrument |
| Source `0` | Normal order |
| Source `1` | RPI order |

Observed source values:

```text
0 → 7,702
```

No `source = 1` trades were observed in this fixture.

### Option Instruments

Observed:

```text
359 instruments

Calls:
4,077 trades

Puts:
3,625 trades
```

Instrument naming example:

```text
BTC-USD-260901-74000-P
│   │   │      │     │
BTC USD expiry strike put
```

Calls use:

```text
-C
```

Puts use:

```text
-P
```

### Trade IDs

Trade IDs are scoped to the individual option instrument.

Example:

```text
BTC-USD-260901-74000-P
trade_id = 485
trade_id = 486

BTC-USD-260901-75000-P
trade_id = 373
trade_id = 374
...
```

Observed validation:

```text
DUPLICATE (instrument, trade_id): 0
PER-INSTRUMENT ID GAPS:          0
```

Therefore the canonical raw identity should be treated as at least:

```text
(instrument_name, trade_id)
```

rather than:

```text
trade_id
```

alone.

### Order Book

N/A

### Example

```text
instrument_name,trade_id,side,price,size,created_time,source
BTC-USD-260901-74000-P,485,buy,0.0001,15.0,1788208540365,0
```

Interpretation:

```text
instrument = BTC-USD-260901-74000-P
trade_id   = 485
side       = buy
price      = 0.0001
size       = 15 contracts
timestamp  = 1788208540365
source     = 0
```

### Notes

- 7,702 trade records observed.
- 359 option instruments observed.
- 4,849 buys.
- 2,853 sells.
- 4,077 call trades.
- 3,625 put trades.
- No duplicate `(instrument_name, trade_id)` pairs.
- No per-instrument trade-ID gaps observed.
- `source = 0` for every observed trade.
- Rows are not globally chronological.
- 311 global timestamp reversals observed.
- Archive represents a UTC+8 calendar day.
- One option-chain CSV combines trades from many contracts.

### Normalization Requirements

```text
ZIP
 ↓
CSV
 ↓
parse OKX-T1 rows
 ↓
preserve instrument_name
 ↓
interpret trade_id within instrument
 ↓
interpret size as option contracts
 ↓
sort by created_time when global chronological order is required
```

Do not assume:

```text
trade_id is globally unique
```

Use:

```text
(instrument_name, trade_id)
```

for trade identity.

Do not assume raw CSV row order is globally chronological.

### Compatibility

Physical row format is identical across all inspected OKX trade datasets:

```text
Spot                 ─┐
Linear Perpetual     ─┤
Inverse Perpetual    ─┼── OKX-T1
Option               ─┘
```

All use:

```text
instrument_name,
trade_id,
side,
price,
size,
created_time,
source
```

Semantics differ:

```text
Spot
size → base quantity

Linear Perpetual
size → contracts

Inverse Perpetual
size → contracts

Option
size → option contracts
trade_id → per instrument
archive → combined option chain
```

**Parser:** `OKX-T1`  
**Ready for normalization:** Yes