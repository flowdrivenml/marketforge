### Identity

| Field | Value |
|---|---|
| Exchange | Bitget |
| Instrument Type | Perpetual |
| Market Category | Linear |
| Data Type | Trade ticks |
| Format ID | `BITGET-T1` |
| Fixture | `BTCUSDT-BTCUSDT_20260901_*.zip` |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | CSV with header |
| Granularity | Daily, UTC+8 calendar day |
| Daily Packaging | Multiple numbered ZIP chunks |

Observed day:

```text
20260901
├── _001 → 100,000 rows
├── _002 → 100,000
├── _003 → 100,000
├── _004 → 100,000
├── _005 → 100,000
├── _006 → 100,000
├── _007 → 100,000
├── _008 → 100,000
├── _009 → 100,000
└── _010 → 83,742
```

### Schema

| Column | Type | Meaning |
|---|---|---|
| `trade_id` | integer/string | Unique trade ID |
| `timestamp` | integer | Unix timestamp encoded in milliseconds |
| `price` | decimal | Execution price |
| `side` | string | `buy` / `sell` |
| `volume(quote)` | decimal | Quote quantity |
| `size(base)` | decimal | Base quantity |

### Time

| Field | Value |
|---|---|
| Timestamp | `timestamp` |
| Representation / Unit | Unix epoch milliseconds |
| Observed Effective Precision | 1 second |
| Ordering | Chronological |
| Archive Day Boundary | UTC+8 |

Observed coverage:

```text
2026-08-31 16:00:00 UTC
        ↓
2026-09-01 15:59:59 UTC
```

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Base Quantity | `size(base)` |
| Quote Quantity | `volume(quote)` |
| Side | `buy` / `sell` |
| Trade ID | `trade_id`, unique |

Observed relationship:

```text
volume(quote) = price × size(base)
```

### Order Book

N/A

### Example

```text
trade_id,timestamp,price,side,volume(quote),size(base)
1478349886673666048,1788192000000,78546,buy,7.8546,0.0001
```

### Notes

- 983,742 records observed.
- Day split across 10 ZIP chunks.
- First 9 chunks contain exactly 100,000 records each.
- Final chunk contains 83,742 records.
- 489,284 buy / 494,458 sell.
- No missing trade IDs.
- No duplicate trade IDs.
- No out-of-order timestamps.
- 923,796 records share their timestamp with the immediately preceding record.
- No observed timestamp contains sub-second information.
- `volume(quote) = price × size(base)` for every observed record.
- Archive uses a UTC+8 calendar boundary.
- Physical schema matches Bitget Spot trades.

### Compatibility

Compatible with Bitget Spot trades:

```text
Bitget Spot Trades             ─┐
                               ├── BITGET-T1
Bitget Linear Perpetual Trades ─┘
```

Both expose normalized base and quote quantities directly:

```text
size(base)
volume(quote)
```

**Parser:** `BITGET-T1`  
**Ready for normalization:** Yes