### Identity

| Field | Value |
|---|---|
| Exchange | Bitget |
| Instrument Type | Spot |
| Market Category | Spot |
| Data Type | Trade ticks |
| Format ID | `BITGET-T1` |
| Fixture | `USDT-BTCUSDT_20260901_*.zip` |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | CSV with header |
| Granularity | Daily, UTC+8 calendar day |
| Daily Packaging | One or more numbered ZIP chunks |

Example:

```text
USDT-BTCUSDT_20260901_001.zip
USDT-BTCUSDT_20260901_002.zip
```

Each archive contains one CSV.

### Schema

| Column | Type | Meaning |
|---|---|---|
| `trade_id` | integer/string | Unique trade ID |
| `timestamp` | integer | Unix timestamp encoded in milliseconds |
| `price` | decimal | Execution price |
| `side` | string | `buy` / `sell` |
| `volume(quote)` | decimal | Quote-asset quantity |
| `size(base)` | decimal | Base-asset quantity |

### Time

| Field | Value |
|---|---|
| Timestamp Column | `timestamp` |
| Representation / Unit | Unix epoch milliseconds |
| Observed Effective Precision | 1 second |
| Ordering | Chronological |
| Archive Day Boundary | UTC+8 |

All observed timestamps were divisible by `1000`.

Observed coverage:

```text
Archive date: 2026-09-01

UTC:
2026-08-31 16:00:00
        ↓
2026-09-01 15:59:59
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
1478349886556192768,1788192000000,78575,buy,3.92875,0.00005
```

### Notes

- 191,651 records observed across the complete archive day.
- Day split across 2 ZIP chunks.
- `_001`: 100,000 records.
- `_002`: 91,651 records.
- Chunk boundary is chronological.
- 86,903 buy / 104,748 sell.
- No missing trade IDs.
- No duplicate trade IDs.
- No out-of-order timestamps.
- 154,869 records share a timestamp with the immediately preceding record.
- No observed timestamp contained sub-second information.
- `volume(quote) = price × size(base)` for every observed record.
- Archive uses a UTC+8 calendar boundary.
- Trade IDs should be treated as identifiers, not assumed to be consecutive integers.

### Compatibility

Not determined yet.

**Parser:** `BITGET-T1`  
**Ready for normalization:** Yes