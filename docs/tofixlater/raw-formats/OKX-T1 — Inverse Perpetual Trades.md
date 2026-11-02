### Identity

| Field | Value |
|---|---|
| Exchange | OKX |
| Instrument Type | Perpetual |
| Market Category | Inverse |
| Data Type | Trade ticks |
| Format ID | `OKX-T1` |
| Fixture | `BTC-USD-SWAP-trades-2026-09-01.zip` |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | CSV with header |
| Granularity | Daily, UTC+8 calendar day |

### Schema

| Column | Type | Meaning |
|---|---|---|
| `instrument_name` | string | Instrument ID |
| `trade_id` | integer/string | Unique trade ID |
| `side` | string | Taker side: `buy` / `sell` |
| `price` | decimal | Execution price |
| `size` | decimal | Executed contract quantity |
| `created_time` | integer | Unix timestamp in milliseconds |
| `source` | integer | Order source |

### Time

| Field | Value |
|---|---|
| Timestamp Column | `created_time` |
| Representation / Unit | Unix epoch milliseconds |
| Precision | Millisecond |
| Ordering | Chronological |
| Archive Day Boundary | UTC+8 |

Observed coverage:

```text
Archive: 2026-09-01

UTC:
2026-08-31 16:00:00
        ↓
2026-09-01 15:59:55
```

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `size`, contract quantity |
| Side | Taker side |
| Trade ID | `trade_id`, unique and consecutive |
| Source `0` | Normal order |
| Source `1` | RPI order |

### Order Book

N/A

### Example

```text
instrument_name,trade_id,side,price,size,created_time,source
BTC-USD-SWAP,472393587,buy,78515.6,1.0,1788192000074,0
```

### Notes

- 143,953 records observed.
- Single instrument: `BTC-USD-SWAP`.
- 75,253 buy / 68,700 sell.
- Trade IDs range from `472393587` to `472537539`.
- No missing, duplicate, or discontinuous trade IDs.
- No out-of-order timestamps.
- `source = 0` for every observed record.
- `size` represents derivative contract quantity.
- Archive uses a UTC+8 calendar boundary.
- Physical schema matches OKX Spot and Linear Perpetual trades.

### Compatibility

Compatible with:

- OKX Spot Trades
- OKX Linear Perpetual Trades

All use `OKX-T1`.

Quantity interpretation differs by instrument type:

```text
Spot
size → base-asset quantity

Linear Perpetual
size → contract quantity

Inverse Perpetual
size → contract quantity
```

Contract-to-base/quote conversion belongs in normalization and requires instrument metadata such as contract value.

**Parser:** `OKX-T1`  
**Ready for normalization:** Yes