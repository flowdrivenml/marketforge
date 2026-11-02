### Identity

| Field | Value |
|---|---|
| Exchange | OKX |
| Instrument Type | Perpetual |
| Market Category | Linear |
| Data Type | Trade ticks |
| Format ID | `OKX-T1` |
| Fixture | `BTC-USDT-SWAP-trades-2026-09-01.zip` |

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
2026-09-01 15:59:59
```

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `size`, number of contracts |
| Side | Taker side |
| Trade ID | `trade_id`, unique and consecutive |
| Source `0` | Normal order |
| Source `1` | RPI order |

### Order Book

N/A

### Example

```text
instrument_name,trade_id,side,price,size,created_time,source
BTC-USDT-SWAP,2885385355,sell,78542.2,14.07,1788192000021,0
```

### Notes

- 2,373,050 records observed.
- Single instrument: `BTC-USDT-SWAP`.
- 1,198,884 buy / 1,174,166 sell.
- Trade IDs range from `2885385355` to `2887758404`.
- No missing, duplicate, or discontinuous trade IDs.
- No out-of-order timestamps.
- `source = 0` for every observed record.
- `size` is contract quantity, unlike Spot `OKX-T1`, where `size` is base-currency quantity.
- Archive uses a UTC+8 calendar boundary.
- Physical schema matches OKX Spot trades exactly.

### Compatibility

Compatible with OKX Spot trades at the physical parsing level:

```text
OKX Spot
size = base-currency quantity

OKX Linear Perpetual
size = contract quantity
```

Normalization must interpret `size` according to instrument type.

**Parser:** `OKX-T1`  
**Ready for normalization:** Yes