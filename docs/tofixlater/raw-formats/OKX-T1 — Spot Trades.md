### Identity

| Field | Value |
|---|---|
| Exchange | OKX |
| Instrument Type | Spot |
| Market Category | Spot |
| Data Type | Trade ticks |
| Format ID | `OKX-T1` |
| Fixture | `BTC-USDT-trades-2026-09-01.zip` |

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
| `size` | decimal | Executed base-asset quantity |
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

Observed archive coverage:

```text
Archive: 2026-09-01

UTC:
2026-08-31 16:00:00
        ↓
2026-09-01 15:59:56
```

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `size`, base currency |
| Side | Taker side |
| Trade ID | `trade_id`, unique and consecutive |
| Source `0` | Normal order |
| Source `1` | RPI order |

### Order Book

N/A

### Example

```text
instrument_name,trade_id,side,price,size,created_time,source
BTC-USDT,1051558370,buy,78588.5,7.98e-05,1788192000381,0
```

### Notes

- 375,373 records observed.
- Single instrument: `BTC-USDT`.
- 197,291 buy / 178,082 sell.
- Trade IDs range from `1051558370` to `1051933742`.
- No missing, duplicate, or discontinuous trade IDs.
- No out-of-order timestamps.
- `source = 0`: 371,791 records.
- `source = 1`: 3,582 records.
- Archive date uses a UTC+8 calendar boundary rather than UTC.

### Compatibility

Not determined yet.

**Parser:** `OKX-T1`  
**Ready for normalization:** Yes