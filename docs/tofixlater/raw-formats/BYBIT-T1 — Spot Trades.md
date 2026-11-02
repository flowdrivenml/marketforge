### Identity

| Field | Value |
|---|---|
| Exchange | Bybit |
| Instrument Type | Spot |
| Market Category | Spot |
| Data Type | Trade ticks |
| Format ID | `BYBIT-T1` |
| Fixture | `BTCUSDT_2026-09-01.csv.gz` |

### Physical Format

| Field           | Value |
| --------------- | ----- |
| Archive         | GZIP  |
| Internal Format | CSV   |
| Granularity     | Daily |

### Schema

| Column | Type | Meaning |
|---|---|---|
| `id` | integer | Sequential row/trade identifier within archive |
| `timestamp` | integer | Unix timestamp in milliseconds |
| `price` | decimal | Execution price |
| `volume` | decimal | Executed trade size |
| `side` | string | Taker side: `buy` / `sell` |
| `rpi` | integer/bool | RPI trade flag: `0` / `1` |

### Time

| Field | Value |
|---|---|
| Timestamp Column | `timestamp` |
| Representation / Unit | Unix epoch milliseconds |
| Precision | Millisecond |
| Ordering | Chronological |

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `volume` |
| Side | Taker side |
| Trade / Sequence ID | `id`, sequential within archive |

### Order Book

N/A

### Example

```text
id,timestamp,price,volume,side,rpi
1,1788220800624,78580.8,0.00016,buy,0
2,1788220801011,78580.7,0.000984,sell,0
3,1788220801011,78580.7,0.001273,sell,0
```

### Notes

- 625,722 records observed.
- `id` is perfectly sequential from `1` through `625722`.
- `side`: 306,876 buy / 318,846 sell.
- `rpi`: 623,396 false / 2,326 true.
- Archive covers one UTC day.

### Compatibility

Not determined yet.

**Parser:** `BYBIT-T1`  
**Ready for normalization:** Yes