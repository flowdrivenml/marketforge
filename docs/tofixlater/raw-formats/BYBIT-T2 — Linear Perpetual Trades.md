### Identity

| Field | Value |
|---|---|
| Exchange | Bybit |
| Instrument Type | Perpetual |
| Market Category | Linear |
| Data Type | Trade ticks |
| Format ID | `BYBIT-T2` |
| Fixture | `BTCUSDT2026-09-01.csv.gz` |

### Physical Format

| Field | Value |
|---|---|
| Archive | GZIP |
| Internal Format | CSV |
| Granularity | Daily |

### Schema

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | decimal | Unix timestamp in fractional seconds |
| `symbol` | string | Instrument symbol |
| `side` | string | Taker side: `Buy` / `Sell` |
| `size` | decimal | Executed size |
| `price` | decimal | Execution price |
| `tickDirection` | string | Direction of price change |
| `trdMatchID` | UUID/string | Unique trade ID |
| `grossValue` | decimal | `foreignNotional × 1e8` |
| `homeNotional` | decimal | Equal to `size` for this linear fixture |
| `foreignNotional` | decimal | `size × price` |
| `RPI` | integer/bool | RPI trade flag |

### Time

| Field | Value |
|---|---|
| Timestamp Column | `timestamp` |
| Representation / Unit | Unix epoch seconds |
| Precision | Up to 4 fractional digits (100 µs representation) |
| Ordering | Chronological |

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `size` |
| Side | Taker side |
| Trade ID | `trdMatchID`, unique |
| Tick Direction | `PlusTick`, `MinusTick`, `ZeroPlusTick`, `ZeroMinusTick` |

### Order Book

N/A

### Example

```text
timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional,RPI
1788220800.1169,BTCUSDT,Sell,0.001,78541.70,MinusTick,555a5c11-8b1d-5c5f-81b1-7f0bfc2ef2f3,7.854169999999999e+09,0.001,78.54169999999999,0
```

### Notes

- 2,017,359 records observed.
- No out-of-order timestamps.
- All trade IDs present and unique.
- `homeNotional = size` for every observed record.
- `foreignNotional = size × price` for every observed record.
- `grossValue = foreignNotional × 1e8` for every observed record.
- `RPI` was `0` for all records in this fixture.
- Structurally different from `BYBIT-T1`.

### Compatibility

Not determined yet.

**Parser:** `BYBIT-T2`  
**Ready for normalization:** Yes