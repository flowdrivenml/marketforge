### Identity

| Field | Value |
|---|---|
| Exchange | Bybit |
| Instrument Type | Perpetual |
| Market Category | Inverse |
| Data Type | Trade ticks |
| Format ID | `BYBIT-T2` |
| Fixture | `BTCUSD2026-09-01.csv.gz` |

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
| `side` | string | `Buy` / `Sell` |
| `size` | decimal | Executed contract size |
| `price` | decimal | Execution price |
| `tickDirection` | string | Direction of price change |
| `trdMatchID` | UUID/string | Unique trade ID |
| `grossValue` | decimal | `foreignNotional × 1e8` |
| `homeNotional` | decimal | Equal to `size` |
| `foreignNotional` | decimal | `size / price` |
| `RPI` | integer/bool | RPI trade flag |

### Time

| Field | Value |
|---|---|
| Timestamp Column | `timestamp` |
| Representation / Unit | Unix epoch seconds |
| Precision | Up to 4 fractional digits |
| Ordering | Chronological |

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `size` |
| Side | `Buy` / `Sell` |
| Trade ID | `trdMatchID`, unique |
| Tick Direction | `PlusTick`, `MinusTick`, `ZeroPlusTick`, `ZeroMinusTick` |
| Inverse Notional | `foreignNotional = size / price` |

### Order Book

N/A

### Example

```text
timestamp,symbol,side,size,price,tickDirection,trdMatchID,grossValue,homeNotional,foreignNotional,RPI
1788220801.0925,BTCUSD,Sell,79,78513.00,PlusTick,e94f18f9-4bc4-5530-87ee-d067f8267a62,100620.27944416848,79,0.0010062027944416848,0
```

### Notes

- 54,049 records observed.
- No out-of-order timestamps.
- All trade IDs present and unique.
- `homeNotional = size` for every observed record.
- `foreignNotional = size / price` for every observed record.
- `grossValue = foreignNotional × 1e8` for every observed record.
- `RPI`: both `0` and `1` observed.
- Physical schema matches Bybit Linear Perpetual trades.
- Notional semantics differ from Linear contracts.

### Compatibility

Compatible with Bybit Linear Perpetual trades at the physical parsing level (`BYBIT-T2`).

Linear:

```text
foreignNotional = size × price
```

Inverse:

```text
foreignNotional = size / price
```

Normalization must therefore use the market category when interpreting notionals.

**Parser:** `BYBIT-T2`  
**Ready for normalization:** Yes