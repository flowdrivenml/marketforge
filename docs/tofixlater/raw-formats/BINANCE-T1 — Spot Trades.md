### Identity

| Field | Value |
|---|---|
| Exchange | Binance |
| Instrument Type | Spot |
| Market Category | Spot |
| Data Type | Trade ticks |
| Format ID | `BINANCE-T1` |
| Fixture | `BTCUSDT-trades-2026-09-01.zip` |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | CSV without header |
| Granularity | Daily |

### Schema

| Position | Type | Meaning |
|---:|---|---|
| 1 | integer | Trade ID |
| 2 | decimal | Price |
| 3 | decimal | Base quantity |
| 4 | decimal | Quote quantity |
| 5 | integer | Unix timestamp in microseconds |
| 6 | boolean | Buyer is maker |
| 7 | boolean | Best-match flag |

### Time

| Field | Value |
|---|---|
| Timestamp | Column 5 |
| Representation / Unit | Unix epoch microseconds |
| Precision | Microsecond |
| Ordering | Chronological |

### Semantics

| Field | Value |
|---|---|
| Price | Column 2 |
| Quantity | Column 3, base quantity |
| Quote Quantity | Column 4 = price × quantity |
| Trade ID | Column 1, consecutive |
| Aggressor Side | Derived from `isBuyerMaker` |
| Best Match | Column 7 |

Aggressor-side mapping:

```text
isBuyerMaker = False → Buy
isBuyerMaker = True  → Sell
```

### Order Book

N/A

### Example

```text
6642078844,78581.30000000,0.00012000,9.42975600,1788220800322740,False,True
```

### Notes

- 3,662,805 records observed.
- Trade IDs range from `6642078844` to `6645741648`.
- No trade-ID gaps or repeats.
- No out-of-order timestamps.
- `isBuyerMaker`: 2,036,926 False / 1,625,879 True.
- `isBestMatch` was True for every observed record.
- `quoteQty = price × qty` for every observed record.
- CSV contains no header.

### Compatibility

Not determined yet.

**Parser:** `BINANCE-T1`  
**Ready for normalization:** Yes