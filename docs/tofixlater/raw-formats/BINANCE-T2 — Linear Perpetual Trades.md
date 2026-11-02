### Identity

| Field           | Value                           |
| --------------- | ------------------------------- |
| Exchange        | Binance                         |
| Instrument Type | Perpetual                       |
| Market Category | Linear                          |
| Data Type       | Trade ticks                     |
| Format ID       | `BINANCE-T2`                    |
| Fixture         | `BTCUSDT-trades-2026-09-01.zip` |
|                 |                                 |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | CSV with header |
| Granularity | Daily |

### Schema

| Column | Type | Meaning |
|---|---|---|
| `id` | integer | Trade ID |
| `price` | decimal | Execution price |
| `qty` | decimal | Executed quantity |
| `quote_qty` | decimal | Quote quantity |
| `time` | integer | Unix timestamp in milliseconds |
| `is_buyer_maker` | boolean | Whether buyer is maker |

### Time

| Field | Value |
|---|---|
| Timestamp Column | `time` |
| Representation / Unit | Unix epoch milliseconds |
| Precision | Millisecond |
| Ordering | Chronological |

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `qty` |
| Quote Quantity | `quote_qty = price × qty` |
| Trade ID | `id`, unique but not contiguous |
| Aggressor Side | Derived from `is_buyer_maker` |

Aggressor-side mapping:

```text
is_buyer_maker = false → Buy
is_buyer_maker = true  → Sell
```

### Order Book

N/A

### Example

```text
id,price,qty,quote_qty,time,is_buyer_maker
8038326171,78549.6,0.024,1885.1904,1788220800003,false
```

### Notes

- 3,622,828 records observed.
- Trade IDs range from `8038326171` to `8041965208`.
- No repeated trade IDs observed.
- 15,899 trade-ID discontinuities observed.
- No out-of-order timestamps.
- `is_buyer_maker`: 1,868,165 false / 1,754,663 true.
- `quote_qty = price × qty` for every observed record.
- Structurally different from Binance Spot trades.
- Binance Spot uses a headerless 7-column format and microsecond timestamps.
- Binance Linear Perpetual uses a headered 6-column format and millisecond timestamps.

### Compatibility

Not compatible with `BINANCE-T1`.

**Parser:** `BINANCE-T2`  
**Ready for normalization:** Yes