### Identity

| Field | Value |
|---|---|
| Exchange | Binance |
| Instrument Type | Perpetual |
| Market Category | Inverse |
| Data Type | Trade ticks |
| Format ID | `BINANCE-T3` |
| Fixture | `BTCUSD_PERP-trades-2026-09-01.zip` |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | CSV with header |
| Granularity | Daily archive |

### Schema

| Column | Type | Meaning |
|---|---|---|
| `id` | integer | Trade ID |
| `price` | decimal | Execution price |
| `qty` | decimal | Contract quantity |
| `base_qty` | decimal | Base-asset quantity |
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
| Quantity | `qty`, contract quantity |
| Base Quantity | `base_qty` |
| Trade ID | `id`, consecutive in observed fixture |
| Aggressor Side | Derived from `is_buyer_maker` |

For the observed `BTCUSD_PERP` contract:

```text
base_qty = qty × 100 / price
```

Aggressor-side mapping:

```text
is_buyer_maker = false → Buy
is_buyer_maker = true  → Sell
```

### Order Book

N/A

### Example

```text
id,price,qty,base_qty,time,is_buyer_maker
1151022504,78526.1,83.0,0.10569734,1788220802731,true
```

### Notes

- 14,800 records observed.
- Trade IDs range from `1151022504` to `1151037303`.
- No trade-ID gaps or repeats observed.
- No out-of-order timestamps.
- `is_buyer_maker` was `true` for all records in this fixture.
- `base_qty = qty × 100 / price` for every observed record.
- The `100` relationship is established for this BTCUSD_PERP fixture; it should not yet be assumed universal across all inverse contracts.
- Observed timestamps cover only part of the UTC day (`1788220802731` → `1788229425912`).
- Structurally similar to `BINANCE-T2`, but column 4 has different name and semantics.

### Compatibility

Related to `BINANCE-T2`, but kept separate because quantity semantics differ:

```text
BINANCE-T2 Linear
qty + quote_qty
quote_qty = qty × price

BINANCE-T3 Inverse
qty + base_qty
base_qty = qty × contract value / price
```

**Parser:** `BINANCE-T3`  
**Ready for normalization:** Yes