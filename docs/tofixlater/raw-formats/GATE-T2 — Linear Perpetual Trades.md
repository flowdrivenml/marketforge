### Identity

| Field | Value |
|---|---|
| Exchange | Gate.io |
| Instrument Type | Perpetual |
| Market Category | Linear |
| Data Type | Trade ticks |
| Format ID | `GATE-T2` |
| Fixture | `BTC_USDT-202608.csv.gz` |

### Physical Format

| Field | Value |
|---|---|
| Archive | GZIP |
| Internal Format | Headerless CSV |
| Granularity | Monthly |
| Columns | 4 |

### Schema

Raw column order:

```text
timestamp,trade_id,price,size
```

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | decimal | Unix timestamp in seconds |
| `trade_id` | integer | Trade/fill ID |
| `price` | decimal | Execution price |
| `size` | signed integer | Number of contracts + trade direction |

### Time

| Field | Value |
|---|---|
| Timestamp | `timestamp` |
| Representation / Unit | Unix epoch seconds |
| Precision | Microsecond |
| Decimal Digits | Exactly 6 observed |
| Ordering | Chronological |
| Archive Granularity | Calendar month |

Observed coverage:

```text
2026-08-01 00:00:00.013040 UTC
        ↓
2026-08-31 23:59:59.476656 UTC
```

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | Absolute value of `size`, in contracts |
| Buy | `size > 0` |
| Sell | `size < 0` |
| Trade ID | `trade_id` |

Direction and magnitude are encoded together:

```text
 size = 671   → buy  671 contracts
 size = -3183 → sell 3183 contracts
```

Contract-to-underlying conversion requires instrument metadata:

```text
contracts × quanto_multiplier
```

### Order Book

N/A

### Example

```text
1785542400.013040,802737548,62855,671
```

Interpretation:

```text
timestamp = 1785542400.013040
trade_id  = 802737548
price     = 62855
size      = +671 contracts
side      = buy
```

### Notes

- 21,349,640 records observed.
- Headerless CSV.
- 10,703,480 positive-size trades.
- 10,646,160 negative-size trades.
- No zero-size trades.
- Trade IDs range from `802737548` to `824087187`.
- No trade-ID gaps.
- No repeated trade IDs.
- No out-of-order timestamps.
- No repeated timestamps.
- Every timestamp contains exactly 6 fractional digits.
- No non-positive prices observed.
- Quantity is contract count, not directly base-asset quantity.
- Side is encoded by the sign of `size`.

### Compatibility

Not compatible with `GATE-T1`.

```text
GATE-T1 — Spot
timestamp,trade_id,price,amount,side

GATE-T2 — Perpetual
timestamp,trade_id,price,signed_size
```

Normalization:

```text
side =
    size > 0 → buy
    size < 0 → sell

quantity_contracts = abs(size)

quantity_base =
    abs(size) × instrument.quanto_multiplier
```

**Parser:** `GATE-T2`  
**Ready for normalization:** Yes