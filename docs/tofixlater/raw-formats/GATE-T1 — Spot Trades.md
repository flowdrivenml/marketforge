### Identity

| Field | Value |
|---|---|
| Exchange | Gate.io |
| Instrument Type | Spot |
| Market Category | Spot |
| Data Type | Trade ticks |
| Format ID | `GATE-T1` |
| Fixture | `BTC_USDT-202608.csv.gz` |

### Physical Format

| Field | Value |
|---|---|
| Archive | GZIP |
| Internal Format | Headerless CSV |
| Granularity | Monthly |
| Columns | 5 |

### Schema

Raw column order:

```text
timestamp,dealid,price,amount,side
```

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | decimal | Unix timestamp in seconds |
| `dealid` | integer | Trade ID |
| `price` | decimal | Execution price |
| `amount` | decimal | Base-asset quantity |
| `side` | integer | Trade side |

Gate's historical-data documentation defines:

```text
side = 1 → sell
side = 2 → buy
```

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
2026-08-01 00:00:02.330460 UTC
        ↓
2026-08-31 23:59:59.082112 UTC
```

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `amount`, base asset |
| Trade ID | `dealid` |
| Sell | `side = 1` |
| Buy | `side = 2` |

### Order Book

N/A

### Example

```text
1785542402.330460,214219192,62892.7,0.000159,1
```

Equivalent interpretation:

```text
timestamp = 1785542402.330460
dealid    = 214219192
price     = 62892.7
amount    = 0.000159 BTC
side      = sell
```

### Notes

- 3,466,762 records observed.
- Headerless CSV.
- 1,694,326 sells.
- 1,772,436 buys.
- Trade IDs range from `214219192` to `217685953`.
- No trade-ID gaps.
- No repeated trade IDs.
- No out-of-order timestamps.
- No repeated timestamps.
- Every timestamp contains exactly 6 fractional digits.
- No non-positive prices observed.
- No non-positive quantities observed.
- Archive covers an entire calendar month rather than one day.
- Side encoding is exchange-specific: `1 = sell`, `2 = buy`.

### Compatibility

New physical format.

```text
GATE-T1
GZIP
 ↓
headerless CSV
 ↓
timestamp,dealid,price,amount,side
```

**Parser:** `GATE-T1`  
**Ready for normalization:** Yes