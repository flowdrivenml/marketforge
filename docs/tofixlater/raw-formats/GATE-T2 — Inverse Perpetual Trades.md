### Identity

| Field | Value |
|---|---|
| Exchange | Gate.io |
| Instrument Type | Perpetual |
| Market Category | Inverse |
| Data Type | Trade ticks |
| Format ID | `GATE-T2` |
| Fixture | `BTC_USD-202608.csv.gz` |

### Physical Format

| Field | Value |
|---|---|
| Archive | GZIP |
| Internal Format | Headerless CSV |
| Granularity | Monthly |
| Columns | 4 |

### Schema

```text
timestamp,trade_id,price,size
```

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | decimal | Unix timestamp in seconds |
| `trade_id` | integer | Trade ID |
| `price` | decimal | Execution price |
| `size` | signed integer | Contract quantity + direction |

### Time

| Field | Value |
|---|---|
| Representation | Unix epoch seconds |
| Precision | Microsecond |
| Decimal Digits | Exactly 6 observed |
| Ordering | Chronological |
| Archive Granularity | Calendar month |

Observed raw range:

```text
1785542504.264791
        ↓
1788220630.145493
```

### Semantics

| Field | Value |
|---|---|
| Price | `price` |
| Quantity | `abs(size)`, contracts |
| Buy | `size > 0` |
| Sell | `size < 0` |
| Trade ID | `trade_id` |

Example:

```text
1785542504.264791,49056826,62847,312
```

Interpretation:

```text
timestamp = 1785542504.264791
trade_id  = 49056826
price     = 62847
size      = +312 contracts
side      = buy
```

### Notes

- 206,757 records observed.
- 110,744 positive-size trades.
- 96,013 negative-size trades.
- No zero-size trades.
- Trade IDs range from `49056826` to `49263582`.
- No trade-ID gaps.
- No repeated trade IDs.
- No out-of-order timestamps.
- No repeated timestamps.
- Every timestamp contains exactly 6 fractional digits.
- No non-positive prices observed.
- Quantity is expressed as contracts.
- Base/quote notional conversion requires instrument metadata.

### Compatibility

Identical physical parser to Linear Perpetual:

```text
Gate Linear Perpetual   ─┐
                         ├── GATE-T2
Gate Inverse Perpetual  ─┘

timestamp,trade_id,price,signed_size
```

Market-specific contract metadata determines normalization:

```text
raw size
   ↓
abs(size) = contracts
   ↓
contract specification
   ↓
base / quote quantity
```

**Parser:** `GATE-T2`  
**Ready for normalization:** Yes