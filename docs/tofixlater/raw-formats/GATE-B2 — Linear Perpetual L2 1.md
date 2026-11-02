### Identity

| Field | Value |
|---|---|
| Exchange | Gate.io |
| Instrument Type | Perpetual |
| Market Category | Linear |
| Data Type | Order book L2 |
| Format ID | `GATE-B2` |
| Fixture | `BTC_USDT-2026090700.csv.gz` |

### Physical Format

| Field | Value |
|---|---|
| Archive | GZIP |
| Internal Format | Headerless CSV |
| Granularity | Hourly |
| Columns | 6 |

### Schema

```text
timestamp,action,price,size,begin_id,merged_count
```

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | decimal | Unix timestamp in seconds |
| `action` | string | `set`, `make`, `take` |
| `price` | decimal | Price level |
| `size` | signed decimal | Contract quantity + book side |
| `begin_id` | integer | First underlying order-book update ID |
| `merged_count` | integer | Number of underlying updates represented |

### Time

| Record | Representation |
|---|---|
| Initial `set` | Integer Unix seconds |
| Updates | Unix seconds at 100 ms resolution |
| Archive Granularity | Hourly |

Observed hour:

```text
1788739200
    ↓
1788742800.0
```

### Semantics

Unlike Gate Spot L2, Futures has no explicit side column.

Book side is encoded by signed `size`:

```text
size > 0 → bid / buy side
size < 0 → ask / sell side
```

Normalize as:

```text
side =
    size > 0 → bid
    size < 0 → ask

quantity_contracts = abs(size)
```

Contract-to-base/quote conversion requires instrument metadata.

### Order Book Model

Each hourly file begins with a full `set` snapshot:

```text
set snapshot
     ↓
make / take
     ↓
make / take
     ↓
...
```

Observed initial snapshot:

```text
rows      = 42,000
timestamp = 1788739200
begin_id  = 124318634771
```

Snapshot side distribution derived from signed size:

```text
positive = 24,760
negative = 17,240
zero     = 0
```

All snapshot rows share the same timestamp and `begin_id`.

### Updates

Observed:

```text
make = 2,061,926
take = 2,089,982
```

Amount/sign distribution:

```text
make:
positive = 1,268,466
negative =   793,460

take:
positive = 1,264,769
negative =   825,213
```

Total update rows:

```text
4,151,908
```

### Sequence Semantics

First update:

```text
timestamp = 1788739200.0
begin_id  = 124318634772
```

Last update:

```text
timestamp = 1788742800.0
begin_id  = 124323187525
```

Observed continuity rule:

```text
next.begin_id =
    current.begin_id + current.merged_count
```

Result:

```text
ID DISCONTINUITIES: 0
```

### Example

Snapshot:

```text
1788739200,set,0.1,5870051.0,124318634771,0
```

Updates:

```text
1788742800.0,make,79769.1,563511.0,124323187523,1
1788742800.0,take,80057.4,401882.0,124323187524,1
1788742800.0,make,80060.6,386772.0,124323187525,1
```

### Notes

- 4,193,908 total rows observed.
- 42,000 initial snapshot rows.
- 4,151,908 incremental update rows.
- No zero-size rows observed.
- Snapshot uses one timestamp and one `begin_id`.
- No sequence-ID discontinuities observed.
- 2 timestamp-order reversals observed among update rows.
- Futures quantities are contract sizes.
- Side is encoded through the sign of `size`.
- Updates are merged at 100 ms resolution.
- `merged_count` must be respected when validating sequence continuity.

### Reconstruction

```text
1. Read initial `set` rows
        ↓
2. Decode side from sign(size)
        ↓
3. Build full initial book
        ↓
4. Process make/take rows
        ↓
5. Decode side from sign(size)
        ↓
6. Validate:

   next.begin_id =
       current.begin_id + current.merged_count
```

Because raw timestamp ordering showed 2 reversals, normalization must not assume perfect timestamp monotonicity.

Sequence IDs should be retained for integrity validation.

### Compatibility

Gate Spot and Futures L2 share the same conceptual event model but require different physical parsers:

```text
GATE-B1 — Spot

timestamp,
side,
action,
price,
amount,
begin_id,
merged_count
```

```text
GATE-B2 — Futures

timestamp,
action,
price,
signed_size,
begin_id,
merged_count
```

Both:

```text
hourly GZIP
    ↓
full `set` snapshot
    ↓
100 ms make/take updates
    ↓
begin_id + merged_count sequencing
```

**Parser:** `GATE-B2`  
**Ready for normalization:** Yes