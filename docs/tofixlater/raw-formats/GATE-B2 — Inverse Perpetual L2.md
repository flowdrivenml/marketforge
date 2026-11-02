### Identity

| Field | Value |
|---|---|
| Exchange | Gate.io |
| Instrument Type | Perpetual |
| Market Category | Inverse |
| Data Type | Order book L2 |
| Format ID | `GATE-B2` |
| Fixture | `BTC_USD-2026090700.csv.gz` |

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
| `begin_id` | integer | First underlying update ID |
| `merged_count` | integer | Number of underlying updates represented |

### Semantics

Book side is encoded by signed size:

```text
size > 0 → bid / buy side
size < 0 → ask / sell side

quantity_contracts = abs(size)
```

Base/quote conversion requires inverse-contract metadata.

### Order Book

Each hourly file begins with a complete `set` snapshot:

```text
set snapshot
     ↓
make / take
     ↓
make / take
     ↓
...
```

Initial snapshot:

```text
rows      = 1,691
timestamp = 1788739200
begin_id  = 5773525244
```

Snapshot side distribution:

```text
positive = 923
negative = 768
zero     = 0
```

All snapshot rows share the same timestamp and `begin_id`.

### Updates

```text
make = 200,911
take = 201,948

total updates = 402,859
```

Sign distribution:

```text
make:
positive = 127,325
negative =  73,586

take:
positive = 128,083
negative =  73,865
```

First update:

```text
1788739200.1
begin_id = 5773525245
```

Last update:

```text
1788742800.0
begin_id = 5773930544
```

### Sequence

Observed continuity rule:

```text
next.begin_id =
    current.begin_id + current.merged_count
```

Result:

```text
ID DISCONTINUITIES: 0
OUT OF ORDER:       0
```

### Example

Snapshot:

```text
1788739200,set,3600.6,5.0,5773525244,0
```

Updates:

```text
1788742800.0,make,80101.8,-22.0,5773930543,1
1788742800.0,take,80101.8,-22.0,5773930544,1
```

### Notes

- 404,550 total rows.
- 1,691 snapshot rows.
- 402,859 incremental updates.
- No zero-size records observed.
- Snapshot uses one timestamp and one `begin_id`.
- No snapshot timestamp or ID mismatches.
- No update sequence discontinuities.
- No out-of-order updates observed.
- Side is encoded through signed contract size.
- Same physical representation as Gate Linear Perpetual L2.

### Reconstruction

```text
initial `set` rows
        ↓
decode side from sign(size)
        ↓
construct full book
        ↓
apply make/take updates
        ↓
validate begin_id + merged_count continuity
```

### Compatibility

```text
Gate Linear Perpetual L2   ─┐
                             ├── GATE-B2
Gate Inverse Perpetual L2  ─┘
```

Physical parser is identical.

Contract interpretation differs during normalization:

```text
Linear
contracts → linear contract metadata → base/quote quantity

Inverse
contracts → inverse contract metadata → base/quote quantity
```

**Parser:** `GATE-B2`  
**Ready for normalization:** Yes