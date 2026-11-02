### Identity

| Field | Value |
|---|---|
| Exchange | Gate.io |
| Instrument Type | Spot |
| Market Category | Spot |
| Data Type | Order book L2 |
| Format ID | `GATE-B1` |
| Fixture | `BTC_USDT-2026090100.csv.gz` |

### Physical Format

| Field | Value |
|---|---|
| Archive | GZIP |
| Internal Format | Headerless CSV |
| Granularity | Hourly |
| Columns | 7 |

### Schema

```text
timestamp,side,action,price,amount,begin_id,merged_count
```

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | decimal | Unix timestamp in seconds |
| `side` | integer | Book side |
| `action` | string | `set`, `make`, `take` |
| `price` | decimal | Price level |
| `amount` | decimal | Quantity |
| `begin_id` | integer | First underlying update ID represented |
| `merged_count` | integer | Number of underlying updates represented |

Side encoding:

```text
1 → ask / sell
2 → bid / buy
```

### Time

| Record | Representation |
|---|---|
| Initial `set` | Integer Unix seconds |
| Updates | Unix seconds with 1 decimal digit |
| Update Resolution | 100 ms |
| Ordering | Chronological |
| Archive Granularity | Hourly |

Observed:

```text
snapshot: 1788220800

updates:
1788220800.0
...
1788224400.0
```

### Order Book Model

Each hourly file begins with a complete `set` snapshot:

```text
set
├── asks: 24,732 levels
└── bids: 29,191 levels
```

All snapshot rows share:

```text
timestamp = 1788220800
begin_id  = 39510882245
merged_count = 0
```

The snapshot is followed by incremental updates:

```text
set snapshot
     ↓
make / take
     ↓
make / take
     ↓
...
```

Observed actions:

| Action | Rows |
|---|---:|
| `set` | 53,923 |
| `make` | 308,116 |
| `take` | 306,934 |

### Sequence Semantics

First update:

```text
begin_id = 39510882246
```

Last update:

```text
begin_id = 39511513966
```

Observed sequence relationship:

```text
next.begin_id =
    current.begin_id + current.merged_count
```

Result:

```text
UPDATE ID DISCONTINUITIES: 0
```

`merged_count` distribution:

```text
0  → 53,923   # snapshot rows
1  → 602,527
2  → 10,060
3  → 1,519
4  → 531
5  → 251
6  → 93
7  → 34
8  → 11
9  → 6
10 → 5
11 → 7
12 → 5
13 → 1
```

Therefore one CSV update row may represent more than one underlying book update.

### Example

Snapshot:

```text
1788220800,2,set,0.01,13717.4576400000000000,39510882245,0
```

Update:

```text
1788224399.9,2,take,78610.1,1.952039,39511513962,1
```

### Notes

- 668,973 total rows.
- 53,923 initial snapshot rows.
- 615,050 incremental update rows.
- Snapshot contains 24,732 ask levels and 29,191 bid levels.
- Snapshot uses one timestamp and one `begin_id`.
- Snapshot rows use `merged_count = 0`.
- Updates use 100 ms timestamp resolution.
- No out-of-order rows observed.
- No update-ID discontinuities observed.
- No non-positive prices observed.
- No negative amounts observed.
- `merged_count` ranges from 1 to 13 for observed updates.
- Most updates represent one underlying update.
- No separate sequence field exists beyond `begin_id` / `merged_count`.

### Reconstruction

```text
1. Read all initial `set` rows
        ↓
2. Construct complete bid/ask book
        ↓
3. Process subsequent rows chronologically
        ↓
4. Apply `make` / `take` updates
        ↓
5. Validate sequence continuity using:

   next.begin_id =
       current.begin_id + current.merged_count
```

The initial snapshot should be treated as one logical event despite occupying 53,923 physical CSV rows.

### Compatibility

New Gate order-book format:

```text
GATE-B1
hourly GZIP
    ↓
headerless CSV
    ↓
large initial `set` snapshot
    ↓
100 ms `make` / `take` update stream
    ↓
begin_id + merged_count sequencing
```

This is fundamentally different from:

```text
Bybit → JSON snapshot + delta events
OKX   → JSON snapshot + update events
Bitget → standalone XLSX snapshots
Gate   → row-oriented snapshot + incremental CSV updates
```

**Parser:** `GATE-B1`  
**Ready for normalization:** Yes