### Identity

| Field | Value |
|---|---|
| Exchange | Bitget |
| Instrument Type | Perpetual |
| Market Category | Inverse |
| Data Type | Order book L2 |
| Format ID | `BITGET-B1` |
| Fixture | `BTCUSD_CM-BTCUSDCM_3_20260901.zip` |

### Physical Format

| Field | Value |
|---|---|
| Outer Container | ZIP / deflate |
| Internal File | XLSX |
| Workbook Sheets | 1 |
| Internal Representation | Spreadsheet rows |
| Granularity | Daily, UTC+8 calendar day |

Container:

```text
BTCUSD_CM-BTCUSDCM_3_20260901.zip
└── 20260901_8.xlsx
    └── Sheet1
```

### Schema

| Column | Type | Meaning |
|---|---|---|
| `timestamp` | integer | Unix timestamp in seconds |
| `asks` | string | JSON-encoded ask levels |
| `bids` | string | JSON-encoded bid levels |

Each decoded level:

```text
[price, quantity]
```

### Time

| Field | Value |
|---|---|
| Timestamp Column | `timestamp` |
| Representation / Unit | Unix epoch seconds |
| Precision | Second |
| Raw Row Ordering | Not chronological |
| Archive Day Boundary | UTC+8 |

Observed timestamp range:

```text
2026-08-31 16:00:23 UTC
        ↓
2026-09-01 15:57:25 UTC
```

Raw workbook ordering must not be trusted.

### Semantics

| Field | Value |
|---|---|
| Ask Price | Level field 1 |
| Ask Quantity | Level field 2 |
| Bid Price | Level field 1 |
| Bid Quantity | Level field 2 |
| Ask Ordering | Ascending price |
| Bid Ordering | Descending price |

Example conceptual row:

```text
timestamp | asks                         | bids
----------|------------------------------|-----------------------------
...       | [[price, qty], [price, qty]] | [[price, qty], [price, qty]]
```

### Order Book

| Field | Value |
|---|---|
| Event Model | Standalone book snapshots |
| Snapshot Depth | Variable |
| Delta Updates | Not present |
| Sequence ID | Not present |
| Update ID | Not present |
| Reconstruction | Not required between rows |
| Sorting Required | Yes, by timestamp |

Observed depth:

```text
Asks:
min = 98
max = 132
avg = 117.93

Bids:
min = 168
max = 196
avg = 180.40
```

All observed:

```text
asks → ascending by price
bids → descending by price
level → exactly [price, quantity]
```

### Example

```text
timestamp = 1788226905

asks = [
    [78514.2, 2000.0],
    [78517.4, 922500.0],
    ...
]

bids = [
    [78493.8, 2000.0],
    [78489.6, 1091250.0],
    ...
]
```

### Notes

- 4,293 snapshot rows observed.
- 4,292 unique timestamps.
- One timestamp therefore occurs more than once, but not in adjacent raw rows.
- 2,105 adjacent timestamp reversals observed.
- Raw workbook is not chronologically ordered.
- No empty ask books observed.
- No empty bid books observed.
- Every observed level contains exactly 2 fields.
- Every observed ask book is price-ascending.
- Every observed bid book is price-descending.
- Snapshot depth is variable.
- No snapshot/delta marker exists because rows are represented as standalone books.
- No sequence or update identifier observed.
- Quantity conversion semantics are not established by this inspection alone.

### Normalization Requirements

```text
ZIP
 ↓
XLSX
 ↓
Sheet1
 ↓
parse timestamp
parse asks JSON
parse bids JSON
 ↓
sort snapshots by timestamp
 ↓
emit normalized L2 snapshots
```

Do not reconstruct a book by applying rows to previous rows.

Do not assume workbook row order is chronological.

Do not infer base/quote quantity from the raw quantity without instrument metadata.

### Compatibility

Not compatible with Bybit or OKX incremental L2 formats.

```text
Bybit
snapshot + delta stream

OKX
snapshot + update stream

Bitget BITGET-B1
independent snapshot rows
```

**Parser:** `BITGET-B1`  
**Ready for normalization:** Yes