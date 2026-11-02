### Identity

| Field | Value |
|---|---|
| Exchange | Bitget |
| Instrument Type | Perpetual |
| Market Category | Linear |
| Data Type | Order book L2 |
| Format ID | `BITGET-B1` |
| Fixture | `BTCUSDT-BTCUSDT_3_20260901.zip` |

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
BTCUSDT-BTCUSDT_3_20260901.zip
└── 20260901_4.xlsx
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
| Timestamp | `timestamp` |
| Representation / Unit | Unix epoch seconds |
| Precision | Second |
| Raw Row Ordering | Not chronological |
| Archive Day Boundary | UTC+8 |

Observed timestamp range:

```text
2026-08-31 16:00:23 UTC
        ↓
2026-09-01 15:57:45 UTC
```

### Semantics

| Field | Value |
|---|---|
| Ask Price | Level field 1 |
| Ask Quantity | Level field 2 |
| Bid Price | Level field 1 |
| Bid Quantity | Level field 2 |
| Ask Ordering | Ascending price |
| Bid Ordering | Descending price |

### Order Book

| Field | Value |
|---|---|
| Event Model | Standalone full snapshots |
| Snapshot Depth | 500 bids + 500 asks |
| Delta Updates | Not present |
| Sequence ID | Not present |
| Update ID | Not present |
| Reconstruction | Not required |
| Sorting Required | Yes, by timestamp |

### Example

```text
timestamp = 1788194305

best ask = [78592.1, 8.2036]
best bid = [78592.0, 0.14]

asks = 500 levels
bids = 500 levels
```

### Notes

- 4,294 snapshot rows observed.
- 4,293 unique timestamps.
- One timestamp occurs twice.
- Maximum rows for one timestamp: 2.
- No adjacent repeated timestamps in raw workbook order.
- 2,104 adjacent timestamp reversals observed.
- Raw workbook is not chronologically ordered.
- Every snapshot contains exactly 500 asks and 500 bids.
- No empty books observed.
- Every observed level contains exactly 2 fields.
- Every ask book is price-ascending.
- Every bid book is price-descending.
- No delta/action field observed.
- No sequence or update identifier observed.
- Quantity is provided directly at each price level.

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
sort by timestamp
 ↓
handle duplicate timestamps
 ↓
emit normalized L2 snapshots
```

Rows are independent snapshots. Do not apply one row as a delta to another.

Raw workbook row order must not be trusted.

### Compatibility

Compatible with Bitget Inverse Perpetual L2:

```text
Bitget Linear L2   ─┐
                    ├── BITGET-B1
Bitget Inverse L2  ─┘
```

Observed depth differs:

```text
Linear:
500 asks × 500 bids

Inverse fixture:
asks 98–132
bids 168–196
```

The parser remains identical.

**Parser:** `BITGET-B1`  
**Ready for normalization:** Yes