### Identity

| Field | Value |
|---|---|
| Exchange | Bitget |
| Instrument Type | Spot |
| Market Category | Spot |
| Data Type | Order book L2 |
| Format ID | `BITGET-B1` |
| Fixture | `USDT-BTCUSDT_1_20260901.zip` |

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
USDT-BTCUSDT_1_20260901.zip
└── 20260901_6.xlsx
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

Observed range:

```text
2026-08-31 16:00:23 UTC
        ↓
2026-09-01 15:58:23 UTC
```

### Semantics

| Field | Value |
|---|---|
| Ask Price | Level field 1 |
| Ask Quantity | Level field 2 |
| Bid Price | Level field 1 |
| Bid Quantity | Level field 2 |
| Quantity Unit | Base asset |
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
timestamp = 1788220545

best ask = [78558.12, 0.829291]
best bid = [78558.11, 1.764908]

asks = 500 levels
bids = 500 levels
```

### Notes

- 4,297 snapshot rows observed.
- 4,296 unique timestamps.
- One timestamp occurs twice.
- Maximum rows per timestamp: 2.
- 2,116 adjacent timestamp reversals observed.
- Raw workbook is not chronologically ordered.
- Every snapshot contains exactly 500 asks and 500 bids.
- No empty books observed.
- Every level contains exactly 2 fields.
- Every ask book is price-ascending.
- Every bid book is price-descending.
- No delta/action field observed.
- No sequence or update identifier observed.

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

Each row is an independent snapshot. Do not apply rows as deltas.

### Compatibility

Compatible with the other inspected Bitget L2 datasets:

```text
Spot L2                ─┐
Linear Perpetual L2    ─┼── BITGET-B1
Inverse Perpetual L2   ─┘
```

Observed depth differs:

```text
Spot:
500 asks × 500 bids

Linear Perpetual:
500 asks × 500 bids

Inverse Perpetual fixture:
asks 98–132
bids 168–196
```

**Parser:** `BITGET-B1`  
**Ready for normalization:** Yes