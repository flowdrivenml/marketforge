### Identity

| Field           | Value                                           |
| --------------- | ----------------------------------------------- |
| Exchange        | OKX                                             |
| Instrument Type | Spot                                            |
| Market Category | Spot                                            |
| Data Type       | Order book L2                                   |
| Format ID       | `OKX-B1`                                        |
| Fixture         | `BTC-USDT-L2orderbook-5000lv-2026-09-01.tar.gz` |

### Physical Format

| Field | Value |
|---|---|
| Archive | TAR.GZ |
| Internal Format | JSON Lines |
| Members | 1 |
| Granularity | Daily, UTC |
| Maximum Depth | 5000 levels per side |

### Schema

| Field | Type | Meaning |
|---|---|---|
| `instId` | string | Instrument ID |
| `action` | string | `snapshot` / `update` |
| `ts` | string/integer | Unix timestamp in milliseconds |
| `asks` | array | Ask price levels |
| `bids` | array | Bid price levels |

Each price level:

```text
[price, quantity, order_count]
```

### Time

| Field | Value |
|---|---|
| Timestamp | `ts` |
| Representation / Unit | Unix epoch milliseconds |
| Precision | Millisecond |
| Ordering | Chronological |
| Archive Day Boundary | UTC |

Observed coverage:

```text
2026-09-01 00:00:00 UTC
        ↓
2026-09-01 23:59:59 UTC
```

### Semantics

| Field | Value |
|---|---|
| Price | Level field 1 |
| Quantity | Level field 2, base currency for Spot |
| Order Count | Level field 3 |
| Asks | `asks` |
| Bids | `bids` |

A zero quantity/count update removes the price level:

```text
["77437.8", "0", "0"]
```

### Order Book

| Field | Value |
|---|---|
| Event Model | Periodic snapshot + incremental updates |
| Snapshot Depth | 5000 bids + 5000 asks |
| Snapshot Frequency | ~15 minutes |
| Update Frequency | ~1 second |
| Delta Insert | New price + non-zero quantity |
| Delta Update | Existing price + non-zero quantity |
| Delta Delete | Quantity `0` |
| Sequence ID | Not present |
| Reconstruction | Reset from snapshot, apply subsequent updates |

### Example

```json
{
  "instId": "BTC-USDT",
  "action": "update",
  "ts": "1788307195007",
  "asks": [
    ["77437.7", "0.00213435", "2"],
    ["77437.8", "0", "0"]
  ],
  "bids": [
    ["77430.5", "0.00205714", "2"]
  ]
}
```

### Notes

- 86,398 events observed.
- 86,302 updates.
- 96 snapshots.
- Every snapshot contains exactly 5000 bids and 5000 asks.
- Snapshots occur approximately every 15 minutes.
- Every observed price level contains exactly 3 fields.
- No out-of-order timestamps.
- No repeated timestamps.
- No sequence ID or update ID exists in this historical format.
- Top-level fields are only `instId`, `action`, `ts`, `asks`, and `bids`.
- Unlike OKX trade archives, this L2 archive uses a UTC calendar day.

### Compatibility

Not determined yet.

**Parser:** `OKX-B1`  
**Ready for normalization:** Yes