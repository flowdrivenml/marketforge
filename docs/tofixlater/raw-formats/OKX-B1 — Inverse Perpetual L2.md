### Identity

| Field | Value |
|---|---|
| Exchange | OKX |
| Instrument Type | Perpetual |
| Market Category | Inverse |
| Data Type | Order book L2 |
| Format ID | `OKX-B1` |
| Fixture | `BTC-USD-SWAP-L2orderbook-5000lv-2026-09-01.tar.gz` |

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
| Quantity | Level field 2, contract quantity |
| Order Count | Level field 3 |
| Asks | `asks` |
| Bids | `bids` |

Zero quantity removes a price level:

```text
[price, "0", "0"]
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
  "instId": "BTC-USD-SWAP",
  "action": "snapshot",
  "ts": "1788220800001",
  "asks": [["...", "...", "..."]],
  "bids": [["...", "...", "..."]]
}
```

### Notes

- 86,400 events observed.
- 86,304 updates.
- 96 snapshots.
- Every snapshot contains exactly 5000 bids and 5000 asks.
- Every observed price level contains exactly 3 fields.
- 11,662,070 individual price-level records observed.
- No out-of-order timestamps.
- No repeated timestamps.
- No sequence or update ID exists in this historical format.
- Archive covers one UTC calendar day.
- Physical schema and reconstruction model match OKX Spot and Linear Perpetual L2.
- Quantity represents derivative contract quantity.

### Compatibility

Compatible with:

- OKX Spot L2
- OKX Linear Perpetual L2

All use `OKX-B1`.

Quantity interpretation differs by instrument type:

```text
Spot
quantity → base-asset quantity

Linear Perpetual
quantity → contract quantity

Inverse Perpetual
quantity → contract quantity
```

Contract-to-base/quote conversion belongs in normalization and requires instrument metadata.

**Parser:** `OKX-B1`  
**Ready for normalization:** Yes