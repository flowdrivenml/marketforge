### Identity

| Field | Value |
|---|---|
| Exchange | Bybit |
| Instrument Type | Perpetual |
| Market Category | Inverse |
| Data Type | Order book L2 |
| Format ID | `BYBIT-B1` |
| Fixture | `2026-09-01_BTCUSD_ob200.data.zip` |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | JSON Lines |
| Granularity | Daily |
| Depth | 200 levels per side |

### Schema

| Field | Type | Meaning |
|---|---|---|
| `topic` | string | Order-book topic |
| `ts` | integer | System timestamp, Unix ms |
| `type` | string | `snapshot` / `delta` |
| `data.s` | string | Symbol |
| `data.b` | array | Bid `[price, quantity]` levels |
| `data.a` | array | Ask `[price, quantity]` levels |
| `data.u` | integer | Update ID |
| `data.seq` | integer | Cross sequence |
| `cts` | integer | Matching-engine timestamp, Unix ms |

### Time

| Field | Value |
|---|---|
| System Time | `ts`, Unix ms |
| Matching Time | `cts`, Unix ms |
| Precision | Millisecond |
| Ordering | Chronological |

### Semantics

| Field | Value |
|---|---|
| Price | String decimal |
| Quantity | String decimal |
| Bids | `data.b` |
| Asks | `data.a` |
| Update ID | `data.u`, consecutive |
| Cross Sequence | `data.seq` |

### Order Book

| Field | Value |
|---|---|
| Event Model | Snapshot + incremental delta |
| Snapshot Depth | 200 bids + 200 asks |
| Delta Insert | New price + non-zero quantity |
| Delta Update | Existing price + non-zero quantity |
| Delta Delete | Quantity `0` |
| Reconstruction | Start from snapshot, apply deltas by `u` |
| Sequence Continuity | Consecutive `u` observed |

### Example

```json
{
  "topic": "orderbook.200.BTCUSD",
  "ts": 1788220801464,
  "type": "snapshot",
  "data": {
    "s": "BTCUSD",
    "b": [["...", "..."]],
    "a": [["...", "..."]],
    "u": 32653621,
    "seq": 117117527769
  },
  "cts": 1788220801355
}
```

### Notes

- 851,828 events observed.
- 851,826 deltas.
- 2 snapshots.
- First and final events are 200 × 200 snapshots.
- First update ID: `32653621`.
- Last update ID: `33505447`.
- No update-ID gaps observed.
- One repeated update ID occurs at the final snapshot.
- Physical schema and reconstruction model match Spot and Linear Perpetual L2.

### Compatibility

Compatible with:

- Bybit Spot L2
- Bybit Linear Perpetual L2

All use `BYBIT-B1`.

**Parser:** `BYBIT-B1`  
**Ready for normalization:** Yes