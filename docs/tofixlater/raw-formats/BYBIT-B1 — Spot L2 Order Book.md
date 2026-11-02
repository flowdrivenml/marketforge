### Identity

| Field | Value |
|---|---|
| Exchange | Bybit |
| Instrument Type | Spot |
| Market Category | Spot |
| Data Type | Order book L2 |
| Format ID | `BYBIT-B1` |
| Fixture | `2026-09-01_BTCUSDT_ob200.data.zip` |

### Physical Format

| Field           | Value               |
| --------------- | ------------------- |
| Archive         | ZIP / deflate       |
| Internal Format | JSON Lines          |
| Granularity     | Daily               |
| Depth           | 200 levels per side |
|                 |                     |

### Schema

| Field      | Type    | Meaning                            |
| ---------- | ------- | ---------------------------------- |
| `topic`    | string  | Order-book topic                   |
| `ts`       | integer | System timestamp, Unix ms          |
| `type`     | string  | `snapshot` / `delta`               |
| `data.s`   | string  | Symbol                             |
| `data.b`   | array   | Bid `[price, quantity]` levels     |
| `data.a`   | array   | Ask `[price, quantity]` levels     |
| `data.u`   | integer | Update ID                          |
| `data.seq` | integer | Cross sequence                     |
| `cts`      | integer | Matching-engine timestamp, Unix ms |

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
{"topic":"orderbook.200.BTCUSDT","ts":1788220802816,"type":"delta","data":{"s":"BTCUSDT","b":[["78579.2","0"]],"a":[["78595.8","0"]],"u":79252481,"seq":113444646693},"cts":1788220802812}
```

### Notes

- 825,283 events observed.
- 825,281 deltas.
- 2 snapshots.
- First event is a 200×200 snapshot.
- Final event is a 200×200 snapshot.
- No update-ID gaps observed.
- One repeated update ID occurs at the final snapshot.

### Compatibility

Not determined yet.

**Parser:** `BYBIT-B1`  
**Ready for normalization:** Yes