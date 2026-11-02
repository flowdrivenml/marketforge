### Identity

| Field | Value |
|---|---|
| Exchange | Bybit |
| Instrument Type | Option |
| Market Category | Option |
| Data Type | Order book L2 |
| Format ID | `BYBIT-B2` |
| Fixture | `2026-09-01_BTC_USDT.ob25.zip` |

### Physical Format

| Field | Value |
|---|---|
| Archive | ZIP / deflate |
| Internal Format | JSON Lines |
| Granularity | Daily |
| Archive Members | 710 option contracts |
| Maximum Depth | 25 levels per side |

### Schema

| Field | Type | Meaning |
|---|---|---|
| `topic` | string | Order-book topic |
| `ts` | integer | System timestamp, Unix ms |
| `type` | string | `snapshot` / `delta` |
| `id` | string | Unique event identifier |
| `data.s` | string | Option instrument |
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
| Event ID | `id`, unique |

### Order Book

| Field | Value |
|---|---|
| Event Model | Snapshot + incremental delta |
| Maximum Depth | 25 levels per side |
| Snapshot Size | Up to 25 populated levels per side |
| Delta Insert | New price + non-zero quantity |
| Delta Update | Existing price + non-zero quantity |
| Delta Delete | Quantity `0` |
| Reconstruction | Per-contract snapshot followed by deltas |
| Sequence Continuity | Consecutive `u` observed |

### Example

```json
{
  "topic": "orderbook.25.BTC-11SEP26-58000-C-USDT",
  "ts": 1788220800635,
  "type": "snapshot",
  "id": "orderbook.25.BTC-11SEP26-58000-C-USDT-74394559710-1788220800634",
  "data": {
    "s": "BTC-11SEP26-58000-C-USDT",
    "b": [["20580", "2"]],
    "a": [["20740", "2"]],
    "u": 179100,
    "seq": 74394559710
  },
  "cts": 1788220339208
}
```

### Notes

- Daily archive contains 710 independent option-contract files.
- 355 Call and 355 Put members.
- No empty members observed.
- Each sampled contract contained 2 snapshots plus incremental deltas.
- Representative contract contained 29,032 events.
- First update ID: `179100`.
- Last update ID: `208130`.
- No update-ID gaps observed.
- One repeated update ID occurs at the final snapshot.
- All event IDs present and unique.
- Representative snapshot contained 3 bids and 2 asks; depth 25 is therefore a maximum, not a requirement for 25 populated levels.
- Event model closely resembles `BYBIT-B1`.
- Packaging, depth, and top-level `id` differ from `BYBIT-B1`.

### Compatibility

Conceptually compatible with `BYBIT-B1` reconstruction semantics, but physically distinct:

```text
BYBIT-B1
daily ZIP
└── one market
    └── orderbook.200 JSONL

BYBIT-B2
daily ZIP
├── option contract 1
├── option contract 2
├── ...
└── option contract 710
    └── orderbook.25 JSONL
```

**Parser:** `BYBIT-B2`  
**Ready for normalization:** Yes