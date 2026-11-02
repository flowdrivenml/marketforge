### Identity

| Field | Value |
|---|---|
| Exchange | OKX |
| Instrument Type | Option |
| Market Category | Option |
| Data Type | Order book L2 |
| Format ID | `OKX-B2` |
| Fixture | `BTC-USD-optionchain-L2orderbook-5000lv-2026-09-01.tar.gz` |

### Physical Format

| Field | Value |
|---|---|
| Archive | TAR.GZ |
| Internal Format | JSON Lines |
| Archive Members | 802 option contracts |
| Granularity | Daily option chain |
| Maximum Depth | 5000 levels per side |

### Schema

| Field | Type | Meaning |
|---|---|---|
| `instId` | string | Option instrument ID |
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
| Member Coverage | Depends on option lifetime |

### Semantics

| Field | Value |
|---|---|
| Price | Level field 1 |
| Quantity | Level field 2 |
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
| Maximum Depth | 5000 levels per side |
| Actual Depth | May be much smaller for sparse option books |
| Delta Insert | New price + non-zero quantity |
| Delta Update | Existing price + non-zero quantity |
| Delta Delete | Quantity `0` |
| Sequence ID | Not present |
| Reconstruction | Independently per option contract |

### Example

```json
{
  "instId": "BTC-USD-260901-70000-C",
  "action": "snapshot",
  "ts": "1788220800008",
  "asks": [["...", "...", "..."]],
  "bids": [["...", "...", "..."]]
}
```

### Notes

- Daily option-chain archive contains 802 contract members.
- Each member represents one option instrument.
- Calls and puts are stored as separate members.
- Sampled member: `BTC-USD-260901-70000-C`.
- Sampled member contained 2,261 events:
  - 33 snapshots.
  - 2,228 updates.
- Every observed level in the sampled member contains exactly 3 fields.
- Sample snapshots are sparse rather than fixed at 5000 populated levels.
- Example initial snapshot contained 4 bids and 2 asks.
- Sample member ended before the end of the UTC day because its instrument lifetime ended during the archive period.
- Event schema matches `OKX-B1`.
- Archive packaging differs substantially from `OKX-B1`.

### Compatibility

Event representation is compatible with `OKX-B1`:

```text
instId
action
ts
asks
bids

level = [price, quantity, order_count]
```

Container structure differs:

```text
OKX-B1
TAR.GZ
└── one instrument stream

OKX-B2
TAR.GZ
├── option stream 1
├── option stream 2
├── ...
└── option stream 802
```

**Parser:** `OKX-B2`  
**Ready for normalization:** Yes