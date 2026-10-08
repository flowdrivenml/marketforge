**Observation:** Bitget distributes historical trade data through numbered archive parts rather than necessarily providing one complete file per day.

Example archives:

```text
Spot:
USDT-BTCUSDT_20260901_001.zip

Linear perpetual:
BTCUSDT-BTCUSDT_20260901_001.zip
```

The filename contains a date and a numbered part identifier (`001`).

### Verified Archive Contents

Full-archive integration testing produced:

| Market | Archive part | Trades |
|---|---|---:|
| Spot | `20260901_001` | 100,000 |
| Linear perpetual | `20260901_001` | 100,000 |

Both archives were successfully decoded and normalized.

**Important:** These results establish the contents of the tested archive parts, not the complete daily trade counts.

Additional numbered parts may exist for the same date.

### Processing Implications

A single archive part does not necessarily represent the complete requested date.

```text
Bitget historical trades
        ↓
Discover archive parts
        ↓
Download available parts
        ↓
Process each archive
        ↓
Normalize trades
        ↓
Combine into requested dataset
```

Processing only `_001.zip` may produce an incomplete dataset if subsequent parts exist.

### Processing Policy

MarketForge must:

- Treat each numbered archive as an independent source file.
- Preserve all discovered parts belonging to the requested interval.
- Process every selected part.
- Combine records across parts during dataset construction.
- Avoid assuming that archive boundaries correspond to complete trading days.

### Current Verification

The Rust worker successfully normalized all 100,000 records from each tested Bitget archive.

However, the integration test selected only one archive per market configuration.

Therefore, **complete daily coverage has not yet been verified**.

### Decision

Preserve Bitget's existing partitioned acquisition structure.

The processing executor must process all archive parts referenced by the processing job.

Completeness depends on the acquisition layer discovering all available parts and the processing layer consuming every selected source.

No archive-format redesign is required.