## Quick Navigation

- [Overview](#overview)
- [Spot Trades](#spot-trades)
- [Linear Perpetual Trades](#linear-perpetual-trades)
- [Inverse Perpetual Trades](#inverse-perpetual-trades)
- [Spot L2 Updates](#spot-l2-updates)
- [Linear Perpetual L2 Updates](#linear-perpetual-l2-updates)
- [Inverse Perpetual L2 Updates](#inverse-perpetual-l2-updates)
- [Spot L2 Bootstrap Snapshot](#spot-l2-bootstrap-snapshot)
- [Linear Perpetual L2 Bootstrap Snapshot](#linear-perpetual-l2-bootstrap-snapshot)
- [Inverse Perpetual L2 Bootstrap Snapshot](#inverse-perpetual-l2-bootstrap-snapshot)
- [Sequence Models](#sequence-models)
- [Bootstrap Model](#bootstrap-model)
- [Provisional Format IDs](#provisional-format-ids)

## Overview

Live Binance data was inspected for:

| Market | Trades | L2 Stream | REST Snapshot |
|---|---:|---:|---:|
| Spot | Yes | Yes | Yes |
| Linear Perpetual | Yes | Yes | Yes |
| Inverse Perpetual | Yes | Yes | Yes |

The live architecture uses two source types:

```text
WebSocket
    → trades
    → incremental L2 updates

REST
    → L2 bootstrap snapshot
```

Live WebSocket and REST formats are independent of Binance historical archive formats.

---

## Spot Trades

### Connection

```text
wss://stream.binance.com:9443/ws/btcusdt@trade
```

### Physical Format

```text
Transport: WebSocket
Encoding: JSON
Event type: trade
```

### Observed Fields

| Field | Type | Meaning |
|---|---|---|
| `e` | string | Event type |
| `E` | integer | Event/system timestamp |
| `s` | string | Symbol |
| `t` | integer | Trade ID |
| `p` | decimal string | Price |
| `q` | decimal string | Base quantity |
| `T` | integer | Trade timestamp |
| `m` | boolean | Buyer is maker |
| `M` | boolean | Additional exchange flag |

Observed trade messages consistently contained these nine fields. :chatgpt-content-reference{index="0"}

### Example

```json
{
  "e": "trade",
  "E": 1790974313247,
  "s": "BTCUSDT",
  "t": 6733202641,
  "p": "84453.72000000",
  "q": "0.00035000",
  "T": 1790974313247,
  "m": false,
  "M": true
}
```

### Semantics

```text
event timestamp  = T
system timestamp = E
trade ID         = t
price            = p
quantity         = q
side source      = m
symbol           = s
```

Aggressor-side mapping:

```text
m = false → buy
m = true  → sell
```

`q` is base-asset quantity for Spot.

---

## Linear Perpetual Trades

### Connection

```text
wss://fstream.binance.com/ws/btcusdt@trade
```

### Physical Format

```text
Transport: WebSocket
Encoding: JSON
Event type: trade
```

### Observed Fields

| Field | Type |
|---|---|
| `e` | string |
| `E` | integer |
| `T` | integer |
| `s` | string |
| `t` | integer |
| `p` | decimal string |
| `q` | decimal string |
| `m` | boolean |
| `X` | string |
| `st` | integer |

The observed Linear trade schema contains ten fields. :chatgpt-content-reference{index="1"}

### Example

```json
{
  "e": "trade",
  "E": 1790974313662,
  "T": 1790974313661,
  "s": "BTCUSDT",
  "t": 8142894255,
  "p": "84424.40",
  "q": "0.001",
  "m": false,
  "X": "MARKET",
  "st": 1
}
```

### Semantics

```text
event timestamp  = T
system timestamp = E
trade ID         = t
price            = p
quantity         = q
side source      = m
symbol           = s
```

Quantity interpretation must use `instrument_specs`.

---

## Inverse Perpetual Trades

### Connection

```text
wss://dstream.binance.com/ws/btcusd_perp@trade
```

### Physical Format

The observed Inverse trade structure matches Linear Perpetual.

### Observed Fields

```text
E
T
X
e
m
p
q
s
st
t
```

:chatgpt-content-reference{index="2"}

### Example

```json
{
  "e": "trade",
  "E": 1790974313636,
  "T": 1790974313636,
  "s": "BTCUSD_PERP",
  "t": 1157063491,
  "p": "84392.1",
  "q": "38",
  "m": true,
  "X": "MARKET",
  "st": 2
}
```

### Semantics

```text
event timestamp  = T
system timestamp = E
trade ID         = t
price            = p
quantity         = q
side source      = m
symbol           = s
```

`q` represents derivative quantity and must be interpreted using the instrument catalog rather than assumed to be base quantity.

Linear and Inverse trades therefore appear compatible with one physical parser.

---

## Spot L2 Updates

### Connection

```text
wss://stream.binance.com:9443/ws/btcusdt@depth@100ms
```

### Observed Fields

| Field | Meaning |
|---|---|
| `e` | Event type |
| `E` | Event/system timestamp |
| `s` | Symbol |
| `U` | First update ID represented |
| `u` | Final update ID represented |
| `b` | Bid level updates |
| `a` | Ask level updates |

No `T` or `pu` field was observed in the Spot depth messages. :chatgpt-content-reference{index="3"}

### Level Format

```text
[price, quantity]
```

Example:

```json
["84453.72000000", "0.03303000"]
```

A zero quantity was observed in both bid and ask updates:

```text
quantity > 0 → resulting level quantity
quantity = 0 → remove level
```

### Sequence

Observed:

```text
U = first update ID
u = final update ID
```

Five consecutive captured events showed:

```text
U=100998300057  u=100998300070
U=100998300071  u=100998300087
U=100998300088  u=100998300094
U=100998300095  u=100998300105
U=100998300106  u=100998300123
```

The observed continuity relation is:

```text
next.U = previous.u + 1
```

:chatgpt-content-reference{index="4"}

---

## Linear Perpetual L2 Updates

### Connection

```text
wss://fstream.binance.com/ws/btcusdt@depth@100ms
```

### Observed Fields

```text
e
E
T
s
ps
st
U
u
pu
b
a
```

:chatgpt-content-reference{index="5"}

### Time

```text
T = event/matching timestamp
E = system/event publication timestamp
```

Both are millisecond Unix timestamps in the captured messages.

### Levels

```text
b = bid updates
a = ask updates

level = [price, quantity]
```

Zero quantity removes the level.

### Sequence

```text
U  = first update ID represented
u  = final update ID represented
pu = previous event final update ID
```

Observed continuity:

```text
current.pu = previous.u
```

Example:

```text
event 1:
u  = 11722246582687

event 2:
pu = 11722246582687
u  = 11722246589852

event 3:
pu = 11722246589852
```

The relation held across the captured sample. :chatgpt-content-reference{index="6"}

---

## Inverse Perpetual L2 Updates

### Connection

```text
wss://dstream.binance.com/ws/btcusd_perp@depth@100ms
```

### Observed Fields

```text
e
E
T
s
ps
st
U
u
pu
b
a
```

The observed structure matches Linear Perpetual depth. :chatgpt-content-reference{index="7"}

### Levels

```text
[price, quantity]
```

The quantity is derivative contract quantity and must be interpreted through `instrument_specs`.

### Sequence

The same relationship observed for Linear also holds for Inverse:

```text
current.pu = previous.u
```

:chatgpt-content-reference{index="8"}

Linear and Inverse L2 streams therefore appear compatible with one physical parser.

---

## Spot L2 Bootstrap Snapshot

### Endpoint

```text
GET https://api.binance.com/api/v3/depth
```

Parameters:

```text
symbol=BTCUSDT
limit=1000
```

### Observed Fields

```text
lastUpdateId
bids
asks
```

:chatgpt-content-reference{index="9"}

### Levels

```text
[price, quantity]
```

Bids are returned highest-price first and asks lowest-price first in the captured snapshot.

The REST response contains no observed exchange timestamp.

---

## Linear Perpetual L2 Bootstrap Snapshot

### Endpoint

```text
GET https://fapi.binance.com/fapi/v1/depth
```

Parameters:

```text
symbol=BTCUSDT
limit=1000
```

### Observed Fields

```text
lastUpdateId
E
T
bids
asks
```

:chatgpt-content-reference{index="10"}

### Levels

```text
[price, quantity]
```

The snapshot therefore provides both sequence identity and exchange timestamps.

---

## Inverse Perpetual L2 Bootstrap Snapshot

### Endpoint

```text
GET https://dapi.binance.com/dapi/v1/depth
```

Parameters:

```text
symbol=BTCUSD_PERP
limit=1000
```

### Observed Fields

```text
lastUpdateId
E
T
symbol
pair
bids
asks
```

:chatgpt-content-reference{index="11"}

### Levels

```text
[price, quantity]
```

The Inverse snapshot additionally carries native symbol and pair identity.

---

## Sequence Models

Binance exposes two observed L2 sequence models.

### Spot

```text
U = first update ID
u = final update ID
```

Observed stream continuity:

```text
next.U = previous.u + 1
```

### Linear / Inverse

```text
U  = first update ID
u  = final update ID
pu = previous final update ID
```

Observed stream continuity:

```text
current.pu = previous.u
```

This suggests the canonical L2 sequence model should preserve sequence ranges rather than only a sequence start/count abstraction.

A suitable canonical representation is:

```text
sequence_first
sequence_last
sequence_previous
```

with nullable fields where unavailable.

Mapping:

```text
Spot:
sequence_first    = U
sequence_last     = u
sequence_previous = null

Linear / Inverse:
sequence_first    = U
sequence_last     = u
sequence_previous = pu
```

---

## Bootstrap Model

Binance L2 requires combining:

```text
WebSocket incremental updates
+
REST order-book snapshot
```

The intended bootstrap architecture is:

```text
Open WebSocket
    ↓
Buffer updates
    ↓
Request REST snapshot
    ↓
Read snapshot.lastUpdateId
    ↓
Discard obsolete buffered updates
    ↓
Locate first compatible update
    ↓
Replay buffered updates
    ↓
Validate sequence continuity
    ↓
Enter LIVE state
```

The initial inspection captured REST snapshots and WebSocket streams independently. Their update IDs therefore occurred at different points in time and cannot by themselves establish snapshot/update reconciliation behavior. :chatgpt-content-reference{index="12"}

Bootstrap fixtures must therefore capture the REST snapshot while the WebSocket stream remains active and buffered.

---

## Live Timestamp Model

The observed Binance streams expose multiple useful clocks.

### Trades

```text
T → trade/event timestamp
E → exchange event/system timestamp
```

### Spot L2

```text
E → exchange event/system timestamp
```

No separate `T` was observed.

### Linear / Inverse L2

```text
T → event/matching timestamp
E → exchange event/system timestamp
```

MarketForge should additionally generate:

```text
arrival_timestamp_ns
```

at ingestion time.

This is a MarketForge runtime timestamp and is not part of the Binance wire format.

---

## Provisional Format IDs

Based on the observed physical schemas:

| Format | Transport | Role | Market |
|---|---|---|---|
| `BINANCE-LIVE-T1` | WebSocket | Trade stream | Spot |
| `BINANCE-LIVE-T2` | WebSocket | Trade stream | Linear + Inverse |
| `BINANCE-LIVE-B1` | WebSocket | L2 update stream | Spot |
| `BINANCE-LIVE-B2` | WebSocket | L2 update stream | Linear + Inverse |
| `BINANCE-LIVE-B3` | REST | L2 bootstrap snapshot | Spot |
| `BINANCE-LIVE-B4` | REST | L2 bootstrap snapshot | Linear |
| `BINANCE-LIVE-B5` | REST | L2 bootstrap snapshot | Inverse |

Parser compatibility may be consolidated further after all live exchanges are inspected.

## Fixtures

Expected fixture layout:

```text
tests/fixtures/live/binance/
├── spot/
│   ├── trades.raw.jsonl
│   ├── book.bootstrap.snapshot.json
│   └── book.bootstrap.updates.jsonl
│
├── linear/
│   ├── trades.raw.jsonl
│   ├── book.bootstrap.snapshot.json
│   └── book.bootstrap.updates.jsonl
│
└── inverse/
    ├── trades.raw.jsonl
    ├── book.bootstrap.snapshot.json
    └── book.bootstrap.updates.jsonl
```

Fixtures preserve raw Binance payloads without field renaming or preprocessing.