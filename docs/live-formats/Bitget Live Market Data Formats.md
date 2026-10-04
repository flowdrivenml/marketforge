## Quick Navigation

- [Overview](#overview)
- [Transport and Subscription](#transport-and-subscription)
- [Trade Stream](#trade-stream)
- [Spot Trades](#spot-trades)
- [USDT Linear Trades](#usdt-linear-trades)
- [USDC Linear Trades](#usdc-linear-trades)
- [WebSocket L2 Book Stream](#websocket-l2-book-stream)
- [Spot WebSocket L2](#spot-websocket-l2)
- [USDT Linear WebSocket L2](#usdt-linear-websocket-l2)
- [USDC Linear WebSocket L2](#usdc-linear-websocket-l2)
- [L2 Update Semantics](#l2-update-semantics)
- [Sequence Model](#sequence-model)
- [WebSocket Bootstrap](#websocket-bootstrap)
- [REST L2 Snapshots](#rest-l2-snapshots)
- [L2 Acquisition Modes](#l2-acquisition-modes)
- [Gap Recovery](#gap-recovery)
- [Timestamp Model](#timestamp-model)
- [Provisional Format IDs](#provisional-format-ids)
- [Fixtures](#fixtures)

## Overview

Bitget live market data was inspected for:

| Market | Trades | WebSocket L2 | REST L2 |
|---|---:|---:|---:|
| Spot | Yes | Yes | Yes |
| USDT Linear Futures | Yes | Yes | Yes |
| USDC Linear Futures | Yes | Yes | Yes |

Bitget exposes two distinct ways to obtain live L2 state:

```text
WebSocket
    → initial snapshot
    → incremental updates
    → continuously maintained local book
```

and:

```text
REST
    → current book snapshot
    → stateless point-in-time acquisition
```

These are separate acquisition capabilities.

REST snapshots are not required for normal WebSocket bootstrap because the `books` channel supplies its own initial snapshot. They remain useful for periodic snapshot acquisition, validation, recovery, and independent comparison against reconstructed books.

---

## Transport and Subscription

### WebSocket Endpoint

```text
wss://ws.bitget.com/v2/ws/public
```

### Subscription Envelope

```json
{
  "op": "subscribe",
  "args": [
    {
      "instType": "...",
      "channel": "...",
      "instId": "..."
    }
  ]
}
```

Observed instrument types:

```text
SPOT
USDT-FUTURES
USDC-FUTURES
```

Observed channels:

```text
trade
books
```

Successful subscriptions return an acknowledgement containing:

```text
event
arg
```

---

## Trade Stream

The observed Spot, USDT Linear, and USDC Linear trade streams share the same physical message structure.

### Envelope

```text
action
arg
data
ts
```

`data` is an array containing one or more trades.

### Trade Record

| Field | Type | Meaning |
|---|---|---|
| `ts` | integer string | Trade timestamp |
| `price` | decimal string | Execution price |
| `size` | decimal string | Exchange-native quantity |
| `side` | string | Aggressor side |
| `tradeId` | integer string | Trade identifier |

Observed side values:

```text
buy
sell
```

These map directly to canonical aggressor side.

### Initial Trade Snapshot

The first trade message after subscription may use:

```text
action = snapshot
```

and contain multiple recent trades rather than only newly arriving executions.

The initial trade snapshot must therefore be distinguished from subsequent live updates.

For continuous live acquisition, replaying the initial trade snapshot after every reconnect could introduce duplicate trades. The live engine must define explicit handling for bootstrap trade history.

---

## Spot Trades

### Subscription

```json
{
  "op": "subscribe",
  "args": [
    {
      "instType": "SPOT",
      "channel": "trade",
      "instId": "BTCUSDT"
    }
  ]
}
```

### Schema

```text
action
arg
data[]
    ts
    price
    size
    side
    tradeId
ts
```

### Quantity

For Spot:

```text
size → base quantity
```

subject to instrument metadata validation during normalization.

---

## USDT Linear Trades

### Subscription

```json
{
  "op": "subscribe",
  "args": [
    {
      "instType": "USDT-FUTURES",
      "channel": "trade",
      "instId": "BTCUSDT"
    }
  ]
}
```

### Schema

The physical trade schema matches Spot:

```text
ts
price
size
side
tradeId
```

### Quantity

`size` is exchange-native derivative quantity.

Its canonical interpretation must use the instrument specification rather than assuming Spot quantity semantics.

Normalization may derive:

```text
quantity_base
quantity_quote
quantity_contracts
```

from the native quantity and instrument metadata.

---

## USDC Linear Trades

### Subscription

```json
{
  "op": "subscribe",
  "args": [
    {
      "instType": "USDC-FUTURES",
      "channel": "trade",
      "instId": "BTCPERP"
    }
  ]
}
```

### Schema

USDC Linear uses the same observed physical trade structure:

```text
ts
price
size
side
tradeId
```

The same physical parser therefore appears suitable for Spot, USDT Linear, and USDC Linear trades.

Quantity semantics remain instrument-dependent.

---

## WebSocket L2 Book Stream

All inspected markets use:

```text
channel = books
```

The first book message is:

```text
action = snapshot
```

Subsequent messages are:

```text
action = update
```

### Envelope

```text
action
arg
data
ts
```

### Book Payload

Each `data` item contains:

| Field | Meaning |
|---|---|
| `bids` | Bid levels |
| `asks` | Ask levels |
| `pseq` | Previous sequence identifier |
| `seq` | Current sequence identifier |
| `ts` | Book timestamp |

### Price Level

Observed level representation:

```text
[price, quantity]
```

Both values are decimal strings.

No order count was observed.

---

## Spot WebSocket L2

### Subscription

```json
{
  "op": "subscribe",
  "args": [
    {
      "instType": "SPOT",
      "channel": "books",
      "instId": "BTCUSDT"
    }
  ]
}
```

### Initial Snapshot

Observed:

```text
action = snapshot
pseq   = 0
seq    = 875539111963

bids = 500
asks = 500
```

### Updates

Observed sequence:

```text
snapshot
pseq = 0
seq  = 875539111963

update
pseq = 875539111963
seq  = 875539125397

update
pseq = 875539125397
seq  = 875539129297

update
pseq = 875539129297
seq  = 875539133712
```

---

## USDT Linear WebSocket L2

### Subscription

```json
{
  "op": "subscribe",
  "args": [
    {
      "instType": "USDT-FUTURES",
      "channel": "books",
      "instId": "BTCUSDT"
    }
  ]
}
```

### Initial Snapshot

Observed:

```text
action = snapshot
pseq   = 0
seq    = 1052002771011

bids = 500
asks = 500
```

### Updates

Observed:

```text
snapshot
pseq = 0
seq  = 1052002771011

update
pseq = 1052002771011
seq  = 1052002775613

update
pseq = 1052002775613
seq  = 1052002779763

update
pseq = 1052002779763
seq  = 1052002784659
```

---

## USDC Linear WebSocket L2

### Subscription

```json
{
  "op": "subscribe",
  "args": [
    {
      "instType": "USDC-FUTURES",
      "channel": "books",
      "instId": "BTCPERP"
    }
  ]
}
```

### Initial Snapshot

Observed:

```text
action = snapshot
pseq   = 0
seq    = 588086662559

bids = 200
asks = 200
```

### Updates

Observed:

```text
snapshot
pseq = 0
seq  = 588086662559

update
pseq = 588086662559
seq  = 588086663692

update
pseq = 588086663692
seq  = 588086664831

update
pseq = 588086664831
seq  = 588086665850
```

---

## L2 Update Semantics

Bitget book updates provide resulting quantities for individual price levels.

```text
[price, quantity]
```

Canonical interpretation:

```text
quantity > 0
    → set price level to supplied quantity

quantity = 0
    → delete price level
```

The quantity is not interpreted as a delta.

For example:

```text
["84490.0", "2.5"]
```

means:

```text
set level 84490.0 to quantity 2.5
```

not:

```text
add 2.5 to the existing quantity
```

Canonical actions therefore remain:

```text
set
delete
```

No comparison against the previous quantity is required to derive the action.

---

## Sequence Model

Bitget exposes:

```text
pseq = previous sequence
seq  = current sequence
```

### Snapshot

Observed snapshots use:

```text
pseq = 0
seq  = initial sequence
```

`pseq = 0` acts as the bootstrap sentinel.

### Updates

Across Spot, USDT Linear, and USDC Linear:

```text
current.pseq = previous.seq
```

Observed model:

```text
snapshot:
    pseq = 0
    seq  = A

update:
    pseq = A
    seq  = B

update:
    pseq = B
    seq  = C

update:
    pseq = C
    seq  = D
```

This provides direct sequence-gap detection.

### Canonical Mapping

Using:

```text
sequence_first
sequence_last
sequence_previous
```

the mapping is:

```text
snapshot:

sequence_first    = seq
sequence_last     = seq
sequence_previous = null
```

The wire value:

```text
pseq = 0
```

is treated as a bootstrap sentinel rather than a real previous sequence.

Updates map as:

```text
sequence_first    = seq
sequence_last     = seq
sequence_previous = pseq
```

Continuity validation becomes:

```text
current.sequence_previous
    ==
previous.sequence_last
```

---

## WebSocket Bootstrap

Bitget provides the initial L2 snapshot directly through the `books` WebSocket channel.

Normal bootstrap is therefore:

```text
CONNECT
    ↓
SUBSCRIBE books
    ↓
WAIT FOR action=snapshot
    ↓
verify pseq=0
    ↓
construct complete local book
    ↓
last_seq = snapshot.seq
    ↓
LIVE
```

Each subsequent update:

```text
receive update
    ↓
verify update.pseq == last_seq
    ↓
apply bids/asks
    ↓
last_seq = update.seq
```

A separate REST request is not required to establish the initial WebSocket book.

---

## REST L2 Snapshots

Bitget also exposes REST endpoints that return current L2 snapshots.

These are treated as independent live data sources rather than only bootstrap helpers.

### Spot

```text
GET https://api.bitget.com/api/v2/spot/market/orderbook
```

Example parameters:

```text
symbol=BTCUSDT
type=step0
limit=150
```

### USDT Linear

```text
GET https://api.bitget.com/api/v2/mix/market/merge-depth
```

Example parameters:

```text
symbol=BTCUSDT
productType=USDT-FUTURES
precision=scale0
limit=max
```

### USDC Linear

```text
GET https://api.bitget.com/api/v2/mix/market/merge-depth
```

Example parameters:

```text
symbol=BTCPERP
productType=USDC-FUTURES
precision=scale0
limit=max
```

The exact REST response schemas and parser compatibility must be finalized from captured REST fixtures.

### Purpose

REST L2 snapshots support several independent use cases:

```text
periodic snapshot acquisition
validation of reconstructed WebSocket books
recovery
diagnostics
independent point-in-time collection
```

They should therefore remain first-class live formats even though WebSocket bootstrap does not require them.

---

## L2 Acquisition Modes

MarketForge should support both continuous reconstruction and direct snapshot acquisition.

### Streaming

```text
WebSocket snapshot
    ↓
incremental updates
    ↓
maintained local book
    ↓
canonical L2 state
```

Suitable for:

```text
continuous microstructure
tick-level book changes
local reconstruction
high-frequency processing
```

### Periodic Snapshot

```text
REST request
    ↓
canonical L2Snapshot
    ↓
wait configured interval
    ↓
REST request
    ↓
canonical L2Snapshot
```

Suitable for:

```text
periodic L2 sampling
lower-frequency datasets
stateless acquisition
users who do not require every book update
```

For example, a user may request:

```text
one L2 snapshot every second
```

without requiring MarketForge to reconstruct and maintain every intervening WebSocket update.

### Reconciliation

REST can additionally validate a locally maintained WebSocket book:

```text
WebSocket updates
    ↓
maintained local book
    │
    │ periodically
    ↓
REST snapshot
    ↓
compare overlapping levels
    ↓
match / mismatch
```

A mismatch can be recorded as a data-integrity event and may trigger rebootstrap according to runtime policy.

---

## Gap Recovery

A WebSocket book is sequence-invalid when:

```text
update.pseq != previous.seq
```

Recovery should not infer missing updates.

```text
sequence mismatch
    ↓
invalidate local book
    ↓
resubscribe / reconnect
    ↓
wait for new action=snapshot
    ↓
rebuild complete book
    ↓
resume updates
```

Because the WebSocket supplies its own snapshot, the normal recovery path can use the same mechanism as initial bootstrap.

REST snapshots may additionally be used by higher-level recovery or validation policies but are not required by the basic WebSocket sequence model.

---

## Timestamp Model

### Trades

Individual trades contain:

```text
data[].ts
```

The WebSocket envelope additionally contains:

```text
ts
```

Canonical trade event time should use the individual trade timestamp.

### WebSocket L2

Book payloads contain:

```text
data[].ts
```

and the outer envelope contains:

```text
ts
```

The inner book timestamp should be used as the canonical market-event timestamp where present.

### REST L2

REST snapshot timestamps must be mapped from the actual response fields once the REST formats are finalized.

MarketForge additionally generates:

```text
arrival_timestamp_ns
```

when a WebSocket message or REST response reaches the ingestion engine.

Observed Bitget timestamps are Unix epoch milliseconds and must be converted to canonical nanoseconds.

---

## Provisional Format IDs

### Trades

| Format | Transport | Role | Markets |
|---|---|---|---|
| `BITGET-LIVE-T1` | WebSocket | Trade stream | Spot, USDT Linear, USDC Linear |

### WebSocket Books

| Format | Transport | Role | Markets |
|---|---|---|---|
| `BITGET-LIVE-B1` | WebSocket | L2 snapshot + updates | Spot, USDT Linear, USDC Linear |

### REST Books

| Format | Transport | Role | Markets |
|---|---|---|---|
| `BITGET-LIVE-B2` | REST | L2 snapshot | Spot |
| `BITGET-LIVE-B3` | REST | L2 snapshot | USDT Linear, USDC Linear |

`BITGET-LIVE-B3` remains provisional until captured USDT and USDC REST responses confirm identical physical schemas.

### Parser Compatibility

Current evidence suggests:

```text
Trade parser:
    Spot
    USDT Linear
    USDC Linear
        → shared

WebSocket book parser:
    Spot
    USDT Linear
    USDC Linear
        → shared

REST book parser:
    Spot
        → separate candidate

    USDT Linear
    USDC Linear
        → shared candidate
```

Instrument-specific quantity semantics remain outside the physical parser and are resolved through MarketForge instrument metadata.

---

## Fixtures

Recommended fixture structure:

```text
tests/fixtures/live/bitget/
├── spot/
│   ├── trades.raw.jsonl
│   ├── books.raw.jsonl
│   └── book.rest.snapshot.json
│
├── linear_usdt/
│   ├── trades.raw.jsonl
│   ├── books.raw.jsonl
│   └── book.rest.snapshot.json
│
└── linear_usdc/
    ├── trades.raw.jsonl
    ├── books.raw.jsonl
    └── book.rest.snapshot.json
```

### WebSocket Book Fixtures

Each `books.raw.jsonl` should preserve:

```text
subscription acknowledgement
initial action=snapshot
multiple contiguous action=update messages
```

The critical sequence pattern is:

```text
snapshot
    pseq = 0
    seq  = A

update
    pseq = A
    seq  = B

update
    pseq = B
    seq  = C
```

### Trade Fixtures

Trade fixtures should preserve:

```text
subscription acknowledgement
initial trade snapshot
subsequent trade updates where available
```

### REST Fixtures

REST fixtures preserve the complete raw response exactly as returned by Bitget.

They are used to establish:

```text
physical schema
available depth
timestamp fields
sequence fields
Spot/Futures parser compatibility
REST → canonical L2Snapshot normalization
```

Fixtures must remain raw and must not be rewritten through `jq`, normalization code, or pretty-print transformations.