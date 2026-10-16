## Quick Navigation

- [Overview](#overview)
- [Instrument Families](#instrument-families)
- [Acquisition Modes](#acquisition-modes)
- [WebSocket Transport](#websocket-transport)
- [Trade Stream](#trade-stream)
- [Spot Trades](#spot-trades)
- [Linear Swap Trades](#linear-swap-trades)
- [Futures Trades](#futures-trades)
- [Option Trades](#option-trades)
- [WebSocket L2 Books](#websocket-l2-books)
- [L2 Price Level](#l2-price-level)
- [L2 Update Semantics](#l2-update-semantics)
- [Sequence Model](#sequence-model)
- [WebSocket Bootstrap](#websocket-bootstrap)
- [Checksum](#checksum)
- [REST L2 Snapshots](#rest-l2-snapshots)
- [REST Trade Snapshot](#rest-trade-snapshot)
- [L2 Acquisition Modes](#l2-acquisition-modes)
- [Gap Detection and Recovery](#gap-detection-and-recovery)
- [Timestamp Model](#timestamp-model)
- [Quantity Semantics](#quantity-semantics)
- [Canonical Outputs](#canonical-outputs)
- [Provisional Format IDs](#provisional-format-ids)
- [Fixtures](#fixtures)
- [Canonical Review Items](#canonical-review-items)

## Overview

OKX live market data was inspected for:

| Market | Trades | WebSocket L2 | REST L2 |
|---|---:|---:|---:|
| Spot | Yes | Yes | Yes |
| Linear Swap | Yes | Yes | Yes |
| Linear Future | No trade observed | Yes | Yes |
| Inverse Future | No trade observed | Yes | Yes |
| Option | REST trades observed | Yes | Yes |

The following instruments were used:

```text
Spot
BTC-USDT

Linear Swap
BTC-USDT-SWAP

Linear Future
BTC-USD_UM-261030

Inverse Future
BTC-USD-261030

Option
BTC-USD-261003-84500-C
```

The same observed L2 physical format is used across Spot, Swap, Futures, and Options.

OKX exposes:

```text
WebSocket trades

WebSocket books
    → snapshot
    → incremental updates

REST books
    → current point-in-time snapshot

REST trades
    → recent executions
```

---

## Instrument Families

OKX instrument metadata must determine market semantics rather than relying only on symbol naming.

### Spot

```text
instType = SPOT
instId   = BTC-USDT
```

### Linear Swap

```text
instType = SWAP
instId   = BTC-USDT-SWAP
```

### Inverse Futures

Observed BTC inverse futures:

```text
instFamily = BTC-USD
uly        = BTC-USD
ctType     = inverse
settleCcy  = BTC
ctVal      = 100
ctValCcy   = USD
```

Example:

```text
BTC-USD-261030
```

### Linear Futures

Observed BTC linear futures:

```text
instFamily = BTC-USD_UM
uly        = BTC-USD
ctType     = linear
settleCcy  = USD
ctVal      = 0.01
ctValCcy   = BTC
```

Example:

```text
BTC-USD_UM-261030
```

OKX also exposes special families such as:

```text
BTC-USD_UM_XPERP
```

These were not included in the initial format investigation.

### Options

Observed BTC option:

```text
instType   = OPTION
instFamily = BTC-USD
uly        = BTC-USD
instId     = BTC-USD-261003-84500-C
optType    = C
stk        = 84500
ctVal      = 1
ctValCcy   = BTC
settleCcy  = BTC
```

Instrument metadata is authoritative for quantity interpretation.

---

## Acquisition Modes

### Trades

```text
WebSocket trades
    ↓
trade messages
    ↓
canonical Trade
```

REST additionally provides recent trade history.

### Continuous L2

```text
WebSocket books
    ↓
snapshot
    ↓
update
    ↓
update
    ↓
maintained local book
```

### Periodic L2

```text
REST books
    ↓
L2Snapshot
    ↓
wait interval
    ↓
REST books
    ↓
L2Snapshot
```

WebSocket reconstruction and REST snapshot polling are separate acquisition modes.

---

## WebSocket Transport

### Endpoint

```text
wss://ws.okx.com:8443/ws/v5/public
```

### Subscription

```json
{
  "op": "subscribe",
  "args": [
    {
      "channel": "books",
      "instId": "BTC-USDT"
    }
  ]
}
```

Trade subscription:

```json
{
  "op": "subscribe",
  "args": [
    {
      "channel": "trades",
      "instId": "BTC-USDT"
    }
  ]
}
```

Successful subscription acknowledgement:

```text
event = subscribe
arg
connId
```

Heartbeat handling is separate from market-data parsing.

---

## Trade Stream

Observed Spot and Linear Swap WebSocket trades share the same physical schema.

### Envelope

```text
arg
data
```

### Trade Record

Observed fields:

| Field | Meaning |
|---|---|
| `instId` | Instrument ID |
| `tradeId` | Trade ID |
| `px` | Execution price |
| `sz` | Exchange-native quantity |
| `side` | Aggressor side |
| `ts` | Trade timestamp |
| `seqId` | Sequence identifier |
| `count` | Trade count metadata |
| `source` | Source metadata |

Observed schema:

```text
count
instId
px
seqId
side
source
sz
tradeId
ts
```

Observed sides:

```text
buy
sell
```

These map directly to canonical aggressor side.

---

## Spot Trades

Observed example:

```text
instId  = BTC-USDT
px      = 84540.2
sz      = 0.00295754
side    = buy
tradeId = 1066072295
seqId   = 81935455620
count   = 1
source  = 0
```

For Spot:

```text
sz → base quantity
```

subject to instrument metadata validation.

---

## Linear Swap Trades

Observed example:

```text
instId  = BTC-USDT-SWAP
px      = 84499.4
sz      = 0.03
side    = sell
tradeId = 2971205617
seqId   = 343661238003
count   = 1
source  = 0
```

The observed physical schema matches Spot.

`sz` must be normalized using the contract specification.

---

## Futures Trades

Subscriptions succeeded for:

```text
BTC-USD_UM-261030
BTC-USD-261030
```

No trade messages arrived during the inspection window.

This does not establish a different format.

The Futures WebSocket trade schema remains provisionally compatible with the observed Spot/Swap trade format until a raw Futures execution is captured.

---

## Option Trades

The WebSocket subscription for:

```text
BTC-USD-261003-84500-C
```

succeeded, but no new execution arrived during the capture window.

Recent trades were successfully retrieved through REST.

### Observed REST Option Trade Schema

```text
instId
px
side
source
sz
tradeId
ts
```

Example:

```text
instId  = BTC-USD-261003-84500-C
px      = 0.0023
side    = sell
source  = 0
sz      = 25
tradeId = 296
```

Unlike the observed Spot/Swap WebSocket trade schema, the REST Option trade record did not contain:

```text
count
seqId
```

Therefore the Option WebSocket trade format is not considered empirically confirmed from the current capture.

---

## WebSocket L2 Books

The `books` channel was inspected for:

```text
Spot
Linear Swap
Linear Future
Inverse Future
Option
```

All use the same observed physical format.

### Envelope

```text
action
arg
data
```

### Actions

Initial message:

```text
action = snapshot
```

Subsequent messages:

```text
action = update
```

### Book Payload

```text
asks
bids
checksum
prevSeqId
seqId
ts
```

Snapshot example:

```text
action    = snapshot
prevSeqId = -1
seqId     = A
```

Update example:

```text
action    = update
prevSeqId = A
seqId     = B
```

---

## L2 Price Level

OKX uses four-element price levels:

```text
[
    price,
    quantity,
    reserved,
    order_count
]
```

Example Spot bid:

```text
[
    "84540.1",
    "0.44737124",
    "0",
    "10"
]
```

Canonical mapping:

```text
level[0] → price
level[1] → exchange-native quantity
level[2] → ignored/reserved
level[3] → order_count
```

This directly populates canonical:

```text
price
quantity_*
order_count
```

OKX is therefore a confirmed source for:

```text
L2PriceLevel.order_count
```

### Order Count

Examples:

```text
["84540.1", "0.44737124", "0", "10"]
                                         ↑
                                    10 orders
```

```text
["84499.5", "775.96", "0", "52"]
                                     ↑
                                52 orders
```

Option books use the same representation:

```text
["0.0022", "240", "0", "2"]
```

---

## L2 Update Semantics

Updates contain resulting quantities rather than quantity deltas.

Canonical interpretation:

```text
quantity > 0
    → set

quantity = 0
    → delete
```

Observed deletion:

```text
["84545.7", "0", "0", "0"]
```

maps to:

```text
delete level 84545.7
```

The associated order count also becomes zero.

No comparison against the previous local quantity is required to derive `set` versus `delete`.

---

## Sequence Model

OKX exposes explicit previous/current sequencing:

```text
prevSeqId
seqId
```

### Snapshot

Observed:

```text
prevSeqId = -1
seqId     = A
```

The snapshot sentinel:

```text
prevSeqId = -1
```

maps to:

```text
canonical previous sequence = null
```

### Updates

Observed pattern:

```text
snapshot:
    prevSeqId = -1
    seqId     = A

update:
    prevSeqId = A
    seqId     = B

update:
    prevSeqId = B
    seqId     = C
```

Continuity validation:

```text
current.prevSeqId
    ==
previous.seqId
```

### Spot Example

```text
snapshot
prevSeqId = -1
seqId     = 81935454294

update
prevSeqId = 81935454294
seqId     = 81935454304

update
prevSeqId = 81935454304
seqId     = 81935454334

update
prevSeqId = 81935454334
seqId     = 81935454346
```

`seqId` is not required to increase by exactly one.

The correct validation is linkage through `prevSeqId`.

### Canonical Mapping

Using the emerging canonical sequence model:

```text
update_sequence_previous
update_sequence_first
update_sequence_last
```

OKX maps as:

```text
snapshot:

update_sequence_previous = null
update_sequence_first    = seqId
update_sequence_last     = seqId
```

Updates:

```text
update_sequence_previous = prevSeqId
update_sequence_first    = seqId
update_sequence_last     = seqId
```

This is structurally similar to Bitget:

```text
Bitget:
pseq → seq

OKX:
prevSeqId → seqId
```

---

## WebSocket Bootstrap

The `books` channel is self-bootstrapping.

```text
CONNECT
    ↓
SUBSCRIBE books
    ↓
WAIT FOR action=snapshot
    ↓
construct local book
    ↓
record seqId
    ↓
receive update
    ↓
validate prevSeqId
    ↓
apply changed levels
    ↓
LIVE
```

No REST snapshot is required for normal WebSocket bootstrap.

---

## Checksum

The WebSocket payload contains:

```text
checksum
```

However, every inspected message returned:

```text
checksum = 0
```

across the observed markets.

Therefore the physical field is preserved in the format specification, but the current evidence does not establish a useful checksum-validation procedure.

MarketForge must not assume that a nonzero usable checksum is available from this channel.

REST `books` responses did not expose a checksum field in the inspected responses.

---

## REST L2 Snapshots

Endpoint:

```text
GET /api/v5/market/books
```

Observed request:

```text
instId=<instrument>
sz=400
```

The endpoint was inspected for:

```text
BTC-USDT
BTC-USDT-SWAP
BTC-USD_UM-261030
BTC-USD-261030
BTC-USD-261003-84500-C
```

### Observed Result Schema

```text
asks
bids
seqId
ts
```

No observed REST fields:

```text
prevSeqId
checksum
```

### Price Levels

REST uses the same four-element representation:

```text
[
    price,
    quantity,
    reserved,
    order_count
]
```

Therefore WebSocket and REST L2 can share price-level parsing logic.

---

## REST Depth Behavior

`sz=400` means up to 400 available levels rather than a guarantee of exactly 400 populated levels.

Observed:

| Market | Requested | Bids | Asks |
|---|---:|---:|---:|
| Spot | 400 | 400 | 400 |
| Linear Swap | 400 | 400 | 400 |
| Linear Future | 400 | 400 | 400 |
| Inverse Future | 400 | 87 | 57 |
| Option | 400 | 10 | 10 |

Sparse markets therefore legitimately return fewer levels.

---

## REST Trade Snapshot

Endpoint:

```text
GET /api/v5/market/trades
```

A specific:

```text
instId
```

was used successfully for Option trades.

Example:

```text
instId=BTC-USD-261003-84500-C
limit=100
```

The response returned 100 recent trades.

Observed fields:

```text
instId
px
side
source
sz
tradeId
ts
```

REST recent trades are useful for:

```text
recent-history acquisition
activity discovery
validation
fixture discovery
```

They are a separate physical format from the confirmed WebSocket trade format.

---

## L2 Acquisition Modes

### Continuous Reconstruction

```text
WebSocket snapshot
    ↓
updates
    ↓
maintained local book
```

Suitable for:

```text
tick-level microstructure
every L2 update
continuous book state
high-frequency processing
```

### Periodic REST Snapshot

```text
REST books
    ↓
L2Snapshot
    ↓
wait configured interval
    ↓
REST books
```

Suitable for:

```text
periodic depth snapshots
stateless acquisition
lower-frequency datasets
independent book observations
```

### Reconciliation

REST can additionally validate a locally reconstructed WebSocket book:

```text
WS snapshot
    ↓
WS updates
    ↓
local book
    │
    │ periodically
    ↓
REST snapshot
    ↓
compare overlapping levels
```

Mismatch handling belongs to runtime/data-integrity policy.

---

## Gap Detection and Recovery

WebSocket continuity is validated through:

```text
current.prevSeqId == previous.seqId
```

If false:

```text
sequence mismatch
    ↓
invalidate local book
    ↓
resubscribe / reconnect
    ↓
wait for new snapshot
    ↓
rebuild book
    ↓
resume updates
```

The engine must not infer missing levels from later updates.

Because `books` provides a new snapshot on subscription, normal recovery does not require REST.

---

## Timestamp Model

### Trades

Trade records contain:

```text
ts
```

Observed as Unix epoch milliseconds.

Canonical mapping:

```text
ts
    ↓
event_timestamp_ns
```

### WebSocket L2

Book payloads contain:

```text
ts
```

No second matching-engine timestamp was observed in the inspected OKX `books` payload.

### REST L2

REST snapshots contain:

```text
ts
```

### MarketForge Arrival Time

MarketForge independently records:

```text
arrival_timestamp_ns
```

when the message or response reaches the ingestion engine.

This is runtime metadata rather than an OKX source field.

---

## Quantity Semantics

The physical field:

```text
sz
```

for trades and:

```text
level[1]
```

for books represent exchange-native quantity.

They must not be interpreted identically across all instruments.

Examples include:

```text
Spot
Linear Swap
Linear Future
Inverse Future
Option
```

Canonical normalization uses instrument metadata such as:

```text
ctType
ctVal
ctValCcy
settleCcy
```

to derive:

```text
quantity_base
quantity_quote
quantity_contracts
```

where applicable.

Physical parser logic therefore remains independent from contract-value normalization.

---

## Canonical Outputs

### Trades

```text
WebSocket trades
    ↓
Trade
```

REST recent trades may also normalize into canonical trade records when used as an acquisition source.

### WebSocket Books

```text
action=snapshot
    ↓
L2Snapshot
```

```text
action=update
    ↓
L2Update
```

### REST Books

```text
REST books
    ↓
L2Snapshot
```

### Price Levels

```text
[price, quantity, reserved, order_count]
                ↓
L2PriceLevel
```

Canonical:

```text
price
quantity_base
quantity_quote
quantity_contracts
order_count