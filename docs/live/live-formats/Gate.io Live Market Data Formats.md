## Quick Navigation

- [Overview](#overview)
- [Instrument Families](#instrument-families)
- [Acquisition Modes](#acquisition-modes)
- [WebSocket Transport](#websocket-transport)
- [Trade Streams](#trade-streams)
- [Spot Trades](#spot-trades)
- [Derivative Trades](#derivative-trades)
- [WebSocket L2 Updates](#websocket-l2-updates)
- [Spot L2 Format](#spot-l2-format)
- [Derivative L2 Format](#derivative-l2-format)
- [L2 Update Semantics](#l2-update-semantics)
- [Sequence Model](#sequence-model)
- [REST L2 Snapshots](#rest-l2-snapshots)
- [Bootstrap Model](#bootstrap-model)
- [Gap Detection and Recovery](#gap-detection-and-recovery)
- [Timestamp Model](#timestamp-model)
- [Quantity Semantics](#quantity-semantics)
- [Canonical Outputs](#canonical-outputs)
- [Provisional Format IDs](#provisional-format-ids)
- [Fixtures](#fixtures)
- [Canonical Sequence Mapping](#canonical-sequence-mapping)

## Overview

Gate.io live market data was inspected for:

| Market | Trades | WebSocket L2 | REST L2 |
|---|---:|---:|---:|
| Spot | Yes | Yes | Yes |
| Linear Perpetual | Yes | Yes | Yes |
| Inverse Perpetual | No trade observed | Yes | Yes |
| Linear Delivery Future | Yes | Yes | Yes |

The inspected instruments were:

```text
Spot
BTC_USDT

Linear Perpetual
BTC_USDT

Inverse Perpetual
BTC_USD

Linear Delivery Future
BTC_USDT_20261009
```

Gate.io exposes a delta-only WebSocket L2 stream together with REST order-book snapshots.

```text
WebSocket L2
    → updates only

REST L2
    → complete point-in-time snapshot
```

A complete continuously maintained book therefore requires synchronization of a REST snapshot with buffered WebSocket updates.

Gate.io also exposes materially different physical formats for Spot and derivative market data.

---

## Instrument Families

### Spot

Observed instrument:

```text
id           = BTC_USDT
base         = BTC
quote        = USDT
trade_status = tradable
type         = normal
```

### Linear Perpetual

Observed contract:

```text
name              = BTC_USDT
type              = direct
quanto_multiplier = 0.0001
order_price_round = 0.1
order_size_min    = 1
```

This is treated as a linear derivative.

### Inverse Perpetual

Observed contract:

```text
name              = BTC_USD
type              = inverse
quanto_multiplier = 0
order_price_round = 0.1
order_size_min    = 1
```

### Linear Delivery Futures

Observed contracts included:

```text
BTC_USDT_20261009
BTC_USDT_20261016
BTC_USDT_20261225
BTC_USDT_20270326
```

The inspected contract was:

```text
name              = BTC_USDT_20261009
underlying        = BTC_USDT
cycle             = BI-WEEKLY
quanto_multiplier = 0.0001
```

Instrument metadata determines contract and quantity semantics independently from the physical market-data parser.

---

## Acquisition Modes

### Trades

```text
WebSocket trade stream
    ↓
exchange trade
    ↓
canonical Trade
```

### Continuous L2

```text
connect WebSocket
    ↓
buffer L2 updates
    ↓
fetch REST snapshot
    ↓
synchronize snapshot ID with U/u ranges
    ↓
construct local book
    ↓
replay buffered updates
    ↓
LIVE
```

### Periodic L2

REST snapshots can also be used independently:

```text
REST order book
    ↓
L2Snapshot
    ↓
wait configured interval
    ↓
REST order book
```

This supports periodic snapshot acquisition without maintaining every WebSocket update.

---

## WebSocket Transport

### Spot Endpoint

```text
wss://api.gateio.ws/ws/v4/
```

### USDT Perpetual Endpoint

```text
wss://fx-ws.gateio.ws/v4/ws/usdt
```

### BTC-Settled Perpetual Endpoint

```text
wss://fx-ws.gateio.ws/v4/ws/btc
```

### USDT Delivery Endpoint

```text
wss://fx-ws.gateio.ws/v4/ws/delivery/usdt
```

### Subscription Envelope

Gate.io subscriptions use:

```json
{
  "time": 1790979879,
  "channel": "spot.trades",
  "event": "subscribe",
  "payload": [
    "BTC_USDT"
  ]
}
```

Successful subscriptions return:

```text
event  = subscribe
result = status: success
```

All inspected Spot, perpetual, and delivery subscriptions succeeded. :chatgpt-content-reference{index="0"}

---

## Trade Streams

Gate.io exposes distinct Spot and derivative trade formats.

```text
Spot
    → dictionary trade record

Derivatives
    → array containing derivative trade records
```

The two formats should remain separate physical parsers even though both normalize into canonical `Trade`.

---

## Spot Trades

### Channel

```text
spot.trades
```

### Subscription

```json
{
  "channel": "spot.trades",
  "event": "subscribe",
  "payload": [
    "BTC_USDT"
  ]
}
```

### Observed Record

```text
amount
create_time
create_time_ms
currency_pair
id
id_market
money
price
range
side
stock
trade_mode
```

Observed example:

```text
currency_pair = BTC_USDT
id            = 221560549
price         = 84547
amount        = 0.000353
side          = buy
stock         = BTC
money         = USDT
```

The complete observed Spot trade schema and example are shown in the captured stream. :chatgpt-content-reference{index="1"}

### Canonical Mapping

```text
currency_pair → instrument
id            → trade_id
price         → price
amount        → quantity_base
side          → aggressor_side
create_time_ms
              → event_timestamp
```

Observed sides:

```text
buy
sell
```

map directly to canonical aggressor side.

---

## Derivative Trades

### Channel

```text
futures.trades
```

The same observed derivative trade structure was used by Linear Perpetual and Linear Delivery.

### Observed Record

```text
contract
create_time
create_time_ms
id
price
size
```

Linear Perpetual example:

```text
contract       = BTC_USDT
id             = 846062333
price          = 84513.7
size           = -440
create_time_ms = 1790979887341
```

:chatgpt-content-reference{index="2"}

Linear Delivery example:

```text
contract       = BTC_USDT_20261009
id             = 61166
price          = 84554.4
size           = -5
create_time_ms = 1790979916083
```

:chatgpt-content-reference{index="3"}

### Signed Size

Derivative `size` is signed.

Canonical side is derived from the sign:

```text
size > 0
    → buy

size < 0
    → sell
```

Canonical native quantity uses:

```text
abs(size)
```

Therefore:

```text
size = -440
```

normalizes as:

```text
aggressor_side = sell
native_quantity = 440
```

Contract metadata determines conversion into:

```text
quantity_base
quantity_quote
quantity_contracts
```

### Inverse Perpetual

The Inverse Perpetual trade subscription succeeded, but no trade arrived during the capture window. :chatgpt-content-reference{index="4"}

Its trade format remains provisionally compatible with the observed derivative format until a raw execution is captured.

---

## WebSocket L2 Updates

Gate.io uses incremental order-book channels:

```text
spot.order_book_update
futures.order_book_update
```

The inspected WebSocket streams delivered:

```text
event = update
```

No initial WebSocket snapshot was observed.

The WebSocket stream must therefore be combined with a REST snapshot to establish a complete book.

### Common Sequence Fields

All inspected markets expose:

```text
U
u
```

where a message represents an update-ID range.

Additional common fields include:

```text
s
t
a
b
```

Spot additionally exposed:

```text
E
e
l
```

while derivative payloads commonly exposed:

```text
l
```

The delivery sample did not contain `l`.

Parser logic should tolerate these market-specific optional fields.

---

## Spot L2 Format

### Channel

```text
spot.order_book_update
```

### Subscription

```text
BTC_USDT
100ms
```

### Observed Payload Fields

```text
E
U
a
b
e
l
s
t
u
```

### Price Level

Spot uses arrays:

```text
[price, quantity]
```

Example:

```text
["84594.5", "0.001148"]
```

Deletion example:

```text
["84587.7", "0"]
```

The observed Spot payload and level representation are shown directly in the capture. :chatgpt-content-reference{index="5"}

### Sequence Example

```text
message 1
U = 40150149034
u = 40150149041

message 2
U = 40150149042
u = 40150149054

message 3
U = 40150149055
u = 40150149070

message 4
U = 40150149071
u = 40150149078
```

:chatgpt-content-reference{index="6"} :chatgpt-content-reference{index="7"} :chatgpt-content-reference{index="8"} :chatgpt-content-reference{index="9"}

---

## Derivative L2 Format

Linear Perpetual, Inverse Perpetual, and Linear Delivery use object-based levels.

### Channel

```text
futures.order_book_update
```

### Price Level

```json
{
  "p": "84513.7",
  "s": 8165
}
```

where:

```text
p → price
s → native quantity
```

Linear Perpetual example: :chatgpt-content-reference{index="10"}

```text
p = 84513.7
s = 8165
```

Inverse Perpetual example: :chatgpt-content-reference{index="11"}

```text
p = 84029.8
s = 2714
```

Linear Delivery example: :chatgpt-content-reference{index="12"}

```text
p = 84554.4
s = 0
```

### Linear Perpetual Sequence

Observed:

```text
U = 126892322962
u = 126892323027

U = 126892323028
u = 126892323055

U = 126892323056
u = 126892323131

U = 126892323132
u = 126892323184
```

:chatgpt-content-reference{index="13"} :chatgpt-content-reference{index="14"} :chatgpt-content-reference{index="15"} :chatgpt-content-reference{index="16"}

### Inverse Perpetual Sequence

Observed:

```text
U = 5928312675
u = 5928312676

U = 5928312677
u = 5928312678

U = 5928312679
u = 5928312680

U = 5928312681
u = 5928312682
```

:chatgpt-content-reference{index="17"} :chatgpt-content-reference{index="18"} :chatgpt-content-reference{index="19"} :chatgpt-content-reference{index="20"}

### Linear Delivery Sequence

Observed:

```text
U = 2481668
u = 2481669

U = 2481670
u = 2481671
```

:chatgpt-content-reference{index="21"} :chatgpt-content-reference{index="22"}

---

## L2 Update Semantics

Both Spot and derivatives expose resulting quantities rather than quantity deltas.

### Spot

```text
[price, quantity]
```

### Derivatives

```text
{
    p: price,
    s: quantity
}
```

Canonical interpretation:

```text
quantity > 0
    → set

quantity = 0
    → delete
```

Observed Spot deletion:

```text
["84587.7", "0"]
```

:chatgpt-content-reference{index="23"}

Observed derivative deletion:

```text
{
    "p": "84553.2",
    "s": 0
}
```

:chatgpt-content-reference{index="24"}

No comparison against the previous local quantity is required to derive the canonical action.

---

## Sequence Model

Gate.io represents each WebSocket update as a range:

```text
U = first update ID
u = last update ID
```

A single message may therefore represent multiple internal exchange updates.

### Continuity

Observed messages linked as:

```text
current.U = previous.u + 1
```

Conceptually:

```text
message 1
U = A
u = B

message 2
U = B + 1
u = C

message 3
U = C + 1
u = D
```

The range itself may contain many sequence IDs.

### Canonical Mapping

```text
update_sequence_first    = U
update_sequence_last     = u
update_sequence_previous = null
cross_sequence           = null
```

Continuity can be validated using:

```text
current.update_sequence_first
    ==
previous.update_sequence_last + 1
```

For bootstrap, a buffered range may also be valid when it contains the expected first sequence:

```text
U <= expected_sequence <= u
```

This sequence model is structurally similar to Binance's range-based sequencing.

---

## REST L2 Snapshots

Gate.io exposes separate REST endpoints for each market family.

### Spot

```text
GET /api/v4/spot/order_book
```

Example:

```text
currency_pair=BTC_USDT
limit=100
with_id=true
```

Observed fields:

```text
asks
bids
current
id
update
```

Observed:

```text
id      = 40150157925
current = 1790979933862
update  = 1790979933859
```

:chatgpt-content-reference{index="25"}

Spot REST levels use:

```text
[price, quantity]
```

### Linear Perpetual

```text
GET /api/v4/futures/usdt/order_book
```

Example:

```text
contract=BTC_USDT
limit=100
with_id=true
```

Observed:

```text
id      = 126892358099
asks    = 100
bids    = 100
```

:chatgpt-content-reference{index="26"}

### Inverse Perpetual

```text
GET /api/v4/futures/btc/order_book
```

Example:

```text
contract=BTC_USD
limit=100
with_id=true
```

Observed:

```text
id      = 5928314169
asks    = 100
bids    = 100
```

:chatgpt-content-reference{index="27"}

### Linear Delivery

```text
GET /api/v4/delivery/usdt/order_book
```

Example:

```text
contract=BTC_USDT_20261009
limit=100
with_id=true
```

Observed:

```text
id   = 2481733
bids = 42
asks = 40
```

:chatgpt-content-reference{index="28"}

The requested depth is an upper bound. Sparse markets may return fewer levels.

---

## Bootstrap Model

Gate.io WebSocket L2 is delta-only.

A complete local book requires a REST snapshot.

### Required Process

```text
CONNECT WebSocket
    ↓
SUBSCRIBE order_book_update
    ↓
BUFFER U/u updates
    ↓
FETCH REST snapshot with ID
    ↓
identify first applicable buffered range
    ↓
initialize local book from REST
    ↓
replay buffered updates
    ↓
LIVE
```

Let:

```text
snapshot_id = REST.id
```

The first required sequence after the snapshot is:

```text
snapshot_id + 1
```

The bootstrap engine searches buffered updates for a range satisfying:

```text
U <= snapshot_id + 1 <= uU <= snapshot_id + 1 <= u
```

Earlier buffered ranges are obsolete and can be discarded.

After the matching range is applied:

```text
local_sequence = u
```

and normal continuity becomes:

```text
next.U = local_sequence + 1
```

followed by:

```text
local_sequence = next.u
```

### Example

```text
REST snapshot
id = 1000

Buffered WebSocket:

U=990   u=998
U=999   u=1000
U=1001  u=1010
U=1011  u=1020
```

The required first post-snapshot sequence is:

```text
1001
```

The first applicable message is:

```text
U=1001
u=1010
```

Bootstrap therefore becomes:

```text
load REST snapshot at id=1000
    ↓
discard ranges ending <= 1000
    ↓
apply U=1001 ... u=1010
    ↓
local_sequence=1010
    ↓
apply U=1011 ... u=1020
    ↓
LIVE
```

A range may also overlap the required sequence:

```text
REST id = 1000

WS:
U=998
u=1005
```

because:

```text
998 <= 1001 <= 1005
```

The exchange-specific bootstrap implementation must follow Gate.io's applicable update-range semantics when handling such overlap.

---

## Gap Detection and Recovery

Once bootstrap is complete, expected continuity is:

```text
next.U = previous.u + 1
```

A gap such as:

```text
previous:
U=1000
u=1010

next:
U=1015
u=1020
```

means sequences:

```text
1011 ... 1014
```

were missed.

The local book can no longer be considered valid.

Recovery:

```text
sequence gap
    ↓
invalidate local book
    ↓
buffer new WebSocket updates
    ↓
fetch new REST snapshot
    ↓
synchronize REST id with buffered U/u
    ↓
rebuild book
    ↓
LIVE
```

MarketForge must not infer missing price-level changes from later updates.

---

## Timestamp Model

Gate.io exposes several timestamp fields.

### WebSocket Envelope

Observed envelopes contain:

```text
time
time_ms
```

These represent server/message timing.

### Spot Trades

Observed Spot trades contain:

```text
create_time
create_time_ms
```

`create_time_ms` provides the more precise trade timestamp.

Example:

```text
create_time    = 1790979881
create_time_ms = 1790979881644.416000
```

The fractional millisecond representation must be parsed without unnecessary floating-point loss.

Canonical conversion should preserve the available precision in:

```text
event_timestamp_ns
```

### Derivative Trades

Observed derivative trades also contain:

```text
create_time
create_time_ms
```

Example:

```text
create_time    = 1790979887
create_time_ms = 1790979887341
```

### L2 Updates

Observed order-book payloads contain:

```text
t
```

representing the book update timestamp.

The WebSocket envelope independently contains:

```text
time
time_ms
```

MarketForge should distinguish exchange event time from message arrival time.

### REST Snapshots

Observed REST snapshots contain:

```text
current
update
```

The physical units differ between inspected market families.

Spot example:

```text
current = 1790979933862
update  = 1790979933859
```

Derivative example:

```text
current = 1790979936.809
update  = 1790979936.808
```

Therefore timestamp parsing must be format-specific rather than assuming the same numeric unit for all Gate.io REST book responses.

### MarketForge Arrival Time

MarketForge independently records:

```text
arrival_timestamp_ns
```

when a WebSocket message or REST response reaches the live engine.

This is generated locally and must remain separate from exchange timestamps.

---

## Quantity Semantics

Gate.io physical quantity fields differ between Spot and derivatives.

### Spot Trades

```text
amount
```

represents Spot trade quantity.

For:

```text
BTC_USDT
```

this maps to base quantity:

```text
amount → quantity_base
```

### Spot L2

Spot levels use:

```text
[price, quantity]
```

with quantity interpreted in Spot base units.

### Derivative Trades

Derivative trades use signed:

```text
size
```

The sign determines aggressor side:

```text
size > 0 → buy
size < 0 → sell
```

while magnitude becomes:

```text
native_quantity = abs(size)
```

### Derivative L2

Derivative levels use:

```text
{
    p: price,
    s: size
}
```

Book `s` is an absolute level quantity rather than an aggressor-side signal.

Therefore:

```text
s > 0 → set
s = 0 → delete
```

The derivative contract specification determines conversion into:

```text
quantity_base
quantity_quote
quantity_contracts
```

using metadata such as:

```text
type
quanto_multiplier
underlying
settlement currency
```

Trade `size` and book `s` must not share sign interpretation.

---

## Canonical Outputs

### Spot Trades

```text
spot.trades
    ↓
GATE-LIVE-T1 parser
    ↓
Trade
```

### Derivative Trades

```text
futures.trades
    ↓
GATE-LIVE-T2 parser
    ↓
derive side from signed size
    ↓
Trade
```

### Spot WebSocket L2

```text
spot.order_book_update
    ↓
GATE-LIVE-B1
    ↓
L2Update
```

### Derivative WebSocket L2

```text
futures.order_book_update
    ↓
GATE-LIVE-B2
    ↓
L2Update
```

### Spot REST L2

```text
spot/order_book
    ↓
GATE-LIVE-B3
    ↓
L2Snapshot
```

### Derivative REST L2

```text
futures/.../order_book
delivery/.../order_book
    ↓
GATE-LIVE-B4
    ↓
L2Snapshot
```

The canonical representation therefore hides Gate.io's physical differences while retaining acquisition provenance and sequence metadata.

---

## Provisional Format IDs

### Trades

| Format | Transport | Role | Markets |
|---|---|---|---|
| `GATE-LIVE-T1` | WebSocket | Trade stream | Spot |
| `GATE-LIVE-T2` | WebSocket | Trade stream | Linear Perpetual, Inverse Perpetual, Delivery |

The derivative format remains provisional for Inverse Perpetual because no execution arrived during the capture window.

### WebSocket Books

| Format | Transport | Role | Markets |
|---|---|---|---|
| `GATE-LIVE-B1` | WebSocket | L2 updates | Spot |
| `GATE-LIVE-B2` | WebSocket | L2 updates | Linear Perpetual, Inverse Perpetual, Delivery |

The split is required because price levels differ physically:

```text
Spot:
[price, quantity]

Derivatives:
{
    p: price,
    s: quantity
}
```

### REST Books

| Format | Transport | Role | Markets |
|---|---|---|---|
| `GATE-LIVE-B3` | REST | L2 snapshot | Spot |
| `GATE-LIVE-B4` | REST | L2 snapshot | Linear Perpetual, Inverse Perpetual, Delivery |

REST Spot and derivative formats also differ in price-level representation and timestamp units.

---

## Parser Compatibility

Current evidence supports:

```text
Trades
────────────────────────────────

Spot
    → GATE-LIVE-T1

Linear Perpetual ─┐
Inverse Perpetual ├→ GATE-LIVE-T2
Delivery ─────────┘
```

Books:

```text
WebSocket
────────────────────────────────

Spot
    → GATE-LIVE-B1

Linear Perpetual ─┐
Inverse Perpetual ├→ GATE-LIVE-B2
Delivery ─────────┘


REST
────────────────────────────────

Spot
    → GATE-LIVE-B3

Linear Perpetual ─┐
Inverse Perpetual ├→ GATE-LIVE-B4
Delivery ─────────┘
```

---

## Data Integrity

Gate.io provides strong sequence information for live book reconstruction.

### Update Range

Every WebSocket update contains:

```text
U
u
```

representing the update range.

This allows MarketForge to detect:

```text
contiguous updates
overlapping bootstrap updates
obsolete buffered updates
missing update ranges
```

### REST Snapshot ID

With:

```text
with_id=true
```

REST provides:

```text
id
```

which anchors the snapshot to the exchange update sequence.

This gives the live engine enough information for deterministic snapshot/update synchronization.

### Integrity Rule

After bootstrap:

```text
current.U == previous.u + 1
```

must hold for the observed continuous stream.

A violation invalidates the locally reconstructed book.

---

## REST Snapshot Polling

REST books are also useful independently of WebSocket reconstruction.

For example:

```text
request REST snapshot
    ↓
normalize L2Snapshot
    ↓
store
    ↓
wait 1 second
    ↓
request next snapshot
```

This allows users to acquire periodic depth without maintaining every intermediate WebSocket update.

REST polling is therefore a first-class acquisition mode rather than merely a bootstrap dependency.

Polling cadence belongs to runtime configuration and must respect Gate.io rate limits.

---

## Book Reconciliation

REST snapshots may additionally be used to validate a reconstructed WebSocket book.

```text
REST snapshot
        │
        │
        ▼
compare
        ▲
        │
maintained WS book
```

Useful checks include:

```text
best bid
best ask
overlapping price levels
quantities
book ordering
spread
```

A mismatch can generate an integrity event and optionally trigger rebootstrap.

This belongs to the live integrity/runtime layer rather than the physical format parser.

---

## Fixtures

Recommended structure:

```text
tests/fixtures/live/gateio/
├── spot/
│   ├── trades.raw.jsonl
│   ├── books.raw.jsonl
│   └── book.rest.snapshot.json
│
├── linear_perpetual/
│   ├── trades.raw.jsonl
│   ├── books.raw.jsonl
│   └── book.rest.snapshot.json
│
├── inverse_perpetual/
│   ├── trades.raw.jsonl
│   ├── books.raw.jsonl
│   └── book.rest.snapshot.json
│
└── linear_delivery/
    ├── trades.raw.jsonl
    ├── books.raw.jsonl
    └── book.rest.snapshot.json
```

### Trade Fixtures

Spot fixture should preserve multiple raw:

```text
spot.trades
```

messages.

Derivative fixtures should preserve raw:

```text
futures.trades
```

messages.

For quiet markets, an acknowledgement-only fixture may be retained temporarily, but a real execution fixture is preferable when available.

### Book Fixtures

Each WebSocket fixture should preserve:

```text
subscription acknowledgement
multiple contiguous U/u updates
```

For example:

```text
U=A
u=B

U=B+1
u=C

U=C+1
u=D
```

### REST Fixtures

REST fixtures must use:

```text
with_id=true
```

so the snapshot includes:

```text
id
```

required for bootstrap synchronization tests.

### Synchronized Bootstrap Fixture

In addition to independent raw-format fixtures, at least one dedicated integration fixture should eventually preserve:

```text
buffered WebSocket updates
        +
REST snapshot captured while WS remains connected
```

This allows deterministic testing of:

```text
snapshot ID matching
buffer pruning
overlap handling
replay
transition to LIVE
```

---

## Canonical Sequence Mapping

Gate.io completes the sequence models observed across the supported exchanges.

### Gate.io

```text
U
u
```

maps to:

```text
update_sequence_first    = U
update_sequence_last     = u
update_sequence_previous = null
cross_sequence           = null
```

### Comparison

```text
Binance
────────────────────────────────
update_sequence_first    = U
update_sequence_last     = u
update_sequence_previous = pu where available
cross_sequence           = null


Gate.io
────────────────────────────────
update_sequence_first    = U
update_sequence_last     = u
update_sequence_previous = null
cross_sequence           = null


Bitget
────────────────────────────────
update_sequence_first    = seq
update_sequence_last     = seq
update_sequence_previous = pseq
cross_sequence           = null


OKX
────────────────────────────────
update_sequence_first    = seqId
update_sequence_last     = seqId
update_sequence_previous = prevSeqId
cross_sequence           = null


Bybit
────────────────────────────────
update_sequence_first    = u
update_sequence_last     = u
update_sequence_previous = null
cross_sequence           = seq
```

This supports the canonical live L2 sequence structure:

```text
update_sequence_first
update_sequence_last
update_sequence_previous
cross_sequence
```

All fields are nullable where the source does not provide the corresponding sequence concept.

### Continuity Strategies

The canonical fields support multiple source protocols without pretending they are identical.

#### Explicit Previous Sequence

Used by:

```text
Bitget
OKX
```

Validation:

```text
current.update_sequence_previous
    ==
previous.update_sequence_last
```

#### Range Continuity

Used by:

```text
Gate.io
Binance
```

Validation:

```text
current.update_sequence_first
    ==
previous.update_sequence_last + 1
```

Bootstrap may additionally accept a range containing the required sequence:

```text
first <= expected <= last
```

#### Consecutive Single Update ID

Used by Bybit:

```text
current.update_sequence_first
    ==
previous.update_sequence_last + 1
```

with:

```text
first = last = u
```

Bybit additionally preserves:

```text
cross_sequence = seq
```

for its separate cross-sequence domain.

---

## Current Gate.io Live Model

```text
GATE.IO
│
├── TRADES
│   │
│   ├── Spot
│   │      └── GATE-LIVE-T1
│   │
│   └── Derivatives
│          └── GATE-LIVE-T2
│
└── L2
    │
    ├── Spot WebSocket Updates
    │      └── GATE-LIVE-B1
    │
    ├── Derivative WebSocket Updates
    │      └── GATE-LIVE-B2
    │
    ├── Spot REST Snapshot
    │      └── GATE-LIVE-B3
    │
    └── Derivative REST Snapshot
           └── GATE-LIVE-B4
```

Continuous L2 reconstruction:

```text
WS updates
    ↓
buffer
    ↓
REST snapshot + id
    ↓
find applicable U/u range
    ↓
initialize book
    ↓
replay buffered updates
    ↓
validate continuity
    ↓
LIVE
```

Gate.io therefore provides the final range-based sequence model needed to finalize MarketForge's canonical live L2 sequence representation.