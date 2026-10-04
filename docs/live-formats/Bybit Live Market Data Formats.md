## Quick Navigation

- [Overview](#overview)
- [Acquisition Modes](#acquisition-modes)
- [WebSocket Endpoints](#websocket-endpoints)
- [Trade Streams](#trade-streams)
- [Spot Trades](#spot-trades)
- [Linear Trades](#linear-trades)
- [Inverse Trades](#inverse-trades)
- [Option Trades](#option-trades)
- [Normal WebSocket L2](#normal-websocket-l2)
- [L2 Update Semantics](#l2-update-semantics)
- [Normal L2 Sequence Model](#normal-l2-sequence-model)
- [Normal WebSocket Bootstrap](#normal-websocket-bootstrap)
- [REST L2 Snapshots](#rest-l2-snapshots)
- [Full-Depth WebSocket L2](#full-depth-websocket-l2)
- [Full-Depth Sequence Model](#full-depth-sequence-model)
- [Full-Depth Bootstrap](#full-depth-bootstrap)
- [Gap Detection and Recovery](#gap-detection-and-recovery)
- [Timestamp Model](#timestamp-model)
- [Sequence Domains](#sequence-domains)
- [Canonical Outputs](#canonical-outputs)
- [Provisional Format IDs](#provisional-format-ids)
- [Fixtures](#fixtures)

## Overview

Bybit live market data was inspected for:

| Market | Trades | Normal WS L2 | REST L2 | Full-Depth WS |
|---|---:|---:|---:|---:|
| Spot | Yes | Yes | Yes | Yes |
| Linear | Yes | Yes | Yes | Yes |
| Inverse | Yes | Yes | Yes | Yes |
| Option | Yes | Yes | Yes | No |

Bybit exposes several independent live acquisition mechanisms:

```text
WebSocket trades

WebSocket normal L2
    → snapshot
    → deltas

REST L2
    → point-in-time snapshot

WebSocket full-depth L2
    → deltas only
    → requires separate snapshot for reconstruction
```

Normal WebSocket L2 is self-bootstrapping.

Full-depth WebSocket L2 is not self-bootstrapping and requires synchronization with a full-depth snapshot.

REST snapshots are also useful independently for periodic snapshot acquisition and validation.

---

## Acquisition Modes

### Trades

```text
WebSocket publicTrade
    ↓
trade batches
    ↓
canonical Trade
```

### Normal Continuous L2

```text
WebSocket orderbook.<depth>
    ↓
snapshot
    ↓
delta
    ↓
delta
    ↓
maintained local book
```

### Full-Depth Continuous L2

```text
WebSocket orderbook.full
        +
full-depth snapshot
        ↓
synchronize
        ↓
replay buffered deltas
        ↓
maintained full book
```

### Periodic REST L2

```text
REST snapshot
    ↓
canonical L2Snapshot
    ↓
wait interval
    ↓
REST snapshot
```

These are separate acquisition capabilities and should not be collapsed into a single bootstrap mechanism.

---

## WebSocket Endpoints

### Spot

```text
wss://stream.bybit.com/v5/public/spot
```

### Linear

```text
wss://stream.bybit.com/v5/public/linear
```

### Inverse

```text
wss://stream.bybit.com/v5/public/inverse
```

### Option

```text
wss://stream.bybit.com/v5/public/option
```

### Subscription

```json
{
  "op": "subscribe",
  "args": [
    "topic"
  ]
}
```

Successful subscriptions return a control response before market-data messages begin.

---

## Trade Streams

Standard trade topic:

```text
publicTrade.<symbol>
```

Options additionally support a broad base-asset topic:

```text
publicTrade.BTC
```

which delivers trades across BTC option contracts.

Trade WebSocket messages use an envelope containing:

```text
topic
type
ts
data
```

`data` is an array and may contain multiple executions.

The observed Bybit trade envelopes use:

```text
type = snapshot
```

for normal trade delivery.

This value must not be interpreted generically as historical bootstrap data. Unlike Bitget, Bybit was observed delivering newly occurring trades in repeated `snapshot` envelopes.

---

## Spot Trades

### Topic

```text
publicTrade.BTCUSDT
```

### Observed Record

| Field | Meaning |
|---|---|
| `T` | Trade timestamp |
| `s` | Symbol |
| `S` | Aggressor side |
| `v` | Quantity |
| `p` | Price |
| `i` | Trade ID |
| `seq` | Cross sequence |
| `BT` | Block-trade flag |
| `RPI` | RPI trade flag |

Observed schema:

```text
BT
RPI
S
T
i
p
s
seq
v
```

Observed example:

```text
S   = Sell
p   = 84548.7
v   = 0.001469
BT  = false
RPI = false
```

Aggressor side maps directly:

```text
Buy  → buy
Sell → sell
```

For Spot:

```text
v → base quantity
```

---

## Linear Trades

### Topic

```text
publicTrade.BTCUSDT
```

### Observed Record

```text
BT
L
RPI
S
T
i
p
s
seq
v
```

Compared with Spot, the observed derivative format additionally contains:

```text
L
```

representing tick direction.

Example values include:

```text
L = PlusTick
```

Quantity `v` is interpreted through instrument metadata.

---

## Inverse Trades

### Topic

```text
publicTrade.BTCUSD
```

The observed physical schema matches Linear:

```text
BT
L
RPI
S
T
i
p
s
seq
v
```

Inverse quantity semantics differ from Linear and must therefore be resolved through the instrument specification rather than the physical parser.

---

## Option Trades

### Specific Contract

```text
publicTrade.<option-symbol>
```

Example:

```text
publicTrade.BTC-3OCT26-77000-C-USDT
```

Individual option contracts may be too inactive for short inspection windows.

### Broad BTC Option Stream

```text
publicTrade.BTC
```

This topic was observed successfully delivering trades across different BTC option contracts.

### Observed Record

| Field | Meaning |
|---|---|
| `T` | Trade timestamp |
| `s` | Option symbol |
| `S` | Aggressor side |
| `v` | Quantity |
| `p` | Execution price |
| `i` | Trade ID |
| `seq` | Cross sequence |
| `BT` | Block-trade flag |
| `iP` | Index price |
| `mP` | Mark price |
| `iv` | Trade implied volatility |
| `mIv` | Mark implied volatility |

Observed schema:

```text
BT
S
T
i
iP
iv
mIv
mP
p
s
seq
v
```

Example:

```text
symbol = BTC-5OCT26-85000-C-USDT

price      = 305
quantity   = 0.01
side       = Buy

index price = 84489.3566542
mark price  = 301.97333143

trade IV = 0.1856
mark IV  = 0.1846
```

These option-specific values are useful market state associated with the execution and should not be discarded before canonical-schema review.

Candidate canonical fields:

```text
index_price
mark_price
implied_volatility
mark_implied_volatility
```

Whether these belong in nullable `Trade` fields or a dedicated `OptionTrade` representation remains unresolved.

---

## Normal WebSocket L2

Normal order-book topic:

```text
orderbook.<depth>.<symbol>
```

Observed examples:

```text
orderbook.200.BTCUSDT
orderbook.200.BTCUSD
orderbook.25.<option-symbol>
```

All inspected markets use the same physical message structure.

### Envelope

```text
topic
type
ts
cts
data
```

### Book Payload

```text
s
b
a
u
seq
```

Where:

| Field | Meaning |
|---|---|
| `s` | Symbol |
| `b` | Bid levels |
| `a` | Ask levels |
| `u` | Order-book update ID |
| `seq` | Cross sequence |
| `ts` | System/message timestamp |
| `cts` | Matching-engine timestamp |

### Level

```text
[price, quantity]
```

No order count was observed.

### Message Types

Initial message:

```text
type = snapshot
```

Subsequent messages:

```text
type = delta
```

A later snapshot can replace the current local-book state.

---

## L2 Update Semantics

Normal and full-depth Bybit updates use the same price-level semantics.

```text
[price, quantity]
```

Canonical mapping:

```text
quantity > 0
    → set

quantity = 0
    → delete
```

Zero-quantity deletions were observed in Spot, Linear, Inverse, and Option streams.

For example:

```text
["84549.5", "0"]
```

means:

```text
delete ask level 84549.5
```

No `T - T1` quantity comparison is required to derive the canonical action.

---

## Normal L2 Sequence Model

Bybit exposes two sequence-related fields:

```text
u
seq
```

They represent different sequence domains.

### Update ID

Observed Spot:

```text
104161171
104161172
104161173
104161174
```

Observed Linear:

```text
61633129
61633130
61633131
61633132
```

Observed Inverse:

```text
59481494
59481495
59481496
59481497
```

Observed Option:

```text
117648
117649
117650
117651
```

Across the inspected samples:

```text
current.u = previous.u + 1
```

### Cross Sequence

`seq` also increases, but not by one.

Example Spot:

```text
114905142534
114905142607
114905142686
114905142754
```

Therefore:

```text
u
    → book update continuity

seq
    → cross-sequence ordering/correlation
```

These fields must not be collapsed into a single sequence value during normalization.

---

## Normal WebSocket Bootstrap

Normal Bybit L2 is self-bootstrapping.

```text
CONNECT
    ↓
SUBSCRIBE
    ↓
WAIT FOR snapshot
    ↓
construct complete local book
    ↓
record u
    ↓
record seq
    ↓
apply deltas
    ↓
LIVE
```

Observed progression:

```text
snapshot
u = A

delta
u = A + 1

delta
u = A + 2

delta
u = A + 3
```

No REST request is required for normal WebSocket bootstrap.

---

## REST L2 Snapshots

Standard REST endpoint:

```text
GET /v5/market/orderbook
```

Observed categories:

```text
spot
linear
inverse
option
```

### Common Result Schema

All four inspected market families expose:

```text
a
b
cts
s
seq
ts
u
```

Therefore the standard REST book appears physically parser-compatible across:

```text
Spot
Linear
Inverse
Option
```

### Spot Example

```text
category = spot
symbol   = BTCUSDT
```

Observed:

```text
bids = 50
asks = 50
```

### Linear Example

```text
category = linear
symbol   = BTCUSDT
```

Observed:

```text
bids = 200
asks = 200
```

### Inverse Example

```text
category = inverse
symbol   = BTCUSD
```

Observed:

```text
bids = 200
asks = 200
```

### Option Example

```text
category = option
symbol   = BTC-3OCT26-77000-C-USDT
```

Observed REST result fields are identical to the other market families.

However, Option demonstrated that REST and normal WebSocket `u` must not automatically be assumed to share the same sequence namespace.

Observed WebSocket snapshot:

```text
u   = 117648
seq = 77167963823
```

Later REST snapshot:

```text
u   = 77168027524
seq = 77168027524
```

REST is therefore treated as an independent snapshot source unless an exchange-defined synchronization procedure explicitly relates it to a particular WebSocket stream.

---

## Full-Depth WebSocket L2

Full-depth topic:

```text
orderbook.full.<symbol>
```

Observed for:

```text
Spot
Linear
Inverse
```

Options were not inspected for full-depth.

### Physical Format

The payload structure matches normal L2:

```text
topic
type
ts
cts

data:
    s
    b
    a
    u
    seq
```

However, the protocol behavior is different.

### Delta Only

Every captured full-depth message used:

```text
type = delta
```

No initial WebSocket snapshot was observed.

Therefore:

```text
orderbook.full
```

cannot independently construct a complete local book.

### Spot Sequence

Observed:

```text
u = 24902638
u = 24902639
u = 24902640
u = 24902641
u = 24902642
```

### Linear Sequence

Observed:

```text
u = 16696405
u = 16696406
u = 16696407
u = 16696408
u = 16696409
```

### Inverse Sequence

Observed:

```text
u = 16695590
u = 16695591
u = 16695592
u = 16695593
u = 16695594
```

In every captured stream:

```text
next.u = previous.u + 1
```

`seq` remained increasing but non-consecutive.

---

## Full-Depth Sequence Model

Full-depth confirms the same two-domain sequence model:

```text
u
    → consecutive local book-update sequence

seq
    → non-consecutive cross sequence
```

Example:

```text
u:
24902638
24902639
24902640

seq:
114905278529
114905278616
114905278801
```

The two fields serve different purposes.

### Continuity

Normal continuity check:

```text
next.u == local_u + 1
```

A jump indicates missing updates.

### Cross Sequence

`seq` should be preserved for ordering and synchronization but must not be validated using:

```text
next.seq == previous.seq + 1
```

because the observed values are not consecutive.

---

## Full-Depth Bootstrap

Full-depth WebSocket data is delta-only.

The required architecture is therefore:

```text
open WebSocket
    ↓
buffer full-depth deltas
    ↓
fetch full-depth snapshot
    ↓
synchronize snapshot with buffered stream
    ↓
initialize complete local book
    ↓
replay applicable buffered deltas
    ↓
LIVE
```

The full-depth snapshot and WebSocket stream expose:

```text
u
seq
```

which provide synchronization metadata.

Conceptually:

```text
WS buffer:

u=A
seq=X

u=A+1
seq=Y

u=A+2
seq=Z

REST snapshot:

u=A+1
seq=Y
```

The engine can establish the snapshot state at the corresponding stream position and replay later deltas:

```text
load REST snapshot
    ↓
discard obsolete buffered updates
    ↓
replay u=A+2
    ↓
continue with u=A+3
    ↓
LIVE
```

The synchronized Spot/Linear/Inverse full-depth capture was started successfully. Final REST/WS synchronization observations should be added once that capture is complete.

---

## Gap Detection and Recovery

### Normal L2

A discontinuity in `u` indicates missing book updates.

```text
expected:
next.u = previous.u + 1
```

On failure:

```text
invalidate local book
    ↓
resubscribe / reconnect
    ↓
wait for new snapshot
    ↓
rebuild book
```

### Full Depth

Full-depth cannot recover from a sequence gap using WebSocket alone because it does not provide a new initial snapshot.

Recovery requires:

```text
invalidate book
    ↓
buffer WS deltas
    ↓
request full-depth snapshot
    ↓
resynchronize
    ↓
replay buffered deltas
    ↓
LIVE
```

The runtime must never guess missing levels from later deltas.

---

## Timestamp Model

Bybit exposes several useful timestamps.

### Trades

Individual trades contain:

```text
T
```

which represents trade time.

The outer WebSocket envelope contains:

```text
ts
```

### L2 WebSocket

Book messages contain:

```text
ts
cts
```

Observed relationship:

```text
cts
    → matching-engine timestamp

ts
    → system/message timestamp
```

### REST L2

Observed REST snapshots contain:

```text
ts
cts
```

The REST envelope additionally contains:

```text
time
```

### MarketForge Arrival Time

MarketForge should independently generate:

```text
arrival_timestamp_ns
```

when a WebSocket message or REST response reaches the ingestion engine.

Observed Bybit timestamps are Unix epoch milliseconds and must be converted to canonical nanoseconds.

---

## Sequence Domains

Bybit demonstrates why MarketForge must distinguish update continuity from broader exchange sequencing.

### Update Sequence

```text
u
```

Properties observed:

```text
book-specific
consecutive
used for gap detection
```

### Cross Sequence

```text
seq
```

Properties observed:

```text
monotonically increasing in captured streams
not consecutive
shared as broader ordering metadata
```

A canonical L2 sequence model should therefore be able to represent both concepts independently.

Candidate structure:

```text
update_sequence_first
update_sequence_last
update_sequence_previous

cross_sequence
```

For Bybit:

```text
update_sequence_first = u
update_sequence_last  = u
cross_sequence        = seq
```

The exact final canonical sequence schema remains pending comparison with Gate.io and OKX.

---

## Canonical Outputs

Bybit sources can produce:

```text
WebSocket tradesWebSocket trades
    ↓
Trade

Normal WebSocket snapshot
    ↓
L2Snapshot

Normal WebSocket delta
    ↓
L2Update

Standard REST order book
    ↓
L2Snapshot

Full-depth REST order book
    ↓
L2Snapshot

Full-depth WebSocket delta
    ↓
L2Update
```

The canonical representation does not depend on the acquisition mechanism.

The source format determines:

```text
transport
wire schema
timestamp mapping
sequence mapping
bootstrap behavior
```

while canonical normalization determines:

```text
instrument identity
price
quantity semantics
aggressor side
L2 side
set/delete action
canonical timestamps
canonical sequence metadata
```

---

## Provisional Format IDs

### Trades

| Format | Transport | Role | Markets |
|---|---|---|---|
| `BYBIT-LIVE-T1` | WebSocket | Trade stream | Spot |
| `BYBIT-LIVE-T2` | WebSocket | Trade stream | Linear, Inverse |
| `BYBIT-LIVE-T3` | WebSocket | Option trade stream | Option |

`BYBIT-LIVE-T1` and `BYBIT-LIVE-T2` are structurally similar, but derivatives expose the additional `L` field.

Option trades are materially different because they expose:

```text
iP
mP
iv
mIv
```

and therefore remain a separate format.

### Normal WebSocket Books

| Format | Transport | Role | Markets |
|---|---|---|---|
| `BYBIT-LIVE-B1` | WebSocket | L2 snapshot + delta | Spot, Linear, Inverse, Option |

All four inspected market families use the same observed physical book structure:

```text
topic
type
ts
cts

data:
    s
    b
    a
    u
    seq
```

Depth is configuration rather than a separate physical format.

### Standard REST Books

| Format | Transport | Role | Markets |
|---|---|---|---|
| `BYBIT-LIVE-B2` | REST | L2 snapshot | Spot, Linear, Inverse, Option |

Observed result schema:

```text
a
b
cts
s
seq
ts
u
```

The physical response format is shared across the inspected market families.

### Full-Depth WebSocket

| Format | Transport | Role | Markets |
|---|---|---|---|
| `BYBIT-LIVE-B3` | WebSocket | Full-depth L2 delta | Spot, Linear, Inverse |

Unlike `BYBIT-LIVE-B1`:

```text
BYBIT-LIVE-B1
    snapshot + delta
```

`BYBIT-LIVE-B3` is:

```text
delta only
```

and therefore requires an external snapshot before a complete local book can exist.

### Full-Depth REST

A separate format should represent the full-depth REST snapshot:

```text
BYBIT-LIVE-B4
```

with provisional role:

```text
REST full-depth L2 snapshot
```

for:

```text
Spot
Linear
Inverse
```

Final field semantics should be frozen after the synchronized full-depth REST/WebSocket capture is completed.

---

## Format Compatibility

Current evidence suggests:

```text
Trades
────────────────────────────────

Spot
    → BYBIT-LIVE-T1

Linear ─┐
        ├→ BYBIT-LIVE-T2
Inverse ┘

Option
    → BYBIT-LIVE-T3
```

Books:

```text
Normal WebSocket
────────────────────────────────

Spot ────┐
Linear ──┤
Inverse ─┼→ BYBIT-LIVE-B1
Option ──┘


Standard REST
────────────────────────────────

Spot ────┐
Linear ──┤
Inverse ─┼→ BYBIT-LIVE-B2
Option ──┘


Full Depth
────────────────────────────────

Spot ────┐
Linear ──┼→ BYBIT-LIVE-B3  WebSocket delta
Inverse ─┘

Spot ────┐
Linear ──┼→ BYBIT-LIVE-B4  REST snapshot
Inverse ─┘
```

---

## Bootstrap Strategies

Bybit requires two distinct L2 bootstrap strategies.

### Normal L2

```text
strategy:
    stream_snapshot
```

Process:

```text
connect
    ↓
subscribe
    ↓
receive snapshot
    ↓
construct book
    ↓
record u / seq
    ↓
apply deltas
    ↓
LIVE
```

No REST dependency exists.

### Full Depth

```text
strategy:
    rest_snapshot_with_buffered_updates
```

Process:

```text
connect
    ↓
subscribe full-depth WS
    ↓
buffer deltas
    ↓
request full-depth REST snapshot
    ↓
synchronize u / seq
    ↓
initialize book
    ↓
replay applicable buffered deltas
    ↓
LIVE
```

These strategies should remain explicit in the live-format specification rather than being inferred by the runtime from transport type.

---

## Trade Normalization

### Spot

```text
T   → event_timestamp
s   → instrument
i   → trade_id
p   → price
v   → quantity_base
S   → aggressor_side
seq → cross_sequence
BT  → block_trade
RPI → rpi
```

### Linear / Inverse

```text
T   → event_timestamp
s   → instrument
i   → trade_id
p   → price
v   → native quantity
S   → aggressor_side
seq → cross_sequence
BT  → block_trade
RPI → rpi
L   → tick_direction
```

Quantity normalization is instrument-aware.

For example:

```text
Linear
    native quantity
        ↓
    instrument specification
        ↓
    quantity_base
    quantity_quote
    quantity_contracts
```

Inverse instruments require their own contract-value conversion.

### Options

```text
T   → event_timestamp
s   → instrument
i   → trade_id
p   → price
v   → native quantity
S   → aggressor_side
seq → cross_sequence
BT  → block_trade

iP  → index_price
mP  → mark_price
iv  → implied_volatility
mIv → mark_implied_volatility
```

Option-specific canonical treatment remains pending canonical-schema review.

---

## Book Normalization

### Snapshot

For normal WebSocket snapshots and REST snapshots:

```text
b[]
    → canonical bids

a[]
    → canonical asks
```

Each level:

```text
[price, quantity]
```

becomes:

```text
price
quantity_base / quantity_contracts
```

according to instrument semantics.

### Delta

For each bid:

```text
quantity > 0
    → side=bid, action=set

quantity = 0
    → side=bid, action=delete
```

For each ask:

```text
quantity > 0
    → side=ask, action=set

quantity = 0
    → side=ask, action=delete
```

The same normalization applies to:

```text
normal WebSocket deltas
full-depth WebSocket deltas
```

The difference between those formats is bootstrap/state behavior, not price-level semantics.

---

## Data Integrity

Bybit exposes unusually useful live integrity metadata.

### Update Continuity

```text
u
```

can detect missing book updates.

Observed:

```text
next.u = previous.u + 1
```

for:

```text
Spot normal L2
Linear normal L2
Inverse normal L2
Option normal L2

Spot full-depth
Linear full-depth
Inverse full-depth
```

### Cross-Sequence Ordering

```text
seq
```

provides a separate ordering domain.

It should be preserved even though it is not consecutive.

### Matching-Engine Time

```text
cts
```

provides matching-engine timing for order-book events.

### Trade Correlation

Trades expose:

```text
T
seq
```

while books expose:

```text
cts
seq
```

This gives MarketForge useful source metadata for later research into:

```text
trade ↔ book consistency
event ordering
matching-engine timing
latency
price impact
book response to executions
```

These fields should therefore be preserved through normalization where practical.

---

## REST Snapshot Polling

Standard REST L2 is also a valid independent acquisition mode.

For example:

```text
request
    ↓
L2Snapshot
    ↓
wait 1 second
    ↓
request
    ↓
L2Snapshot
```

This allows acquisition such as:

```text
one 200-level snapshot every second
```

without maintaining a WebSocket order book.

The polling interval is runtime configuration and must respect Bybit rate limits.

It does not belong to the physical format definition.

---

## Book Reconciliation

REST snapshots may also be used as independent validation against locally reconstructed WebSocket books.

```text
WebSocket snapshot
    ↓
deltas
    ↓
maintained local book
    │
    │ periodic validation
    ↓
REST snapshot
    ↓
compare overlapping levels
```

Possible result:

```text
MATCH
    → continue

MISMATCH
    → integrity event
    → optional rebootstrap
```

The validation policy belongs to the runtime/analytics layer rather than the source format itself.

---

## Fixtures

Recommended structure:

```text
tests/fixtures/live/bybit/
├── spot/
│   ├── trades.raw.jsonl
│   ├── books.raw.jsonl
│   ├── book.rest.snapshot.json
│   ├── full.books.raw.jsonl
│   └── full.book.rest.snapshot.json
│
├── linear/
│   ├── trades.raw.jsonl
│   ├── books.raw.jsonl
│   ├── book.rest.snapshot.json
│   ├── full.books.raw.jsonl
│   └── full.book.rest.snapshot.json
│
├── inverse/
│   ├── trades.raw.jsonl
│   ├── books.raw.jsonl
│   ├── book.rest.snapshot.json
│   ├── full.books.raw.jsonl
│   └── full.book.rest.snapshot.json
│
└── option/
    ├── trades.raw.jsonl
    ├── books.raw.jsonl
    └── book.rest.snapshot.json
```

### Trade Fixtures

Preserve raw WebSocket envelopes.

For Options, use the broad stream:

```text
publicTrade.BTC
```

so the fixture reliably contains actual option executions.

### Normal Book Fixtures

Each fixture should contain:

```text
subscription acknowledgement
snapshot
multiple consecutive deltas
```

Expected pattern:

```text
snapshot:
    u = A

delta:
    u = A + 1

delta:
    u = A + 2
```

### Standard REST Fixtures

Preserve the complete raw response from:

```text
/v5/market/orderbook
```

for:

```text
Spot
Linear
Inverse
Option
```

### Full-Depth Fixtures

Full-depth fixtures require both:

```text
full.books.raw.jsonl
```

and:

```text
full.book.rest.snapshot.json
```

The WebSocket fixture should contain multiple consecutive deltas:

```text
u=A
u=A+1
u=A+2
...
```

The REST fixture must be captured while the WebSocket stream is active so the pair can later test the actual bootstrap/reconciliation engine.

---

## Remaining Canonical Review Items

Bybit discovery exposes several issues that should be reviewed after all exchanges have been inspected.

### Separate Sequence Domains

Current canonical sequencing must be able to preserve:

```text
book update sequence
cross sequence
```

independently.

Bybit demonstrates that these are not interchangeable.

### Option Trade Metadata

Determine whether:

```text
index_price
mark_price
implied_volatility
mark_implied_volatility
```

should become nullable fields on canonical `Trade` or belong to a specialized `OptionTrade`.

### Source Provenance

A canonical L2 snapshot may originate from:

```text
WebSocket native snapshot
REST native snapshot
reconstructed local book
```

The normalized representation may need explicit provenance metadata if downstream consumers must distinguish these sources.

### Arrival Time

Live ingestion should preserve:

```text
arrival_timestamp_ns
```

independently from exchange timestamps.

This is generated by MarketForge and is not part of the Bybit physical format.

---

## Current Bybit Live Model

```text
BYBIT
│
├── TRADES
│   │
│   ├── Spot
│   │      └── BYBIT-LIVE-T1
│   │
│   ├── Linear / Inverse
│   │      └── BYBIT-LIVE-T2
│   │
│   └── Option
│          └── BYBIT-LIVE-T3
│
└── L2
    │
    ├── Normal WebSocket
    │      └── BYBIT-LIVE-B1
    │
    ├── Standard REST Snapshot
    │      └── BYBIT-LIVE-B2
    │
    ├── Full-Depth WebSocket
    │      └── BYBIT-LIVE-B3
    │
    └── Full-Depth REST Snapshot
           └── BYBIT-LIVE-B4
```

Normal WebSocket:

```text
snapshot
    ↓
delta
    ↓
delta
```

Full-depth:

```text
REST snapshot
        +
buffered WS deltas
        ↓
synchronize
        ↓
replay
        ↓
LIVE
```

The physical formats are now sufficiently characterized for live-format catalog design. The synchronized full-depth REST/WS capture remains the final evidence required to freeze `BYBIT-LIVE-B4` bootstrap semantics.